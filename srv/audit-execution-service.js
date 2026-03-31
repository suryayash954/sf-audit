const cds = require('@sap/cds');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const https = require('https');
const { getDestination } = require('@sap-cloud-sdk/connectivity');
const pino = require('pino');
const fs = require('fs');
const path = require('path');

// Configure logger
const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    base: null
});

// Batch configuration
const BATCH_CONFIG = {
    USER_BATCH_SIZE: 1000,
    GROUP_BATCH_SIZE: 20,
    MEMBER_BATCH_SIZE: 500,
    API_PARALLEL_LIMIT: 5,
    RETRY_ATTEMPTS: 3,
    RETRY_DELAY: 1000,
    STATUS_UPDATE_INTERVAL: 2000 // Update status every 2 seconds
};

// Check if running locally
const isLocal = !process.env.VCAP_APPLICATION;

// Try to load local destination from default-env.json for local development
let localDestinations = null;
if (isLocal) {
    try {
        const envPath = path.join(__dirname, '..', 'default-env.json');
        if (fs.existsSync(envPath)) {
            const envConfig = JSON.parse(fs.readFileSync(envPath, 'utf8'));
            if (envConfig.destinations) {
                localDestinations = envConfig.destinations;
                console.log('✅ Loaded local destinations from default-env.json');
            }
        }
    } catch (err) {
        console.log('⚠️ No local destination file found, will use environment variables');
    }
}

module.exports = cds.service.impl(async function () {

    this.activeRuns = new Map();

    // ============================
    // USER SYNC ACTION
    // ============================
    this.on('syncUsers', async (req) => {
        const syncLogger = logger.child({ operation: 'SYNC_USERS' });
        const tx = cds.tx();

        try {
            syncLogger.info('Starting user synchronization from SuccessFactors');

            await tx.begin();

            const sfConfig = await _getSFDestination(syncLogger);
            const rawUsers = await _fetchAllUsers(sfConfig, syncLogger);
            const users = formatUsers(rawUsers);
            syncLogger.info({ totalFetched: users.length }, 'Users fetched from SuccessFactors');

            const stats = await _upsertUsers(tx, users, syncLogger);
            await tx.commit();

            syncLogger.info(stats, 'User synchronization completed');

            return {
                success: true,
                message: 'Users synchronized successfully',
                stats: {
                    inserted: stats.inserted,
                    updated: stats.updated,
                    total: users.length,
                    timestamp: new Date().toISOString()
                }
            };

        } catch (error) {
            syncLogger.error({ error: error.message, stack: error.stack }, 'User sync failed');

            try {
                await tx.rollback();
            } catch (rollbackError) {
                syncLogger.error({ error: rollbackError.message }, 'Rollback failed');
            }

            req.error(500, `User sync failed: ${error.message}`);
            return {
                success: false,
                message: error.message,
                stats: null
            };
        }
    });

    function formatUsers(rawUsers) {
        const formatted = rawUsers.map(user => {
            const parseDate = (dateValue) => {
                if (!dateValue) return null;
                if (typeof dateValue === 'string') {
                    const match = dateValue.match(/\/Date\((\d+)\)\//);
                    if (match) return new Date(parseInt(match[1]));
                    const parsed = new Date(dateValue);
                    if (!isNaN(parsed.getTime())) return parsed;
                }
                if (dateValue instanceof Date && !isNaN(dateValue.getTime())) return dateValue;
                return null;
            };
            
            let hireDate = user.hireDate ? parseDate(user.hireDate) : null;
            let terminationDate = user.companyExitDate ? parseDate(user.companyExitDate) : null;
            let lastModifiedDateTime = user.lastModifiedDateTime ? parseDate(user.lastModifiedDateTime) : new Date();
            
            return {
                userId: user.userId,
                userName: user.userName || user.username || '',
                firstName: user.firstName || '',
                lastName: user.lastName || '',
                status: user.status === 'T' ? 'inactive' : 'active',
                email: user.email || '',
                hireDate: hireDate,
                terminationDate: terminationDate,
                lastModifiedDateTime: lastModifiedDateTime,
                timeZone: user.timeZone || '',
                jobTitle: user.jobTitle || user.title || '',
                jobCode: user.jobCode || '',
                department: user.department || '',
                division: user.division || '',
                location: user.location || '',
                company: user.company || '',
                businessUnit: user.businessSegment || '',
                custom01: user.custom01 || '',
                custom02: user.custom02 || '',
                custom03: user.custom03 || '',
                lastSyncAt: new Date(),
                isActive: user.status !== 'T'
            };
        });
        return formatted;
    }

    // ============================
    // GET USERS ACTION
    // ============================
    this.on('getUsers', async (req) => {
        const { top = 100, skip = 0, status = null, search = null } = req.data;

        try {
            const db = await cds.connect.to('db');
            let query = SELECT.from('Users');
            let conditions = [];
            
            if (status) conditions.push({ status: status });
            if (search && search.trim()) {
                const searchTerm = `%${search.trim()}%`;
                conditions.push({
                    or: [
                        { userName: { like: searchTerm } },
                        { firstName: { like: searchTerm } },
                        { lastName: { like: searchTerm } },
                        { email: { like: searchTerm } },
                        { userId: { like: searchTerm } }
                    ]
                });
            }
            
            if (conditions.length > 0) {
                query = query.where(conditions.length === 1 ? conditions[0] : conditions);
            }
            
            let countQuery = SELECT.from('Users').columns('count(*) as total');
            if (conditions.length > 0) {
                countQuery = countQuery.where(conditions.length === 1 ? conditions[0] : conditions);
            }
            
            const countResult = await db.run(countQuery);
            const total = countResult[0]?.total || 0;
            
            const users = await db.run(
                query.orderBy('userName').limit(top, skip).columns([
                    'userId', 'userName', 'firstName', 'lastName', 'status', 'email',
                    'hireDate', 'jobTitle', 'department', 'lastModifiedDateTime'
                ])
            );

            return {
                success: true,
                data: users,
                pagination: { total, top, skip }
            };

        } catch (error) {
            logger.error({ error: error.message }, 'Failed to get users');
            req.error(500, `Failed to get users: ${error.message}`);
            return { success: false, data: [], pagination: { total: 0, top, skip } };
        }
    });

    // ============================
    // RUN AUDIT ACTION (Manual trigger)
    // ============================
    this.on('runAudit', async (req) => {
        const { auditRunID } = req.data;
        
        if (!auditRunID) {
            req.error(400, 'auditRunID is required');
            return;
        }
        
        const tx = cds.transaction(req);
        
        try {
            const auditRun = await tx.run(
                SELECT.one.from('AuditRuns').where({ ID: auditRunID })
            );
            
            if (!auditRun) {
                req.error(404, `Audit run ${auditRunID} not found`);
                return;
            }
            
            if (auditRun.status === 'RUNNING') {
                req.error(409, `Audit run ${auditRunID} is already running`);
                return;
            }
            
            if (auditRun.status === 'COMPLETED') {
                req.error(409, `Audit run ${auditRunID} is already completed`);
                return;
            }
            
            // Start audit asynchronously
            setImmediate(() => {
                _runAuditAsync.call(this, auditRunID, {
                    mode: auditRun.mode,
                    extractGroups: auditRun.extractGroups,
                    extractRoles: auditRun.extractRoles,
                    sampleGroupSize: auditRun.sampleGroupSize,
                    sampleMemberSize: auditRun.sampleMemberSize,
                    sampleRoleSize: auditRun.sampleRoleSize
                }).catch(err => {
                    logger.error({ auditRunID, error: err.message }, 'Background audit failed');
                });
            });
            
            return {
                success: true,
                message: `Audit run ${auditRunID} started successfully`,
                auditRunID: auditRunID
            };
            
        } catch (error) {
            logger.error({ error: error.message }, 'Failed to start audit');
            req.error(500, `Failed to start audit: ${error.message}`);
        }
    });

    // ============================
    // GET AUDIT STATUS (with real-time updates)
    // ============================
    this.on('getAuditStatus', async (req) => {
        const { auditRunID } = req.data;
        const tx = cds.transaction(req);

        try {
            const auditRun = await tx.run(
                SELECT.one.from('AuditRuns').where({ ID: auditRunID })
            );

            if (!auditRun) {
                req.error(404, `Audit run ${auditRunID} not found`);
                return;
            }

            const activeRun = this.activeRuns?.get(auditRunID);

            // Calculate progress based on actual data if audit is running
            let progress = activeRun?.progress || 0;
            let currentPhase = activeRun?.message || 'Idle';
            
            // If audit is running, calculate progress from actual counts
            if (auditRun.status === 'RUNNING') {
                let totalGroups = auditRun.groupsProcessed || 0;
                let totalMembers = auditRun.membershipsProcessed || 0;
                let totalRoles = auditRun.rolesProcessed || 0;
                
                // Estimate progress based on what's been processed
                if (auditRun.mode === 'SAMPLE') {
                    const targetGroups = auditRun.sampleGroupSize || 5;
                    const targetMembers = auditRun.sampleMemberSize || 10;
                    const targetRoles = auditRun.sampleRoleSize || 3;
                    const totalTarget = targetGroups + targetMembers + targetRoles;
                    const processed = totalGroups + totalMembers + totalRoles;
                    progress = totalTarget > 0 ? Math.min(Math.floor((processed / totalTarget) * 100), 99) : 0;
                } else {
                    // For FULL mode, progress is based on phases
                    progress = activeRun?.progress || 0;
                }
            }

            return {
                status: auditRun.status,
                progress: progress,
                currentPhase: currentPhase,
                message: currentPhase,
                groupCount: auditRun.groupsProcessed || 0,
                userCount: auditRun.membershipsProcessed || 0,
                roleCount: auditRun.rolesProcessed || 0,
                memberCount: 0,
                startTime: auditRun.startTime,
                endTime: auditRun.endTime,
                errorMessage: auditRun.errorMessage
            };

        } catch (error) {
            req.error(500, error.message);
        }
    });

    // ============================
    // LIST AUDIT RUNS
    // ============================
    this.on('listAuditRuns', async (req) => {
        const { status = null, top = 50, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            let query = SELECT.from('AuditRuns')
                .columns([
                    'ID', 'name', 'description', 'status', 'mode',
                    'startTime', 'endTime', 'createdAt',
                    'groupsProcessed', 'membershipsProcessed', 'rolesProcessed'
                ]);

            if (status) {
                query = query.where({ status: status });
            }

            const audits = await tx.run(
                query.orderBy('createdAt desc').limit(top, skip)
            );

            return audits.map(a => ({
                ID: a.ID,
                name: a.name,
                description: a.description,
                status: a.status,
                mode: a.mode,
                startTime: a.startTime,
                endTime: a.endTime,
                createdAt: a.createdAt,
                groupCount: a.groupsProcessed || 0,
                userCount: a.membershipsProcessed || 0,
                roleCount: a.rolesProcessed || 0
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    });

    // ============================
    // DELETE AUDIT RUN
    // ============================
    this.on('deleteAuditRun', async (req) => {
        const { auditRunID } = req.data;
        const tx = cds.transaction(req);

        try {
            await tx.begin();

            await tx.run(DELETE.from('GroupMembers').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('Groups').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('Roles').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('MultiGroupUsers').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('UnusedRoles').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('ExecutiveSummary').where({ auditRunID_ID: auditRunID }));
            await tx.run(DELETE.from('AuditRuns').where({ ID: auditRunID }));

            await tx.commit();

            return {
                success: true,
                message: `Audit run ${auditRunID} deleted successfully`
            };

        } catch (error) {
            await tx.rollback();
            req.error(500, error.message);
        }
    });

    // ============================
    // BEFORE CREATE - Audit Runs
    // ============================
    this.before('CREATE', 'AuditRuns', async (req) => {
        const auditLogger = logger.child({ operation: 'CREATE_AUDIT' });

        if (!req.data.name || !req.data.mode) {
            auditLogger.error('Missing required fields');
            req.error(400, 'Missing required fields: name, mode');
        }

        if (!['FULL', 'SAMPLE'].includes(req.data.mode)) {
            auditLogger.error({ mode: req.data.mode }, 'Invalid mode');
            req.error(400, 'Mode must be FULL or SAMPLE');
        }

        req.data.status = 'CREATED';
        req.data.startTime = null;
        req.data.endTime = null;
        req.data.userSyncRequired = true;

        auditLogger.info({ name: req.data.name, mode: req.data.mode }, 'Audit run created');
    });

    // ============================
    // AFTER CREATE - Audit Runs (No auto-audit)
    // ============================
    this.after('CREATE', 'AuditRuns', async (data, req) => {
        logger.info({ auditRunID: data.ID, name: data.name }, 
            'Audit run created. Use runAudit action to start execution.');
        // Do nothing - audit will not run automatically
    });

    // ============================
    // MAIN BACKGROUND JOB with Continuous Status Updates
    // ============================
    async function _runAuditAsync(auditRunID, config) {
        const auditLogger = logger.child({ auditRunID, phase: 'AUDIT' });
        const tx = cds.tx();

        const startTime = Date.now();

        await tx.begin();

        try {
            auditLogger.info({ config }, 'Starting audit execution');

            await tx.run(
                UPDATE('AuditRuns')
                    .set({ status: 'RUNNING', startTime: new Date() })
                    .where({ ID: auditRunID })
            );

            const activeRun = {
                id: auditRunID,
                cancelled: false,
                progress: 0,
                message: 'Initializing...',
                startTime: startTime
            };
            this.activeRuns.set(auditRunID, activeRun);

            const sfConfig = await _getSFDestination(auditLogger);

            // PHASE 1: User Sync (10%)
            activeRun.message = 'Checking user sync status...';
            activeRun.progress = 5;
            this.activeRuns.set(auditRunID, activeRun);
            
            const auditRun = await tx.run(SELECT.one.from('AuditRuns').where({ ID: auditRunID }));

            if (auditRun.userSyncRequired && !auditRun.userSyncCompleted) {
                auditLogger.info('User sync required - fetching from SF');
                activeRun.message = 'Fetching users from SuccessFactors...';
                activeRun.progress = 8;
                this.activeRuns.set(auditRunID, activeRun);
                
                const users = await _fetchAllUsers(sfConfig, auditLogger);
                
                activeRun.message = 'Saving users to database...';
                activeRun.progress = 9;
                this.activeRuns.set(auditRunID, activeRun);
                
                await _upsertUsers(tx, users, auditLogger);
                await tx.run(
                    UPDATE('AuditRuns')
                        .set({ userSyncCompleted: true, userSyncAt: new Date() })
                        .where({ ID: auditRunID })
                );
                
                activeRun.progress = 10;
                auditLogger.info({ userCount: users.length }, 'Users synced successfully');
            } else {
                activeRun.progress = 10;
            }
            this.activeRuns.set(auditRunID, activeRun);

            // PHASE 2: Groups (10% - 30%)
            let groupStats = { static: 0, dynamic: 0, total: 0 };
            let memberStats = { membershipsProcessed: 0 };

            if (config.extractGroups) {
                auditLogger.info('Starting group extraction');
                activeRun.message = 'Fetching static groups...';
                activeRun.progress = 15;
                this.activeRuns.set(auditRunID, activeRun);
                
                const [staticGroups, dynamicGroups] = await Promise.all([
                    _fetchGroupsWithPagination(sfConfig, true, auditLogger),
                    _fetchGroupsWithPagination(sfConfig, false, auditLogger)
                ]);

                groupStats = {
                    static: staticGroups.length,
                    dynamic: dynamicGroups.length,
                    total: staticGroups.length + dynamicGroups.length
                };

                activeRun.message = 'Saving groups to database...';
                activeRun.progress = 20;
                this.activeRuns.set(auditRunID, activeRun);
                
                await _saveGroups(tx, auditRunID, staticGroups, 'STATIC', auditLogger);
                await _saveGroups(tx, auditRunID, dynamicGroups, 'DYNAMIC', auditLogger);
                
                activeRun.progress = 25;
                this.activeRuns.set(auditRunID, activeRun);

                // PHASE 3: Group Members (30% - 60%)
                auditLogger.info('Starting member extraction');
                activeRun.message = 'Extracting group members...';
                activeRun.progress = 35;
                this.activeRuns.set(auditRunID, activeRun);

                const allGroups = [...staticGroups, ...dynamicGroups];
                let groupsToProcess = allGroups;

                if (config.mode === 'SAMPLE' && config.sampleGroupSize > 0) {
                    groupsToProcess = allGroups.slice(0, config.sampleGroupSize);
                    auditLogger.info({ original: allGroups.length, sampled: groupsToProcess.length }, 'Applied group sampling');
                }

                // Process members with progress updates
                const totalGroupsToProcess = groupsToProcess.length;
                let processedGroups = 0;
                
                const memberStatsResult = await _fetchAndSaveMembersWithProgress(
                    tx, auditRunID, groupsToProcess, sfConfig, config, auditLogger,
                    (processed, total, message) => {
                        processedGroups = processed;
                        const progressPercent = 35 + Math.floor((processed / total) * 25);
                        activeRun.progress = Math.min(progressPercent, 60);
                        activeRun.message = message || `Processing group ${processed}/${total}...`;
                        this.activeRuns.set(auditRunID, activeRun);
                    }
                );
                
                memberStats = memberStatsResult;
                activeRun.progress = 60;
                activeRun.message = 'Updating group statistics...';
                this.activeRuns.set(auditRunID, activeRun);
                
                await _updateGroupStatistics(tx, auditRunID, auditLogger);
            }

            // PHASE 4: Roles (60% - 80%)
            let rolesProcessed = 0;
            if (config.extractRoles) {
                auditLogger.info('Starting role extraction');
                activeRun.message = 'Fetching roles...';
                activeRun.progress = 65;
                this.activeRuns.set(auditRunID, activeRun);
                
                const roles = await _fetchRolesWithPagination(sfConfig, auditLogger);
                let rolesToProcess = roles;

                if (config.mode === 'SAMPLE' && config.sampleRoleSize > 0) {
                    rolesToProcess = roles.slice(0, config.sampleRoleSize);
                    auditLogger.info({ original: roles.length, sampled: rolesToProcess.length }, 'Applied role sampling');
                }

                activeRun.message = `Saving ${rolesToProcess.length} roles...`;
                activeRun.progress = 75;
                this.activeRuns.set(auditRunID, activeRun);
                
                rolesProcessed = rolesToProcess.length;
                await _saveRoles(tx, auditRunID, rolesToProcess, auditLogger);
                
                activeRun.progress = 80;
                this.activeRuns.set(auditRunID, activeRun);
            }

            // PHASE 5: Analytics (80% - 100%)
            auditLogger.info('Generating analytics');
            activeRun.message = 'Generating analytics...';
            activeRun.progress = 85;
            this.activeRuns.set(auditRunID, activeRun);
            
            await _generateAnalytics(tx, auditRunID, auditLogger);
            
            activeRun.message = 'Finalizing audit...';
            activeRun.progress = 95;
            this.activeRuns.set(auditRunID, activeRun);

            // COMPLETE
            const executionTime = Date.now() - startTime;

            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'COMPLETED',
                        endTime: new Date(),
                        groupsProcessed: groupStats.total || 0,
                        membershipsProcessed: memberStats.membershipsProcessed || 0,
                        rolesProcessed: rolesProcessed
                    })
                    .where({ ID: auditRunID })
            );

            await tx.commit();
            
            activeRun.message = 'Audit completed successfully!';
            activeRun.progress = 100;
            this.activeRuns.set(auditRunID, activeRun);

            auditLogger.info({
                executionTime: `${executionTime}ms`,
                groups: groupStats,
                members: memberStats,
                roles: rolesProcessed
            }, 'Audit completed successfully');

        } catch (error) {
            auditLogger.error({ error: error.message, stack: error.stack }, 'Audit failed');

            try {
                await tx.run(
                    UPDATE('AuditRuns')
                        .set({
                            status: 'FAILED',
                            endTime: new Date(),
                            errorMessage: error.message.substring(0, 2000)
                        })
                        .where({ ID: auditRunID })
                );
                await tx.commit();
            } catch (dbError) {
                auditLogger.error({ error: dbError.message }, 'Failed to update audit status');
            }
            
            const activeRun = this.activeRuns.get(auditRunID);
            if (activeRun) {
                activeRun.message = `Failed: ${error.message.substring(0, 100)}`;
                this.activeRuns.set(auditRunID, activeRun);
            }
            
        } finally {
            // Keep active run for a while to allow status queries
            setTimeout(() => {
                this.activeRuns.delete(auditRunID);
            }, 30000);
        }
    }

    // ============================
    // HELPER FUNCTIONS
    // ============================

    async function _getSFDestination(auditLogger) {
        try {
            auditLogger.info('Attempting to get destination from BTP Destination Service');
            const destination = await getDestination({ destinationName: 'successfactors' });

            if (!destination) {
                throw new Error('Destination "successfactors" not found');
            }

            auditLogger.info({ url: destination.url, name: destination.name }, 'Destination retrieved from BTP');

            let baseURL = destination.url;
            if (!baseURL.endsWith('/')) baseURL = baseURL + '/';

            return {
                baseURL: baseURL,
                auth: { username: destination.username, password: destination.password },
                headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
                httpsAgent: new https.Agent({ rejectUnauthorized: false }),
                timeout: 30000
            };

        } catch (error) {
            auditLogger.error({ error: error.message }, 'Failed to get destination from BTP');
            throw new Error(
                'No SuccessFactors credentials found. Please set up:\n' +
                '  - default-env.json with destination, OR\n' +
                '  - Environment variables (SF_BASE_URL, SF_USERNAME, SF_PASSWORD), OR\n' +
                '  - BTP Destination Service with name "successfactors"'
            );
        }
    }

    async function _fetchAllUsers(sfConfig, auditLogger) {
        const users = [];
        let skip = 0;
        const top = 1000;
        let hasMore = true;

        auditLogger.info('Fetching all users from SuccessFactors');

        while (hasMore) {
            try {
                const url = `${sfConfig.baseURL}/User?$format=json&$top=${top}&$skip=${skip}`;
                const response = await axios.get(url, {
                    auth: sfConfig.auth,
                    headers: sfConfig.headers,
                    httpsAgent: sfConfig.httpsAgent
                });
                const results = response.data?.d?.results || [];
                users.push(...results);
                hasMore = results.length === top;
                skip += top;
                auditLogger.debug({ fetched: results.length, total: users.length }, 'Users batch fetched');
            } catch (error) {
                auditLogger.error({ error: error.message, skip }, 'Failed to fetch users batch');
                throw error;
            }
        }

        auditLogger.info({ totalUsers: users.length }, 'All users fetched');
        return users;
    }

   async function _upsertUsers(tx, users, auditLogger) {
    let inserted = 0;
    let updated = 0;

    auditLogger.info({ count: users.length }, 'Starting user upsert');

    for (let i = 0; i < users.length; i += BATCH_CONFIG.USER_BATCH_SIZE) {
        const batch = users.slice(i, i + BATCH_CONFIG.USER_BATCH_SIZE);
        
        for (const user of batch) {
            try {
                // Check if user exists
                const existing = await tx.run(
                    SELECT.one.from('Users').where({ userId: user.userId })
                );

                const userData = {
                    userId: user.userId,
                    userName: user.userName || '',
                    firstName: user.firstName || '',
                    lastName: user.lastName || '',
                    status: user.status || 'active',
                    email: user.email || '',
                    hireDate: user.hireDate instanceof Date && !isNaN(user.hireDate) ? user.hireDate : null,
                    terminationDate: user.terminationDate instanceof Date && !isNaN(user.terminationDate) ? user.terminationDate : null,
                    lastModifiedDateTime: user.lastModifiedDateTime instanceof Date && !isNaN(user.lastModifiedDateTime) ? user.lastModifiedDateTime : new Date(),
                    jobTitle: user.jobTitle || '',
                    department: user.department || '',
                    lastSyncAt: user.lastSyncAt instanceof Date && !isNaN(user.lastSyncAt) ? user.lastSyncAt : new Date(),
                    isActive: user.isActive !== false
                };

                if (existing && existing.length > 0) {
                    // UPDATE existing user
                    await tx.run(
                        UPDATE('Users')
                            .set(userData)
                            .where({ userId: user.userId })
                    );
                    updated++;
                    // Log every 100 updates to avoid spam
                    if (updated % 100 === 0) {
                        auditLogger.debug({ updated }, 'Users updated so far');
                    }
                } else {
                    // INSERT new user
                    await tx.run(
                        INSERT.into('Users').entries(userData)
                    );
                    inserted++;
                }

            } catch (error) {
                // Check if it's a unique constraint violation (user already exists)
                if (error.message.includes('unique constraint') || error.message.includes('already exists')) {
                    // User already exists - try to update instead
                    try {
                        const userData = {
                            userName: user.userName || '',
                            firstName: user.firstName || '',
                            lastName: user.lastName || '',
                            status: user.status || 'active',
                            email: user.email || '',
                            hireDate: user.hireDate instanceof Date && !isNaN(user.hireDate) ? user.hireDate : null,
                            terminationDate: user.terminationDate instanceof Date && !isNaN(user.terminationDate) ? user.terminationDate : null,
                            lastModifiedDateTime: user.lastModifiedDateTime instanceof Date && !isNaN(user.lastModifiedDateTime) ? user.lastModifiedDateTime : new Date(),
                            jobTitle: user.jobTitle || '',
                            department: user.department || '',
                            lastSyncAt: user.lastSyncAt instanceof Date && !isNaN(user.lastSyncAt) ? user.lastSyncAt : new Date(),
                            isActive: user.isActive !== false
                        };
                        
                        await tx.run(
                            UPDATE('Users')
                                .set(userData)
                                .where({ userId: user.userId })
                        );
                        updated++;
                    } catch (updateError) {
                        auditLogger.error({ 
                            userId: user.userId, 
                            error: updateError.message
                        }, 'Failed to update existing user');
                    }
                } else {
                    auditLogger.error({ 
                        userId: user.userId, 
                        error: error.message
                    }, 'Failed to upsert user');
                }
            }
        }

        auditLogger.debug({
            batch: Math.floor(i / BATCH_CONFIG.USER_BATCH_SIZE) + 1,
            inserted,
            updated
        }, 'User batch processed');
    }

    auditLogger.info({ inserted, updated, total: users.length }, 'User upsert completed');
    return { inserted, updated };
}
    async function _fetchGroupsWithPagination(sfConfig, isStatic, auditLogger) {
        const groups = [];
        let skip = 0;
        const top = 100;
        let hasMore = true;

        while (hasMore) {
            try {
                const url = `${sfConfig.baseURL}/DynamicGroup?$format=json&$top=${top}&$skip=${skip}&$filter=staticGroup eq ${isStatic}`;
                const response = await axios.get(url, {
                    auth: sfConfig.auth,
                    headers: sfConfig.headers,
                    httpsAgent: sfConfig.httpsAgent
                });
                const results = response.data?.d?.results || [];
                groups.push(...results);
                hasMore = results.length === top;
                skip += top;
            } catch (error) {
                auditLogger.error({ error: error.message, isStatic, skip }, 'Failed to fetch groups batch');
                throw error;
            }
        }
        return groups;
    }

    async function _saveGroups(tx, auditRunID, groups, groupType, auditLogger) {
        if (!groups.length) return;

        const entries = groups.map(g => ({
            ID: uuidv4(),
            auditRunID_ID: auditRunID,
            groupID: String(g.groupID || g.id),
            groupName: g.groupName || g.name,
            groupType: groupType,
            groupTypeInternal: g.groupTypeInternal || 'permission',
            activeMembershipCount: g.activeMembershipCount || 0,
            totalMemberCount: g.totalMemberCount || 0,
            createdBy: g.createdBy || '',
            lastModifiedDate: g.lastModifiedDate ? new Date(g.lastModifiedDate) : null
        }));

        for (let i = 0; i < entries.length; i += BATCH_CONFIG.GROUP_BATCH_SIZE) {
            const batch = entries.slice(i, i + BATCH_CONFIG.GROUP_BATCH_SIZE);
            const validBatch = batch.filter(entry => {
                if (entry.lastModifiedDate === null) return true;
                return !isNaN(entry.lastModifiedDate.getTime());
            });
            if (validBatch.length > 0) {
                await tx.run(INSERT.into('Groups').entries(validBatch));
            }
        }
        auditLogger.info({ groupType, count: entries.length }, 'Groups saved');
    }

    async function _fetchAndSaveMembersWithProgress(tx, auditRunID, groups, sfConfig, config, auditLogger, progressCallback) {
        const memberships = new Map();
        let processedGroups = 0;

        auditLogger.info({ groupCount: groups.length }, 'Starting member extraction');

        const groupChunks = [];
        for (let i = 0; i < groups.length; i += BATCH_CONFIG.API_PARALLEL_LIMIT) {
            groupChunks.push(groups.slice(i, i + BATCH_CONFIG.API_PARALLEL_LIMIT));
        }

        for (const chunk of groupChunks) {
            const chunkPromises = chunk.map(async (group) => {
                try {
                    const members = await _fetchGroupMembers(group.groupID, sfConfig, auditLogger);
                    for (const member of members) {
                        const membershipKey = `${group.groupID}-${member.userId}`;
                        if (!memberships.has(membershipKey)) {
                            memberships.set(membershipKey, {
                                groupID: group.groupID,
                                groupName: group.groupName,
                                userId: member.userId,
                                userName: member.userName || member.username
                            });
                        }
                    }
                    processedGroups++;
                    if (progressCallback) {
                        progressCallback(processedGroups, groups.length, `Extracting members from group ${processedGroups}/${groups.length}...`);
                    }
                } catch (error) {
                    auditLogger.error({ groupID: group.groupID, error: error.message }, 'Failed to fetch group members');
                }
            });
            await Promise.all(chunkPromises);
        }

        auditLogger.info({ memberships: memberships.size }, 'Membership data collected');
        
        if (progressCallback) {
            progressCallback(processedGroups, groups.length, 'Saving memberships to database...');
        }
        
        const membershipsSaved = await _saveMemberships(tx, auditRunID, Array.from(memberships.values()), auditLogger);

        return { membershipsProcessed: membershipsSaved };
    }

    async function _fetchGroupMembers(groupId, sfConfig, auditLogger) {
        const approaches = [
            `${sfConfig.baseURL}/getUsersByDynamicGroup?groupId=${groupId}L&$format=json&$select=userId,userName,firstName,lastName`,
            `${sfConfig.baseURL}/getUsersByDynamicGroup?groupId=${groupId}&$format=json&$select=userId,userName,firstName,lastName`,
            `${sfConfig.baseURL}/DynamicGroup(${groupId})/users?$format=json&$select=userId,userName,firstName,lastName`
        ];

        for (const url of approaches) {
            try {
                const response = await axios.get(url, {
                    auth: sfConfig.auth,
                    headers: sfConfig.headers,
                    httpsAgent: sfConfig.httpsAgent,
                    timeout: sfConfig.timeout
                });

                let members = [];
                if (response.data?.d?.results) members = response.data.d.results;
                else if (response.data?.d) members = response.data.d;
                else if (response.data?.results) members = response.data.results;
                else if (Array.isArray(response.data)) members = response.data;

                if (members && members.length > 0) {
                    auditLogger.debug({ groupId, memberCount: members.length }, 'Members fetched successfully');
                    return members;
                }
            } catch (error) {
                auditLogger.debug({ groupId, url, error: error.message }, 'Approach failed');
            }
        }
        auditLogger.warn({ groupId }, 'All approaches failed to fetch group members');
        return [];
    }

    async function _saveMemberships(tx, auditRunID, memberships, auditLogger) {
        if (!memberships.length) return 0;

        const userIds = [...new Set(memberships.map(m => m.userId))];
        const existingUsers = await tx.run(
            SELECT.from('Users').where({ userId: { in: userIds } })
        );
        const existingUserIds = new Set(existingUsers.map(u => u.userId));
        const validMemberships = memberships.filter(m => existingUserIds.has(m.userId));

        const entries = validMemberships.map(m => ({
            ID: uuidv4(),
            auditRunID_ID: auditRunID,
            groupID: m.groupID,
            groupName: m.groupName,
            userId: m.userId,
            userName: m.userName
        }));

        await tx.run(DELETE.from('GroupMembers').where({ auditRunID_ID: auditRunID }));

        for (let i = 0; i < entries.length; i += BATCH_CONFIG.MEMBER_BATCH_SIZE) {
            const batch = entries.slice(i, i + BATCH_CONFIG.MEMBER_BATCH_SIZE);
            await tx.run(INSERT.into('GroupMembers').entries(batch));
        }

        auditLogger.info({ count: entries.length }, 'Memberships saved');
        return entries.length;
    }

    async function _fetchRolesWithPagination(sfConfig, auditLogger) {
        const roles = [];
        let skip = 0;
        const top = 100;
        let hasMore = true;

        while (hasMore) {
            try {
                const url = `${sfConfig.baseURL}/RBPRole?$format=json&$top=${top}&$skip=${skip}`;
                const response = await axios.get(url, {
                    auth: sfConfig.auth,
                    headers: sfConfig.headers,
                    httpsAgent: sfConfig.httpsAgent
                });
                const results = response.data?.d?.results || [];
                roles.push(...results);
                hasMore = results.length === top;
                skip += top;
            } catch (error) {
                auditLogger.error({ error: error.message, skip }, 'Failed to fetch roles batch');
                throw error;
            }
        }
        return roles;
    }

    async function _saveRoles(tx, auditRunID, roles, auditLogger) {
        if (!roles.length) return;

        const entries = roles.map(r => {
            let lastModifiedDate = null;
            if (r.lastModifiedDate) {
                try {
                    lastModifiedDate = new Date(r.lastModifiedDate);
                    if (isNaN(lastModifiedDate.getTime())) lastModifiedDate = new Date();
                } catch (e) {
                    lastModifiedDate = new Date();
                }
            }
            return {
                ID: uuidv4(),
                auditRunID_ID: auditRunID,
                roleId: r.roleId || r.id,
                roleName: r.roleName || r.name,
                roleDesc: r.roleDesc || r.description || '',
                roleType: r.roleType || 'standard',
                userType: r.userType || 'user',
                lastModifiedBy: r.lastModifiedBy || '',
                lastModifiedDate: lastModifiedDate
            };
        });

        for (let i = 0; i < entries.length; i += 500) {
            const batch = entries.slice(i, i + 500);
            await tx.run(INSERT.into('Roles').entries(batch));
        }
        auditLogger.info({ count: entries.length }, 'Roles saved');
    }

    async function _updateGroupStatistics(tx, auditRunID, auditLogger) {
        try {
            await tx.run(`
                UPDATE sap_sf_audit_Groups 
                SET activeMembershipCount = (
                    SELECT COUNT(*) 
                    FROM sap_sf_audit_GroupMembers 
                    WHERE sap_sf_audit_GroupMembers.groupID = Groups.groupID 
                    AND sap_sf_audit_GroupMembers.auditRunID_ID = ?
                )
                WHERE auditRunID_ID = ?
            `, [auditRunID, auditRunID]);
            
            auditLogger.info('Group statistics updated successfully');
        } catch (error) {
            auditLogger.error({ error: error.message }, 'Failed to update group statistics');
            throw error;
        }
    }

    async function _generateAnalytics(tx, auditRunID, auditLogger) {
        try {
            await tx.run(`
                INSERT INTO sap_sf_audit_UserGroupCountDistribution (ID, auditRunID_ID, bucket, userCount)
                SELECT 
                    lower(hex(randomblob(16))),
                    ?,
                    CASE 
                        WHEN group_count = 1 THEN '1 group'
                        WHEN group_count = 2 THEN '2 groups'
                        WHEN group_count <= 4 THEN '3–4 groups'
                        WHEN group_count <= 7 THEN '5–7 groups'
                        ELSE '8+ groups'
                    END,
                    COUNT(*)
                FROM (
                    SELECT userId, COUNT(*) as group_count
                    FROM sap_sf_audit_GroupMembers
                    WHERE auditRunID_ID = ?
                    GROUP BY userId
                )
                GROUP BY bucket
            `, [auditRunID, auditRunID]);
            
            await tx.run(`
                INSERT INTO sap_sf_audit_MultiGroupUsers (ID, auditRunID_ID, userId, userName, groupCount, groupNames, riskLevel, riskScore, riskCategory, recommendedAction)
                SELECT 
                    lower(hex(randomblob(16))),
                    ?,
                    gm.userId,
                    u.userName,
                    COUNT(DISTINCT gm.groupID) as groupCount,
                    GROUP_CONCAT(DISTINCT gm.groupName, ', ') as groupNames,
                    CASE 
                        WHEN COUNT(DISTINCT gm.groupID) >= 4 THEN 'High'
                        WHEN COUNT(DISTINCT gm.groupID) >= 3 THEN 'Medium'
                        ELSE 'Low'
                    END as riskLevel,
                    COUNT(DISTINCT gm.groupID) as riskScore,
                    CASE 
                        WHEN COUNT(DISTINCT gm.groupID) >= 4 THEN 'High Risk'
                        WHEN COUNT(DISTINCT gm.groupID) >= 3 THEN 'Medium Risk'
                        ELSE 'Low Risk'
                    END as riskCategory,
                    CASE 
                        WHEN COUNT(DISTINCT gm.groupID) >= 4 THEN 'Review access immediately'
                        WHEN COUNT(DISTINCT gm.groupID) >= 3 THEN 'Review access'
                        ELSE 'Monitor'
                    END as recommendedAction
                FROM sap_sf_audit_GroupMembers gm
                LEFT JOIN sap_sf_audit_Users u ON u.userId = gm.userId AND u.auditRunID_ID = ?
                WHERE gm.auditRunID_ID = ?
                GROUP BY gm.userId
                HAVING COUNT(DISTINCT gm.groupID) > 1
            `, [auditRunID, auditRunID, auditRunID, auditRunID]);
            
            await tx.run(`
                INSERT INTO sap_sf_audit_GroupSizeDistribution (ID, auditRunID_ID, bucket, groupCount)
                SELECT 
                    lower(hex(randomblob(16))),
                    ?,
                    CASE 
                        WHEN totalMemberCount <= 5 THEN '1–5 members'
                        WHEN totalMemberCount <= 20 THEN '6–20 members'
                        WHEN totalMemberCount <= 50 THEN '21–50 members'
                        WHEN totalMemberCount <= 100 THEN '51–100 members'
                        ELSE '100+ members'
                    END,
                    COUNT(*)
                FROM sap_sf_audit_Groups
                WHERE auditRunID_ID = ?
                GROUP BY 
                    CASE 
                        WHEN totalMemberCount <= 5 THEN '1–5 members'
                        WHEN totalMemberCount <= 20 THEN '6–20 members'
                        WHEN totalMemberCount <= 50 THEN '21–50 members'
                        WHEN totalMemberCount <= 100 THEN '51–100 members'
                        ELSE '100+ members'
                    END
            `, [auditRunID, auditRunID]);
            
            await tx.run(`
                INSERT INTO sap_sf_audit_UnusedRoles (ID, auditRunID_ID, roleId, roleName, recommendation)
                SELECT 
                    lower(hex(randomblob(16))),
                    ?,
                    r.roleId,
                    r.roleName,
                    'Review / Decommission'
                FROM sap_sf_audit_Roles r
                LEFT JOIN sap_sf_audit_RoleTargetPopulations tp 
                    ON tp.roleId = r.roleId AND tp.auditRunID_ID = ?
                WHERE r.auditRunID_ID = ? AND tp.ID IS NULL
            `, [auditRunID, auditRunID, auditRunID]);
            
            auditLogger.info('Analytics generated successfully');
            
        } catch (error) {
            auditLogger.error({ error: error.message }, 'Failed to generate analytics');
            throw error;
        }
    }
});