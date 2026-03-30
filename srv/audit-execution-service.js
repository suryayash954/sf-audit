// srv/audit-execution-service.js
const cds = require('@sap/cds');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const https = require('https');
const { getDestination } = require('@sap-cloud-sdk/connectivity');
const pino = require('pino');

// Configure logger
const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    // transport: {
    //     target: 'pino-pretty',
    //     options: {
    //         colorize: true,
    //         translateTime: 'SYS:standard',
    //         ignore: 'pid,hostname'
    //     }
    // }
    base: null
});

// Batch configuration
const BATCH_CONFIG = {
    USER_BATCH_SIZE: 1000,
    GROUP_BATCH_SIZE: 20,
    MEMBER_BATCH_SIZE: 500,
    API_PARALLEL_LIMIT: 5,
    RETRY_ATTEMPTS: 3,
    RETRY_DELAY: 1000
};

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

            // Get SF destination
            const sfConfig = await _getSFDestination(syncLogger);

            // Fetch all users with pagination
            // const users = await _fetchAllUsers(sfConfig, syncLogger);
            const rawUsers = await _fetchAllUsers(sfConfig, syncLogger);
            const users = formatUsers(rawUsers);
            syncLogger.info({ totalFetched: users.length }, 'Users fetched from SuccessFactors');

            // Upsert users to central table
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
  const formatted = rawUsers.map(user => ({
    userId: user.userId,
    userName: user.userName || user.username || '',
    firstName: user.firstName || '',
    lastName: user.lastName || '',
    status: user.status === 'T' ? 'inactive' : 'active', // map SF status
    email: user.email || '',
    hireDate: user.hireDate ? new Date(user.hireDate) : null,
    terminationDate: user.companyExitDate ? new Date(user.companyExitDate) : null,
    lastModifiedDateTime: user.lastModifiedDateTime 
      ? new Date(parseInt(user.lastModifiedDateTime.replace(/\D/g,''))) 
      : new Date(),
    timeZone: user.timeZone || '',
    
    // Job & Organization
    jobTitle: user.jobTitle || user.title || '',
    jobCode: user.jobCode || '',
    department: user.department || '',
    division: user.division || '',
    location: user.location || '',
    company: user.company || '',           // may need mapping if not directly available
    businessUnit: user.businessSegment || '',

    // Custom fields
    custom01: user.custom01 || '',         // Cost Center
    custom02: user.custom02 || '',
    custom03: user.custom03 || '',

    // Sync metadata
    lastSyncAt: new Date(),
    isActive: user.status !== 'T'
  }));

  return formatted;
}


    // ============================
    // GET USERS ACTION (For UI)
    // ============================
    this.on('getUsers', async (req) => {
        const { top = 100, skip = 0, status = null, search = null } = req.data;

        try {
            const db = await cds.connect.to('db');

            let query = SELECT.from('Users');

            // Apply filters
            if (status) {
                query = query.where({ status: status });
            }

            if (search && search.trim()) {
                const searchTerm = `%${search.trim()}%`;
                query = query.where({
                    or: [
                        { userName: { like: searchTerm } },
                        { firstName: { like: searchTerm } },
                        { lastName: { like: searchTerm } },
                        { email: { like: searchTerm } },
                        { userId: { like: searchTerm } }
                    ]
                });
            }

            // Get total count
            const countQuery = query.clone();
            const countResult = await db.run(countQuery.SELECT('count(*) as total'));
            const total = countResult[0]?.total || 0;

            // Get paginated results
            const users = await db.run(
                query
                    .orderBy('userName')
                    .limit(top, skip)
                    .columns([
                        'userId',
                        'userName',
                        'firstName',
                        'lastName',
                        'status',
                        'email',
                        'hireDate',
                        'jobTitle',
                        'department',
                        'lastModifiedDateTime'
                    ])
            );

            return {
                success: true,
                users: users,
                total: total,
                top: top,
                skip: skip
            };

        } catch (error) {
            logger.error({ error: error.message, stack: error.stack }, 'Failed to get users');
            req.error(500, `Failed to get users: ${error.message}`);
            return {
                success: false,
                users: [],
                total: 0,
                top: top,
                skip: skip
            };
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
    // AFTER CREATE - Audit Runs
    // ============================
    this.after('CREATE', 'AuditRuns', async (data, req) => {
        setImmediate(() => {
            _runAuditAsync.call(this, data.ID, {
                mode: data.mode,
                extractGroups: data.extractGroups,
                extractRoles: data.extractRoles,
                sampleGroupSize: data.sampleGroupSize,
                sampleMemberSize: data.sampleMemberSize,
                sampleRoleSize: data.sampleRoleSize
            }).catch(err => {
                logger.error({ auditRunID: data.ID, error: err.message }, 'Background audit failed');
            });
        });
    });

    // ============================
    // MAIN BACKGROUND JOB
    // ============================
    async function _runAuditAsync(auditRunID, config) {
        const auditLogger = logger.child({ auditRunID, phase: 'AUDIT' });
        const tx = cds.tx();

        const startTime = Date.now();

        await tx.begin();

        try {
            auditLogger.info({ config }, 'Starting audit execution');

            // Update status to RUNNING
            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'RUNNING',
                        startTime: new Date()
                    })
                    .where({ ID: auditRunID })
            );

            const activeRun = {
                id: auditRunID,
                cancelled: false,
                progress: 0,
                message: 'Starting audit...'
            };

            this.activeRuns.set(auditRunID, activeRun);

            // Get SF destination
            const sfConfig = await _getSFDestination(auditLogger);

            // ============================
            // PHASE 1: ENSURE USERS ARE SYNCED
            // ============================
            activeRun.message = 'Checking user sync status...';

            const auditRun = await tx.run(
                SELECT.one.from('AuditRuns').where({ ID: auditRunID })
            );

            if (auditRun.userSyncRequired && !auditRun.userSyncCompleted) {
                auditLogger.info('User sync required - fetching from SF');

                // Fetch all users from SF
                const users = await _fetchAllUsers(sfConfig, auditLogger);

                // Upsert to central Users table
                await _upsertUsers(tx, users, auditLogger);

                // Update audit run with sync completion
                await tx.run(
                    UPDATE('AuditRuns')
                        .set({
                            userSyncCompleted: true,
                            userSyncAt: new Date()
                        })
                        .where({ ID: auditRunID })
                );

                activeRun.progress = 10;
                auditLogger.info({ userCount: users.length }, 'Users synced successfully');
            } else {
                auditLogger.info('Using existing user data from central table');
                activeRun.progress = 10;
            }

            // ============================
            // PHASE 2: GROUPS
            // ============================
            let groupStats = { static: 0, dynamic: 0 };

            if (config.extractGroups) {
                auditLogger.info('Starting group extraction');
                activeRun.message = 'Extracting groups...';

                const staticGroups = await _fetchGroupsWithPagination(sfConfig, true, auditLogger);
                const dynamicGroups = await _fetchGroupsWithPagination(sfConfig, false, auditLogger);

                groupStats = {
                    static: staticGroups.length,
                    dynamic: dynamicGroups.length,
                    total: staticGroups.length + dynamicGroups.length
                };

                await _saveGroups(tx, auditRunID, staticGroups, 'STATIC', auditLogger);
                await _saveGroups(tx, auditRunID, dynamicGroups, 'DYNAMIC', auditLogger);

                activeRun.progress = 30;

                // ============================
                // PHASE 3: GROUP MEMBERS (Links to central Users)
                // ============================
                auditLogger.info('Starting member extraction');
                activeRun.message = 'Extracting group members...';

                const allGroups = [...staticGroups, ...dynamicGroups];
                let groupsToProcess = allGroups;

                if (config.mode === 'SAMPLE' && config.sampleGroupSize > 0) {
                    groupsToProcess = allGroups.slice(0, config.sampleGroupSize);
                    auditLogger.info({ original: allGroups.length, sampled: groupsToProcess.length }, 'Applied group sampling');
                }

                const memberStats = await _fetchAndSaveMembers(
                    tx,
                    auditRunID,
                    groupsToProcess,
                    sfConfig,
                    config,
                    auditLogger
                );

                activeRun.progress = 60;
                await _updateGroupStatistics(tx, auditRunID, auditLogger);
            }

            // ============================
            // PHASE 4: ROLES
            // ============================
            if (config.extractRoles) {
                auditLogger.info('Starting role extraction');
                activeRun.message = 'Extracting roles...';

                const roles = await _fetchRolesWithPagination(sfConfig, auditLogger);

                let rolesToProcess = roles;
                if (config.mode === 'SAMPLE' && config.sampleRoleSize > 0) {
                    rolesToProcess = roles.slice(0, config.sampleRoleSize);
                    auditLogger.info({ original: roles.length, sampled: rolesToProcess.length }, 'Applied role sampling');
                }

                await _saveRoles(tx, auditRunID, rolesToProcess, auditLogger);
                activeRun.progress = 80;
            }

            // ============================
            // PHASE 5: ANALYTICS
            // ============================
            auditLogger.info('Generating analytics');
            activeRun.message = 'Generating analytics...';

            await _generateAnalytics(tx, auditRunID, auditLogger);

            // ============================
            // COMPLETE
            // ============================
            const executionTime = Date.now() - startTime;

            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'COMPLETED',
                        endTime: new Date(),
                        groupsProcessed: groupStats.total || 0,
                        membershipsProcessed: memberStats?.membershipsProcessed || 0,
                        rolesProcessed: roleCount || 0
                    })
                    .where({ ID: auditRunID })
            );

            await tx.commit();

            auditLogger.info({
                executionTime: `${executionTime}ms`,
                groups: groupStats,
                members: memberStats
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

        } finally {
            this.activeRuns.delete(auditRunID);
        }
    }

    // ============================
    // HELPER FUNCTIONS
    // ============================

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
                // console.log("response users",response.length);

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
                        userName: user.userName || user.username,
                        firstName: user.firstName || '',
                        lastName: user.lastName || '',
                        status: user.status || 'active',
                        email: user.email || '',
                        hireDate: user.hireDate ? new Date(user.hireDate) : null,
                        terminationDate: user.terminationDate ? new Date(user.terminationDate) : null,
                        lastModifiedDateTime: user.lastModifiedDateTime ? new Date(user.lastModifiedDateTime) : new Date(),
                        jobTitle: user.jobTitle || '',
                        department: user.department || '',
                        lastSyncAt: new Date(),
                        isActive: user.status === 'active'
                    };

                    if (existing) {
                        // Update existing user
                        await tx.run(
                            UPDATE('Users')
                                .set(userData)
                                .where({ userId: user.userId })
                        );
                        updated++;
                    } else {
                        // Insert new user
                        await tx.run(
                            INSERT.into('Users').entries(userData)
                        );
                        inserted++;
                    }

                } catch (error) {
                    auditLogger.error({ userId: user.userId, error: error.message }, 'Failed to upsert user');
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

    async function _fetchAndSaveMembers(tx, auditRunID, groups, sfConfig, config, auditLogger) {
        const memberships = new Set(); // Use Set for deduplication
        let processedGroups = 0;
        console.log("sfConfig",sfConfig);

        auditLogger.info({ groupCount: groups.length }, 'Starting member extraction');

        // Process groups in parallel with concurrency limit
        const groupChunks = [];
        for (let i = 0; i < groups.length; i += BATCH_CONFIG.API_PARALLEL_LIMIT) {
            groupChunks.push(groups.slice(i, i + BATCH_CONFIG.API_PARALLEL_LIMIT));
        }

        for (const chunk of groupChunks) {
            const chunkPromises = chunk.map(async (group) => {
                try {
                    const members = await _fetchGroupMembers(group.groupID, sfConfig, auditLogger);

                    for (const member of members) {
                        // Use composite key for deduplication
                        const membershipKey = `${group.groupID}-${member.userId}`;
                        memberships.add({
                            key: membershipKey,
                            groupID: group.groupID,
                            groupName: group.groupName,
                            userId: member.userId,
                            userName: member.userName || member.username
                        });
                    }

                    processedGroups++;

                    if (processedGroups % 10 === 0) {
                        auditLogger.info({ processed: processedGroups, total: groups.length }, 'Membership progress');
                    }

                } catch (error) {
                    auditLogger.error({ groupID: group.groupID, error: error.message }, 'Failed to fetch group members');
                }
            });

            await Promise.all(chunkPromises);
        }

        auditLogger.info({ memberships: memberships.size }, 'Membership data collected');

        // Save memberships in batches (linking to central Users table)
        const membershipsSaved = await _saveMemberships(tx, auditRunID, Array.from(memberships), auditLogger);

        return {
            membershipsProcessed: membershipsSaved
        };
    }

    async function _fetchGroupMembers(groupId, sfConfig, auditLogger) {
        // =========================================
        // ISSUE 
        // =========================================
    //     columns_to_select = "userId,userName,firstName,lastName"
    // api_url = f"{base_url}/getUsersByDynamicGroup?groupId={group_id}L&$format=json&$select={columns_to_select}"
        // const url = `${sfConfig.baseURL}/getUsersByDynamicGroup?groupId=${parseInt(groupId)}L&$format=json`;
        const url = `${sfConfig.baseURL}/getUsersByDynamicGroup?groupId=${groupId}L&$format=json&$select=userId,userName,firstName,lastName`;
        // console.log("url",url);
        try {
            const response = await axios.get(url, {
                auth: sfConfig.auth,
                headers: sfConfig.headers,
                httpsAgent: sfConfig.httpsAgent,
                timeout: sfConfig.timeout
            });
            return response.data?.d?.results || [];

        } catch (error) {
            // console.log("\nerror:\n",error);
            auditLogger.error({ groupId, error: error.message }, 'Failed to fetch group members');
            return [];
        }
    }

    async function _saveMemberships(tx, auditRunID, memberships, auditLogger) {
        if (!memberships.length) return 0;

        auditLogger.info({ count: memberships.length }, 'Saving memberships');

        // First, verify users exist in central table
        const userIds = [...new Set(memberships.map(m => m.userId))];
        const existingUsers = await tx.run(
            SELECT.from('Users').where({ userId: { in: userIds } })
        );

        const existingUserIds = new Set(existingUsers.map(u => u.userId));

        // Filter out memberships for non-existent users
        const validMemberships = memberships.filter(m => existingUserIds.has(m.userId));

        if (validMemberships.length < memberships.length) {
            auditLogger.warn({
                total: memberships.length,
                valid: validMemberships.length,
                invalid: memberships.length - validMemberships.length
            }, 'Some memberships skipped due to missing users');
        }

        const entries = [];

        for (const membership of validMemberships) {
            entries.push({
                ID: uuidv4(),
                auditRunID_ID: auditRunID,
                groupID: membership.groupID,
                groupName: membership.groupName,
                userId: membership.userId,
                userName: membership.userName
            });
        }

        // Delete existing memberships for this audit run to avoid duplicates
        await tx.run(
            DELETE.from('GroupMembers').where({ auditRunID_ID: auditRunID })
        );

        // Insert in batches
        for (let i = 0; i < entries.length; i += BATCH_CONFIG.MEMBER_BATCH_SIZE) {
            const batch = entries.slice(i, i + BATCH_CONFIG.MEMBER_BATCH_SIZE);
            await tx.run(INSERT.into('GroupMembers').entries(batch));
            auditLogger.debug({ batch: i / BATCH_CONFIG.MEMBER_BATCH_SIZE + 1, size: batch.length }, 'Memberships batch saved');
        }

        auditLogger.info({ count: entries.length }, 'Memberships saved');
        return entries.length;
    }

    // ... rest of helper functions (_getSFDestination, _fetchGroupsWithPagination, etc.)

    async function _getSFDestination(auditLogger) {
        try {
            const destination = await getDestination({
                destinationName: 'successfactors'
            });

            if (!destination) {
                throw new Error('Destination not found');
            }

            auditLogger.info({ url: destination.url }, 'Destination configured');

            return {
                baseURL: destination.url,
                auth: {
                    username: destination.username,
                    password: destination.password
                },
                headers: {
                    Accept: 'application/json',
                    'Content-Type': 'application/json',
                },
                httpsAgent: new https.Agent({ rejectUnauthorized: false }),
                timeout: 30000
            };

        } catch (error) {
            auditLogger.error({ error: error.message }, 'Failed to get SF destination');
            throw error;
        }
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
                    httpsAgent: sfConfig.httpsAgent,
                    timeout: sfConfig.timeout
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

        const batchSize = 500;
        for (let i = 0; i < entries.length; i += batchSize) {
            const batch = entries.slice(i, i + batchSize);
            await tx.run(INSERT.into('Groups').entries(batch));
        }

        auditLogger.info({ groupType, count: entries.length }, 'Groups saved');
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
                    httpsAgent: sfConfig.httpsAgent,
                    timeout: sfConfig.timeout
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

        const entries = roles.map(r => ({
            ID: uuidv4(),
            auditRunID_ID: auditRunID,
            roleId: r.roleId || r.id,
            roleName: r.roleName || r.name,
            roleDesc: r.roleDesc || r.description || '',
            roleType: r.roleType || 'standard',
            userType: r.userType || 'user',
            lastModifiedBy: r.lastModifiedBy || '',
            lastModifiedDate: r.lastModifiedDate ? new Date(r.lastModifiedDate) : new Date()
        }));

        const batchSize = 500;
        for (let i = 0; i < entries.length; i += batchSize) {
            const batch = entries.slice(i, i + batchSize);
            await tx.run(INSERT.into('Roles').entries(batch));
        }

        auditLogger.info({ count: entries.length }, 'Roles saved');
    }

    async function _updateGroupStatistics(tx, auditRunID, auditLogger) {
        await tx.run(`
            UPDATE sap_sf_audit_Groups 
            SET activeMembershipCount = (
                SELECT COUNT(*) 
                FROM sap_sf_audit_GroupMembers 
                WHERE GroupMembers.groupID = Groups.groupID 
                AND GroupMembers.auditRunID_ID = ?
            )
            WHERE auditRunID_ID = ?
        `, [auditRunID, auditRunID]);

        auditLogger.info('Group statistics updated');
    }

    async function _generateAnalytics(tx, auditRunID, auditLogger) {
        try {
            // User group count distribution using central Users table
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

            auditLogger.info('Analytics generated');

        } catch (error) {
            auditLogger.error({ error: error.message }, 'Failed to generate analytics');
        }
    }
});