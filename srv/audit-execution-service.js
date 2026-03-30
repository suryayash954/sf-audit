const cds = require('@sap/cds');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const https = require('https');
const { retrieveJwt, getDestination } = require('@sap-cloud-sdk/connectivity');

module.exports = cds.service.impl(async function () {

    this.activeRuns = new Map();

    // ============================
    // BEFORE CREATE
    // ============================
    this.before('CREATE', 'AuditRuns', async (req) => {
        if (!req.data.name || !req.data.mode) {
            req.error(400, 'Missing required fields: name, mode');
        }

        // Initialize status
        req.data.status = 'CREATED';
        req.data.startTime = null;
        req.data.endTime = null;
    });

    // ============================
    // AFTER CREATE (NON-BLOCKING)
    // ============================
    this.after('CREATE', 'AuditRuns', async (data, req) => {

        // Detach execution from request
        setImmediate(() => {
            _runAuditAsync.call(this, data.ID, {
                mode: data.mode,
                extractGroups: data.extractGroups,
                extractRoles: data.extractRoles,
                sampleGroupSize: data.sampleGroupSize,
                sampleMemberSize: data.sampleMemberSize,
                sampleRoleSize: data.sampleRoleSize
            }).catch(err => {
                console.error("Background audit failed:", err);
            });
        });
    });

    // ============================
    // MAIN BACKGROUND JOB
    // ============================
    async function _runAuditAsync(auditRunID, config) {
        console.log("Starting Audit");
        const tx = cds.tx(); // independent transaction

        await tx.begin(); // 🔥 IMPORTANT

        try {

            // ------------------------
            // SET RUNNING
            // ------------------------
            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'RUNNING',
                        startTime: new Date()
                    })
                    .where({ ID: auditRunID })
            );

            console.log(`Audit ${auditRunID} started`);

            const activeRun = {
                id: auditRunID,
                cancelled: false,
                progress: 0,
                message: 'Starting audit...'
            };

            this.activeRuns.set(auditRunID, activeRun);

            // ------------------------
            // GET DESTINATION
            // ------------------------
            const sfConfig = await _getSFDestination();

            // ------------------------
            // PHASE 1 - GROUPS
            // ------------------------


            if (config.extractGroups) {
                staticGroups = await _fetchGroups(sfConfig, 'staticGroup eq true');
                dynamicGroups = await _fetchGroups(sfConfig, 'staticGroup eq false');

                await _saveGroups(tx, auditRunID, staticGroups, 'STATIC');
                await _saveGroups(tx, auditRunID, dynamicGroups, 'DYNAMIC');

                // 🔥 ADD THIS
                await _fetchAndSaveMembers(
                    tx,
                    auditRunID,
                    [...staticGroups, ...dynamicGroups],
                    sfConfig
                );
            }

            // ------------------------
            // PHASE 2 - ROLES
            // ------------------------
            let roles = [];

            if (config.extractRoles) {
                console.log("Auditing Roles");
                roles = await _fetchRoles(sfConfig);
                await _saveRoles(tx, auditRunID, roles);
            }

            // ------------------------
            // COMPLETE
            // ------------------------
            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'COMPLETED',
                        endTime: new Date()
                    })
                    .where({ ID: auditRunID })
            );
            await tx.commit();
            console.log(`Audit ${auditRunID} completed`);

        } catch (error) {

            console.error(`Audit ${auditRunID} failed:`, error);

            await tx.run(
                UPDATE('AuditRuns')
                    .set({
                        status: 'FAILED',
                        endTime: new Date(),
                        errorMessage: error.message
                    })
                    .where({ ID: auditRunID })
            );

        } finally {
            this.activeRuns.delete(auditRunID);
        }
    }

    // ============================
    // HELPERS
    // ============================

    async function _getSFDestination() {

        const destination = await getDestination({
            destinationName: 'successfactors'
        });

        if (!destination) {
            throw new Error('Destination not found');
        } else {
            console.log("destination config connected");
        }

        return {
            baseURL: destination.url,
            auth: {
                username: destination.username,
                password: destination.password
            },
            headers: {
                Accept: 'application/json'
            },
            httpsAgent: new https.Agent({ rejectUnauthorized: false })
        };
    }


    async function _fetchAndSaveMembers(tx, auditRunID, groups, sfConfig) {

        console.log(`Fetching members for ${groups.length} groups`);

        const userCache = new Map();
        const memberships = [];

        const groupBatchSize = 10;

        for (let i = 0; i < groups.length; i += groupBatchSize) {

            const batch = groups.slice(i, i + groupBatchSize);

            for (const group of batch) {

                try {

                    // ⚠️ IMPORTANT: This API must match your working SF endpoint
                    const url = `${sfConfig.baseURL}/User?$format=json`;

                    const res = await axios.get(url, {
                        auth: sfConfig.auth,
                        headers: sfConfig.headers,
                        httpsAgent: sfConfig.httpsAgent
                    });

                    const users = res.data?.d?.results || [];

                    for (const user of users) {

                        // ⚠️ You MUST adapt this condition based on your SF response
                        // For now assume ALL users belong (demo-safe)
                        memberships.push({
                            groupID: parseInt(group.groupID),
                            userID: user.userId || user.username
                        });

                        if (!userCache.has(user.username)) {
                            userCache.set(user.username, user);
                        }
                    }

                } catch (err) {
                    console.error(`Failed group ${group.groupID}`, err.message);
                }
            }

            console.log(`Processed ${Math.min(i + groupBatchSize, groups.length)} / ${groups.length}`);
        }

        // ============================
        // INSERT USERS
        // ============================
        if (userCache.size > 0) {

            const userEntries = [];

            for (const [username, user] of userCache.entries()) {

                userEntries.push({
                    ID: cds.utils.uuid(),
                    auditRunID_ID: auditRunID,
                    userName: username,
                    firstName: user.firstName || '',
                    lastName: user.lastName || '',
                    email: user.email || '',
                    department: user.department || '',
                    jobTitle: user.jobTitle || ''
                });
            }

            await tx.run(
                INSERT.into('sap_sf_audit_Users').entries(userEntries)
            );

            console.log(`Inserted ${userEntries.length} users`);
        }

        // ============================
        // INSERT MEMBERSHIPS
        // ============================
        if (memberships.length > 0) {

            const seen = new Set();
            const memberEntries = [];

            for (const m of memberships) {

                const key = `${m.groupID}-${m.userID}`;

                if (seen.has(key)) continue;
                seen.add(key);

                memberEntries.push({
                    ID: cds.utils.uuid(),
                    auditRunID_ID: auditRunID,
                    groupID: m.groupID,
                    userId: m.userID
                });
            }

            await tx.run(
                INSERT.into('sap_sf_audit_GroupMembers').entries(memberEntries)
            );

            console.log(`Inserted ${memberEntries.length} memberships`);
        }
    }
    async function _populateUsers(tx, auditRunID) {

        console.log("Populating Users table...");

        await tx.run(`
        INSERT INTO sap_sf_audit_Users (ID, auditRunID_ID, userName)
        SELECT 
            lower(hex(randomblob(16))),  -- SQLite UUID
            auditRunID_ID,
            userName
        FROM sap_sf_audit_GroupMembers
        WHERE auditRunID_ID = ?
        GROUP BY userName
    `, [auditRunID]);

    }

    async function _fetchGroups(sfConfig) {
        const url = `${sfConfig.baseURL}/DynamicGroup?$format=json`;

        console.log(" Started fetch Groups:", url);
        const res = await axios.get(url, {
            auth: sfConfig.auth,
            headers: sfConfig.headers,
            httpsAgent: sfConfig.httpsAgent
        });

        return res.data.d?.results || [];
    }

    async function _saveGroups(tx, auditRunID, groups) {
        console.log(" Started Saving Groups:", groups.length);
        if (!groups.length) return;

        const entries = groups.map(g => ({
            ID: uuidv4(),
            auditRunID_ID: auditRunID,
            groupID: String(g.groupID),
            groupName: g.groupName
        }));

        await tx.run(INSERT.into('Groups').entries(entries));
    }

    async function _fetchRoles(sfConfig) {
        const url = `${sfConfig.baseURL}/RBPRole?$format=json`;
        console.log(" Started fetchRoles:", url);

        const res = await axios.get(url, {
            auth: sfConfig.auth,
            headers: sfConfig.headers,
            httpsAgent: sfConfig.httpsAgent
        });

        return res.data.d?.results || [];
    }

    async function _saveRoles(tx, auditRunID, roles) {

        console.log(" Started Saving Roles:", roles.length);

        if (!roles.length) return;

        const entries = roles.map(r => ({
            ID: uuidv4(),
            auditRunID_ID: auditRunID,
            roleId: r.roleId,
            roleName: r.roleName
        }));

        await tx.run(INSERT.into('Roles').entries(entries));
    }

});