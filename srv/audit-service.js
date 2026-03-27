const cds = require('@sap/cds');
const SFApiClient = require('./sf-api-client');
const DataProcessor = require('./data-processor');

/** 
 * Audit Service Implementation
 */
module.exports = cds.service.impl(async function() {
    const { AuditHeaders } = this.entities;
    
    /**
     * Action: runAudit - Trigger a new audit extraction
     */
    this.on('runAudit', async (req) => {
        const { instance, mode, sampleSize, extractMembers, extractRoles } = req.data;
        
        // Create audit header record
        const auditID = cds.utils.uuid();
        
        const tx = cds.tx(req);
        await tx.run(INSERT.into(AuditHeaders).entries({
            AuditID: auditID,
            Instance: instance,
            GeneratedOn: new Date(),
            ExtractionMode: mode,
            Status: 'Running',
            CreatedBy: req.user.id
        }));
        
        // Start async audit process
        processAudit(auditID, {
            instance,
            mode,
            sampleSize,
            extractMembers,
            extractRoles,
            user: req.user.id
        }).catch(error => {
            console.error(`Audit ${auditID} failed:`, error);
        });
        
        return {
            AuditID: auditID,
            Status: 'Started',
            Message: 'Audit process initiated'
        };
    });
    
    /**
     * Action: getLatestAudit - Get most recent audit run
     */
    this.on('getLatestAudit', async (req) => {
        const tx = cds.tx(req);
        const latest = await tx.run(SELECT.from(AuditHeaders)
            .orderBy('GeneratedOn desc')
            .limit(1));
        
        if (latest.length === 0) {
            req.reject(404, 'No audit runs found');
        }
        
        return {
            AuditID: latest[0].AuditID,
            GeneratedOn: latest[0].GeneratedOn,
            Instance: latest[0].Instance
        };
    });
    
    /**
     * Action: getAuditProgress - Track running audit
     */
    this.on('getAuditProgress', async (req) => {
        const { AuditID } = req.data;
        // Will be implemented with audit log table in Phase 3
        return {
            AuditID,
            Status: 'Running',
            Progress: 50,
            Phase: 'Fetching users'
        };
    });
    
    /**
     * Action: downloadReport - Generate Excel report
     */
    this.on('downloadReport', async (req) => {
        const { AuditID } = req.data;
        
        // This will be implemented in Phase 4
        // For now, return placeholder
        return Buffer.from('Report generation pending');
    });
});

/**
 * Async audit processing function
 */
async function processAudit(auditID, config) {
    const db = await cds.connect.to('db');
    const startTime = Date.now();
    
    try {
        console.log(`\n🚀 Starting audit ${auditID}`);
        console.log(`📋 Config:`, config);
        
        // 1. Initialize SF API client
        const sfClient = new SFApiClient();
        await sfClient.init();
        
        // 2. Initialize data processor
        const processor = new DataProcessor(db, auditID);
        
        // ============================================
        // PHASE 2A: Fetch Permission Groups
        // ============================================
        console.log('\n📊 Phase 2A: Fetching permission groups...');
        
        let staticGroups = [], dynamicGroups = [];
        if (config.extractMembers) {
            // Fetch groups in parallel
            [staticGroups, dynamicGroups] = await Promise.all([
                sfClient.fetchGroups(true),
                sfClient.fetchGroups(false)
            ]);
            
            console.log(`✅ Found ${staticGroups.length} static groups`);
            console.log(`✅ Found ${dynamicGroups.length} dynamic groups`);
            
            // Apply sampling if in SAMPLE mode
            if (config.mode === 'SAMPLE' && config.sampleSize) {
                staticGroups = staticGroups.slice(0, config.sampleSize);
                dynamicGroups = dynamicGroups.slice(0, config.sampleSize);
                console.log(`  Sample mode: limited to ${config.sampleSize} groups each`);
            }
            
            // Insert groups
            if (staticGroups.length > 0) {
                await processor.insertGroups(staticGroups, 'STATIC');
            }
            if (dynamicGroups.length > 0) {
                await processor.insertGroups(dynamicGroups, 'DYNAMIC');
            }
        }
        
        // ============================================
        // PHASE 2B: Fetch Group Members and Users
        // ============================================
        console.log('\n👥 Phase 2B: Fetching group members and user details...');
        
        const allGroups = [...staticGroups, ...dynamicGroups];
        const userCache = new Map();
        const memberships = [];
        
        if (config.extractMembers && allGroups.length > 0) {
            // Process groups in batches to avoid overwhelming the API
            const groupBatchSize = 10; // Renamed from batchSize
            for (let i = 0; i < allGroups.length; i += groupBatchSize) {
                const batch = allGroups.slice(i, i + groupBatchSize);
                console.log(`  Processing groups ${i + 1}-${Math.min(i + groupBatchSize, allGroups.length)}/${allGroups.length}`);
                
                // Fetch members for each group in parallel
                const memberPromises = batch.map(group => 
                    sfClient.fetchGroupMembers(group.groupID)
                        .then(members => ({ group, members }))
                );
                
                const results = await Promise.all(memberPromises);
                
                // Collect memberships and queue user details
                for (const { group, members } of results) {
                    for (const member of members) {
                        if (member?.userName) {
                            memberships.push({
                                groupID: parseInt(group.groupID),
                                userID: member.userId || member.userName
                            });
                            
                            if (!userCache.has(member.userName)) {
                                userCache.set(member.userName, null); // Placeholder
                            }
                        }
                    }
                }
            }
            
            // Fetch user details in parallel
            if (userCache.size > 0) {
                console.log(`  Fetching details for ${userCache.size} unique users...`);
                const userNames = Array.from(userCache.keys());
                const userBatchSize = 20; // Renamed from batchSize
                
                for (let i = 0; i < userNames.length; i += userBatchSize) {
                    const batch = userNames.slice(i, i + userBatchSize);
                    const userPromises = batch.map(name => sfClient.fetchUserDetails(name));
                    const users = await Promise.all(userPromises);
                    
                    users.forEach((user, idx) => {
                        if (user) {
                            userCache.set(batch[idx], user);
                        }
                    });
                    
                    console.log(`    → Fetched ${Math.min(i + userBatchSize, userNames.length)}/${userNames.length} users`);
                }
            }
            
            // Insert users and memberships
            if (userCache.size > 0) {
                await processor.insertUsers(userCache);
            }
            if (memberships.length > 0) {
                await processor.insertMemberships(memberships);
            }
        }
        
        // ============================================
        // PHASE 2C: Fetch Roles and Target Population
        // ============================================
        console.log('\n🔐 Phase 2C: Fetching roles and target population...');
        
        if (config.extractRoles) {
            console.log('Fetching roles...');
            const roles = await sfClient.fetchRoles();
            console.log(`✅ Found ${roles.length} roles`);
            
            // Insert roles
            if (roles.length > 0) {
                const roleInserts = roles.map(role => ({
                    RoleID: role.roleId || role.id,
                    parent_AuditID_AuditID: auditID,
                    RoleName: role.roleName || role.name || '',
                    RoleDesc: role.roleDesc || role.description || '',
                    RoleType: role.roleType || '',
                    UserType: role.userType || '',
                    LastModifiedBy: role.lastModifiedBy || '',
                    LastModifiedDate: processor.parseSFDate(role.lastModifiedDate)
                }));
                
                // Use CAP's INSERT with entries
                await db.run(INSERT.into('sap.sf.audit.RBPRoles').entries(roleInserts));
                
                console.log(`  ✅ Inserted ${roles.length} roles`);
                
                // Fetch target population for each role (optional - can be heavy)
                if (false) { // Disabled by default for performance
                    for (const role of roles.slice(0, 10)) { // Limit for demo
                        const targetPop = await sfClient.fetchRoleTargetPopulation(role.roleId);
                        // Process target population...
                    }
                }
            }
        }
        
        // ============================================
        // PHASE 2D: Compute Analytics
        // ============================================
        console.log('\n📈 Phase 2D: Computing analytics...');
        await computeAnalytics(db, auditID);
        
        // ============================================
        // Complete Audit
        // ============================================
        const duration = ((Date.now() - startTime) / 1000).toFixed(1);
        const stats = processor.getStats();
        
        console.log('\n' + '='.repeat(50));
        console.log(`✅ AUDIT ${auditID} COMPLETED SUCCESSFULLY`);
        console.log('='.repeat(50));
        console.log(`⏱️  Duration: ${duration}s`);
        console.log(`📊 Statistics:`);
        console.log(`   • Groups: ${stats.groups}`);
        console.log(`   • Users: ${stats.users}`);
        console.log(`   • Memberships: ${stats.memberships}`);
        if (sfClient.requestCount) {
            console.log(`   • API Calls: ${sfClient.requestCount}`);
        }
        console.log('='.repeat(50));
        
        // Update audit status
        await db.run(UPDATE('sap.sf.audit.AuditHeaders')
            .set({ 
                Status: 'Completed',
                ReportName: `Audit_${new Date().toISOString().slice(0,10)}`
            })
            .where({ AuditID: auditID }));
        
    } catch (error) {
        console.error(`\n❌ Audit ${auditID} failed:`, error);
        
        await db.run(UPDATE('sap.sf.audit.AuditHeaders')
            .set({ Status: 'Failed' })
            .where({ AuditID: auditID }));
    }
}

/**
 * Compute analytics from raw data
 */
async function computeAnalytics(db, auditID) {
    try {
        // Multi-Group Users (users in 2+ groups)
        await db.run(`
            INSERT INTO sap_sf_audit_MultiGroupUsers (parent_AuditID_AuditID, UserID, GroupCount, GroupNames, RiskLevel)
            SELECT 
                ? as parent_AuditID_AuditID,
                gm.UserID,
                COUNT(DISTINCT gm.GroupID) as GroupCount,
                GROUP_CONCAT(pg.GroupName, ', ') as GroupNames,
                CASE 
                    WHEN COUNT(DISTINCT gm.GroupID) >= 4 THEN 'High'
                    WHEN COUNT(DISTINCT gm.GroupID) >= 3 THEN 'Medium'
                    ELSE 'Low'
                END as RiskLevel
            FROM sap_sf_audit_GroupMemberships gm
            JOIN sap_sf_audit_PermissionGroups pg ON pg.GroupID = gm.GroupID AND pg.parent_AuditID_AuditID = gm.parent_AuditID_AuditID
            WHERE gm.parent_AuditID_AuditID = ?
            GROUP BY gm.UserID
            HAVING COUNT(DISTINCT gm.GroupID) > 1
        `, [auditID, auditID]);
        
        // Group Size Distribution
        await db.run(`
            INSERT INTO sap_sf_audit_GroupSizeDistribution (parent_AuditID_AuditID, Bucket, GroupCount)
            SELECT 
                ? as parent_AuditID_AuditID,
                CASE 
                    WHEN TotalMemberCount <= 5 THEN 'SIZE_1_5'
                    WHEN TotalMemberCount <= 20 THEN 'SIZE_6_20'
                    WHEN TotalMemberCount <= 50 THEN 'SIZE_21_50'
                    WHEN TotalMemberCount <= 100 THEN 'SIZE_51_100'
                    ELSE 'SIZE_100_PLUS'
                END as Bucket,
                COUNT(*) as GroupCount
            FROM sap_sf_audit_PermissionGroups
            WHERE parent_AuditID_AuditID = ?
            GROUP BY 
                CASE 
                    WHEN TotalMemberCount <= 5 THEN 'SIZE_1_5'
                    WHEN TotalMemberCount <= 20 THEN 'SIZE_6_20'
                    WHEN TotalMemberCount <= 50 THEN 'SIZE_21_50'
                    WHEN TotalMemberCount <= 100 THEN 'SIZE_51_100'
                    ELSE 'SIZE_100_PLUS'
                END
        `, [auditID, auditID]);
        
        // User Group Count Distribution
        await db.run(`
            INSERT INTO sap_sf_audit_UserGroupCountDistribution (parent_AuditID_AuditID, Bucket, UserCount)
            SELECT 
                ? as parent_AuditID_AuditID,
                CASE 
                    WHEN group_count = 1 THEN 'ONE_GROUP'
                    WHEN group_count = 2 THEN 'TWO_GROUPS'
                    WHEN group_count <= 4 THEN 'THREE_TO_FOUR'
                    WHEN group_count <= 7 THEN 'FIVE_TO_SEVEN'
                    ELSE 'EIGHT_PLUS'
                END as Bucket,
                COUNT(*) as UserCount
            FROM (
                SELECT UserID, COUNT(DISTINCT GroupID) as group_count
                FROM sap_sf_audit_GroupMemberships
                WHERE parent_AuditID_AuditID = ?
                GROUP BY UserID
            )
            GROUP BY 
                CASE 
                    WHEN group_count = 1 THEN 'ONE_GROUP'
                    WHEN group_count = 2 THEN 'TWO_GROUPS'
                    WHEN group_count <= 4 THEN 'THREE_TO_FOUR'
                    WHEN group_count <= 7 THEN 'FIVE_TO_SEVEN'
                    ELSE 'EIGHT_PLUS'
                END
        `, [auditID, auditID]);
        
        // Unused Roles
        await db.run(`
            INSERT INTO sap_sf_audit_UnusedRoles (parent_AuditID_AuditID, RoleID, Recommendation)
            SELECT 
                ? as parent_AuditID_AuditID,
                r.RoleID,
                'Review / Decommission' as Recommendation
            FROM sap_sf_audit_RBPRoles r
            LEFT JOIN sap_sf_audit_RoleTargetPopulation tp 
                ON tp.RoleID = r.RoleID AND tp.parent_AuditID_AuditID = r.parent_AuditID_AuditID
            WHERE r.parent_AuditID_AuditID = ? AND tp.TargetID IS NULL
        `, [auditID, auditID]);
        
        console.log('  ✅ Analytics computed successfully');
        
    } catch (error) {
        console.error('Error in computeAnalytics:', error);
        throw error;
    }
}