const cds = require('@sap/cds');

module.exports = cds.service.impl(async function () {

    // ============================
    // REGISTER HANDLERS
    // ============================

    this.on('getRiskDashboard', getRiskDashboard);
    this.on('getGroupDetails', getGroupDetails);
    this.on('getUserAccessAnalysis', getUserAccessAnalysis);

    // ============================
    // RISK DASHBOARD
    // ============================
    async function getRiskDashboard(req) {
        // console.log("req",req);
        const { auditRunID } = req.data;
        const tx = cds.transaction(req);

        try {

            const auditRun = await tx.run(
                SELECT.one.from('AuditRuns').where({ ID: auditRunID })
            );

            if (!auditRun) {
                req.error(404, `Audit run ${auditRunID} not found`);
            }

            const [riskMetrics, totals] = await Promise.all([
                _getRiskMetrics(tx, auditRunID),
                _getTotals(tx, auditRunID)
            ]);

            return {
                auditInfo: {
                    name: auditRun.name,
                    instance: auditRun.instance,
                    status: auditRun.status,
                    mode: auditRun.mode,
                    generatedAt: auditRun.endTime || auditRun.startTime
                },
                riskMetrics,
                totals
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // GROUP DETAILS (OPTIMIZED)
    // ============================
    async function getGroupDetails(req) {

        const { auditRunID, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {

            const groups = await tx.run(`
                SELECT 
                    g.groupID,
                    g.groupName,
                    g.groupType,
                    g.totalMemberCount,
                    COUNT(DISTINCT gm.user_ID) as actualMemberCount,
                    COUNT(DISTINCT rtp.roleId) as roleTargetCount
                FROM sap_sf_audit_Groups g
                LEFT JOIN sap_sf_audit_GroupMembers gm 
                    ON gm.group_ID = g.ID AND gm.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_RoleTargetPopulations rtp 
                    ON rtp.groupId = g.groupID AND rtp.auditRunID_ID = ?
                WHERE g.auditRunID_ID = ?
                GROUP BY g.ID
                ORDER BY g.totalMemberCount DESC
                LIMIT ? OFFSET ?
            `, [auditRunID, auditRunID, auditRunID, top, skip]);

            return groups.map(g => ({
                groupID: g.groupID,
                groupName: g.groupName,
                groupType: g.groupType,
                totalMemberCount: Number(g.totalMemberCount || 0),
                actualMemberCount: Number(g.actualMemberCount || 0),
                roleTargetCount: Number(g.roleTargetCount || 0)
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // USER ACCESS ANALYSIS
    // ============================
    async function getUserAccessAnalysis(req) {

        const { auditRunID, top = 20 } = req.data;
        const tx = cds.transaction(req);

        try {

            const users = await tx.run(`
                SELECT 
                    u.userName,
                    COUNT(DISTINCT gm.groupID) as groupCount,
                    COUNT(DISTINCT urm.roleId) as roleCount
                FROM sap_sf_audit_Users u
                LEFT JOIN sap_sf_audit_GroupMembers gm 
                    ON gm.user_ID = u.ID AND gm.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_UserRoleMappings urm 
                    ON urm.user_ID = u.ID AND urm.auditRunID_ID = ?
                WHERE u.auditRunID_ID = ?
                GROUP BY u.userName
                ORDER BY groupCount DESC
                LIMIT ?
            `, [auditRunID, auditRunID, auditRunID, top]);

            return users.map(u => ({
                userName: u.userName,
                groupCount: Number(u.groupCount || 0),
                roleCount: Number(u.roleCount || 0)
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // HELPERS
    // ============================

    async function _getRiskMetrics(tx, auditRunID) {

        const [result] = await tx.run(`
            SELECT 
                COUNT(CASE WHEN riskLevel = 'High' THEN 1 END) as highRisk,
                COUNT(CASE WHEN riskLevel = 'Medium' THEN 1 END) as mediumRisk,
                COUNT(CASE WHEN riskLevel = 'Low' THEN 1 END) as lowRisk
            FROM sap_sf_audit_MultiGroupUsers
            WHERE auditRunID_ID = ?
        `, [auditRunID]);

        return {
            highRiskUsers: Number(result.highRisk || 0),
            mediumRiskUsers: Number(result.mediumRisk || 0),
            lowRiskUsers: Number(result.lowRisk || 0)
        };
    }

    async function _getTotals(tx, auditRunID) {
    const [groups, users, roles] = await Promise.all([
        tx.run(SELECT.from('Groups').where({ auditRunID_ID: auditRunID }).columns('count(*) as count')),
        tx.run(SELECT.from('Users').where({ auditRunID_ID: auditRunID }).columns('count(*) as count')),
        tx.run(SELECT.from('Roles').where({ auditRunID_ID: auditRunID }).columns('count(*) as count'))
    ]);

    return {
        // Extract the 'count' alias from the first object in each result array
        totalGroups: Number(groups[0]?.count || 0),
        totalUsers: Number(users[0]?.count || 0),
        totalRoles: Number(roles[0]?.count || 0)
    };
}

});