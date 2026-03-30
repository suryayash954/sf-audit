const cds = require('@sap/cds');

module.exports = cds.service.impl(async function () {

    // ============================
    // REGISTER HANDLERS
    // ============================

    this.on('getRiskDashboard', getRiskDashboard);
    this.on('getExecutiveSummary', getExecutiveSummary);
    this.on('getGroupDetails', getGroupDetails);
    this.on('getGroupSizeAnalysis', getGroupSizeAnalysis);
    this.on('getUserDetails', getUserDetails);
    this.on('getUserAccessAnalysis', getUserAccessAnalysis);
    this.on('getHighRiskUsers', getHighRiskUsers);
    this.on('getInactiveUsersWithAccess', getInactiveUsersWithAccess);
    this.on('getRoleDetails', getRoleDetails);
    this.on('getRoleCoverageAnalysis', getRoleCoverageAnalysis);
    this.on('getGroupRoleMatrix', getGroupRoleMatrix);
    this.on('getUserRoleMatrix', getUserRoleMatrix);
    this.on('getUsersByRole', getUsersByRole);
    this.on('getRolesByUser', getRolesByUser);
    this.on('exportAuditData', exportAuditData);
    this.on('exportReport', exportReport);

    // ============================
    // RISK DASHBOARD
    // ============================
    async function getRiskDashboard(req) {
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

            const [riskMetrics, totals] = await Promise.all([
                _getRiskMetrics(tx, auditRunID),
                _getTotals(tx, auditRunID)
            ]);

            return {
                auditInfo: {
                    name: auditRun.name,
                    instance: auditRun.instance || 'QAS',
                    generatedAt: auditRun.endTime || auditRun.startTime,
                    status: auditRun.status,
                    mode: auditRun.mode
                },
                riskMetrics,
                totals
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // EXECUTIVE SUMMARY
    // ============================
    async function getExecutiveSummary(req) {
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

            // Get group counts
            const groups = await tx.run(
                SELECT.from('Groups').where({ auditRunID_ID: auditRunID })
                    .columns('groupType', 'count(*) as cnt')
                    .groupBy('groupType')
            );

            const staticCount = groups.find(g => g.groupType === 'STATIC')?.cnt || 0;
            const dynamicCount = groups.find(g => g.groupType === 'DYNAMIC')?.cnt || 0;

            // Get risk metrics
            const riskMetrics = await _getRiskMetrics(tx, auditRunID);
            
            // Get unused roles
            const unusedRoles = await tx.run(
                SELECT.from('UnusedRoles').where({ auditRunID_ID: auditRunID }).columns('count(*) as cnt')
            );

            // Get inactive users
            const inactiveUsers = await tx.run(
                SELECT.from('InactiveUserAccess').where({ auditRunID_ID: auditRunID }).columns('count(*) as cnt')
            );

            const recommendations = [];

            if (riskMetrics.highRiskUsers > 0) {
                recommendations.push({
                    category: 'Security Risk',
                    priority: 'High',
                    description: `Found ${riskMetrics.highRiskUsers} users with high-risk access levels`,
                    affectedCount: riskMetrics.highRiskUsers
                });
            }

            if (unusedRoles[0]?.cnt > 0) {
                recommendations.push({
                    category: 'Role Cleanup',
                    priority: 'Medium',
                    description: `${unusedRoles[0].cnt} roles have no target population assigned`,
                    affectedCount: unusedRoles[0].cnt
                });
            }

            if (inactiveUsers[0]?.cnt > 0) {
                recommendations.push({
                    category: 'Access Cleanup',
                    priority: 'High',
                    description: `${inactiveUsers[0].cnt} inactive users still have active access`,
                    affectedCount: inactiveUsers[0].cnt
                });
            }

            return {
                auditInfo: {
                    name: auditRun.name,
                    instance: auditRun.instance || 'QAS',
                    generatedAt: auditRun.endTime || auditRun.startTime,
                    mode: auditRun.mode,
                    status: auditRun.status
                },
                metrics: {
                    totalGroups: staticCount + dynamicCount,
                    staticGroups: staticCount,
                    dynamicGroups: dynamicCount,
                    totalUsers: auditRun.membershipsProcessed || 0,
                    highAccessUsers: riskMetrics.highRiskUsers,
                    largeGroups: 0,
                    totalRoles: auditRun.rolesProcessed || 0,
                    unusedRoles: unusedRoles[0]?.cnt || 0
                },
                riskIndicators: {
                    highAccessUsersCount: riskMetrics.highRiskUsers,
                    largeGroupsCount: 0,
                    inactiveUsersWithAccess: inactiveUsers[0]?.cnt || 0,
                    unusedRolesCount: unusedRoles[0]?.cnt || 0
                },
                recommendations
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // GROUP DETAILS
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
                    COUNT(DISTINCT gm.userId) as actualMemberCount,
                    COUNT(DISTINCT rtp.roleId) as roleTargetCount,
                    g.createdBy,
                    g.lastModifiedDate
                FROM sap_sf_audit_Groups g
                LEFT JOIN sap_sf_audit_GroupMembers gm 
                    ON gm.groupID = g.groupID AND gm.auditRunID_ID = ?
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
                activeMemberCount: Number(g.totalMemberCount || 0),
                actualMemberCount: Number(g.actualMemberCount || 0),
                roleTargetCount: Number(g.roleTargetCount || 0),
                createdBy: g.createdBy || '',
                lastModifiedDate: g.lastModifiedDate
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // GROUP SIZE ANALYSIS
    // ============================
    async function getGroupSizeAnalysis(req) {
        const { auditRunID } = req.data;
        const tx = cds.transaction(req);

        try {
            const groups = await tx.run(`
                SELECT 
                    g.groupName,
                    g.totalMemberCount,
                    g.groupType
                FROM sap_sf_audit_Groups g
                WHERE g.auditRunID_ID = ?
            `, [auditRunID]);

            const distribution = [
                { sizeRange: '1–5 members', groupCount: 0, percentage: 0 },
                { sizeRange: '6–20 members', groupCount: 0, percentage: 0 },
                { sizeRange: '21–50 members', groupCount: 0, percentage: 0 },
                { sizeRange: '51–100 members', groupCount: 0, percentage: 0 },
                { sizeRange: '100+ members', groupCount: 0, percentage: 0 }
            ];

            let totalMembers = 0;
            let groupSizes = [];

            for (const group of groups) {
                const count = group.totalMemberCount || 0;
                totalMembers += count;
                groupSizes.push(count);

                if (count <= 5) distribution[0].groupCount++;
                else if (count <= 20) distribution[1].groupCount++;
                else if (count <= 50) distribution[2].groupCount++;
                else if (count <= 100) distribution[3].groupCount++;
                else distribution[4].groupCount++;
            }

            const totalGroups = groups.length;
            for (let d of distribution) {
                d.percentage = totalGroups > 0 ? (d.groupCount / totalGroups) * 100 : 0;
            }

            groupSizes.sort((a, b) => a - b);
            const avg = totalGroups > 0 ? totalMembers / totalGroups : 0;
            const median = groupSizes[Math.floor(groupSizes.length / 2)] || 0;
            const max = groupSizes[groupSizes.length - 1] || 0;
            const min = groupSizes[0] || 0;

            const staticGroups = groups.filter(g => g.groupType === 'STATIC');
            const dynamicGroups = groups.filter(g => g.groupType === 'DYNAMIC');

            const staticAvg = staticGroups.length > 0 
                ? staticGroups.reduce((sum, g) => sum + (g.totalMemberCount || 0), 0) / staticGroups.length : 0;
            const dynamicAvg = dynamicGroups.length > 0 
                ? dynamicGroups.reduce((sum, g) => sum + (g.totalMemberCount || 0), 0) / dynamicGroups.length : 0;

            const oversizedGroups = groups
                .filter(g => (g.totalMemberCount || 0) >= 51)
                .map(g => ({
                    groupName: g.groupName,
                    totalMembers: g.totalMemberCount,
                    activeMembers: g.totalMemberCount
                }))
                .slice(0, 20);

            return {
                distribution,
                summary: {
                    totalGroups,
                    averageGroupSize: Math.round(avg * 100) / 100,
                    medianGroupSize: median,
                    maxGroupSize: max,
                    minGroupSize: min
                },
                oversizedGroups,
                staticVsDynamic: [
                    {
                        groupType: 'Static',
                        count: staticGroups.length,
                        averageSize: Math.round(staticAvg * 100) / 100,
                        totalMembers: staticGroups.reduce((sum, g) => sum + (g.totalMemberCount || 0), 0)
                    },
                    {
                        groupType: 'Dynamic',
                        count: dynamicGroups.length,
                        averageSize: Math.round(dynamicAvg * 100) / 100,
                        totalMembers: dynamicGroups.reduce((sum, g) => sum + (g.totalMemberCount || 0), 0)
                    }
                ]
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // USER DETAILS
    // ============================
    async function getUserDetails(req) {
        const { auditRunID, status = null, minGroupCount = 0, department = null, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            let query = `
                SELECT 
                    u.userId,
                    u.userName,
                    u.firstName,
                    u.lastName,
                    u.status,
                    u.email,
                    u.department,
                    u.division,
                    u.location,
                    u.jobTitle,
                    COUNT(DISTINCT gm.groupID) as groupCount,
                    GROUP_CONCAT(DISTINCT gm.groupName, ', ') as groupNames,
                    mgu.riskLevel
                FROM sap_sf_audit_Users u
                LEFT JOIN sap_sf_audit_GroupMembers gm 
                    ON gm.userId = u.userId AND gm.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_MultiGroupUsers mgu 
                    ON mgu.userId = u.userId AND mgu.auditRunID_ID = ?
                WHERE u.auditRunID_ID = ?
            `;
            const params = [auditRunID, auditRunID, auditRunID];

            if (status) {
                query += ` AND u.status = ?`;
                params.push(status);
            }
            if (department) {
                query += ` AND u.department = ?`;
                params.push(department);
            }

            query += ` GROUP BY u.userId HAVING groupCount >= ? ORDER BY groupCount DESC LIMIT ? OFFSET ?`;
            params.push(minGroupCount, top, skip);

            const users = await tx.run(query, params);

            return users.map(u => ({
                userId: u.userId,
                userName: u.userName,
                firstName: u.firstName,
                lastName: u.lastName,
                status: u.status,
                email: u.email,
                department: u.department,
                division: u.division,
                location: u.location,
                jobTitle: u.jobTitle,
                groupCount: Number(u.groupCount || 0),
                roleCount: 0,
                groupNames: u.groupNames || '',
                riskLevel: u.riskLevel || 'Low'
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
                    u.firstName,
                    u.lastName,
                    u.department,
                    u.jobTitle,
                    COUNT(DISTINCT gm.groupID) as groupCount,
                    COUNT(DISTINCT urm.roleId) as roleCount,
                    mgu.riskLevel
                FROM sap_sf_audit_Users u
                LEFT JOIN sap_sf_audit_GroupMembers gm 
                    ON gm.userId = u.userId AND gm.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_UserRoleMappings urm 
                    ON urm.userId = u.userId AND urm.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_MultiGroupUsers mgu 
                    ON mgu.userId = u.userId AND mgu.auditRunID_ID = ?
                WHERE u.auditRunID_ID = ?
                GROUP BY u.userId
                ORDER BY groupCount DESC
                LIMIT ?
            `, [auditRunID, auditRunID, auditRunID, auditRunID, top]);

            const accessDistribution = [
                { groupCountRange: '1 group', userCount: 0, percentage: 0 },
                { groupCountRange: '2 groups', userCount: 0, percentage: 0 },
                { groupCountRange: '3–4 groups', userCount: 0, percentage: 0 },
                { groupCountRange: '5–7 groups', userCount: 0, percentage: 0 },
                { groupCountRange: '8+ groups', userCount: 0, percentage: 0 }
            ];

            const totalUsers = users.length;
            for (const user of users) {
                const count = user.groupCount;
                if (count === 1) accessDistribution[0].userCount++;
                else if (count === 2) accessDistribution[1].userCount++;
                else if (count <= 4) accessDistribution[2].userCount++;
                else if (count <= 7) accessDistribution[3].userCount++;
                else accessDistribution[4].userCount++;
            }

            for (let d of accessDistribution) {
                d.percentage = totalUsers > 0 ? (d.userCount / totalUsers) * 100 : 0;
            }

            const usersWithExcessiveAccess = users
                .filter(u => u.groupCount >= 5)
                .map(u => ({
                    userName: u.userName,
                    groupCount: u.groupCount,
                    roleCount: u.roleCount,
                    groups: ''
                }));

            return {
                topUsersByGroupCount: users.map(u => ({
                    userName: u.userName,
                    firstName: u.firstName,
                    lastName: u.lastName,
                    department: u.department,
                    jobTitle: u.jobTitle,
                    groupCount: u.groupCount,
                    roleCount: u.roleCount,
                    riskLevel: u.riskLevel || 'Low'
                })),
                accessDistribution,
                usersWithExcessiveAccess
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // HIGH RISK USERS
    // ============================
    async function getHighRiskUsers(req) {
        const { auditRunID, minRiskScore = 5, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            const users = await tx.run(`
                SELECT 
                    mgu.userId,
                    mgu.userName,
                    mgu.groupCount,
                    mgu.groupNames,
                    mgu.riskLevel,
                    mgu.riskScore,
                    mgu.riskCategory,
                    mgu.recommendedAction,
                    u.firstName,
                    u.lastName,
                    u.department,
                    u.jobTitle,
                    u.status
                FROM sap_sf_audit_MultiGroupUsers mgu
                LEFT JOIN sap_sf_audit_Users u 
                    ON u.userId = mgu.userId AND u.auditRunID_ID = ?
                WHERE mgu.auditRunID_ID = ?
                    AND mgu.riskScore >= ?
                ORDER BY mgu.riskScore DESC
                LIMIT ? OFFSET ?
            `, [auditRunID, auditRunID, minRiskScore, top, skip]);

            return users.map(u => ({
                userName: u.userName,
                firstName: u.firstName || '',
                lastName: u.lastName || '',
                department: u.department || '',
                jobTitle: u.jobTitle || '',
                userStatus: u.status || 'active',
                groupCount: Number(u.groupCount || 0),
                groupNames: u.groupNames || '',
                riskLevel: u.riskLevel || 'Medium',
                riskScore: Number(u.riskScore || 0),
                riskCategory: u.riskCategory || 'Medium Risk',
                recommendedAction: u.recommendedAction || 'Review access',
                isInactive: u.status === 'inactive'
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // INACTIVE USERS WITH ACCESS
    // ============================
    async function getInactiveUsersWithAccess(req) {
        const { auditRunID, riskCategory = null, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            let query = `
                SELECT 
                    iua.userId,
                    iua.userName,
                    iua.firstName,
                    iua.lastName,
                    iua.status,
                    iua.hireDate,
                    iua.permissionGroups,
                    iua.groupCount,
                    iua.riskScore,
                    iua.riskCategory,
                    iua.recommendedAction,
                    u.department
                FROM sap_sf_audit_InactiveUserAccess iua
                LEFT JOIN sap_sf_audit_Users u 
                    ON u.userId = iua.userId AND u.auditRunID_ID = ?
                WHERE iua.auditRunID_ID = ?
            `;
            const params = [auditRunID, auditRunID];

            if (riskCategory) {
                query += ` AND iua.riskCategory = ?`;
                params.push(riskCategory);
            }

            query += ` ORDER BY iua.riskScore DESC LIMIT ? OFFSET ?`;
            params.push(top, skip);

            const users = await tx.run(query, params);

            return users.map(u => ({
                userName: u.userName,
                firstName: u.firstName || '',
                lastName: u.lastName || '',
                department: u.department || '',
                status: u.status || 'inactive',
                hireDate: u.hireDate,
                permissionGroups: u.permissionGroups || '',
                groupCount: Number(u.groupCount || 0),
                riskScore: Number(u.riskScore || 0),
                riskCategory: u.riskCategory || 'Medium',
                recommendedAction: u.recommendedAction || 'Remove access'
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // ROLE DETAILS
    // ============================
    async function getRoleDetails(req) {
        const { auditRunID, usageStatus = null, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            const roles = await tx.run(`
                SELECT 
                    r.roleId,
                    r.roleName,
                    r.roleDesc,
                    r.roleType,
                    r.userType,
                    r.lastModifiedBy,
                    r.lastModifiedDate,
                    COUNT(DISTINCT rtp.groupId) as targetPopulationCount,
                    COUNT(DISTINCT urm.userId) as assignedUserCount,
                    CASE 
                        WHEN COUNT(DISTINCT rtp.groupId) = 0 THEN 'Unused'
                        ELSE 'Active'
                    END as usageStatus
                FROM sap_sf_audit_Roles r
                LEFT JOIN sap_sf_audit_RoleTargetPopulations rtp 
                    ON rtp.roleId = r.roleId AND rtp.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_UserRoleMappings urm 
                    ON urm.roleId = r.roleId AND urm.auditRunID_ID = ?
                WHERE r.auditRunID_ID = ?
                GROUP BY r.ID
                ${usageStatus === 'Unused' ? 'HAVING targetPopulationCount = 0' : ''}
                ${usageStatus === 'Active' ? 'HAVING targetPopulationCount > 0' : ''}
                ORDER BY assignedUserCount DESC
                LIMIT ? OFFSET ?
            `, [auditRunID, auditRunID, auditRunID, top, skip]);

            return roles.map(r => ({
                roleId: r.roleId,
                roleName: r.roleName,
                roleDesc: r.roleDesc,
                roleType: r.roleType,
                userType: r.userType,
                lastModifiedBy: r.lastModifiedBy,
                lastModifiedDate: r.lastModifiedDate,
                targetPopulationCount: Number(r.targetPopulationCount || 0),
                assignedUserCount: Number(r.assignedUserCount || 0),
                usageStatus: r.usageStatus
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // ROLE COVERAGE ANALYSIS
    // ============================
    async function getRoleCoverageAnalysis(req) {
        const { auditRunID } = req.data;
        const tx = cds.transaction(req);

        try {
            const roles = await tx.run(`
                SELECT 
                    r.roleType,
                    r.roleId,
                    r.roleName,
                    COUNT(DISTINCT rtp.groupId) as targetCount,
                    COUNT(DISTINCT urm.userId) as assignmentCount
                FROM sap_sf_audit_Roles r
                LEFT JOIN sap_sf_audit_RoleTargetPopulations rtp 
                    ON rtp.roleId = r.roleId AND rtp.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_UserRoleMappings urm 
                    ON urm.roleId = r.roleId AND urm.auditRunID_ID = ?
                WHERE r.auditRunID_ID = ?
                GROUP BY r.ID
            `, [auditRunID, auditRunID, auditRunID]);

            const totalRoles = roles.length;
            const rolesWithTargets = roles.filter(r => r.targetCount > 0).length;
            const unusedRoles = roles.filter(r => r.targetCount === 0).length;
            const rolesWithMultipleTargets = roles.filter(r => r.targetCount > 1).length;

            const rolesByType = {};
            for (const role of roles) {
                const type = role.roleType || 'unknown';
                if (!rolesByType[type]) {
                    rolesByType[type] = { count: 0, usedCount: 0, unusedCount: 0, totalAssignments: 0 };
                }
                rolesByType[type].count++;
                if (role.targetCount > 0) rolesByType[type].usedCount++;
                else rolesByType[type].unusedCount++;
                rolesByType[type].totalAssignments += role.assignmentCount;
            }

            const rolesByTypeArray = Object.entries(rolesByType).map(([roleType, data]) => ({
                roleType,
                count: data.count,
                usedCount: data.usedCount,
                unusedCount: data.unusedCount,
                totalAssignments: data.totalAssignments
            }));

            const topRoles = [...roles]
                .sort((a, b) => b.assignmentCount - a.assignmentCount)
                .slice(0, 10)
                .map(r => ({
                    roleName: r.roleName,
                    assignedUserCount: r.assignmentCount,
                    targetGroupCount: r.targetCount
                }));

            const unusedRolesList = roles
                .filter(r => r.targetCount === 0)
                .map(r => ({
                    roleId: r.roleId,
                    roleName: r.roleName,
                    lastModifiedDate: null
                }));

            return {
                summary: {
                    totalRoles,
                    rolesWithTargets,
                    unusedRoles,
                    rolesWithMultipleTargets,
                    averageTargetsPerRole: totalRoles > 0 ? Math.round((rolesWithTargets / totalRoles) * 100) / 100 : 0,
                    averageAssignmentsPerRole: totalRoles > 0 ? Math.round((roles.reduce((sum, r) => sum + r.assignmentCount, 0) / totalRoles) * 100) / 100 : 0
                },
                rolesByType: rolesByTypeArray,
                topRolesByAssignments: topRoles,
                unusedRolesList
            };

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // GROUP ROLE MATRIX
    // ============================
    async function getGroupRoleMatrix(req) {
        const { auditRunID, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            const matrix = await tx.run(`
                SELECT 
                    rtp.groupId,
                    rtp.groupName,
                    rtp.roleId,
                    rtp.roleName,
                    rtp.ruleId,
                    rtp.ruleMyFilter,
                    rtp.source
                FROM sap_sf_audit_RoleTargetPopulations rtp
                WHERE rtp.auditRunID_ID = ?
                ORDER BY rtp.groupName, rtp.roleName
                LIMIT ? OFFSET ?
            `, [auditRunID, top, skip]);

            return matrix.map(m => ({
                groupId: m.groupId,
                groupName: m.groupName,
                roleId: m.roleId,
                roleName: m.roleName,
                ruleId: m.ruleId,
                ruleMyFilter: m.ruleMyFilter,
                source: m.source
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // USER ROLE MATRIX
    // ============================
    async function getUserRoleMatrix(req) {
        const { auditRunID, userName = null, roleName = null, department = null, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            let query = `
                SELECT 
                    urm.userName,
                    urm.roleName,
                    urm.assignedViaGroup,
                    u.firstName,
                    u.lastName,
                    u.status as userStatus,
                    u.department,
                    u.jobTitle,
                    r.roleType
                FROM sap_sf_audit_UserRoleMappings urm
                LEFT JOIN sap_sf_audit_Users u 
                    ON u.userId = urm.userId AND u.auditRunID_ID = ?
                LEFT JOIN sap_sf_audit_Roles r 
                    ON r.roleId = urm.roleId AND r.auditRunID_ID = ?
                WHERE urm.auditRunID_ID = ?
            `;
            const params = [auditRunID, auditRunID, auditRunID];

            if (userName) {
                query += ` AND urm.userName LIKE ?`;
                params.push(`%${userName}%`);
            }
            if (roleName) {
                query += ` AND urm.roleName LIKE ?`;
                params.push(`%${roleName}%`);
            }
            if (department) {
                query += ` AND u.department LIKE ?`;
                params.push(`%${department}%`);
            }

            query += ` ORDER BY urm.userName LIMIT ? OFFSET ?`;
            params.push(top, skip);

            const mappings = await tx.run(query, params);

            return mappings.map(m => ({
                userName: m.userName,
                firstName: m.firstName || '',
                lastName: m.lastName || '',
                userStatus: m.userStatus || 'active',
                department: m.department || '',
                jobTitle: m.jobTitle || '',
                roleName: m.roleName,
                roleType: m.roleType,
                assignedViaGroup: m.assignedViaGroup
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // USERS BY ROLE
    // ============================
    async function getUsersByRole(req) {
        const { auditRunID, roleId, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            const users = await tx.run(`
                SELECT 
                    urm.userName,
                    urm.assignedViaGroup,
                    u.firstName,
                    u.lastName,
                    u.department,
                    u.jobTitle,
                    u.status as userStatus
                FROM sap_sf_audit_UserRoleMappings urm
                LEFT JOIN sap_sf_audit_Users u 
                    ON u.userId = urm.userId AND u.auditRunID_ID = ?
                WHERE urm.auditRunID_ID = ? AND urm.roleId = ?
                ORDER BY urm.userName
                LIMIT ? OFFSET ?
            `, [auditRunID, auditRunID, roleId, top, skip]);

            return users.map(u => ({
                userName: u.userName,
                firstName: u.firstName || '',
                lastName: u.lastName || '',
                department: u.department || '',
                jobTitle: u.jobTitle || '',
                userStatus: u.userStatus || 'active',
                assignedViaGroup: u.assignedViaGroup
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // ROLES BY USER
    // ============================
    async function getRolesByUser(req) {
        const { auditRunID, userName, top = 100, skip = 0 } = req.data;
        const tx = cds.transaction(req);

        try {
            const roles = await tx.run(`
                SELECT 
                    urm.roleId,
                    urm.roleName,
                    urm.assignedViaGroup,
                    r.roleType
                FROM sap_sf_audit_UserRoleMappings urm
                LEFT JOIN sap_sf_audit_Roles r 
                    ON r.roleId = urm.roleId AND r.auditRunID_ID = ?
                WHERE urm.auditRunID_ID = ? AND urm.userName = ?
                ORDER BY urm.roleName
                LIMIT ? OFFSET ?
            `, [auditRunID, auditRunID, userName, top, skip]);

            return roles.map(r => ({
                roleId: r.roleId,
                roleName: r.roleName,
                roleType: r.roleType,
                assignedViaGroup: r.assignedViaGroup
            }));

        } catch (error) {
            req.error(500, error.message);
        }
    }

    // ============================
    // EXPORT FUNCTIONS (Placeholders)
    // ============================
    async function exportAuditData(req) {
        const { auditRunID, format = 'JSON' } = req.data;
        return Buffer.from(JSON.stringify({ message: 'Export not implemented yet', auditRunID, format }));
    }

    async function exportReport(req) {
        const { auditRunID, reportType, format = 'CSV' } = req.data;
        return Buffer.from(JSON.stringify({ message: 'Export not implemented yet', auditRunID, reportType, format }));
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
            highRiskUsers: Number(result?.highRisk || 0),
            mediumRiskUsers: Number(result?.mediumRisk || 0),
            lowRiskUsers: Number(result?.lowRisk || 0),
            inactiveUsersWithAccess: 0,
            unusedRolesCount: 0,
            largeGroupsCount: 0
        };
    }

    async function _getTotals(tx, auditRunID) {
        const [groups, users, roles] = await Promise.all([
            tx.run(SELECT.from('Groups').where({ auditRunID_ID: auditRunID }).columns('count(*) as count')),
            tx.run(SELECT.from('Users').where({ auditRunID_ID: auditRunID }).columns('count(*) as count')),
            tx.run(SELECT.from('Roles').where({ auditRunID_ID: auditRunID }).columns('count(*) as count'))
        ]);

        return {
            totalGroups: Number(groups[0]?.count || 0),
            totalUsers: Number(users[0]?.count || 0),
            totalRoles: Number(roles[0]?.count || 0),
            totalGroupMembers: 0,
            totalRoleAssignments: 0
        };
    }
});