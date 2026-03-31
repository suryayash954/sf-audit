using { sap.sf.audit as db } from '../db/schema';

/**
 * Audit Reporting Service
 */
service AuditReportingService @(path: '/SuccessFactorRBPReport') {
    
    // Core Entities - Read-only for reporting
    entity Groups as projection on db.Groups;
    entity Users as projection on db.Users;
    entity Roles as projection on db.Roles;
    entity GroupMembers as projection on db.GroupMembers;
    entity UserRoleMappings as projection on db.UserRoleMappings;
    entity MultiGroupUsers as projection on db.MultiGroupUsers;
    entity UnusedRoles as projection on db.UnusedRoles;
    
    // ============================================
    // Actions (POST) - NOT functions (GET)
    // ============================================
    
    action getRiskDashboard(
        auditRunID: UUID
    ) returns {
        auditInfo: {
            name: String(255);
            instance: String(20);
            generatedAt: Timestamp;
            status: String(20);
            mode: String(20);
        };
        riskMetrics: {
            highRiskUsers: Integer;
            mediumRiskUsers: Integer;
            lowRiskUsers: Integer;
            inactiveUsersWithAccess: Integer;
            unusedRolesCount: Integer;
            largeGroupsCount: Integer;
        };
        totals: {
            totalGroups: Integer;
            totalUsers: Integer;
            totalRoles: Integer;
            totalGroupMembers: Integer;
            totalRoleAssignments: Integer;
        };
    };
    
    action getExecutiveSummary(
        auditRunID: UUID
    ) returns {
        auditInfo: {
            name: String(255);
            instance: String(20);
            generatedAt: Timestamp;
            mode: String(20);
            status: String(20);
        };
        metrics: {
            totalGroups: Integer;
            staticGroups: Integer;
            dynamicGroups: Integer;
            totalUsers: Integer;
            highAccessUsers: Integer;
            largeGroups: Integer;
            totalRoles: Integer;
            unusedRoles: Integer;
        };
        riskIndicators: {
            highAccessUsersCount: Integer;
            largeGroupsCount: Integer;
            inactiveUsersWithAccess: Integer;
            unusedRolesCount: Integer;
        };
        recommendations: array of {
            category: String(50);
            priority: String(20);
            description: String(500);
            affectedCount: Integer;
        };
    };
    
    action getGroupDetails(
        auditRunID: UUID,
        groupType: String(20) default null,
        minMembers: Integer default 0,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        groupID: String(50);
        groupName: String(255);
        groupType: String(50);
        totalMemberCount: Integer;
        activeMemberCount: Integer;
        actualMemberCount: Integer;
        roleTargetCount: Integer;
        createdBy: String(255);
        lastModifiedDate: Timestamp;
    };
    
    action getGroupSizeAnalysis(
        auditRunID: UUID
    ) returns {
        distribution: array of {
            sizeRange: String(50);
            groupCount: Integer;
            percentage: Decimal;
        };
        summary: {
            totalGroups: Integer;
            averageGroupSize: Decimal;
            medianGroupSize: Decimal;
            maxGroupSize: Integer;
            minGroupSize: Integer;
        };
        oversizedGroups: array of {
            groupName: String(255);
            totalMembers: Integer;
            activeMembers: Integer;
        };
        staticVsDynamic: array of {
            groupType: String(20);
            count: Integer;
            averageSize: Decimal;
            totalMembers: Integer;
        };
    };
    
    action getUserDetails(
        auditRunID: UUID,
        status: String(20) default null,
        minGroupCount: Integer default 0,
        department: String(255) default null,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        userId: String(100);
        userName: String(255);
        firstName: String(255);
        lastName: String(255);
        status: String(20);
        email: String(255);
        department: String(255);
        division: String(255);
        location: String(255);
        jobTitle: String(255);
        groupCount: Integer;
        roleCount: Integer;
        groupNames: String(5000);
        riskLevel: String(20);
    };
    
    action getUserAccessAnalysis(
        auditRunID: UUID,
        top: Integer default 20
    ) returns {
        topUsersByGroupCount: array of {
            userName: String(255);
            firstName: String(255);
            lastName: String(255);
            department: String(255);
            jobTitle: String(255);
            groupCount: Integer;
            roleCount: Integer;
            riskLevel: String(20);
        };
        accessDistribution: array of {
            groupCountRange: String(50);
            userCount: Integer;
            percentage: Decimal;
        };
        usersWithExcessiveAccess: array of {
            userName: String(255);
            groupCount: Integer;
            roleCount: Integer;
            groups: String(5000);
        };
    };
    
    action getHighRiskUsers(
        auditRunID: UUID,
        minRiskScore: Integer default 5,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        userName: String(255);
        firstName: String(255);
        lastName: String(255);
        department: String(255);
        jobTitle: String(255);
        userStatus: String(20);
        groupCount: Integer;
        groupNames: String(5000);
        riskLevel: String(20);
        riskScore: Integer;
        riskCategory: String(20);
        recommendedAction: String(255);
        isInactive: Boolean;
    };
    
    action getRoleDetails(
        auditRunID: UUID,
        usageStatus: String(20) default null,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        roleId: String(100);
        roleName: String(255);
        roleDesc: String(1000);
        roleType: String(50);
        userType: String(50);
        lastModifiedBy: String(255);
        lastModifiedDate: Timestamp;
        targetPopulationCount: Integer;
        assignedUserCount: Integer;
        usageStatus: String(20);
    };
    
    action getRoleCoverageAnalysis(
        auditRunID: UUID
    ) returns {
        summary: {
            totalRoles: Integer;
            rolesWithTargets: Integer;
            unusedRoles: Integer;
            rolesWithMultipleTargets: Integer;
            averageTargetsPerRole: Decimal;
            averageAssignmentsPerRole: Decimal;
        };
        rolesByType: array of {
            roleType: String(50);
            count: Integer;
            usedCount: Integer;
            unusedCount: Integer;
            totalAssignments: Integer;
        };
        topRolesByAssignments: array of {
            roleName: String(255);
            assignedUserCount: Integer;
            targetGroupCount: Integer;
        };
        unusedRolesList: array of {
            roleId: String(100);
            roleName: String(255);
            lastModifiedDate: Timestamp;
        };
    };
    
    action getGroupRoleMatrix(
        auditRunID: UUID,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        groupId: String(50);
        groupName: String(255);
        roleId: String(100);
        roleName: String(255);
        ruleId: String(50);
        ruleMyFilter: String(50);
        source: String(50);
    };
    
    action getUserRoleMatrix(
        auditRunID: UUID,
        userName: String(255) default null,
        roleName: String(255) default null,
        department: String(255) default null,
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        userName: String(255);
        firstName: String(255);
        lastName: String(255);
        userStatus: String(20);
        department: String(255);
        jobTitle: String(255);
        roleName: String(255);
        roleType: String(50);
        assignedViaGroup: String(255);
    };
    
    action getUsersByRole(
        auditRunID: UUID,
        roleId: String(100),
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        userName: String(255);
        firstName: String(255);
        lastName: String(255);
        department: String(255);
        jobTitle: String(255);
        userStatus: String(20);
        assignedViaGroup: String(255);
    };
    
    action getRolesByUser(
        auditRunID: UUID,
        userName: String(255),
        top: Integer default 100,
        skip: Integer default 0
    ) returns array of {
        roleId: String(100);
        roleName: String(255);
        roleType: String(50);
        assignedViaGroup: String(255);
    };
    
    action exportAuditData(
        auditRunID: UUID,
        format: String(20) default 'JSON'
    ) returns LargeBinary;
    
    action exportReport(
        auditRunID: UUID,
        reportType: String(50),
        format: String(20) default 'CSV'
    ) returns LargeBinary;
}