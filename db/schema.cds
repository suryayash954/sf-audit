// srv/schema.cds
namespace sap.sf.audit;

using { managed } from '@sap/cds/common';

/**
 * Audit Run - Main container for each audit execution
 */
entity AuditRuns : managed {
    key ID      : UUID;
        name    : String(255);
        description : String(1000);
        status  : String(20) default 'CREATED';
        mode    : String(20); // FULL, SAMPLE
        sampleGroupSize : Integer default 0;
        sampleMemberSize : Integer default 0;
        sampleRoleSize : Integer default 0;
        extractGroups : Boolean default true;
        extractRoles : Boolean default true;
        startTime : Timestamp;
        endTime : Timestamp;
        errorMessage : String(2000);
        
        // Performance tracking
        groupsProcessed : Integer default 0;
        membershipsProcessed : Integer default 0;
        rolesProcessed : Integer default 0;
        
        // Sync metadata
        userSyncRequired : Boolean default false;
        userSyncCompleted : Boolean default false;
        userSyncAt : Timestamp;
}

/**
 * Permission Groups (Static and Dynamic)
 */
entity Groups {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        groupID     : String(50);
        groupName   : String(255);
        groupType   : String(50); // STATIC, DYNAMIC
        groupTypeInternal : String(20);
        activeMembershipCount : Integer;
        totalMemberCount : Integer;
        createdBy   : String(255);
        lastModifiedDate : Timestamp;
        
        // Navigation
        members : Association to GroupMembers;
}
/**
 * Group Members - Users belonging to groups
 */
entity GroupMembers {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        group       : Association to Groups;
        user        : Association to Users;  // Links to central Users table
        
        // Denormalized for performance
        groupID     : String(50);
        groupName   : String(255);
        groupType   : String(50);
        userId      : String(100);  // References Users.userId
        userName    : String(255);
}


/**
 * Users - Person data from SF
 */
entity Users : managed {
    key userId     : String(100);  // Natural key from SuccessFactors
        userName   : String(255) @title: 'Username';
        firstName  : String(255);
        lastName   : String(255);
        status     : String(20);   // active, inactive
        email      : String(255);
        hireDate   : Timestamp;
        terminationDate : Timestamp;
        lastModifiedDateTime : Timestamp;
        timeZone   : String(100);
        
        // Job & Organization fields
        jobTitle   : String(255);
        jobCode    : String(50);
        department : String(255);
        division   : String(255);
        location   : String(255);
        company    : String(255);
        businessUnit : String(255);
        
        // Custom fields
        custom01   : String(255); // Cost Center
        custom02   : String(255);
        custom03   : String(255);
        
        // Sync metadata
        lastSyncAt : Timestamp;
        isActive   : Boolean default true;
        
        // Navigation
        groupMemberships : Association to GroupMembers;
}

/**
 * Permission Roles
 */
entity Roles : managed {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        roleId      : String(100);
        roleName    : String(255);
        roleDesc    : String(1000);
        roleType    : String(50);
        userType    : String(50);
        lastModifiedBy : String(255);
        lastModifiedDate : Timestamp;
        
        // Navigation
        userMappings : Association to UserRoleMappings;
}

/**
 * Role Target Populations - Groups/Users that receive role assignments
 */
entity RoleTargetPopulations {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        role        : Association to Roles;
        
        roleId      : String(100);
        roleName    : String(255);
        ruleId      : String(50);
        source      : String(50); // targetGroups, embedded_rules, api_fetch
        ruleMyFilter : String(50); // SELF, DEPARTMENT, etc.
        ruleStatus  : Integer;
        
        // Target group information
        groupId     : String(50);
        groupName   : String(255);
        groupType   : String(50);
        activeMembershipCount : Integer;
        totalMemberCount : Integer;
        staticGroup : Boolean;
        userType    : String(50);
        
        // Denormalized for quick lookup
        createdBy   : String(255);
        lastModifiedDate : Timestamp;
}

/**
 * User-Role Mappings - Derived from groups targeting roles
 */
entity UserRoleMappings {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        user        : Association to Users;  // Links to central Users table
        role        : Association to Roles;
        userId      : String(100);
        userName    : String(255);
        roleId      : String(100);
        roleName    : String(255);
        assignedViaGroup : String(255);
}

/**
 * Users with Multiple Group Access - Risk Analysis
 */
entity MultiGroupUsers {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        userId      : String(100);  // Reference to central Users.userId
        
        userName    : String(255);
        groupCount  : Integer;
        groupNames  : String(5000);
        riskLevel   : String(20);
        riskScore   : Integer;
        riskCategory : String(20);
        recommendedAction : String(255);
}

/**
 * Inactive Users with Active Access - Risk Report
 */
entity InactiveUserAccess {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        userId      : String(100);  // Reference to central Users.userId
        
        userName    : String(255);
        firstName   : String(255);
        lastName    : String(255);
        status      : String(20);
        hireDate    : Timestamp;
        permissionGroups : String(5000);
        groupCount  : Integer;
        riskScore   : Integer;
        riskCategory : String(20);
        recommendedAction : String(255);
}

/**
 * Group Size Distribution - Analytics
 */
entity GroupSizeDistribution {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        bucket      : String(50); // 1–5 members, 6–20 members, etc.
        groupCount  : Integer;
}

/**
 * Static vs Dynamic Group Comparison - Analytics
 */
entity StaticVsDynamicGroups {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        groupType   : String(20); // Static, Dynamic
        averageGroupSize : Integer;
        totalGroups : Integer;
        totalMembers : Integer;
}

/**
 * Users by Group Count Distribution - Analytics
 */
entity UserGroupCountDistribution {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        bucket      : String(50); // 1 group, 2 groups, 3–4 groups, 5–7 groups, 8+ groups
        userCount   : Integer;
        
        // For highest bucket tracking
        isHighestBucket : Boolean default false;
}

/**
 * Unused Roles - Roles with no target population
 */
entity UnusedRoles {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        role        : Association to Roles;
        
        roleId      : String(100);
        roleName    : String(255);
        roleType    : String(50);
        lastModifiedDate : Timestamp;
        recommendation : String(255) default 'Review / Decommission';
}

/**
 * Executive Summary Metrics - Aggregated KPIs
 */
entity ExecutiveSummary {
    key ID          : UUID;
        auditRunID  : Association to AuditRuns;
        
        totalStaticGroups : Integer;
        totalDynamicGroups : Integer;
        totalGroups : Integer;
        totalUniqueUsers : Integer;
        usersHighAccess : Integer; // 5+ groups
        groupsLargeMembership : Integer; // 51+ members
        
        // Risk indicators
        highAccessUsersCount : Integer;
        largeGroupsCount : Integer;
        
        // Instance metadata
        instanceLabel : String(20);
        generationTimestamp : Timestamp;
        extractionMode : String(20);
}

