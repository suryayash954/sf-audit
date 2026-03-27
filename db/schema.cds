namespace sap.sf.audit;

using { Currency, Country, Timezone, Language } from '@sap/cds/common';

/**
 * Audit Header - Master record for each audit run
 */
entity AuditHeaders {
    key AuditID         : UUID                  @title: 'Audit ID';
        Instance        : String(10)            @title: 'Instance' enum { QAS; PROD; };
        GeneratedOn     : Timestamp             @title: 'Generated On';
        ExtractionMode  : String(10)            @title: 'Extraction Mode' enum { FULL; SAMPLE; };
        ReportName      : String(255)           @title: 'Report Name';
        Status          : String(20)            @title: 'Status' enum { Running; Completed; Failed; } default 'Running';
        CreatedAt       : Timestamp             @title: 'Created At' @cds.on.insert: $now;
        CreatedBy       : String(100)           @title: 'Created By' @cds.on.insert: $user;
        ModifiedAt      : Timestamp             @title: 'Modified At' @cds.on.insert: $now @cds.on.update: $now;
        ModifiedBy      : String(100)           @title: 'Modified By' @cds.on.insert: $user @cds.on.update: $user;
}

/**
 * Permission Groups (Static and Dynamic combined)
 */
/**
 * Permission Groups (Static and Dynamic combined)
 */
entity PermissionGroups {
    key GroupID         : Integer               @title: 'Group ID';
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';

        GroupName       : String(255)           @title: 'Group Name';
        GroupType       : String(50)            @title: 'Group Type' enum { permission; ectworkflow; onboarding2; homepage_tile_group; };
        StaticOrDynamic : String(10)            @title: 'Type' enum { STATIC; DYNAMIC; };
        
        // Membership counts
        ActiveMemberCount : Integer             @title: 'Active Members' default 0;
        TotalMemberCount  : Integer             @title: 'Total Members' default 0;
        
        // Metadata from SF
        CreatedBy_SF    : String(100)           @title: 'Created By (SF)';  // Renamed to avoid conflict
        LastModifiedDate : Timestamp            @title: 'Last Modified';
        
        // Audit trail (CAP managed)
        createdAt       : Timestamp             @cds.on.insert: $now;
        createdBy       : String(100)           @cds.on.insert: $user;
}


/**
 * Users - Master user data from SF
 */
entity Users {
    key UserID          : String(100)           @title: 'User ID';
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';

        UserName        : String(255)           @title: 'Username';
        FirstName       : String(255)           @title: 'First Name';
        LastName        : String(255)           @title: 'Last Name';
        Email           : String(255)           @title: 'Email';
        Status          : String(10)            @title: 'Status' enum { active; inactive; };
        
        // Job & Organization
        JobTitle        : String(255)           @title: 'Job Title';
        JobCode         : String(50)            @title: 'Job Code';
        Department      : String(255)           @title: 'Department';
        Division        : String(255)           @title: 'Division';
        Location        : String(255)           @title: 'Location';
        Company         : String(255)           @title: 'Company';
        BusinessUnit    : String(255)           @title: 'Business Unit';
        HireDate        : Date                  @title: 'Hire Date';
        TimeZone        : String(50)            @title: 'Time Zone';
        
        // SF metadata
        LastModifiedDateTime : Timestamp        @title: 'Last Modified in SF';
        CreatedBy_SF    : String(100)           @title: 'Created By (SF)';  // Add if needed from SF
        
        // Audit trail (CAP managed)
        createdAt       : Timestamp             @cds.on.insert: $now;
        createdBy       : String(100)           @cds.on.insert: $user;
}

/**
 * Group Memberships - Junction between Users and PermissionGroups
 */
entity GroupMemberships {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key GroupID         : Integer;
    key UserID          : String(100);

    // Navigation
    Group : Association to PermissionGroups on Group.parent_AuditID = $self.parent_AuditID and Group.GroupID = $self.GroupID;
    User  : Association to Users on User.parent_AuditID = $self.parent_AuditID and User.UserID = $self.UserID;
}

/**
 * RBP Roles
 */
entity RBPRoles {
    key RoleID          : String(100)           @title: 'Role ID';
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';

        RoleName        : String(255)           @title: 'Role Name';
        RoleDesc        : String(1000)          @title: 'Description';
        RoleType        : String(50)            @title: 'Role Type';
        UserType        : String(50)            @title: 'User Type';
        LastModifiedBy  : String(100)           @title: 'Last Modified By';
        LastModifiedDate : Timestamp            @title: 'Last Modified';
        
        // Audit trail
        createdAt       : Timestamp             @cds.on.insert: $now;
        createdBy       : String(100)           @cds.on.insert: $user;
}

/**
 * Role Target Population - Groups assigned to roles
 */
entity RoleTargetPopulation {
    key TargetID       : Integer @cds.autoIncrement @title: 'Target ID';
    key parent_AuditID : Association to AuditHeaders @title: 'Audit';

    RoleID             : String(100) @title: 'Role ID';
    RuleID             : String(100) @title: 'Rule ID';
    Source             : String(50)  @title: 'Source' enum { 
        targetGroups; 
        embedded_rules; 
        api_fetch; 
    };
    GroupID            : Integer     @title: 'Group ID';
    GroupName          : String(255) @title: 'Group Name';
    RuleMyFilter       : String(500) @title: 'Rule Filter';
    RuleStatus         : Integer     @title: 'Rule Status';
    StaticGroup        : Boolean     @title: 'Is Static' default false;
    UserType           : String(50)  @title: 'User Type';

    // Navigation
    Role : Association to RBPRoles 
        on Role.parent_AuditID = $self.parent_AuditID 
       and Role.RoleID = $self.RoleID;
}


/**
 * User-Role Assignments (Derived from group memberships)
 */
entity UserRoleAssignments {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key UserID          : String(100);
    key RoleID          : String(100);

        AssignedViaGroup : String(255)          @title: 'Assigned Via Group';
        
        // Navigations
        User : Association to Users on User.parent_AuditID = $self.parent_AuditID and User.UserID = $self.UserID;
        Role : Association to RBPRoles on Role.parent_AuditID = $self.parent_AuditID and Role.RoleID = $self.RoleID;
}

/**
 * Analytics: Users with multiple groups (pre-computed)
 */
entity MultiGroupUsers {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key UserID          : String(100);

        GroupCount      : Integer               @title: 'Number of Groups';
        GroupNames      : String(5000)          @title: 'Group Names';
        RiskLevel       : String(10)            @title: 'Risk Level' enum { Low; Medium; High; };
        
        // Navigation
        User : Association to Users on User.parent_AuditID = $self.parent_AuditID and User.UserID = $self.UserID;
}

/**
 * Analytics: Group Size Distribution
 */
entity GroupSizeDistribution {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key Bucket          : String(20)            @title: 'Size Bucket' enum { 
        SIZE_1_5        @title: '1–5 members';
        SIZE_6_20       @title: '6–20 members';
        SIZE_21_50      @title: '21–50 members';
        SIZE_51_100     @title: '51–100 members';
        SIZE_100_PLUS   @title: '100+ members';
    };

        GroupCount      : Integer               @title: 'Number of Groups';
}

/**
 * Analytics: Users by Group Count Distribution
 */
entity UserGroupCountDistribution {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key Bucket          : String(20)            @title: 'Group Count Bucket' enum { 
        ONE_GROUP       @title: '1 group';
        TWO_GROUPS      @title: '2 groups';
        THREE_TO_FOUR   @title: '3–4 groups';
        FIVE_TO_SEVEN   @title: '5–7 groups';
        EIGHT_PLUS      @title: '8+ groups';
    };

        UserCount       : Integer               @title: 'Number of Users';
}

/**
 * Unused Roles (for cleanup recommendations)
 */
entity UnusedRoles {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';
    key RoleID          : String(100);

        Recommendation  : String(255)           @title: 'Recommendation' default 'Review / Decommission';
        
        // Navigation
        Role : Association to RBPRoles on Role.parent_AuditID = $self.parent_AuditID and Role.RoleID = $self.RoleID;
}

/**
 * Audit Sign-Off (for compliance tracking)
 */
entity AuditSignOff {
    key parent_AuditID  : Association to AuditHeaders @title: 'Audit';

        ReviewedBy      : String(100)           @title: 'Reviewed By';
        ReviewDate      : Date                  @title: 'Review Date';
        ReviewStatus    : String(20)            @title: 'Status' enum { Draft; Reviewed; Approved; };
        ReviewerComments : String(1000)         @title: 'Comments';
        
        ApprovedBy      : String(100)           @title: 'Approved By';
        ApprovalDate    : Date                  @title: 'Approval Date';
        FinalNotes      : String(1000)          @title: 'Final Notes';
        
        // Audit trail
        createdAt       : Timestamp             @cds.on.insert: $now;
        createdBy       : String(100)           @cds.on.insert: $user;
        modifiedAt      : Timestamp             @cds.on.insert: $now @cds.on.update: $now;
        modifiedBy      : String(100)           @cds.on.insert: $user @cds.on.update: $user;
}