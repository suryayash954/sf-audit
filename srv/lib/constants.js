module.exports = {
    // API Endpoints
    SF_ENDPOINTS: {
        GROUPS: '/DynamicGroup',
        GROUP_MEMBERS: '/getUsersByDynamicGroup',
        USERS: '/User',
        ROLES: '/RBPRole',
        ROLE_TARGET: '/RBPRole({roleId})/targetPopulationNav'
    },
    
    // Field selections
    GROUP_FIELDS: [
        'groupID', 'groupName', 'groupType', 
        'activeMembershipCount', 'createdBy', 
        'lastModifiedDate', 'totalMemberCount'
    ].join(','),
    
    USER_FIELDS: [
        'username', 'userId', 'status', 'title', 
        'timeZone', 'email', 'hireDate'
    ].join(','),
    
    JOB_FIELDS: [
        'empInfo/jobInfoNav/jobCode',
        'empInfo/jobInfoNav/jobTitle',
        'empInfo/jobInfoNav/department',
        'empInfo/jobInfoNav/division',
        'empInfo/jobInfoNav/location',
        'empInfo/jobInfoNav/company',
        'empInfo/jobInfoNav/businessUnit'
    ].join(','),
    
    // Batch sizes for parallel processing
    BATCH_SIZE: 50,
    MAX_CONCURRENT: 5,
    
    // Risk thresholds (matches your Python app)
    RISK_THRESHOLDS: {
        HIGH_ACCESS_USERS: 5,  // 5+ groups
        LARGE_GROUP: 51,        // 51+ members
        MULTI_GROUP: {
            LOW: 2,
            MEDIUM: 3,
            HIGH: 4
        }
    }
};