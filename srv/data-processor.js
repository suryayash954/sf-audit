const cds = require('@sap/cds');
 
class DataProcessor {
    constructor(db, auditID) {
        this.db = db;
        this.auditID = auditID;
        this.stats = {
            groups: 0,
            users: 0,
            memberships: 0
        };
    }

    /**
     * Parse SuccessFactors date format
     */
    parseSFDate(dateStr) {
        if (!dateStr) return null;
        
        // Handle /Date(1234567890000)/ format
        const match = dateStr.match(/\/Date\((\d+)\)\//);
        if (match) {
            return new Date(parseInt(match[1]));
        }
        
        // Try parsing as regular date
        const date = new Date(dateStr);
        return isNaN(date.getTime()) ? null : date;
    }

    /**
     * Insert permission groups
     */
    async insertGroups(groups, type) {
        if (!groups || groups.length === 0) return;
        
        const entries = [];
        
        for (const group of groups) {
            // Parse dates properly
            const lastModified = this.parseSFDate(group.lastModified);
            
            entries.push({
                GroupID: group.groupID,
                parent_AuditID_AuditID: this.auditID,
                GroupName: group.groupName || '',
                GroupType: group.groupType || '',
                StaticOrDynamic: type,
                ActiveMemberCount: group.activeMemberCount || 0,
                TotalMemberCount: group.totalMemberCount || 0,
                CreatedBy_SF: group.createdBy || '',
                LastModifiedDate: lastModified
            });
        }
        
        // Use CAP's INSERT with multiple entries
        await this.db.run(INSERT.into('sap.sf.audit.PermissionGroups').entries(entries));
        this.stats.groups += entries.length;
        console.log(`  ✅ Inserted ${entries.length} ${type} groups`);
    }

    /**
     * Insert users
     */
    async insertUsers(userCache) {
        if (!userCache || userCache.size === 0) return;
        
        const entries = [];
        
        for (const [username, userData] of userCache.entries()) {
            if (userData) {
                entries.push({
                    UserID: username,
                    parent_AuditID_AuditID: this.auditID,
                    FirstName: userData.firstName || '',
                    LastName: userData.lastName || '',
                    Email: userData.email || '',
                    Status: userData.status || 'active',
                    Department: userData.department || '',
                    Division: userData.division || '',
                    JobTitle: userData.jobTitle || '',
                    Location: userData.location || '',
                    HireDate: this.parseSFDate(userData.hireDate),
                    LastLoginDate: this.parseSFDate(userData.lastLoginDate)
                });
            } else {
                // Insert minimal user data if details not available
                entries.push({
                    UserID: username,
                    parent_AuditID_AuditID: this.auditID,
                    FirstName: '',
                    LastName: '',
                    Email: '',
                    Status: 'unknown',
                    Department: '',
                    Division: '',
                    JobTitle: '',
                    Location: '',
                    HireDate: null,
                    LastLoginDate: null
                });
            }
        }
        
        // Insert in batches to avoid SQL limitations
        const batchSize = 100;
        for (let i = 0; i < entries.length; i += batchSize) {
            const batch = entries.slice(i, i + batchSize);
            await this.db.run(INSERT.into('sap.sf.audit.Users').entries(batch));
        }
        
        this.stats.users += entries.length;
        console.log(`  ✅ Inserted ${entries.length} users`);
    }

    /**
     * Insert group memberships
     */
    async insertMemberships(memberships) {
        if (!memberships || memberships.length === 0) return;
        
        const entries = memberships.map(m => ({
            GroupID: m.groupID,
            UserID: m.userID,
            parent_AuditID_AuditID: this.auditID
        }));
        
        // Remove duplicates (same user in same group)
        const uniqueEntries = [];
        const seen = new Set();
        
        for (const entry of entries) {
            const key = `${entry.GroupID}|${entry.UserID}`;
            if (!seen.has(key)) {
                seen.add(key);
                uniqueEntries.push(entry);
            }
        }
        
        // Insert in batches
        const batchSize = 100;
        for (let i = 0; i < uniqueEntries.length; i += batchSize) {
            const batch = uniqueEntries.slice(i, i + batchSize);
            await this.db.run(INSERT.into('sap.sf.audit.GroupMemberships').entries(batch));
        }
        
        this.stats.memberships += uniqueEntries.length;
        console.log(`  ✅ Inserted ${uniqueEntries.length} group memberships (${memberships.length - uniqueEntries.length} duplicates removed)`);
    }

    /**
     * Get statistics
     */
    getStats() {
        return this.stats;
    }
}

module.exports = DataProcessor;