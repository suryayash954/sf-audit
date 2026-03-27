const axios = require('axios');
const cds = require('@sap/cds');
const constants = require('../lib/constants');
const { retrieveJwt, getDestination } = require('@sap-cloud-sdk/connectivity');

/** 
 * SAP SuccessFactors API Client
 * Handles OData v2 calls with pagination, retry logic, and parallel processing
 */
class SFApiClient {
    constructor(destinationName = 'sf-qas') {
        this.destinationName = destinationName;
        this.baseUrl = null;
        this.auth = null;
        this.requestCount = 0;
        this.startTime = null;
    }
    
    /**
     * Initialize connection using destination
     */
    async init() {
        try {
            // Try to get destination from BTP first
            const dest = await this.getSFDestination();
            this.baseUrl = dest.destinationConfiguration.URL.replace(/\/$/, '');
            this.auth = {
                username: dest.destinationConfiguration.User,
                password: dest.destinationConfiguration.Password
            };
            console.log(`✅ Connected to SF API: ${this.baseUrl}`);
        } catch (error) {
            console.error('❌ Failed to initialize SF API client:', error.message);
            throw error;
        }
    }
    
    /**
     * Get destination from BTP or environment
     */
    async getSFDestination() {
        const dest = await getDestination({
                destinationName: 'successfactors'
                });
        // console.log("dest",dest.url);
        const destconfig = {
            destinationConfiguration: {
                URL: dest.url,
                User: dest.User,
                Password: dest.Password
            }
        };
        return destconfig;
    }
    
    /**
     * Fetch all permission groups with pagination
     */
    async fetchGroups(staticFlag) {
        const filter = staticFlag ? 'staticGroup eq true' : 'staticGroup eq false';
        const url = `${this.baseUrl}${constants.SF_ENDPOINTS.GROUPS}?$filter=${filter}&$format=json&$select=${constants.GROUP_FIELDS}`;
        
        console.log(`Fetching ${staticFlag ? 'static' : 'dynamic'} groups...`);
        const groups = await this.fetchAllPages(url);
        console.log(`✅ Found ${groups.length} ${staticFlag ? 'static' : 'dynamic'} groups`);
        return groups;
    }
    
    /**
     * Fetch group members with pagination
     */
    async fetchGroupMembers(groupId) {
        const url = `${this.baseUrl}${constants.SF_ENDPOINTS.GROUP_MEMBERS}?groupId=${groupId}L&$format=json&$select=userId,userName,firstName,lastName`;
        
        try {
            const response = await this.request(url);
            const members = response.data.d?.results || [];
            return members;
        } catch (error) {
            if (error.response?.status === 404) {
                return []; // Group has no members or not applicable
            }
            console.error(`Error fetching members for group ${groupId}:`, error.message);
            return [];
        }
    }
    
    /**
     * Fetch user details with job information
     */
    async fetchUserDetails(userName) {
        const url = `${this.baseUrl}${constants.SF_ENDPOINTS.USERS}?$filter=username eq '${userName}'&$format=json&$expand=empInfo/jobInfoNav&$select=${constants.USER_FIELDS},${constants.JOB_FIELDS}`;
        
        try {
            const response = await this.request(url);
            const results = response.data.d?.results || [];
            if (results.length === 0) {
                // Try inactive users
                return await this.fetchInactiveUserDetails(userName);
            }
            return this.flattenUserData(results[0]);
        } catch (error) {
            console.error(`Error fetching user ${userName}:`, error.message);
            return null;
        }
    }
    
    /**
     * Fetch inactive user details
     */
    async fetchInactiveUserDetails(userName) {
        const url = `${this.baseUrl}${constants.SF_ENDPOINTS.USERS}?$filter=username eq '${userName}' and status eq 'f'&$format=json&$expand=empInfo/jobInfoNav&$select=${constants.USER_FIELDS},${constants.JOB_FIELDS}`;
        
        try {
            const response = await this.request(url);
            const results = response.data.d?.results || [];
            if (results.length > 0) {
                const user = this.flattenUserData(results[0]);
                user.Status = 'inactive';
                return user;
            }
            return null;
        } catch (error) {
            return null;
        }
    }
    
    /**
     * Flatten nested jobInfo data
     */
    flattenUserData(user) {
        const flattened = {
            userId: user.userId,
            userName: user.username,
            firstName: user.firstName,
            lastName: user.lastName,
            email: user.email,
            status: user.status === 't' ? 'active' : 'inactive',
            title: user.title,
            timeZone: user.timeZone,
            hireDate: user.hireDate,
            lastModifiedDateTime: user.lastModifiedDateTime
        };
        
        // Extract job info if available
        if (user.empInfo?.jobInfoNav?.results?.[0]) {
            const job = user.empInfo.jobInfoNav.results[0];
            flattened.jobTitle = job.jobTitle;
            flattened.jobCode = job.jobCode;
            flattened.department = job.department;
            flattened.division = job.division;
            flattened.location = job.location;
            flattened.company = job.company;
            flattened.businessUnit = job.businessUnit;
        }
        
        return flattened;
    }
    
    /**
     * Fetch all roles with expanded rules
     */
    async fetchRoles() {
        const url = `${this.baseUrl}${constants.SF_ENDPOINTS.ROLES}?$format=json&$expand=rules`;
        
        console.log('Fetching roles...');
        const roles = await this.fetchAllPages(url);
        console.log(`✅ Found ${roles.length} roles`);
        return roles;
    }
    
    /**
     * Fetch target population for a role
     */
    async fetchRoleTargetPopulation(roleId) {
        const url = `${this.baseUrl}/RBPRole(${roleId})?$format=json&$expand=targetPopulationNav`;
        
        try {
            const response = await this.request(url);
            const data = response.data.d;
            return data.targetPopulationNav?.results || [];
        } catch (error) {
            return [];
        }
    }
    
    /**
     * Fetch all pages with pagination
     */
    async fetchAllPages(startUrl, maxPages = null) {
        let allResults = [];
        let nextUrl = startUrl;
        let pageCount = 0;
        
        while (nextUrl && (maxPages === null || pageCount < maxPages)) {
            try {
                const response = await this.request(nextUrl);
                const data = response.data.d;
                
                if (data.results) {
                    allResults = allResults.concat(data.results);
                }
                
                nextUrl = data.__next || null;
                pageCount++;
                
                if (nextUrl) {
                    console.log(`  → Page ${pageCount}: ${allResults.length} records so far...`);
                }
            } catch (error) {
                console.error('Error fetching page:', error.message);
                throw error;
            }
        }
        
        return allResults;
    }
    
    /**
     * Make HTTP request with retry logic
     */
    async request(url, retries = 3) {
        this.requestCount++;
        
        for (let i = 0; i < retries; i++) {
            try {
                const response = await axios.get(url, {
                    auth: this.auth,
                    headers: { 
                        'Accept': 'application/json',
                        'User-Agent': 'SAP-CAP-Audit-Tool/1.0'
                    },
                    timeout: 30000
                });
                return response;
            } catch (error) {
                if (i === retries - 1) throw error;
                
                // Exponential backoff
                const delay = 1000 * Math.pow(2, i);
                console.log(`  ⚠ Retry ${i + 1}/${retries} after ${delay}ms: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
    
    /**
     * Get request statistics
     */
    getStats() {
        return {
            requestCount: this.requestCount,
            duration: this.startTime ? Date.now() - this.startTime : 0
        };
    }
}

module.exports = SFApiClient;