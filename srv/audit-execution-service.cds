using { sap.sf.audit as db } from '../db/schema';
/**
 * Audit Execution Service
 * Handles starting, monitoring, and managing audit runs
 */
service AuditExecutionService @(path: '/SuccessFactorRBPAudit') {

  /**
   * Audit Runs - Read-only for monitoring
   * Posting to AuditRuns will trigger backend audit start
   */
  entity AuditRuns as projection on db.AuditRuns 
    excluding { errorMessage };

  // ============================================
  // Actions for Audit Management
  // ============================================

  action getAuditStatus(
    auditRunID: UUID
  ) returns {
    status: String(20);
    progress: Integer;
    currentPhase: String(50);
    message: String(255);
    groupCount: Integer;
    userCount: Integer;
    roleCount: Integer;
    memberCount: Integer;
  };


  action deleteAuditRun(
    auditRunID: UUID
  ) returns {
    success: Boolean;
    message: String(255);
  };

  function listAuditRuns(
    status: String(20) default null,
    top: Integer default 50,
    skip: Integer default 0
  ) returns array of {
    ID: UUID;
    name: String(255);
    description: String(1000);
    status: String(20);
    mode: String(20);
    instance: String(50);
    startTime: Timestamp;
    endTime: Timestamp;
    createdAt: Timestamp;
    groupCount: Integer;
    userCount: Integer;
    roleCount: Integer;
  };

  type AuditRequest: {
    name: String(255);
    description: String(1000);
    mode: String(20);
    extractGroups: Boolean;
    extractRoles: Boolean;
    sampleGroupSize: Integer;
    sampleMemberSize: Integer;
    sampleRoleSize: Integer;
  };
}
