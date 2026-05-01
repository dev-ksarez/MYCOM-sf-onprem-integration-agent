import { Connection } from "jsforce";
import { SalesforceConfig } from "../../infrastructure/config/salesforce-config";

export interface SalesforceAccountRecord {
  Id: string;
  Name: string;
  AccountNumber?: string;
  BillingStreet?: string;
  BillingPostalCode?: string;
  BillingCity?: string;
  BillingCountry?: string;
  Phone?: string;
  Website?: string;
  LastModifiedDate: string;
}

export interface SalesforceScheduleRecord {
  Id: string;
  Name: string;
  Active__c: boolean;
  SourceSystem__c?: string;
  TargetSystem__c?: string;
  ObjectName__c?: string;
  Operation__c?: string;
  MSD_Connector__c?: string;
  MSD_MappingDefinition__c?: string;
  MSD_Direction__c?: string;
  MSD_SourceType__c?: string;
  MSD_TargetType__c?: string;
  MSD_SourceDefinition__c?: string;
  MSD_TargetDefinition__c?: string;
  BatchSize__c?: number;
  NextRunAt__c?: string;
  LastRunAt__c?: string;
}

export interface SalesforceRunRecord {
  Id: string;
  MSD_Status__c?: string;
  MSD_Schedule__c?: string;
  MSD_Schedule__r?: {
    Name?: string;
  };
  MSD_StartedAt__c?: string;
  MSD_FinishedAt__c?: string;
  MSD_RecordsRead__c?: number;
  MSD_RecordsProcessed__c?: number;
  MSD_RecordsSucceeded__c?: number;
  MSD_RecordsFailed__c?: number;
  MSD_ErrorMessage__c?: string;
  MSD_CorrelationId__c?: string;
  MSD_AgentId__c?: string;
}

export interface SalesforceLogRecord {
  Id: string;
  MSD_Run__c?: string;
  MSD_Run__r?: {
    MSD_Schedule__r?: {
      Name?: string;
    };
  };
  MSD_Level__c?: string;
  MSD_Step__c?: string;
  MSD_Message__c?: string;
  MSD_RecordKey__c?: string;
  MSD_CorrelationId__c?: string;
  CreatedDate?: string;
}

export interface SalesforceCheckpointRecord {
  Id: string;
  Name: string;
  MSD_Schedule__c?: string;
  MSD_ObjectName__c?: string;
  MSD_LastCheckpoint__c?: string;
  MSD_LastRecordId__c?: string;
  MSD_Run__c?: string;
}

export interface SalesforceObjectMappingRecord {
  DeveloperName: string;
  MasterLabel: string;
  SourceObject__c?: string;
  TargetSystem__c?: string;
  TargetEntity__c?: string;
  Operation__c?: string;
  Active__c?: boolean;
}

export interface SalesforceFieldMappingRecord {
  DeveloperName: string;
  MasterLabel: string;
  MSD_ObjectMapping__c?: string;
  SourceFieldApi__c?: string;
  CanonicalField__c?: string;
  Active__c?: boolean;
  Sequence__c?: number;
}

export interface SalesforceConnectorRecord {
  Id: string;
  Name: string;
  MSD_Active__c?: boolean;
  MSD_ConnectorType__c?: string;
  MSD_TargetSystem__c?: string;
  MSD_Direction__c?: string;
  MSD_SecretKey__c?: string;
  MSD_TimeoutMs__c?: number;
  MSD_MaxRetries__c?: number;
  MSD_Parameters__c?: string;
  MSD_Description__c?: string;
}

export interface ConnectorConfig {
  id: string;
  name: string;
  active: boolean;
  connectorType: string;
  targetSystem?: string;
  direction?: string;
  secretKey?: string;
  timeoutMs?: number;
  maxRetries?: number;
  parameters: Record<string, unknown>;
  description?: string;
}

export interface CreateLogInput {
  runId: string;
  level: "INFO" | "WARN" | "ERROR";
  step: string;
  message: string;
  correlationId: string;
  recordKey?: string;
}

export interface CheckpointData {
  id: string;
  scheduleId?: string;
  objectName?: string;
  lastCheckpoint?: string;
  lastRecordId?: string;
  lastRunId?: string;
}

export interface ObjectMappingData {
  developerName: string;
  label: string;
  sourceObject?: string;
  targetSystem?: string;
  targetEntity?: string;
  operation?: string;
  active: boolean;
}

export interface FieldMappingData {
  developerName: string;
  label: string;
  objectMappingKey?: string;
  sourceFieldApi?: string;
  canonicalField?: string;
  active: boolean;
  sequence: number;
}

export interface UpsertCheckpointInput {
  checkpointId?: string;
  scheduleId: string;
  objectName: string;
  lastCheckpoint: string;
  lastRecordId: string;
  lastRunId: string;
}

export interface CreateRunInput {
  scheduleId: string;
  correlationId: string;
  agentId: string;
  startedAt: string;
}

export interface UpdateRunInput {
  status: "Running" | "Success" | "Partial Success" | "Failed";
  finishedAt?: string;
  recordsRead?: number;
  recordsProcessed?: number;
  recordsSucceeded?: number;
  recordsFailed?: number;
  errorMessage?: string;
}

export interface SalesforcePicklistValue {
  value: string;
  label: string;
}

export interface SalesforceObjectFieldMetadata {
  name: string;
  label: string;
  type: string;
  nillable: boolean;
}

export interface SalesforceObjectMetadata {
  name: string;
  label: string;
}

export interface SalesforceOrgOverview {
  domain: string;
  instanceUrl?: string;
  organizationId?: string;
  organizationName?: string;
  environment: "Sandbox" | "Production" | "Unknown";
  apiUsage?: {
    max: number;
    used: number;
    remaining: number;
  };
  dataStorageMb?: {
    max: number;
    used: number;
    remaining: number;
  };
  fileStorageMb?: {
    max: number;
    used: number;
    remaining: number;
  };
  licenses?: {
    total: number;
    used: number;
    remaining: number;
  };
}

export interface SalesforceMetadataDeployResult {
  id?: string;
  status?: string;
  success: boolean;
  numberComponentsDeployed?: number;
  numberComponentErrors?: number;
  details?: unknown;
}

export interface GlobalPicklistEntry {
  apiName: string;
  label: string;
}

interface OAuthTokenResponse {
  access_token: string;
  instance_url: string;
  token_type: string;
  scope?: string;
}

interface CachedSalesforceSession {
  instanceUrl: string;
  accessToken: string;
  expiresAt: number;
}

const TOKEN_CACHE_TTL_MS = Number(process.env.SF_TOKEN_CACHE_TTL_MS || 8 * 60 * 1000);

function formatSoqlDateTime(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid datetime value for SOQL filter: ${value}`);
  }

  return date.toISOString();
}

function setNestedValue(target: Record<string, unknown>, path: string, value: unknown): void {
  const segments = path.split(".").filter(Boolean);
  if (segments.length === 0) {
    return;
  }

  let cursor: Record<string, unknown> = target;
  for (let i = 0; i < segments.length - 1; i += 1) {
    const segment = segments[i];
    const existing = cursor[segment];

    if (!existing || typeof existing !== "object" || Array.isArray(existing)) {
      cursor[segment] = {};
    }

    cursor = cursor[segment] as Record<string, unknown>;
  }

  cursor[segments[segments.length - 1]] = value;
}

function buildSalesforceRecordPayload(values: Record<string, unknown>): Record<string, unknown> {
  const payload: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) {
      continue;
    }

    if (key.includes(".")) {
      setNestedValue(payload, key, value);
      continue;
    }

    payload[key] = value;
  }

  return payload;
}

export class SalesforceClient {
  private static readonly sessionCache = new Map<string, CachedSalesforceSession>();
  private static readonly loginInFlight = new Map<string, Promise<CachedSalesforceSession>>();

  private readonly config: SalesforceConfig;
  private connection?: Connection;
  private readonly objectPicklistCache: Map<string, SalesforcePicklistValue[]>;
  private readonly globalPicklistCache: Map<string, SalesforcePicklistValue[]>;

  public constructor(config: SalesforceConfig) {
    this.config = config;
    this.objectPicklistCache = new Map();
    this.globalPicklistCache = new Map();
  }

  private getCacheKey(): string {
    return `${this.config.loginUrl}|${this.config.clientId}`;
  }

  private applyConnection(session: CachedSalesforceSession): void {
    this.connection = new Connection({
      instanceUrl: session.instanceUrl,
      accessToken: session.accessToken
    });
  }

  public async login(): Promise<void> {
    if (this.connection) {
      return;
    }

    const cacheKey = this.getCacheKey();
    const now = Date.now();
    const cachedSession = SalesforceClient.sessionCache.get(cacheKey);
    if (cachedSession && cachedSession.expiresAt > now) {
      this.applyConnection(cachedSession);
      return;
    }

    const existingLogin = SalesforceClient.loginInFlight.get(cacheKey);
    if (existingLogin) {
      const session = await existingLogin;
      this.applyConnection(session);
      return;
    }

    const tokenUrl = `${this.config.loginUrl.replace(/\/$/, "")}/services/oauth2/token`;

    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.config.clientId,
      client_secret: this.config.clientSecret
    });

    const loginPromise = (async (): Promise<CachedSalesforceSession> => {
      const response = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body
      });

      if (!response.ok) {
        const errorText = await response.text();
        if (cachedSession && /login rate exceeded/i.test(errorText)) {
          return cachedSession;
        }
        throw new Error(`Salesforce token request failed: ${response.status} ${errorText}`);
      }

      const tokenData = (await response.json()) as OAuthTokenResponse;

      if (!tokenData.access_token || !tokenData.instance_url) {
        throw new Error("Salesforce token response is missing access_token or instance_url");
      }

      return {
        instanceUrl: tokenData.instance_url,
        accessToken: tokenData.access_token,
        expiresAt: Date.now() + Math.max(60_000, TOKEN_CACHE_TTL_MS)
      };
    })();

    SalesforceClient.loginInFlight.set(cacheKey, loginPromise);
    try {
      const session = await loginPromise;
      SalesforceClient.sessionCache.set(cacheKey, session);
      this.applyConnection(session);
    } finally {
      SalesforceClient.loginInFlight.delete(cacheKey);
    }
  }

  public async queryAccounts(
    lastCheckpoint?: string,
    lastRecordId?: string
  ): Promise<SalesforceAccountRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedLastRecordId = lastRecordId ? lastRecordId.replace(/'/g, "\\'") : undefined;

    const whereClause = lastCheckpoint
      ? escapedLastRecordId
        ? `WHERE (LastModifiedDate > ${formatSoqlDateTime(lastCheckpoint)} OR (LastModifiedDate = ${formatSoqlDateTime(lastCheckpoint)} AND Id > '${escapedLastRecordId}'))`
        : `WHERE LastModifiedDate > ${formatSoqlDateTime(lastCheckpoint)}`
      : "";

    const soql = `
      SELECT
        Id,
        Name,
        AccountNumber,
        BillingStreet,
        BillingPostalCode,
        BillingCity,
        BillingCountry,
        Phone,
        Website,
        LastModifiedDate
      FROM Account
      ${whereClause}
      ORDER BY LastModifiedDate ASC, Id ASC
      LIMIT ${this.config.queryLimit}
    `;

    const result = await this.connection.query<SalesforceAccountRecord>(soql);
    return result.records;
  }

  public async queryGeneric(soql: string): Promise<Record<string, unknown>[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const trimmedSoql = soql.trim();
    if (!trimmedSoql) {
      throw new Error("SOQL query must not be empty");
    }

    const result = await this.connection.query<Record<string, unknown>>(trimmedSoql);
    return result.records.map((record) => ({ ...record }));
  }

  public async ensurePermissionSetAssigned(permissionSetName: string): Promise<{ assigned: boolean; alreadyExisted: boolean }> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const identity = await this.connection.identity();
    const userId = (identity as unknown as { user_id?: string }).user_id;
    if (!userId) {
      throw new Error("Could not determine current Salesforce user ID from identity endpoint.");
    }

    const psResult = await this.connection.query<{ Id: string }>(
      `SELECT Id FROM PermissionSet WHERE Name = '${permissionSetName}' LIMIT 1`
    );
    if (!psResult.records.length) {
      throw new Error(`PermissionSet '${permissionSetName}' not found in Salesforce.`);
    }
    const permissionSetId = psResult.records[0].Id;

    const assignResult = await this.connection.query<{ Id: string }>(
      `SELECT Id FROM PermissionSetAssignment WHERE AssigneeId = '${userId}' AND PermissionSetId = '${permissionSetId}' LIMIT 1`
    );
    if (assignResult.records.length > 0) {
      return { assigned: true, alreadyExisted: true };
    }

    const createResult = await this.connection.sobject("PermissionSetAssignment").create({
      AssigneeId: userId,
      PermissionSetId: permissionSetId
    });
    if (!createResult.success) {
      const errors = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown error";
      throw new Error(`Failed to assign PermissionSet '${permissionSetName}': ${errors}`);
    }
    return { assigned: true, alreadyExisted: false };
  }

  public async querySchedules(activeOnly = true): Promise<SalesforceScheduleRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const whereClause = activeOnly ? "WHERE Active__c = true" : "";

    const soql = `
      SELECT
        Id,
        Name,
        Active__c,
        SourceSystem__c,
        TargetSystem__c,
        ObjectName__c,
        Operation__c,
        MSD_Connector__c,
        MSD_MappingDefinition__c,
        MSD_Direction__c,
        MSD_SourceType__c,
        MSD_TargetType__c,
        MSD_SourceDefinition__c,
        MSD_TargetDefinition__c,
        BatchSize__c,
        NextRunAt__c,
        LastRunAt__c
      FROM MSD_Schedule__c
      ${whereClause}
      ORDER BY NextRunAt__c ASC
      LIMIT 100
    `;

    const result = await this.connection.query<SalesforceScheduleRecord>(soql);
    return result.records;
  }

  public async queryScheduleById(scheduleId: string): Promise<SalesforceScheduleRecord> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedScheduleId = scheduleId.replace(/'/g, "\\'");

    const soql = `
      SELECT
        Id,
        Name,
        Active__c,
        SourceSystem__c,
        TargetSystem__c,
        ObjectName__c,
        Operation__c,
        MSD_Connector__c,
        MSD_MappingDefinition__c,
        MSD_Direction__c,
        MSD_SourceType__c,
        MSD_TargetType__c,
        MSD_SourceDefinition__c,
        MSD_TargetDefinition__c,
        BatchSize__c,
        NextRunAt__c,
        LastRunAt__c
      FROM MSD_Schedule__c
      WHERE Id = '${escapedScheduleId}'
      LIMIT 1
    `;

    const result = await this.connection.query<SalesforceScheduleRecord>(soql);
    if (result.records.length === 0) {
      throw new Error(`Schedule not found: ${scheduleId}`);
    }

    return result.records[0];
  }

  public async createScheduleRecord(fields: Record<string, unknown>): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Schedule__c").create(fields);
    if (!result.success || !result.id) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown create error";
      throw new Error(`Failed to create MSD_Schedule__c record - ${details}`);
    }

    return result.id;
  }

  public async updateScheduleRecord(scheduleId: string, fields: Record<string, unknown>): Promise<void> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Schedule__c").update({
      Id: scheduleId,
      ...fields
    });

    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown update error";
      throw new Error(`Failed to update MSD_Schedule__c record: ${scheduleId} - ${details}`);
    }
  }

  public async deleteScheduleRecord(scheduleId: string): Promise<void> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Schedule__c").destroy(scheduleId);
    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown delete error";
      throw new Error(`Failed to delete MSD_Schedule__c record: ${scheduleId} - ${details}`);
    }
  }

  public async queryConnectors(): Promise<ConnectorConfig[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const soql = `
      SELECT
        Id,
        Name,
        MSD_Active__c,
        MSD_ConnectorType__c,
        MSD_TargetSystem__c,
        MSD_Direction__c,
        MSD_SecretKey__c,
        MSD_TimeoutMs__c,
        MSD_MaxRetries__c,
        MSD_Parameters__c,
        MSD_Description__c
      FROM MSD_Connector__c
      ORDER BY Name ASC
      LIMIT 100
    `;

    const result = await this.connection.query<SalesforceConnectorRecord>(soql);
    return result.records.map((record) => this.toConnectorConfig(record));
  }

  public async queryConnector(connectorId: string): Promise<ConnectorConfig> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedConnectorId = connectorId.replace(/'/g, "\\'");

    const soql = `
      SELECT
        Id,
        Name,
        MSD_Active__c,
        MSD_ConnectorType__c,
        MSD_TargetSystem__c,
        MSD_Direction__c,
        MSD_SecretKey__c,
        MSD_TimeoutMs__c,
        MSD_MaxRetries__c,
        MSD_Parameters__c,
        MSD_Description__c
      FROM MSD_Connector__c
      WHERE Id = '${escapedConnectorId}'
      LIMIT 1
    `;

    const result = await this.connection.query<SalesforceConnectorRecord>(soql);

    if (result.records.length === 0) {
      throw new Error(`Connector not found: ${connectorId}`);
    }

    const record = result.records[0];
    return this.toConnectorConfig(record);
  }

  public async createConnectorRecord(fields: Record<string, unknown>): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Connector__c").create(fields);
    if (!result.success || !result.id) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown create error";
      throw new Error(`Failed to create MSD_Connector__c record - ${details}`);
    }

    return result.id;
  }

  public async updateConnectorRecord(connectorId: string, fields: Record<string, unknown>): Promise<void> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Connector__c").update({
      Id: connectorId,
      ...fields
    });

    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown update error";
      throw new Error(`Failed to update MSD_Connector__c record: ${connectorId} - ${details}`);
    }
  }

  private toConnectorConfig(record: SalesforceConnectorRecord): ConnectorConfig {
    const rawParameters = record.MSD_Parameters__c?.trim();

    let parameters: Record<string, unknown> = {};
    if (rawParameters) {
      try {
        const parsed = JSON.parse(rawParameters) as unknown;

        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
          throw new Error("Connector parameters must be a JSON object.");
        }

        parameters = parsed as Record<string, unknown>;
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown JSON parsing error";
        throw new Error(`Invalid JSON in MSD_Parameters__c for connector ${record.Name}: ${message}`);
      }
    }

    if (!record.MSD_ConnectorType__c) {
      throw new Error(`Connector ${record.Name} is missing MSD_ConnectorType__c`);
    }

    return {
      id: record.Id,
      name: record.Name,
      active: record.MSD_Active__c ?? false,
      connectorType: record.MSD_ConnectorType__c,
      targetSystem: record.MSD_TargetSystem__c,
      direction: record.MSD_Direction__c,
      secretKey: record.MSD_SecretKey__c,
      timeoutMs: record.MSD_TimeoutMs__c,
      maxRetries: record.MSD_MaxRetries__c,
      parameters,
      description: record.MSD_Description__c
    };
  }

  public async queryObjectMappings(
    sourceObject: string,
    targetSystem: string,
    operation: string
  ): Promise<ObjectMappingData[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedSourceObject = sourceObject.replace(/'/g, "\\'");
    const escapedTargetSystem = targetSystem.replace(/'/g, "\\'");
    const escapedOperation = operation.replace(/'/g, "\\'");

    const soql = `
      SELECT
        DeveloperName,
        MasterLabel,
        SourceObject__c,
        TargetSystem__c,
        TargetEntity__c,
        Operation__c,
        Active__c
      FROM MSD_ObjectMapping__mdt
      WHERE Active__c = true
        AND SourceObject__c = '${escapedSourceObject}'
        AND TargetSystem__c = '${escapedTargetSystem}'
        AND Operation__c = '${escapedOperation}'
      ORDER BY DeveloperName ASC
    `;

    const result = await this.connection.query<SalesforceObjectMappingRecord>(soql);

    return result.records.map((record) => ({
      developerName: record.DeveloperName,
      label: record.MasterLabel,
      sourceObject: record.SourceObject__c,
      targetSystem: record.TargetSystem__c,
      targetEntity: record.TargetEntity__c,
      operation: record.Operation__c,
      active: record.Active__c ?? false
    }));
  }

  public async queryFieldMappings(objectMappingKey: string): Promise<FieldMappingData[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedObjectMappingKey = objectMappingKey.replace(/'/g, "\\'");

    const soql = `
      SELECT
        DeveloperName,
        MasterLabel,
        MSD_ObjectMapping__c,
        SourceFieldApi__c,
        CanonicalField__c,
        Active__c,
        Sequence__c
      FROM MSD_FieldMapping__mdt
      WHERE Active__c = true
        AND MSD_ObjectMapping__r.DeveloperName = '${escapedObjectMappingKey}'
      ORDER BY Sequence__c ASC, DeveloperName ASC
    `;

    const result = await this.connection.query<SalesforceFieldMappingRecord>(soql);

    return result.records.map((record) => ({
      developerName: record.DeveloperName,
      label: record.MasterLabel,
      objectMappingKey: record.MSD_ObjectMapping__c,
      sourceFieldApi: record.SourceFieldApi__c,
      canonicalField: record.CanonicalField__c,
      active: record.Active__c ?? false,
      sequence: record.Sequence__c ?? 0
    }));
  }

  public async getCheckpoint(scheduleId: string, objectName: string): Promise<CheckpointData | null> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedScheduleId = scheduleId.replace(/'/g, "\\'");
    const escapedObjectName = objectName.replace(/'/g, "\\'");

    const soql = `
      SELECT
        Id,
        Name,
        MSD_Schedule__c,
        MSD_ObjectName__c,
        MSD_LastCheckpoint__c,
        MSD_LastRecordId__c,
        MSD_Run__c
      FROM MSD_Checkpoint__c
      WHERE MSD_Schedule__c = '${escapedScheduleId}'
        AND MSD_ObjectName__c = '${escapedObjectName}'
      ORDER BY CreatedDate DESC
      LIMIT 1
    `;

    const result = await this.connection.query<SalesforceCheckpointRecord>(soql);

    if (result.records.length === 0) {
      return null;
    }

    const record = result.records[0];

    return {
      id: record.Id,
      scheduleId: record.MSD_Schedule__c,
      objectName: record.MSD_ObjectName__c,
      lastCheckpoint: record.MSD_LastCheckpoint__c,
      lastRecordId: record.MSD_LastRecordId__c,
      lastRunId: record.MSD_Run__c
    };
  }

  public async upsertCheckpoint(input: UpsertCheckpointInput): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    if (input.checkpointId) {
      const updateResult = await this.connection.sobject("MSD_Checkpoint__c").update({
        Id: input.checkpointId,
        MSD_LastCheckpoint__c: input.lastCheckpoint,
        MSD_LastRecordId__c: input.lastRecordId,
        MSD_Run__c: input.lastRunId
      });

      if (!updateResult.success) {
        const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
        throw new Error(`Failed to update MSD_Checkpoint__c record: ${input.checkpointId} - ${details}`);
      }

      return input.checkpointId;
    }

    const createResult = await this.connection.sobject("MSD_Checkpoint__c").create({
      MSD_Schedule__c: input.scheduleId,
      MSD_ObjectName__c: input.objectName,
      MSD_LastCheckpoint__c: input.lastCheckpoint,
      MSD_LastRecordId__c: input.lastRecordId,
      MSD_Run__c: input.lastRunId
    });

    if (!createResult.success || !createResult.id) {
      const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
      throw new Error(`Failed to create MSD_Checkpoint__c record - ${details}`);
    }

    return createResult.id;
  }

  public async createRun(input: CreateRunInput): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Run__c").create({
      MSD_Schedule__c: input.scheduleId,
      MSD_Status__c: "Running",
      MSD_StartedAt__c: input.startedAt,
      MSD_CorrelationId__c: input.correlationId,
      MSD_AgentId__c: input.agentId
    });

    if (!result.success || !result.id) {
      throw new Error("Failed to create MSD_Run__c record");
    }

    return result.id;
  }

  public async hasRunningRunForSchedule(scheduleId: string): Promise<boolean> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedScheduleId = scheduleId.replace(/'/g, "\\'");

    const soql = `
      SELECT
        Id,
        MSD_Status__c
      FROM MSD_Run__c
      WHERE MSD_Schedule__c = '${escapedScheduleId}'
        AND MSD_Status__c = 'Running'
      ORDER BY CreatedDate DESC
      LIMIT 1
    `;

    const result = await this.connection.query<SalesforceRunRecord>(soql);
    return result.records.length > 0;
  }

  public async queryRuns(limit = 50): Promise<SalesforceRunRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const normalizedLimit = Math.max(1, Math.min(limit, 200));
    const soql = `
      SELECT
        Id,
        MSD_Status__c,
        MSD_Schedule__c,
        MSD_Schedule__r.Name,
        MSD_StartedAt__c,
        MSD_FinishedAt__c,
        MSD_RecordsRead__c,
        MSD_RecordsProcessed__c,
        MSD_RecordsSucceeded__c,
        MSD_RecordsFailed__c,
        MSD_ErrorMessage__c,
        MSD_CorrelationId__c,
        MSD_AgentId__c
      FROM MSD_Run__c
      ORDER BY CreatedDate DESC
      LIMIT ${normalizedLimit}
    `;

    const result = await this.connection.query<SalesforceRunRecord>(soql);
    return result.records;
  }

  public async queryLogsByRunId(runId: string, limit = 200): Promise<SalesforceLogRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const escapedRunId = runId.replace(/'/g, "\\'");
    const normalizedLimit = Math.max(1, Math.min(limit, 500));
    const soql = `
      SELECT
        Id,
        MSD_Run__c,
        MSD_Level__c,
        MSD_Step__c,
        MSD_Message__c,
        MSD_RecordKey__c,
        MSD_CorrelationId__c,
        CreatedDate
      FROM MSD_Log__c
      WHERE MSD_Run__c = '${escapedRunId}'
      ORDER BY CreatedDate DESC
      LIMIT ${normalizedLimit}
    `;

    const result = await this.connection.query<SalesforceLogRecord>(soql);
    return result.records;
  }

  public async queryLogsByDateRange(startIso: string, endIso: string, limit = 2000): Promise<SalesforceLogRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const normalizedLimit = Math.max(1, Math.min(limit, 5000));
    const soql = `
      SELECT
        Id,
        MSD_Run__c,
        MSD_Run__r.MSD_Schedule__r.Name,
        MSD_Level__c,
        MSD_Step__c,
        MSD_Message__c,
        MSD_RecordKey__c,
        MSD_CorrelationId__c,
        CreatedDate
      FROM MSD_Log__c
      WHERE CreatedDate >= ${formatSoqlDateTime(startIso)}
        AND CreatedDate < ${formatSoqlDateTime(endIso)}
      ORDER BY CreatedDate DESC
      LIMIT ${normalizedLimit}
    `;

    const result = await this.connection.query<SalesforceLogRecord>(soql);
    return result.records;
  }

  public async updateRun(runId: string, input: UpdateRunInput): Promise<void> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Run__c").update({
      Id: runId,
      MSD_Status__c: input.status,
      MSD_FinishedAt__c: input.finishedAt,
      MSD_RecordsRead__c: input.recordsRead,
      MSD_RecordsProcessed__c: input.recordsProcessed,
      MSD_RecordsSucceeded__c: input.recordsSucceeded,
      MSD_RecordsFailed__c: input.recordsFailed,
      MSD_ErrorMessage__c: input.errorMessage
    });

    if (!result.success) {
      throw new Error(`Failed to update MSD_Run__c record: ${runId}`);
    }
  }

  public async createLog(input: CreateLogInput): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const result = await this.connection.sobject("MSD_Log__c").create({
      MSD_Run__c: input.runId,
      MSD_Level__c: input.level,
      MSD_Step__c: input.step,
      MSD_Message__c: input.message,
      MSD_RecordKey__c: input.recordKey,
      MSD_CorrelationId__c: input.correlationId
    });

    if (!result.success || !result.id) {
      throw new Error("Failed to create MSD_Log__c record");
    }

    return result.id;
  }

  public async upsertGenericRecord(input: {
    objectApiName: string;
    externalIdField: string;
    values: Record<string, unknown>;
  }): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const objectApiName = input.objectApiName.trim();
    const externalIdField = input.externalIdField.trim();

    if (!objectApiName) {
      throw new Error("objectApiName must not be empty");
    }

    if (!externalIdField) {
      throw new Error("externalIdField must not be empty");
    }

    const externalIdValue = input.values[externalIdField];
    if (externalIdValue === undefined || externalIdValue === null || externalIdValue === "") {
      throw new Error(`Missing external id value for field ${externalIdField}`);
    }

    const recordPayload = buildSalesforceRecordPayload(input.values);

    const result = await this.connection
      .sobject(objectApiName)
      .upsert(recordPayload, externalIdField);

    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown upsert error";
      throw new Error(
        `Failed to upsert ${objectApiName} via external id ${externalIdField}=${String(externalIdValue)} - ${details}`
      );
    }

    if (!result.id) {
      throw new Error(
        `Salesforce upsert succeeded for ${objectApiName} but no record id was returned for ${externalIdField}=${String(externalIdValue)}`
      );
    }

    return result.id;
  }

  public async createGenericRecord(
    objectApiName: string,
    values: Record<string, unknown>
  ): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const apiName = objectApiName.trim();
    if (!apiName) {
      throw new Error("objectApiName must not be empty");
    }

    const recordPayload = buildSalesforceRecordPayload(values);
    const result = await this.connection.sobject(apiName).create(recordPayload);

    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown create error";
      throw new Error(`Failed to create ${apiName} - ${details}`);
    }

    if (!result.id) {
      throw new Error(`Salesforce create succeeded for ${apiName} but no record id was returned`);
    }

    return result.id;
  }

  public async updateGenericRecord(
    objectApiName: string,
    values: Record<string, unknown>
  ): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const apiName = objectApiName.trim();
    if (!apiName) {
      throw new Error("objectApiName must not be empty");
    }

    const idValue = values["Id"];
    if (!idValue || typeof idValue !== "string" || !idValue.trim()) {
      throw new Error(`Update requires 'Id' field in mapped values for object ${apiName}`);
    }

    const recordPayload = buildSalesforceRecordPayload(values) as { Id: string } & Record<string, unknown>;
    const result = await this.connection.sobject(apiName).update(recordPayload);

    if (!result.success) {
      const details = "errors" in result ? JSON.stringify(result.errors) : "unknown update error";
      throw new Error(`Failed to update ${apiName} Id=${idValue} - ${details}`);
    }

    return idValue.trim();
  }

  public async upsertPricebookEntryByCompositeKey(values: Record<string, unknown>): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const pricebook2Id = String(values.Pricebook2Id ?? "").trim();
    let product2Id = String(values.Product2Id ?? "").trim();
    const productCode = String(values.ProductCode ?? "").trim();

    if (!product2Id && productCode) {
      const escapedProductCode = productCode.replace(/'/g, "\\'");
      const productLookup = await this.connection.query<{ Id: string }>(`
        SELECT Id
        FROM Product2
        WHERE ProductCode = '${escapedProductCode}'
        LIMIT 1
      `);

      if (productLookup.records.length > 0) {
        product2Id = productLookup.records[0].Id;
      }
    }

    if (!pricebook2Id || !product2Id) {
      const missing = [
        !pricebook2Id ? "Pricebook2Id" : null,
        !product2Id ? "Product2Id" : null
      ].filter(Boolean);
      throw new Error(`PricebookEntry upsert missing required key field(s): ${missing.join(", ")}`);
    }

    const escapedPricebook2Id = pricebook2Id.replace(/'/g, "\\'");
    const escapedProduct2Id = product2Id.replace(/'/g, "\\'");

    const existing = await this.connection.query<{ Id: string }>(`
      SELECT Id
      FROM PricebookEntry
      WHERE Pricebook2Id = '${escapedPricebook2Id}'
        AND Product2Id = '${escapedProduct2Id}'
      LIMIT 1
    `);

    const recordPayload = buildSalesforceRecordPayload({
      ...values,
      Product2Id: product2Id
    });
    delete recordPayload.ProductCode;

    if (existing.records.length > 0) {
      // Product2Id/Pricebook2Id are immutable after creation; keep only updateable payload.
      delete recordPayload.Pricebook2Id;
      delete recordPayload.Product2Id;

      const updateResult = await this.connection.sobject("PricebookEntry").update({
        Id: existing.records[0].Id,
        ...recordPayload
      });

      if (!updateResult.success) {
        const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
        throw new Error(`Failed to update PricebookEntry by composite key - ${details}`);
      }

      return existing.records[0].Id;
    }

    const createResult = await this.connection.sobject("PricebookEntry").create(recordPayload);
    if (!createResult.success || !createResult.id) {
      const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
      throw new Error(`Failed to create PricebookEntry by composite key - ${details}`);
    }

    return createResult.id;
  }

  public async upsertProduct2ByProductCode(values: Record<string, unknown>): Promise<string> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const productCode = String(values.ProductCode ?? "").trim();
    if (!productCode) {
      throw new Error("Product2 upsert requires ProductCode");
    }

    const escapedProductCode = productCode.replace(/'/g, "\\'");
    const existing = await this.connection.query<{ Id: string }>(`
      SELECT Id
      FROM Product2
      WHERE ProductCode = '${escapedProductCode}'
      LIMIT 1
    `);

    const recordPayload = buildSalesforceRecordPayload(values);

    if (existing.records.length > 0) {
      const updateResult = await this.connection.sobject("Product2").update({
        Id: existing.records[0].Id,
        ...recordPayload
      });

      if (!updateResult.success) {
        const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
        throw new Error(`Failed to update Product2 by ProductCode - ${details}`);
      }

      return existing.records[0].Id;
    }

    const createResult = await this.connection.sobject("Product2").create(recordPayload);
    if (!createResult.success || !createResult.id) {
      const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
      throw new Error(`Failed to create Product2 by ProductCode - ${details}`);
    }

    return createResult.id;
  }

  public async getObjectPicklistValues(
    objectApiName: string,
    fieldApiName: string
  ): Promise<SalesforcePicklistValue[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const cacheKey = `${objectApiName}.${fieldApiName}`;
    const cachedValues = this.objectPicklistCache.get(cacheKey);
    if (cachedValues) {
      return cachedValues;
    }

    const describeResult = await this.connection.sobject(objectApiName).describe();
    const fieldDefinition = describeResult.fields.find((field) => field.name === fieldApiName);

    if (!fieldDefinition) {
      throw new Error(`Field ${fieldApiName} does not exist on Salesforce object ${objectApiName}`);
    }

    if (fieldDefinition.type !== "picklist" && fieldDefinition.type !== "multipicklist") {
      throw new Error(`Field ${objectApiName}.${fieldApiName} is not a picklist field`);
    }

    const values = (fieldDefinition.picklistValues || [])
      .filter((entry) => entry.active !== false)
      .map((entry) => ({
        value: String(entry.value ?? "").trim(),
        label: String(entry.label ?? entry.value ?? "").trim()
      }))
      .filter((entry) => entry.value);

    if (values.length === 0) {
      throw new Error(`No active picklist values found for ${objectApiName}.${fieldApiName}`);
    }

    this.objectPicklistCache.set(cacheKey, values);
    return values;
  }

  public async describeObjectFields(objectApiName: string): Promise<SalesforceObjectFieldMetadata[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const describeResult = await this.connection.sobject(objectApiName).describe();
    return (describeResult.fields || [])
      .map((field) => ({
        name: String(field.name ?? "").trim(),
        label: String(field.label ?? field.name ?? "").trim(),
        type: String(field.type ?? "unknown").trim(),
        nillable: Boolean(field.nillable)
      }))
      .filter((field) => field.name);
  }

  public async listObjectMetadata(): Promise<SalesforceObjectMetadata[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const describeGlobalResult = await (this.connection as any).describeGlobal();
    const sobjects = Array.isArray(describeGlobalResult?.sobjects)
      ? describeGlobalResult.sobjects
      : [];

    return sobjects
      .filter((entry: any) => Boolean(entry?.name) && entry.queryable !== false)
      .map((entry: any) => ({
        name: String(entry.name || "").trim(),
        label: String(entry.label || entry.name || "").trim()
      }))
      .filter((entry: SalesforceObjectMetadata) => entry.name)
      .sort((a: SalesforceObjectMetadata, b: SalesforceObjectMetadata) => a.name.localeCompare(b.name));
  }

  public async getOrgOverview(): Promise<SalesforceOrgOverview> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const domain = this.config.loginUrl.replace(/^https?:\/\//i, "").replace(/\/$/, "");
    const instanceUrl = String((this.connection as unknown as { instanceUrl?: string }).instanceUrl || "").trim();

    const overview: SalesforceOrgOverview = {
      domain,
      instanceUrl,
      environment: "Unknown"
    };

    try {
      const orgResult = await this.connection.query<{
        Id?: string;
        Name?: string;
        IsSandbox?: boolean;
      }>("SELECT Id, Name, IsSandbox FROM Organization LIMIT 1");

      const org = orgResult.records[0] || {};
      overview.organizationId = org.Id;
      overview.organizationName = org.Name;
      overview.environment = org.IsSandbox === true ? "Sandbox" : "Production";
    } catch {
      // Keep partial overview if org query is not accessible.
    }

    try {
      const limitsApi = this.connection as unknown as {
        limits: () => Promise<Record<string, { Max?: number; Remaining?: number }>>;
      };
      const limits = await limitsApi.limits();

      const apiLimit = limits?.DailyApiRequests;
      if (apiLimit && Number.isFinite(apiLimit.Max) && Number.isFinite(apiLimit.Remaining)) {
        const max = Number(apiLimit.Max);
        const remaining = Number(apiLimit.Remaining);
        overview.apiUsage = {
          max,
          remaining,
          used: Math.max(0, max - remaining)
        };
      }

      const dataStorageLimit = limits?.DataStorageMB;
      if (dataStorageLimit && Number.isFinite(dataStorageLimit.Max) && Number.isFinite(dataStorageLimit.Remaining)) {
        const max = Number(dataStorageLimit.Max);
        const remaining = Number(dataStorageLimit.Remaining);
        overview.dataStorageMb = {
          max,
          remaining,
          used: Math.max(0, max - remaining)
        };
      }

      const fileStorageLimit = limits?.FileStorageMB;
      if (fileStorageLimit && Number.isFinite(fileStorageLimit.Max) && Number.isFinite(fileStorageLimit.Remaining)) {
        const max = Number(fileStorageLimit.Max);
        const remaining = Number(fileStorageLimit.Remaining);
        overview.fileStorageMb = {
          max,
          remaining,
          used: Math.max(0, max - remaining)
        };
      }
    } catch {
      // Limits are optional in the dashboard panel.
    }

    try {
      const licensesResult = await this.connection.query<{
        TotalLicenses?: number;
        UsedLicenses?: number;
      }>("SELECT TotalLicenses, UsedLicenses FROM UserLicense");

      const totals = licensesResult.records.reduce(
        (acc, item) => {
          acc.total += Number(item.TotalLicenses || 0);
          acc.used += Number(item.UsedLicenses || 0);
          return acc;
        },
        { total: 0, used: 0 }
      );

      if (totals.total > 0) {
        overview.licenses = {
          total: totals.total,
          used: totals.used,
          remaining: Math.max(0, totals.total - totals.used)
        };
      }
    } catch {
      // License visibility can depend on org permissions.
    }

    return overview;
  }

  public async getGlobalPicklistValues(
    globalValueSetApiName: string
  ): Promise<SalesforcePicklistValue[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const cachedValues = this.globalPicklistCache.get(globalValueSetApiName);
    if (cachedValues) {
      return cachedValues;
    }

    const metadataResult = await this.connection.metadata.read(
      "GlobalValueSet",
      globalValueSetApiName
    );

    const metadataEntry = Array.isArray(metadataResult) ? metadataResult[0] : metadataResult;
    if (!metadataEntry || typeof metadataEntry !== "object") {
      throw new Error(`Global value set not found: ${globalValueSetApiName}`);
    }

    const customValuesRaw = (metadataEntry as Record<string, unknown>).customValue;
    const customValues = Array.isArray(customValuesRaw)
      ? customValuesRaw
      : customValuesRaw
        ? [customValuesRaw]
        : [];

    const values = customValues
      .filter((entry) => {
        if (!entry || typeof entry !== "object") {
          return false;
        }

        const active = (entry as Record<string, unknown>).isActive;
        return active === undefined || active === true;
      })
      .map((entry) => {
        const record = entry as Record<string, unknown>;
        const value = String(record.fullName ?? "").trim();
        const label = String(record.label ?? record.fullName ?? "").trim();

        return {
          value,
          label
        };
      })
      .filter((entry) => entry.value);

    if (values.length === 0) {
      throw new Error(`No active values found in global value set ${globalValueSetApiName}`);
    }

    this.globalPicklistCache.set(globalValueSetApiName, values);
    return values;
  }

  public async syncGlobalValueSetValues(input: {
    globalValueSetApiName: string;
    entries: GlobalPicklistEntry[];
  }): Promise<{ added: number; updated: number; total: number }> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const globalValueSetApiName = input.globalValueSetApiName.trim();
    if (!globalValueSetApiName) {
      throw new Error("globalValueSetApiName must not be empty");
    }

    const normalizedEntries = Array.from(
      new Map(
        input.entries
          .map((entry) => ({
            apiName: String(entry.apiName || "").trim(),
            label: String(entry.label || entry.apiName || "").trim()
          }))
          .filter((entry) => entry.apiName)
          .map((entry) => [entry.apiName, entry])
      ).values()
    );

    if (normalizedEntries.length === 0) {
      throw new Error(`No valid entries provided for global value set ${globalValueSetApiName}`);
    }

    const metadataApi = this.connection.metadata as unknown as {
      read: (type: string, fullName: string) => Promise<unknown>;
      update: (type: string, metadata: unknown) => Promise<unknown>;
      create: (type: string, metadata: unknown) => Promise<unknown>;
    };

    let existing: Record<string, unknown> | null = null;
    try {
      const readResult = await metadataApi.read("GlobalValueSet", globalValueSetApiName);
      const metadataEntry = Array.isArray(readResult) ? readResult[0] : readResult;
      if (metadataEntry && typeof metadataEntry === "object") {
        existing = metadataEntry as Record<string, unknown>;
      }
    } catch {
      existing = null;
    }

    const existingValuesRaw = existing?.customValue;
    const existingValues = Array.isArray(existingValuesRaw)
      ? existingValuesRaw
      : existingValuesRaw
        ? [existingValuesRaw]
        : [];

    const byApiName = new Map<string, Record<string, unknown>>();
    for (const value of existingValues) {
      if (!value || typeof value !== "object") {
        continue;
      }

      const record = value as Record<string, unknown>;
      const fullName = String(record.fullName ?? "").trim();
      if (!fullName) {
        continue;
      }

      byApiName.set(fullName, record);
    }

    let added = 0;
    let updated = 0;

    for (const entry of normalizedEntries) {
      const existingValue = byApiName.get(entry.apiName);
      if (!existingValue) {
        byApiName.set(entry.apiName, {
          fullName: entry.apiName,
          default: false,
          label: entry.label,
          isActive: true
        });
        added += 1;
        continue;
      }

      const previousLabel = String(existingValue.label ?? existingValue.fullName ?? "").trim();
      if (previousLabel !== entry.label || existingValue.isActive === false) {
        existingValue.label = entry.label;
        existingValue.isActive = true;
        updated += 1;
      }
    }

    const mergedValues = Array.from(byApiName.values());

    const metadataPayload: Record<string, unknown> = {
      fullName: globalValueSetApiName,
      masterLabel: String(existing?.masterLabel ?? globalValueSetApiName),
      sorted: existing?.sorted === true,
      customValue: mergedValues
    };

    const writeResult = existing
      ? await metadataApi.update("GlobalValueSet", metadataPayload)
      : await metadataApi.create("GlobalValueSet", metadataPayload);

    const resultArray = Array.isArray(writeResult) ? writeResult : [writeResult];
    const failed = resultArray.find(
      (entry) => entry && typeof entry === "object" && "success" in (entry as Record<string, unknown>) && (entry as Record<string, unknown>).success === false
    ) as Record<string, unknown> | undefined;

    if (failed) {
      const errors = failed.errors ? JSON.stringify(failed.errors) : "unknown metadata error";
      throw new Error(`Failed to sync global value set ${globalValueSetApiName}: ${errors}`);
    }

    this.globalPicklistCache.delete(globalValueSetApiName);
    return { added, updated, total: normalizedEntries.length };
  }

  public async createOrUpdateMetadata(
    metadataType: string,
    fullName: string,
    metadata: Record<string, unknown>
  ): Promise<unknown> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const metadataApi = this.connection.metadata as unknown as {
      read: (type: string, fullName: string) => Promise<unknown>;
      update: (type: string, metadata: unknown) => Promise<unknown>;
      create: (type: string, metadata: unknown) => Promise<unknown>;
    };

    const payload: Record<string, unknown> = {
      ...metadata,
      fullName
    };

    let exists = false;
    if (metadataType === "CustomObject") {
      try {
        const objects = await this.listObjectMetadata();
        exists = objects.some((entry) => String(entry.name || "").toLowerCase() === fullName.toLowerCase());
      } catch {
        exists = false;
      }
    } else {
      try {
        const readResult = await metadataApi.read(metadataType, fullName);
        const metadataEntry = Array.isArray(readResult) ? readResult[0] : readResult;
        if (metadataEntry && typeof metadataEntry === "object") {
          const entryFullName = String((metadataEntry as Record<string, unknown>).fullName || "").trim();
          exists = entryFullName
            ? entryFullName.toLowerCase() === fullName.toLowerCase()
            : true;
        } else {
          exists = false;
        }
      } catch {
        exists = false;
      }
    }

    if (exists) {
      return {
        success: true,
        action: "exists",
        type: metadataType,
        fullName
      };
    }

    const writeResult = await metadataApi.create(metadataType, payload);

    const resultArray = Array.isArray(writeResult) ? writeResult : [writeResult];
    const failed = resultArray.find(
      (entry) => entry && typeof entry === "object" && "success" in (entry as Record<string, unknown>) && (entry as Record<string, unknown>).success === false
    ) as Record<string, unknown> | undefined;

    if (failed) {
      const errors = failed.errors ? JSON.stringify(failed.errors) : "unknown metadata error";

      if (metadataType === "CustomObject") {
        try {
          const objects = await this.listObjectMetadata();
          const present = objects.some((entry) => String(entry.name || "").toLowerCase() === fullName.toLowerCase());
          if (present) {
            return {
              success: true,
              action: "exists",
              type: metadataType,
              fullName
            };
          }
        } catch {
          // Keep original error if fallback object lookup fails.
        }
      }

      throw new Error(`Failed to create ${metadataType} ${fullName}: ${errors}`);
    }

    return writeResult;
  }

  public async deployMetadataZip(zipBase64: string): Promise<SalesforceMetadataDeployResult> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

    const metadataApi = this.connection.metadata as unknown as {
      deploy: (zipContent: string, options: { rollbackOnError: boolean; singlePackage: boolean }) => Promise<{ id?: string; status?: string }>;
      checkDeployStatus: (id: string, includeDetails?: boolean) => Promise<Record<string, unknown>>;
    };

    const deployResult = await metadataApi.deploy(zipBase64, {
      rollbackOnError: true,
      singlePackage: true
    });

    const deployId = String(deployResult?.id || "").trim();
    if (!deployId) {
      throw new Error("Metadata deploy did not return a deployment id");
    }

    for (let attempt = 0; attempt < 120; attempt += 1) {
      const status = await metadataApi.checkDeployStatus(deployId, true);
      if (status.done === true) {
        return {
          id: deployId,
          status: String(status.status || "").trim() || undefined,
          success: status.success === true,
          numberComponentsDeployed: Number(status.numberComponentsDeployed || 0),
          numberComponentErrors: Number(status.numberComponentErrors || 0),
          details: status.details
        };
      }

      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    throw new Error(`Metadata deploy timed out: ${deployId}`);
  }
}