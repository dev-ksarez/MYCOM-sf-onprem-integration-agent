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

interface SalesforceRunRecord {
  Id: string;
  MSD_Status__c?: string;
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

interface OAuthTokenResponse {
  access_token: string;
  instance_url: string;
  token_type: string;
  scope?: string;
}

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
  private readonly config: SalesforceConfig;
  private connection?: Connection;
  private readonly objectPicklistCache: Map<string, SalesforcePicklistValue[]>;
  private readonly globalPicklistCache: Map<string, SalesforcePicklistValue[]>;

  public constructor(config: SalesforceConfig) {
    this.config = config;
    this.objectPicklistCache = new Map();
    this.globalPicklistCache = new Map();
  }

  public async login(): Promise<void> {
    const tokenUrl = `${this.config.loginUrl.replace(/\/$/, "")}/services/oauth2/token`;

    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.config.clientId,
      client_secret: this.config.clientSecret
    });

    const response = await fetch(tokenUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Salesforce token request failed: ${response.status} ${errorText}`);
    }

    const tokenData = (await response.json()) as OAuthTokenResponse;

    if (!tokenData.access_token || !tokenData.instance_url) {
      throw new Error("Salesforce token response is missing access_token or instance_url");
    }

    this.connection = new Connection({
      instanceUrl: tokenData.instance_url,
      accessToken: tokenData.access_token
    });
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

  public async querySchedules(): Promise<SalesforceScheduleRecord[]> {
    if (!this.connection) {
      throw new Error("Salesforce connection not initialized. Call login() first.");
    }

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
      WHERE Active__c = true
      ORDER BY NextRunAt__c ASC
      LIMIT 20
    `;

    const result = await this.connection.query<SalesforceScheduleRecord>(soql);
    return result.records;
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
}