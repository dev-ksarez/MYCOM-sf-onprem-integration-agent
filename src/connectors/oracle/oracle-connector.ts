import { randomUUID } from "crypto";
import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { OracleDatabase } from "../../infrastructure/db/oracle";
import { CanonicalAccount } from "../../types/canonical-account";
import { ConnectorResult } from "../../types/connector-result";
import { DatabaseMetadata } from "../../types/database-metadata";
import { JobContext } from "../../types/job-context";
import { MappedRecord } from "../../types/mapped-record";
import { TargetConnector } from "../../types/target-connector";
import { OracleRepository } from "./oracle-repository";

function getRequiredString(parameters: Record<string, unknown>, key: string): string {
  const value = parameters[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required Oracle connector parameter: ${key}`);
  }
  return value.trim();
}

function getStringWithAliases(
  parameters: Record<string, unknown>,
  keys: string[],
  options?: { defaultValue?: string }
): string {
  for (const key of keys) {
    const value = parameters[key];
    if (typeof value === "string" && value.trim() !== "") {
      return value.trim();
    }
  }

  return options?.defaultValue ?? "";
}

function getOptionalNumber(parameters: Record<string, unknown>, key: string): number | undefined {
  const value = parameters[key];
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  throw new Error(`Invalid numeric Oracle connector parameter: ${key}`);
}

function resolvePassword(config: ConnectorConfig): string {
  const inlinePassword = config.parameters.password;
  if (typeof inlinePassword === "string" && inlinePassword.trim() !== "") {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`Oracle connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(
      `Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`
    );
  }

  return password;
}

function getOptionalUpsertKey(config: ConnectorConfig): string | undefined {
  const upsertKey = config.parameters.upsertKey;
  if (typeof upsertKey !== "string" || upsertKey.trim() === "") {
    return undefined;
  }
  return upsertKey.trim();
}

function normalizeFieldKey(value: string): string {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function getMappedValueByEquivalentKey(values: Record<string, unknown>, key: string): unknown {
  if (Object.prototype.hasOwnProperty.call(values, key)) {
    return values[key];
  }

  const normalizedKey = normalizeFieldKey(key);
  for (const [candidateKey, candidateValue] of Object.entries(values)) {
    if (normalizeFieldKey(candidateKey) === normalizedKey) {
      return candidateValue;
    }
  }

  return undefined;
}

function isMappedRecordConfigurationError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("missing required upsert key") ||
    normalized.includes("does not contain any writable fields") ||
    normalized.includes("invalid oracle identifier") ||
    normalized.includes("ora-00904") ||
    normalized.includes("ora-00942")
  );
}

function buildConnectString(parameters: Record<string, unknown>): string {
  const connectString = getStringWithAliases(parameters, ["connectString", "connectionString", "tnsName"]);
  if (connectString) {
    return connectString;
  }

  const host = getRequiredString(parameters, "server");
  const port = getOptionalNumber(parameters, "port") ?? 1521;
  const serviceName = getStringWithAliases(parameters, ["serviceName", "service"]);
  const sid = getStringWithAliases(parameters, ["sid"]);

  if (serviceName) {
    return `${host}:${port}/${serviceName}`;
  }
  if (sid) {
    return `${host}:${port}:${sid}`;
  }

  throw new Error("Missing required Oracle connector parameter: connectString or serviceName/sid");
}

export class OracleConnector implements TargetConnector {
  private readonly config: ConnectorConfig;
  private readonly database: OracleDatabase;
  private readonly repository?: OracleRepository;
  private readonly upsertKey?: string;
  private initializationPromise?: Promise<void>;

  public constructor(config: ConnectorConfig) {
    this.config = config;

    const schemaName = getStringWithAliases(config.parameters, ["schema", "schemaName"], {
      defaultValue: getRequiredString(config.parameters, "user")
    });
    const tableName = getStringWithAliases(config.parameters, ["table", "tableName"]);

    this.database = new OracleDatabase({
      connectString: buildConnectString(config.parameters),
      user: getRequiredString(config.parameters, "user"),
      password: resolvePassword(config),
      poolMax: getOptionalNumber(config.parameters, "poolMax"),
      poolMin: getOptionalNumber(config.parameters, "poolMin"),
      poolIncrement: getOptionalNumber(config.parameters, "poolIncrement"),
      poolTimeout: getOptionalNumber(config.parameters, "poolTimeout")
    });

    if (tableName) {
      this.repository = new OracleRepository(this.database, schemaName, tableName);
    }
    this.upsertKey = getOptionalUpsertKey(config);
  }

  private ensureRepositoryConfigured(): OracleRepository {
    if (!this.repository) {
      throw new Error("Missing required Oracle connector parameter: table (or tableName)");
    }
    return this.repository;
  }

  private ensureUpsertKeyConfigured(): string {
    if (!this.upsertKey) {
      throw new Error("Missing required Oracle connector parameter: upsertKey");
    }
    return this.upsertKey;
  }

  private async initialize(): Promise<void> {
    if (!this.repository) {
      return;
    }
    if (!this.initializationPromise) {
      this.initializationPromise = this.repository.ensureSchema();
    }
    await this.initializationPromise;
  }

  public systemName(): string {
    return this.config.targetSystem || this.config.connectorType;
  }

  public async testConnection(): Promise<boolean> {
    try {
      await this.database.testConnection();
      await this.initialize();
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Oracle connection error";
      throw new Error(`ORACLE_CONNECTION_FAILED: ${message}`);
    }
  }

  public async close(): Promise<void> {
    await this.database.close();
  }

  public async getDatabaseMetadata(): Promise<DatabaseMetadata> {
    const result = await this.database.query<{
      OWNER: string;
      TABLE_NAME: string;
      COLUMN_NAME: string;
      DATA_TYPE: string;
      NULLABLE: string;
      COLUMN_ID: number;
      DATA_LENGTH?: number;
      DATA_PRECISION?: number;
      DATA_SCALE?: number;
    }>(`
      SELECT
        c.OWNER,
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.NULLABLE,
        c.COLUMN_ID,
        c.DATA_LENGTH,
        c.DATA_PRECISION,
        c.DATA_SCALE
      FROM ALL_TAB_COLUMNS c
      INNER JOIN ALL_TABLES t
        ON t.OWNER = c.OWNER
       AND t.TABLE_NAME = c.TABLE_NAME
      WHERE c.OWNER NOT IN ('SYS', 'SYSTEM')
      ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID
    `);

    const tableMap = new Map<string, DatabaseMetadata["tables"][number]>();
    for (const row of result.rows) {
      const schema = String(row.OWNER || "").trim();
      const tableName = String(row.TABLE_NAME || "").trim();
      const key = `${schema}.${tableName}`;
      const table = tableMap.get(key) || {
        schema,
        name: tableName,
        label: key,
        type: "table",
        columns: []
      };
      table.columns.push({
        name: String(row.COLUMN_NAME || "").trim(),
        type: String(row.DATA_TYPE || "unknown").trim() || "unknown",
        nullable: String(row.NULLABLE || "").toUpperCase() === "Y",
        ordinal: Number(row.COLUMN_ID || 0) || undefined,
        length: row.DATA_LENGTH === undefined ? undefined : Number(row.DATA_LENGTH),
        precision: row.DATA_PRECISION === undefined ? undefined : Number(row.DATA_PRECISION),
        scale: row.DATA_SCALE === undefined ? undefined : Number(row.DATA_SCALE)
      });
      tableMap.set(key, table);
    }

    return {
      connectorId: this.config.id,
      connectorName: this.config.name,
      connectorType: this.config.connectorType,
      databaseName: String(this.config.parameters.serviceName || this.config.parameters.sid || this.config.parameters.connectString || "").trim() || undefined,
      refreshedAt: new Date().toISOString(),
      tables: Array.from(tableMap.values())
    };
  }

  public async upsertAccounts(records: CanonicalAccount[], context: JobContext): Promise<ConnectorResult[]> {
    const repository = this.ensureRepositoryConfigured();
    await this.initialize();

    const results: ConnectorResult[] = [];
    for (const record of records) {
      try {
        if (!record.externalKey || !record.name) {
          const validationMessage = "externalKey and name are required";
          results.push({
            externalKey: record.externalKey,
            success: false,
            statusCode: "VALIDATION_ERROR",
            message: validationMessage,
            retryable: false
          });
          continue;
        }

        const operation = await repository.upsertAccount(record);
        const statusCode = operation === "INSERTED" ? "UPSERT_INSERTED" : "UPSERT_UPDATED";
        results.push({
          externalKey: record.externalKey,
          success: true,
          targetId: `ORACLE-${randomUUID()}`,
          statusCode,
          message: `Account ${operation.toLowerCase()} in run ${context.runId}`,
          retryable: false
        });
      } catch (error) {
        results.push({
          externalKey: record.externalKey,
          success: false,
          statusCode: "TECHNICAL_ERROR",
          message: error instanceof Error ? error.message : "Unknown error",
          retryable: true
        });
      }
    }

    return results;
  }

  public async upsertMappedRecords(
    records: MappedRecord[],
    context: JobContext,
    upsertKeyOverride?: string
  ): Promise<ConnectorResult[]> {
    const repository = this.ensureRepositoryConfigured();
    await this.initialize();

    const results: ConnectorResult[] = [];
    const upsertKey = typeof upsertKeyOverride === "string" && upsertKeyOverride.trim()
      ? upsertKeyOverride.trim()
      : this.ensureUpsertKeyConfigured();

    for (const record of records) {
      const externalKeyValue = getMappedValueByEquivalentKey(record.values, upsertKey);
      const externalKey = typeof externalKeyValue === "string" ? externalKeyValue : String(externalKeyValue ?? "UNKNOWN");

      try {
        const operation = await repository.upsertMappedRecord(record, upsertKey);
        const statusCode = operation === "INSERTED" ? "UPSERT_INSERTED" : "UPSERT_UPDATED";
        results.push({
          externalKey,
          success: true,
          targetId: `ORACLE-${randomUUID()}`,
          statusCode,
          message: `Mapped record ${operation.toLowerCase()} in run ${context.runId}`,
          retryable: false
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";
        const isConfigurationError = isMappedRecordConfigurationError(message);
        results.push({
          externalKey,
          success: false,
          statusCode: isConfigurationError ? "CONFIGURATION_ERROR" : "TECHNICAL_ERROR",
          message,
          retryable: !isConfigurationError
        });

        if (isConfigurationError) {
          return results;
        }
      }
    }

    return results;
  }
}
