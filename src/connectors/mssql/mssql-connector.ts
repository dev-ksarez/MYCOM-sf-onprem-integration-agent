import { randomUUID } from "crypto";
import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { MssqlDatabase } from "../../infrastructure/db/mssql";
import { CanonicalAccount } from "../../types/canonical-account";
import { ConnectorResult } from "../../types/connector-result";
import { JobContext } from "../../types/job-context";
import { MappedRecord } from "../../types/mapped-record";
import { TargetConnector } from "../../types/target-connector";
import { MssqlRepository } from "./mssql-repository";

function getRequiredString(parameters: Record<string, unknown>, key: string): string {
  const value = parameters[key];

  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required MSSQL connector parameter: ${key}`);
  }

  return value.trim();
}

function getStringWithAliases(
  parameters: Record<string, unknown>,
  keys: string[],
  options?: { required?: boolean; defaultValue?: string }
): string {
  for (const key of keys) {
    const value = parameters[key];
    if (typeof value === "string" && value.trim() !== "") {
      return value.trim();
    }
  }

  if (options?.defaultValue !== undefined) {
    return options.defaultValue;
  }

  if (options?.required) {
    throw new Error(`Missing required MSSQL connector parameter: ${keys[0]}`);
  }

  return "";
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

  throw new Error(`Invalid numeric MSSQL connector parameter: ${key}`);
}

function getOptionalBoolean(parameters: Record<string, unknown>, key: string): boolean | undefined {
  const value = parameters[key];

  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    if (value.toLowerCase() === "true") {
      return true;
    }
    if (value.toLowerCase() === "false") {
      return false;
    }
  }

  throw new Error(`Invalid boolean MSSQL connector parameter: ${key}`);
}

function getRequiredUpsertKey(config: ConnectorConfig): string {
  const upsertKey = config.parameters.upsertKey;

  if (typeof upsertKey !== "string" || upsertKey.trim() === "") {
    throw new Error(`Missing required MSSQL connector parameter: upsertKey`);
  }

  return upsertKey.trim();
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
  if (!normalizedKey) {
    return undefined;
  }

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
    normalized.includes("invalid mssql identifier") ||
    normalized.includes("invalid column name") ||
    normalized.includes("invalid object name")
  );
}

function resolvePassword(config: ConnectorConfig): string {
  const inlinePassword = config.parameters.password;
  if (typeof inlinePassword === "string" && inlinePassword.trim() !== "") {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`MSSQL connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(
      `Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`
    );
  }

  return password;
}

export class MssqlConnector implements TargetConnector {
  private readonly config: ConnectorConfig;
  private readonly repository?: MssqlRepository;
  private readonly database: MssqlDatabase;
  private readonly upsertKey?: string;
  private initializationPromise?: Promise<void>;

  public constructor(config: ConnectorConfig) {
    this.config = config;

    const server = getRequiredString(config.parameters, "server");
    const databaseName = getRequiredString(config.parameters, "database");
    // Backward compatibility: older connector definitions used schemaName/tableName.
    const schemaName = getStringWithAliases(config.parameters, ["schema", "schemaName"], { defaultValue: "dbo" });
    const tableName = getStringWithAliases(config.parameters, ["table", "tableName"]);
    const upsertKey = getOptionalUpsertKey(config);
    const user = getRequiredString(config.parameters, "user");
    const password = resolvePassword(config);

    this.database = new MssqlDatabase({
      server,
      port: getOptionalNumber(config.parameters, "port"),
      database: databaseName,
      user,
      password,
      encrypt: getOptionalBoolean(config.parameters, "encrypt"),
      trustServerCertificate: getOptionalBoolean(config.parameters, "trustServerCertificate"),
      connectionTimeout: config.timeoutMs,
      requestTimeout: config.timeoutMs
    });

    if (tableName) {
      this.repository = new MssqlRepository(this.database, schemaName, tableName);
    }
    this.upsertKey = upsertKey;
  }

  private ensureRepositoryConfigured(): MssqlRepository {
    if (!this.repository) {
      throw new Error(
        "Missing required MSSQL connector parameter: table (or tableName)"
      );
    }

    return this.repository;
  }

  private ensureUpsertKeyConfigured(): string {
    if (!this.upsertKey) {
      throw new Error("Missing required MSSQL connector parameter: upsertKey");
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
      const message = error instanceof Error ? error.message : "Unknown MSSQL connection error";
      throw new Error(`MSSQL_CONNECTION_FAILED: ${message}`);
    }
  }

  public async close(): Promise<void> {
    await this.database.close();
  }

  public async upsertAccounts(
    records: CanonicalAccount[],
    context: JobContext
  ): Promise<ConnectorResult[]> {
    const repository = this.ensureRepositoryConfigured();
    await this.initialize();

    const results: ConnectorResult[] = [];

    for (const record of records) {
      try {
        if (!record.externalKey || !record.name) {
          const validationMessage = "externalKey and name are required";

          await repository.writeOperationLog(
            context,
            record.externalKey || "UNKNOWN",
            "VALIDATION",
            "VALIDATION_ERROR",
            validationMessage
          );

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
        const targetId = `MSSQL-${randomUUID()}`;
        const message = `Account ${operation.toLowerCase()} in run ${context.runId}`;

        await repository.writeOperationLog(
          context,
          record.externalKey,
          operation,
          statusCode,
          message
        );

        results.push({
          externalKey: record.externalKey,
          success: true,
          targetId,
          statusCode,
          message,
          retryable: false
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";

        await repository.writeOperationLog(
          context,
          record.externalKey || "UNKNOWN",
          "EXCEPTION",
          "TECHNICAL_ERROR",
          message
        );

        results.push({
          externalKey: record.externalKey,
          success: false,
          statusCode: "TECHNICAL_ERROR",
          message,
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
        const targetId = `MSSQL-${randomUUID()}`;
        const message = `Mapped record ${operation.toLowerCase()} in run ${context.runId}`;

        await repository.writeOperationLog(
          context,
          externalKey,
          operation,
          statusCode,
          message
        );

        results.push({
          externalKey,
          success: true,
          targetId,
          statusCode,
          message,
          retryable: false
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";
        const isConfigurationError = isMappedRecordConfigurationError(message);

        await repository.writeOperationLog(
          context,
          externalKey,
          "EXCEPTION",
          isConfigurationError ? "CONFIGURATION_ERROR" : "TECHNICAL_ERROR",
          message
        );

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