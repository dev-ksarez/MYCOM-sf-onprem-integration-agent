

import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { MssqlDatabase } from "../../infrastructure/db/mssql";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";

function getRequiredString(parameters: Record<string, unknown>, key: string): string {
  const value = parameters[key];

  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required MSSQL source parameter: ${key}`);
  }

  return value.trim();
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

  throw new Error(`Invalid numeric MSSQL source parameter: ${key}`);
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
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return true;
    }
    if (normalized === "false") {
      return false;
    }
  }

  throw new Error(`Invalid boolean MSSQL source parameter: ${key}`);
}

function resolvePassword(config: ConnectorConfig): string {
  const inlinePassword = config.parameters.password;
  if (typeof inlinePassword === "string" && inlinePassword.trim() !== "") {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`MSSQL source connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(
      `Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`
    );
  }

  return password;
}

export class MssqlQuerySourceAdapter implements SourceAdapter {
  private readonly database: MssqlDatabase;
  private readonly sqlQuery: string;

  public constructor(config: ConnectorConfig, sqlQuery: string) {
    const server = getRequiredString(config.parameters, "server");
    const databaseName = getRequiredString(config.parameters, "database");
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

    this.sqlQuery = sqlQuery.trim();
  }

  public async readRecords(_context: TransferContext): Promise<GenericRecord[]> {
    if (!this.sqlQuery) {
      throw new Error("MSSQL source query must not be empty");
    }

    const result = await this.database.query<Record<string, unknown>>(this.sqlQuery);

    return result.recordset.map((row) => ({
      values: { ...row }
    }));
  }
}