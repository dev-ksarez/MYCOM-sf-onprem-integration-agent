

import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { MssqlDatabase } from "../../infrastructure/db/mssql";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";
import {
  DeltaConfig,
  getRecordIdentifier,
  getRecordValueByField,
  normalizeCheckpointValue,
  parseQuerySourceDefinition
} from "../../utils/query-source-definition";

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

function getOptionalString(parameters: Record<string, unknown>, key: string): string | undefined {
  const value = parameters[key];
  return typeof value === "string" && value.trim() !== "" ? value.trim() : undefined;
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
  private readonly definition: ReturnType<typeof parseQuerySourceDefinition>;

  private formatSqlLiteral(value: string): string {
    if (/^0x[0-9a-f]+$/i.test(value) || /^-?\d+(\.\d+)?$/.test(value)) {
      return value;
    }
    return `'${value.replace(/'/g, "''")}'`;
  }

  private appendDeltaFilter(queryText: string, delta: DeltaConfig, checkpointValue: string): string {
    const baseQuery = queryText.replace(/;\s*$/, "").trim();
    const orderMatch = baseQuery.match(/\s+ORDER\s+BY\s+[\s\S]*$/i);
    const withoutOrder = orderMatch ? baseQuery.slice(0, orderMatch.index).trimEnd() : baseQuery;
    const connector = /\bWHERE\b/i.test(withoutOrder) ? " AND " : " WHERE ";
    const condition = `${delta.field} > ${this.formatSqlLiteral(checkpointValue)}`;
    return `${withoutOrder}${connector}${condition} ORDER BY ${delta.field} ASC`;
  }

  public constructor(config: ConnectorConfig, sqlQuery: string) {
    const server = getRequiredString(config.parameters, "server");
    const databaseName = getRequiredString(config.parameters, "database");
    const user = getRequiredString(config.parameters, "user");
    const password = resolvePassword(config);
    const authType = getOptionalString(config.parameters, "authType") || getOptionalString(config.parameters, "authenticationType");

    this.database = new MssqlDatabase({
      server,
      port: getOptionalNumber(config.parameters, "port"),
      database: databaseName,
      user,
      password,
      authType,
      domain: getOptionalString(config.parameters, "domain"),
      encrypt: getOptionalBoolean(config.parameters, "encrypt"),
      trustServerCertificate: getOptionalBoolean(config.parameters, "trustServerCertificate"),
      connectionTimeout: config.timeoutMs,
      requestTimeout: config.timeoutMs
    });

    this.definition = parseQuerySourceDefinition(sqlQuery);
  }

  public async readRecords(context: TransferContext): Promise<GenericRecord[]> {
    if (!this.definition.queryText) {
      throw new Error("MSSQL source query must not be empty");
    }

    const delta = this.definition.delta;
    const checkpointCursor = delta?.strategy === "datetime"
      ? context.checkpoint?.value
      : context.checkpoint?.value || context.checkpoint?.recordId;
    const queryText = delta && checkpointCursor
      ? this.appendDeltaFilter(this.definition.queryText, delta, checkpointCursor)
      : this.definition.queryText;
    const result = await this.database.query<Record<string, unknown>>(queryText);

    return result.recordset.map((row) => ({
      values: { ...row },
      checkpoint: delta
        ? (() => {
            const checkpointValue = normalizeCheckpointValue(getRecordValueByField(row, delta.field));
            if (!checkpointValue) {
              return undefined;
            }
            return {
              value: checkpointValue,
              recordId: getRecordIdentifier(row) || (delta.strategy === "id" ? checkpointValue : undefined)
            };
          })()
        : undefined
    }));
  }
}
