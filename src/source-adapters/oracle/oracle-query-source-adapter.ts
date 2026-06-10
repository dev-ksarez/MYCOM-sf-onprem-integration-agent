import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { OracleDatabase } from "../../infrastructure/db/oracle";
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
    throw new Error(`Missing required Oracle source parameter: ${key}`);
  }
  return value.trim();
}

function getOptionalString(parameters: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = parameters[key];
    if (typeof value === "string" && value.trim() !== "") {
      return value.trim();
    }
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
  throw new Error(`Invalid numeric Oracle source parameter: ${key}`);
}

function resolvePassword(config: ConnectorConfig): string {
  const inlinePassword = config.parameters.password;
  if (typeof inlinePassword === "string" && inlinePassword.trim() !== "") {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`Oracle source connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(
      `Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`
    );
  }

  return password;
}

function buildConnectString(parameters: Record<string, unknown>): string {
  const connectString = getOptionalString(parameters, ["connectString", "connectionString", "tnsName"]);
  if (connectString) {
    return connectString;
  }

  const host = getRequiredString(parameters, "server");
  const port = getOptionalNumber(parameters, "port") ?? 1521;
  const serviceName = getOptionalString(parameters, ["serviceName", "service"]);
  const sid = getOptionalString(parameters, ["sid"]);

  if (serviceName) {
    return `${host}:${port}/${serviceName}`;
  }
  if (sid) {
    return `${host}:${port}:${sid}`;
  }

  throw new Error("Missing required Oracle source parameter: connectString or serviceName/sid");
}

export class OracleQuerySourceAdapter implements SourceAdapter {
  private readonly database: OracleDatabase;
  private readonly definition: ReturnType<typeof parseQuerySourceDefinition>;

  private formatSqlLiteral(value: string): string {
    if (/^-?\d+(\.\d+)?$/.test(value)) {
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
    this.database = new OracleDatabase({
      connectString: buildConnectString(config.parameters),
      user: getRequiredString(config.parameters, "user"),
      password: resolvePassword(config),
      poolMax: getOptionalNumber(config.parameters, "poolMax"),
      poolMin: getOptionalNumber(config.parameters, "poolMin"),
      poolIncrement: getOptionalNumber(config.parameters, "poolIncrement"),
      poolTimeout: getOptionalNumber(config.parameters, "poolTimeout")
    });

    this.definition = parseQuerySourceDefinition(sqlQuery);
  }

  public async readRecords(context: TransferContext): Promise<GenericRecord[]> {
    if (!this.definition.queryText) {
      throw new Error("Oracle source query must not be empty");
    }

    const delta = this.definition.delta;
    const checkpointCursor = delta?.strategy === "datetime"
      ? context.checkpoint?.value
      : context.checkpoint?.value || context.checkpoint?.recordId;
    const queryText = delta && checkpointCursor
      ? this.appendDeltaFilter(this.definition.queryText, delta, checkpointCursor)
      : this.definition.queryText;
    const result = await this.database.query<Record<string, unknown>>(queryText);

    return result.rows.map((row) => ({
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
