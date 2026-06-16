import { MssqlConnector } from "../../connectors/mssql/mssql-connector";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { MappedRecord } from "../../types/mapped-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

interface MssqlTargetDefinition {
  upsertKey?: string;
  schema?: string;
  table?: string;
}

function parseMssqlTargetDefinition(rawDefinition?: string): MssqlTargetDefinition {
  const raw = String(rawDefinition || "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const parsedDefinition = parsed as MssqlTargetDefinition;
    const upsertKey = typeof parsedDefinition.upsertKey === "string"
      ? parsedDefinition.upsertKey.trim()
      : "";
    const schema = typeof parsedDefinition.schema === "string"
      ? parsedDefinition.schema.trim()
      : "";
    const table = typeof parsedDefinition.table === "string"
      ? parsedDefinition.table.trim()
      : typeof (parsed as { tableName?: unknown }).tableName === "string"
        ? String((parsed as { tableName?: unknown }).tableName).trim()
        : "";

    return {
      ...(upsertKey ? { upsertKey } : {}),
      ...(schema ? { schema } : {}),
      ...(table ? { table } : {})
    };
  } catch {
    return {};
  }
}

export class MssqlTargetAdapter implements TargetAdapter {
  private readonly connector: MssqlConnector;
  private readonly targetDefinition: MssqlTargetDefinition;

  public constructor(connector: MssqlConnector, targetDefinition?: string) {
    this.connector = connector;
    this.targetDefinition = parseMssqlTargetDefinition(targetDefinition);
  }

  public async writeRecords(
    records: GenericRecord[],
    context: TransferContext
  ): Promise<ConnectorResult[]> {
    const mappedRecords: MappedRecord[] = records.map((record) => ({
      values: { ...record.values }
    }));

    return this.connector.upsertMappedRecords(
      mappedRecords,
      {
        runId: context.runId,
        correlationId: context.correlationId,
        scheduleId: context.scheduleId,
        targetSystem: context.targetType,
        batchSize: context.batchSize,
        maxRetries: context.maxRetries
      },
      this.targetDefinition.upsertKey,
      this.targetDefinition.table
        ? {
          schema: this.targetDefinition.schema,
          table: this.targetDefinition.table
        }
        : undefined
    );
  }
}
