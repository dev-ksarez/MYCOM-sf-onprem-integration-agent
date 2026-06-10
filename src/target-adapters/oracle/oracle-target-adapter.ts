import { OracleConnector } from "../../connectors/oracle/oracle-connector";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { MappedRecord } from "../../types/mapped-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

interface OracleTargetDefinition {
  upsertKey?: string;
}

function parseOracleTargetDefinition(rawDefinition?: string): OracleTargetDefinition {
  const raw = String(rawDefinition || "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const parsedDefinition = parsed as OracleTargetDefinition;
    const upsertKey = typeof parsedDefinition.upsertKey === "string"
      ? parsedDefinition.upsertKey.trim()
      : "";

    return upsertKey ? { upsertKey } : {};
  } catch {
    return {};
  }
}

export class OracleTargetAdapter implements TargetAdapter {
  private readonly connector: OracleConnector;
  private readonly targetDefinition: OracleTargetDefinition;

  public constructor(connector: OracleConnector, targetDefinition?: string) {
    this.connector = connector;
    this.targetDefinition = parseOracleTargetDefinition(targetDefinition);
  }

  public async writeRecords(records: GenericRecord[], context: TransferContext): Promise<ConnectorResult[]> {
    const mappedRecords: MappedRecord[] = records.map((record) => ({
      values: { ...record.values }
    }));

    return this.connector.upsertMappedRecords(mappedRecords, {
      runId: context.runId,
      correlationId: context.correlationId,
      scheduleId: context.scheduleId,
      targetSystem: context.targetType,
      batchSize: context.batchSize,
      maxRetries: context.maxRetries
    }, this.targetDefinition.upsertKey);
  }
}
