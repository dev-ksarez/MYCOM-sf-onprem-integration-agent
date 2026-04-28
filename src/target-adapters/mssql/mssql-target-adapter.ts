import { MssqlConnector } from "../../connectors/mssql/mssql-connector";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { MappedRecord } from "../../types/mapped-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

export class MssqlTargetAdapter implements TargetAdapter {
  private readonly connector: MssqlConnector;

  public constructor(connector: MssqlConnector) {
    this.connector = connector;
  }

  public async writeRecords(
    records: GenericRecord[],
    context: TransferContext
  ): Promise<ConnectorResult[]> {
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
    });
  }
}
