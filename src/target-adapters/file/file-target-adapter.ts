import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";
import { writeFileFromConnector } from "../../utils/file-transfer";

export class FileTargetAdapter implements TargetAdapter {
  private readonly connectorConfig: ConnectorConfig;
  private readonly targetDefinition: string;

  public constructor(connectorConfig: ConnectorConfig, targetDefinition: string) {
    this.connectorConfig = connectorConfig;
    this.targetDefinition = targetDefinition;
  }

  public async writeRecords(records: GenericRecord[], context: TransferContext): Promise<ConnectorResult[]> {
    const mappedRows = records.map((record) => ({ ...record.values }));

    try {
      const result = await writeFileFromConnector(this.connectorConfig, this.targetDefinition, mappedRows);
      return mappedRows.map((row, index) => ({
        externalKey: String(row.id || row.Id || row.externalId || row.externalKey || `row-${index + 1}`),
        success: true,
        targetId: `${result.fileName}:${index + 1}`,
        statusCode: "FILE_WRITE_OK",
        message: `Datensatz in ${result.fileName} geschrieben (Run ${context.runId})`,
        retryable: false
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unbekannter Datei-Schreibfehler";
      return mappedRows.map((row, index) => ({
        externalKey: String(row.id || row.Id || row.externalId || row.externalKey || `row-${index + 1}`),
        success: false,
        statusCode: "FILE_WRITE_ERROR",
        message,
        retryable: true
      }));
    }
  }
}
