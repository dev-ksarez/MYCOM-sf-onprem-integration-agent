import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { FileMakerDataApiClient } from "../../connectors/filemaker/filemaker-data-api";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";

export class FileMakerSourceAdapter implements SourceAdapter {
  private readonly client: FileMakerDataApiClient;
  private readonly sourceDefinition: string;

  public constructor(config: ConnectorConfig, sourceDefinition: string) {
    this.client = new FileMakerDataApiClient(config);
    this.sourceDefinition = sourceDefinition;
  }

  public async readRecords(context: TransferContext): Promise<GenericRecord[]> {
    const rows = await this.client.readRecords(this.sourceDefinition, context.batchSize || 100);
    return rows.map((row) => ({
      values: row,
      checkpoint: row.recordId !== undefined
        ? { value: String(row.recordId), recordId: String(row.recordId) }
        : undefined
    }));
  }
}
