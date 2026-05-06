import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";
import { parseFileFromConnector } from "../../utils/file-transfer";

export class FileSourceAdapter implements SourceAdapter {
  private readonly connectorConfig: ConnectorConfig;
  private readonly sourceDefinition: string;

  public constructor(connectorConfig: ConnectorConfig, sourceDefinition: string) {
    this.connectorConfig = connectorConfig;
    this.sourceDefinition = sourceDefinition;
  }

  public async readRecords(_context: TransferContext): Promise<GenericRecord[]> {
    const payload = await parseFileFromConnector(this.connectorConfig, this.sourceDefinition);
    return payload.rows.map((row) => ({ values: { ...row } }));
  }
}
