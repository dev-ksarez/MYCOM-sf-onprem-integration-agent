import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { CanonicalAccount } from "../../types/canonical-account";
import { ConnectorResult } from "../../types/connector-result";
import { DatabaseMetadata } from "../../types/database-metadata";
import { JobContext } from "../../types/job-context";
import { MappedRecord } from "../../types/mapped-record";
import { TargetConnector } from "../../types/target-connector";
import { FileMakerDataApiClient } from "./filemaker-data-api";

export class FileMakerConnector implements TargetConnector {
  private readonly config: ConnectorConfig;
  private readonly client: FileMakerDataApiClient;

  public constructor(config: ConnectorConfig) {
    this.config = config;
    this.client = new FileMakerDataApiClient(config);
  }

  public systemName(): string {
    return this.config.targetSystem || this.config.connectorType;
  }

  public async testConnection(): Promise<boolean> {
    try {
      return await this.client.testConnection();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown FileMaker connection error";
      throw new Error(`FILEMAKER_CONNECTION_FAILED: ${message}`);
    }
  }

  public async getDatabaseMetadata(): Promise<DatabaseMetadata> {
    return await this.client.getDatabaseMetadata();
  }

  public async upsertAccounts(_records: CanonicalAccount[], _context: JobContext): Promise<ConnectorResult[]> {
    throw new Error("FileMaker target upsert is not implemented yet. Use FileMaker as source via FILEMAKER_SQL.");
  }

  public async upsertMappedRecords(_records: MappedRecord[], _context: JobContext): Promise<ConnectorResult[]> {
    throw new Error("FileMaker target upsert is not implemented yet. Use FileMaker as source via FILEMAKER_SQL.");
  }
}
