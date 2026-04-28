import { CanonicalAccount } from "./canonical-account";
import { ConnectorResult } from "./connector-result";
import { JobContext } from "./job-context";
import { MappedRecord } from "./mapped-record";

export interface TargetConnector {
  systemName(): string;
  testConnection(): Promise<boolean>;
  upsertAccounts(
    records: CanonicalAccount[],
    context: JobContext
  ): Promise<ConnectorResult[]>;
  upsertMappedRecords(
    records: MappedRecord[],
    context: JobContext
  ): Promise<ConnectorResult[]>;
}