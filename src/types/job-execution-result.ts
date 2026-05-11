import { ConnectorResult } from "./connector-result";
import { GenericRecord } from "./generic-record";
import { SourceRecordCheckpoint } from "../utils/query-source-definition";

export type JobExecutionStatus = "Success" | "Partial Success" | "Failed";

export interface FailedJobRecord {
  rowIndex: number;
  externalKey?: string;
  statusCode?: string;
  message?: string;
  retryable?: boolean;
  sourceRecord?: Record<string, unknown>;
  mappedRecord?: Record<string, unknown>;
}

export interface JobExecutionResult {
  recordsRead: number;
  recordsProcessed: number;
  recordsSucceeded: number;
  recordsFailed: number;
  status: JobExecutionStatus;
  connectorResults: ConnectorResult[];
  lastProcessedRecord?: SourceRecordCheckpoint;
  successfulSourceRecords?: GenericRecord[];
  failedRecords?: FailedJobRecord[];
}