import { ConnectorResult } from "./connector-result";

export type JobExecutionStatus = "Success" | "Partial Success" | "Failed";

export interface JobExecutionResult {
  recordsRead: number;
  recordsProcessed: number;
  recordsSucceeded: number;
  recordsFailed: number;
  status: JobExecutionStatus;
  connectorResults: ConnectorResult[];
  lastProcessedRecord?: {
    lastModified: string;
    sourceId: string;
  };
}