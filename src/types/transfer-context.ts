import { DeltaCheckpoint } from "../utils/query-source-definition";

export interface TransferContext {
  runId: string;
  correlationId: string;
  scheduleId: string;
  direction: string;
  sourceType: string;
  targetType: string;
  batchSize: number;
  maxRetries: number;
  checkpoint?: DeltaCheckpoint;
}
