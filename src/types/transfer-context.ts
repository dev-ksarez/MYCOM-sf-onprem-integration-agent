import { DeltaCheckpoint } from "../utils/query-source-definition";

export interface TransferProgress {
  phase: "source-read" | "batch-written";
  processedRecords: number;
  totalRecords?: number;
  batchStart?: number;
  batchSize?: number;
}

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
  onProgress?: (progress: TransferProgress) => void | Promise<void>;
}
