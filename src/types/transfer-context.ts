export interface TransferContext {
  runId: string;
  correlationId: string;
  scheduleId: string;
  direction: string;
  sourceType: string;
  targetType: string;
  batchSize: number;
  maxRetries: number;
}
