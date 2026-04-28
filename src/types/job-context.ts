export interface JobContext {
  runId: string;
  correlationId: string;
  scheduleId: string;
  targetSystem: string;
  batchSize: number;
  maxRetries: number;
}