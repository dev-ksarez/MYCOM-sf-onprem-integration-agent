export interface ConnectorResult {
  externalKey: string;
  success: boolean;
  targetId?: string;
  statusCode?: string;
  message?: string;
  retryable?: boolean;
}