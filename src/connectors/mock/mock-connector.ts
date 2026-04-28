import { randomUUID } from "crypto";
import { SqliteDatabase } from "../../infrastructure/db/sqlite";
import { CanonicalAccount } from "../../types/canonical-account";
import { ConnectorResult } from "../../types/connector-result";
import { JobContext } from "../../types/job-context";
import { TargetConnector } from "../../types/target-connector";
import { MockRepository } from "./mock-repository";
import { MappedRecord } from "../../types/mapped-record";

export class MockConnector implements TargetConnector {
  private readonly repository: MockRepository;
  private readonly initializationPromise: Promise<void>;

  public constructor() {
    const database = new SqliteDatabase();
    this.repository = new MockRepository(database);
    this.initializationPromise = this.repository.initialize();
  }

  public systemName(): string {
    return "mock";
  }

  public async testConnection(): Promise<boolean> {
    await this.initializationPromise;
    return true;
  }

  public async upsertAccounts(
    records: CanonicalAccount[],
    context: JobContext
  ): Promise<ConnectorResult[]> {
    await this.initializationPromise;

    const results: ConnectorResult[] = [];

    for (const record of records) {
      try {
        if (!record.externalKey || !record.name) {
          const validationMessage = "externalKey and name are required";

          await this.repository.writeOperationLog(
            context,
            record.externalKey || "UNKNOWN",
            "VALIDATION",
            "VALIDATION_ERROR",
            validationMessage
          );

          results.push({
            externalKey: record.externalKey,
            success: false,
            statusCode: "VALIDATION_ERROR",
            message: validationMessage,
            retryable: false
          });
          continue;
        }

        const errorRule = await this.repository.findActiveErrorRule(record.externalKey);
        if (errorRule) {
          const isRetryable = errorRule.retryable === 1;
          const statusCode = errorRule.error_type || "SIMULATED_ERROR";

          await this.repository.writeOperationLog(
            context,
            record.externalKey,
            "ERROR_RULE",
            statusCode,
            errorRule.message
          );

          results.push({
            externalKey: record.externalKey,
            success: false,
            statusCode,
            message: errorRule.message,
            retryable: isRetryable
          });
          continue;
        }

        const operation = await this.repository.upsertAccount(record);
        const statusCode = operation === "INSERTED" ? "UPSERT_INSERTED" : "UPSERT_UPDATED";
        const targetId = `MOCK-${randomUUID()}`;
        const message = `Account ${operation.toLowerCase()} in run ${context.runId}`;

        await this.repository.writeOperationLog(
          context,
          record.externalKey,
          operation,
          statusCode,
          message
        );

        results.push({
          externalKey: record.externalKey,
          success: true,
          targetId,
          statusCode,
          message,
          retryable: false
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";

        await this.repository.writeOperationLog(
          context,
          record.externalKey || "UNKNOWN",
          "EXCEPTION",
          "TECHNICAL_ERROR",
          message
        );

        results.push({
          externalKey: record.externalKey,
          success: false,
          statusCode: "TECHNICAL_ERROR",
          message,
          retryable: true
        });
      }
    }

    return results;
  }
  public async upsertMappedRecords(
    records: MappedRecord[],
    context: JobContext
  ): Promise<ConnectorResult[]> {
    await this.initializationPromise;

    const results: ConnectorResult[] = [];

    for (const record of records) {
      const externalKeyValue = record.values.external_key ?? record.values.externalKey;
      const externalKey =
        typeof externalKeyValue === "string"
          ? externalKeyValue
          : String(externalKeyValue ?? "UNKNOWN");

      await this.repository.writeOperationLog(
        context,
        externalKey,
        "MAPPED_RECORD",
        "NOT_IMPLEMENTED",
        `Generic mock mapped-record upsert is not implemented yet for run ${context.runId}`
      );

      results.push({
        externalKey,
        success: false,
        statusCode: "NOT_IMPLEMENTED",
        message: `Generic mock mapped-record upsert is not implemented yet for run ${context.runId}`,
        retryable: false
      });
    }

    return results;
  }
}