import pino from "pino";
import { JobContext } from "../../types/job-context";
import { TargetConnector } from "../../types/target-connector";
import { CanonicalAccount } from "../../types/canonical-account";
import { JobExecutionResult } from "../../types/job-execution-result";

export interface AccountSource {
  getAccounts(
    targetSystem: string,
    lastCheckpoint?: string,
    lastRecordId?: string,
    mappingDefinition?: string
  ): Promise<CanonicalAccount[]>;
}

export class AccountExportJob {
  private readonly logger: pino.Logger;
  private readonly source: AccountSource;
  private readonly connector: TargetConnector;

  public constructor(
    logger: pino.Logger,
    source: AccountSource,
    connector: TargetConnector
  ) {
    this.logger = logger;
    this.source = source;
    this.connector = connector;
  }

  public async execute(
    context: JobContext,
    lastCheckpoint?: string,
    lastRecordId?: string,
    mappingDefinition?: string
  ): Promise<JobExecutionResult> {
    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        targetSystem: context.targetSystem
      },
      "Starting account export job"
    );

    const accounts = await this.source.getAccounts(
      context.targetSystem,
      lastCheckpoint,
      lastRecordId,
      mappingDefinition
    );

    this.logger.info(
      {
        runId: context.runId,
        recordsRead: accounts.length
      },
      "Accounts loaded from source"
    );

    const results = await this.connector.upsertAccounts(accounts, context);

    const successCount = results.filter((result) => result.success).length;
    const errorCount = results.length - successCount;

    const status =
      results.length === 0
        ? "Failed"
        : errorCount === 0
          ? "Success"
          : successCount === 0
            ? "Failed"
            : "Partial Success";

    this.logger.info(
      {
        runId: context.runId,
        processed: results.length,
        successCount,
        errorCount,
        results
      },
      "Account export job finished"
    );

    return {
      recordsRead: accounts.length,
      recordsProcessed: results.length,
      recordsSucceeded: successCount,
      recordsFailed: errorCount,
      status,
      connectorResults: results,
      lastProcessedRecord:
        accounts.length > 0
          ? {
              lastModified: accounts[accounts.length - 1].lastModified,
              sourceId: accounts[accounts.length - 1].sourceId
            }
          : undefined
    };
  }
}