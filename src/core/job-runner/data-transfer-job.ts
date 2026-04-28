

import { Logger } from "pino";
import { MappingDefinitionEngine } from "../mapping-dsl/mapping-definition-engine";
import { MappingDefinitionParser } from "../mapping-dsl/mapping-definition-parser";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { JobExecutionResult } from "../../types/job-execution-result";
import { SourceAdapter } from "../../types/source-adapter";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

function resolveStatus(successCount: number, errorCount: number): "Success" | "Partial Success" | "Failed" {
  if (errorCount === 0) {
    return "Success";
  }

  if (successCount === 0) {
    return "Failed";
  }

  return "Partial Success";
}

export class DataTransferJob {
  private readonly logger: Logger;
  private readonly sourceAdapter: SourceAdapter;
  private readonly targetAdapter: TargetAdapter;
  private readonly mappingDefinitionParser: MappingDefinitionParser;
  private readonly mappingDefinitionEngine: MappingDefinitionEngine;

  public constructor(logger: Logger, sourceAdapter: SourceAdapter, targetAdapter: TargetAdapter) {
    this.logger = logger;
    this.sourceAdapter = sourceAdapter;
    this.targetAdapter = targetAdapter;
    this.mappingDefinitionParser = new MappingDefinitionParser();
    this.mappingDefinitionEngine = new MappingDefinitionEngine();
  }

  public async execute(
    context: TransferContext,
    mappingDefinition: string
  ): Promise<JobExecutionResult> {
    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        direction: context.direction,
        sourceType: context.sourceType,
        targetType: context.targetType
      },
      "Starting data transfer job"
    );

    const sourceRecords = await this.sourceAdapter.readRecords(context);

    this.logger.info(
      {
        runId: context.runId,
        recordsRead: sourceRecords.length
      },
      "Source records loaded"
    );

    const parsedDefinition = this.mappingDefinitionParser.parse(mappingDefinition);

    const mappedRecords: GenericRecord[] = sourceRecords.map((record) => {
      const mapped = this.mappingDefinitionEngine.mapRecord(record.values, parsedDefinition.lines);
      return { values: mapped.values };
    });

    const results: ConnectorResult[] = await this.targetAdapter.writeRecords(mappedRecords, context);

    const successCount = results.filter((result) => result.success).length;
    const errorCount = results.length - successCount;
    const status = resolveStatus(successCount, errorCount);

    this.logger.info(
      {
        runId: context.runId,
        processed: results.length,
        successCount,
        errorCount,
        status
      },
      "Data transfer job finished"
    );

    return {
      recordsRead: sourceRecords.length,
      recordsProcessed: results.length,
      recordsSucceeded: successCount,
      recordsFailed: errorCount,
      status,
      connectorResults: results,
      lastProcessedRecord: undefined
    };
  }
}