

import { Logger } from "pino";
import { MappingDefinitionEngine, LookupResolverFn } from "../mapping-dsl/mapping-definition-engine";
import { MappingDefinitionParser } from "../mapping-dsl/mapping-definition-parser";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { FailedJobRecord } from "../../types/job-execution-result";
import { JobExecutionResult } from "../../types/job-execution-result";
import { SourceAdapter } from "../../types/source-adapter";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

export type BulkLookupResolverFn = (
  requests: Array<{ objectName: string; field: string; values: string[] }>
) => Promise<Map<string, string | null>>;

function resolveStatus(successCount: number, errorCount: number): "Success" | "Partial Success" | "Failed" {
  if (errorCount === 0) {
    return "Success";
  }

  if (successCount === 0) {
    return "Failed";
  }

  return "Partial Success";
}

function readSourceValue(values: Record<string, unknown>, sourceField: string): unknown {
  const key = String(sourceField || "").trim();
  if (!key) {
    return undefined;
  }

  if (Object.prototype.hasOwnProperty.call(values, key)) {
    return values[key];
  }

  if (!key.includes(".")) {
    return values[key];
  }

  return key.split(".").reduce<unknown>((current, part) => {
    if (current === undefined || current === null || typeof current !== "object") {
      return undefined;
    }

    return (current as Record<string, unknown>)[part];
  }, values);
}

export class DataTransferJob {
  private readonly logger: Logger;
  private readonly sourceAdapter: SourceAdapter;
  private readonly targetAdapter: TargetAdapter;
  private readonly mappingDefinitionParser: MappingDefinitionParser;
  private readonly mappingDefinitionEngine: MappingDefinitionEngine;
  private readonly bulkLookupResolver?: BulkLookupResolverFn;

  public constructor(
    logger: Logger,
    sourceAdapter: SourceAdapter,
    targetAdapter: TargetAdapter,
    lookupResolver?: LookupResolverFn,
    bulkLookupResolver?: BulkLookupResolverFn
  ) {
    this.logger = logger;
    this.sourceAdapter = sourceAdapter;
    this.targetAdapter = targetAdapter;
    this.mappingDefinitionParser = new MappingDefinitionParser();
    this.mappingDefinitionEngine = new MappingDefinitionEngine(lookupResolver);
    this.bulkLookupResolver = bulkLookupResolver;
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

    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        phase: "source-read"
      },
      "Starting source read phase"
    );

    const sourceRecords = await this.sourceAdapter.readRecords(context);
    await context.onProgress?.({
      phase: "source-read",
      processedRecords: 0,
      totalRecords: sourceRecords.length
    });

    this.logger.info(
      {
        runId: context.runId,
        recordsRead: sourceRecords.length
      },
      "Source records loaded"
    );

    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        phase: "mapping",
        recordsRead: sourceRecords.length
      },
      "Starting record mapping phase"
    );

    const parsedDefinition = this.mappingDefinitionParser.parse(mappingDefinition);
    const lookupLines = parsedDefinition.lines.filter((line) => line.transform.type === "LOOKUP");

    if (lookupLines.length > 0 && sourceRecords.length > 0 && this.bulkLookupResolver) {
      const lookupRequestMap = new Map<string, { objectName: string; field: string; values: Set<string> }>();

      for (const line of lookupLines) {
        const transform = line.transform;
        const objectName = String(transform.lookupObject || "").trim();
        const field = String(transform.lookupField || "").trim();
        if (!objectName || !field) {
          continue;
        }

        const requestKey = `${objectName}|${field}`;
        const existing = lookupRequestMap.get(requestKey) || {
          objectName,
          field,
          values: new Set<string>()
        };

        for (const sourceRecord of sourceRecords) {
          const rawValue = readSourceValue(sourceRecord?.values || {}, line.sourceField);
          if (rawValue === undefined || rawValue === null || rawValue === "") {
            continue;
          }

          existing.values.add(String(rawValue));
        }

        lookupRequestMap.set(requestKey, existing);
      }

      const lookupRequests = Array.from(lookupRequestMap.values())
        .map((entry) => ({
          objectName: entry.objectName,
          field: entry.field,
          values: Array.from(entry.values)
        }))
        .filter((entry) => entry.values.length > 0);

      if (lookupRequests.length > 0) {
        const preloadedLookupCache = await this.bulkLookupResolver(lookupRequests);
        this.mappingDefinitionEngine.seedLookupCache(preloadedLookupCache);

        this.logger.info(
          {
            runId: context.runId,
            scheduleId: context.scheduleId,
            lookupGroups: lookupRequests.length,
            lookupValues: lookupRequests.reduce((sum, item) => sum + item.values.length, 0)
          },
          "Lookup cache preloaded for mapping"
        );
      }
    }

    const chunkSize = Math.max(1, Math.trunc(Number(context.batchSize || 100)));
    const results: ConnectorResult[] = [];
    const successfulSourceRecords: GenericRecord[] = [];
    const failedRecords: FailedJobRecord[] = [];

    for (let index = 0; index < sourceRecords.length; index += chunkSize) {
      const sourceChunk = sourceRecords.slice(index, index + chunkSize);
      const mappedChunk: GenericRecord[] = await Promise.all(
        sourceChunk.map(async (record) => {
          const mapped = await this.mappingDefinitionEngine.mapRecord(record.values, parsedDefinition.lines);
          return { values: mapped.values };
        })
      );

      this.logger.info(
        {
          runId: context.runId,
          scheduleId: context.scheduleId,
          batchStart: index,
          batchSize: sourceChunk.length
        },
        "Writing batch"
      );

      const batchResults = await this.targetAdapter.writeRecords(mappedChunk, context);
      results.push(...batchResults);
      await context.onProgress?.({
        phase: "batch-written",
        processedRecords: Math.min(index + sourceChunk.length, sourceRecords.length),
        totalRecords: sourceRecords.length,
        batchStart: index,
        batchSize: sourceChunk.length
      });

      for (let batchIndex = 0; batchIndex < batchResults.length; batchIndex++) {
        const result = batchResults[batchIndex];
        const sourceRecord = sourceChunk[batchIndex];
        const mappedRecord = mappedChunk[batchIndex];
        if (!result) {
          continue;
        }

        if (result.success) {
          successfulSourceRecords.push(sourceRecord);
        } else {
          failedRecords.push({
            rowIndex: index + batchIndex,
            externalKey: result.externalKey,
            statusCode: result.statusCode,
            message: result.message,
            retryable: result.retryable,
            sourceRecord: sourceRecord?.values,
            mappedRecord: mappedRecord?.values
          });
        }
      }
    }

    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        phase: "target-write",
        mappedRecords: sourceRecords.length,
        chunkSize
      },
      "Target write phase completed in batches"
    );

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

    const lastCheckpointRecord = [...successfulSourceRecords]
      .reverse()
      .find((record) => record.checkpoint && record.checkpoint.value);

    return {
      recordsRead: sourceRecords.length,
      recordsProcessed: results.length,
      recordsSucceeded: successCount,
      recordsFailed: errorCount,
      status,
      connectorResults: results,
      lastProcessedRecord: lastCheckpointRecord?.checkpoint,
      successfulSourceRecords,
      failedRecords
    };
  }
}
