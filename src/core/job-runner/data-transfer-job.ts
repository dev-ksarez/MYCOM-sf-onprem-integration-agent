

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

    const parsedDefinition = this.mappingDefinitionParser.parse(mappingDefinition);
    const lookupLines = parsedDefinition.lines.filter((line) => line.transform.type === "LOOKUP");
    const canReadSourceCountWithoutExtraRequest = !this.sourceAdapter.readRecordStream;
    const needsLookupPreload = lookupLines.length > 0 && Boolean(this.bulkLookupResolver);
    const bufferedSourceRecords = needsLookupPreload ? await this.readAllSourceRecords(context) : undefined;
    const sourceRecords = bufferedSourceRecords
      || (canReadSourceCountWithoutExtraRequest ? await this.readAllSourceRecords(context) : undefined);
    const totalRecords = sourceRecords?.length;

    await context.onProgress?.({
      phase: "source-read",
      processedRecords: 0,
      totalRecords
    });

    if (sourceRecords) {
      this.logger.info(
        {
          runId: context.runId,
          recordsRead: sourceRecords.length
        },
        "Source records loaded"
      );
    }

    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        phase: "mapping",
        recordsRead: totalRecords
      },
      "Starting record mapping phase"
    );

    if (lookupLines.length > 0 && bufferedSourceRecords && bufferedSourceRecords.length > 0 && this.bulkLookupResolver) {
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

        for (const sourceRecord of bufferedSourceRecords) {
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
    let recordsRead = 0;
    let processedRecords = 0;
    let lastProcessedRecord: GenericRecord["checkpoint"] | undefined;
    let checkpointBlockedByFailure = false;

    const processChunk = async (sourceChunk: GenericRecord[], batchStart: number): Promise<void> => {
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
          batchStart,
          batchSize: sourceChunk.length
        },
        "Writing batch"
      );

      const batchResults = await this.targetAdapter.writeRecords(mappedChunk, context);
      results.push(...batchResults);
      processedRecords += sourceChunk.length;
      await context.onProgress?.({
        phase: "batch-written",
        processedRecords,
        totalRecords,
        batchStart,
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
          if (!checkpointBlockedByFailure && sourceRecord?.checkpoint?.value) {
            lastProcessedRecord = sourceRecord.checkpoint;
          }
        } else {
          checkpointBlockedByFailure = true;
          failedRecords.push({
            rowIndex: batchStart + batchIndex,
            externalKey: result.externalKey,
            statusCode: result.statusCode,
            message: result.message,
            retryable: result.retryable,
            sourceRecord: sourceRecord?.values,
            mappedRecord: mappedRecord?.values
          });
        }
      }
    };

    let sourceChunk: GenericRecord[] = [];
    const sourceIterable = sourceRecords || this.readSourceRecordStream(context);
    for await (const sourceRecord of sourceIterable) {
      recordsRead += 1;
      sourceChunk.push(sourceRecord);
      if (sourceChunk.length >= chunkSize) {
        const batchStart = recordsRead - sourceChunk.length;
        await processChunk(sourceChunk, batchStart);
        sourceChunk = [];
      }
    }

    if (sourceChunk.length > 0) {
      const batchStart = recordsRead - sourceChunk.length;
      await processChunk(sourceChunk, batchStart);
    }

    this.logger.info(
      {
        runId: context.runId,
        scheduleId: context.scheduleId,
        phase: "target-write",
        mappedRecords: recordsRead,
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

    return {
      recordsRead,
      recordsProcessed: results.length,
      recordsSucceeded: successCount,
      recordsFailed: errorCount,
      status,
      connectorResults: results,
      lastProcessedRecord,
      successfulSourceRecords,
      failedRecords
    };
  }

  private async *readSourceRecordStream(context: TransferContext): AsyncIterable<GenericRecord> {
    if (this.sourceAdapter.readRecordStream) {
      yield* this.sourceAdapter.readRecordStream(context);
      return;
    }

    const records = await this.sourceAdapter.readRecords(context);
    for (const record of records) {
      yield record;
    }
  }

  private async readAllSourceRecords(context: TransferContext): Promise<GenericRecord[]> {
    const records: GenericRecord[] = [];
    for await (const record of this.readSourceRecordStream(context)) {
      records.push(record);
    }
    return records;
  }
}
