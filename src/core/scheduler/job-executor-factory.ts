import pino from "pino";
import { SalesforceClient, ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { IntegrationSchedule } from "../../types/integration-schedule";
import { JobContext } from "../../types/job-context";
import { TransferContext } from "../../types/transfer-context";
import { JobExecutionResult } from "../../types/job-execution-result";
import { isFileScheduleType } from "../../types/file-schedule-type";
import { BulkLookupResolverFn, DataTransferJob } from "../job-runner/data-transfer-job";
import { LookupResolverFn } from "../mapping-dsl/mapping-definition-engine";
import { AccountExportJob } from "../job-runner/account-export-job";
import { SalesforceAccountSource } from "../../source/salesforce/salesforce-account-source";
import { SalesforceSoqlSourceAdapter } from "../../source-adapters/salesforce/salesforce-soql-source-adapter";
import { MssqlQuerySourceAdapter } from "../../source-adapters/mssql/mssql-query-source-adapter";
import { FileMakerSourceAdapter } from "../../source-adapters/filemaker/filemaker-source-adapter";
import { RestApiSourceAdapter } from "../../source-adapters/rest/rest-api-source-adapter";
import { EndpointSourceAdapter } from "../../source-adapters/endpoint/endpoint-source-adapter";
import { MssqlTargetAdapter } from "../../target-adapters/mssql/mssql-target-adapter";
import { FileSourceAdapter } from "../../source-adapters/file/file-source-adapter";
import { FileTargetAdapter } from "../../target-adapters/file/file-target-adapter";
import { SalesforceTargetAdapter } from "../../target-adapters/salesforce/salesforce-target-adapter";
import { SalesforceGlobalPicklistTargetAdapter } from "../../target-adapters/salesforce/salesforce-global-picklist-target-adapter";
import { MssqlConnector } from "../../connectors/mssql/mssql-connector";
import { GenericRecord } from "../../types/generic-record";

function isValidSalesforceIdentifier(value: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_.]*$/.test(value);
}

export interface JobExecutionOptions {
  salesforceClient: SalesforceClient;
  logger: pino.Logger;
  schedule: IntegrationSchedule;
  context: JobContext;
  connectorConfig: ConnectorConfig;
  connector: any;
  lastCheckpoint?: string;
  lastRecordId?: string;
  endpointRecords?: GenericRecord[];
  onProgress: TransferContext["onProgress"];
}

export interface JobExecutor {
  canExecute(schedule: IntegrationSchedule): boolean;
  execute(options: JobExecutionOptions): Promise<JobExecutionResult>;
}

export class SalesforceToTargetJobExecutor implements JobExecutor {
  public canExecute(schedule: IntegrationSchedule): boolean {
    const isFileTarget = isFileScheduleType(schedule.targetType);
    const isGenericSalesforceToMssql =
      schedule.sourceType === "SALESFORCE_SOQL" && schedule.targetType === "MSSQL";
    const isGenericSalesforceToFile = schedule.sourceType === "SALESFORCE_SOQL" && isFileTarget;

    return isGenericSalesforceToMssql || isGenericSalesforceToFile;
  }

  public async execute(options: JobExecutionOptions): Promise<JobExecutionResult> {
    const {
      salesforceClient,
      logger,
      schedule,
      context,
      connectorConfig,
      connector,
      lastCheckpoint,
      lastRecordId,
      onProgress,
    } = options;

    if (!schedule.sourceDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
    }

    if (!schedule.mappingDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
    }

    const isFileTarget = isFileScheduleType(schedule.targetType);
    const isGenericSalesforceToMssql =
      schedule.sourceType === "SALESFORCE_SOQL" && schedule.targetType === "MSSQL";
    const isGenericSalesforceToFile = schedule.sourceType === "SALESFORCE_SOQL" && isFileTarget;

    if (isGenericSalesforceToMssql && !(connector instanceof MssqlConnector)) {
      throw new Error(`Connector type ${connectorConfig.connectorType} is not supported by MssqlTargetAdapter`);
    }

    if (isGenericSalesforceToFile && !schedule.targetDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_TargetDefinition__c`);
    }

    const transferContext: TransferContext = {
      runId: context.runId,
      correlationId: context.correlationId,
      scheduleId: context.scheduleId,
      direction: schedule.direction || "Outbound",
      sourceType: schedule.sourceType || "SALESFORCE_SOQL",
      targetType: schedule.targetType || (isGenericSalesforceToFile ? "FILE_CSV" : "MSSQL"),
      batchSize: context.batchSize,
      maxRetries: context.maxRetries,
      checkpoint: {
        value: lastCheckpoint,
        recordId: lastRecordId
      },
      onProgress
    };

    const sourceAdapter = new SalesforceSoqlSourceAdapter(salesforceClient, schedule.sourceDefinition);
    const targetAdapter = isGenericSalesforceToFile
      ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition || "")
      : new MssqlTargetAdapter(connector as MssqlConnector, schedule.targetDefinition);
    const job = new DataTransferJob(logger, sourceAdapter, targetAdapter);
    return await job.execute(transferContext, schedule.mappingDefinition);
  }
}

export class SourceToSalesforceJobExecutor implements JobExecutor {
  public canExecute(schedule: IntegrationSchedule): boolean {
    const isFileSource = isFileScheduleType(schedule.sourceType);
    const isFileTarget = isFileScheduleType(schedule.targetType);
    const isRestSource = schedule.sourceType === "REST_API";
    const isFileMakerSource = schedule.sourceType === "FILEMAKER_SQL";
    const isEndpointSource = schedule.sourceType === "ENDPOINT";

    const isGenericMssqlToSalesforce =
      schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE";
    const isGenericMssqlToGlobalPicklist =
      schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericMssqlToFile = schedule.sourceType === "MSSQL_SQL" && isFileTarget;
    const isGenericRestToSalesforce = isRestSource && schedule.targetType === "SALESFORCE";
    const isGenericRestToGlobalPicklist = isRestSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericFileMakerToSalesforce = isFileMakerSource && schedule.targetType === "SALESFORCE";
    const isGenericFileMakerToGlobalPicklist = isFileMakerSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericEndpointToSalesforce = isEndpointSource && schedule.targetType === "SALESFORCE";
    const isGenericEndpointToGlobalPicklist = isEndpointSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericFileToSalesforce = isFileSource && schedule.targetType === "SALESFORCE";
    const isGenericFileToGlobalPicklist = isFileSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericFileToMssql = isFileSource && schedule.targetType === "MSSQL";

    return (
      isGenericMssqlToSalesforce ||
      isGenericMssqlToGlobalPicklist ||
      isGenericMssqlToFile ||
      isGenericRestToSalesforce ||
      isGenericRestToGlobalPicklist ||
      isGenericFileMakerToSalesforce ||
      isGenericFileMakerToGlobalPicklist ||
      isGenericEndpointToSalesforce ||
      isGenericEndpointToGlobalPicklist ||
      isGenericFileToSalesforce ||
      isGenericFileToGlobalPicklist ||
      isGenericFileToMssql
    );
  }

  public async execute(options: JobExecutionOptions): Promise<JobExecutionResult> {
    const {
      salesforceClient,
      logger,
      schedule,
      context,
      connectorConfig,
      connector,
      lastCheckpoint,
      lastRecordId,
      onProgress,
    } = options;

    if (!schedule.sourceDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
    }

    if (!schedule.mappingDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
    }

    if (!schedule.targetDefinition?.trim()) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_TargetDefinition__c`);
    }

    const isFileSource = isFileScheduleType(schedule.sourceType);
    const isFileTarget = isFileScheduleType(schedule.targetType);
    const isRestSource = schedule.sourceType === "REST_API";
    const isFileMakerSource = schedule.sourceType === "FILEMAKER_SQL";
    const isEndpointSource = schedule.sourceType === "ENDPOINT";

    const isGenericMssqlToFile = schedule.sourceType === "MSSQL_SQL" && isFileTarget;
    const isGenericFileToMssql = isFileSource && schedule.targetType === "MSSQL";
    const isGenericMssqlToGlobalPicklist = schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericRestToGlobalPicklist = isRestSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericFileMakerToGlobalPicklist = isFileMakerSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericEndpointToGlobalPicklist = isEndpointSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
    const isGenericFileToGlobalPicklist = isFileSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";

    const isFileConnector = /file|csv|excel|xlsx|json/i.test(connectorConfig.connectorType || "");

    if ((isGenericFileToMssql || isGenericMssqlToFile) && !(connector instanceof MssqlConnector) && !isFileConnector) {
      throw new Error(`Connector type ${connectorConfig.connectorType} is not supported by MssqlTargetAdapter`);
    }

    const transferContext: TransferContext = {
      runId: context.runId,
      correlationId: context.correlationId,
      scheduleId: context.scheduleId,
      direction: schedule.direction || "Inbound",
      sourceType: schedule.sourceType || (isFileSource ? "FILE_CSV" : isRestSource ? "REST_API" : "MSSQL_SQL"),
      targetType:
        schedule.targetType ||
        (isGenericMssqlToGlobalPicklist || isGenericFileToGlobalPicklist || isGenericEndpointToGlobalPicklist
          ? "SALESFORCE_GLOBAL_PICKLIST"
          : isGenericMssqlToFile
            ? "FILE_CSV"
            : isGenericFileToMssql
              ? "MSSQL"
              : "SALESFORCE"),
      batchSize: context.batchSize,
      maxRetries: context.maxRetries,
      checkpoint: {
        value: lastCheckpoint,
        recordId: lastRecordId
      },
      onProgress
    };

    const sourceAdapter = isFileSource
      ? new FileSourceAdapter(connectorConfig, schedule.sourceDefinition)
      : isEndpointSource
        ? new EndpointSourceAdapter(options.endpointRecords || [])
      : isRestSource
        ? new RestApiSourceAdapter(connectorConfig, schedule.sourceDefinition)
      : isFileMakerSource
        ? new FileMakerSourceAdapter(connectorConfig, schedule.sourceDefinition)
      : new MssqlQuerySourceAdapter(connectorConfig, schedule.sourceDefinition);

    const targetAdapter = isGenericMssqlToFile
      ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition)
      : isGenericFileToMssql
        ? new MssqlTargetAdapter(connector as MssqlConnector, schedule.targetDefinition)
        : isGenericMssqlToGlobalPicklist || isGenericRestToGlobalPicklist || isGenericFileMakerToGlobalPicklist || isGenericEndpointToGlobalPicklist || isGenericFileToGlobalPicklist
          ? new SalesforceGlobalPicklistTargetAdapter(salesforceClient, schedule.targetDefinition, schedule.lastRunAt)
          : new SalesforceTargetAdapter(salesforceClient, schedule.targetDefinition, connectorConfig, schedule.lastRunAt);

    const salesforceLookupResolver: LookupResolverFn = async (objectName, field, value) => {
      const escapedValue = String(value).replace(/'/g, "\\'");
      const soql = `SELECT Id FROM ${objectName} WHERE ${field} = '${escapedValue}' LIMIT 1`;
      try {
        logger.debug({ scheduleId: schedule.id, runId: context.runId, objectName, field, rawValue: value, soql }, "Lookup resolver: executing SOQL");
        const records = await salesforceClient.queryGeneric(soql);
        logger.debug(
          { scheduleId: schedule.id, runId: context.runId, soql, found: records.length > 0, recordsCount: records.length, sample: records[0] ?? null },
          "Lookup resolver: result"
        );
        return records.length > 0 ? String(records[0].Id) : null;
      } catch (err) {
        logger.error({ scheduleId: schedule.id, runId: context.runId, soql, error: err instanceof Error ? err.message : String(err) }, "Lookup resolver: query failed");
        return null;
      }
    };

    const salesforceBulkLookupResolver: BulkLookupResolverFn = async (requests) => {
      const cache = new Map<string, string | null>();
      const chunkSize = Math.max(1, Math.min(200, Math.trunc(context.batchSize || 100)));

      for (const request of requests) {
        const objectName = String(request.objectName || "").trim();
        const field = String(request.field || "").trim();
        if (!objectName || !field || !isValidSalesforceIdentifier(objectName) || !isValidSalesforceIdentifier(field)) {
          continue;
        }

        const values = Array.from(new Set((request.values || [])
          .map((value) => String(value ?? "").trim())
          .filter((value) => value.length > 0)));

        if (values.length === 0) {
          continue;
        }

        for (const value of values) {
          cache.set(`${objectName}|${field}|${value}`, null);
        }

        for (let index = 0; index < values.length; index += chunkSize) {
          const chunk = values.slice(index, index + chunkSize);
          const inClause = chunk.map((value) => `'${value.replace(/'/g, "\\'")}'`).join(", ");
          const soql = `SELECT Id, ${field} FROM ${objectName} WHERE ${field} IN (${inClause})`;

          try {
            const records = await salesforceClient.queryGeneric(soql);
            for (const record of records) {
              const rawLookupValue = record[field];
              if (rawLookupValue === undefined || rawLookupValue === null || rawLookupValue === "") {
                continue;
              }

              const normalizedLookupValue = String(rawLookupValue).trim();
              const id = String(record.Id || "").trim();
              if (!id) {
                continue;
              }

              cache.set(`${objectName}|${field}|${normalizedLookupValue}`, id);
            }
          } catch (err) {
            logger.error(
              {
                scheduleId: schedule.id,
                runId: context.runId,
                objectName,
                field,
                chunkStart: index,
                chunkSize: chunk.length,
                error: err instanceof Error ? err.message : String(err)
              },
              "Bulk lookup preload failed for chunk"
            );
          }
        }
      }

      return cache;
    };

    const job = new DataTransferJob(
      logger,
      sourceAdapter,
      targetAdapter,
      salesforceLookupResolver,
      salesforceBulkLookupResolver
    );
    return await job.execute(transferContext, schedule.mappingDefinition);
  }
}

export class LegacyAccountExportJobExecutor implements JobExecutor {
  public canExecute(schedule: IntegrationSchedule): boolean {
    return schedule.objectName === "Account";
  }

  public async execute(options: JobExecutionOptions): Promise<JobExecutionResult> {
    const {
      salesforceClient,
      logger,
      schedule,
      context,
      connector,
      lastCheckpoint,
      lastRecordId,
    } = options;

    if (!connector) {
      throw new Error(`LegacyAccountExportJobExecutor requires a valid Connector, but none was provided for schedule ${schedule.name}`);
    }

    const source = new SalesforceAccountSource(salesforceClient);
    const job = new AccountExportJob(logger, source, connector);
    return await job.execute(context, lastCheckpoint, lastRecordId, schedule.mappingDefinition);
  }
}

export class JobExecutorFactory {
  private readonly executors: JobExecutor[] = [
    new SalesforceToTargetJobExecutor(),
    new SourceToSalesforceJobExecutor(),
    new LegacyAccountExportJobExecutor(),
  ];

  public getExecutor(schedule: IntegrationSchedule): JobExecutor | undefined {
    return this.executors.find((executor) => executor.canExecute(schedule));
  }
}
