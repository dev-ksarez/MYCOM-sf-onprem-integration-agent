import "dotenv/config";
import pino from "pino";
import { SalesforceClient } from "./clients/salesforce/salesforce-client";
import { ConnectorRegistry } from "./core/connector-registry/connector-registry";
import { DataTransferJob } from "./core/job-runner/data-transfer-job";
import { AccountExportJob } from "./core/job-runner/account-export-job";
import { isScheduleDue } from "./core/scheduler/is-schedule-due";
import { getSalesforceConfig } from "./infrastructure/config/salesforce-config";
import { MssqlConnector } from "./connectors/mssql/mssql-connector";
import { SalesforceAccountSource } from "./source/salesforce/salesforce-account-source";
import { SalesforceSoqlSourceAdapter } from "./source-adapters/salesforce/salesforce-soql-source-adapter";
import { MssqlQuerySourceAdapter } from "./source-adapters/mssql/mssql-query-source-adapter";
import { SalesforceScheduleSource } from "./source/salesforce/salesforce-schedule-source";
import { MssqlTargetAdapter } from "./target-adapters/mssql/mssql-target-adapter";
import { SalesforceTargetAdapter } from "./target-adapters/salesforce/salesforce-target-adapter";
import { JobContext } from "./types/job-context";
import { TransferContext } from "./types/transfer-context";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const agentId = process.env.AGENT_ID || "local-agent-01";

async function main(): Promise<void> {
  const salesforceConfig = getSalesforceConfig();
  const salesforceClient = new SalesforceClient(salesforceConfig);
  await salesforceClient.login();

  logger.info("Salesforce login successful");

  const scheduleSource = new SalesforceScheduleSource(salesforceClient);
  const schedules = await scheduleSource.getActiveSchedules();

  logger.info({ schedulesFound: schedules.length }, "Active schedules loaded");

  const dueSchedules = schedules.filter(isScheduleDue);

  logger.info({ dueSchedules: dueSchedules.length }, "Due schedules identified");

  for (const schedule of dueSchedules) {
    const isGenericSalesforceToMssql =
      schedule.sourceType === "SALESFORCE_SOQL" && schedule.targetType === "MSSQL";
    const isGenericMssqlToSalesforce =
      schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE";

    if (!isGenericSalesforceToMssql && !isGenericMssqlToSalesforce && schedule.objectName !== "Account") {
      logger.info(
        { scheduleId: schedule.id, objectName: schedule.objectName },
        "Skipping schedule because object is not supported yet"
      );
      continue;
    }

    if (!schedule.connectorId) {
      throw new Error(`Schedule ${schedule.name} is missing MSD_Connector__c`);
    }

    const registry = new ConnectorRegistry();
    const connectorConfig = await salesforceClient.queryConnector(schedule.connectorId);
    const connector = registry.getConnectorByConfig(connectorConfig);

    const context: JobContext = {
      runId: `RUN-${Date.now()}`,
      correlationId: `CORR-${Date.now()}`,
      scheduleId: schedule.id,
      targetSystem: schedule.targetSystem,
      batchSize: schedule.batchSize || 100,
      maxRetries: 3
    };

    const hasRunningRun = await salesforceClient.hasRunningRunForSchedule(schedule.id);
    if (hasRunningRun) {
      logger.info(
        {
          scheduleId: schedule.id,
          scheduleName: schedule.name
        },
        "Skipping schedule because a previous run is still running"
      );
      continue;
    }

    const runId = await salesforceClient.createRun({
      scheduleId: schedule.id,
      correlationId: context.correlationId,
      agentId,
      startedAt: new Date().toISOString()
    });

    await salesforceClient.createLog({
      runId,
      level: "INFO",
      step: "RUN_START",
      message: `Run started for schedule ${schedule.name}`,
      correlationId: context.correlationId
    });

    logger.info(
      {
        scheduleId: schedule.id,
        connectorId: connectorConfig.id,
        connectorName: connectorConfig.name,
        connectorType: connectorConfig.connectorType
      },
      "Connector configuration loaded"
    );

    const checkpoint = await salesforceClient.getCheckpoint(schedule.id, schedule.objectName);
    const lastCheckpoint = checkpoint?.lastCheckpoint;
    const lastRecordId = checkpoint?.lastRecordId;
    await salesforceClient.createLog({
      runId,
      level: "INFO",
      step: "CHECKPOINT_LOADED",
      message: lastCheckpoint
        ? `Using checkpoint ${lastCheckpoint}${lastRecordId ? ` / ${lastRecordId}` : ""}`
        : "No checkpoint found. Running initial load.",
      correlationId: context.correlationId
    });

    try {
      const connectionOk = await connector.testConnection();
      if (!connectionOk) {
        throw new Error(`Connection test failed for target system: ${schedule.targetSystem}`);
      }

      let result;

      if (isGenericSalesforceToMssql) {
        if (!schedule.sourceDefinition?.trim()) {
          throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
        }

        if (!schedule.mappingDefinition?.trim()) {
          throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
        }

        if (!(connector instanceof MssqlConnector)) {
          throw new Error(`Connector type ${connectorConfig.connectorType} is not supported by MssqlTargetAdapter`);
        }

        const transferContext: TransferContext = {
          runId: context.runId,
          correlationId: context.correlationId,
          scheduleId: context.scheduleId,
          direction: schedule.direction || "Outbound",
          sourceType: schedule.sourceType || "SALESFORCE_SOQL",
          targetType: schedule.targetType || "MSSQL",
          batchSize: context.batchSize,
          maxRetries: context.maxRetries
        };

        const sourceAdapter = new SalesforceSoqlSourceAdapter(
          salesforceClient,
          schedule.sourceDefinition
        );
        const targetAdapter = new MssqlTargetAdapter(connector);
        const job = new DataTransferJob(logger, sourceAdapter, targetAdapter);

        result = await job.execute(transferContext, schedule.mappingDefinition);
      } else if (isGenericMssqlToSalesforce) {
        if (!schedule.sourceDefinition?.trim()) {
          throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
        }

        if (!schedule.mappingDefinition?.trim()) {
          throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
        }

        if (!schedule.targetDefinition?.trim()) {
          throw new Error(`Schedule ${schedule.name} is missing MSD_TargetDefinition__c`);
        }

        const transferContext: TransferContext = {
          runId: context.runId,
          correlationId: context.correlationId,
          scheduleId: context.scheduleId,
          direction: schedule.direction || "Inbound",
          sourceType: schedule.sourceType || "MSSQL_SQL",
          targetType: schedule.targetType || "SALESFORCE",
          batchSize: context.batchSize,
          maxRetries: context.maxRetries
        };

        const sourceAdapter = new MssqlQuerySourceAdapter(
          connectorConfig,
          schedule.sourceDefinition
        );
        const targetAdapter = new SalesforceTargetAdapter(
          salesforceClient,
          schedule.targetDefinition,
          connectorConfig
        );

        if (!targetAdapter.isProfileSchedulerDue()) {
          logger.info(
            {
              scheduleId: schedule.id,
              profileName: targetAdapter.getActiveProfileName()
            },
            "Skipping schedule because selected import profile scheduler is not active/due"
          );
          continue;
        }

        const job = new DataTransferJob(logger, sourceAdapter, targetAdapter);

        result = await job.execute(transferContext, schedule.mappingDefinition);
      } else {
        const source = new SalesforceAccountSource(salesforceClient);
        const job = new AccountExportJob(logger, source, connector);

        result = await job.execute(
          context,
          lastCheckpoint,
          lastRecordId,
          schedule.mappingDefinition
        );
      }

      for (const connectorResult of result.connectorResults) {
        if (connectorResult.success) {
          continue;
        }

        const statusCode = connectorResult.statusCode || "UNKNOWN_STATUS";
        const message = connectorResult.message || "Unknown connector error";
        const retryableText = connectorResult.retryable ? "retryable=true" : "retryable=false";

        await salesforceClient.createLog({
          runId,
          level: "ERROR",
          step: "RECORD_ERROR",
          message: `${statusCode}: ${message} (${retryableText})`,
          correlationId: context.correlationId,
          recordKey: connectorResult.externalKey
        });
      }
      await salesforceClient.createLog({
        runId,
        level: "INFO",
        step: "RUN_FINISHED",
        message: `Run finished with status ${result.status}`,
        correlationId: context.correlationId
      });

      await salesforceClient.updateRun(runId, {
        status: result.status,
        finishedAt: new Date().toISOString(),
        recordsRead: result.recordsRead,
        recordsProcessed: result.recordsProcessed,
        recordsSucceeded: result.recordsSucceeded,
        recordsFailed: result.recordsFailed
      });

      if (result.lastProcessedRecord) {
        await salesforceClient.upsertCheckpoint({
          checkpointId: checkpoint?.id,
          scheduleId: schedule.id,
          objectName: schedule.objectName,
          lastCheckpoint: result.lastProcessedRecord.lastModified,
          lastRecordId: result.lastProcessedRecord.sourceId,
          lastRunId: runId
        });

        await salesforceClient.createLog({
          runId,
          level: "INFO",
          step: "CHECKPOINT_SAVED",
          message: `Checkpoint updated to ${result.lastProcessedRecord.lastModified} / ${result.lastProcessedRecord.sourceId}`,
          correlationId: context.correlationId
        });
      } else {
        await salesforceClient.createLog({
          runId,
          level: "INFO",
          step: "CHECKPOINT_SKIPPED",
          message: "No records processed. Checkpoint unchanged.",
          correlationId: context.correlationId
        });
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      if (errorMessage.startsWith("MSSQL_CONNECTION_FAILED:")) {
        await salesforceClient.createLog({
          runId,
          level: "ERROR",
          step: "CONNECTOR_CONNECTION_FAILED",
          message: errorMessage,
          correlationId: context.correlationId
        });
      }

      await salesforceClient.createLog({
        runId,
        level: "ERROR",
        step: "RUN_FAILED",
        message: errorMessage,
        correlationId: context.correlationId
      });

      await salesforceClient.updateRun(runId, {
        status: "Failed",
        finishedAt: new Date().toISOString(),
        errorMessage
      });

      throw error;
    }
  }

  if (dueSchedules.length === 0) {
    logger.info("No due schedules found");
  }
}

main().catch((error) => {
  logger.error({ err: error }, "Application failed");
  process.exit(1);
});