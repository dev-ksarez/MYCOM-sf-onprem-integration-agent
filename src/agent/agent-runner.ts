import pino from "pino";
import { SalesforceClient, SalesforceScheduleRecord } from "../clients/salesforce/salesforce-client";
import { ConnectorRegistry } from "../core/connector-registry/connector-registry";
import { DataTransferJob } from "../core/job-runner/data-transfer-job";
import { LookupResolverFn } from "../core/mapping-dsl/mapping-definition-engine";
import { AccountExportJob } from "../core/job-runner/account-export-job";
import { isScheduleDue } from "../core/scheduler/is-schedule-due";
import { getSalesforceConfig } from "../infrastructure/config/salesforce-config";
import { MssqlConnector } from "../connectors/mssql/mssql-connector";
import { SalesforceAccountSource } from "../source/salesforce/salesforce-account-source";
import { SalesforceSoqlSourceAdapter } from "../source-adapters/salesforce/salesforce-soql-source-adapter";
import { MssqlQuerySourceAdapter } from "../source-adapters/mssql/mssql-query-source-adapter";
import { SalesforceScheduleSource } from "../source/salesforce/salesforce-schedule-source";
import { MssqlTargetAdapter } from "../target-adapters/mssql/mssql-target-adapter";
import { SalesforceTargetAdapter } from "../target-adapters/salesforce/salesforce-target-adapter";
import { SalesforceGlobalPicklistTargetAdapter } from "../target-adapters/salesforce/salesforce-global-picklist-target-adapter";
import { JobContext } from "../types/job-context";
import { TransferContext } from "../types/transfer-context";
import { IntegrationSchedule } from "../types/integration-schedule";
import { SalesforceConfig } from "../infrastructure/config/salesforce-config";

export interface AgentRunSummary {
  schedulesFound: number;
  dueSchedules: number;
  processedSchedules: number;
}

export interface ManualRunResult {
  scheduleId: string;
  scheduleName: string;
  triggered: boolean;
  message: string;
}

function extractHierarchySettings(targetDefinition?: string): {
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
} {
  const raw = String(targetDefinition || "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const candidate = parsed as Record<string, unknown>;
    const parentScheduleId =
      typeof candidate.parentScheduleId === "string" && candidate.parentScheduleId.trim()
        ? candidate.parentScheduleId.trim()
        : undefined;

    return {
      parentScheduleId,
      inheritTimingFromParent: candidate.inheritTimingFromParent === true
    };
  } catch {
    return {};
  }
}

function extractTimingDefinition(targetDefinition?: string): string | undefined {
  const raw = String(targetDefinition || "").trim();
  if (!raw) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return undefined;
    }

    const timingDefinition = (parsed as Record<string, unknown>).timingDefinition;
    if (typeof timingDefinition !== "string" || !timingDefinition.trim()) {
      return undefined;
    }

    return timingDefinition.trim();
  } catch {
    return undefined;
  }
}

function calculateNextRunAtFromTiming(timingDefinition?: string, now: Date = new Date()): string | undefined {
  const raw = String(timingDefinition || "").trim();
  if (!raw) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return undefined;
    }

    const intervalMinutes = Number((parsed as Record<string, unknown>).intervalMinutes || 0);
    
    // Für Intervall-basiertes Timing (z.B. alle 5 Minuten): nächster Lauf = jetzt + intervalMinutes
    if (Number.isInteger(intervalMinutes) && intervalMinutes > 0 && intervalMinutes < 1440) {
      const nextRun = new Date(now);
      nextRun.setMinutes(nextRun.getMinutes() + intervalMinutes);
      return nextRun.toISOString();
    }

    // Für Zeit-basiertes Timing (Wochentag + Uhrzeit): nächsten passenden Tag um startTime berechnen
    const days = Array.isArray((parsed as Record<string, unknown>).days)
      ? ((parsed as Record<string, unknown>).days as unknown[])
          .map((value) => Number(value))
          .filter((value) => Number.isInteger(value) && value >= 0 && value <= 6)
      : [];

    const uniqueDays = Array.from(new Set(days));
    if (uniqueDays.length === 0) {
      return undefined;
    }

    const startTime = String((parsed as Record<string, unknown>).startTime || "09:00");
    const match = startTime.match(/^(\d{1,2}):(\d{2})$/);
    if (!match) {
      return undefined;
    }

    const hours = Number(match[1]);
    const minutes = Number(match[2]);
    if (!Number.isInteger(hours) || !Number.isInteger(minutes) || hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
      return undefined;
    }

    for (let offset = 0; offset <= 60; offset += 1) {
      const candidate = new Date(now);
      candidate.setDate(now.getDate() + offset);
      candidate.setHours(hours, minutes, 0, 0);

      if (candidate.getTime() <= now.getTime()) {
        continue;
      }

      if (uniqueDays.includes(candidate.getDay())) {
        return candidate.toISOString();
      }
    }

    return undefined;
  } catch {
    return undefined;
  }
}

function buildHierarchyOrderedSchedules(schedules: IntegrationSchedule[]): IntegrationSchedule[] {
  const byId = new Map(schedules.map((schedule) => [schedule.id, schedule]));
  const childrenByParent = new Map<string, IntegrationSchedule[]>();
  const roots: IntegrationSchedule[] = [];

  for (const schedule of schedules) {
    const parentId = schedule.parentScheduleId;
    if (parentId && byId.has(parentId) && parentId !== schedule.id) {
      const children = childrenByParent.get(parentId) || [];
      children.push(schedule);
      childrenByParent.set(parentId, children);
      continue;
    }
    roots.push(schedule);
  }

  const ordered: IntegrationSchedule[] = [];
  const visited = new Set<string>();

  const visit = (schedule: IntegrationSchedule, trail: Set<string>) => {
    if (visited.has(schedule.id)) {
      return;
    }
    if (trail.has(schedule.id)) {
      return;
    }

    trail.add(schedule.id);
    visited.add(schedule.id);
    ordered.push(schedule);

    const children = (childrenByParent.get(schedule.id) || []).sort((a, b) =>
      a.name.localeCompare(b.name, "de", { sensitivity: "base" })
    );

    for (const child of children) {
      visit(child, trail);
    }

    trail.delete(schedule.id);
  };

  for (const root of roots.sort((a, b) => a.name.localeCompare(b.name, "de", { sensitivity: "base" }))) {
    visit(root, new Set<string>());
  }

  for (const schedule of schedules) {
    if (!visited.has(schedule.id)) {
      visit(schedule, new Set<string>());
    }
  }

  return ordered;
}

function mapSchedule(record: SalesforceScheduleRecord): IntegrationSchedule {
  return {
    ...extractHierarchySettings(record.MSD_TargetDefinition__c),
    id: record.Id,
    name: record.Name,
    active: record.Active__c,
    sourceSystem: record.SourceSystem__c || "",
    targetSystem: record.TargetSystem__c || "",
    objectName: record.ObjectName__c || "",
    operation: record.Operation__c || "",
    connectorId: record.MSD_Connector__c,
    mappingDefinition: record.MSD_MappingDefinition__c,
    direction: record.MSD_Direction__c,
    sourceType: record.MSD_SourceType__c,
    targetType: record.MSD_TargetType__c,
    sourceDefinition: record.MSD_SourceDefinition__c,
    targetDefinition: record.MSD_TargetDefinition__c,
    batchSize: record.BatchSize__c || 100,
    nextRunAt: record.NextRunAt__c,
    lastRunAt: record.LastRunAt__c,
    timingDefinition: extractTimingDefinition(record.MSD_TargetDefinition__c)
  };
}

async function executeSchedule(
  salesforceClient: SalesforceClient,
  logger: pino.Logger,
  agentId: string,
  schedule: IntegrationSchedule,
  options?: { forceRun?: boolean }
): Promise<boolean> {
  const forceRun = options?.forceRun ?? false;
  const isGenericSalesforceToMssql =
    schedule.sourceType === "SALESFORCE_SOQL" && schedule.targetType === "MSSQL";
  const isGenericMssqlToSalesforce =
    schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE";
  const isGenericMssqlToGlobalPicklist =
    schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";

  if (
    !isGenericSalesforceToMssql &&
    !isGenericMssqlToSalesforce &&
    !isGenericMssqlToGlobalPicklist &&
    schedule.objectName !== "Account"
  ) {
    logger.info(
      { scheduleId: schedule.id, objectName: schedule.objectName },
      "Skipping schedule because object is not supported yet"
    );
    return false;
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
      { scheduleId: schedule.id, scheduleName: schedule.name },
      "Skipping schedule because a previous run is still running"
    );
    return false;
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
    step: forceRun ? "RUN_NOW_START" : "RUN_START",
    message: forceRun
      ? `Manual run started for schedule ${schedule.name}`
      : `Run started for schedule ${schedule.name}`,
    correlationId: context.correlationId
  });

  logger.info(
    {
      scheduleId: schedule.id,
      connectorId: connectorConfig.id,
      connectorName: connectorConfig.name,
      connectorType: connectorConfig.connectorType,
      forceRun
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

      const sourceAdapter = new SalesforceSoqlSourceAdapter(salesforceClient, schedule.sourceDefinition);
      const targetAdapter = new MssqlTargetAdapter(connector);
      const job = new DataTransferJob(logger, sourceAdapter, targetAdapter);
      result = await job.execute(transferContext, schedule.mappingDefinition);
    } else if (isGenericMssqlToSalesforce || isGenericMssqlToGlobalPicklist) {
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
        targetType:
          schedule.targetType ||
          (isGenericMssqlToGlobalPicklist ? "SALESFORCE_GLOBAL_PICKLIST" : "SALESFORCE"),
        batchSize: context.batchSize,
        maxRetries: context.maxRetries
      };

      const sourceAdapter = new MssqlQuerySourceAdapter(connectorConfig, schedule.sourceDefinition);
      const targetAdapter = isGenericMssqlToGlobalPicklist
        ? new SalesforceGlobalPicklistTargetAdapter(salesforceClient, schedule.targetDefinition)
        : new SalesforceTargetAdapter(salesforceClient, schedule.targetDefinition, connectorConfig);

      const salesforceLookupResolver: LookupResolverFn = async (objectName, field, value) => {
        const escapedValue = String(value).replace(/'/g, "\\'");
        const soql = `SELECT Id FROM ${objectName} WHERE ${field} = '${escapedValue}' LIMIT 1`;
        const records = await salesforceClient.queryGeneric(soql);
        return records.length > 0 ? String(records[0].Id) : null;
      };

      if (!forceRun && !targetAdapter.isProfileSchedulerDue()) {
        logger.info(
          { scheduleId: schedule.id, profileName: targetAdapter.getActiveProfileName() },
          "Skipping schedule because selected import profile scheduler is not active/due"
        );

        await salesforceClient.createLog({
          runId,
          level: "INFO",
          step: "RUN_SKIPPED",
          message: "Run skipped because selected import profile scheduler is not active/due",
          correlationId: context.correlationId
        });

        await salesforceClient.updateRun(runId, {
          status: "Success",
          finishedAt: new Date().toISOString(),
          recordsRead: 0,
          recordsProcessed: 0,
          recordsSucceeded: 0,
          recordsFailed: 0
        });

        return false;
      }

      const job = new DataTransferJob(logger, sourceAdapter, targetAdapter, salesforceLookupResolver);
      result = await job.execute(transferContext, schedule.mappingDefinition);
    } else {
      const source = new SalesforceAccountSource(salesforceClient);
      const job = new AccountExportJob(logger, source, connector);
      result = await job.execute(context, lastCheckpoint, lastRecordId, schedule.mappingDefinition);
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
      step: forceRun ? "RUN_NOW_FINISHED" : "RUN_FINISHED",
      message: `Run finished with status ${result.status}`,
      correlationId: context.correlationId
    });

    const finishedAt = new Date().toISOString();

    await salesforceClient.updateRun(runId, {
      status: result.status,
      finishedAt,
      recordsRead: result.recordsRead,
      recordsProcessed: result.recordsProcessed,
      recordsSucceeded: result.recordsSucceeded,
      recordsFailed: result.recordsFailed
    });

    const scheduleFields: Record<string, unknown> = {
      LastRunAt__c: finishedAt
    };

    if (!schedule.inheritTimingFromParent) {
      const calculatedNextRunAt = calculateNextRunAtFromTiming(schedule.timingDefinition || extractTimingDefinition(schedule.targetDefinition), new Date(finishedAt));
      if (calculatedNextRunAt) {
        scheduleFields.NextRunAt__c = calculatedNextRunAt;
      }
    }

    await salesforceClient.updateScheduleRecord(schedule.id, scheduleFields);

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

    return true;
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
      step: forceRun ? "RUN_NOW_FAILED" : "RUN_FAILED",
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

export async function runDueSchedulesOnce(logger: pino.Logger, agentId: string): Promise<AgentRunSummary> {
  const salesforceConfig = getSalesforceConfig();
  const salesforceClient = new SalesforceClient(salesforceConfig);
  await salesforceClient.login();

  logger.info("Salesforce login successful");

  const scheduleSource = new SalesforceScheduleSource(salesforceClient);
  const schedules = await scheduleSource.getActiveSchedules();

  logger.info({ schedulesFound: schedules.length }, "Active schedules loaded");

  const orderedSchedules = buildHierarchyOrderedSchedules(schedules);
  const dueState = new Map<string, boolean>();
  const executedState = new Map<string, boolean>();

  let dueSchedules = 0;
  let processedSchedules = 0;

  for (const schedule of orderedSchedules) {
    const parentId = schedule.parentScheduleId;
    const hasValidParent = Boolean(parentId && orderedSchedules.some((entry) => entry.id === parentId));
    const ownDue = isScheduleDue(schedule);

    const inheritedDue =
      schedule.inheritTimingFromParent && hasValidParent && parentId
        ? Boolean(dueState.get(parentId))
        : ownDue;

    dueState.set(schedule.id, inheritedDue);

    if (!inheritedDue) {
      continue;
    }

    dueSchedules += 1;

    if (schedule.inheritTimingFromParent && hasValidParent && parentId && !executedState.get(parentId)) {
      logger.info(
        {
          scheduleId: schedule.id,
          parentScheduleId: parentId
        },
        "Skipping child schedule because parent schedule did not run successfully"
      );
      executedState.set(schedule.id, false);
      continue;
    }

    try {
      const processed = await executeSchedule(salesforceClient, logger, agentId, schedule);
      executedState.set(schedule.id, processed);
      if (processed) {
        processedSchedules += 1;
      }
    } catch (scheduleError) {
      const message = scheduleError instanceof Error ? scheduleError.message : "Unknown error";
      logger.error(
        { scheduleId: schedule.id, scheduleName: schedule.name, err: scheduleError },
        `Schedule execution failed and was skipped: ${message}`
      );
      executedState.set(schedule.id, false);
    }
  }

  logger.info({ dueSchedules }, "Due schedules identified");

  if (dueSchedules === 0) {
    logger.info("No due schedules found");
  }

  return {
    schedulesFound: schedules.length,
    dueSchedules,
    processedSchedules
  };
}

export async function runScheduleNow(
  logger: pino.Logger,
  agentId: string,
  scheduleId: string,
  salesforceConfigOverride?: SalesforceConfig
): Promise<ManualRunResult> {
  const salesforceClient = new SalesforceClient(salesforceConfigOverride || getSalesforceConfig());
  await salesforceClient.login();

  const record = await salesforceClient.queryScheduleById(scheduleId);
  const schedule = mapSchedule(record);
  const triggered = await executeSchedule(salesforceClient, logger, agentId, schedule, { forceRun: true });

  return {
    scheduleId: schedule.id,
    scheduleName: schedule.name,
    triggered,
    message: triggered
      ? `Manual run started for ${schedule.name}`
      : `Schedule ${schedule.name} was skipped because another run is already active or prerequisites were not met`
  };
}
