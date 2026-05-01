import pino from "pino";
import fs from "node:fs";
import path from "node:path";
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
import { RestApiSourceAdapter } from "../source-adapters/rest/rest-api-source-adapter";
import { SalesforceScheduleSource } from "../source/salesforce/salesforce-schedule-source";
import { MssqlTargetAdapter } from "../target-adapters/mssql/mssql-target-adapter";
import { FileSourceAdapter } from "../source-adapters/file/file-source-adapter";
import { FileTargetAdapter } from "../target-adapters/file/file-target-adapter";
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

const AUTO_DISABLE_FAILURE_THRESHOLD = Math.max(
  2,
  Number.parseInt(process.env.SCHEDULE_AUTO_DISABLE_FAILURE_THRESHOLD || "3", 10) || 3
);
const LOCAL_SCHEDULE_HEALTH_FILE =
  process.env.SF_SCHEDULE_HEALTH_FILE || path.resolve(process.cwd(), "artifacts/schedule-health.json");

interface LocalScheduleHealthItem {
  consecutiveFailures: number;
  lastError?: string;
  lastFailedAt?: string;
  autoDisabled?: boolean;
  autoDisabledAt?: string;
}

interface LocalScheduleHealthDocument {
  version: number;
  updatedAt: string;
  schedules: Record<string, LocalScheduleHealthItem>;
}

function readLocalScheduleHealth(): Record<string, LocalScheduleHealthItem> {
  try {
    if (!fs.existsSync(LOCAL_SCHEDULE_HEALTH_FILE)) {
      return {};
    }

    const raw = fs.readFileSync(LOCAL_SCHEDULE_HEALTH_FILE, "utf8").trim();
    if (!raw) {
      return {};
    }

    const parsed = JSON.parse(raw) as unknown;
    const schedulesCandidate = (
      parsed && typeof parsed === "object" && !Array.isArray(parsed) && "schedules" in parsed
        ? (parsed as { schedules?: unknown }).schedules
        : parsed
    );

    if (!schedulesCandidate || typeof schedulesCandidate !== "object" || Array.isArray(schedulesCandidate)) {
      return {};
    }

    return Object.entries(schedulesCandidate).reduce<Record<string, LocalScheduleHealthItem>>((acc, [scheduleId, item]) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        return acc;
      }

      const candidate = item as Record<string, unknown>;
      acc[scheduleId] = {
        consecutiveFailures: Math.max(0, Number(candidate.consecutiveFailures || 0) || 0),
        lastError: typeof candidate.lastError === "string" ? candidate.lastError : undefined,
        lastFailedAt: typeof candidate.lastFailedAt === "string" ? candidate.lastFailedAt : undefined,
        autoDisabled: candidate.autoDisabled === true,
        autoDisabledAt: typeof candidate.autoDisabledAt === "string" ? candidate.autoDisabledAt : undefined
      };
      return acc;
    }, {});
  } catch {
    return {};
  }
}

function writeLocalScheduleHealth(store: Record<string, LocalScheduleHealthItem>): void {
  const directory = path.dirname(LOCAL_SCHEDULE_HEALTH_FILE);
  fs.mkdirSync(directory, { recursive: true });
  const document: LocalScheduleHealthDocument = {
    version: 1,
    updatedAt: new Date().toISOString(),
    schedules: store
  };
  fs.writeFileSync(LOCAL_SCHEDULE_HEALTH_FILE, JSON.stringify(document, null, 2), "utf8");
}

function markScheduleRunSuccess(scheduleId: string): void {
  const store = readLocalScheduleHealth();
  const existing = store[scheduleId];
  if (!existing) {
    return;
  }

  if ((existing.consecutiveFailures || 0) === 0 && existing.autoDisabled !== true) {
    return;
  }

  store[scheduleId] = {
    ...existing,
    consecutiveFailures: 0,
    autoDisabled: false,
    autoDisabledAt: undefined,
    lastError: undefined
  };
  writeLocalScheduleHealth(store);
}

function markScheduleRunFailure(scheduleId: string, errorMessage: string): LocalScheduleHealthItem {
  const store = readLocalScheduleHealth();
  const existing = store[scheduleId] || { consecutiveFailures: 0 };
  const updated: LocalScheduleHealthItem = {
    ...existing,
    consecutiveFailures: Math.max(0, Number(existing.consecutiveFailures || 0) || 0) + 1,
    lastError: errorMessage,
    lastFailedAt: new Date().toISOString()
  };
  store[scheduleId] = updated;
  writeLocalScheduleHealth(store);
  return updated;
}

function markScheduleAutoDisabled(scheduleId: string): void {
  const store = readLocalScheduleHealth();
  const existing = store[scheduleId] || { consecutiveFailures: 0 };
  store[scheduleId] = {
    ...existing,
    autoDisabled: true,
    autoDisabledAt: new Date().toISOString()
  };
  writeLocalScheduleHealth(store);
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
  const isFileSource = schedule.sourceType === "FILE_CSV" || schedule.sourceType === "FILE_EXCEL" || schedule.sourceType === "FILE_JSON";
  const isFileTarget = schedule.targetType === "FILE_CSV" || schedule.targetType === "FILE_EXCEL" || schedule.targetType === "FILE_JSON";
  const isRestSource = schedule.sourceType === "REST_API";

  const isGenericSalesforceToMssql =
    schedule.sourceType === "SALESFORCE_SOQL" && schedule.targetType === "MSSQL";
  const isGenericSalesforceToFile = schedule.sourceType === "SALESFORCE_SOQL" && isFileTarget;
  const isGenericMssqlToSalesforce =
    schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE";
  const isGenericMssqlToGlobalPicklist =
    schedule.sourceType === "MSSQL_SQL" && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
  const isGenericMssqlToFile = schedule.sourceType === "MSSQL_SQL" && isFileTarget;
  const isGenericRestToSalesforce = isRestSource && schedule.targetType === "SALESFORCE";
  const isGenericRestToGlobalPicklist = isRestSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
  const isGenericFileToSalesforce = isFileSource && schedule.targetType === "SALESFORCE";
  const isGenericFileToGlobalPicklist = isFileSource && schedule.targetType === "SALESFORCE_GLOBAL_PICKLIST";
  const isGenericFileToMssql = isFileSource && schedule.targetType === "MSSQL";

  const isHandledGenericFlow =
    isGenericSalesforceToMssql ||
    isGenericSalesforceToFile ||
    isGenericMssqlToSalesforce ||
    isGenericMssqlToGlobalPicklist ||
    isGenericMssqlToFile ||
    isGenericRestToSalesforce ||
    isGenericRestToGlobalPicklist ||
    isGenericFileToSalesforce ||
    isGenericFileToGlobalPicklist ||
    isGenericFileToMssql;

  if (!isHandledGenericFlow && schedule.objectName !== "Account") {
    logger.info(
      { scheduleId: schedule.id, objectName: schedule.objectName },
      "Skipping schedule because object is not supported yet"
    );
    return false;
  }

  if (!schedule.connectorId) {
    throw new Error(`Schedule ${schedule.name} is missing MSD_Connector__c`);
  }

  const connectorConfig = await salesforceClient.queryConnector(schedule.connectorId);
  const isFileConnector = /file|csv|excel|xlsx|json/i.test(connectorConfig.connectorType || "");
  const connector = (isFileConnector || isRestSource) ? undefined : new ConnectorRegistry().getConnectorByConfig(connectorConfig);

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
    const connectionOk = (isFileConnector || isRestSource) ? true : await connector!.testConnection();
    if (!connectionOk) {
      throw new Error(`Connection test failed for target system: ${schedule.targetSystem}`);
    }

    let result;

    if (isGenericSalesforceToMssql || isGenericSalesforceToFile) {
      if (!schedule.sourceDefinition?.trim()) {
        throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
      }

      if (!schedule.mappingDefinition?.trim()) {
        throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
      }

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
        maxRetries: context.maxRetries
      };

      const sourceAdapter = new SalesforceSoqlSourceAdapter(salesforceClient, schedule.sourceDefinition);
      const targetAdapter = isGenericSalesforceToFile
        ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition || "")
        : new MssqlTargetAdapter(connector as MssqlConnector);
      const job = new DataTransferJob(logger, sourceAdapter, targetAdapter);
      result = await job.execute(transferContext, schedule.mappingDefinition);
    } else if (
      isGenericMssqlToSalesforce ||
      isGenericMssqlToGlobalPicklist ||
      isGenericMssqlToFile ||
      isGenericRestToSalesforce ||
      isGenericRestToGlobalPicklist ||
      isGenericFileToSalesforce ||
      isGenericFileToGlobalPicklist ||
      isGenericFileToMssql
    ) {
      if (!schedule.sourceDefinition?.trim()) {
        throw new Error(`Schedule ${schedule.name} is missing MSD_SourceDefinition__c`);
      }

      if (!schedule.mappingDefinition?.trim()) {
        throw new Error(`Schedule ${schedule.name} is missing MSD_MappingDefinition__c`);
      }

      if (!schedule.targetDefinition?.trim()) {
        throw new Error(`Schedule ${schedule.name} is missing MSD_TargetDefinition__c`);
      }

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
          (isGenericMssqlToGlobalPicklist || isGenericFileToGlobalPicklist
            ? "SALESFORCE_GLOBAL_PICKLIST"
            : isGenericMssqlToFile
              ? "FILE_CSV"
              : isGenericFileToMssql
                ? "MSSQL"
                : "SALESFORCE"),
        batchSize: context.batchSize,
        maxRetries: context.maxRetries
      };

      const sourceAdapter = isFileSource
        ? new FileSourceAdapter(connectorConfig, schedule.sourceDefinition)
        : isRestSource
          ? new RestApiSourceAdapter(connectorConfig, schedule.sourceDefinition)
        : new MssqlQuerySourceAdapter(connectorConfig, schedule.sourceDefinition);

      const targetAdapter = isGenericMssqlToFile
        ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition)
        : isGenericFileToMssql
          ? new MssqlTargetAdapter(connector as MssqlConnector)
          : isGenericMssqlToGlobalPicklist || isGenericFileToGlobalPicklist
            ? new SalesforceGlobalPicklistTargetAdapter(salesforceClient, schedule.targetDefinition)
            : new SalesforceTargetAdapter(salesforceClient, schedule.targetDefinition, connectorConfig);

      const salesforceLookupResolver: LookupResolverFn = async (objectName, field, value) => {
        const escapedValue = String(value).replace(/'/g, "\\'");
        const soql = `SELECT Id FROM ${objectName} WHERE ${field} = '${escapedValue}' LIMIT 1`;
        const records = await salesforceClient.queryGeneric(soql);
        return records.length > 0 ? String(records[0].Id) : null;
      };

      if (
        !forceRun &&
        (targetAdapter instanceof SalesforceTargetAdapter || targetAdapter instanceof SalesforceGlobalPicklistTargetAdapter) &&
        !targetAdapter.isProfileSchedulerDue()
      ) {
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
      const job = new AccountExportJob(logger, source, connector!);
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
    markScheduleRunSuccess(schedule.id);

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

    const health = markScheduleRunFailure(schedule.id, errorMessage);
    const shouldAutoDisable =
      !forceRun &&
      schedule.active &&
      health.consecutiveFailures >= AUTO_DISABLE_FAILURE_THRESHOLD;

    if (shouldAutoDisable) {
      await salesforceClient.updateScheduleRecord(schedule.id, {
        Active__c: false,
        LastRunAt__c: new Date().toISOString()
      });

      markScheduleAutoDisabled(schedule.id);

      await salesforceClient.createLog({
        runId,
        level: "ERROR",
        step: "RUN_AUTO_DISABLED",
        message: `Scheduler automatisch deaktiviert nach ${health.consecutiveFailures} aufeinanderfolgenden Fehlern`,
        correlationId: context.correlationId
      });

      logger.warn(
        {
          scheduleId: schedule.id,
          scheduleName: schedule.name,
          consecutiveFailures: health.consecutiveFailures,
          threshold: AUTO_DISABLE_FAILURE_THRESHOLD
        },
        "Schedule auto-disabled after consecutive failures"
      );
    }

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
