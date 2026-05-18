import pino from "pino";
import fs from "node:fs";
import path from "node:path";
import { ConnectorConfig, CreateLogInput, SalesforceClient, SalesforceScheduleRecord } from "../clients/salesforce/salesforce-client";
import { ConnectorRegistry } from "../core/connector-registry/connector-registry";
import { BulkLookupResolverFn, DataTransferJob } from "../core/job-runner/data-transfer-job";
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
import { calculateNextRunAtFromTiming } from "../core/scheduler/schedule-timing";
import { JobContext } from "../types/job-context";
import { TransferContext } from "../types/transfer-context";
import { IntegrationSchedule } from "../types/integration-schedule";
import { isFileScheduleType } from "../types/file-schedule-type";
import { SalesforceConfig } from "../infrastructure/config/salesforce-config";
import { GenericRecord } from "../types/generic-record";
import { FailedJobRecord } from "../types/job-execution-result";
import { parseQuerySourceDefinition, resolveAfterExportValue } from "../utils/query-source-definition";

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
const FAILED_RUN_RECORDS_DIR =
  process.env.FAILED_RUN_RECORDS_DIR || path.resolve(process.cwd(), "artifacts/runtime/failed-run-records");

interface FailedRunRecordEntry {
  rowIndex: number;
  externalKey?: string;
  statusCode?: string;
  message?: string;
  retryable?: boolean;
  sourceRecord?: Record<string, unknown>;
  mappedRecord?: Record<string, unknown>;
}

interface FailedRunRecordsDocument {
  runId: string;
  scheduleId: string;
  scheduleName: string;
  connectorId?: string;
  connectorName?: string;
  createdAt: string;
  total: number;
  items: FailedRunRecordEntry[];
}

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

interface EffectiveTargetDefinition {
  objectApiName?: string;
  externalIdField?: string;
  profileName?: string;
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

async function runLogRetentionIfDue(salesforceClient: SalesforceClient, logger: pino.Logger): Promise<void> {
  const retentionDays = salesforceClient.getLogRetentionDays();
  if (retentionDays <= 0 || !salesforceClient.shouldRunLogCleanup()) {
    return;
  }

  const startedAt = Date.now();
  salesforceClient.markLogCleanupRun(startedAt);
  const cutoffIso = new Date(startedAt - retentionDays * 24 * 60 * 60 * 1000).toISOString();

  try {
    const result = await salesforceClient.deleteLogsOlderThan(cutoffIso);
    logger.info(
      {
        retentionDays,
        deletedLogs: result.deletedCount,
        batches: result.batches,
        cutoffIso
      },
      "Salesforce log retention applied"
    );
  } catch (error) {
    logger.warn(
      {
        retentionDays,
        cutoffIso,
        err: error
      },
      "Salesforce log retention failed"
    );
  }
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

function persistFailedRunRecords(
  runId: string,
  schedule: IntegrationSchedule,
  connectorConfig: ConnectorConfig,
  failedRecords: FailedJobRecord[] | undefined
): void {
  const entries = Array.isArray(failedRecords) ? failedRecords : [];
  const document: FailedRunRecordsDocument = {
    runId,
    scheduleId: schedule.id,
    scheduleName: schedule.name,
    connectorId: schedule.connectorId,
    connectorName: connectorConfig.name,
    createdAt: new Date().toISOString(),
    total: entries.length,
    items: entries.map((entry) => ({
      rowIndex: entry.rowIndex,
      externalKey: entry.externalKey,
      statusCode: entry.statusCode,
      message: entry.message,
      retryable: entry.retryable,
      sourceRecord: entry.sourceRecord,
      mappedRecord: entry.mappedRecord
    }))
  };

  fs.mkdirSync(FAILED_RUN_RECORDS_DIR, { recursive: true });
  const filePath = path.join(FAILED_RUN_RECORDS_DIR, `${runId}.json`);
  fs.writeFileSync(filePath, JSON.stringify(document, null, 2), "utf8");
}

type NotificationErrorClass = "CONNECTION" | "AUTH" | "DATA" | "VALIDATION" | "UNKNOWN";

function normalizeNotificationErrorClass(value: unknown): NotificationErrorClass | null {
  const normalized = String(value || "").trim().toUpperCase();
  if (
    normalized === "CONNECTION" ||
    normalized === "AUTH" ||
    normalized === "DATA" ||
    normalized === "VALIDATION" ||
    normalized === "UNKNOWN"
  ) {
    return normalized;
  }
  return null;
}

function getConnectorNotificationSettings(connectorConfig: ConnectorConfig): {
  enabled: boolean;
  ownerId?: string;
  errorClasses: Set<NotificationErrorClass>;
} {
  const parameters = connectorConfig.parameters || {};
  const ownerId = String(parameters.notificationTaskOwnerId || "").trim() || undefined;
  const enabled = parameters.notificationTaskEnabled === true && !!ownerId;
  const rawClasses = Array.isArray(parameters.notificationTaskErrorClasses)
    ? parameters.notificationTaskErrorClasses
    : String(parameters.notificationTaskErrorClasses || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
  const normalizedClasses = rawClasses
    .map((value) => normalizeNotificationErrorClass(value))
    .filter((value): value is NotificationErrorClass => value !== null);

  return {
    enabled,
    ownerId,
    errorClasses: new Set(normalizedClasses.length ? normalizedClasses : ["CONNECTION", "AUTH", "DATA", "VALIDATION", "UNKNOWN"])
  };
}

function classifyNotificationError(message: string, statusCode?: string): NotificationErrorClass {
  const combined = `${String(statusCode || "")} ${String(message || "")}`.toLowerCase();
  if (/invalid_client|invalid_grant|oauth|auth|unauthori|forbidden|login/i.test(combined)) {
    return "AUTH";
  }
  if (/connection|timeout|econn|enotfound|network|socket|unreachable|refused|mssql_connection_failed/i.test(combined)) {
    return "CONNECTION";
  }
  if (/validation|required|invalid field|field integrity|picklist|malformed|bad request/i.test(combined)) {
    return "VALIDATION";
  }
  if (/duplicate|record_error|upsert|insert|update|delete|constraint|conflict/i.test(combined)) {
    return "DATA";
  }
  return "UNKNOWN";
}

function getNotificationNextStep(errorClass: NotificationErrorClass): string {
  if (errorClass === "AUTH") {
    return "Connected App, Credentials und Login-URL prüfen.";
  }
  if (errorClass === "CONNECTION") {
    return "Connector-Erreichbarkeit, Netzwerk und Zielsystem-Verbindung prüfen.";
  }
  if (errorClass === "VALIDATION") {
    return "Feldmapping, Pflichtfelder und Zielvalidierungen prüfen.";
  }
  if (errorClass === "DATA") {
    return "Fehlerhafte Datensätze, Dubletten und Upsert-Schlüssel prüfen.";
  }
  return "Run-Logs und Connector-Konfiguration prüfen.";
}

async function maybeCreateConnectorNotificationTask(
  logger: pino.Logger,
  salesforceClient: SalesforceClient,
  connectorConfig: ConnectorConfig,
  schedule: IntegrationSchedule,
  context: {
    runId: string;
    errorMessage: string;
    errorClass: NotificationErrorClass;
    statusCode?: string;
    failureCount?: number;
  }
): Promise<void> {
  const settings = getConnectorNotificationSettings(connectorConfig);
  if (!settings.enabled || !settings.ownerId || !settings.errorClasses.has(context.errorClass)) {
    return;
  }

  const subject = `[Agent] ${context.errorClass} | ${connectorConfig.name} | ${schedule.name}`;
  const description = [
    "SF OnPrem Integration Agent",
    "==========================",
    "",
    `Fehlerklasse: ${context.errorClass}`,
    `Connector: ${connectorConfig.name} (${connectorConfig.connectorType})`,
    `Scheduler: ${schedule.name}`,
    `Objekt / Operation: ${schedule.objectName} / ${schedule.operation}`,
    `Run-ID: ${context.runId}`,
    context.statusCode ? `Statuscode: ${context.statusCode}` : undefined,
    context.failureCount ? `Anzahl betroffener Fehler: ${context.failureCount}` : undefined,
    "",
    "Fehlerdetail:",
    String(context.errorMessage || "Unbekannter Fehler").trim(),
    "",
    `Nächster Schritt: ${getNotificationNextStep(context.errorClass)}`
  ].filter(Boolean).join("\n");

  try {
    const taskId = await salesforceClient.createTask({
      ownerId: settings.ownerId,
      subject,
      description,
      priority: "High",
      status: "Not Started"
    });
    logger.info(
      {
        scheduleId: schedule.id,
        connectorId: connectorConfig.id,
        taskId,
        errorClass: context.errorClass
      },
      "Created Salesforce notification task for connector failure"
    );
  } catch (error) {
    logger.warn(
      {
        scheduleId: schedule.id,
        connectorId: connectorConfig.id,
        errorClass: context.errorClass,
        err: error
      },
      "Failed to create Salesforce notification task"
    );
  }
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

function extractSourceObjectApiNameFromSoql(queryText: string): string | undefined {
  const normalized = String(queryText || "").trim();
  if (!normalized) {
    return undefined;
  }

  const match = /\bfrom\s+([A-Za-z][A-Za-z0-9_]*)\b/i.exec(normalized);
  return match?.[1] ? match[1].trim() : undefined;
}

async function runSalesforceAfterExport(
  salesforceClient: SalesforceClient,
  schedule: IntegrationSchedule,
  runId: string,
  correlationId: string,
  successfulSourceRecords: GenericRecord[] | undefined,
  finishedAt: string
): Promise<void> {
  const parsedSourceDefinition = parseQuerySourceDefinition(schedule.sourceDefinition || "");
  const afterExport = parsedSourceDefinition.afterExport;
  if (!afterExport || !successfulSourceRecords?.length) {
    return;
  }

  const sourceObjectApiName = extractSourceObjectApiNameFromSoql(parsedSourceDefinition.queryText);
  const updateObjectApiName = String(sourceObjectApiName || schedule.objectName || "").trim();
  if (!updateObjectApiName) {
    return;
  }

  const afterExportEntries = Object.entries(afterExport.updates);
  const allowsSafePostStatusWriteback =
    afterExportEntries.length === 1
    && afterExportEntries[0][0].trim().toLowerCase() === "post_status__c"
    && String(afterExportEntries[0][1] || "").trim().toLowerCase() === "success";

  const normalizedDeltaField = String(parsedSourceDefinition.delta?.field || "").trim().toLowerCase();
  const usesMutableSalesforceTimestamp =
    parsedSourceDefinition.delta?.strategy === "datetime"
    && (normalizedDeltaField === "lastmodifieddate" || normalizedDeltaField === "systemmodstamp");

  if (usesMutableSalesforceTimestamp && !allowsSafePostStatusWriteback) {
    await salesforceClient.createLog({
      runId,
      level: "WARN",
      step: "AFTER_EXPORT_SKIPPED",
      message: "After Export wurde uebersprungen, weil Delta auf LastModifiedDate/SystemModstamp sonst denselben Salesforce-Datensatz erneut aendern und Folgeexporte ausloesen wuerde.",
      correlationId
    });
    return;
  }

  const updatePayloads: Record<string, unknown>[] = [];
  for (const record of successfulSourceRecords) {
    const sourceId = typeof record.values.Id === "string" ? record.values.Id.trim() : "";
    if (!sourceId) {
      continue;
    }

    const payload = Object.entries(afterExport.updates).reduce<Record<string, unknown>>((acc, [fieldName, fieldValue]) => {
      acc[fieldName] = resolveAfterExportValue(fieldValue, finishedAt, runId);
      return acc;
    }, { Id: sourceId });
    updatePayloads.push(payload);
  }

  const updatedIds = await salesforceClient.updateGenericRecords(updateObjectApiName, updatePayloads);

  if (updatedIds.length > 0) {
    await salesforceClient.createLog({
      runId,
      level: "INFO",
      step: "AFTER_EXPORT_UPDATED",
      message: `${updatedIds.length} exportierte Datensaetze in ${updateObjectApiName} nachbearbeitet`,
      correlationId
    });
  }
}

function extractEffectiveTargetDefinition(targetDefinition?: string): EffectiveTargetDefinition {
  const raw = String(targetDefinition || "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      return {};
    }

    if (typeof parsed.objectApiName === "string" && parsed.objectApiName.trim()) {
      return {
        objectApiName: parsed.objectApiName.trim(),
        externalIdField: typeof parsed.externalIdField === "string" ? parsed.externalIdField.trim() : undefined
      };
    }

    const selectedImportProfileName = typeof parsed.selectedImportProfileName === "string"
      ? parsed.selectedImportProfileName.trim()
      : "";
    const profiles = Array.isArray(parsed.importProfiles) ? parsed.importProfiles : [];
    const selectedProfile = profiles.find((entry) =>
      entry &&
      typeof entry === "object" &&
      typeof (entry as Record<string, unknown>).name === "string" &&
      String((entry as Record<string, unknown>).name).trim() === selectedImportProfileName
    ) as Record<string, unknown> | undefined;
    const fallbackProfile = profiles.find((entry) => entry && typeof entry === "object") as Record<string, unknown> | undefined;
    const profile = selectedProfile || fallbackProfile;
    if (!profile) {
      return {};
    }

    return {
      objectApiName: typeof profile.objectApiName === "string" ? profile.objectApiName.trim() : undefined,
      externalIdField: typeof profile.externalIdField === "string" ? profile.externalIdField.trim() : undefined,
      profileName: typeof profile.name === "string" ? profile.name.trim() : undefined
    };
  } catch {
    return {};
  }
}

function buildScheduleConflictKey(schedule: IntegrationSchedule): string | undefined {
  const direction = String(schedule.direction || "").trim().toLowerCase();
  const targetType = String(schedule.targetType || "").trim().toUpperCase();
  if (targetType !== "SALESFORCE" && targetType !== "SALESFORCE_GLOBAL_PICKLIST") {
    return undefined;
  }

  const effectiveTarget = extractEffectiveTargetDefinition(schedule.targetDefinition);
  const objectApiName = String(effectiveTarget.objectApiName || schedule.objectName || "").trim();
  const externalIdField = String(effectiveTarget.externalIdField || "").trim();
  const connectorId = String(schedule.connectorId || "").trim();
  const sourceType = String(schedule.sourceType || "").trim().toUpperCase();
  const operation = String(schedule.operation || "").trim().toLowerCase();
  if (!direction || !objectApiName || !connectorId) {
    return undefined;
  }

  return [direction, targetType, objectApiName, externalIdField, connectorId, sourceType, operation].join("|");
}

function getScheduleConflictPriority(schedule: IntegrationSchedule): number {
  const normalizedName = String(schedule.name || "").trim().toLowerCase();
  let score = 0;
  if (normalizedName.includes("test")) {
    score -= 200;
  }
  if (normalizedName.includes("copy")) {
    score -= 150;
  }
  if (normalizedName.includes("scenario") || normalizedName.includes("szenario")) {
    score += 25;
  }
  if (schedule.inheritTimingFromParent) {
    score -= 50;
  }
  if (schedule.nextRunAt) {
    score += 5;
  }
  return score;
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

function getScheduleTimingDefinition(schedule: IntegrationSchedule): string | undefined {
  return schedule.timingDefinition || extractTimingDefinition(schedule.targetDefinition);
}

function buildScheduleRunTimestampFields(
  schedule: IntegrationSchedule,
  finishedAt: string,
  options?: { updateNextRunAt?: boolean }
): Record<string, unknown> {
  const fields: Record<string, unknown> = {
    LastRunAt__c: finishedAt
  };

  if (options?.updateNextRunAt !== false && !schedule.inheritTimingFromParent) {
    const calculatedNextRunAt = calculateNextRunAtFromTiming(getScheduleTimingDefinition(schedule), new Date(finishedAt));
    if (calculatedNextRunAt) {
      fields.NextRunAt__c = calculatedNextRunAt;
    }
  }

  return fields;
}

async function ensureScheduleHasNextRunAt(
  salesforceClient: SalesforceClient,
  logger: pino.Logger,
  schedule: IntegrationSchedule
): Promise<IntegrationSchedule> {
  if (schedule.inheritTimingFromParent || schedule.nextRunAt || !getScheduleTimingDefinition(schedule)) {
    return schedule;
  }

  const calculatedNextRunAt = calculateNextRunAtFromTiming(getScheduleTimingDefinition(schedule), new Date());
  if (!calculatedNextRunAt) {
    return schedule;
  }

  await salesforceClient.updateScheduleRecord(schedule.id, {
    NextRunAt__c: calculatedNextRunAt
  });

  logger.info(
    {
      scheduleId: schedule.id,
      scheduleName: schedule.name,
      nextRunAt: calculatedNextRunAt
    },
    "Initialized missing scheduler NextRunAt from timing definition"
  );

  return {
    ...schedule,
    nextRunAt: calculatedNextRunAt
  };
}

function isValidSalesforceIdentifier(value: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_.]*$/.test(value);
}

async function executeSchedule(
  salesforceClient: SalesforceClient,
  logger: pino.Logger,
  agentId: string,
  schedule: IntegrationSchedule,
  options?: { forceRun?: boolean }
): Promise<boolean> {
  const forceRun = options?.forceRun ?? false;
  const latestScheduleRecord = await salesforceClient.queryScheduleById(schedule.id);
  const latestSchedule = mapSchedule(latestScheduleRecord);
  if (!latestSchedule.active) {
    logger.info(
      { scheduleId: schedule.id, scheduleName: schedule.name },
      "Skipping schedule because it is no longer active after refresh"
    );
    return false;
  }

  if (!forceRun && !isScheduleDue(latestSchedule)) {
    logger.info(
      { scheduleId: schedule.id, scheduleName: schedule.name },
      "Skipping schedule because it is no longer due after refresh"
    );
    return false;
  }

  schedule = latestSchedule;
  const isFileSource = isFileScheduleType(schedule.sourceType);
  const isFileTarget = isFileScheduleType(schedule.targetType);
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
  let lastProgressLogAt = 0;
  const writeTransferProgressLog: TransferContext["onProgress"] = async (progress) => {
    const now = Date.now();
    const isSourceRead = progress.phase === "source-read";
    const isComplete = progress.totalRecords !== undefined && progress.processedRecords >= progress.totalRecords;
    if (!isSourceRead && !isComplete && now - lastProgressLogAt < 60_000) {
      return;
    }

    lastProgressLogAt = now;
    await salesforceClient.createLog({
      runId,
      level: "INFO",
      step: progress.phase === "source-read" ? "SOURCE_READ" : "BATCH_PROGRESS",
      message: progress.phase === "source-read"
        ? `Source records loaded: records=${progress.totalRecords ?? 0}`
        : `Batch progress: records=${progress.processedRecords}/${progress.totalRecords ?? "?"}, batchSize=${progress.batchSize ?? 0}`,
      correlationId: context.correlationId
    });
  };

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
        maxRetries: context.maxRetries,
        checkpoint: {
          value: lastCheckpoint,
          recordId: lastRecordId
        },
        onProgress: writeTransferProgressLog
      };

      const sourceAdapter = new SalesforceSoqlSourceAdapter(salesforceClient, schedule.sourceDefinition);
      const targetAdapter = isGenericSalesforceToFile
        ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition || "")
        : new MssqlTargetAdapter(connector as MssqlConnector, schedule.targetDefinition);
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
        maxRetries: context.maxRetries,
        checkpoint: {
          value: lastCheckpoint,
          recordId: lastRecordId
        },
        onProgress: writeTransferProgressLog
      };

      const sourceAdapter = isFileSource
        ? new FileSourceAdapter(connectorConfig, schedule.sourceDefinition)
        : isRestSource
          ? new RestApiSourceAdapter(connectorConfig, schedule.sourceDefinition)
        : new MssqlQuerySourceAdapter(connectorConfig, schedule.sourceDefinition);

      const targetAdapter = isGenericMssqlToFile
        ? new FileTargetAdapter(connectorConfig, schedule.targetDefinition)
        : isGenericFileToMssql
          ? new MssqlTargetAdapter(connector as MssqlConnector, schedule.targetDefinition)
          : isGenericMssqlToGlobalPicklist || isGenericFileToGlobalPicklist
            ? new SalesforceGlobalPicklistTargetAdapter(salesforceClient, schedule.targetDefinition, schedule.lastRunAt)
            : new SalesforceTargetAdapter(salesforceClient, schedule.targetDefinition, connectorConfig, schedule.lastRunAt);

      const salesforceLookupResolver: LookupResolverFn = async (objectName, field, value) => {
        const escapedValue = String(value).replace(/'/g, "\\'");
        const soql = `SELECT Id FROM ${objectName} WHERE ${field} = '${escapedValue}' LIMIT 1`;
        try {
          logger.debug({ scheduleId: schedule.id, runId, objectName, field, rawValue: value, soql }, "Lookup resolver: executing SOQL");
          const records = await salesforceClient.queryGeneric(soql);
          logger.debug(
            { scheduleId: schedule.id, runId, soql, found: records.length > 0, recordsCount: records.length, sample: records[0] ?? null },
            "Lookup resolver: result"
          );
          return records.length > 0 ? String(records[0].Id) : null;
        } catch (err) {
          logger.error({ scheduleId: schedule.id, runId, soql, error: err instanceof Error ? err.message : String(err) }, "Lookup resolver: query failed");
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
                  runId,
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

      if (
        !forceRun &&
        (targetAdapter instanceof SalesforceTargetAdapter || targetAdapter instanceof SalesforceGlobalPicklistTargetAdapter) &&
        !targetAdapter.isProfileSchedulerDue()
      ) {
        logger.info(
          { scheduleId: schedule.id, profileName: targetAdapter.getActiveProfileName() },
          "Skipping schedule because selected import profile scheduler is not active/due"
        );

        await salesforceClient.updateRun(runId, {
          status: "Success",
          finishedAt: new Date().toISOString(),
          recordsRead: 0,
          recordsProcessed: 0,
          recordsSucceeded: 0,
          recordsFailed: 0
        });

        const skippedFinishedAt = new Date().toISOString();
        await salesforceClient.updateScheduleRecord(
          schedule.id,
          buildScheduleRunTimestampFields(schedule, skippedFinishedAt)
        );

        return false;
      }

      const job = new DataTransferJob(
        logger,
        sourceAdapter,
        targetAdapter,
        salesforceLookupResolver,
        salesforceBulkLookupResolver
      );
      result = await job.execute(transferContext, schedule.mappingDefinition);
    } else {
      const source = new SalesforceAccountSource(salesforceClient);
      const job = new AccountExportJob(logger, source, connector!);
      result = await job.execute(context, lastCheckpoint, lastRecordId, schedule.mappingDefinition);
    }

    persistFailedRunRecords(runId, schedule, connectorConfig, result.failedRecords);

    const failedConnectorResults = result.connectorResults.filter((connectorResult) => !connectorResult.success);
    if (failedConnectorResults.length > 0) {
      const errorLogInputs: CreateLogInput[] = failedConnectorResults.map((connectorResult) => {
        const statusCode = connectorResult.statusCode || "UNKNOWN_STATUS";
        const message = connectorResult.message || "Unknown connector error";
        const retryableText = connectorResult.retryable ? "retryable=true" : "retryable=false";

        return {
          runId,
          level: "ERROR",
          step: "RECORD_ERROR",
          message: `${statusCode}: ${message} (${retryableText})`,
          correlationId: context.correlationId,
          recordKey: connectorResult.externalKey
        };
      });

      try {
        await salesforceClient.createLogsBulk(errorLogInputs);
      } catch (bulkError) {
        logger.warn(
          {
            scheduleId: schedule.id,
            runId,
            failedCount: errorLogInputs.length,
            error: bulkError instanceof Error ? bulkError.message : String(bulkError)
          },
          "Bulk error log write failed. Falling back to single inserts."
        );

        for (const logInput of errorLogInputs) {
          await salesforceClient.createLog(logInput);
        }
      }
    }

    if (failedConnectorResults.length > 0) {
      const primaryFailure = failedConnectorResults[0];
      await maybeCreateConnectorNotificationTask(logger, salesforceClient, connectorConfig, schedule, {
        runId,
        errorMessage: primaryFailure.message || "Unknown connector error",
        statusCode: primaryFailure.statusCode,
        errorClass: classifyNotificationError(primaryFailure.message || "Unknown connector error", primaryFailure.statusCode),
        failureCount: failedConnectorResults.length
      });
    }

    const finishedAt = new Date().toISOString();

    if (schedule.sourceType === "SALESFORCE_SOQL") {
      await runSalesforceAfterExport(
        salesforceClient,
        schedule,
        runId,
        context.correlationId,
        result.successfulSourceRecords,
        finishedAt
      );
    }

    if (result.status !== "Success" || result.recordsRead > 0 || result.recordsProcessed > 0 || result.recordsSucceeded > 0 || result.recordsFailed > 0) {
      await salesforceClient.createLog({
        runId,
        level: "INFO",
        step: forceRun ? "RUN_NOW_FINISHED" : "RUN_FINISHED",
        message: `Run finished with status ${result.status}. Records: read=${result.recordsRead}, processed=${result.recordsProcessed}, ok=${result.recordsSucceeded}, fail=${result.recordsFailed}`,
        correlationId: context.correlationId
      });
    }

    await salesforceClient.updateRun(runId, {
      status: result.status,
      finishedAt,
      recordsRead: result.recordsRead,
      recordsProcessed: result.recordsProcessed,
      recordsSucceeded: result.recordsSucceeded,
      recordsFailed: result.recordsFailed
    });

    await salesforceClient.updateScheduleRecord(schedule.id, buildScheduleRunTimestampFields(schedule, finishedAt));
    markScheduleRunSuccess(schedule.id);

    if (result.lastProcessedRecord) {
      const parsedSourceDefinition = parseQuerySourceDefinition(schedule.sourceDefinition || "");
      const deltaStrategy = parsedSourceDefinition.delta?.strategy;
      const checkpointValue = result.lastProcessedRecord.value;
      const checkpointRecordId = deltaStrategy === "datetime"
        ? result.lastProcessedRecord.recordId
        : undefined;

      await salesforceClient.upsertCheckpoint({
        checkpointId: checkpoint?.id,
        scheduleId: schedule.id,
        objectName: schedule.objectName,
        lastCheckpoint: checkpointValue,
        lastRecordId: checkpointRecordId,
        lastRunId: runId
      });

      await salesforceClient.createLog({
        runId,
        level: "INFO",
        step: "CHECKPOINT_SAVED",
        message: `Checkpoint updated to ${checkpointValue || checkpointRecordId || "-"}${checkpointRecordId && checkpointRecordId !== checkpointValue ? ` / ${checkpointRecordId}` : ""}`,
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

    const failedAt = new Date().toISOString();
    await salesforceClient.updateRun(runId, {
      status: "Failed",
      finishedAt: failedAt,
      errorMessage
    });

    if (!forceRun) {
      await salesforceClient.updateScheduleRecord(schedule.id, buildScheduleRunTimestampFields(schedule, failedAt));
    }

    const health = markScheduleRunFailure(schedule.id, errorMessage);
    const shouldAutoDisable =
      !forceRun &&
      schedule.active &&
      health.consecutiveFailures >= AUTO_DISABLE_FAILURE_THRESHOLD;

    await maybeCreateConnectorNotificationTask(logger, salesforceClient, connectorConfig, schedule, {
      runId,
      errorMessage,
      statusCode: forceRun ? "RUN_NOW_FAILED" : "RUN_FAILED",
      errorClass: classifyNotificationError(errorMessage),
      failureCount: 1
    });

    if (shouldAutoDisable) {
      await salesforceClient.updateScheduleRecord(schedule.id, {
        Active__c: false,
        ...buildScheduleRunTimestampFields(schedule, failedAt, { updateNextRunAt: false })
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

  logger.debug("Salesforce login successful");

  await runLogRetentionIfDue(salesforceClient, logger);

  const scheduleSource = new SalesforceScheduleSource(salesforceClient);
  const schedules = await scheduleSource.getActiveSchedules();

  if (schedules.length > 0) {
    logger.info({ schedulesFound: schedules.length }, "Active schedules loaded");
  } else {
    logger.debug({ schedulesFound: 0 }, "Active schedules loaded");
  }

  const duplicateProtectionWinners = new Map<string, IntegrationSchedule>();
  const duplicateProtectionLosers = new Set<string>();
  const duplicateGroups = new Map<string, IntegrationSchedule[]>();
  for (const schedule of schedules) {
    const conflictKey = buildScheduleConflictKey(schedule);
    if (!conflictKey) {
      continue;
    }

    const bucket = duplicateGroups.get(conflictKey) || [];
    bucket.push(schedule);
    duplicateGroups.set(conflictKey, bucket);
  }

  duplicateGroups.forEach((group, conflictKey) => {
    if (group.length <= 1) {
      return;
    }

    const sorted = group.slice().sort((left, right) => {
      const priorityDelta = getScheduleConflictPriority(right) - getScheduleConflictPriority(left);
      if (priorityDelta !== 0) {
        return priorityDelta;
      }
      return String(left.name || left.id).localeCompare(String(right.name || right.id), "de", { sensitivity: "base" });
    });
    duplicateProtectionWinners.set(conflictKey, sorted[0]);
    sorted.slice(1).forEach((schedule) => duplicateProtectionLosers.add(schedule.id));

    logger.warn(
      {
        conflictKey,
        keptScheduleId: sorted[0].id,
        keptScheduleName: sorted[0].name,
        skippedScheduleIds: sorted.slice(1).map((item) => item.id),
        skippedScheduleNames: sorted.slice(1).map((item) => item.name)
      },
      "Detected duplicate active schedulers for the same Salesforce target; lower-priority schedules will be skipped"
    );
  });

  const orderedSchedules = buildHierarchyOrderedSchedules(schedules);
  const dueState = new Map<string, boolean>();
  const executedState = new Map<string, boolean>();

  let dueSchedules = 0;
  let processedSchedules = 0;

  for (const scheduleEntry of orderedSchedules) {
    const schedule = await ensureScheduleHasNextRunAt(salesforceClient, logger, scheduleEntry);
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

    if (duplicateProtectionLosers.has(schedule.id)) {
      logger.warn(
        {
          scheduleId: schedule.id,
          scheduleName: schedule.name
        },
        "Skipping duplicate scheduler because a higher-priority scheduler already owns the same Salesforce target"
      );
      executedState.set(schedule.id, false);
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

  if (dueSchedules > 0) {
    logger.info({ dueSchedules, schedulesFound: schedules.length }, "Due schedules identified");
  } else {
    logger.debug({ dueSchedules, schedulesFound: schedules.length }, "No due schedules found");
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

  await runLogRetentionIfDue(salesforceClient, logger);

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
