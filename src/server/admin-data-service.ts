import pino from "pino";
import fs from "node:fs";
import path from "node:path";
import {
  ConnectorConfig,
  SalesforceClient,
  SalesforceScheduleRecord
} from "../clients/salesforce/salesforce-client";
import { ConnectorRegistry } from "../core/connector-registry/connector-registry";
import { MappingDefinitionEngine } from "../core/mapping-dsl/mapping-definition-engine";
import { MappingDefinitionParser } from "../core/mapping-dsl/mapping-definition-parser";
import {
  getSalesforceConfig,
  SalesforceConfig
} from "../infrastructure/config/salesforce-config";
import { MssqlDatabase } from "../infrastructure/db/mssql";
import { IntegrationSchedule } from "../types/integration-schedule";
import { runScheduleNow } from "../agent/agent-runner";

interface SalesforceInstanceEnvConfig {
  id: string;
  name?: string;
  loginUrl: string;
  clientId?: string;
  clientSecret?: string;
  clientIdEnv?: string;
  clientSecretEnv?: string;
  queryLimit?: number;
}

export interface SalesforceInstanceMutationInput {
  id: string;
  name?: string;
  loginUrl: string;
  clientId: string;
  clientSecret: string;
  queryLimit?: number;
}

interface ResolvedInstance {
  id: string;
  name: string;
  config: SalesforceConfig;
}

const LOCAL_INSTANCES_FILE = process.env.SF_INSTANCES_FILE || path.resolve(process.cwd(), "artifacts/sf-instances.json");
const LOCAL_SCHEDULE_TIMING_FILE = process.env.SF_SCHEDULE_TIMING_FILE || path.resolve(process.cwd(), "artifacts/schedule-timing.json");
const LOCAL_SCHEDULE_TIMING_VERSION = 1;

type LocalScheduleTimingStore = Record<string, Record<string, string>>;

interface LocalScheduleTimingDocument {
  version: number;
  updatedAt: string;
  instances: LocalScheduleTimingStore;
}

function normalizeLocalScheduleTimingStore(parsed: unknown): LocalScheduleTimingStore {
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return {};
  }

  const instancesCandidate = (
    "instances" in parsed
      ? (parsed as { instances?: unknown }).instances
      : parsed
  );

  if (!instancesCandidate || typeof instancesCandidate !== "object" || Array.isArray(instancesCandidate)) {
    return {};
  }

  return Object.entries(instancesCandidate).reduce<LocalScheduleTimingStore>((store, [instanceId, scheduleMap]) => {
    if (!scheduleMap || typeof scheduleMap !== "object" || Array.isArray(scheduleMap)) {
      store[instanceId] = {};
      return store;
    }

    store[instanceId] = Object.entries(scheduleMap).reduce<Record<string, string>>((instanceStore, [scheduleId, timingDefinition]) => {
      if (typeof timingDefinition === "string" && timingDefinition.trim()) {
        instanceStore[scheduleId] = timingDefinition;
      }

      return instanceStore;
    }, {});

    return store;
  }, {});
}

function readLocalScheduleTimingStore(): LocalScheduleTimingStore {
  try {
    if (!fs.existsSync(LOCAL_SCHEDULE_TIMING_FILE)) {
      return {};
    }

    const raw = fs.readFileSync(LOCAL_SCHEDULE_TIMING_FILE, "utf8").trim();
    if (!raw) {
      return {};
    }

    const parsed = JSON.parse(raw) as unknown;
    return normalizeLocalScheduleTimingStore(parsed);
  } catch {
    return {};
  }
}

function writeLocalScheduleTimingStore(store: LocalScheduleTimingStore): void {
  const directory = path.dirname(LOCAL_SCHEDULE_TIMING_FILE);
  fs.mkdirSync(directory, { recursive: true });
  const document: LocalScheduleTimingDocument = {
    version: LOCAL_SCHEDULE_TIMING_VERSION,
    updatedAt: new Date().toISOString(),
    instances: store
  };
  fs.writeFileSync(LOCAL_SCHEDULE_TIMING_FILE, JSON.stringify(document, null, 2), "utf8");
}

function readLocalInstances(): SalesforceInstanceEnvConfig[] {
  try {
    if (!fs.existsSync(LOCAL_INSTANCES_FILE)) {
      return [];
    }

    const raw = fs.readFileSync(LOCAL_INSTANCES_FILE, "utf8").trim();
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed as SalesforceInstanceEnvConfig[];
  } catch {
    return [];
  }
}

function writeLocalInstances(instances: SalesforceInstanceEnvConfig[]): void {
  const directory = path.dirname(LOCAL_INSTANCES_FILE);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(LOCAL_INSTANCES_FILE, JSON.stringify(instances, null, 2), "utf8");
}

function toResolvedInstance(
  item: SalesforceInstanceEnvConfig,
  fallbackQueryLimit: number
): ResolvedInstance | null {
  if (!item || typeof item !== "object") {
    return null;
  }

  if (!item.id || !item.loginUrl) {
    return null;
  }

  const resolvedClientId = item.clientId || (item.clientIdEnv ? process.env[item.clientIdEnv] : undefined);
  const resolvedClientSecret = item.clientSecret || (item.clientSecretEnv ? process.env[item.clientSecretEnv] : undefined);

  if (!resolvedClientId || !resolvedClientSecret) {
    return null;
  }

  return {
    id: item.id,
    name: item.name?.trim() || item.id,
    config: {
      loginUrl: item.loginUrl,
      clientId: resolvedClientId,
      clientSecret: resolvedClientSecret,
      queryLimit: item.queryLimit || fallbackQueryLimit
    }
  };
}

export interface SalesforceInstanceOption {
  id: string;
  name: string;
  isDefault: boolean;
}

export interface ScheduleListItem {
  id: string;
  name: string;
  active: boolean;
  status: "due" | "scheduled" | "inactive";
  sourceSystem: string;
  targetSystem: string;
  sourceType?: string;
  targetType?: string;
  direction?: string;
  objectName: string;
  operation: string;
  connectorId?: string;
  mappingDefinition?: string;
  sourceDefinition?: string;
  targetDefinition?: string;
  nextRunAt?: string;
  lastRunAt?: string;
  batchSize: number;
  timingDefinition?: string;
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
}

export interface ConnectorListItem {
  id: string;
  name: string;
  active: boolean;
  connectorType: string;
  targetSystem?: string;
  direction?: string;
  timeoutMs?: number;
  maxRetries?: number;
  description?: string;
  hasSecret: boolean;
  parameterKeys: string[];
}

export interface ConnectorTestResult {
  ok: boolean;
  connectorId: string;
  connectorName: string;
  connectorType: string;
  message: string;
}

export interface RunListItem {
  id: string;
  scheduleId?: string;
  scheduleName?: string;
  status: string;
  startedAt?: string;
  finishedAt?: string;
  recordsRead?: number;
  recordsProcessed?: number;
  recordsSucceeded?: number;
  recordsFailed?: number;
  errorMessage?: string;
}

export interface LogListItem {
  id: string;
  runId?: string;
  scheduleName?: string;
  level?: string;
  step?: string;
  message?: string;
  recordKey?: string;
  createdAt?: string;
}

export type LogChartRange = "last_hour" | "last_24h" | "last_30d";

export interface LogChartBucket {
  label: string;
  start: string;
  end: string;
  total: number;
  errors: number;
}

export interface LogChartSummary {
  range: LogChartRange;
  buckets: LogChartBucket[];
}

export interface SqlPreviewResult {
  fields: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

export interface SourcePreviewResult {
  fields: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

export interface SourceFieldMetadata {
  name: string;
  label?: string;
  type: string;
}

export interface MappingPreviewResult {
  fields: string[];
  rows: Record<string, unknown>[];
}

export interface ScheduleMutationInput {
  id?: string;
  name: string;
  active: boolean;
  sourceSystem: string;
  targetSystem: string;
  objectName: string;
  operation: string;
  connectorId?: string;
  mappingDefinition?: string;
  direction?: string;
  sourceType?: string;
  targetType?: string;
  sourceDefinition?: string;
  targetDefinition?: string;
  batchSize?: number;
  nextRunAt?: string;
  lastRunAt?: string;
  timingDefinition?: string;
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
}

export interface DeleteScheduleResult {
  deletedIds: string[];
  deletedNames: string[];
}

export interface ScheduleDryRunResult {
  ok: boolean;
  scheduleId: string;
  scheduleName: string;
  sourceType?: string;
  rowCount?: number;
  fields?: string[];
  message: string;
}

export interface ConnectorMutationInput {
  id?: string;
  name: string;
  active: boolean;
  connectorType: string;
  targetSystem?: string;
  direction?: string;
  secretKey?: string;
  timeoutMs?: number;
  maxRetries?: number;
  description?: string;
  parameters?: Record<string, unknown>;
}

export interface GraphNode {
  id: string;
  kind: "connector" | "scheduler";
  label: string;
  subtitle?: string;
  direction?: string;
  objectName?: string;
  directionIcon?: string;
  connectorType?: string;
  x: number;
  y: number;
  refId: string;
}

export interface ScheduleFormOptions {
  objectNames: string[];
  operations: string[];
  sourceSystems: string[];
  targetSystems: string[];
  directions: string[];
}

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  direction?: string;
}

export interface ConnectionGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

interface PersistedTargetDefinitionEnvelope {
  timingDefinition?: string;
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
  [key: string]: unknown;
}

interface ParsedSoqlSelectedField {
  expression: string;
  alias?: string;
}

function getRequiredString(parameters: Record<string, unknown>, key: string): string {
  const value = parameters[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required MSSQL parameter: ${key}`);
  }

  return value.trim();
}

function getOptionalNumber(parameters: Record<string, unknown>, key: string): number | undefined {
  const value = parameters[key];
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  if (typeof value === "number") {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  throw new Error(`Invalid numeric MSSQL parameter: ${key}`);
}

function getOptionalBoolean(parameters: Record<string, unknown>, key: string): boolean | undefined {
  const value = parameters[key];
  if (value === undefined || value === null || value === "") {
    return undefined;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    if (value.toLowerCase() === "true") {
      return true;
    }
    if (value.toLowerCase() === "false") {
      return false;
    }
  }

  throw new Error(`Invalid boolean MSSQL parameter: ${key}`);
}

function resolvePassword(config: ConnectorConfig): string {
  if (!config.secretKey) {
    throw new Error(`Connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(`Environment variable ${config.secretKey} is not set for connector ${config.name}`);
  }

  return password;
}

function resolveInstances(): ResolvedInstance[] {
  const instances: ResolvedInstance[] = [];
  let fallbackQueryLimit = 200;

  try {
    const defaultConfig = getSalesforceConfig();
    fallbackQueryLimit = defaultConfig.queryLimit;
    instances.push({
      id: "default",
      name: "Default",
      config: defaultConfig
    });
  } catch {
    // Default instance is optional when only named instances are configured.
  }

  const raw = process.env.SF_INSTANCES_JSON?.trim();
  if (raw) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw) as unknown;
    } catch {
      parsed = [];
    }

    if (Array.isArray(parsed)) {
      for (const item of parsed as SalesforceInstanceEnvConfig[]) {
        const resolved = toResolvedInstance(item, fallbackQueryLimit);
        if (resolved) {
          instances.push(resolved);
        }
      }
    }
  }

  for (const item of readLocalInstances()) {
    const resolved = toResolvedInstance(item, fallbackQueryLimit);
    if (resolved) {
      instances.push(resolved);
    }
  }

  const deduped = new Map<string, ResolvedInstance>();
  for (const instance of instances) {
    deduped.set(instance.id, instance);
  }

  return [...deduped.values()];
}

export class AdminDataService {
  public listInstances(): SalesforceInstanceOption[] {
    const instances = resolveInstances();
    return instances.map((instance, index) => ({
      id: instance.id,
      name: instance.name,
      isDefault: index === 0
    }));
  }

  public saveInstance(input: SalesforceInstanceMutationInput): SalesforceInstanceOption {
    const id = input.id.trim();
    const loginUrl = input.loginUrl.trim();
    const clientId = input.clientId.trim();
    const clientSecret = input.clientSecret.trim();

    if (!id || !loginUrl || !clientId || !clientSecret) {
      throw new Error("id, loginUrl, clientId und clientSecret sind erforderlich");
    }

    const localInstances = readLocalInstances();
    const nextItem: SalesforceInstanceEnvConfig = {
      id,
      name: input.name?.trim() || id,
      loginUrl,
      clientId,
      clientSecret,
      queryLimit: input.queryLimit
    };

    const existingIndex = localInstances.findIndex((item) => item.id === id);
    if (existingIndex >= 0) {
      localInstances[existingIndex] = nextItem;
    } else {
      localInstances.push(nextItem);
    }

    writeLocalInstances(localInstances);
    return { id: nextItem.id, name: nextItem.name || nextItem.id, isDefault: false };
  }

  public async listSchedules(instanceId?: string): Promise<ScheduleListItem[]> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const records = await client.querySchedules(false);
    const localTiming = readLocalScheduleTimingStore()[resolvedInstance.id] || {};

    return records.map((record) => {
      const schedule = this.toIntegrationSchedule(record);
      const persistedTimingDefinition = localTiming[schedule.id] || schedule.timingDefinition;
      const effectiveSchedule: IntegrationSchedule = {
        ...schedule,
        timingDefinition: persistedTimingDefinition
      };

      return {
        id: schedule.id,
        name: schedule.name,
        active: schedule.active,
        status: this.getScheduleStatus(effectiveSchedule),
        sourceSystem: schedule.sourceSystem,
        targetSystem: schedule.targetSystem,
        sourceType: schedule.sourceType,
        targetType: schedule.targetType,
        direction: schedule.direction,
        objectName: schedule.objectName,
        operation: schedule.operation,
        connectorId: schedule.connectorId,
        mappingDefinition: schedule.mappingDefinition,
        sourceDefinition: schedule.sourceDefinition,
        targetDefinition: schedule.targetDefinition,
        nextRunAt: schedule.nextRunAt,
        lastRunAt: schedule.lastRunAt,
        batchSize: schedule.batchSize,
        timingDefinition: persistedTimingDefinition,
        parentScheduleId: schedule.parentScheduleId,
        inheritTimingFromParent: schedule.inheritTimingFromParent
      };
    });
  }

  public async getScheduleFormOptions(instanceId?: string): Promise<ScheduleFormOptions> {
    const client = await this.createClient(instanceId);
    const records = await client.querySchedules(false);

    const collectUnique = (values: Array<string | undefined>, fallback: string[] = []): string[] => {
      const merged = [...fallback, ...values.filter((value): value is string => Boolean(value && value.trim()))];
      return Array.from(new Set(merged.map((value) => value.trim()).filter(Boolean)));
    };

    const readPicklist = async (fieldApiName: string): Promise<string[]> => {
      try {
        const values = await client.getObjectPicklistValues("MSD_Schedule__c", fieldApiName);
        return values.map((entry) => entry.value).filter(Boolean);
      } catch {
        return [];
      }
    };

    const [sourceSystems, targetSystems, operations, directions] = await Promise.all([
      readPicklist("SourceSystem__c"),
      readPicklist("TargetSystem__c"),
      readPicklist("Operation__c"),
      readPicklist("MSD_Direction__c")
    ]);

    return {
      objectNames: collectUnique(records.map((record) => record.ObjectName__c), [
        "Account",
        "Contact",
        "Lead",
        "Order",
        "Opportunity"
      ]),
      operations: collectUnique(records.map((record) => record.Operation__c), operations.length ? operations : [
        "Insert",
        "Update",
        "Upsert",
        "Delete"
      ]),
      sourceSystems: collectUnique(records.map((record) => record.SourceSystem__c), sourceSystems),
      targetSystems: collectUnique(records.map((record) => record.TargetSystem__c), targetSystems),
      directions: collectUnique(records.map((record) => record.MSD_Direction__c), directions.length ? directions : [
        "Outbound",
        "Inbound",
        "Bidirectional"
      ])
    };
  }

  public async listConnectors(instanceId?: string): Promise<ConnectorListItem[]> {
    const client = await this.createClient(instanceId);
    const connectors = await client.queryConnectors();

    return connectors.map((connector) => ({
      id: connector.id,
      name: connector.name,
      active: connector.active,
      connectorType: connector.connectorType,
      targetSystem: connector.targetSystem,
      direction: connector.direction,
      timeoutMs: connector.timeoutMs,
      maxRetries: connector.maxRetries,
      description: connector.description,
      hasSecret: Boolean(connector.secretKey),
      parameterKeys: Object.keys(connector.parameters).sort()
    }));
  }

  public async testConnector(connectorId: string, instanceId?: string): Promise<ConnectorTestResult> {
    const client = await this.createClient(instanceId);
    const config = await client.queryConnector(connectorId);
    const registry = new ConnectorRegistry();
    const connector = registry.getConnectorByConfig(config);

    try {
      const ok = await connector.testConnection();
      return {
        ok,
        connectorId: config.id,
        connectorName: config.name,
        connectorType: config.connectorType,
        message: ok ? "Connection test successful" : "Connection test returned false"
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown connector test error";
      return {
        ok: false,
        connectorId: config.id,
        connectorName: config.name,
        connectorType: config.connectorType,
        message
      };
    }
  }

  public async triggerScheduleNow(
    agentId: string,
    scheduleId: string,
    instanceId?: string
  ): Promise<{ triggered: boolean; message: string; scheduleId: string; scheduleName: string }> {
    const config = this.resolveInstance(instanceId).config;
    return runScheduleNow(this.createLogger(), agentId, scheduleId, config);
  }

  public async dryRunScheduleSource(scheduleId: string, instanceId?: string): Promise<ScheduleDryRunResult> {
    const schedule = (await this.listSchedules(instanceId)).find((item) => item.id === scheduleId);
    if (!schedule) {
      throw new Error(`Schedule not found: ${scheduleId}`);
    }

    if (!schedule.sourceType || !schedule.sourceDefinition) {
      return {
        ok: false,
        scheduleId,
        scheduleName: schedule.name,
        sourceType: schedule.sourceType,
        message: "Quelle unvollständig: Source Type und Source Definition werden benötigt"
      };
    }

    try {
      const preview = await this.previewSource(
        schedule.sourceType,
        schedule.sourceDefinition,
        schedule.connectorId,
        1,
        instanceId
      );

      return {
        ok: true,
        scheduleId,
        scheduleName: schedule.name,
        sourceType: schedule.sourceType,
        rowCount: preview.rowCount,
        fields: preview.fields,
        message: `Quelle erreichbar (${preview.rowCount} Testdatensatz/saetze gelesen)`
      };
    } catch (error) {
      return {
        ok: false,
        scheduleId,
        scheduleName: schedule.name,
        sourceType: schedule.sourceType,
        message: error instanceof Error ? error.message : "Dry-Run fehlgeschlagen"
      };
    }
  }

  public async listRuns(limit = 50, instanceId?: string): Promise<RunListItem[]> {
    const client = await this.createClient(instanceId);
    const runs = await client.queryRuns(limit);
    return runs.map((run) => ({
      id: run.Id,
      scheduleId: run.MSD_Schedule__c,
      scheduleName: run.MSD_Schedule__r?.Name,
      status: run.MSD_Status__c || "Unknown",
      startedAt: run.MSD_StartedAt__c,
      finishedAt: run.MSD_FinishedAt__c,
      recordsRead: run.MSD_RecordsRead__c,
      recordsProcessed: run.MSD_RecordsProcessed__c,
      recordsSucceeded: run.MSD_RecordsSucceeded__c,
      recordsFailed: run.MSD_RecordsFailed__c,
      errorMessage: run.MSD_ErrorMessage__c
    }));
  }

  public async listLogs(runId: string, limit = 200, instanceId?: string): Promise<LogListItem[]> {
    const client = await this.createClient(instanceId);
    const logs = await client.queryLogsByRunId(runId, limit);
    return logs.map((log) => ({
      id: log.Id,
      runId: log.MSD_Run__c,
      scheduleName: log.MSD_Run__r?.MSD_Schedule__r?.Name,
      level: log.MSD_Level__c,
      step: log.MSD_Step__c,
      message: log.MSD_Message__c,
      recordKey: log.MSD_RecordKey__c,
      createdAt: log.CreatedDate
    }));
  }

  public async summarizeLogsByRange(range: LogChartRange, instanceId?: string): Promise<LogChartSummary> {
    const { from, to } = this.getRangeWindow(range);
    const buckets = this.createLogBuckets(range, from, to);
    const items = await this.listLogsByRange(from.toISOString(), to.toISOString(), "all", 5000, instanceId);

    for (const item of items) {
      if (!item.createdAt) {
        continue;
      }

      const createdAt = new Date(item.createdAt);
      if (Number.isNaN(createdAt.getTime())) {
        continue;
      }

      const bucket = buckets.find((entry) => {
        const start = new Date(entry.start).getTime();
        const end = new Date(entry.end).getTime();
        const value = createdAt.getTime();
        return value >= start && value < end;
      });

      if (!bucket) {
        continue;
      }

      bucket.total += 1;
      if ((item.level || "").toUpperCase() === "ERROR") {
        bucket.errors += 1;
      }
    }

    return {
      range,
      buckets
    };
  }

  public async listLogsByRange(
    startIso: string,
    endIso: string,
    type: "all" | "error" = "all",
    limit = 300,
    instanceId?: string
  ): Promise<LogListItem[]> {
    const client = await this.createClient(instanceId);
    const records = await client.queryLogsByDateRange(startIso, endIso, Math.max(limit * 4, 1000));

    const mapped = records.map((log) => ({
      id: log.Id,
      runId: log.MSD_Run__c,
      scheduleName: log.MSD_Run__r?.MSD_Schedule__r?.Name,
      level: log.MSD_Level__c,
      step: log.MSD_Step__c,
      message: log.MSD_Message__c,
      recordKey: log.MSD_RecordKey__c,
      createdAt: log.CreatedDate
    }));

    const filtered = type === "error"
      ? mapped.filter((item) => (item.level || "").toUpperCase() === "ERROR")
      : mapped;

    return filtered.slice(0, Math.max(1, Math.min(limit, 1000)));
  }

  public async previewSql(
    connectorId: string,
    query: string,
    limit = 10,
    instanceId?: string
  ): Promise<SqlPreviewResult> {
    const client = await this.createClient(instanceId);
    const connector = await client.queryConnector(connectorId);
    if (connector.connectorType.toLowerCase() !== "mssql") {
      throw new Error(`SQL preview is currently only supported for MSSQL connectors, got ${connector.connectorType}`);
    }

    const database = new MssqlDatabase({
      server: getRequiredString(connector.parameters, "server"),
      port: getOptionalNumber(connector.parameters, "port"),
      database: getRequiredString(connector.parameters, "database"),
      user: getRequiredString(connector.parameters, "user"),
      password: resolvePassword(connector),
      encrypt: getOptionalBoolean(connector.parameters, "encrypt"),
      trustServerCertificate: getOptionalBoolean(connector.parameters, "trustServerCertificate"),
      connectionTimeout: connector.timeoutMs,
      requestTimeout: connector.timeoutMs
    });

    const normalizedQuery = query.trim().replace(/;\s*$/, "");
    if (!normalizedQuery) {
      throw new Error("SQL query must not be empty");
    }

    const limitedQuery = `SELECT TOP (${Math.max(1, Math.min(limit, 100))}) * FROM (${normalizedQuery}) AS preview_query`;
    try {
      const result = await database.query<Record<string, unknown>>(limitedQuery);
      const rows = result.recordset.map((row) => ({ ...row }));
      const fields = rows.length > 0 ? Object.keys(rows[0]) : [];
      return {
        fields,
        rows,
        rowCount: rows.length
      };
    } finally {
      await database.close();
    }
  }

  public async previewSource(
    sourceType: string,
    sourceDefinition: string,
    connectorId: string | undefined,
    limit = 10,
    instanceId?: string
  ): Promise<SourcePreviewResult> {
    const normalizedType = String(sourceType || "").trim().toUpperCase();
    const normalizedLimit = Math.max(1, Math.min(limit, 100));
    const trimmedDefinition = String(sourceDefinition || "").trim();

    if (!trimmedDefinition) {
      throw new Error("Quellabfrage darf nicht leer sein");
    }

    if (normalizedType === "MSSQL_SQL") {
      if (!connectorId) {
        throw new Error("Für SQL-Vorschau muss ein MSSQL-Connector ausgewählt sein");
      }

      return this.previewSql(connectorId, trimmedDefinition, normalizedLimit, instanceId);
    }

    if (normalizedType === "SALESFORCE_SOQL") {
      const client = await this.createClient(instanceId);
      const limitedSoql = /\bLIMIT\s+\d+\b/i.test(trimmedDefinition)
        ? trimmedDefinition.replace(/;\s*$/, "")
        : `${trimmedDefinition.replace(/;\s*$/, "")}\nLIMIT ${normalizedLimit}`;
      const rows = (await client.queryGeneric(limitedSoql)).slice(0, normalizedLimit).map((row) => {
        const normalizedRow = { ...row };
        delete (normalizedRow as { attributes?: unknown }).attributes;
        return normalizedRow;
      });
      const fields = rows.length > 0 ? Object.keys(rows[0]) : [];

      return {
        fields,
        rows,
        rowCount: rows.length
      };
    }

    throw new Error(`Source Type ${sourceType} wird für Vorschau/Test noch nicht unterstützt`);
  }

  public async getSourceFields(
    sourceType: string,
    sourceDefinition: string,
    objectName: string | undefined,
    connectorId: string | undefined,
    instanceId?: string
  ): Promise<SourceFieldMetadata[]> {
    const normalizedType = String(sourceType || "").trim().toUpperCase();

    if (normalizedType === "SALESFORCE_SOQL") {
      const client = await this.createClient(instanceId);
      const resolvedObjectName = String(objectName || "").trim() || this.extractSalesforceObjectName(sourceDefinition);
      if (!resolvedObjectName) {
        throw new Error("Salesforce-Objekt konnte aus Object oder SOQL-FROM nicht ermittelt werden");
      }

      const objectFields = await client.describeObjectFields(resolvedObjectName);
      const byName = new Map(
        objectFields.map((field) => [field.name.toLowerCase(), field])
      );

      const selectedFields = this.extractSalesforceSelectedFields(sourceDefinition);
      if (!selectedFields.length) {
        return objectFields.map((field) => ({
          name: field.name,
          label: field.label,
          type: field.type
        }));
      }

      const seen = new Set<string>();
      const mapped = selectedFields.map((selectedField): SourceFieldMetadata | null => {
        const normalized = selectedField.expression.toLowerCase();
        const direct = byName.get(normalized);
        const resolvedName = selectedField.alias || direct?.name || selectedField.expression;

        if (seen.has(resolvedName.toLowerCase())) {
          return null;
        }
        seen.add(resolvedName.toLowerCase());

        if (direct) {
          return {
            name: resolvedName,
            label: selectedField.alias
              ? `${selectedField.alias} (${direct.label})`
              : direct.label,
            type: direct.type
          };
        }

        const isCalculated = selectedField.expression.includes("(");
        const isAggregate = /^\s*(COUNT|SUM|AVG|MIN|MAX)\s*\(/i.test(selectedField.expression);
        return {
          name: resolvedName,
          label: selectedField.alias
            ? `${selectedField.alias} (${selectedField.expression})`
            : selectedField.expression,
          type: isAggregate ? "aggregate" : isCalculated ? "calculated" : "unknown"
        };
      }).filter((entry): entry is SourceFieldMetadata => entry !== null);

      return mapped;
    }

    if (normalizedType === "MSSQL_SQL") {
      if (!connectorId) {
        throw new Error("Für SQL-Feldmetadaten muss ein MSSQL-Connector ausgewählt sein");
      }

      return this.getMssqlSourceFields(connectorId, sourceDefinition, instanceId);
    }

    throw new Error(`Source Type ${sourceType} wird für Feldmetadaten noch nicht unterstützt`);
  }

  public async previewMapping(mappingDefinition: string, sourceData: Record<string, unknown>[]): Promise<MappingPreviewResult> {
    const parser = new MappingDefinitionParser();
    const engine = new MappingDefinitionEngine();
    const parsed = parser.parse(mappingDefinition);
    const rows = await Promise.all(sourceData.map(async (row) => (await engine.mapRecord(row, parsed.lines)).values));
    const fields = rows.length > 0 ? Object.keys(rows[0]) : parsed.lines.map((line) => line.targetField);
    return { fields, rows };
  }

  public async saveSchedule(
    input: ScheduleMutationInput,
    instanceId?: string
  ): Promise<{ id: string; action: "created" | "updated" }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const normalizedParentScheduleId =
      input.parentScheduleId && input.parentScheduleId !== input.id
        ? input.parentScheduleId
        : undefined;

    const fields: Record<string, any> = {
      Active__c: input.active,
      SourceSystem__c: input.sourceSystem,
      TargetSystem__c: input.targetSystem,
      ObjectName__c: input.objectName,
      Operation__c: input.operation,
      MSD_Connector__c: input.connectorId,
      MSD_MappingDefinition__c: input.mappingDefinition,
      MSD_Direction__c: input.direction,
      MSD_SourceType__c: input.sourceType,
      MSD_TargetType__c: input.targetType,
      MSD_SourceDefinition__c: input.sourceDefinition,
      MSD_TargetDefinition__c: this.mergeScheduleEnvelope(input.targetDefinition, {
        timingDefinition: input.timingDefinition,
        parentScheduleId: normalizedParentScheduleId,
        inheritTimingFromParent: normalizedParentScheduleId ? input.inheritTimingFromParent : false
      }),
      BatchSize__c: input.batchSize,
      NextRunAt__c: input.nextRunAt,
      LastRunAt__c: input.lastRunAt
    };

    if (input.id) {
      // Update existing record - Name field is read-only (auto-number), never update it
      await client.updateScheduleRecord(input.id, fields);
      this.saveLocalTimingDefinition(resolvedInstance.id, input.id, input.timingDefinition);
      return { id: input.id, action: "updated" };
    }

    // Create new record - Name field should not be set as it's auto-generated
    const id = await client.createScheduleRecord(fields);
    this.saveLocalTimingDefinition(resolvedInstance.id, id, input.timingDefinition);
    return { id, action: "created" };
  }

  public async duplicateSchedule(
    scheduleId: string,
    newName?: string,
    instanceId?: string
  ): Promise<{ id: string; action: "created" }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const record = await client.queryScheduleById(scheduleId);
    const cloneName = newName?.trim() || `${record.Name} (Copy)`;
    const id = await client.createScheduleRecord({
      Name: cloneName,
      Active__c: false,
      SourceSystem__c: record.SourceSystem__c,
      TargetSystem__c: record.TargetSystem__c,
      ObjectName__c: record.ObjectName__c,
      Operation__c: record.Operation__c,
      MSD_Connector__c: record.MSD_Connector__c,
      MSD_MappingDefinition__c: record.MSD_MappingDefinition__c,
      MSD_Direction__c: record.MSD_Direction__c,
      MSD_SourceType__c: record.MSD_SourceType__c,
      MSD_TargetType__c: record.MSD_TargetType__c,
      MSD_SourceDefinition__c: record.MSD_SourceDefinition__c,
      MSD_TargetDefinition__c: record.MSD_TargetDefinition__c,
      BatchSize__c: record.BatchSize__c,
      NextRunAt__c: record.NextRunAt__c,
      LastRunAt__c: record.LastRunAt__c
    });

    this.copyLocalTimingDefinition(resolvedInstance.id, scheduleId, id);

    return { id, action: "created" };
  }

  public async deleteSchedule(scheduleId: string, instanceId?: string): Promise<DeleteScheduleResult> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const schedules = await this.listSchedules(resolvedInstance.id);
    const scheduleById = new Map(schedules.map((schedule) => [schedule.id, schedule]));

    if (!scheduleById.has(scheduleId)) {
      throw new Error(`Schedule not found: ${scheduleId}`);
    }

    const childrenByParent = new Map<string, ScheduleListItem[]>();
    for (const schedule of schedules) {
      const parentId = String(schedule.parentScheduleId || "").trim();
      if (!parentId || parentId === schedule.id) {
        continue;
      }

      const children = childrenByParent.get(parentId) || [];
      children.push(schedule);
      childrenByParent.set(parentId, children);
    }

    const deletedIds: string[] = [];
    const deletedNames: string[] = [];
    const visited = new Set<string>();

    const collect = (currentId: string) => {
      if (!currentId || visited.has(currentId)) {
        return;
      }

      visited.add(currentId);
      const children = (childrenByParent.get(currentId) || []).slice().sort((a, b) =>
        String(a.name || "").localeCompare(String(b.name || ""), "de", { sensitivity: "base" })
      );
      children.forEach((child) => collect(child.id));
      deletedIds.push(currentId);
      deletedNames.push(scheduleById.get(currentId)?.name || currentId);
    };

    collect(scheduleId);

    for (const id of deletedIds) {
      await client.deleteScheduleRecord(id);
      this.removeLocalTimingDefinition(resolvedInstance.id, id);
    }

    return { deletedIds, deletedNames };
  }

  public async saveConnector(
    input: ConnectorMutationInput,
    instanceId?: string
  ): Promise<{ id: string; action: "created" | "updated" }> {
    const client = await this.createClient(instanceId);
    const fields = {
      Name: input.name,
      MSD_Active__c: input.active,
      MSD_ConnectorType__c: input.connectorType,
      MSD_TargetSystem__c: input.targetSystem,
      MSD_Direction__c: input.direction,
      MSD_SecretKey__c: input.secretKey,
      MSD_TimeoutMs__c: input.timeoutMs,
      MSD_MaxRetries__c: input.maxRetries,
      MSD_Description__c: input.description,
      MSD_Parameters__c: JSON.stringify(input.parameters || {})
    };

    if (input.id) {
      await client.updateConnectorRecord(input.id, fields);
      return { id: input.id, action: "updated" };
    }

    const id = await client.createConnectorRecord(fields);
    return { id, action: "created" };
  }

  public async getConnectionGraph(instanceId?: string): Promise<ConnectionGraph> {
    const [schedules, connectors] = await Promise.all([
      this.listSchedules(instanceId),
      this.listConnectors(instanceId)
    ]);

    const scheduleById = new Map(schedules.map((schedule) => [schedule.id, schedule]));
    const childrenByParent = new Map<string, ScheduleListItem[]>();
    const rootSchedules: ScheduleListItem[] = [];

    for (const schedule of schedules) {
      const parentId = schedule.parentScheduleId;
      if (parentId && parentId !== schedule.id && scheduleById.has(parentId)) {
        const children = childrenByParent.get(parentId) || [];
        children.push(schedule);
        childrenByParent.set(parentId, children);
        continue;
      }
      rootSchedules.push(schedule);
    }

    const scheduleDepth = new Map<string, number>();
    const orderedSchedules: ScheduleListItem[] = [];
    const visitedSchedules = new Set<string>();

    const visitSchedule = (schedule: ScheduleListItem, depth: number, path: Set<string>) => {
      if (visitedSchedules.has(schedule.id) || path.has(schedule.id)) {
        return;
      }

      path.add(schedule.id);
      visitedSchedules.add(schedule.id);
      scheduleDepth.set(schedule.id, depth);
      orderedSchedules.push(schedule);

      const children = (childrenByParent.get(schedule.id) || []).sort((a, b) =>
        a.name.localeCompare(b.name, "de", { sensitivity: "base" })
      );
      for (const child of children) {
        visitSchedule(child, depth + 1, path);
      }

      path.delete(schedule.id);
    };

    for (const root of rootSchedules.sort((a, b) => a.name.localeCompare(b.name, "de", { sensitivity: "base" }))) {
      visitSchedule(root, 0, new Set<string>());
    }

    for (const schedule of schedules) {
      if (!visitedSchedules.has(schedule.id)) {
        visitSchedule(schedule, 0, new Set<string>());
      }
    }

    const connectorNodes: GraphNode[] = connectors.map((connector, index) => ({
      id: `connector-${connector.id}`,
      kind: "connector",
      label: connector.name,
      subtitle: connector.connectorType || "Connector",
      connectorType: connector.connectorType,
      x: 72,
      y: 70 + index * 104,
      refId: connector.id
    }));

    const scheduleNodes: GraphNode[] = orderedSchedules.map((schedule, index) => ({
      id: `schedule-${schedule.id}`,
      kind: "scheduler",
      label: schedule.name,
      subtitle: `${schedule.objectName || "-"} | ${schedule.direction || "source-to-target"}${schedule.parentScheduleId ? " | Parent" : ""}`,
      direction: schedule.direction,
      objectName: schedule.objectName,
      directionIcon: this.toDirectionIcon(schedule.direction),
      x: 456 + (scheduleDepth.get(schedule.id) || 0) * 300,
      y: 70 + index * 104,
      refId: schedule.id
    }));

    const edges: GraphEdge[] = [];
    for (const schedule of orderedSchedules) {
      const hasParent = !!(schedule.parentScheduleId && scheduleById.has(schedule.parentScheduleId));

      if (!hasParent && schedule.connectorId) {
        edges.push({
          id: `edge-${schedule.id}-${schedule.connectorId}`,
          from: `connector-${schedule.connectorId}`,
          to: `schedule-${schedule.id}`,
          direction: schedule.direction
        });
      }

      if (hasParent) {
        edges.push({
          id: `edge-parent-${schedule.parentScheduleId}-${schedule.id}`,
          from: `schedule-${schedule.parentScheduleId}`,
          to: `schedule-${schedule.id}`
        });
      }
    }

    return {
      nodes: [...connectorNodes, ...scheduleNodes],
      edges
    };
  }

  private async createClient(instanceId?: string): Promise<SalesforceClient> {
    const resolved = this.resolveInstance(instanceId);
    const client = new SalesforceClient(resolved.config);
    await client.login();
    return client;
  }

  private extractSalesforceObjectName(sourceDefinition: string): string | undefined {
    const match = String(sourceDefinition || "").match(/\bFROM\s+([A-Za-z0-9_]+)/i);
    return match?.[1]?.trim();
  }

  private extractSalesforceSelectClause(sourceDefinition: string): string | undefined {
    const match = String(sourceDefinition || "").match(/\bSELECT\b([\s\S]*?)\bFROM\b/i);
    return match?.[1]?.trim();
  }

  private splitSoqlSelectFields(selectClause: string): string[] {
    const fields: string[] = [];
    let buffer = "";
    let depth = 0;

    for (const char of selectClause) {
      if (char === "(") {
        depth += 1;
      } else if (char === ")" && depth > 0) {
        depth -= 1;
      }

      if (char === "," && depth === 0) {
        const value = buffer.trim();
        if (value) {
          fields.push(value);
        }
        buffer = "";
        continue;
      }

      buffer += char;
    }

    const tail = buffer.trim();
    if (tail) {
      fields.push(tail);
    }

    return fields;
  }

  private parseSoqlSelectedField(fieldToken: string): ParsedSoqlSelectedField | null {
    const cleaned = String(fieldToken || "").trim().replace(/\s+/g, " ");
    if (!cleaned) {
      return null;
    }

    const asMatch = cleaned.match(/^(.*)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)$/i);
    if (asMatch) {
      return {
        expression: asMatch[1].trim(),
        alias: asMatch[2].trim()
      };
    }

    const parts = cleaned.split(" ").filter(Boolean);
    if (parts.length > 1) {
      const possibleAlias = parts[parts.length - 1];
      const expression = parts.slice(0, -1).join(" ").trim();
      if (expression && /^[A-Za-z_][A-Za-z0-9_]*$/.test(possibleAlias)) {
        return {
          expression,
          alias: possibleAlias
        };
      }
    }

    return { expression: cleaned };
  }

  private extractSalesforceSelectedFields(sourceDefinition: string): ParsedSoqlSelectedField[] {
    const selectClause = this.extractSalesforceSelectClause(sourceDefinition);
    if (!selectClause) {
      return [];
    }

    const tokens = this.splitSoqlSelectFields(selectClause)
      .map((token) => this.parseSoqlSelectedField(token))
      .filter((entry): entry is ParsedSoqlSelectedField => Boolean(entry));

    const deduped = new Map<string, ParsedSoqlSelectedField>();
    for (const entry of tokens) {
      const key = `${entry.expression.toLowerCase()}::${String(entry.alias || "").toLowerCase()}`;
      deduped.set(key, entry);
    }

    return [...deduped.values()];
  }

  private async getMssqlSourceFields(
    connectorId: string,
    query: string,
    instanceId?: string
  ): Promise<SourceFieldMetadata[]> {
    const client = await this.createClient(instanceId);
    const connector = await client.queryConnector(connectorId);
    if (connector.connectorType.toLowerCase() !== "mssql") {
      throw new Error(`SQL-Feldmetadaten werden nur für MSSQL-Connectoren unterstützt, erhalten: ${connector.connectorType}`);
    }

    const database = new MssqlDatabase({
      server: getRequiredString(connector.parameters, "server"),
      port: getOptionalNumber(connector.parameters, "port"),
      database: getRequiredString(connector.parameters, "database"),
      user: getRequiredString(connector.parameters, "user"),
      password: resolvePassword(connector),
      encrypt: getOptionalBoolean(connector.parameters, "encrypt"),
      trustServerCertificate: getOptionalBoolean(connector.parameters, "trustServerCertificate"),
      connectionTimeout: connector.timeoutMs,
      requestTimeout: connector.timeoutMs
    });

    const normalizedQuery = query.trim().replace(/;\s*$/, "");
    if (!normalizedQuery) {
      throw new Error("SQL query must not be empty");
    }

    const metadataQuery = `SELECT TOP (0) * FROM (${normalizedQuery}) AS metadata_query`;
    try {
      const result = await database.query<Record<string, unknown>>(metadataQuery);
      const recordset = result.recordset as Array<Record<string, unknown>> & {
        columns?: Record<string, { name?: string; type?: { name?: string; declaration?: string } }>;
      };
      const columnEntries = Object.values(recordset.columns || {});

      if (columnEntries.length > 0) {
        return columnEntries
          .map((column) => ({
            name: String(column.name || "").trim(),
            type: String(column.type?.declaration || column.type?.name || "unknown").trim()
          }))
          .filter((column) => column.name);
      }

      const preview = await this.previewSql(connectorId, normalizedQuery, 1, instanceId);
      return preview.fields.map((fieldName) => ({ name: fieldName, type: "unknown" }));
    } finally {
      await database.close();
    }
  }

  private resolveInstance(instanceId?: string): ResolvedInstance {
    const instances = resolveInstances();
    if (instances.length === 0) {
      throw new Error(
        "Keine Salesforce-Instanz konfiguriert. Setze SF_LOGIN_URL/SF_CLIENT_ID/SF_CLIENT_SECRET oder SF_INSTANCES_JSON."
      );
    }

    if (!instanceId) {
      return instances[0];
    }

    const selected = instances.find((item) => item.id === instanceId);
    if (!selected) {
      throw new Error(`Unknown Salesforce instance: ${instanceId}`);
    }

    return selected;
  }

  private createLogger() {
    return pino({
      level: process.env.LOG_LEVEL || "info"
    });
  }

  private toDirectionIcon(direction?: string): string {
    const normalized = direction?.toLowerCase() || "";
    if (normalized.includes("bidirectional") || normalized.includes("both")) {
      return "↔";
    }

    if (normalized.includes("target-to-source") || normalized.includes("inbound") || normalized.includes("import")) {
      return "←";
    }

    return "→";
  }

  private getRangeWindow(range: LogChartRange): { from: Date; to: Date } {
    const to = new Date();
    const from = new Date(to);

    if (range === "last_hour") {
      from.setHours(from.getHours() - 1);
      return { from, to };
    }

    if (range === "last_24h") {
      from.setHours(from.getHours() - 24);
      return { from, to };
    }

    from.setDate(from.getDate() - 30);
    return { from, to };
  }

  private createLogBuckets(range: LogChartRange, from: Date, to: Date): LogChartBucket[] {
    const buckets: LogChartBucket[] = [];

    if (range === "last_hour") {
      const aligned = new Date(from);
      aligned.setSeconds(0, 0);
      const minute = aligned.getMinutes();
      aligned.setMinutes(minute - (minute % 5));

      for (let index = 0; index < 12; index += 1) {
        const start = new Date(aligned.getTime() + index * 5 * 60_000);
        const end = new Date(start.getTime() + 5 * 60_000);
        buckets.push({
          label: start.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          errors: 0
        });
      }

      return buckets;
    }

    if (range === "last_24h") {
      const aligned = new Date(from);
      aligned.setMinutes(0, 0, 0);

      for (let index = 0; index < 24; index += 1) {
        const start = new Date(aligned.getTime() + index * 60 * 60_000);
        const end = new Date(start.getTime() + 60 * 60_000);
        buckets.push({
          label: start.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          errors: 0
        });
      }

      return buckets;
    }

    const aligned = new Date(from);
    aligned.setHours(0, 0, 0, 0);

    for (let index = 0; index < 30; index += 1) {
      const start = new Date(aligned);
      start.setDate(aligned.getDate() + index);
      const end = new Date(start);
      end.setDate(start.getDate() + 1);
      buckets.push({
        label: start.toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" }),
        start: start.toISOString(),
        end: end.toISOString(),
        total: 0,
        errors: 0
      });
    }

    if (buckets.length > 0) {
      const firstStart = new Date(buckets[0].start);
      if (from > firstStart) {
        buckets[0].start = from.toISOString();
      }

      const last = buckets[buckets.length - 1];
      const lastEnd = new Date(last.end);
      if (to < lastEnd) {
        last.end = to.toISOString();
      }
    }

    return buckets;
  }

  private toIntegrationSchedule(record: SalesforceScheduleRecord): IntegrationSchedule {
    const extractedTimingDefinition = this.extractTimingDefinition(record.MSD_TargetDefinition__c);
    const extractedHierarchy = this.extractHierarchySettings(record.MSD_TargetDefinition__c);

    return {
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
      targetDefinition: this.stripScheduleEnvelopeFromTargetDefinition(record.MSD_TargetDefinition__c),
      batchSize: record.BatchSize__c || 100,
      nextRunAt: record.NextRunAt__c,
      lastRunAt: record.LastRunAt__c,
      timingDefinition: extractedTimingDefinition,
      parentScheduleId: extractedHierarchy.parentScheduleId,
      inheritTimingFromParent: extractedHierarchy.inheritTimingFromParent
    };
  }

  private extractHierarchySettings(targetDefinition?: string): {
    parentScheduleId?: string;
    inheritTimingFromParent?: boolean;
  } {
    const trimmedTargetDefinition = String(targetDefinition || "").trim();
    if (!trimmedTargetDefinition) {
      return {};
    }

    try {
      const candidate = JSON.parse(trimmedTargetDefinition) as unknown;
      if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
        return {};
      }

      const parentScheduleId = (candidate as PersistedTargetDefinitionEnvelope).parentScheduleId;
      const inheritTimingFromParent = (candidate as PersistedTargetDefinitionEnvelope).inheritTimingFromParent;

      return {
        parentScheduleId:
          typeof parentScheduleId === "string" && parentScheduleId.trim()
            ? parentScheduleId.trim()
            : undefined,
        inheritTimingFromParent: inheritTimingFromParent === true
      };
    } catch {
      return {};
    }
  }

  private extractTimingDefinition(targetDefinition?: string): string | undefined {
    const trimmedTargetDefinition = String(targetDefinition || "").trim();
    if (!trimmedTargetDefinition) {
      return undefined;
    }

    try {
      const candidate = JSON.parse(trimmedTargetDefinition) as unknown;
      if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
        return undefined;
      }

      const timingDefinition = (candidate as PersistedTargetDefinitionEnvelope).timingDefinition;
      return typeof timingDefinition === "string" && timingDefinition.trim() ? timingDefinition : undefined;
    } catch {
      return undefined;
    }
  }

  private stripScheduleEnvelopeFromTargetDefinition(targetDefinition?: string): string | undefined {
    const trimmedTargetDefinition = String(targetDefinition || "").trim();
    if (!trimmedTargetDefinition) {
      return targetDefinition;
    }

    try {
      const candidate = JSON.parse(trimmedTargetDefinition) as unknown;
      if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
        return targetDefinition;
      }

      const sanitized = { ...(candidate as PersistedTargetDefinitionEnvelope) };
      const hadTiming = "timingDefinition" in sanitized;
      const hadParent = "parentScheduleId" in sanitized;
      const hadInheritance = "inheritTimingFromParent" in sanitized;

      if (!hadTiming && !hadParent && !hadInheritance) {
        return targetDefinition;
      }

      delete sanitized.timingDefinition;
      delete sanitized.parentScheduleId;
      delete sanitized.inheritTimingFromParent;
      return JSON.stringify(sanitized, null, 2);
    } catch {
      return targetDefinition;
    }
  }

  private mergeScheduleEnvelope(
    targetDefinition: string | undefined,
    envelope: {
      timingDefinition?: string;
      parentScheduleId?: string;
      inheritTimingFromParent?: boolean;
    }
  ): string | undefined {
    const trimmed = String(targetDefinition || "").trim();

    let base: PersistedTargetDefinitionEnvelope;
    if (!trimmed) {
      base = {};
    } else {
      try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          return targetDefinition;
        }
        base = { ...(parsed as PersistedTargetDefinitionEnvelope) };
      } catch {
        return targetDefinition;
      }
    }

    const timingDefinition = String(envelope.timingDefinition || "").trim();
    if (timingDefinition) {
      base.timingDefinition = timingDefinition;
    } else {
      delete base.timingDefinition;
    }

    const parentScheduleId = String(envelope.parentScheduleId || "").trim();
    if (parentScheduleId) {
      base.parentScheduleId = parentScheduleId;
      base.inheritTimingFromParent = envelope.inheritTimingFromParent === true;
    } else {
      delete base.parentScheduleId;
      delete base.inheritTimingFromParent;
    }

    return JSON.stringify(base, null, 2);
  }

  private saveLocalTimingDefinition(instanceId: string, scheduleId: string, timingDefinition?: string): void {
    const store = readLocalScheduleTimingStore();
    const scopedStore = { ...(store[instanceId] || {}) };
    const trimmedTimingDefinition = String(timingDefinition || "").trim();

    if (trimmedTimingDefinition) {
      scopedStore[scheduleId] = trimmedTimingDefinition;
    } else {
      delete scopedStore[scheduleId];
    }

    if (Object.keys(scopedStore).length > 0) {
      store[instanceId] = scopedStore;
    } else {
      delete store[instanceId];
    }

    writeLocalScheduleTimingStore(store);
  }

  private copyLocalTimingDefinition(instanceId: string, sourceScheduleId: string, targetScheduleId: string): void {
    const store = readLocalScheduleTimingStore();
    const scopedStore = store[instanceId];
    if (!scopedStore || !scopedStore[sourceScheduleId]) {
      return;
    }

    store[instanceId] = {
      ...scopedStore,
      [targetScheduleId]: scopedStore[sourceScheduleId]
    };

    writeLocalScheduleTimingStore(store);
  }

  private getScheduleStatus(schedule: IntegrationSchedule): "due" | "scheduled" | "inactive" {
    if (!schedule.active) {
      return "inactive";
    }

    const profileSchedulerDue = this.isSelectedImportProfileSchedulerDue(schedule.targetDefinition);
    if (profileSchedulerDue === false) {
      return "scheduled";
    }

    if (schedule.nextRunAt) {
      const timestamp = new Date(schedule.nextRunAt).getTime();
      if (!Number.isNaN(timestamp)) {
        return timestamp <= Date.now() ? "due" : "scheduled";
      }
    }

    if (String(schedule.timingDefinition || "").trim()) {
      return "scheduled";
    }

    return "due";
  }

  private isSelectedImportProfileSchedulerDue(targetDefinition?: string): boolean | undefined {
    const raw = String(targetDefinition || "").trim();
    if (!raw || !raw.startsWith("{")) {
      return undefined;
    }

    try {
      const parsed = JSON.parse(raw) as {
        selectedImportProfileName?: unknown;
        importProfiles?: Array<{
          name?: unknown;
          active?: unknown;
          schedulerEnabled?: unknown;
          scheduler?: {
            mode?: unknown;
            rules?: Array<{
              days?: unknown;
              startTime?: unknown;
              endTime?: unknown;
            }>;
          };
        }>;
      };

      if (!Array.isArray(parsed.importProfiles) || parsed.importProfiles.length === 0) {
        return undefined;
      }

      const selectedName = String(parsed.selectedImportProfileName || "").trim();
      const selectedProfile = (selectedName
        ? parsed.importProfiles.find((profile) => String(profile?.name || "").trim() === selectedName)
        : parsed.importProfiles[0]) || parsed.importProfiles[0];

      if (!selectedProfile) {
        return undefined;
      }

      if (selectedProfile.active === false || selectedProfile.schedulerEnabled === false) {
        return false;
      }

      const rules = Array.isArray(selectedProfile.scheduler?.rules)
        ? selectedProfile.scheduler?.rules
        : [];

      if (!rules.length) {
        return true;
      }

      const now = new Date();
      const weekdayMap = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
      const weekday = weekdayMap[now.getDay()];
      const nowMinutes = now.getHours() * 60 + now.getMinutes();

      const parseMinutes = (value: unknown, fallback: number): number => {
        const text = String(value || "").trim();
        const match = text.match(/^(\d{1,2}):(\d{2})$/);
        if (!match) {
          return fallback;
        }
        const hours = Number(match[1]);
        const minutes = Number(match[2]);
        if (Number.isNaN(hours) || Number.isNaN(minutes)) {
          return fallback;
        }
        return Math.min(23, Math.max(0, hours)) * 60 + Math.min(59, Math.max(0, minutes));
      };

      const isRuleActiveNow = rules.some((rule) => {
        const days = Array.isArray(rule?.days)
          ? rule.days.map((day) => String(day || "").trim().toLowerCase()).filter(Boolean)
          : [];
        if (days.length > 0 && !days.includes(weekday)) {
          return false;
        }

        const startMinutes = parseMinutes(rule?.startTime, 0);
        const endMinutes = parseMinutes(rule?.endTime, 23 * 60 + 59);

        if (startMinutes <= endMinutes) {
          return nowMinutes >= startMinutes && nowMinutes <= endMinutes;
        }

        // Overnight window, e.g. 22:00-06:00
        return nowMinutes >= startMinutes || nowMinutes <= endMinutes;
      });

      return isRuleActiveNow;
    } catch {
      return undefined;
    }
  }

  private removeLocalTimingDefinition(instanceId: string, scheduleId: string): void {
    const store = readLocalScheduleTimingStore();
    const scopedStore = { ...(store[instanceId] || {}) };

    if (!(scheduleId in scopedStore)) {
      return;
    }

    delete scopedStore[scheduleId];

    if (Object.keys(scopedStore).length > 0) {
      store[instanceId] = scopedStore;
    } else {
      delete store[instanceId];
    }

    writeLocalScheduleTimingStore(store);
  }

  public getTransformFunctions(): Promise<{ functions: Array<{ id: string; label: string; description?: string }> }> {
    return Promise.resolve({
      functions: [
        { id: 'NONE', label: 'Keine Umwandlung', description: 'Feldwert wird nicht transformiert' },
        { id: 'UPPERCASE', label: 'Großbuchstaben', description: 'Alle Zeichen in Großbuchstaben' },
        { id: 'LOWERCASE', label: 'Kleinbuchstaben', description: 'Alle Zeichen in Kleinbuchstaben' },
        { id: 'TRIM', label: 'Whitespace entfernen', description: 'Führende und nachfolgende Leerzeichen entfernen' },
        { id: 'DATE_FORMAT', label: 'Datumsformat', description: 'Datumsformat konvertieren (Parameter: Format-String)' },
        { id: 'CUSTOM', label: 'Benutzerdefiniert', description: 'Benutzerdefinierter Expression (z. B. JavaScript)' }
      ]
    });
  }

  private async getMssqlTables(
    connectorId: string,
    instanceId?: string
  ): Promise<SourceFieldMetadata[]> {
    const client = await this.createClient(instanceId);
    const connector = await client.queryConnector(connectorId);
    if (connector.connectorType.toLowerCase() !== 'mssql') {
      throw new Error(`MSSQL-Tabellen werden nur für MSSQL-Connectoren unterstützt, erhalten: ${connector.connectorType}`);
    }

    const database = new MssqlDatabase({
      server: getRequiredString(connector.parameters, 'server'),
      port: getOptionalNumber(connector.parameters, 'port'),
      database: getRequiredString(connector.parameters, 'database'),
      user: getRequiredString(connector.parameters, 'user'),
      password: resolvePassword(connector),
      encrypt: getOptionalBoolean(connector.parameters, 'encrypt'),
      trustServerCertificate: getOptionalBoolean(connector.parameters, 'trustServerCertificate'),
      connectionTimeout: connector.timeoutMs,
      requestTimeout: connector.timeoutMs
    });

    try {
      const result = await database.query<{ TABLE_NAME: string; TABLE_SCHEMA: string }>(` SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME`);
      return result.recordset.map((row) => ({
        name: row.TABLE_NAME || '',
        label: row.TABLE_SCHEMA ? `${row.TABLE_SCHEMA}.${row.TABLE_NAME}` : row.TABLE_NAME,
        type: 'table'
      }));
    } finally {
      await database.close();
    }
  }

  private normalizeTargetSystem(targetSystem?: string): string {
    return String(targetSystem || "")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, "");
  }

  private isMssqlTargetSystem(normalizedTargetSystem: string): boolean {
    return normalizedTargetSystem === "MSSQL"
      || normalizedTargetSystem === "MSSQLSERVER"
      || normalizedTargetSystem === "MSSQLDB"
      || normalizedTargetSystem === "MSSQLDATABASE"
      || normalizedTargetSystem === "MSSQLSQL"
      || normalizedTargetSystem === "MSSQLTABLE";
  }

  public async getTargetObjects(
    targetSystem?: string,
    connectorId?: string,
    instanceId?: string
  ): Promise<{ objects: Array<{ name: string; label?: string; type: string }> }> {
    const normalizedTargetSystem = this.normalizeTargetSystem(targetSystem);

    if (!normalizedTargetSystem) {
      return { objects: [] };
    }

    if (normalizedTargetSystem === "SALESFORCE") {
      try {
        const client = await this.createClient(instanceId);
        const objects = await client.listObjectMetadata();
        return {
          objects: objects.map((entry) => ({
            name: entry.name,
            label: entry.label,
            type: "object"
          }))
        };
      } catch {
        return { objects: [] };
      }
    }

    if (this.isMssqlTargetSystem(normalizedTargetSystem) && connectorId) {
      try {
        const tables = await this.getMssqlTables(connectorId, instanceId);
        return {
          objects: tables.map((entry) => ({
            name: entry.name,
            label: entry.label,
            type: "table"
          }))
        };
      } catch {
        return { objects: [] };
      }
    }

    return { objects: [] };
  }

  public async getTargetFields(
    targetSystem?: string,
    targetObject?: string,
    connectorId?: string,
    instanceId?: string
  ): Promise<{ fields: Array<{ name: string; type: string; label?: string }> }> {
    const normalizedTargetSystem = this.normalizeTargetSystem(targetSystem);

    if (!normalizedTargetSystem) {
      return { fields: [] };
    }

    if (normalizedTargetSystem === "SALESFORCE" && targetObject && instanceId) {
      try {
        const client = await this.createClient(instanceId);
        const fields = await client.describeObjectFields(targetObject);
        return {
          fields: (fields || []).map((field: any) => ({
            name: field.name || '',
            type: field.type || 'string',
            label: field.label
          }))
        };
      } catch {
        return { fields: [] };
      }
    }

    if (this.isMssqlTargetSystem(normalizedTargetSystem) && connectorId) {
      try {
        const tables = await this.getMssqlTables(connectorId, instanceId);
        return { fields: tables };
      } catch {
        return { fields: [] };
      }
    }

    return { fields: [] };
  }
}
