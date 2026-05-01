import pino from "pino";
import fs from "node:fs";
import path from "node:path";
import { PassThrough } from "node:stream";
import archiver from "archiver";
import {
  ConnectorConfig,
  SalesforceClient,
  SalesforceOrgOverview,
  SalesforceScheduleRecord
} from "../clients/salesforce/salesforce-client";
import { ConnectorRegistry } from "../core/connector-registry/connector-registry";
import { MappingDefinitionEngine } from "../core/mapping-dsl/mapping-definition-engine";
import { MappingDefinitionParser } from "../core/mapping-dsl/mapping-definition-parser";
import {
  MappingDefinitionLine,
  MappingPicklistEntry,
  MappingTargetType,
  MappingTransformType
} from "../core/mapping-dsl/mapping-definition-types";
import {
  getSalesforceConfig,
  SalesforceConfig
} from "../infrastructure/config/salesforce-config";
import { MigrationStagingSqlite } from "../infrastructure/db/migration-staging-sqlite";
import { MssqlDatabase } from "../infrastructure/db/mssql";
import { IntegrationSchedule } from "../types/integration-schedule";
import { runScheduleNow } from "../agent/agent-runner";
import { analyzeUploadedFile, parseFileFromConnector } from "../utils/file-transfer";
import { fetchRestRows } from "../source-adapters/rest/rest-api-source-adapter";

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
const LOCAL_SCHEDULE_HEALTH_FILE = process.env.SF_SCHEDULE_HEALTH_FILE || path.resolve(process.cwd(), "artifacts/schedule-health.json");
const LOCAL_MIGRATIONS_FILE = path.resolve(process.cwd(), "artifacts/migrations.json");
const SALESFORCE_METADATA_DIR = path.resolve(process.cwd(), "salesforce/metadata");
const LOCAL_SCHEDULE_TIMING_VERSION = 1;

type LocalScheduleTimingStore = Record<string, Record<string, string>>;

interface LocalScheduleTimingDocument {
  version: number;
  updatedAt: string;
  instances: LocalScheduleTimingStore;
}

interface LocalScheduleHealthItem {
  consecutiveFailures: number;
  autoDisabled?: boolean;
  autoDisabledAt?: string;
}

interface LocalScheduleHealthDocument {
  version: number;
  updatedAt: string;
  schedules: Record<string, LocalScheduleHealthItem>;
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

function readLocalScheduleHealthStore(): Record<string, LocalScheduleHealthItem> {
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
        autoDisabled: candidate.autoDisabled === true,
        autoDisabledAt: typeof candidate.autoDisabledAt === "string" ? candidate.autoDisabledAt : undefined
      };
      return acc;
    }, {});
  } catch {
    return {};
  }
}

function writeLocalScheduleHealthStore(store: Record<string, LocalScheduleHealthItem>): void {
  const directory = path.dirname(LOCAL_SCHEDULE_HEALTH_FILE);
  fs.mkdirSync(directory, { recursive: true });
  const document: LocalScheduleHealthDocument = {
    version: 1,
    updatedAt: new Date().toISOString(),
    schedules: store
  };
  fs.writeFileSync(LOCAL_SCHEDULE_HEALTH_FILE, JSON.stringify(document, null, 2), "utf8");
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
  autoDisabledDueToErrors?: boolean;
  autoDisabledAt?: string;
}

export interface ConnectorListItem {
  id: string;
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
  hasSecret: boolean;
  parameterKeys: string[];
}

interface SetupExportScheduleItem extends ScheduleMutationInput {
  connectorName?: string;
  parentScheduleName?: string;
}

export interface SetupExportDocument {
  version: number;
  exportedAt: string;
  instanceId: string;
  connectors: ConnectorMutationInput[];
  schedules: SetupExportScheduleItem[];
}

export interface SetupImportResult {
  connectorsCreated: number;
  connectorsUpdated: number;
  schedulesCreated: number;
  schedulesUpdated: number;
}

export interface UploadedFileAnalysisResult {
  connectorId: string;
  fileName: string;
  format: "csv" | "excel" | "json";
  charset: string;
  delimiter: string;
  headers: string[];
  sourceType: "FILE_CSV" | "FILE_EXCEL" | "FILE_JSON";
  sourceDefinition: string;
  mappingDefinition: string;
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

export interface CreateCustomObjectFromSourceInput {
  objectApiName: string;
  sourceFields: SourceFieldMetadata[];
  label?: string;
  fieldOverrides?: Array<{ sourceName: string; type?: string }>;
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
  sourceType?: string;
  targetType?: string;
  x: number;
  y: number;
  refId: string;
}

export interface MigrationFieldMapping {
  sourceColumn: string;
  targetField: string;
  targetFieldLabel?: string;
  targetFieldType?: string;
  targetType?: MappingTargetType;
  transformFunction?: MappingTransformType;
  lookupEnabled?: boolean;
  lookupObject?: string;
  lookupField?: string;
  picklistMappings?: MappingPicklistEntry[];
  isRequired?: boolean;
  transformExpression?: string;
}

export interface MigrationObjectConfig {
  id: string;
  salesforceObject: string;
  salesforceObjectLabel?: string;
  processingMode?: "file" | "sqlite";
  filePath?: string;
  fileFormat?: "csv" | "excel" | "json";
  fileCharset?: string;
  fileDelimiter?: string;
  fileTextQualifier?: string;
  fileRecordCount?: number;
  fileColumns?: string[];
  previewRows?: Record<string, unknown>[];
  stagingMode?: "file" | "sqlite";
  stagingDatabasePath?: string;
  stagingImportedAt?: string;
  stagingStatus?: "pending" | "ready" | "processing" | "done" | "error";
  fieldMappings: MigrationFieldMapping[];
  externalIdField?: string;
  operation: "insert" | "upsert" | "update";
}

export interface MigrationDependency {
  fromObjectId: string;
  toObjectId: string;
  fromField: string;
  toField: string;
  description?: string;
}

export interface MigrationExecutionStep {
  order: number;
  objectId: string;
  description?: string;
}

export interface MigrationConfig {
  id: string;
  name: string;
  description?: string;
  instanceId?: string;
  status: "draft" | "ready" | "running" | "done" | "error";
  createdAt: string;
  updatedAt: string;
  objects: MigrationObjectConfig[];
  dependencies: MigrationDependency[];
  executionPlan: MigrationExecutionStep[];
  lastRunAt?: string;
  lastRunResult?: {
    startedAt: string;
    finishedAt?: string;
    steps: Array<{
      objectId: string;
      salesforceObject: string;
      status: "pending" | "running" | "done" | "error";
      recordsProcessed?: number;
      recordsSucceeded?: number;
      recordsFailed?: number;
      errorMessage?: string;
    }>;
  };
}

export interface MigrationFailedRecord {
  rowIndex: number;
  sourceRecord: Record<string, unknown>;
  mappedRecord?: Record<string, unknown>;
  error: string;
  errorType: 'mapping' | 'salesforce';
}

export interface MigrationRunResult {
  migrationId: string;
  startedAt: string;
  reportPath?: string;
  steps: Array<{
    objectId: string;
    salesforceObject: string;
    status: "done" | "error";
    recordsProcessed: number;
    recordsSucceeded: number;
    recordsFailed: number;
    errorMessage?: string;
    failedRecordsId?: string;
  }>;
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
  const inlinePassword = typeof config.parameters?.password === "string"
    ? config.parameters.password.trim()
    : "";
  if (inlinePassword) {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`Connector ${config.name} has no password configured. Set parameters.password or configure MSD_SecretKey__c.`);
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
  private readonly migrationStaging = new MigrationStagingSqlite();

  private getEffectiveMigrationProcessingMode(obj: MigrationObjectConfig): "file" | "sqlite" {
    if (obj.processingMode === "file" || obj.processingMode === "sqlite") {
      return obj.processingMode;
    }

    return obj.stagingMode === "sqlite" ? "sqlite" : "file";
  }

  private toMappingTargetType(fieldType?: string): MappingTargetType {
    const normalized = String(fieldType || "").trim().toLowerCase();
    if (["int", "integer"].includes(normalized)) {
      return "integer";
    }
    if (["double", "currency", "percent", "number"].includes(normalized)) {
      return "number";
    }
    if (["boolean", "checkbox"].includes(normalized)) {
      return "boolean";
    }
    if (["date", "datetime"].includes(normalized)) {
      return "datetime";
    }
    return "string";
  }

  private toMappingTransformType(value?: string): MappingTransformType {
    const normalized = String(value || "NONE").trim().toUpperCase();
    const allowed: MappingTransformType[] = [
      "NONE",
      "TRIM",
      "UPPERCASE",
      "LOWERCASE",
      "TO_INTEGER",
      "TO_BOOLEAN",
      "DATETIME_ISO",
      "STATIC",
      "LOOKUP"
    ];
    return allowed.includes(normalized as MappingTransformType)
      ? (normalized as MappingTransformType)
      : "NONE";
  }

  private buildMigrationMappingLines(obj: MigrationObjectConfig): MappingDefinitionLine[] {
    return (obj.fieldMappings || [])
      .filter((mapping) => String(mapping.sourceColumn || "").trim() && String(mapping.targetField || "").trim())
      .map((mapping, index) => {
        const lookupEnabled = mapping.lookupEnabled === true;
        const lookupObject = String(mapping.lookupObject || "").trim();
        const lookupField = String(mapping.lookupField || "").trim();
        const transformType = lookupEnabled && lookupObject && lookupField
          ? "LOOKUP"
          : this.toMappingTransformType(mapping.transformFunction);

        const transformExpression = String(mapping.transformExpression || "").trim();

        return {
          lineNumber: index + 1,
          rawLine: JSON.stringify(mapping),
          targetField: String(mapping.targetField).trim(),
          targetType: mapping.targetType || this.toMappingTargetType(mapping.targetFieldType),
          sourceField: String(mapping.sourceColumn).trim(),
          transform: {
            type: transformType,
            raw: transformType,
            argument: transformType === "STATIC" ? transformExpression : undefined,
            lookupObject: transformType === "LOOKUP" ? lookupObject : undefined,
            lookupField: transformType === "LOOKUP" ? lookupField : undefined
          },
          picklistMappings: Array.isArray(mapping.picklistMappings) ? mapping.picklistMappings : []
        } satisfies MappingDefinitionLine;
      });
  }

  private toSoqlLiteral(value: unknown): string {
    const raw = String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    return `'${raw}'`;
  }

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
    const localHealth = readLocalScheduleHealthStore();

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
        inheritTimingFromParent: schedule.inheritTimingFromParent,
        autoDisabledDueToErrors: localHealth[schedule.id]?.autoDisabled === true,
        autoDisabledAt: localHealth[schedule.id]?.autoDisabledAt
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
      secretKey: connector.secretKey,
      timeoutMs: connector.timeoutMs,
      maxRetries: connector.maxRetries,
      description: connector.description,
      parameters: connector.parameters,
      hasSecret: Boolean(connector.secretKey),
      parameterKeys: Object.keys(connector.parameters).sort()
    }));
  }

  public async testConnector(connectorId: string, instanceId?: string): Promise<ConnectorTestResult> {
    const client = await this.createClient(instanceId);
    const config = await client.queryConnector(connectorId);

    if (this.isFileConnectorType(config.connectorType)) {
      return {
        ok: true,
        connectorId: config.id,
        connectorName: config.name,
        connectorType: config.connectorType,
        message: "Datei-Connector bereit (Pfad-/Datei-Pruefung erfolgt zur Laufzeit pro Scheduler)."
      };
    }

    if (this.isRestConnectorType(config.connectorType)) {
      return {
        ok: true,
        connectorId: config.id,
        connectorName: config.name,
        connectorType: config.connectorType,
        message: "REST-Connector konfiguriert (Endpunkt-Pruefung erfolgt zur Laufzeit pro Scheduler)."
      };
    }

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

    if (normalizedType === "FILE_CSV" || normalizedType === "FILE_EXCEL" || normalizedType === "FILE_JSON") {
      if (!connectorId) {
        throw new Error("Fuer Datei-Vorschau muss ein Datei-Connector ausgewaehlt sein");
      }

      const client = await this.createClient(instanceId);
      const connector = await client.queryConnector(connectorId);
      if (!this.isFileConnectorType(connector.connectorType)) {
        throw new Error(`Connector ${connector.name} ist kein Datei-Connector`);
      }

      const payload = await parseFileFromConnector(connector, trimmedDefinition, { archiveOnRead: false });
      return {
        fields: payload.headers,
        rows: payload.rows.slice(0, normalizedLimit),
        rowCount: payload.rows.length
      };
    }

    if (normalizedType === "REST_API") {
      if (!connectorId) {
        throw new Error("Fuer REST-Vorschau muss ein REST-Connector ausgewaehlt sein");
      }

      const client = await this.createClient(instanceId);
      const connector = await client.queryConnector(connectorId);
      const rows = await fetchRestRows(connector, trimmedDefinition, normalizedLimit);
      const fields = rows.length > 0 ? Object.keys(rows[0] || {}) : [];
      return {
        fields,
        rows,
        rowCount: rows.length
      };
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

    if (normalizedType === "FILE_CSV" || normalizedType === "FILE_EXCEL" || normalizedType === "FILE_JSON") {
      if (!connectorId) {
        throw new Error("Fuer Datei-Feldmetadaten muss ein Datei-Connector ausgewaehlt sein");
      }

      const client = await this.createClient(instanceId);
      const connector = await client.queryConnector(connectorId);
      if (!this.isFileConnectorType(connector.connectorType)) {
        throw new Error(`Connector ${connector.name} ist kein Datei-Connector`);
      }

      const payload = await parseFileFromConnector(connector, sourceDefinition, { archiveOnRead: false });
      return payload.headers.map((header) => ({
        name: header,
        label: header,
        type: "string"
      }));
    }

    if (normalizedType === "REST_API") {
      if (!connectorId) {
        throw new Error("Fuer REST-Feldmetadaten muss ein REST-Connector ausgewaehlt sein");
      }

      const client = await this.createClient(instanceId);
      const connector = await client.queryConnector(connectorId);
      const rows = await fetchRestRows(connector, sourceDefinition, 1);
      const fields = rows.length > 0 ? Object.keys(rows[0] || {}) : [];

      return fields.map((fieldName) => ({
        name: fieldName,
        label: fieldName,
        type: "string"
      }));
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
    const sourceType = String(input.sourceType || "").toUpperCase();
    const targetType = String(input.targetType || "").toUpperCase();
    const usesFileSource = sourceType === "FILE_CSV" || sourceType === "FILE_EXCEL" || sourceType === "FILE_JSON";
    const usesFileTarget = targetType === "FILE_CSV" || targetType === "FILE_EXCEL" || targetType === "FILE_JSON";

    if (usesFileSource && !String(input.sourceDefinition || "").trim()) {
      throw new Error("FILE SourceType erfordert eine SourceDefinition mit Dateiangaben");
    }

    if (usesFileTarget && !String(input.targetDefinition || "").trim()) {
      throw new Error("FILE TargetType erfordert eine TargetDefinition mit Dateiangaben");
    }

    if ((usesFileSource || usesFileTarget) && !String(input.connectorId || "").trim()) {
      throw new Error("Datei-Scheduler benoetigt einen Datei-Connector");
    }

    if ((usesFileSource || usesFileTarget) && input.connectorId) {
      const connector = await client.queryConnector(input.connectorId);
      if (!this.isFileConnectorType(connector.connectorType)) {
        throw new Error(`Ausgewaehlter Connector ${connector.name} ist kein Datei-Connector`);
      }
    }

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
      if (input.active) {
        this.clearScheduleAutoDisabledFlag(input.id);
      }
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
      this.removeScheduleHealthState(id);
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

  public async exportSetup(instanceId?: string): Promise<SetupExportDocument> {
    const resolved = this.resolveInstance(instanceId);
    const client = await this.createClient(resolved.id);
    const [connectorConfigs, schedules] = await Promise.all([
      client.queryConnectors(),
      this.listSchedules(resolved.id)
    ]);

    const connectorById = new Map(connectorConfigs.map((item) => [item.id, item]));
    const scheduleById = new Map(schedules.map((item) => [item.id, item]));

    const connectors: ConnectorMutationInput[] = connectorConfigs.map((connector) => ({
      name: connector.name,
      active: connector.active,
      connectorType: connector.connectorType,
      targetSystem: connector.targetSystem,
      direction: connector.direction,
      secretKey: connector.secretKey,
      timeoutMs: connector.timeoutMs,
      maxRetries: connector.maxRetries,
      description: connector.description,
      parameters: connector.parameters
    }));

    const scheduleItems: SetupExportScheduleItem[] = schedules.map((schedule) => ({
      name: schedule.name,
      active: schedule.active,
      sourceSystem: schedule.sourceSystem,
      targetSystem: schedule.targetSystem,
      objectName: schedule.objectName,
      operation: schedule.operation,
      connectorName: schedule.connectorId ? connectorById.get(schedule.connectorId)?.name : undefined,
      mappingDefinition: schedule.mappingDefinition,
      direction: schedule.direction,
      sourceType: schedule.sourceType,
      targetType: schedule.targetType,
      sourceDefinition: schedule.sourceDefinition,
      targetDefinition: schedule.targetDefinition,
      batchSize: schedule.batchSize,
      nextRunAt: schedule.nextRunAt,
      lastRunAt: schedule.lastRunAt,
      timingDefinition: schedule.timingDefinition,
      parentScheduleId: schedule.parentScheduleId,
      parentScheduleName: schedule.parentScheduleId
        ? scheduleById.get(schedule.parentScheduleId)?.name
        : undefined,
      inheritTimingFromParent: schedule.inheritTimingFromParent
    }));

    return {
      version: 1,
      exportedAt: new Date().toISOString(),
      instanceId: resolved.id,
      connectors,
      schedules: scheduleItems
    };
  }

  public async importSetup(document: SetupExportDocument, instanceId?: string): Promise<SetupImportResult> {
    if (!document || typeof document !== "object") {
      throw new Error("Import-Dokument ist ungueltig");
    }

    if (!Array.isArray(document.connectors) || !Array.isArray(document.schedules)) {
      throw new Error("Import-Dokument muss connectors und schedules als Arrays enthalten");
    }

    const resolved = this.resolveInstance(instanceId);
    const client = await this.createClient(resolved.id);
    const existingConnectors = await client.queryConnectors();
    const connectorByName = new Map(existingConnectors.map((item) => [item.name, item]));
    const connectorIdByName = new Map(existingConnectors.map((item) => [item.name, item.id]));

    let connectorsCreated = 0;
    let connectorsUpdated = 0;

    for (const entry of document.connectors) {
      const existing = connectorByName.get(entry.name);
      const result = await this.saveConnector(
        {
          ...entry,
          id: existing?.id
        },
        resolved.id
      );

      if (result.action === "created") {
        connectorsCreated += 1;
      } else {
        connectorsUpdated += 1;
      }

      connectorIdByName.set(entry.name, result.id);
    }

    const existingSchedules = await this.listSchedules(resolved.id);
    const scheduleByName = new Map(existingSchedules.map((item) => [item.name, item]));
    const scheduleIdByName = new Map(existingSchedules.map((item) => [item.name, item.id]));

    let schedulesCreated = 0;
    let schedulesUpdated = 0;

    let pending = [...document.schedules];
    let guard = 0;

    while (pending.length > 0) {
      guard += 1;
      if (guard > document.schedules.length + 5) {
        throw new Error("Scheduler-Import konnte nicht aufgeloest werden (moeglicher Parent-Zyklus)");
      }

      const remaining: SetupExportScheduleItem[] = [];
      let progressed = false;

      for (const entry of pending) {
        const existing = scheduleByName.get(entry.name);
        const connectorId = entry.connectorName ? connectorIdByName.get(entry.connectorName) : undefined;

        if (entry.connectorName && !connectorId) {
          throw new Error(`Connector fuer Scheduler nicht gefunden: ${entry.connectorName}`);
        }

        const desiredParentName = String(entry.parentScheduleName || "").trim();
        const resolvedParentId = desiredParentName
          ? scheduleIdByName.get(desiredParentName)
          : undefined;

        if (desiredParentName && !resolvedParentId) {
          remaining.push(entry);
          continue;
        }

        const result = await this.saveSchedule(
          {
            ...entry,
            id: existing?.id,
            connectorId,
            parentScheduleId: resolvedParentId,
            inheritTimingFromParent: resolvedParentId ? entry.inheritTimingFromParent : false
          },
          resolved.id
        );

        scheduleIdByName.set(entry.name, result.id);
        progressed = true;

        if (result.action === "created") {
          schedulesCreated += 1;
        } else {
          schedulesUpdated += 1;
        }
      }

      if (!progressed && remaining.length > 0) {
        const unresolved = remaining
          .map((item) => `${item.name} -> ${String(item.parentScheduleName || "unbekannt")}`)
          .join(", ");
        throw new Error(`Parent-Scheduler konnten nicht aufgeloest werden: ${unresolved}`);
      }

      pending = remaining;
    }

    return {
      connectorsCreated,
      connectorsUpdated,
      schedulesCreated,
      schedulesUpdated
    };
  }

  public async analyzeUploadedSourceFile(
    connectorId: string,
    fileName: string,
    contentBase64: string,
    instanceId?: string
  ): Promise<UploadedFileAnalysisResult> {
    if (!connectorId) {
      throw new Error("connectorId ist erforderlich");
    }
    if (!fileName) {
      throw new Error("fileName ist erforderlich");
    }
    if (!contentBase64) {
      throw new Error("contentBase64 ist erforderlich");
    }

    const client = await this.createClient(instanceId);
    const connector = await client.queryConnector(connectorId);
    if (!this.isFileConnectorType(connector.connectorType)) {
      throw new Error(`Connector ${connector.name} unterstuetzt keinen Datei-Import`);
    }

    const fileBuffer = Buffer.from(contentBase64, "base64");
    const analysis = analyzeUploadedFile(fileName, fileBuffer);
    const sourceType: "FILE_CSV" | "FILE_EXCEL" | "FILE_JSON" =
      analysis.format === "excel" ? "FILE_EXCEL" : analysis.format === "json" ? "FILE_JSON" : "FILE_CSV";

    // Save the uploaded file to the connector's importPath so that source preview works afterwards
    const params = connector.parameters || {};
    const basePath = path.resolve(
      process.cwd(),
      String(params.basePath || params.fileBasePath || "artifacts/files")
    );
    const importPath = path.resolve(basePath, String(params.importPath || "inbound"));
    await fs.promises.mkdir(importPath, { recursive: true });
    await fs.promises.writeFile(path.resolve(importPath, fileName), fileBuffer);

    const sourceDefinition = {
      fileName,
      format: analysis.format,
      charset: analysis.charset,
      delimiter: analysis.delimiter,
      hasHeader: true
    };

    const mappingDefinition = analysis.headers.map((header) => ({
      sourceField: header,
      sourceType: "string",
      targetField: "",
      transformFunction: "NONE"
    }));

    return {
      connectorId,
      fileName,
      format: analysis.format,
      charset: analysis.charset,
      delimiter: analysis.delimiter,
      headers: analysis.headers,
      sourceType,
      sourceDefinition: JSON.stringify(sourceDefinition, null, 2),
      mappingDefinition: JSON.stringify(mappingDefinition, null, 2)
    };
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
      sourceType: schedule.sourceType,
      targetType: schedule.targetType,
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

  private isFileConnectorType(connectorType: string | undefined): boolean {
    const normalized = String(connectorType || "").toLowerCase();
    return (
      normalized.includes("file") ||
      normalized.includes("csv") ||
      normalized.includes("excel") ||
      normalized.includes("xlsx") ||
      normalized.includes("json")
    );
  }

  private isRestConnectorType(connectorType: string | undefined): boolean {
    const normalized = String(connectorType || "").trim().toLowerCase();
    return normalized.includes("rest") || normalized.includes("http") || normalized.includes("api");
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

  private clearScheduleAutoDisabledFlag(scheduleId: string): void {
    const store = readLocalScheduleHealthStore();
    const entry = store[scheduleId];
    if (!entry || entry.autoDisabled !== true) {
      return;
    }

    store[scheduleId] = {
      ...entry,
      autoDisabled: false,
      autoDisabledAt: undefined
    };
    writeLocalScheduleHealthStore(store);
  }

  private removeScheduleHealthState(scheduleId: string): void {
    const store = readLocalScheduleHealthStore();
    if (!(scheduleId in store)) {
      return;
    }

    delete store[scheduleId];
    writeLocalScheduleHealthStore(store);
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

    if (normalizedTargetSystem === "SALESFORCE" && targetObject) {
      const client = await this.createClient(instanceId);
      const fields = await client.describeObjectFields(targetObject);
      return {
        fields: (fields || []).map((field: any) => ({
          name: field.name || '',
          type: field.type || 'string',
          label: field.label
        }))
      };
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

  public async createCustomObjectMetadata(
    metadata: Record<string, unknown>,
    instanceId?: string
  ): Promise<unknown> {
    const client = await this.createClient(instanceId);
    try {
      const fullName = String(metadata.fullName ?? "").trim();
      if (!fullName) {
        throw new Error("Custom object metadata requires a fullName");
      }

      return await client.createOrUpdateMetadata("CustomObject", fullName, metadata);
    } catch (error) {
      throw error;
    }
  }

  public async deployEzbMetadata(instanceId?: string): Promise<unknown> {
    const client = await this.createClient(instanceId);
    const zipBase64 = await this.createEzbDeployZipBase64();
    const result = await client.deployMetadataZip(zipBase64);

    if (!result.success) {
      const details = result.details ? `: ${JSON.stringify(result.details)}` : "";
      throw new Error(`EZB metadata deployment failed with status ${result.status || "unknown"}${details}`);
    }

    const psAssignment = await client.ensurePermissionSetAssigned("MSD_Integration_Agent");

    return { ...result, permissionSetAssignment: psAssignment };
  }

  public async getSalesforceOverview(instanceId?: string): Promise<SalesforceOrgOverview> {
    const client = await this.createClient(instanceId);
    return await client.getOrgOverview();
  }

  public async listSalesforceObjects(instanceId?: string): Promise<{ name: string; label: string }[]> {
    const client = await this.createClient(instanceId);
    return await client.listObjectMetadata();
  }

  public async describeSalesforceObjectFields(objectApiName: string, instanceId?: string): Promise<{ name: string; label: string; type: string; nillable: boolean }[]> {
    const client = await this.createClient(instanceId);
    return await client.describeObjectFields(objectApiName);
  }

  public async createSalesforceCustomField(
    objectApiName: string,
    fieldApiName: string,
    fieldType: string,
    instanceId?: string
  ): Promise<unknown> {
    const client = await this.createClient(instanceId);
    const ensuredApiName = fieldApiName.endsWith("__c") ? fieldApiName : fieldApiName + "__c";
    const sfType = this.mapFieldTypeToSalesforceType(fieldType);
    const metadata: Record<string, unknown> = {
      label: fieldApiName.replace(/__c$/, "").replace(/_/g, " "),
      type: sfType.type,
      ...sfType.extra
    };
    return await client.createOrUpdateMetadata("CustomField", objectApiName + "." + ensuredApiName, metadata);
  }

  public analyzeFileBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string }
  ): {
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
  } {
    const parsed = this.parseMigrationSourceBuffer(fileName, fileBuffer, options);
    return {
      format: parsed.format,
      charset: parsed.charset,
      delimiter: parsed.delimiter,
      textQualifier: parsed.textQualifier,
      fields: parsed.fields,
      rows: parsed.previewRows,
      recordCount: parsed.recordCount
    };
  }

  public async stageMigrationSourceFile(
    migrationId: string,
    objectId: string,
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string }
  ): Promise<{
    filePath: string;
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
    stagingMode: "sqlite";
    stagingDatabasePath: string;
    stagingImportedAt: string;
    stagingStatus: "ready";
  }> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} not found`);
    }

    const obj = migration.objects.find((item) => item.id === objectId);
    if (!obj) {
      throw new Error(`Object ${objectId} not found in migration`);
    }

    const safeFileName = path.basename(String(fileName || "").trim());
    if (!safeFileName) {
      throw new Error("fileName ist erforderlich");
    }

    const targetDir = path.resolve(process.cwd(), "artifacts/files/inbound/migrations", migrationId);
    await fs.promises.mkdir(targetDir, { recursive: true });
    const absolutePath = path.resolve(targetDir, safeFileName);
    await fs.promises.writeFile(absolutePath, fileBuffer);

    const parsed = this.parseMigrationSourceBuffer(safeFileName, fileBuffer, options);
    const relativePath = path.relative(process.cwd(), absolutePath).split(path.sep).join("/");
    const stagingDatabasePath = path.relative(process.cwd(), this.migrationStaging.getFilePath()).split(path.sep).join("/");
    const importedAt = new Date().toISOString();

    await this.migrationStaging.replaceObjectRows(
      {
        migrationId,
        objectId,
        filePath: relativePath,
        sourceFileName: safeFileName,
        fileFormat: parsed.format,
        fileCharset: parsed.charset,
        fileDelimiter: parsed.delimiter,
        fileTextQualifier: parsed.textQualifier,
        recordCount: parsed.recordCount,
        columns: parsed.fields,
        uploadedAt: importedAt
      },
      parsed.allRows
    );

    obj.filePath = relativePath;
    obj.fileFormat = parsed.format;
    obj.fileCharset = parsed.charset;
    obj.fileDelimiter = parsed.delimiter;
    obj.fileTextQualifier = parsed.textQualifier;
    obj.fileRecordCount = parsed.recordCount;
    obj.fileColumns = parsed.fields;
    obj.previewRows = parsed.previewRows.slice(0, 3);
    obj.processingMode = obj.processingMode || "sqlite";
    obj.stagingMode = "sqlite";
    obj.stagingDatabasePath = stagingDatabasePath;
    obj.stagingImportedAt = importedAt;
    obj.stagingStatus = "ready";
    this.saveMigration(migration);

    return {
      filePath: relativePath,
      format: parsed.format,
      charset: parsed.charset,
      delimiter: parsed.delimiter,
      textQualifier: parsed.textQualifier,
      fields: parsed.fields,
      rows: parsed.previewRows,
      recordCount: parsed.recordCount,
      stagingMode: "sqlite",
      stagingDatabasePath,
      stagingImportedAt: importedAt,
      stagingStatus: "ready"
    };
  }

  public async analyzeMigrationObjectSource(
    migrationId: string,
    objectId: string,
    options?: { offset?: number; limit?: number }
  ): Promise<{
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
    processingMode?: "file" | "sqlite";
    stagingMode?: "sqlite" | "file";
    stagingDatabasePath?: string;
    stagingImportedAt?: string;
    stagingStatus?: string;
    previewOffset?: number;
    previewLimit?: number;
    statusSummary?: Record<string, number>;
  }> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} not found`);
    }

    const obj = migration.objects.find((item) => item.id === objectId);
    if (!obj) {
      throw new Error(`Object ${objectId} not found in migration`);
    }

    if (obj.stagingMode === "sqlite") {
      const effectiveProcessingMode = this.getEffectiveMigrationProcessingMode(obj);
      const meta = await this.migrationStaging.getObjectMeta(migrationId, objectId);
      if (meta && effectiveProcessingMode === "sqlite") {
        const desiredCharset = String(obj.fileCharset || meta.fileCharset || "utf8");
        const desiredDelimiter = String(obj.fileDelimiter || meta.fileDelimiter || ";");
        const desiredTextQualifier = String(obj.fileTextQualifier ?? meta.fileTextQualifier ?? '"');
        const stagingNeedsRefresh = desiredCharset !== meta.fileCharset
          || desiredDelimiter !== meta.fileDelimiter
          || desiredTextQualifier !== meta.fileTextQualifier;

        if (stagingNeedsRefresh && obj.filePath) {
          const absolutePath = path.isAbsolute(obj.filePath)
            ? obj.filePath
            : path.resolve(process.cwd(), obj.filePath);
          const fileBuffer = await fs.promises.readFile(absolutePath);
          const fileName = path.basename(absolutePath);
          await this.stageMigrationSourceFile(migrationId, objectId, fileName, fileBuffer, {
            charset: desiredCharset,
            delimiter: desiredDelimiter,
            textQualifier: desiredTextQualifier
          });
          return this.analyzeMigrationObjectSource(migrationId, objectId, options);
        }

        const previewLimit = typeof options?.limit === "number" && options.limit > 0 ? Math.floor(options.limit) : 10;
        const previewOffset = typeof options?.offset === "number" && options.offset > 0 ? Math.floor(options.offset) : 0;
        const [stagedRows, statusSummary] = await Promise.all([
          this.migrationStaging.listObjectRows(migrationId, objectId, { limit: previewLimit, offset: previewOffset }),
          this.migrationStaging.getObjectStatusSummary(migrationId, objectId)
        ]);
        return {
          format: meta.fileFormat,
          charset: meta.fileCharset,
          delimiter: meta.fileDelimiter,
          textQualifier: meta.fileTextQualifier,
          fields: meta.columns,
          rows: stagedRows.map((row) => row.payload),
          recordCount: meta.recordCount,
          processingMode: effectiveProcessingMode,
          stagingMode: "sqlite",
          stagingDatabasePath: obj.stagingDatabasePath,
          stagingImportedAt: meta.uploadedAt,
          stagingStatus: obj.stagingStatus || "ready",
          previewOffset,
          previewLimit,
          statusSummary: statusSummary.byStatus
        };
      }
    }

    if (!obj.filePath) {
      throw new Error(`Kein Dateipfad konfiguriert für Objekt ${obj.salesforceObject}`);
    }

    const absolutePath = path.isAbsolute(obj.filePath)
      ? obj.filePath
      : path.resolve(process.cwd(), obj.filePath);
    const fileBuffer = await fs.promises.readFile(absolutePath);
    const fileName = path.basename(absolutePath);
    const analysis = this.analyzeFileBuffer(fileName, fileBuffer, {
      charset: obj.fileCharset,
      delimiter: obj.fileDelimiter,
      textQualifier: obj.fileTextQualifier
    });
    return {
      ...analysis,
      processingMode: this.getEffectiveMigrationProcessingMode(obj),
      stagingMode: obj.stagingMode || "file",
      stagingDatabasePath: obj.stagingDatabasePath,
      stagingImportedAt: obj.stagingImportedAt,
      stagingStatus: obj.stagingStatus,
      previewOffset: 0,
      previewLimit: analysis.rows.length,
      statusSummary: undefined
    };
  }

  private parseMigrationSourceBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string }
  ): {
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    previewRows: Record<string, unknown>[];
    allRows: Record<string, unknown>[];
    recordCount: number;
  } {
    const analysis = analyzeUploadedFile(fileName, fileBuffer);
    const fields = analysis.headers || [];
    const format = analysis.format;
    const charset = String(options?.charset || analysis.charset || "utf8").trim() || "utf8";
    const delimiter = String(options?.delimiter || analysis.delimiter || ';');
    const textQualifier = String(options?.textQualifier || '"') || '"';
    let allRows: Record<string, unknown>[] = [];
    let recordCount = 0;
    try {
      if (analysis.format === 'excel') {
        const XLSX = require('xlsx') as any;
        const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
        const firstSheet = workbook.SheetNames[0];
        const worksheetRows = XLSX.utils.sheet_to_json(workbook.Sheets[firstSheet]) as Record<string, unknown>[];
        recordCount = worksheetRows.length;
        allRows = worksheetRows;
      } else if (analysis.format === 'json') {
        const parsed = JSON.parse(fileBuffer.toString(charset as BufferEncoding));
        const normalizedRows = Array.isArray(parsed) ? parsed : [];
        recordCount = normalizedRows.length;
        allRows = normalizedRows;
      } else {
        const splitCsvLine = (line: string): string[] => {
          const values: string[] = [];
          let current = '';
          let inQuotes = false;

          for (let index = 0; index < line.length; index += 1) {
            const char = line[index];
            const nextChar = line[index + 1];

            if (char === textQualifier) {
              if (inQuotes && nextChar === textQualifier) {
                current += textQualifier;
                index += 1;
              } else {
                inQuotes = !inQuotes;
              }
              continue;
            }

            if (!inQuotes && char === delimiter) {
              values.push(current);
              current = '';
              continue;
            }

            current += char;
          }

          values.push(current);
          return values.map((value) => value.trim());
        };

        const lines = fileBuffer
          .toString(charset as BufferEncoding)
          .replace(/^\uFEFF/, '')
          .split(/\r?\n/)
          .map((line) => line.trimEnd())
          .filter((line) => line.length > 0);

        const headerLine = lines[0] || '';
        const headers = splitCsvLine(headerLine).map((h) => h.trim());
        for (let i = 1; i < lines.length; i++) {
          const values = splitCsvLine(lines[i]);
          const record: Record<string, unknown> = {};
          headers.forEach((h, idx) => {
            record[h] = values[idx] ?? '';
          });
          allRows.push(record);
        }
        recordCount = allRows.length;
      }
    } catch {
      allRows = [];
      recordCount = 0;
    }

    return {
      format,
      charset,
      delimiter,
      textQualifier,
      fields,
      previewRows: allRows.slice(0, 10),
      allRows,
      recordCount
    };
  }

  private mapFieldTypeToSalesforceType(fieldType: string): { type: string; extra?: Record<string, unknown> } {
    const map: Record<string, { type: string; extra?: Record<string, unknown> }> = {
      Text: { type: "Text", extra: { length: 255 } },
      Number: { type: "Number", extra: { precision: 18, scale: 0 } },
      Date: { type: "Date" },
      DateTime: { type: "DateTime" },
      Checkbox: { type: "Checkbox", extra: { defaultValue: false } },
      Currency: { type: "Currency", extra: { precision: 18, scale: 2 } },
      Percent: { type: "Percent", extra: { precision: 18, scale: 2 } },
      Email: { type: "Email" },
      Phone: { type: "Phone" },
      Url: { type: "Url" }
    };
    return map[fieldType] || { type: "Text", extra: { length: 255 } };
  }

  private async loadMigrationSourceRows(
    migrationId: string,
    obj: MigrationObjectConfig
  ): Promise<Array<{ rowIndex: number; row: Record<string, unknown> }>> {
    if (this.getEffectiveMigrationProcessingMode(obj) === "sqlite") {
      const stagedRows = await this.migrationStaging.listObjectRows(migrationId, obj.id);
      if (stagedRows.length > 0) {
        return stagedRows.map((entry) => ({ rowIndex: entry.rowIndex, row: entry.payload }));
      }
    }

    if (!obj.filePath) {
      throw new Error(`Kein Dateipfad konfiguriert für Objekt ${obj.salesforceObject}`);
    }

    const absolutePath = path.isAbsolute(obj.filePath)
      ? obj.filePath
      : path.resolve(process.cwd(), obj.filePath);
    const fileBuffer = await fs.promises.readFile(absolutePath);
    const fileName = path.basename(absolutePath);
    const parsed = this.parseMigrationSourceBuffer(fileName, fileBuffer, {
      charset: obj.fileCharset,
      delimiter: obj.fileDelimiter,
      textQualifier: obj.fileTextQualifier
    });

    return parsed.allRows.map((row, index) => ({ rowIndex: index + 1, row }));
  }

  public async createCustomObjectFromSource(
    input: CreateCustomObjectFromSourceInput,
    instanceId?: string
  ): Promise<{
    objectApiName: string;
    label: string;
    fieldsCreated: number;
    result: unknown;
    tabResult: unknown;
  }> {
    const sourceFields = Array.isArray(input.sourceFields) ? input.sourceFields : [];
    const fieldOverrides = Array.isArray(input.fieldOverrides) ? input.fieldOverrides : [];
    const objectApiName = this.normalizeCustomObjectApiName(input.objectApiName);
    const label = String(input.label || this.customObjectLabelFromApiName(objectApiName)).trim()
      || this.customObjectLabelFromApiName(objectApiName);

    if (!sourceFields.length) {
      throw new Error("sourceFields darf nicht leer sein");
    }

    const fieldMetadata = this.buildCustomFieldMetadataFromSource(sourceFields, fieldOverrides);
    if (!fieldMetadata.length) {
      throw new Error("Es konnten keine Felder aus den Quelldaten erzeugt werden");
    }

    const client = await this.createClient(instanceId);
    const result = await client.createOrUpdateMetadata("CustomObject", objectApiName, {
      fullName: objectApiName,
      label,
      nameField: {
        displayFormat: `${objectApiName.replace(/__c$/, "")}-{000000}`,
        label: "Record ID",
        type: "AutoNumber"
      },
      sharingModel: "ReadWrite",
      fields: fieldMetadata
    });

    const tabResult = await this.createCustomTabMetadata(objectApiName, instanceId);

    return {
      objectApiName,
      label,
      fieldsCreated: fieldMetadata.length,
      result,
      tabResult
    };
  }

  private async createCustomTabMetadata(objectApiName: string, instanceId?: string): Promise<unknown> {
    const client = await this.createClient(instanceId);
    return await client.createOrUpdateMetadata("CustomTab", objectApiName, {
      fullName: objectApiName,
      customObject: true,
      motif: "Custom40: Currency"
    });
  }

  private normalizeCustomObjectApiName(rawValue: string): string {
    const normalized = String(rawValue || "")
      .trim()
      .replace(/[^A-Za-z0-9_]/g, "_")
      .replace(/^_+|_+$/g, "");

    if (!normalized) {
      throw new Error("objectApiName ist erforderlich");
    }

    const baseName = /__c$/i.test(normalized)
      ? normalized.replace(/__c$/i, "")
      : normalized;

    const safeBaseName = /^[A-Za-z]/.test(baseName)
      ? baseName
      : `X_${baseName}`;

    return `${safeBaseName}__c`;
  }

  private customObjectLabelFromApiName(objectApiName: string): string {
    const base = objectApiName.replace(/__c$/i, "");
    const words = base
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/_/g, " ")
      .split(/\s+/)
      .filter(Boolean)
      .map((word) => word.slice(0, 1).toUpperCase() + word.slice(1).toLowerCase());
    return words.join(" ") || "Custom Object";
  }

  private normalizeCustomFieldApiName(rawValue: string): string {
    const normalized = String(rawValue || "")
      .trim()
      .replace(/[^A-Za-z0-9_]/g, "_")
      .replace(/^_+|_+$/g, "");

    if (!normalized) {
      return "Field";
    }

    const baseName = /__c$/i.test(normalized)
      ? normalized.replace(/__c$/i, "")
      : normalized;

    const safeBaseName = /^[A-Za-z]/.test(baseName)
      ? baseName
      : `F_${baseName}`;

    return `${safeBaseName}__c`;
  }

  private fieldLabelFromSource(field: SourceFieldMetadata): string {
    const raw = String(field.label || field.name || "").trim();
    const words = raw
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/_/g, " ")
      .split(/\s+/)
      .filter(Boolean)
      .map((word) => word.slice(0, 1).toUpperCase() + word.slice(1));
    return words.join(" ") || "Field";
  }

  private mapSourceTypeToSalesforceField(typeName: string): Record<string, unknown> {
    const normalized = String(typeName || "").trim().toLowerCase();

    if (normalized === "boolean" || normalized === "bool") {
      return { type: "Checkbox", defaultValue: false };
    }

    if (normalized === "date") {
      return { type: "Date" };
    }

    if (normalized === "datetime" || normalized === "timestamp") {
      return { type: "DateTime" };
    }

    if (normalized.includes("int") || normalized === "number" || normalized === "double" || normalized === "float" || normalized === "decimal") {
      return { type: "Number", precision: 18, scale: 6 };
    }

    return { type: "Text", length: 255 };
  }

  private buildCustomFieldMetadataFromSource(
    sourceFields: SourceFieldMetadata[],
    fieldOverrides: Array<{ sourceName: string; type?: string }> = []
  ): Record<string, unknown>[] {
    const usedApiNames = new Set<string>();
    const result: Record<string, unknown>[] = [];
    const overrideBySourceName = new Map<string, string>();

    for (const override of fieldOverrides) {
      const sourceName = String(override?.sourceName || "").trim().toLowerCase();
      const fieldType = String(override?.type || "").trim();
      if (sourceName && fieldType) {
        overrideBySourceName.set(sourceName, fieldType);
      }
    }

    for (const sourceField of sourceFields) {
      const sourceName = String(sourceField?.name || "").trim();
      if (!sourceName) {
        continue;
      }

      let apiName = this.normalizeCustomFieldApiName(sourceName);
      if (apiName.toLowerCase() === "name" || apiName.toLowerCase() === "name__c") {
        apiName = "SourceName__c";
      }

      let uniqueApiName = apiName;
      let suffix = 2;
      while (usedApiNames.has(uniqueApiName.toLowerCase())) {
        uniqueApiName = apiName.replace(/__c$/i, `_${suffix}__c`);
        suffix += 1;
      }
      usedApiNames.add(uniqueApiName.toLowerCase());

      result.push({
        fullName: uniqueApiName,
        label: this.fieldLabelFromSource(sourceField),
        required: false,
        ...this.mapSourceTypeToSalesforceField(
          overrideBySourceName.get(sourceName.toLowerCase()) || sourceField.type
        )
      });
    }

    return result;
  }

  private async createEzbDeployZipBase64(): Promise<string> {
    const files = [
      {
        source: path.join(SALESFORCE_METADATA_DIR, "objects/EZB__c.object"),
        target: "objects/EZB__c.object"
      },
      {
        source: path.join(SALESFORCE_METADATA_DIR, "tabs/EZB__c.tab"),
        target: "tabs/EZB__c.tab"
      },
      {
        source: path.join(SALESFORCE_METADATA_DIR, "permissionsets/MSD_Integration_Agent.permissionset"),
        target: "permissionsets/MSD_Integration_Agent.permissionset"
      },
      {
        source: path.join(SALESFORCE_METADATA_DIR, "applications/MSD_Integration.app"),
        target: "applications/MSD_Integration.app"
      }
    ];

    for (const file of files) {
      if (!fs.existsSync(file.source)) {
        throw new Error(`Required metadata file is missing: ${file.source}`);
      }
    }

    const packageXml = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<Package xmlns="http://soap.sforce.com/2006/04/metadata">',
      '  <types>',
      '    <members>EZB__c</members>',
      '    <name>CustomObject</name>',
      '  </types>',
      '  <types>',
      '    <members>MSD_Integration</members>',
      '    <name>CustomApplication</name>',
      '  </types>',
      '  <types>',
      '    <members>MSD_Integration_Agent</members>',
      '    <name>PermissionSet</name>',
      '  </types>',
      '  <types>',
      '    <members>EZB__c</members>',
      '    <name>CustomTab</name>',
      '  </types>',
      '  <version>61.0</version>',
      '</Package>'
    ].join('\n');

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on("data", (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });

    const archive = archiver("zip", { zlib: { level: 9 } });

    const zipPromise = new Promise<string>((resolve, reject) => {
      stream.on("end", () => {
        resolve(Buffer.concat(chunks).toString("base64"));
      });
      stream.on("error", reject);
      archive.on("error", reject);
    });

    archive.pipe(stream);
    archive.append(packageXml, { name: "package.xml" });
    for (const file of files) {
      archive.file(file.source, { name: file.target });
    }
    void archive.finalize();

    return await zipPromise;
  }

  // ─── Migration Config Storage ─────────────────────────────────────────────

  private readMigrationsStore(): MigrationConfig[] {
    if (!fs.existsSync(LOCAL_MIGRATIONS_FILE)) {
      return [];
    }
    try {
      const raw = fs.readFileSync(LOCAL_MIGRATIONS_FILE, "utf8").trim();
      const parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? (parsed as MigrationConfig[]) : [];
    } catch {
      return [];
    }
  }

  private writeMigrationsStore(migrations: MigrationConfig[]): void {
    const dir = path.dirname(LOCAL_MIGRATIONS_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(LOCAL_MIGRATIONS_FILE, JSON.stringify(migrations, null, 2), "utf8");
  }

  public listMigrations(): MigrationConfig[] {
    return this.readMigrationsStore();
  }

  public getMigration(id: string): MigrationConfig | undefined {
    return this.readMigrationsStore().find((m) => m.id === id);
  }

  public saveMigration(input: Omit<MigrationConfig, "createdAt" | "updatedAt"> & { createdAt?: string; updatedAt?: string }): MigrationConfig {
    const migrations = this.readMigrationsStore();
    const now = new Date().toISOString();
    const existing = migrations.find((m) => m.id === input.id);
    const saved: MigrationConfig = {
      ...input,
      createdAt: existing?.createdAt ?? input.createdAt ?? now,
      updatedAt: now
    };
    if (existing) {
      const idx = migrations.indexOf(existing);
      migrations[idx] = saved;
    } else {
      migrations.push(saved);
    }
    this.writeMigrationsStore(migrations);
    return saved;
  }

  public deleteMigration(id: string): boolean {
    const migrations = this.readMigrationsStore();
    const filtered = migrations.filter((m) => m.id !== id);
    if (filtered.length === migrations.length) {
      return false;
    }
    this.writeMigrationsStore(filtered);
    return true;
  }

  private classifyMigrationError(errorMessage: string): string {
    const message = String(errorMessage || "").toLowerCase();
    if (!message) {
      return "Sonstige";
    }

    if (
      message.includes("duplicate") ||
      message.includes("duplik") ||
      message.includes("duplicate value found") ||
      message.includes("duplicate external")
    ) {
      return "Dubletten";
    }

    if (
      message.includes("invalid field") ||
      message.includes("no such column") ||
      message.includes("unknown field")
    ) {
      return "Invalid Field";
    }

    if (
      message.includes("picklist") ||
      message.includes("invalid_or_null_for_restricted_picklist")
    ) {
      return "Picklist Fehler";
    }

    if (
      message.includes("required") ||
      message.includes("required field") ||
      message.includes("required fields are missing")
    ) {
      return "Pflichtfeld fehlt";
    }

    if (
      message.includes("invalid cross reference") ||
      message.includes("reference") ||
      message.includes("lookup")
    ) {
      return "Lookup/Referenz Fehler";
    }

    if (
      message.includes("string too long") ||
      message.includes("max length") ||
      message.includes("value too long")
    ) {
      return "Feldlaenge";
    }

    return "Sonstige";
  }

  private formatDuration(durationMs: number): string {
    const safeMs = Math.max(0, Math.floor(durationMs));
    const totalSeconds = Math.floor(safeMs / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  private async writeMigrationRunReport(
    migration: MigrationConfig,
    runResult: MigrationRunResult,
    finishedAt: string,
    failedRecordsByObjectId: Record<string, MigrationFailedRecord[]>
  ): Promise<string> {
    const startedAtMs = new Date(runResult.startedAt).getTime();
    const finishedAtMs = new Date(finishedAt).getTime();
    const durationMs = Math.max(0, finishedAtMs - startedAtMs);

    const totalSource = runResult.steps.reduce((sum, step) => sum + (step.recordsProcessed || 0), 0);
    const totalSuccess = runResult.steps.reduce((sum, step) => sum + (step.recordsSucceeded || 0), 0);
    const totalFailed = runResult.steps.reduce((sum, step) => sum + (step.recordsFailed || 0), 0);

    const errorGroupCounter = new Map<string, number>();
    for (const failedRecords of Object.values(failedRecordsByObjectId)) {
      for (const record of failedRecords) {
        const group = this.classifyMigrationError(record.error);
        errorGroupCounter.set(group, (errorGroupCounter.get(group) || 0) + 1);
      }
    }

    const reportLines: string[] = [];
    reportLines.push(`# Migrationsprotokoll: ${migration.name}`);
    reportLines.push("");
    reportLines.push(`- Migration-ID: ${migration.id}`);
    reportLines.push(`- Start: ${runResult.startedAt}`);
    reportLines.push(`- Ende: ${finishedAt}`);
    reportLines.push(`- Dauer: ${this.formatDuration(durationMs)} (${durationMs} ms)`);
    reportLines.push("");
    reportLines.push("## Gesamtübersicht");
    reportLines.push("");
    reportLines.push(`- Anzahl Quelldatensätze: ${totalSource}`);
    reportLines.push(`- Erfolgreich importiert: ${totalSuccess}`);
    reportLines.push(`- Fehlerhaft: ${totalFailed}`);
    reportLines.push("");

    reportLines.push("## Ergebnis pro Objekt");
    reportLines.push("");
    reportLines.push("| Objekt | Verarbeitet | Erfolgreich | Fehlerhaft | Status |");
    reportLines.push("| --- | ---: | ---: | ---: | --- |");
    for (const step of runResult.steps) {
      reportLines.push(
        `| ${step.salesforceObject} | ${step.recordsProcessed || 0} | ${step.recordsSucceeded || 0} | ${step.recordsFailed || 0} | ${step.status} |`
      );
    }
    reportLines.push("");

    reportLines.push("## Fehlergruppen");
    reportLines.push("");
    if (!errorGroupCounter.size) {
      reportLines.push("Keine Fehlergruppen vorhanden.");
    } else {
      reportLines.push("| Fehlerbild | Anzahl |");
      reportLines.push("| --- | ---: |");
      for (const [group, count] of [...errorGroupCounter.entries()].sort((a, b) => b[1] - a[1])) {
        reportLines.push(`| ${group} | ${count} |`);
      }
    }
    reportLines.push("");

    reportLines.push("## Mapping-Tabellen");
    reportLines.push("");
    for (const obj of migration.objects) {
      reportLines.push(`### ${obj.salesforceObject}`);
      reportLines.push("");
      if (!obj.fieldMappings.length) {
        reportLines.push("Keine Mappings definiert.");
        reportLines.push("");
        continue;
      }

      reportLines.push("| Quelle | Ziel | Typ | Transform | Lookup | Picklist-Mapping |");
      reportLines.push("| --- | --- | --- | --- | --- | --- |");
      for (const mapping of obj.fieldMappings) {
        const lookup = mapping.lookupEnabled
          ? `${mapping.lookupObject || ""}.${mapping.lookupField || ""}`
          : "-";
        const transform = mapping.transformFunction === "STATIC"
          ? `STATIC(${mapping.transformExpression || ""})`
          : (mapping.transformFunction || "NONE");
        const picklist = (mapping.picklistMappings || [])
          .map((entry) => `${entry.source}=${entry.target}`)
          .join("; ");
        reportLines.push(
          `| ${mapping.sourceColumn || ""} | ${mapping.targetField || ""} | ${mapping.targetFieldType || ""} | ${transform} | ${lookup} | ${picklist || "-"} |`
        );
      }
      reportLines.push("");
    }

    const reportDir = path.join(process.cwd(), "artifacts", "migrations", migration.id, "reports");
    await fs.promises.mkdir(reportDir, { recursive: true });
    const reportFileName = `${new Date(finishedAt).toISOString().replace(/[:.]/g, "-")}-migration-report.md`;
    const reportFilePath = path.join(reportDir, reportFileName);
    await fs.promises.writeFile(reportFilePath, reportLines.join("\n"), "utf-8");

    return path.relative(process.cwd(), reportFilePath).split(path.sep).join("/");
  }

  public async runMigration(id: string, instanceId?: string): Promise<MigrationRunResult> {
    const migration = this.getMigration(id);
    if (!migration) {
      throw new Error(`Migration ${id} not found`);
    }

    const client = await this.createClient(instanceId ?? migration.instanceId);

    const startedAt = new Date().toISOString();
    const stepResults: MigrationRunResult["steps"] = [];
    const failedRecordsByObjectId: Record<string, MigrationFailedRecord[]> = {};

    // Execute in order defined by executionPlan
    const orderedObjects = [...migration.executionPlan]
      .sort((a, b) => a.order - b.order)
      .map((step) => migration.objects.find((o) => o.id === step.objectId))
      .filter((o): o is MigrationObjectConfig => !!o);

    // Objects not in plan appended at end
    for (const obj of migration.objects) {
      if (!orderedObjects.find((o) => o.id === obj.id)) {
        orderedObjects.push(obj);
      }
    }

    // Mark running
    migration.status = "running";
    this.saveMigration(migration);

    try {
      for (const obj of orderedObjects) {
        obj.stagingStatus = obj.stagingMode === "sqlite" ? "processing" : obj.stagingStatus;
        const stepResult: MigrationRunResult["steps"][number] = {
          objectId: obj.id,
          salesforceObject: obj.salesforceObject,
          status: "done",
          recordsProcessed: 0,
          recordsSucceeded: 0,
          recordsFailed: 0,
          failedRecordsId: undefined
        };

        try {
          const sourceRows = await this.loadMigrationSourceRows(migration.id, obj);
          const rows = sourceRows.map((entry) => entry.row);

          stepResult.recordsProcessed = rows.length;

          const mappingLines = this.buildMigrationMappingLines(obj);
          const lookupResolver = async (lookupObject: string, lookupField: string, value: unknown): Promise<string | null> => {
            if (value === undefined || value === null || value === "") {
              return null;
            }
            const soql = `SELECT Id FROM ${lookupObject} WHERE ${lookupField} = ${this.toSoqlLiteral(value)} LIMIT 1`;
            const result = await client.queryGeneric(soql);
            if (!result.length) {
              return null;
            }
            const idValue = result[0].Id;
            return typeof idValue === "string" ? idValue : null;
          };
          const engine = new MappingDefinitionEngine(lookupResolver);

          // Track each record: {index in original rows, mapped SF record or error, source record}
          const recordStates: Array<{
            rowIndex: number;
            sourceRecord: Record<string, unknown>;
            sfRecord?: Record<string, unknown>;
            mappingError?: string;
          }> = [];
          const mappingErrorsPreview: string[] = [];

          for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex++) {
            const sourceRow = sourceRows[rowIndex];
            const row = sourceRow.row;
            try {
              if (mappingLines.length > 0) {
                const mapped = await engine.mapRecord(row, mappingLines);
                const record: Record<string, unknown> = {};
                for (const [key, value] of Object.entries(mapped.values)) {
                  record[key] = value !== undefined && value !== "" ? value : null;
                }
                recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, sfRecord: record });
              } else {
                const record: Record<string, unknown> = {};
                for (const mapping of obj.fieldMappings) {
                  const rawValue = row[mapping.sourceColumn];
                  record[mapping.targetField] = rawValue !== undefined && rawValue !== "" ? rawValue : null;
                }
                recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, sfRecord: record });
              }
            } catch (error) {
              const errorMsg = error instanceof Error ? error.message : String(error);
              recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, mappingError: errorMsg });
              if (mappingErrorsPreview.length < 3) {
                mappingErrorsPreview.push(errorMsg);
              }
            }
          }

          const sfRecords = recordStates.filter((s) => s.sfRecord).map((s) => s.sfRecord!);

          const batchSize = 200;
          let succeeded = 0;
          let failed = 0;
          const sfRecordStateIndexes = recordStates
            .map((state, idx) => (state.sfRecord ? idx : -1))
            .filter((idx) => idx >= 0);

          for (let i = 0; i < sfRecords.length; i += batchSize) {
            const batch = sfRecords.slice(i, i + batchSize);
            let batchResults: Array<any> = [];
            if (obj.operation === "upsert" && obj.externalIdField) {
              const results = await (client as any).connection.sobject(obj.salesforceObject).upsert(batch, obj.externalIdField);
              batchResults = Array.isArray(results) ? results : [results];
            } else if (obj.operation === "update") {
              const results = await (client as any).connection.sobject(obj.salesforceObject).update(batch);
              batchResults = Array.isArray(results) ? results : [results];
            } else {
              const results = await (client as any).connection.sobject(obj.salesforceObject).insert(batch);
              batchResults = Array.isArray(results) ? results : [results];
            }

            // Map batch results back to the exact recordStates slice for this batch.
            const batchStateIndexes = sfRecordStateIndexes.slice(i, i + batch.length);
            for (let batchIdx = 0; batchIdx < batchResults.length; batchIdx++) {
              const stateIdx = batchStateIndexes[batchIdx];
              if (stateIdx === undefined) continue;
              const res = batchResults[batchIdx];
              if (res.success) {
                succeeded++;
              } else {
                failed++;
                if (!recordStates[stateIdx].mappingError) {
                  const sfError = Array.isArray(res.errors) && res.errors.length
                    ? res.errors.map((e: { message?: string }) => e.message || String(e)).join("; ")
                    : (res.error?.message || String(res.error || "Unknown Salesforce error"));
                  recordStates[stateIdx].mappingError = sfError;
                }
              }
            }
          }

          stepResult.recordsSucceeded = succeeded;
          const mappingFailed = recordStates.filter((s) => s.mappingError).length;
          stepResult.recordsFailed = mappingFailed;

          // Save failed records to artifact
          const failedRecords: MigrationFailedRecord[] = recordStates
            .filter((s) => s.mappingError)
            .map((s) => ({
              rowIndex: s.rowIndex,
              sourceRecord: s.sourceRecord,
              mappedRecord: s.sfRecord,
              error: s.mappingError!,
              errorType: s.mappingError && s.sfRecord ? 'salesforce' : 'mapping'
            }));

          if (obj.stagingMode === "sqlite") {
            await this.migrationStaging.updateRowStatuses(
              migration.id,
              obj.id,
              recordStates.map((state) => ({
                rowIndex: state.rowIndex,
                status: state.mappingError ? (state.sfRecord ? "salesforce_error" : "mapping_error") : "success",
                errorMessage: state.mappingError
              }))
            );
          }

          if (failedRecords.length > 0) {
            const failedRecordsId = `${obj.id}-${Date.now()}`;
            const failedDir = path.join(process.cwd(), 'artifacts', 'migrations', migration.id, 'failed-records');
            await fs.promises.mkdir(failedDir, { recursive: true });
            await fs.promises.writeFile(
              path.join(failedDir, `${failedRecordsId}.json`),
              JSON.stringify(failedRecords, null, 2),
              'utf-8'
            );
            stepResult.failedRecordsId = failedRecordsId;
          }
          failedRecordsByObjectId[obj.id] = failedRecords;

          if (mappingFailed > 0) {
            const prefix = `${mappingFailed} Datensätze fehlgeschlagen.`;
            const detail = mappingErrorsPreview.length ? ` Beispiele: ${mappingErrorsPreview.join(" | ")}` : "";
            stepResult.errorMessage = (stepResult.errorMessage ? `${stepResult.errorMessage} ` : "") + prefix + detail;
          }

          if (failed > 0 && succeeded === 0) {
            stepResult.status = "error";
            stepResult.errorMessage = `Alle ${failed} Datensätze fehlgeschlagen`;
          }
          if (mappingFailed > 0 && succeeded === 0 && failed === 0) {
            stepResult.status = "error";
          }
          obj.stagingStatus = obj.stagingMode === "sqlite"
            ? (stepResult.status === "error" ? "error" : "done")
            : obj.stagingStatus;
        } catch (err: unknown) {
          stepResult.status = "error";
          stepResult.errorMessage = err instanceof Error ? err.message : String(err);
          obj.stagingStatus = obj.stagingMode === "sqlite" ? "error" : obj.stagingStatus;
        }

        stepResults.push(stepResult);
      }

      const hasErrors = stepResults.some((s) => s.status === "error");
      const finishedAt = new Date().toISOString();
      const reportPath = await this.writeMigrationRunReport(migration, {
        migrationId: id,
        startedAt,
        steps: stepResults
      }, finishedAt, failedRecordsByObjectId);
      migration.status = hasErrors ? "error" : "done";
      migration.lastRunAt = startedAt;
      migration.lastRunResult = {
        startedAt,
        finishedAt,
        steps: stepResults.map((s) => ({ ...s }))
      };
      this.saveMigration(migration);

      return { migrationId: id, startedAt, reportPath, steps: stepResults };
    } catch (err: unknown) {
      migration.status = "error";
      this.saveMigration(migration);
      throw err;
    }
  }

  public async retryFailedMigrationRecords(
    migrationId: string,
    objectId: string,
    failedRecordsId: string,
    editedRecords: Array<{ rowIndex: number; sourceRecord: Record<string, unknown> }> = [],
    instanceId?: string
  ): Promise<{
    objectId: string;
    salesforceObject: string;
    recordsProcessed: number;
    recordsSucceeded: number;
    recordsFailed: number;
    failedRecordsId?: string;
    errorMessage?: string;
  }> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} not found`);
    }

    const obj = migration.objects.find((item) => item.id === objectId);
    if (!obj) {
      throw new Error(`Object ${objectId} not found in migration`);
    }

    const failedFile = path.join(
      process.cwd(),
      "artifacts",
      "migrations",
      migrationId,
      "failed-records",
      `${failedRecordsId}.json`
    );

    let previousFailed: MigrationFailedRecord[] = [];
    try {
      const raw = await fs.promises.readFile(failedFile, "utf-8");
      previousFailed = JSON.parse(raw) as MigrationFailedRecord[];
    } catch {
      throw new Error(`Failed records ${failedRecordsId} not found`);
    }

    const editedByRow = new Map<number, Record<string, unknown>>();
    for (const record of editedRecords) {
      if (!record || typeof record.rowIndex !== "number" || typeof record.sourceRecord !== "object") {
        continue;
      }
      editedByRow.set(record.rowIndex, record.sourceRecord);
    }

    const client = await this.createClient(instanceId ?? migration.instanceId);
    const mappingLines = this.buildMigrationMappingLines(obj);
    const lookupResolver = async (lookupObject: string, lookupField: string, value: unknown): Promise<string | null> => {
      if (value === undefined || value === null || value === "") {
        return null;
      }
      const soql = `SELECT Id FROM ${lookupObject} WHERE ${lookupField} = ${this.toSoqlLiteral(value)} LIMIT 1`;
      const result = await client.queryGeneric(soql);
      if (!result.length) {
        return null;
      }
      const idValue = result[0].Id;
      return typeof idValue === "string" ? idValue : null;
    };
    const engine = new MappingDefinitionEngine(lookupResolver);

    const recordStates: Array<{
      rowIndex: number;
      sourceRecord: Record<string, unknown>;
      sfRecord?: Record<string, unknown>;
      mappingError?: string;
    }> = [];

    for (const failed of previousFailed) {
      const rowIndex = Number(failed.rowIndex || 0);
      const sourceRecord = editedByRow.get(rowIndex) || failed.sourceRecord || {};
      try {
        if (mappingLines.length > 0) {
          const mapped = await engine.mapRecord(sourceRecord, mappingLines);
          const record: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(mapped.values)) {
            record[key] = value !== undefined && value !== "" ? value : null;
          }
          recordStates.push({ rowIndex: Math.max(1, rowIndex), sourceRecord, sfRecord: record });
        } else {
          const record: Record<string, unknown> = {};
          for (const mapping of obj.fieldMappings) {
            const rawValue = sourceRecord[mapping.sourceColumn];
            record[mapping.targetField] = rawValue !== undefined && rawValue !== "" ? rawValue : null;
          }
          recordStates.push({ rowIndex: Math.max(1, rowIndex), sourceRecord, sfRecord: record });
        }
      } catch (error) {
        recordStates.push({
          rowIndex: Math.max(1, rowIndex),
          sourceRecord,
          mappingError: error instanceof Error ? error.message : String(error)
        });
      }
    }

    const sfRecords = recordStates.filter((s) => s.sfRecord).map((s) => s.sfRecord!);
    const sfRecordStateIndexes = recordStates
      .map((state, idx) => (state.sfRecord ? idx : -1))
      .filter((idx) => idx >= 0);

    const batchSize = 200;
    let succeeded = 0;
    let failed = 0;

    for (let i = 0; i < sfRecords.length; i += batchSize) {
      const batch = sfRecords.slice(i, i + batchSize);
      let batchResults: Array<{ success?: boolean; errors?: Array<{ message?: string }>; error?: { message?: string } }> = [];

      if (obj.operation === "upsert" && obj.externalIdField) {
        const results = await (client as any).connection.sobject(obj.salesforceObject).upsert(batch, obj.externalIdField);
        batchResults = Array.isArray(results) ? results : [results];
      } else if (obj.operation === "update") {
        const results = await (client as any).connection.sobject(obj.salesforceObject).update(batch);
        batchResults = Array.isArray(results) ? results : [results];
      } else {
        const results = await (client as any).connection.sobject(obj.salesforceObject).insert(batch);
        batchResults = Array.isArray(results) ? results : [results];
      }

      const batchStateIndexes = sfRecordStateIndexes.slice(i, i + batch.length);
      for (let batchIdx = 0; batchIdx < batchResults.length; batchIdx++) {
        const stateIdx = batchStateIndexes[batchIdx];
        if (stateIdx === undefined) continue;
        const res = batchResults[batchIdx];
        if (res.success) {
          succeeded++;
        } else {
          failed++;
          const sfError = Array.isArray(res.errors) && res.errors.length
            ? res.errors.map((e) => e.message || String(e)).join("; ")
            : (res.error?.message || String(res.error || "Unknown Salesforce error"));
          recordStates[stateIdx].mappingError = sfError;
        }
      }
    }

    const stillFailed: MigrationFailedRecord[] = recordStates
      .filter((state) => !!state.mappingError)
      .map((state) => ({
        rowIndex: state.rowIndex,
        sourceRecord: state.sourceRecord,
        mappedRecord: state.sfRecord,
        error: state.mappingError || "Unknown error",
        errorType: state.sfRecord ? "salesforce" : "mapping"
      }));

    let newFailedRecordsId: string | undefined;
    if (stillFailed.length > 0) {
      newFailedRecordsId = `${objectId}-${Date.now()}`;
      const failedDir = path.join(process.cwd(), "artifacts", "migrations", migrationId, "failed-records");
      await fs.promises.mkdir(failedDir, { recursive: true });
      await fs.promises.writeFile(
        path.join(failedDir, `${newFailedRecordsId}.json`),
        JSON.stringify(stillFailed, null, 2),
        "utf-8"
      );
    }

    if (obj.stagingMode === "sqlite") {
      await this.migrationStaging.updateRowStatuses(
        migrationId,
        objectId,
        recordStates.map((state) => ({
          rowIndex: state.rowIndex,
          status: state.mappingError ? (state.sfRecord ? "salesforce_error" : "mapping_error") : "success",
          errorMessage: state.mappingError
        }))
      );
    }

    return {
      objectId: obj.id,
      salesforceObject: obj.salesforceObject,
      recordsProcessed: recordStates.length,
      recordsSucceeded: succeeded,
      recordsFailed: stillFailed.length,
      failedRecordsId: newFailedRecordsId,
      errorMessage: stillFailed.length > 0 ? `${stillFailed.length} Datensätze konnten weiterhin nicht importiert werden.` : undefined
    };
  }
}
