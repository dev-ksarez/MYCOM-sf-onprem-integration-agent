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
import { LookupResolverFn, MappingDefinitionEngine } from "../core/mapping-dsl/mapping-definition-engine";
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
import { isImportProfileSchedulerRuleDue, type SchedulerDay } from "../core/scheduler/import-profile-scheduler";
import { getDefaultStaleRunInactivityThresholdMinutes, getStaleRunInactivityThresholdMinutesForSchedule } from "../core/scheduler/stale-run-policy";
import { MigrationStagingSqlite } from "../infrastructure/db/migration-staging-sqlite";
import { MssqlDatabase } from "../infrastructure/db/mssql";
import {
  ConnectorTemplateDraft,
  listBuiltInTemplates,
  ScheduleTemplateDraft,
  TemplateDefinition,
  TemplateKind,
  TemplateBundleDraft,
  TemplateMutationInput
} from "./template-library";
import { IntegrationSchedule } from "../types/integration-schedule";
import { FileScheduleType, isFileScheduleType, normalizeScheduleType } from "../types/file-schedule-type";
import { runScheduleNow } from "../agent/agent-runner";
import { analyzeUploadedFile, decodeTextBuffer, parseDelimitedRows, parseFileFromConnector } from "../utils/file-transfer";
import { parseQuerySourceDefinition } from "../utils/query-source-definition";
import { fetchRestRows, testRestConnection } from "../source-adapters/rest/rest-api-source-adapter";

export interface SalesforceInstanceEnvConfig {
  id: string;
  name?: string;
  loginUrl: string;
  projectId?: string;
  role?: "test" | "production";
  clientId?: string;
  clientSecret?: string;
  clientIdEnv?: string;
  clientSecretEnv?: string;
  queryLimit?: number;
}

export interface SalesforceProjectConfig {
  id: string;
  name: string;
  description?: string;
  archived?: boolean;
  productionWriteProtection: boolean;
  createdAt: string;
  updatedAt: string;
}

interface MigrationSalesforceInstanceConfig {
  id: string;
  name: string;
  environment: "sandbox" | "production";
  loginUrl: string;
  authType?: "oauth_refresh_token" | "password" | "client_credentials";
  username?: string;
  password?: string;
  securityToken?: string;
  clientId?: string;
  clientSecret?: string;
  instanceUrl?: string;
  accessToken?: string;
  refreshToken?: string;
  tokenIssuedAt?: string;
  queryLimit?: number;
  lastConnectionStatus?: "never" | "connected" | "error";
  lastConnectedAt?: string;
  lastConnectionError?: string;
  orgOverview?: SalesforceOrgOverview;
  objectCount?: number;
  updatedAt?: string;
}

export interface SalesforceInstanceMutationInput {
  id: string;
  name?: string;
  loginUrl: string;
  projectId?: string;
  role?: "test" | "production";
  clientId: string;
  clientSecret: string;
  queryLimit?: number;
}

export interface SalesforceProjectMutationInput {
  id?: string;
  name: string;
  description?: string;
  archived?: boolean;
  productionWriteProtection?: boolean;
}

export interface MigrationSalesforceInstanceMutationInput {
  id?: string;
  name: string;
  environment: "sandbox" | "production";
  loginUrl?: string;
  authType?: "oauth_refresh_token" | "password" | "client_credentials";
  username?: string;
  password?: string;
  securityToken?: string;
  clientId?: string;
  clientSecret?: string;
  queryLimit?: number;
}

interface MigrationOAuthTokenResponse {
  access_token?: string;
  refresh_token?: string;
  instance_url?: string;
  issued_at?: string;
  token_type?: string;
  scope?: string;
  error?: string;
  error_description?: string;
}

function formatSalesforceOauthError(error?: string, description?: string, loginUrl?: string): string {
  const normalizedError = String(error || "").trim();
  const normalizedDescription = String(description || "").trim();
  const normalizedLoginUrl = String(loginUrl || "").trim();

  if (normalizedError === "invalid_client_id") {
    return [
      "Salesforce Client ID ist ungueltig.",
      normalizedLoginUrl ? `Pruefe, ob die Connected App in ${normalizedLoginUrl} angelegt ist.` : "",
      "Pruefe ausserdem, ob Consumer Key und Umgebung (Production oder Sandbox) zusammenpassen."
    ]
      .filter(Boolean)
      .join(" ");
  }

  return normalizedDescription || normalizedError;
}

function normalizeMigrationSalesforceLoginUrl(loginUrl?: string, environment?: "sandbox" | "production"): string {
  const fallbackUrl = environment === "sandbox" ? "https://test.salesforce.com" : "https://login.salesforce.com";
  const rawValue = String(loginUrl || "").trim();
  if (!rawValue) {
    return fallbackUrl;
  }

  try {
    const parsedUrl = new URL(rawValue);
    parsedUrl.pathname = parsedUrl.pathname.replace(/\/(services\/(Soap|oauth2).*)$/i, "") || "/";
    parsedUrl.search = "";
    parsedUrl.hash = "";
    return parsedUrl.toString().replace(/\/$/, "");
  } catch {
    return rawValue.replace(/\/(services\/(Soap|oauth2).*)$/i, "").replace(/\/$/, "") || fallbackUrl;
  }
}

export interface MigrationSalesforceInstanceSummary {
  id: string;
  name: string;
  environment: "sandbox" | "production";
  loginUrl: string;
  authType?: "oauth_refresh_token" | "password" | "client_credentials";
  queryLimit?: number;
  connectionStatus: "never" | "connected" | "error";
  lastConnectedAt?: string;
  lastConnectionError?: string;
  orgOverview?: SalesforceOrgOverview;
  objectCount?: number;
  lastMigration?: {
    id: string;
    name: string;
    status: "draft" | "ready" | "running" | "done" | "error";
    lastRunAt?: string;
    objectNames: string[];
    recordsProcessed: number;
    recordsSucceeded: number;
    recordsFailed: number;
    errorMessage?: string;
  };
}

interface ResolvedInstance {
  id: string;
  name: string;
  config: SalesforceConfig;
}

const LOCAL_INSTANCES_FILE = process.env.SF_INSTANCES_FILE || path.resolve(process.cwd(), "artifacts/sf-instances.json");
const LOCAL_PROJECTS_FILE = process.env.SF_PROJECTS_FILE || path.resolve(process.cwd(), "artifacts/projects.json");
const LOCAL_MIGRATION_INSTANCES_FILE = process.env.SF_MIGRATION_INSTANCES_FILE || path.resolve(process.cwd(), "artifacts/migration-instances.json");
const LOCAL_SCHEDULE_TIMING_FILE = process.env.SF_SCHEDULE_TIMING_FILE || path.resolve(process.cwd(), "artifacts/schedule-timing.json");
const LOCAL_SCHEDULE_HEALTH_FILE = process.env.SF_SCHEDULE_HEALTH_FILE || path.resolve(process.cwd(), "artifacts/schedule-health.json");
const LOCAL_MIGRATIONS_FILE = path.resolve(process.cwd(), "artifacts/migrations.json");
const LOCAL_FAILED_RUN_RECORDS_DIR = path.resolve(process.cwd(), "artifacts/runtime/failed-run-records");
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

    let changed = false;
    const items = (parsed as SalesforceInstanceEnvConfig[]).map((item) => {
      const projectId = String(item.projectId || "").trim() || "default-project";
      const role: "test" | "production" = item.role === "production" ? "production" : "test";
      if (item.projectId !== projectId || item.role !== role) {
        changed = true;
      }

      return {
        ...item,
        projectId,
        role
      };
    });

    if (changed) {
      writeLocalInstances(items);
    }

    return items;
  } catch {
    return [];
  }
}

function writeLocalInstances(instances: SalesforceInstanceEnvConfig[]): void {
  const directory = path.dirname(LOCAL_INSTANCES_FILE);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(LOCAL_INSTANCES_FILE, JSON.stringify(instances, null, 2), "utf8");
}

function buildProjectIdFromName(name: string): string {
  const slug = String(name || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  return slug || "project";
}

function defaultProjectConfig(): SalesforceProjectConfig {
  const now = new Date().toISOString();
  return {
    id: "default-project",
    name: "Default-Projekt",
    archived: false,
    productionWriteProtection: true,
    createdAt: now,
    updatedAt: now
  };
}

function readLocalProjects(): SalesforceProjectConfig[] {
  try {
    if (!fs.existsSync(LOCAL_PROJECTS_FILE)) {
      return [defaultProjectConfig()];
    }

    const raw = fs.readFileSync(LOCAL_PROJECTS_FILE, "utf8").trim();
    if (!raw) {
      return [defaultProjectConfig()];
    }

    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [defaultProjectConfig()];
    }

    const items = parsed
      .filter((entry) => entry && typeof entry === "object" && !Array.isArray(entry))
      .map((entry): SalesforceProjectConfig | null => {
        const candidate = entry as Record<string, unknown>;
        const id = String(candidate.id || "").trim();
        const name = String(candidate.name || "").trim();
        if (!id || !name) {
          return null;
        }

        return {
          id,
          name,
          description: typeof candidate.description === "string" ? candidate.description.trim() || undefined : undefined,
          archived: candidate.archived === true,
          productionWriteProtection: candidate.productionWriteProtection !== false,
          createdAt: typeof candidate.createdAt === "string" && candidate.createdAt.trim()
            ? candidate.createdAt
            : new Date().toISOString(),
          updatedAt: typeof candidate.updatedAt === "string" && candidate.updatedAt.trim()
            ? candidate.updatedAt
            : new Date().toISOString()
        };
      })
      .filter((entry): entry is SalesforceProjectConfig => entry !== null);

    if (!items.some((entry) => entry.id === "default-project")) {
      items.unshift(defaultProjectConfig());
    }

    return items;
  } catch {
    return [defaultProjectConfig()];
  }
}

function writeLocalProjects(projects: SalesforceProjectConfig[]): void {
  const directory = path.dirname(LOCAL_PROJECTS_FILE);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(LOCAL_PROJECTS_FILE, JSON.stringify(projects, null, 2), "utf8");
}

function readEnvInstances(): SalesforceInstanceEnvConfig[] {
  const raw = process.env.SF_INSTANCES_JSON?.trim();
  if (!raw) {
    return [];
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    return [];
  }

  if (!Array.isArray(parsed)) {
    return [];
  }

  return (parsed as SalesforceInstanceEnvConfig[])
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      ...item,
      projectId: String(item.projectId || "").trim() || "default-project",
      role: item.role === "production" ? "production" : "test"
    }));
}

function readConfiguredInstancesWithMetadata(): SalesforceInstanceEnvConfig[] {
  const deduped = new Map<string, SalesforceInstanceEnvConfig>();

  for (const item of readEnvInstances()) {
    if (item.id) {
      deduped.set(item.id, item);
    }
  }

  for (const item of readLocalInstances()) {
    if (item.id) {
      deduped.set(item.id, item);
    }
  }

  return [...deduped.values()];
}

export function readConfiguredSalesforceInstances(): SalesforceInstanceEnvConfig[] {
  return readLocalInstances();
}

export function writeConfiguredSalesforceInstances(instances: SalesforceInstanceEnvConfig[]): void {
  writeLocalInstances(instances);
}

function readLocalMigrationInstances(): MigrationSalesforceInstanceConfig[] {
  try {
    if (!fs.existsSync(LOCAL_MIGRATION_INSTANCES_FILE)) {
      return [];
    }

    const raw = fs.readFileSync(LOCAL_MIGRATION_INSTANCES_FILE, "utf8").trim();
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed) ? (parsed as MigrationSalesforceInstanceConfig[]) : [];
  } catch {
    return [];
  }
}

function writeLocalMigrationInstances(instances: MigrationSalesforceInstanceConfig[]): void {
  const directory = path.dirname(LOCAL_MIGRATION_INSTANCES_FILE);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(LOCAL_MIGRATION_INSTANCES_FILE, JSON.stringify(instances, null, 2), "utf8");
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
      authType: "client_credentials",
      clientId: resolvedClientId,
      clientSecret: resolvedClientSecret,
      queryLimit: item.queryLimit || fallbackQueryLimit
    }
  };
}

function toResolvedMigrationInstance(
  item: MigrationSalesforceInstanceConfig,
  fallbackQueryLimit: number,
  oauthClient: { clientId: string; clientSecret: string }
): ResolvedInstance | null {
  if (!item || typeof item !== "object") {
    return null;
  }

  if (!item.id || !item.loginUrl) {
    return null;
  }

  if (item.authType === "password") {
    if (!item.username || !item.password) {
      return null;
    }

    return {
      id: item.id,
      name: item.name?.trim() || item.id,
      config: {
        loginUrl: item.loginUrl,
        authType: "password",
        username: item.username,
        password: item.password,
        securityToken: item.securityToken,
        queryLimit: item.queryLimit || fallbackQueryLimit
      }
    };
  }

  if (item.authType === "client_credentials") {
    const clientId = String(item.clientId || "").trim();
    const clientSecret = String(item.clientSecret || "").trim();
    if (!clientId || !clientSecret) {
      return null;
    }

    return {
      id: item.id,
      name: item.name?.trim() || item.id,
      config: {
        loginUrl: item.loginUrl,
        authType: "client_credentials",
        clientId,
        clientSecret,
        queryLimit: item.queryLimit || fallbackQueryLimit
      }
    };
  }

  if (!item.refreshToken) {
    return null;
  }

  return {
    id: item.id,
    name: item.name?.trim() || item.id,
    config: {
      loginUrl: item.loginUrl,
      authType: "oauth_refresh_token",
      clientId: oauthClient.clientId,
      clientSecret: oauthClient.clientSecret,
      refreshToken: item.refreshToken,
      accessToken: item.accessToken,
      instanceUrl: item.instanceUrl,
      queryLimit: item.queryLimit || fallbackQueryLimit
    }
  };
}

export interface SalesforceInstanceOption {
  id: string;
  name: string;
  isDefault: boolean;
  projectId: string;
  projectName: string;
  role: "test" | "production";
}

export interface SalesforceProjectOption {
  id: string;
  name: string;
  description?: string;
  archived?: boolean;
  productionWriteProtection: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface ScheduleListItem {
  id: string;
  name: string;
  createdAt?: string;
  createdByName?: string;
  createdByUsername?: string;
  lastModifiedAt?: string;
  lastModifiedByName?: string;
  lastModifiedByUsername?: string;
  active: boolean;
  status: "due" | "scheduled" | "inactive" | "running";
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
  currentDeltaCheckpoint?: string;
  currentDeltaRecordId?: string;
  currentDeltaRunId?: string;
}

export interface ScheduleCheckpointMutationInput {
  lastCheckpoint?: string;
  lastRecordId?: string;
}

export interface ConnectorListItem {
  id: string;
  name: string;
  createdAt?: string;
  createdByName?: string;
  createdByUsername?: string;
  lastModifiedAt?: string;
  lastModifiedByName?: string;
  lastModifiedByUsername?: string;
  active: boolean;
  connectorType: string;
  targetSystem?: string;
  direction?: string;
  secretKey?: string;
  timeoutMs?: number;
  maxRetries?: number;
  description?: string;
  parameters?: Record<string, unknown>;
  filePaths?: FileConnectorPathSummary;
  hasSecret: boolean;
  parameterKeys: string[];
}

export interface FileConnectorPathSummary {
  basePath: string;
  importPath: string;
  exportPath: string;
  archivePath: string;
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

export interface MigrationImportSuggestion {
  objectApiName: string;
  label: string;
  score: number;
  confidence: "high" | "medium" | "low";
  matchedHeaders: string[];
  reason: string;
}

export interface MigrationImportSheetAnalysis {
  sheetName: string;
  headers: string[];
  recordCount: number;
  suggestions: MigrationImportSuggestion[];
}

export interface MigrationImportAnalysisResult {
  fileName: string;
  format: "csv" | "excel" | "json";
  charset: string;
  delimiter: string;
  headers: string[];
  recordCount: number;
  suggestions: MigrationImportSuggestion[];
  sheetName?: string;
  sheets?: MigrationImportSheetAnalysis[];
}

export interface ConnectorTestResult {
  ok: boolean;
  connectorId: string;
  connectorName: string;
  connectorType: string;
  message: string;
  testedAt: string;
  checks: Array<{
    label: string;
    ok: boolean;
    details: string;
  }>;
}

export interface RunListItem {
  id: string;
  scheduleId?: string;
  scheduleName?: string;
  connectorId?: string;
  connectorName?: string;
  status: string;
  startedAt?: string;
  finishedAt?: string;
  recordsRead?: number;
  recordsProcessed?: number;
  recordsSucceeded?: number;
  recordsFailed?: number;
  errorMessage?: string;
}

export interface StaleRunListItem extends RunListItem {
  ageMinutes: number;
  staleThresholdMinutes: number;
  inactivityThresholdMinutes: number;
}

export interface ReleaseStaleRunsResult {
  releasedCount: number;
  releasedRunIds: string[];
}

export interface CancelRunResult {
  cancelled: boolean;
  runId: string;
  scheduleId?: string;
  scheduleName?: string;
  previousStatus?: string;
}

export interface LogListItem {
  id: string;
  runId?: string;
  scheduleName?: string;
  connectorName?: string;
  level?: string;
  step?: string;
  message?: string;
  recordKey?: string;
  createdAt?: string;
}

export interface RunFailedRecordItem {
  rowIndex: number;
  externalKey?: string;
  statusCode?: string;
  message?: string;
  retryable?: boolean;
  sourceRecord?: Record<string, unknown>;
  mappedRecord?: Record<string, unknown>;
}

export interface RunFailedRecordsResult {
  runId: string;
  scheduleId?: string;
  scheduleName?: string;
  connectorId?: string;
  connectorName?: string;
  createdAt?: string;
  total: number;
  items: RunFailedRecordItem[];
}

export type LogChartRange = "last_hour" | "last_24h" | "last_30d";

export interface LogChartBucket {
  label: string;
  start: string;
  end: string;
  total: number;
  errors: number;
  connectorErrors: Record<string, number>;
}

export interface LogChartSummary {
  range: LogChartRange;
  buckets: LogChartBucket[];
  connectors: string[];
}

export type OverviewStatsRange = "day" | "month" | "year";

export interface RecordsChartScheduleSummary {
  scheduleId?: string;
  scheduleName?: string;
  connectorId?: string;
  connectorName?: string;
  total: number;
  succeeded: number;
  failed: number;
}

export interface RecordsChartBucket {
  label: string;
  start: string;
  end: string;
  total: number;
  succeeded: number;
  failed: number;
  connectorTotals: Record<string, number>;
  connectorFailures: Record<string, number>;
  connectorSchedules: Record<string, RecordsChartScheduleSummary[]>;
}

export interface RecordsChartSummary {
  range: OverviewStatsRange;
  buckets: RecordsChartBucket[];
  connectors: string[];
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

export interface DeleteConnectorResult {
  connectorId: string;
  connectorName: string;
  deletedScheduleIds: string[];
  deletedScheduleNames: string[];
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

export interface ScheduleConfigurationValidationIssue {
  severity: "error" | "warning";
  area: "general" | "source" | "target" | "mapping" | "connector";
  message: string;
}

export interface ScheduleConfigurationValidationResult {
  ok: boolean;
  issues: ScheduleConfigurationValidationIssue[];
}

async function validatePricebookEntryDryRunConfiguration(
  schedule: ScheduleListItem,
  createClient: () => Promise<SalesforceClient>
): Promise<string | undefined> {
  const targetType = String(schedule.targetType || "").trim().toUpperCase();
  const targetSystem = String(schedule.targetSystem || "").trim().toLowerCase() || (targetType === "SALESFORCE" ? "salesforce" : "");
  const objectName = String(schedule.objectName || "").trim();
  const operation = String(schedule.operation || "").trim().toLowerCase();

  if (targetType !== "SALESFORCE" || targetSystem !== "salesforce" || objectName !== "PricebookEntry" || operation !== "upsert") {
    return undefined;
  }

  let externalIdField = "";
  let targetPricebook2Id = "";
  try {
    const resolvedTargetDefinition = resolveSelectedSalesforceTargetDefinition(String(schedule.targetDefinition || "{}")) as {
      externalIdField?: unknown;
      pricebook2Id?: unknown;
    };

    externalIdField = String(resolvedTargetDefinition?.externalIdField || "").trim();
    targetPricebook2Id = String(resolvedTargetDefinition?.pricebook2Id || "").trim();
  } catch {
    return "Zielkonfiguration ungueltig: Target Definition ist kein valides JSON.";
  }

  if (externalIdField !== "ProductCode") {
    return undefined;
  }

  let hasMappedPricebook2Id = false;
  let mappedStaticPricebook2Id = "";
  try {
    const parsedMapping = JSON.parse(String(schedule.mappingDefinition || "[]"));
    hasMappedPricebook2Id = Array.isArray(parsedMapping)
      && parsedMapping.some((rule) => String((rule as { targetField?: unknown })?.targetField || "").trim() === "Pricebook2Id");
    if (Array.isArray(parsedMapping)) {
      const staticRule = parsedMapping.find((rule) => (
        String((rule as { targetField?: unknown })?.targetField || "").trim() === "Pricebook2Id"
        && String((rule as { transformFunction?: unknown })?.transformFunction || "").trim().toUpperCase() === "STATIC"
        && String((rule as { transformExpression?: unknown })?.transformExpression || "").trim()
      )) as { transformExpression?: unknown } | undefined;
      mappedStaticPricebook2Id = String(staticRule?.transformExpression || "").trim();
    }
  } catch {
    // mapping parser errors are handled elsewhere; keep the config check focused.
  }

  if (!targetPricebook2Id && !hasMappedPricebook2Id) {
    return "Zielkonfiguration unvollstaendig: PricebookEntry mit Upsert-Feld ProductCode benoetigt Pricebook2Id als sichtbares Ziel-Feld oder als Mapping-Ziel.";
  }

  const mappingMessage = validatePricebookEntryMappingDefinition(schedule);
  if (mappingMessage) {
    return mappingMessage;
  }

  const fixedPricebook2Id = targetPricebook2Id || mappedStaticPricebook2Id;
  if (!fixedPricebook2Id) {
    return undefined;
  }

  try {
    const client = await createClient();
    const exists = await client.pricebook2Exists(fixedPricebook2Id);
    if (!exists) {
      return `Zielkonfiguration ungueltig: Pricebook2Id ${fixedPricebook2Id} wurde in Salesforce nicht gefunden.`;
    }
  } catch (error) {
    const details = error instanceof Error ? error.message : String(error || "unbekannter Fehler");
    return `Pricebook2Id-Pruefung fehlgeschlagen: ${details}`;
  }

  return undefined;
}

async function validateRequiredSalesforceFieldMappings(
  input: {
    active?: boolean;
    targetType?: string;
    targetSystem?: string;
    objectName?: string;
    operation?: string;
    targetDefinition?: string;
    mappingDefinition?: string;
  },
  createClient: () => Promise<SalesforceClient>,
  options?: { enforceOnlyWhenActive?: boolean }
): Promise<string | undefined> {
  const enforceOnlyWhenActive = options?.enforceOnlyWhenActive !== false;
  if (enforceOnlyWhenActive && input.active === false) {
    return undefined;
  }

  const targetType = String(input.targetType || "").trim().toUpperCase();
  const targetSystem = String(input.targetSystem || "").trim().toLowerCase() || (targetType === "SALESFORCE" ? "salesforce" : "");
  const objectName = String(input.objectName || "").trim();
  const operation = String(input.operation || "").trim().toLowerCase();

  if (targetType !== "SALESFORCE" || targetSystem !== "salesforce" || !objectName || (operation !== "insert" && operation !== "upsert" && operation !== "update")) {
    return undefined;
  }

  const client = await createClient();
  const objectFields = await client.describeObjectFields(objectName);
  const knownTargetFieldNames = new Set(
    objectFields
      .map((field) => String(field?.name || "").trim().toLowerCase())
      .filter(Boolean)
  );
  const providedTargetFields = new Set<string>();

  try {
    const mappingLines = new MappingDefinitionParser().parse(String(input.mappingDefinition || "")).lines;
    for (const line of mappingLines) {
      const targetField = String(line.targetField || "").trim();
      if (targetField) {
        providedTargetFields.add(targetField.toLowerCase());
      }
    }
  } catch {
    // mapping syntax is validated elsewhere; missing required fields are only checked for parsable mappings.
  }

  try {
    const parsedMapping = JSON.parse(String(input.mappingDefinition || "[]")) as unknown;
    if (Array.isArray(parsedMapping)) {
      for (const rule of parsedMapping) {
        const targetField = String((rule as { targetField?: unknown })?.targetField || "").trim();
        if (targetField) {
          providedTargetFields.add(targetField.toLowerCase());
        }
      }
    }
  } catch {
    // JSON mapping syntax is validated elsewhere.
  }

  let resolvedTargetDefinition: Record<string, unknown> | undefined;
  let parsedTargetDefinition: Record<string, unknown> | undefined;

  try {
    const rawTargetDefinition = String(input.targetDefinition || "{}");
    const parsed = JSON.parse(rawTargetDefinition) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      parsedTargetDefinition = parsed as Record<string, unknown>;
    }

    resolvedTargetDefinition = resolveSelectedSalesforceTargetDefinition(rawTargetDefinition);
    for (const [key, value] of Object.entries(resolvedTargetDefinition)) {
      const normalizedKey = String(key || "").trim().toLowerCase();
      if (!normalizedKey || !knownTargetFieldNames.has(normalizedKey)) {
        continue;
      }
      if (typeof value === "string") {
        if (value.trim()) {
          providedTargetFields.add(normalizedKey.toLowerCase());
        }
        continue;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        providedTargetFields.add(normalizedKey.toLowerCase());
      }
    }
  } catch {
    // targetDefinition syntax is validated elsewhere.
  }

  if (operation === "upsert" || operation === "update") {
    const externalIdField = String(resolvedTargetDefinition?.externalIdField || "").trim();
    if (!externalIdField) {
      return `Aktivierung nicht moeglich: Fuer ${objectName} fehlt ein Upsert-Feld in der Zielkonfiguration.`;
    }

    if (!providedTargetFields.has(externalIdField.toLowerCase())) {
      return `Aktivierung nicht moeglich: Das Upsert-Feld ${externalIdField} ist in der Zielkonfiguration gesetzt, wird aber weder im Mapping noch als statischer Zielwert bereitgestellt.`;
    }

  }

  const missingRequiredFields = objectFields
    .filter((field) => field.requiredOnCreate)
    .map((field) => String(field.name || "").trim())
    .filter((fieldName) => fieldName && !providedTargetFields.has(fieldName.toLowerCase()));

  if (!missingRequiredFields.length) {
    return undefined;
  }

  const actionLabel = input.active === false
    ? "Pflichtfelder fehlen"
    : "Aktivierung nicht moeglich";
  return `${actionLabel}: Fuer ${objectName} fehlen erforderliche Zielfelder im Mapping oder in der Zielkonfiguration: ${missingRequiredFields.join(", ")}.`;
}

const PRICEBOOK_ENTRY_DISALLOWED_PRODUCT_FIELDS = new Set([
  "Name",
  "Description",
  "StockKeepingUnit",
  "Family"
]);

function resolveSelectedSalesforceTargetDefinition(rawTargetDefinition: string): Record<string, unknown> {
  const parsedTargetDefinition = JSON.parse(rawTargetDefinition) as unknown;
  if (!parsedTargetDefinition || typeof parsedTargetDefinition !== "object" || Array.isArray(parsedTargetDefinition)) {
    return {};
  }

  const baseTargetDefinition = parsedTargetDefinition as Record<string, unknown>;

  const typedDefinition = parsedTargetDefinition as {
    selectedImportProfileName?: unknown;
    importProfiles?: Array<{ name?: unknown; target?: unknown }>;
  };

  if (!Array.isArray(typedDefinition.importProfiles) || !typedDefinition.importProfiles.length) {
    return baseTargetDefinition;
  }

  const selectedName = String(typedDefinition.selectedImportProfileName || "").trim();
  const selectedProfile = (selectedName
    ? typedDefinition.importProfiles.find((profile) => String(profile?.name || "").trim() === selectedName)
    : typedDefinition.importProfiles[0]) || typedDefinition.importProfiles[0];

  if (!selectedProfile || typeof selectedProfile !== "object" || Array.isArray(selectedProfile)) {
    return baseTargetDefinition;
  }

  if (selectedProfile.target && typeof selectedProfile.target === "object" && !Array.isArray(selectedProfile.target)) {
    return {
      ...baseTargetDefinition,
      ...(selectedProfile.target as Record<string, unknown>)
    };
  }

  return {
    ...baseTargetDefinition,
    ...(selectedProfile as Record<string, unknown>)
  };
}

function validatePricebookEntryMappingDefinition(input: {
  targetType?: string;
  targetSystem?: string;
  objectName?: string;
  operation?: string;
  targetDefinition?: string;
  mappingDefinition?: string;
}): string | undefined {
  const targetType = String(input.targetType || "").trim().toUpperCase();
  const targetSystem = String(input.targetSystem || "").trim().toLowerCase() || (targetType === "SALESFORCE" ? "salesforce" : "");
  const objectName = String(input.objectName || "").trim();
  const operation = String(input.operation || "").trim().toLowerCase();

  if (targetType !== "SALESFORCE" || targetSystem !== "salesforce" || objectName !== "PricebookEntry" || operation !== "upsert") {
    return undefined;
  }

  let externalIdField = "";
  try {
    const resolvedTargetDefinition = resolveSelectedSalesforceTargetDefinition(String(input.targetDefinition || "{}"));
    externalIdField = String(resolvedTargetDefinition.externalIdField || "").trim();
  } catch {
    return undefined;
  }

  if (externalIdField !== "ProductCode") {
    return undefined;
  }

  let mappingLines: MappingDefinitionLine[] = [];
  try {
    mappingLines = new MappingDefinitionParser().parse(String(input.mappingDefinition || "")).lines;
  } catch {
    return undefined;
  }

  const mappedTargetFields = new Set(
    mappingLines
      .map((line) => String(line.targetField || "").trim())
      .filter(Boolean)
  );

  const invalidTargetFields = Array.from(mappedTargetFields).filter((field) =>
    PRICEBOOK_ENTRY_DISALLOWED_PRODUCT_FIELDS.has(field)
  );
  if (invalidTargetFields.length > 0) {
    return `Zielkonfiguration ungueltig: PricebookEntry unterstuetzt diese Produkt-Felder nicht: ${invalidTargetFields.join(", ")}. Verwende stattdessen ProductCode, UnitPrice, IsActive, Product2Id oder Pricebook2Id.`;
  }

  if (!mappedTargetFields.has("ProductCode")) {
    return "Zielkonfiguration unvollstaendig: PricebookEntry mit Upsert-Feld ProductCode benoetigt ein Mapping auf ProductCode.";
  }

  if (!mappedTargetFields.has("UnitPrice")) {
    return "Zielkonfiguration unvollstaendig: Der Produktpreise-Scheduler benoetigt ein Mapping auf UnitPrice.";
  }

  return undefined;
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

export type { TemplateDefinition, TemplateKind, TemplateMutationInput } from "./template-library";

export interface ApplyTemplateResult {
  templateId: string;
  templateKind: TemplateKind;
  connector?: { id: string; name?: string; action?: "created" | "updated" };
  schedule?: { id: string; name?: string; action?: "created" | "updated" };
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
  fileSheetName?: string;
  availableSheetNames?: string[];
  fileCharset?: string;
  fileDelimiter?: string;
  fileTextQualifier?: string;
  fileRecordCount?: number;
  fileColumns?: string[];
  previewRows?: Record<string, unknown>[];
  previewFilter?: string;
  previewStatusFilter?: string;
  filteredRecordCount?: number;
  stagingMode?: "file" | "sqlite";
  stagingDatabasePath?: string;
  stagingImportedAt?: string;
  stagingStatus?: "pending" | "ready" | "processing" | "done" | "error";
  confirmedSalesforceFields?: string[];
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
  batchSize?: number;
  instanceId?: string;
  salesforceLogin?: MigrationSalesforceInstanceConfig;
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
    reportPath?: string;
    steps: Array<{
      objectId: string;
      salesforceObject: string;
      status: "pending" | "running" | "done" | "error";
      recordsProcessed?: number;
      recordsSucceeded?: number;
      recordsFailed?: number;
      errorMessage?: string;
      failedRecordsId?: string;
    }>;
  };
  runHistory?: Array<{
    startedAt: string;
    finishedAt?: string;
    reportPath?: string;
    steps: Array<{
      objectId: string;
      salesforceObject: string;
      status: "pending" | "running" | "done" | "error";
      recordsProcessed?: number;
      recordsSucceeded?: number;
      recordsFailed?: number;
      errorMessage?: string;
      failedRecordsId?: string;
    }>;
  }>;
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
    status: "pending" | "running" | "done" | "error";
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
  private readonly adaptiveSalesforceCache = new Map<string, { expiresAt: number; value: unknown }>();
  private readonly salesforceApiUsageByInstance = new Map<string, number>();

  private getMigrationInstanceLoginUrl(environment: "sandbox" | "production"): string {
    return environment === "sandbox"
      ? "https://test.salesforce.com"
      : "https://login.salesforce.com";
  }

  private getMigrationOAuthClientCredentials(): { clientId: string; clientSecret: string } {
    const clientId = String(process.env.SF_MIGRATION_OAUTH_CLIENT_ID || process.env.SF_CLIENT_ID || "").trim();
    const clientSecret = String(process.env.SF_MIGRATION_OAUTH_CLIENT_SECRET || process.env.SF_CLIENT_SECRET || "").trim();

    if (!clientId || !clientSecret) {
      throw new Error("Migrations-OAuth benoetigt SF_MIGRATION_OAUTH_CLIENT_ID und SF_MIGRATION_OAUTH_CLIENT_SECRET oder faellt auf SF_CLIENT_ID und SF_CLIENT_SECRET zurueck");
    }

    return { clientId, clientSecret };
  }

  private buildMigrationInstanceId(name: string): string {
    const base = String(name || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || "migration-instance";

    const existing = new Set(readLocalMigrationInstances().map((item) => item.id));
    let nextId = `miginst-${base}`;
    let suffix = 2;
    while (existing.has(nextId)) {
      nextId = `miginst-${base}-${suffix}`;
      suffix += 1;
    }
    return nextId;
  }

  private getFallbackQueryLimit(): number {
    try {
      return getSalesforceConfig().queryLimit;
    } catch {
      return 200;
    }
  }

  private getMigrationInstanceConfig(instanceId: string): MigrationSalesforceInstanceConfig | undefined {
    if (String(instanceId || "").startsWith("migration:")) {
      const migrationId = String(instanceId || "").slice("migration:".length);
      const migration = this.getMigration(migrationId);
      return migration?.salesforceLogin;
    }

    return readLocalMigrationInstances().find((item) => item.id === instanceId);
  }

  private resolveRuntimeInstance(instanceId?: string): ResolvedInstance {
    if (instanceId) {
      const migrationInstance = this.getMigrationInstanceConfig(instanceId);
      if (migrationInstance) {
        const resolvedMigrationInstance = toResolvedMigrationInstance(
          migrationInstance,
          this.getFallbackQueryLimit(),
          this.getMigrationOAuthClientCredentials()
        );
        if (resolvedMigrationInstance) {
          return resolvedMigrationInstance;
        }
      }
    }

    return this.resolveInstance(instanceId);
  }

  private summarizeMigrationForInstance(migration: MigrationConfig): MigrationSalesforceInstanceSummary["lastMigration"] {
    const steps = Array.isArray(migration.lastRunResult?.steps) ? migration.lastRunResult!.steps : [];
    const objectNames = migration.objects
      .map((item) => String(item.salesforceObjectLabel || item.salesforceObject || "").trim())
      .filter(Boolean)
      .slice(0, 6);

    return {
      id: migration.id,
      name: migration.name,
      status: migration.status,
      lastRunAt: migration.lastRunAt,
      objectNames,
      recordsProcessed: steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsProcessed || 0)), 0),
      recordsSucceeded: steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsSucceeded || 0)), 0),
      recordsFailed: steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsFailed || 0)), 0),
      errorMessage: steps.find((step) => String(step.status || "") === "error" && String(step.errorMessage || "").trim())?.errorMessage
    };
  }

  private toMigrationInstanceSummary(
    instance: MigrationSalesforceInstanceConfig,
    migrations: MigrationConfig[]
  ): MigrationSalesforceInstanceSummary {
    const lastMigration = migrations
      .filter((migration) => migration.id === instance.id || migration.instanceId === instance.id || migration.instanceId === `migration:${instance.id}`)
      .sort((left, right) => {
        const rightTime = new Date(String(right.lastRunAt || right.updatedAt || right.createdAt || 0)).getTime();
        const leftTime = new Date(String(left.lastRunAt || left.updatedAt || left.createdAt || 0)).getTime();
        return rightTime - leftTime;
      })[0];

    return {
      id: instance.id,
      name: instance.name,
      environment: instance.environment,
      loginUrl: instance.loginUrl,
      authType: instance.authType || "oauth_refresh_token",
      queryLimit: instance.queryLimit,
      connectionStatus: instance.lastConnectionStatus || "never",
      lastConnectedAt: instance.lastConnectedAt,
      lastConnectionError: instance.lastConnectionError,
      orgOverview: instance.orgOverview,
      objectCount: instance.objectCount,
      lastMigration: lastMigration ? this.summarizeMigrationForInstance(lastMigration) : undefined
    };
  }

  private getStaleRunThresholdMinutes(): number {
    const configured = Number(process.env.SF_STALE_RUN_TIMEOUT_MINUTES?.trim() || "360");
    return Number.isFinite(configured) && configured > 0 ? configured : 360;
  }

  private getStaleRunInactivityThresholdMinutes(): number {
    return getDefaultStaleRunInactivityThresholdMinutes();
  }

  private normalizeSuggestionKey(value: unknown): string {
    return String(value || "")
      .toLowerCase()
      .replace(/__c$/g, "")
      .replace(/[^a-z0-9]+/g, "");
  }

  private tokenizeSuggestionValue(value: unknown): string[] {
    const normalized = String(value || "")
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/__c$/g, "")
      .toLowerCase();

    return Array.from(new Set(
      normalized
        .split(/[^a-z0-9]+/g)
        .map((token) => token.trim())
        .filter((token) => token.length >= 3)
    ));
  }

  private inferSuggestionConfidence(score: number): "high" | "medium" | "low" {
    if (score >= 70) {
      return "high";
    }

    if (score >= 35) {
      return "medium";
    }

    return "low";
  }

  private buildMigrationImportSuggestions(
    fileName: string,
    headers: string[],
    objects: Array<{ name: string; label: string }>,
    describedFieldsByObject: Map<string, Array<{ name: string; label: string }>>
  ): MigrationImportSuggestion[] {
    const fileBaseName = path.basename(String(fileName || "")).replace(/\.[^.]+$/, "");
    const fileKey = this.normalizeSuggestionKey(fileBaseName);
    const fileTokens = this.tokenizeSuggestionValue(fileBaseName);
    const headerTokens = headers.map((header) => this.normalizeSuggestionKey(header)).filter(Boolean);
    const genericHeaderKeys = new Set([
      "id",
      "name",
      "active",
      "operation",
      "objectname",
      "source",
      "target",
      "direction",
      "createddate",
      "lastmodifieddate"
    ]);

    return objects.map((objectMeta) => {
      const objectKey = this.normalizeSuggestionKey(objectMeta.name);
      const objectLabelKey = this.normalizeSuggestionKey(objectMeta.label);
      const objectTokens = [
        ...this.tokenizeSuggestionValue(objectMeta.name),
        ...this.tokenizeSuggestionValue(objectMeta.label)
      ];

      let score = 0;
      const reasons: string[] = [];

      if (fileKey && objectKey && (fileKey.includes(objectKey) || objectKey.includes(fileKey))) {
        score += 50;
        reasons.push("Dateiname passt zum Objektnamen");
      } else if (fileKey && objectLabelKey && (fileKey.includes(objectLabelKey) || objectLabelKey.includes(fileKey))) {
        score += 40;
        reasons.push("Dateiname passt zum Objektlabel");
      } else if (fileTokens.some((token) => objectTokens.includes(token))) {
        score += 28;
        reasons.push("Dateiname enthält einen passenden Objektbegriff");
      }

      const fields = describedFieldsByObject.get(objectMeta.name) || [];
      const matchedHeaders = headers.filter((header) => {
        const headerKey = this.normalizeSuggestionKey(header);
        if (!headerKey || genericHeaderKeys.has(headerKey)) {
          return false;
        }

        return fields.some((field) => {
          const fieldNameKey = this.normalizeSuggestionKey(field.name);
          const fieldLabelKey = this.normalizeSuggestionKey(field.label);
          return headerKey === fieldNameKey || headerKey === fieldLabelKey;
        });
      });

      if (matchedHeaders.length > 0) {
        score += Math.min(48, matchedHeaders.length * 12);
        reasons.push(matchedHeaders.length + " Feldübereinstimmungen");
      } else {
        const partialHeaderMatches = headerTokens.filter((headerKey) => {
          if (!headerKey || genericHeaderKeys.has(headerKey)) {
            return false;
          }

          return fields.some((field) => {
            const fieldNameKey = this.normalizeSuggestionKey(field.name);
            const fieldLabelKey = this.normalizeSuggestionKey(field.label);
            return fieldNameKey.includes(headerKey) || headerKey.includes(fieldNameKey) || fieldLabelKey.includes(headerKey) || headerKey.includes(fieldLabelKey);
          });
        });

        if (partialHeaderMatches.length > 0) {
          score += Math.min(24, partialHeaderMatches.length * 6);
          reasons.push(partialHeaderMatches.length + " teilweise passende Felder");
        }
      }

      return {
        objectApiName: objectMeta.name,
        label: objectMeta.label || objectMeta.name,
        score,
        confidence: this.inferSuggestionConfidence(score),
        matchedHeaders: matchedHeaders.slice(0, 6),
        reason: reasons.join(", ") || "Allgemeine Heuristik"
      } satisfies MigrationImportSuggestion;
    }).filter((suggestion) => suggestion.score > 0)
      .sort((left, right) => right.score - left.score || left.objectApiName.localeCompare(right.objectApiName, "de"))
      .slice(0, 5);
  }

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

  private resolveMigrationBatchSize(migrationOrBatchSize?: MigrationConfig | number): number {
    const rawValue = typeof migrationOrBatchSize === "number"
      ? migrationOrBatchSize
      : migrationOrBatchSize?.batchSize;
    const parsed = Number(rawValue || 200);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 200;
    }
    return Math.max(1, Math.min(200, Math.trunc(parsed)));
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
        const targetFieldType = String(mapping.targetFieldType || "").trim().toLowerCase();
        const lookupAllowedForTarget = targetFieldType === "reference" || targetFieldType === "id";
        const transformType = lookupEnabled && lookupObject && lookupField && lookupAllowedForTarget
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

  private async createMigrationLookupResolver(
    client: SalesforceClient,
    mappingLines: MappingDefinitionLine[],
    sourceRecords: Array<Record<string, unknown>>
  ): Promise<LookupResolverFn> {
    const lookupLines = mappingLines.filter(
      (line) => line.transform.type === "LOOKUP" && line.transform.lookupObject && line.transform.lookupField
    );
    const lookupCache = new Map<string, string | null>();
    const groupedLookups = new Map<string, { lookupObject: string; lookupField: string; values: Set<string> }>();

    for (const line of lookupLines) {
      const lookupObject = String(line.transform.lookupObject || "").trim();
      const lookupField = String(line.transform.lookupField || "").trim();
      const sourceField = String(line.sourceField || "").trim();
      if (!lookupObject || !lookupField || !sourceField) {
        continue;
      }

      const groupKey = `${lookupObject}|${lookupField}`;
      if (!groupedLookups.has(groupKey)) {
        groupedLookups.set(groupKey, { lookupObject, lookupField, values: new Set<string>() });
      }

      const group = groupedLookups.get(groupKey)!;
      for (const record of sourceRecords) {
        const rawValue = record[sourceField];
        if (rawValue === undefined || rawValue === null || rawValue === "") {
          continue;
        }
        group.values.add(String(rawValue));
      }
    }

    for (const group of groupedLookups.values()) {
      const uniqueValues = Array.from(group.values);
      const chunkSize = 200;

      for (let index = 0; index < uniqueValues.length; index += chunkSize) {
        const chunk = uniqueValues.slice(index, index + chunkSize);
        if (!chunk.length) {
          continue;
        }

        const soql = `SELECT Id, ${group.lookupField} FROM ${group.lookupObject} WHERE ${group.lookupField} IN (${chunk
          .map((value) => this.toSoqlLiteral(value))
          .join(", ")})`;
        const result = await client.queryGeneric(soql);

        for (const row of result) {
          const fieldValue = row[group.lookupField];
          if (fieldValue === undefined || fieldValue === null || fieldValue === "") {
            continue;
          }
          const cacheKey = `${group.lookupObject}|${group.lookupField}|${String(fieldValue)}`;
          if (!lookupCache.has(cacheKey)) {
            lookupCache.set(cacheKey, typeof row.Id === "string" ? row.Id : null);
          }
        }

        for (const value of chunk) {
          const cacheKey = `${group.lookupObject}|${group.lookupField}|${value}`;
          if (!lookupCache.has(cacheKey)) {
            lookupCache.set(cacheKey, null);
          }
        }
      }
    }

    return async (lookupObject: string, lookupField: string, value: unknown): Promise<string | null> => {
      if (value === undefined || value === null || value === "") {
        return null;
      }

      const normalizedValue = String(value);
      const cacheKey = `${lookupObject}|${lookupField}|${normalizedValue}`;
      if (lookupCache.has(cacheKey)) {
        return lookupCache.get(cacheKey) ?? null;
      }

      const soql = `SELECT Id FROM ${lookupObject} WHERE ${lookupField} = ${this.toSoqlLiteral(value)} LIMIT 1`;
      const result = await client.queryGeneric(soql);
      const resolvedId = result.length && typeof result[0].Id === "string" ? result[0].Id : null;
      lookupCache.set(cacheKey, resolvedId);
      return resolvedId;
    };
  }

  public listInstances(): SalesforceInstanceOption[] {
    const projectById = new Map(readLocalProjects().map((project) => [project.id, project]));
    const configById = new Map(readConfiguredInstancesWithMetadata().map((instance) => [instance.id, instance]));
    const instances = resolveInstances();
    return instances.map((instance, index) => {
      const configured = configById.get(instance.id);
      const projectId = String(configured?.projectId || "default-project").trim() || "default-project";
      const projectName = projectById.get(projectId)?.name || projectById.get("default-project")?.name || "Default-Projekt";
      const role = configured?.role === "production" ? "production" : "test";

      return {
        id: instance.id,
        name: instance.name,
        isDefault: index === 0,
        projectId,
        projectName,
        role
      };
    });
  }

  public assertInstanceWriteAllowed(instanceId?: string, operation?: string): void {
    const resolved = this.resolveRuntimeInstance(instanceId);
    const metadataById = new Map(readConfiguredInstancesWithMetadata().map((item) => [item.id, item]));
    const instanceMeta = metadataById.get(resolved.id);

    const role: "test" | "production" = instanceMeta?.role === "production"
      ? "production"
      : (resolved.id === "default" ? "production" : "test");
    if (role !== "production") {
      return;
    }

    const projects = readLocalProjects();
    const projectById = new Map(projects.map((project) => [project.id, project]));
    const projectId = String(instanceMeta?.projectId || "default-project").trim() || "default-project";
    const project = projectById.get(projectId) || projectById.get("default-project") || defaultProjectConfig();

    if (project.productionWriteProtection) {
      const details = operation ? ` (${operation})` : "";
      throw new Error(`Schreibzugriff blockiert: Instanz ${resolved.name} (${resolved.id}) ist als Produktion im Projekt ${project.name} mit aktivem Produktionsschutz konfiguriert${details}.`);
    }
  }

  public listConfiguredInstanceConfigs(): SalesforceInstanceEnvConfig[] {
    return readConfiguredSalesforceInstances();
  }

  public listProjects(): SalesforceProjectOption[] {
    return readLocalProjects()
      .map((project) => ({ ...project }))
      .sort((left, right) => left.name.localeCompare(right.name, "de"));
  }

  public saveProject(input: SalesforceProjectMutationInput): SalesforceProjectOption {
    const name = String(input.name || "").trim();
    if (!name) {
      throw new Error("name ist erforderlich");
    }

    const projects = readLocalProjects();
    const now = new Date().toISOString();
    const desiredId = String(input.id || "").trim() || buildProjectIdFromName(name);

    const existingByName = projects.find((project) => project.name.toLowerCase() === name.toLowerCase());
    const id = existingByName && !input.id ? existingByName.id : desiredId;
    const existingProject = projects.find((project) => project.id === id);

    const nextItem: SalesforceProjectConfig = {
      id,
      name,
      description: String(input.description || "").trim() || undefined,
      archived: input.archived === undefined ? (existingProject?.archived === true) : input.archived === true,
      productionWriteProtection: input.productionWriteProtection !== false,
      createdAt: existingProject?.createdAt || now,
      updatedAt: now
    };

    const existingIndex = projects.findIndex((project) => project.id === id);
    if (existingIndex >= 0) {
      projects[existingIndex] = nextItem;
    } else {
      projects.push(nextItem);
    }

    writeLocalProjects(projects);
    return { ...nextItem };
  }

  public setProjectArchived(projectId: string, archived: boolean): SalesforceProjectOption {
    const normalizedProjectId = String(projectId || "").trim();
    if (!normalizedProjectId) {
      throw new Error("projectId ist erforderlich");
    }
    if (normalizedProjectId === "default-project" && archived) {
      throw new Error("Default-Projekt kann nicht archiviert werden");
    }

    const projects = readLocalProjects();
    const projectIndex = projects.findIndex((entry) => entry.id === normalizedProjectId);
    if (projectIndex < 0) {
      throw new Error(`Projekt ${normalizedProjectId} nicht gefunden`);
    }

    const updated: SalesforceProjectConfig = {
      ...projects[projectIndex],
      archived,
      updatedAt: new Date().toISOString()
    };
    projects[projectIndex] = updated;
    writeLocalProjects(projects);

    return { ...updated };
  }

  public deleteProject(projectId: string): { deleted: boolean; projectId: string } {
    const normalizedProjectId = String(projectId || "").trim();
    if (!normalizedProjectId) {
      throw new Error("projectId ist erforderlich");
    }
    if (normalizedProjectId === "default-project") {
      throw new Error("Default-Projekt kann nicht geloescht werden");
    }

    const assignedInstances = readConfiguredInstancesWithMetadata().filter((instance) =>
      String(instance.projectId || "default-project").trim() === normalizedProjectId
    );
    if (assignedInstances.length) {
      throw new Error(`Projekt ${normalizedProjectId} kann nicht geloescht werden, da noch ${assignedInstances.length} Instanz(en) zugeordnet sind.`);
    }

    const projects = readLocalProjects();
    const filtered = projects.filter((entry) => entry.id !== normalizedProjectId);
    const deleted = filtered.length < projects.length;

    if (deleted) {
      writeLocalProjects(filtered);
    }

    return { deleted, projectId: normalizedProjectId };
  }

  public saveInstance(input: SalesforceInstanceMutationInput): SalesforceInstanceOption {
    const id = input.id.trim();
    const loginUrl = input.loginUrl.trim();
    const projectId = String(input.projectId || "default-project").trim() || "default-project";
    const role = input.role === "production" ? "production" : "test";
    const clientId = input.clientId.trim();
    const clientSecret = input.clientSecret.trim();

    if (!id || !loginUrl || !clientId || !clientSecret) {
      throw new Error("id, loginUrl, clientId und clientSecret sind erforderlich");
    }

    const projects = readLocalProjects();
    const project = projects.find((item) => item.id === projectId);
    if (!project) {
      throw new Error(`Projekt ${projectId} nicht gefunden`);
    }

    const localInstances = readLocalInstances();
    const configuredInstances = readConfiguredInstancesWithMetadata();

    if (role === "production") {
      const conflictingProduction = configuredInstances.find((item) => (
        item.projectId === projectId
        && item.role === "production"
        && item.id !== id
      ));
      if (conflictingProduction) {
        throw new Error(`Projekt ${project.name} hat bereits eine Produktionsinstanz (${conflictingProduction.id}).`);
      }
    }

    const nextItem: SalesforceInstanceEnvConfig = {
      id,
      name: input.name?.trim() || id,
      loginUrl,
      projectId,
      role,
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
    return {
      id: nextItem.id,
      name: nextItem.name || nextItem.id,
      isDefault: false,
      projectId,
      projectName: project.name,
      role
    };
  }

  public listMigrationInstances(): MigrationSalesforceInstanceSummary[] {
    const migrations = this.readMigrationsStore();
    return migrations
      .filter((migration) => migration.salesforceLogin)
      .map((migration) => {
        const instance = {
          ...(migration.salesforceLogin as MigrationSalesforceInstanceConfig),
          id: migration.id,
          name: migration.salesforceLogin?.name || migration.name
        } as MigrationSalesforceInstanceConfig;
        return this.toMigrationInstanceSummary(instance, migrations);
      })
      .sort((left, right) => left.name.localeCompare(right.name, "de"));
  }

  public saveMigrationInstance(input: MigrationSalesforceInstanceMutationInput): MigrationSalesforceInstanceSummary {
    const name = String(input.name || "").trim();
    const environment = input.environment === "sandbox" ? "sandbox" : "production";
    const loginUrl = String(input.loginUrl || this.getMigrationInstanceLoginUrl(environment)).trim() || this.getMigrationInstanceLoginUrl(environment);
    const authType = input.authType === "password"
      ? "password"
      : (input.authType === "client_credentials" ? "client_credentials" : "oauth_refresh_token");
    const username = String(input.username || "").trim();
    const password = String(input.password || "");
    const securityToken = String(input.securityToken || "").trim() || undefined;
    const clientId = String(input.clientId || "").trim() || undefined;
    const clientSecret = String(input.clientSecret || "").trim() || undefined;

    if (!input.id) {
      throw new Error("Migration ID ist fuer eingebettete Migrations-Logins erforderlich");
    }

    const migration = this.getMigration(String(input.id));
    if (!migration) {
      throw new Error(`Migration ${input.id} nicht gefunden`);
    }
    const authTypeChanged = String(migration.salesforceLogin?.authType || "") !== authType;

    const nextItem: MigrationSalesforceInstanceConfig = {
      id: migration.id,
      name: name || migration.name,
      environment,
      loginUrl,
      authType,
      username: authType === "password" ? username : undefined,
      password: authType === "password" ? password : undefined,
      securityToken: authType === "password" ? securityToken : undefined,
      clientId: authType === "client_credentials" ? clientId : undefined,
      clientSecret: authType === "client_credentials" ? clientSecret : undefined,
      instanceUrl: migration.salesforceLogin?.instanceUrl,
      accessToken: authType === "oauth_refresh_token" ? migration.salesforceLogin?.accessToken : undefined,
      refreshToken: authType === "oauth_refresh_token" ? migration.salesforceLogin?.refreshToken : undefined,
      tokenIssuedAt: authType === "oauth_refresh_token" ? migration.salesforceLogin?.tokenIssuedAt : undefined,
      queryLimit: input.queryLimit,
      lastConnectionStatus: authTypeChanged ? "never" : (migration.salesforceLogin?.lastConnectionStatus || "never"),
      lastConnectedAt: authTypeChanged ? undefined : migration.salesforceLogin?.lastConnectedAt,
      lastConnectionError: authTypeChanged ? undefined : migration.salesforceLogin?.lastConnectionError,
      orgOverview: migration.salesforceLogin?.orgOverview,
      objectCount: migration.salesforceLogin?.objectCount,
      updatedAt: new Date().toISOString()
    };

    const savedMigration = this.saveMigration({
      ...migration,
      salesforceLogin: nextItem
    });

    return this.toMigrationInstanceSummary(nextItem, this.readMigrationsStore().filter((entry) => entry.id === savedMigration.id));
  }

  public getMigrationOAuthAuthorizationUrl(instanceId: string, state: string, redirectUri: string): string {
    const migration = this.getMigration(instanceId);
    if (!migration?.salesforceLogin) {
      throw new Error(`Migration ${instanceId} hat keinen konfigurierten Salesforce-Login`);
    }

    if (migration.salesforceLogin.authType === "password") {
      throw new Error("Diese Migration verwendet Benutzername/Passwort. Ein OAuth-Allow-Flow ist dafuer nicht erforderlich.");
    }

    const { clientId } = this.getMigrationOAuthClientCredentials();
    const authorizationUrl = new URL(`${normalizeMigrationSalesforceLoginUrl(migration.salesforceLogin.loginUrl, migration.salesforceLogin.environment)}/services/oauth2/authorize`);
    authorizationUrl.searchParams.set("response_type", "code");
    authorizationUrl.searchParams.set("client_id", clientId);
    authorizationUrl.searchParams.set("redirect_uri", redirectUri);
    authorizationUrl.searchParams.set("state", state);
    authorizationUrl.searchParams.set("scope", String(process.env.SF_MIGRATION_OAUTH_SCOPE || "api refresh_token").trim() || "api refresh_token");
    authorizationUrl.searchParams.set("display", "popup");
    return authorizationUrl.toString();
  }

  public async completeMigrationOAuth(instanceId: string, code: string, redirectUri: string): Promise<MigrationSalesforceInstanceSummary> {
    const migration = this.getMigration(instanceId);
    if (!migration?.salesforceLogin) {
      throw new Error(`Migration ${instanceId} hat keinen konfigurierten Salesforce-Login`);
    }

    const oauthClient = this.getMigrationOAuthClientCredentials();
    const tokenUrl = `${normalizeMigrationSalesforceLoginUrl(migration.salesforceLogin.loginUrl, migration.salesforceLogin.environment)}/services/oauth2/token`;
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: oauthClient.clientId,
      client_secret: oauthClient.clientSecret,
      redirect_uri: redirectUri,
      code: String(code || "").trim()
    });

    try {
      const response = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body
      });
      const rawText = await response.text();
      let tokenData: MigrationOAuthTokenResponse = {};

      try {
        tokenData = JSON.parse(rawText) as MigrationOAuthTokenResponse;
      } catch {
        tokenData = {};
      }

      if (!response.ok) {
        throw new Error(
          formatSalesforceOauthError(
            tokenData.error,
            tokenData.error_description,
            migration.salesforceLogin.loginUrl
          ) || rawText || `Salesforce OAuth Callback fehlgeschlagen (${response.status})`
        );
      }

      const accessToken = String(tokenData.access_token || "").trim();
      const refreshToken = String(tokenData.refresh_token || migration.salesforceLogin.refreshToken || "").trim();
      const instanceUrl = String(tokenData.instance_url || migration.salesforceLogin.instanceUrl || "").trim();
      if (!accessToken || !refreshToken || !instanceUrl) {
        throw new Error("Salesforce OAuth Antwort enthaelt kein access_token, refresh_token oder instance_url");
      }

      migration.salesforceLogin = {
        ...migration.salesforceLogin,
        authType: "oauth_refresh_token",
        accessToken,
        refreshToken,
        instanceUrl,
        tokenIssuedAt: tokenData.issued_at && Number.isFinite(Number(tokenData.issued_at))
          ? new Date(Number(tokenData.issued_at)).toISOString()
          : new Date().toISOString(),
        lastConnectionStatus: "connected",
        lastConnectedAt: new Date().toISOString(),
        lastConnectionError: undefined,
        updatedAt: new Date().toISOString()
      };

      this.saveMigration(migration);
      return this.connectMigrationInstance(instanceId);
    } catch (error) {
      migration.salesforceLogin = {
        ...migration.salesforceLogin,
        lastConnectionStatus: "error",
        lastConnectionError: error instanceof Error ? error.message : String(error),
        updatedAt: new Date().toISOString()
      };
      this.saveMigration(migration);
      throw error;
    }
  }

  public async connectMigrationInstance(instanceId: string): Promise<MigrationSalesforceInstanceSummary> {
    const migration = this.getMigration(instanceId);
    if (!migration?.salesforceLogin) {
      throw new Error(`Migration ${instanceId} hat keinen gespeicherten Salesforce-Login`);
    }

    if (migration.salesforceLogin.authType === "oauth_refresh_token" && !String(migration.salesforceLogin.refreshToken || "").trim()) {
      throw new Error(`Migration ${instanceId} hat noch keine Salesforce-Freigabe. Bitte den Login ueber die Salesforce-Login-Seite starten.`);
    }

    const fallbackQueryLimit = this.getFallbackQueryLimit();
    const resolved = toResolvedMigrationInstance(
      migration.salesforceLogin,
      fallbackQueryLimit,
      this.getMigrationOAuthClientCredentials()
    );
    if (!resolved) {
      throw new Error(`Migration instance ${instanceId} ist unvollstaendig konfiguriert`);
    }

    try {
      const client = new SalesforceClient(resolved.config);
      await client.login();
      const [orgOverview, objectMetadata] = await Promise.all([
        client.getOrgOverview(),
        client.listObjectMetadata()
      ]);

      migration.salesforceLogin = {
        ...migration.salesforceLogin,
        lastConnectionStatus: "connected",
        lastConnectedAt: new Date().toISOString(),
        lastConnectionError: undefined,
        orgOverview,
        objectCount: objectMetadata.length,
        updatedAt: new Date().toISOString()
      };
    } catch (error) {
      migration.salesforceLogin = {
        ...migration.salesforceLogin,
        lastConnectionStatus: "error",
        lastConnectionError: error instanceof Error ? error.message : String(error),
        updatedAt: new Date().toISOString()
      };
      this.saveMigration(migration);
      throw error;
    }

    const savedMigration = this.saveMigration(migration);
    return this.toMigrationInstanceSummary(savedMigration.salesforceLogin!, this.readMigrationsStore().filter((entry) => entry.id === savedMigration.id));
  }

  public async listSchedules(instanceId?: string): Promise<ScheduleListItem[]> {
    const resolvedInstance = this.resolveInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, "listSchedules", async () => {
      const client = await this.createClient(resolvedInstance.id);
      const records = await client.querySchedules(false);
      const runningRuns = await client.queryRunningRuns(200);
      const localTiming = readLocalScheduleTimingStore()[resolvedInstance.id] || {};
      const localHealth = readLocalScheduleHealthStore();
      const runningScheduleIds = new Set(
        runningRuns
          .map((run) => String(run.MSD_Schedule__c || "").trim())
          .filter(Boolean)
      );

      const checkpointEntries = await Promise.all(records.map(async (record) => {
        const schedule = this.toIntegrationSchedule(record);
        try {
          const checkpoint = await client.getCheckpoint(schedule.id, schedule.objectName);
          return [schedule.id, checkpoint] as const;
        } catch {
          return [schedule.id, null] as const;
        }
      }));
      const checkpointsByScheduleId = new Map(checkpointEntries);

      return records.map((record) => {
        const schedule = this.toIntegrationSchedule(record);
        const persistedTimingDefinition = localTiming[schedule.id] || schedule.timingDefinition;
        const effectiveSchedule: IntegrationSchedule = {
          ...schedule,
          timingDefinition: persistedTimingDefinition
        };
        const checkpoint = checkpointsByScheduleId.get(schedule.id) || null;

        return {
          id: schedule.id,
          name: schedule.name,
          createdAt: record.CreatedDate,
          createdByName: record.CreatedBy?.Name,
          createdByUsername: record.CreatedBy?.Username,
          lastModifiedAt: record.LastModifiedDate,
          lastModifiedByName: record.LastModifiedBy?.Name,
          lastModifiedByUsername: record.LastModifiedBy?.Username,
          active: schedule.active,
          status: runningScheduleIds.has(schedule.id) ? "running" : this.getScheduleStatus(effectiveSchedule),
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
          autoDisabledAt: localHealth[schedule.id]?.autoDisabledAt,
          currentDeltaCheckpoint: checkpoint?.lastCheckpoint,
          currentDeltaRecordId: checkpoint?.lastRecordId,
          currentDeltaRunId: checkpoint?.lastRunId
        };
      });
    });
  }

  public async getScheduleCheckpoint(scheduleId: string, instanceId?: string) {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const schedules = await this.listSchedules(resolvedInstance.id);
    const schedule = schedules.find((item) => item.id === scheduleId);
    if (!schedule) {
      throw new Error(`Scheduler ${scheduleId} wurde nicht gefunden`);
    }

    return (await client.getCheckpoint(schedule.id, schedule.objectName)) || {
      id: undefined,
      scheduleId: schedule.id,
      objectName: schedule.objectName,
      lastCheckpoint: undefined,
      lastRecordId: undefined,
      lastRunId: undefined
    };
  }

  public async updateScheduleCheckpoint(
    scheduleId: string,
    input: ScheduleCheckpointMutationInput,
    instanceId?: string
  ): Promise<{ id: string; action: "updated" }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const schedules = await this.listSchedules(resolvedInstance.id);
    const schedule = schedules.find((item) => item.id === scheduleId);
    if (!schedule) {
      throw new Error(`Scheduler ${scheduleId} wurde nicht gefunden`);
    }

    const checkpoint = await client.getCheckpoint(schedule.id, schedule.objectName);
    if (!checkpoint?.id || !checkpoint.lastRunId) {
      throw new Error("Für diesen Scheduler existiert noch kein Delta-Checkpoint. Bitte zuerst mindestens einen Lauf ausführen.");
    }

    const id = await client.upsertCheckpoint({
      checkpointId: checkpoint.id,
      scheduleId: schedule.id,
      objectName: schedule.objectName,
      lastCheckpoint: String(input.lastCheckpoint || "").trim() || undefined,
      lastRecordId: String(input.lastRecordId || "").trim() || undefined,
      lastRunId: checkpoint.lastRunId
    });

    return { id, action: "updated" };
  }

  public async getScheduleFormOptions(instanceId?: string): Promise<ScheduleFormOptions> {
    const resolvedInstance = this.resolveInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, "scheduleFormOptions", async () => {
    const client = await this.createClient(resolvedInstance.id);
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
      sourceSystems: collectUnique(records.map((record) => record.SourceSystem__c), sourceSystems.length ? sourceSystems : [
        "MS SQL",
        "Salesforce",
        "File"
      ]),
      targetSystems: collectUnique(records.map((record) => record.TargetSystem__c), targetSystems.length ? targetSystems : [
        "Salesforce",
        "MS SQL",
        "File"
      ]),
      directions: collectUnique(records.map((record) => record.MSD_Direction__c), directions.length ? directions : [
        "Outbound",
        "Inbound",
        "Bidirectional"
      ])
    };
    });
  }

  public async listConnectors(instanceId?: string): Promise<ConnectorListItem[]> {
    const resolvedInstance = this.resolveInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, "listConnectors", async () => {
      const client = await this.createClient(resolvedInstance.id);
      const connectors = await client.queryConnectors();

      return connectors.map((connector) => ({
        id: connector.id,
        name: connector.name,
        createdAt: connector.createdAt,
        createdByName: connector.createdByName,
        createdByUsername: connector.createdByUsername,
        lastModifiedAt: connector.lastModifiedAt,
        lastModifiedByName: connector.lastModifiedByName,
        lastModifiedByUsername: connector.lastModifiedByUsername,
        active: connector.active,
        connectorType: connector.connectorType,
        targetSystem: connector.targetSystem,
        direction: connector.direction,
        secretKey: connector.secretKey,
        timeoutMs: connector.timeoutMs,
        maxRetries: connector.maxRetries,
        description: connector.description,
        parameters: connector.parameters,
        filePaths: this.isFileConnectorType(connector.connectorType)
          ? this.resolveFileConnectorPaths(connector.parameters || {})
          : undefined,
        hasSecret: Boolean(connector.secretKey),
        parameterKeys: Object.keys(connector.parameters).sort()
      }));
    });
  }

  public async testConnector(connectorId: string, instanceId?: string): Promise<ConnectorTestResult> {
    const client = await this.createClient(instanceId);
    const config = await client.queryConnector(connectorId);

    if (String(config.connectorType || "").trim().toLowerCase() === "mssql") {
      return this.testMssqlConnector(config);
    }

    if (this.isFileConnectorType(config.connectorType)) {
      return this.testFileConnector(config);
    }

    if (this.isRestConnectorType(config.connectorType)) {
      return this.testRestConnector(config);
    }

    const registry = new ConnectorRegistry();

    try {
      const connector = registry.getConnectorByConfig(config);
      const ok = await connector.testConnection();
      return this.buildConnectorTestResult(config, [
        {
          label: "Verbindungstest",
          ok,
          details: ok ? "Verbindung erfolgreich hergestellt." : "Der Connector hat false zurueckgegeben."
        }
      ]);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown connector test error";
      return this.buildConnectorTestResult(config, [
        {
          label: "Verbindungstest",
          ok: false,
          details: message
        }
      ]);
    }
  }

  private buildConnectorTestResult(
    config: ConnectorConfig,
    checks: Array<{ label: string; ok: boolean; details: string }>
  ): ConnectorTestResult {
    const normalizedChecks = checks.map((check) => ({
      label: String(check.label || "Pruefung").trim() || "Pruefung",
      ok: Boolean(check.ok),
      details: String(check.details || "").trim() || (check.ok ? "OK" : "Fehlgeschlagen")
    }));
    const ok = normalizedChecks.every((check) => check.ok);
    const failedCheck = normalizedChecks.find((check) => !check.ok);
    const passedCount = normalizedChecks.filter((check) => check.ok).length;
    const message = ok
      ? `${passedCount}/${normalizedChecks.length} Pruefungen erfolgreich.`
      : failedCheck?.details || `${normalizedChecks.length - passedCount} Pruefung(en) fehlgeschlagen.`;

    return {
      ok,
      connectorId: config.id,
      connectorName: config.name,
      connectorType: config.connectorType,
      message,
      testedAt: new Date().toISOString(),
      checks: normalizedChecks
    };
  }

  private async testMssqlConnector(config: ConnectorConfig): Promise<ConnectorTestResult> {
    const server = getRequiredString(config.parameters, "server");
    const port = getOptionalNumber(config.parameters, "port") || 1433;
    const databaseName = getRequiredString(config.parameters, "database");
    const database = new MssqlDatabase({
      server,
      port,
      database: databaseName,
      user: getRequiredString(config.parameters, "user"),
      password: resolvePassword(config),
      encrypt: getOptionalBoolean(config.parameters, "encrypt"),
      trustServerCertificate: getOptionalBoolean(config.parameters, "trustServerCertificate"),
      connectionTimeout: config.timeoutMs,
      requestTimeout: config.timeoutMs
    });

    const checks: Array<{ label: string; ok: boolean; details: string }> = [];

    try {
      await database.testConnection();
      checks.push({
        label: "SQL Server erreichbar",
        ok: true,
        details: `${server}:${port} / ${databaseName}`
      });

      const result = await database.query<{ TABLE_NAME: string; TABLE_SCHEMA: string }>(
        "SELECT TOP 10 TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME"
      );
      const tables = result.recordset
        .map((row) => (row.TABLE_SCHEMA ? `${row.TABLE_SCHEMA}.${row.TABLE_NAME}` : row.TABLE_NAME))
        .filter(Boolean);
      checks.push({
        label: "Tabellen lesbar",
        ok: true,
        details: tables.length ? `${tables.length} Tabelle(n) gefunden, z. B. ${tables.slice(0, 3).join(", ")}` : "Verbindung erfolgreich, aber keine Basistabellen gefunden."
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unbekannter MSSQL-Fehler";
      if (!checks.length) {
        checks.push({
          label: "SQL Server erreichbar",
          ok: false,
          details: message
        });
      }
      if (checks.length === 1) {
        checks.push({
          label: "Tabellen lesbar",
          ok: false,
          details: "Tabellen konnten wegen des Verbindungsfehlers nicht gelesen werden."
        });
      }
    } finally {
      await database.close();
    }

    return this.buildConnectorTestResult(config, checks);
  }

  private async testRestConnector(config: ConnectorConfig): Promise<ConnectorTestResult> {
    const baseUrl = String(config.parameters?.baseUrl || "").trim();
    const endpoint = String(config.parameters?.resourcePath || config.parameters?.endpoint || "").trim();
    const method = String(config.parameters?.method || "GET").trim().toUpperCase() || "GET";
    const authType = String(config.parameters?.authType || "none").trim().toLowerCase() || "none";
    const checks: Array<{ label: string; ok: boolean; details: string }> = [];

    try {
      const resolvedUrl = endpoint ? new URL(endpoint, baseUrl).toString() : new URL(baseUrl).toString();
      checks.push({
        label: "API URL valide",
        ok: true,
        details: `${method} ${resolvedUrl}`
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Ungueltige URL";
      return this.buildConnectorTestResult(config, [
        {
          label: "API URL valide",
          ok: false,
          details: message
        },
        {
          label: authType === "none" ? "API Zugriff" : "Authentifizierung + API Zugriff",
          ok: false,
          details: "Request wurde wegen ungueltiger URL nicht ausgefuehrt."
        }
      ]);
    }

    try {
      const response = await testRestConnection(config, {
        endpoint: endpoint || baseUrl,
        method
      });
      checks.push({
        label: authType === "none" ? "API Zugriff" : "Authentifizierung + API Zugriff",
        ok: true,
        details: `Request erfolgreich (${response.status} ${response.statusText || "OK"}), Content-Type: ${response.contentType}`
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unbekannter REST-Fehler";
      checks.push({
        label: authType === "none" ? "API Zugriff" : "Authentifizierung + API Zugriff",
        ok: false,
        details: message
      });
    }

    return this.buildConnectorTestResult(config, checks);
  }

  private testFileConnector(config: ConnectorConfig): ConnectorTestResult {
    const { importPath, exportPath, archivePath } = this.resolveFileConnectorPaths(config.parameters || {});
    const checks: Array<{ label: string; ok: boolean; details: string }> = [];

    try {
      fs.accessSync(importPath, fs.constants.R_OK);
      checks.push({
        label: "Dateipfad lesbar",
        ok: true,
        details: importPath
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Importpfad ist nicht lesbar";
      checks.push({
        label: "Dateipfad lesbar",
        ok: false,
        details: `${importPath} (${message})`
      });
    }

    try {
      fs.mkdirSync(archivePath, { recursive: true });
      fs.accessSync(archivePath, fs.constants.W_OK);
      checks.push({
        label: "Archivpfad beschreibbar",
        ok: true,
        details: archivePath
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Archivpfad ist nicht beschreibbar";
      checks.push({
        label: "Archivpfad beschreibbar",
        ok: false,
        details: `${archivePath} (${message})`
      });
    }

    try {
      fs.mkdirSync(exportPath, { recursive: true });
      fs.accessSync(exportPath, fs.constants.W_OK);
      checks.push({
        label: "Exportpfad beschreibbar",
        ok: true,
        details: exportPath
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Exportpfad ist nicht beschreibbar";
      checks.push({
        label: "Exportpfad beschreibbar",
        ok: false,
        details: `${exportPath} (${message})`
      });
    }

    return this.buildConnectorTestResult(config, checks);
  }

  private resolveFileConnectorPaths(parameters: Record<string, unknown>): FileConnectorPathSummary {
    const basePath = path.resolve(process.cwd(), String(parameters.basePath || parameters.fileBasePath || "artifacts/files"));
    return {
      basePath,
      importPath: path.resolve(basePath, String(parameters.importPath || "inbound")),
      exportPath: path.resolve(basePath, String(parameters.exportPath || "outbound")),
      archivePath: path.resolve(basePath, String(parameters.archivePath || "archive"))
    };
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

    const requiredFieldsMessage = await validateRequiredSalesforceFieldMappings(
      schedule,
      () => this.createClient(instanceId),
      { enforceOnlyWhenActive: false }
    );
    if (requiredFieldsMessage) {
      return {
        ok: false,
        scheduleId,
        scheduleName: schedule.name,
        sourceType: schedule.sourceType,
        message: requiredFieldsMessage
      };
    }

    const targetConfigMessage = await validatePricebookEntryDryRunConfiguration(
      schedule,
      () => this.createClient(instanceId)
    );
    if (targetConfigMessage) {
      return {
        ok: false,
        scheduleId,
        scheduleName: schedule.name,
        sourceType: schedule.sourceType,
        message: targetConfigMessage
      };
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
    const resolvedInstance = this.resolveInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, `listRuns:${limit}`, async () => {
      const client = await this.createClient(resolvedInstance.id);
      const runs = await client.queryRuns(limit);
      return runs.map((run) => ({
        id: run.Id,
        scheduleId: run.MSD_Schedule__c,
        scheduleName: run.MSD_Schedule__r?.Name,
        connectorId: run.MSD_Schedule__r?.MSD_Connector__c,
        connectorName: run.MSD_Schedule__r?.MSD_Connector__r?.Name,
        status: run.MSD_Status__c || "Unknown",
        startedAt: run.MSD_StartedAt__c,
        finishedAt: run.MSD_FinishedAt__c,
        recordsRead: run.MSD_RecordsRead__c,
        recordsProcessed: run.MSD_RecordsProcessed__c,
        recordsSucceeded: run.MSD_RecordsSucceeded__c,
        recordsFailed: run.MSD_RecordsFailed__c,
        errorMessage: run.MSD_ErrorMessage__c
      }));
    });
  }

  public async summarizeRecordsByRange(range: OverviewStatsRange, instanceId?: string): Promise<RecordsChartSummary> {
    const { from, to } = this.getOverviewStatsRangeWindow(range);
    const buckets = this.createRecordsChartBuckets(range, from, to);
    const client = await this.createClient(instanceId);
    const runs = await client.queryRunsByDateRange(from.toISOString(), to.toISOString(), 5000);
    const connectorNames = new Set<string>();

    for (const run of runs) {
      if (!run.MSD_StartedAt__c) {
        continue;
      }

      const startedAt = new Date(run.MSD_StartedAt__c);
      if (Number.isNaN(startedAt.getTime())) {
        continue;
      }

      const bucket = buckets.find((entry) => {
        const start = new Date(entry.start).getTime();
        const end = new Date(entry.end).getTime();
        const value = startedAt.getTime();
        return value >= start && value < end;
      });

      if (!bucket) {
        continue;
      }

      const connectorName = String(
        run.MSD_Schedule__r?.MSD_Connector__r?.Name || run.MSD_Schedule__r?.Name || "Ohne Connector"
      ).trim() || "Ohne Connector";
      const connectorId = String(run.MSD_Schedule__r?.MSD_Connector__c || "").trim() || undefined;
      const scheduleId = String(run.MSD_Schedule__c || "").trim() || undefined;
      const scheduleName = String(run.MSD_Schedule__r?.Name || scheduleId || "Ohne Scheduler").trim() || "Ohne Scheduler";
      const succeeded = Math.max(0, Number(run.MSD_RecordsSucceeded__c || 0));
      const failed = Math.max(0, Number(run.MSD_RecordsFailed__c || 0));
      const total = Math.max(0, succeeded + failed);

      bucket.total += total;
      bucket.succeeded += succeeded;
      bucket.failed += failed;
      bucket.connectorTotals[connectorName] = Number(bucket.connectorTotals[connectorName] || 0) + total;
      bucket.connectorFailures[connectorName] = Number(bucket.connectorFailures[connectorName] || 0) + failed;
      connectorNames.add(connectorName);

      const scheduleEntries = bucket.connectorSchedules[connectorName] || [];
      const scheduleEntry = scheduleEntries.find((entry) => {
        if (scheduleId && entry.scheduleId) {
          return entry.scheduleId === scheduleId;
        }
        return entry.scheduleName === scheduleName;
      });

      if (scheduleEntry) {
        scheduleEntry.total += total;
        scheduleEntry.succeeded += succeeded;
        scheduleEntry.failed += failed;
      } else {
        scheduleEntries.push({
          scheduleId,
          scheduleName,
          connectorId,
          connectorName,
          total,
          succeeded,
          failed
        });
      }

      bucket.connectorSchedules[connectorName] = scheduleEntries.sort((left, right) => {
        if (right.total !== left.total) {
          return right.total - left.total;
        }
        return String(left.scheduleName || "").localeCompare(String(right.scheduleName || ""), undefined, { sensitivity: "base" });
      });
    }

    return {
      range,
      buckets,
      connectors: Array.from(connectorNames).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }))
    };
  }

  public async setScheduleActive(scheduleId: string, active: boolean, instanceId?: string): Promise<{ id: string; active: boolean }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    await client.updateScheduleRecord(scheduleId, {
      Active__c: active
    });

    if (active) {
      this.clearScheduleAutoDisabledFlag(scheduleId);
    }

    return {
      id: scheduleId,
      active
    };
  }

  public async listStaleRuns(limit = 50, instanceId?: string): Promise<StaleRunListItem[]> {
    const resolvedInstance = this.resolveInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, `listStaleRuns:${limit}`, async () => {
      const client = await this.createClient(resolvedInstance.id);
      const staleThresholdMinutes = this.getStaleRunThresholdMinutes();
      const staleThresholdMs = staleThresholdMinutes * 60 * 1000;
      const inactivityThresholdMinutes = this.getStaleRunInactivityThresholdMinutes();
      const inactivityThresholdMs = inactivityThresholdMinutes * 60 * 1000;
      const now = Date.now();
      const runs = await client.queryRunningRuns(limit);

    const staleCandidates = await Promise.all(runs.map(async (run) => {
        const startedAt = run.MSD_StartedAt__c;
        const startedAtMs = startedAt ? new Date(startedAt).getTime() : Number.NaN;
        const ageMinutes = Number.isNaN(startedAtMs)
          ? staleThresholdMinutes
          : Math.max(0, Math.round((now - startedAtMs) / 60000));
        const latestLogs = await client.queryLogsByRunId(run.Id, 1);
        const latestLogCreatedAt = latestLogs[0]?.CreatedDate;
        const latestLogMs = latestLogCreatedAt ? new Date(latestLogCreatedAt).getTime() : Number.NaN;
        const activityReferenceMs = !Number.isNaN(latestLogMs) ? latestLogMs : startedAtMs;
        const inactivityThresholdMinutes = getStaleRunInactivityThresholdMinutesForSchedule(
          run.MSD_Schedule__c,
          run.MSD_Schedule__r?.Name
        );
        const inactivityThresholdMs = inactivityThresholdMinutes * 60 * 1000;
        const isInactiveStale = Number.isNaN(activityReferenceMs)
          ? true
          : now - activityReferenceMs >= inactivityThresholdMs;
        const isStartedAtStale = Number.isNaN(startedAtMs)
          ? true
          : now - startedAtMs >= staleThresholdMs;

        return {
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
          errorMessage: run.MSD_ErrorMessage__c,
          ageMinutes,
          staleThresholdMinutes,
          inactivityThresholdMinutes,
          isStale: isStartedAtStale || isInactiveStale
        };
      }));

      return staleCandidates
        .filter((run) => run.isStale)
        .map((run) => ({
          id: run.id,
          scheduleId: run.scheduleId,
          scheduleName: run.scheduleName,
          status: run.status,
          startedAt: run.startedAt,
          finishedAt: run.finishedAt,
          recordsRead: run.recordsRead,
          recordsProcessed: run.recordsProcessed,
          recordsSucceeded: run.recordsSucceeded,
          recordsFailed: run.recordsFailed,
          errorMessage: run.errorMessage,
          ageMinutes: run.ageMinutes,
          staleThresholdMinutes: run.staleThresholdMinutes,
          inactivityThresholdMinutes: run.inactivityThresholdMinutes
        }));
    });
  }

  public async releaseStaleRuns(runIds: string[] | undefined, instanceId?: string): Promise<ReleaseStaleRunsResult> {
    const client = await this.createClient(instanceId);
    const staleRuns = await this.listStaleRuns(200, instanceId);
    const requestedRunIds = Array.isArray(runIds)
      ? new Set(runIds.map((runId) => String(runId || "").trim()).filter(Boolean))
      : null;
    const runsToRelease = requestedRunIds
      ? staleRuns.filter((run) => requestedRunIds.has(run.id))
      : staleRuns;

    const releasedRunIds: string[] = [];
    for (const run of runsToRelease) {
      await client.updateRun(run.id, {
        status: "Failed",
        finishedAt: new Date().toISOString(),
        errorMessage: "Manual stale-run release from admin UI"
      });
      releasedRunIds.push(run.id);
    }

    return {
      releasedCount: releasedRunIds.length,
      releasedRunIds
    };
  }

  public async cancelRun(runId: string, instanceId?: string): Promise<CancelRunResult> {
    const normalizedRunId = String(runId || "").trim();
    if (!normalizedRunId) {
      throw new Error("Run-ID fehlt");
    }

    const client = await this.createClient(instanceId);
    const run = await client.queryRunById(normalizedRunId);
    if (!run) {
      throw new Error("Run nicht gefunden");
    }

    const previousStatus = String(run.MSD_Status__c || "Unknown");
    if (previousStatus !== "Running") {
      throw new Error(`Run ist nicht aktiv und kann nicht abgebrochen werden (${previousStatus})`);
    }

    const finishedAt = new Date().toISOString();
    const errorMessage = "Manual abort from admin UI";
    await client.updateRun(normalizedRunId, {
      status: "Failed",
      finishedAt,
      recordsRead: run.MSD_RecordsRead__c,
      recordsProcessed: run.MSD_RecordsProcessed__c,
      recordsSucceeded: run.MSD_RecordsSucceeded__c,
      recordsFailed: run.MSD_RecordsFailed__c,
      errorMessage
    });
    await client.createLog({
      runId: normalizedRunId,
      level: "WARN",
      step: "RUN_ABORTED",
      message: errorMessage,
      correlationId: run.MSD_CorrelationId__c || `manual-abort-${Date.now()}`
    });

    return {
      cancelled: true,
      runId: normalizedRunId,
      scheduleId: run.MSD_Schedule__c,
      scheduleName: run.MSD_Schedule__r?.Name,
      previousStatus
    };
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

  public getRunFailedRecords(runId: string): RunFailedRecordsResult {
    const normalizedRunId = String(runId || "").trim();
    if (!normalizedRunId) {
      throw new Error("Run-ID fehlt");
    }

    const filePath = path.join(LOCAL_FAILED_RUN_RECORDS_DIR, `${normalizedRunId}.json`);
    if (!fs.existsSync(filePath)) {
      return {
        runId: normalizedRunId,
        total: 0,
        items: []
      };
    }

    try {
      const raw = fs.readFileSync(filePath, "utf8").trim();
      if (!raw) {
        return {
          runId: normalizedRunId,
          total: 0,
          items: []
        };
      }

      const parsed = JSON.parse(raw) as RunFailedRecordsResult;
      return {
        runId: normalizedRunId,
        scheduleId: String(parsed.scheduleId || "").trim() || undefined,
        scheduleName: String(parsed.scheduleName || "").trim() || undefined,
        connectorId: String(parsed.connectorId || "").trim() || undefined,
        connectorName: String(parsed.connectorName || "").trim() || undefined,
        createdAt: String(parsed.createdAt || "").trim() || undefined,
        total: Math.max(0, Number(parsed.total || 0) || 0),
        items: Array.isArray(parsed.items)
          ? parsed.items.map((item) => ({
              rowIndex: Math.max(0, Number(item.rowIndex || 0) || 0),
              externalKey: String(item.externalKey || "").trim() || undefined,
              statusCode: String(item.statusCode || "").trim() || undefined,
              message: String(item.message || "").trim() || undefined,
              retryable: item.retryable === true,
              sourceRecord: item.sourceRecord && typeof item.sourceRecord === "object" ? item.sourceRecord : undefined,
              mappedRecord: item.mappedRecord && typeof item.mappedRecord === "object" ? item.mappedRecord : undefined
            }))
          : []
      };
    } catch {
      return {
        runId: normalizedRunId,
        total: 0,
        items: []
      };
    }
  }

  public async summarizeLogsByRange(range: LogChartRange, instanceId?: string): Promise<LogChartSummary> {
    const { from, to } = this.getRangeWindow(range);
    const buckets = this.createLogBuckets(range, from, to);
    const items = await this.listLogsByRange(from.toISOString(), to.toISOString(), "error", 5000, undefined, instanceId);
    const connectorNames = new Set<string>();

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
        const connectorName = String(item.connectorName || item.scheduleName || "Ohne Connector").trim() || "Ohne Connector";
        bucket.connectorErrors[connectorName] = Number(bucket.connectorErrors[connectorName] || 0) + 1;
        connectorNames.add(connectorName);
      }
    }

    return {
      range,
      buckets,
      connectors: Array.from(connectorNames).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }))
    };
  }

  public async listLogsByRange(
    startIso: string,
    endIso: string,
    type: "all" | "error" = "all",
    limit = 300,
    connectorName?: string,
    instanceId?: string
  ): Promise<LogListItem[]> {
    const client = await this.createClient(instanceId);
    const records = await client.queryLogsByDateRange(startIso, endIso, Math.max(limit * 4, 1000));

    const mapped = records.map((log) => ({
      id: log.Id,
      runId: log.MSD_Run__c,
      scheduleName: log.MSD_Run__r?.MSD_Schedule__r?.Name,
      connectorName: log.MSD_Run__r?.MSD_Schedule__r?.MSD_Connector__r?.Name,
      level: log.MSD_Level__c,
      step: log.MSD_Step__c,
      message: log.MSD_Message__c,
      recordKey: log.MSD_RecordKey__c,
      createdAt: log.CreatedDate
    }));

    const filteredByType = type === "error"
      ? mapped.filter((item) => (item.level || "").toUpperCase() === "ERROR")
      : mapped;

    const normalizedConnectorName = String(connectorName || "").trim().toLowerCase();
    const filtered = normalizedConnectorName
      ? filteredByType.filter((item) => String(item.connectorName || item.scheduleName || "Ohne Connector").trim().toLowerCase() === normalizedConnectorName)
      : filteredByType;

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
    const rawType = String(sourceType || "").trim().toUpperCase();
    const normalizedType = rawType === "MSSQL" ? "MSSQL_SQL" : rawType;
    const normalizedLimit = Math.max(1, Math.min(limit, 100));
    const trimmedDefinition = String(sourceDefinition || "").trim();

    if (!trimmedDefinition) {
      throw new Error("Quellabfrage darf nicht leer sein");
    }

    if (normalizedType === "MSSQL_SQL") {
      if (!connectorId) {
        throw new Error("Für SQL-Vorschau muss ein MSSQL-Connector ausgewählt sein");
      }

      return this.previewSql(connectorId, parseQuerySourceDefinition(trimmedDefinition).queryText, normalizedLimit, instanceId);
    }

    if (isFileScheduleType(normalizedType)) {
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
      const soqlText = parseQuerySourceDefinition(trimmedDefinition).queryText;
      const limitedSoql = /\bLIMIT\s+\d+\b/i.test(soqlText)
        ? soqlText.replace(/;\s*$/, "")
        : `${soqlText.replace(/;\s*$/, "")}\nLIMIT ${normalizedLimit}`;
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
    const rawType = String(sourceType || "").trim().toUpperCase();
    const normalizedType = rawType === "MSSQL" ? "MSSQL_SQL" : rawType;

    if (normalizedType === "SALESFORCE_SOQL") {
      const client = await this.createClient(instanceId);
      const soqlText = parseQuerySourceDefinition(sourceDefinition).queryText;
      const resolvedObjectName = String(objectName || "").trim() || this.extractSalesforceObjectName(soqlText);
      if (!resolvedObjectName) {
        throw new Error("Salesforce-Objekt konnte aus Object oder SOQL-FROM nicht ermittelt werden");
      }

      const objectFields = await client.describeObjectFields(resolvedObjectName);
      const byName = new Map(
        objectFields.map((field) => [field.name.toLowerCase(), field])
      );

      const selectedFields = this.extractSalesforceSelectedFields(soqlText);
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

      return this.getMssqlSourceFields(connectorId, parseQuerySourceDefinition(sourceDefinition).queryText, instanceId);
    }

    if (isFileScheduleType(normalizedType)) {
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
    // Preview should stay usable even when LOOKUP transforms are configured.
    // For preview, unresolved lookups fall back to the source value.
    const engine = new MappingDefinitionEngine(async (_lookupObject, _lookupField, rawValue) => {
      const normalized = String(rawValue ?? "").trim();
      return normalized ? normalized : null;
    });
    const parsed = parser.parse(mappingDefinition);
    const rows = await Promise.all(sourceData.map(async (row) => (await engine.mapRecord(row, parsed.lines)).values));
    const fields = rows.length > 0 ? Object.keys(rows[0]) : parsed.lines.map((line) => line.targetField);
    return { fields, rows };
  }

  public async validateScheduleConfiguration(
    input: ScheduleMutationInput,
    instanceId?: string
  ): Promise<ScheduleConfigurationValidationResult> {
    const issues: ScheduleConfigurationValidationIssue[] = [];
    const add = (severity: "error" | "warning", area: ScheduleConfigurationValidationIssue["area"], message: string) => {
      issues.push({ severity, area, message });
    };
    const sourceType = normalizeScheduleType(input.sourceType);
    const targetType = normalizeScheduleType(input.targetType);
    const connectorId = String(input.connectorId || "").trim();
    const sourceDefinition = String(input.sourceDefinition || "").trim();
    const targetDefinition = String(input.targetDefinition || "").trim();

    if (!String(input.sourceSystem || "").trim()) add("error", "general", "Source System fehlt.");
    if (!String(input.targetSystem || "").trim()) add("error", "general", "Target System fehlt.");
    if (!sourceType) add("error", "source", "Source Type fehlt.");
    if (!targetType) add("error", "target", "Target Type fehlt.");
    if (!Number.isFinite(Number(input.batchSize || 0)) || Number(input.batchSize || 0) <= 0) {
      add("error", "general", "Batch Size muss groesser als 0 sein.");
    }

    let connector: ConnectorConfig | undefined;
    if (connectorId) {
      try {
        connector = await (await this.createClient(instanceId)).queryConnector(connectorId);
      } catch (error) {
        add("error", "connector", `Connector konnte nicht geladen werden: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    const connectorType = String(connector?.connectorType || "").trim().toUpperCase();
    const isFileSource = isFileScheduleType(sourceType);
    const isFileTarget = isFileScheduleType(targetType);
    const isMssqlSource = sourceType === "MSSQL" || sourceType === "MSSQL_SQL";
    const isMssqlTarget = targetType === "MSSQL" || targetType === "MSSQL_SQL";
    const isRestSource = sourceType === "REST_API" || sourceType === "API";
    const isRestTarget = targetType === "REST_API" || targetType === "API";

    if (!isFileTarget && !String(input.operation || "").trim()) add("error", "general", "Operation fehlt.");
    if (!isFileTarget && !String(input.objectName || "").trim()) add("error", "target", "Zielobjekt/Zielname fehlt.");

    if ((isFileSource || isFileTarget || isMssqlSource || isMssqlTarget || isRestSource || isRestTarget) && !connectorId) {
      add("error", "connector", "Diese Scheduler-Variante benoetigt einen Connector.");
    }
    if ((isFileSource || isFileTarget) && connector && !this.isFileConnectorType(connector.connectorType)) {
      add("error", "connector", `Datei-Scheduler benoetigt einen Datei-Connector, gewaehlt ist ${connector.connectorType}.`);
    }
    if ((isMssqlSource || isMssqlTarget) && connector && !this.isMssqlTargetSystem(connectorType)) {
      add("error", "connector", `MSSQL-Scheduler benoetigt einen MSSQL-Connector, gewaehlt ist ${connector.connectorType}.`);
    }
    if ((isRestSource || isRestTarget) && connector && connectorType !== "REST_API") {
      add("error", "connector", `API-Scheduler benoetigt einen REST_API-Connector, gewaehlt ist ${connector.connectorType}.`);
    }
    if (isRestTarget) {
      add("error", "target", "REST_API als TargetType ist im generischen Scheduler-Lauf noch nicht implementiert. Unterstuetzt sind REST_API-Quellen nach Salesforce/Global Picklist.");
    }

    if (isMssqlSource) {
      const parsed = parseQuerySourceDefinition(sourceDefinition);
      if (!parsed.queryText) add("error", "source", "MSSQL SourceDefinition braucht eine SQL-Abfrage.");
      if (parsed.queryText && !/^\s*(SELECT|WITH)\b/i.test(parsed.queryText)) {
        add("warning", "source", "MSSQL-Quelle wirkt nicht wie eine SELECT/WITH-Abfrage.");
      }
      if (parsed.delta && !parsed.delta.field) add("error", "source", "Delta-Konfiguration braucht ein Feld.");
    } else if (isFileSource) {
      if (!sourceDefinition) {
        add("error", "source", "File SourceDefinition darf nicht leer sein.");
      } else {
        try {
          const parsed = JSON.parse(sourceDefinition) as Record<string, unknown>;
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("kein Objekt");
          if (!String(parsed.fileName || parsed.filePath || "").trim()) {
            add("error", "source", "File SourceDefinition braucht fileName oder filePath.");
          }
        } catch {
          if (!/\.(csv|txt|json|xlsx|xls)$/i.test(sourceDefinition)) {
            add("warning", "source", "File SourceDefinition ist weder JSON noch ein erkennbarer Dateiname.");
          }
        }
      }
    } else if (isRestSource) {
      try {
        const parsed = JSON.parse(sourceDefinition) as Record<string, unknown>;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("kein Objekt");
        if (!String(parsed.endpoint || "").trim()) add("error", "source", "REST SourceDefinition braucht endpoint.");
        const method = String(parsed.method || "GET").trim().toUpperCase();
        if (!["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"].includes(method)) {
          add("error", "source", `REST Methode ${method} wird nicht unterstuetzt.`);
        }
      } catch {
        add("error", "source", "REST SourceDefinition muss gueltiges JSON sein.");
      }
    } else if (sourceType === "SALESFORCE_SOQL") {
      const parsed = parseQuerySourceDefinition(sourceDefinition);
      if (!parsed.queryText) add("error", "source", "Salesforce SOQL SourceDefinition braucht eine SOQL-Abfrage.");
      if (parsed.queryText && !/^\s*SELECT\b/i.test(parsed.queryText)) {
        add("warning", "source", "SOQL-Quelle wirkt nicht wie eine SELECT-Abfrage.");
      }
    }

    if (isMssqlTarget) {
      try {
        const parsed = targetDefinition ? JSON.parse(targetDefinition) as Record<string, unknown> : {};
        if (String(input.operation || "").trim().toLowerCase() === "upsert" && !String(parsed.upsertKey || "").trim()) {
          add("error", "target", "MSSQL Upsert braucht targetDefinition.upsertKey.");
        }
      } catch {
        add("error", "target", "MSSQL TargetDefinition muss gueltiges JSON sein.");
      }
    } else if (isFileTarget) {
      if (!targetDefinition) {
        add("error", "target", "File TargetDefinition darf nicht leer sein.");
      } else {
        try {
          const parsed = JSON.parse(targetDefinition) as Record<string, unknown>;
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("kein Objekt");
          if (!String(parsed.fileName || parsed.filePath || "").trim()) {
            add("error", "target", "File TargetDefinition braucht fileName oder filePath.");
          }
        } catch {
          if (!/\.(csv|txt|json|xlsx|xls)$/i.test(targetDefinition)) {
            add("warning", "target", "File TargetDefinition ist weder JSON noch ein erkennbarer Dateiname.");
          }
        }
      }
    } else if (targetType === "SALESFORCE_GLOBAL_PICKLIST") {
      try {
        const parsed = JSON.parse(targetDefinition) as Record<string, unknown>;
        const resolved = resolveSelectedSalesforceTargetDefinition(targetDefinition) as Record<string, unknown>;
        if (!Array.isArray(parsed.importProfiles) || !parsed.importProfiles.length) {
          add("error", "target", "Global-Picklist TargetDefinition braucht importProfiles.");
        }
        if (!String(resolved.globalValueSetApiName || "").trim()) add("error", "target", "Global-Picklist Ziel braucht globalValueSetApiName.");
        if (!String(resolved.externalIdField || "").trim()) add("error", "target", "Global-Picklist Ziel braucht externalIdField.");
        if (!String(resolved.labelField || "").trim()) add("error", "target", "Global-Picklist Ziel braucht labelField.");
      } catch {
        add("error", "target", "Global-Picklist TargetDefinition muss gueltiges JSON sein.");
      }
    } else if (targetType === "SALESFORCE") {
      try {
        const resolved = resolveSelectedSalesforceTargetDefinition(targetDefinition || "{}") as Record<string, unknown>;
        if (String(input.operation || "").trim().toLowerCase() === "upsert" && !String(resolved.externalIdField || "").trim()) {
          add("error", "target", "Salesforce Upsert braucht ein externalIdField.");
        }
      } catch {
        add("error", "target", "Salesforce TargetDefinition muss gueltiges JSON sein.");
      }
    }

    if (String(input.mappingDefinition || "").trim()) {
      try {
        new MappingDefinitionParser().parse(String(input.mappingDefinition || ""));
      } catch (error) {
        add("error", "mapping", `MappingDefinition ist ungueltig: ${error instanceof Error ? error.message : String(error)}`);
      }
    } else {
      add("warning", "mapping", "Keine MappingDefinition hinterlegt.");
    }

    return {
      ok: !issues.some((issue) => issue.severity === "error"),
      issues
    };
  }

  public async saveSchedule(
    input: ScheduleMutationInput,
    instanceId?: string
  ): Promise<{ id: string; action: "created" | "updated" }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const sourceType = normalizeScheduleType(input.sourceType);
    const targetType = normalizeScheduleType(input.targetType);
    const usesFileSource = isFileScheduleType(sourceType);
    const usesFileTarget = isFileScheduleType(targetType);

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

    const configurationValidation = await this.validateScheduleConfiguration(input, resolvedInstance.id);
    const firstConfigurationError = configurationValidation.issues.find((issue) => issue.severity === "error");
    if (firstConfigurationError) {
      throw new Error(firstConfigurationError.message);
    }

    const pricebookEntryMappingMessage = validatePricebookEntryMappingDefinition(input);
    if (pricebookEntryMappingMessage) {
      throw new Error(pricebookEntryMappingMessage);
    }

    const requiredFieldsMessage = await validateRequiredSalesforceFieldMappings(
      input,
      () => this.createClient(instanceId)
    );
    if (requiredFieldsMessage) {
      throw new Error(requiredFieldsMessage);
    }

    const normalizedParentScheduleId =
      input.parentScheduleId && input.parentScheduleId !== input.id
        ? input.parentScheduleId
        : undefined;

    const fields: Record<string, any> = {
      Active__c: input.active,
      SourceSystem__c: input.sourceSystem || (usesFileSource ? "File" : undefined),
      TargetSystem__c: input.targetSystem || (usesFileTarget ? "File" : undefined),
      ObjectName__c: input.objectName || (usesFileTarget ? "FileExport" : undefined),
      Operation__c: input.operation || (usesFileTarget ? "Write" : undefined),
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
    const sanitizedParameters = { ...(input.parameters || {}) };
    if (String(input.secretKey || "").trim()) {
      delete sanitizedParameters.password;
      delete sanitizedParameters.bearerToken;
      delete sanitizedParameters.apiKeyValue;
      delete sanitizedParameters.clientSecret;
    }
    if (String(input.connectorType || "").trim().toUpperCase() === "MSSQL") {
      if (sanitizedParameters.encrypt === undefined) {
        sanitizedParameters.encrypt = true;
      }
      if (sanitizedParameters.trustServerCertificate === undefined) {
        sanitizedParameters.trustServerCertificate = false;
      }
    }
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
      MSD_Parameters__c: JSON.stringify(sanitizedParameters)
    };

    if (input.id) {
      await client.updateConnectorRecord(input.id, fields);
      return { id: input.id, action: "updated" };
    }

    const id = await client.createConnectorRecord(fields);
    return { id, action: "created" };
  }

  public async deleteConnector(connectorId: string, instanceId?: string): Promise<DeleteConnectorResult> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const connector = await client.queryConnector(connectorId);
    const schedules = await this.listSchedules(resolvedInstance.id);
    const scheduleById = new Map(schedules.map((schedule) => [schedule.id, schedule]));
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

    const directlyLinkedSchedules = schedules.filter((schedule) => schedule.connectorId === connectorId);
    const directlyLinkedIds = new Set(directlyLinkedSchedules.map((schedule) => schedule.id));
    const rootScheduleIds = directlyLinkedSchedules
      .filter((schedule) => {
        const parentId = String(schedule.parentScheduleId || "").trim();
        return !parentId || !directlyLinkedIds.has(parentId);
      })
      .map((schedule) => schedule.id)
      .sort((leftId, rightId) => {
        const leftName = String(scheduleById.get(leftId)?.name || leftId);
        const rightName = String(scheduleById.get(rightId)?.name || rightId);
        return leftName.localeCompare(rightName, "de", { sensitivity: "base" });
      });

    const deletedScheduleIds: string[] = [];
    const deletedScheduleNames: string[] = [];
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
      deletedScheduleIds.push(currentId);
      deletedScheduleNames.push(scheduleById.get(currentId)?.name || currentId);
    };

    rootScheduleIds.forEach((scheduleId) => collect(scheduleId));

    for (const scheduleId of deletedScheduleIds) {
      await client.deleteScheduleRecord(scheduleId);
      this.removeLocalTimingDefinition(resolvedInstance.id, scheduleId);
      this.removeScheduleHealthState(scheduleId);
    }

    await client.deleteConnectorRecord(connectorId);

    return {
      connectorId,
      connectorName: connector.name,
      deletedScheduleIds,
      deletedScheduleNames
    };
  }

  public async listTemplates(kind?: TemplateKind): Promise<TemplateDefinition[]> {
    const normalizedKind = kind === "connector" || kind === "schedule" || kind === "bundle" ? kind : undefined;
    const customTemplates = await this.readCustomTemplates();
    return [...listBuiltInTemplates(), ...customTemplates]
      .filter((item) => {
        if (!normalizedKind) {
          return true;
        }
        if (normalizedKind === "connector") {
          return item.kind === "connector" || item.kind === "bundle";
        }
        if (normalizedKind === "schedule") {
          return item.kind === "schedule" || item.kind === "bundle";
        }
        return item.kind === normalizedKind;
      })
      .sort((left, right) => {
        if (left.scope !== right.scope) {
          return left.scope === "system" ? -1 : 1;
        }
        return String(left.name || "").localeCompare(String(right.name || ""), "de", { sensitivity: "base" });
      });
  }

  public async applyTemplate(templateId: string, instanceId?: string): Promise<ApplyTemplateResult> {
    const normalizedTemplateId = String(templateId || "").trim();
    if (!normalizedTemplateId) {
      throw new Error("Template-ID fehlt");
    }

    const template = (await this.listTemplates()).find((item) => item.id === normalizedTemplateId);
    if (!template) {
      throw new Error("Vorlage nicht gefunden");
    }

    if (template.kind === "bundle") {
      return this.applyBundleTemplate(template, instanceId);
    }

    throw new Error("Diese Vorlage kann nicht direkt ausgerollt werden");
  }

  public async saveTemplate(input: TemplateMutationInput): Promise<TemplateDefinition> {
    const kind = input.kind === "connector" || input.kind === "schedule" ? input.kind : null;
    const name = String(input.name || "").trim();
    if (!kind) {
      throw new Error("Vorlagen-Typ fehlt oder ist ungueltig");
    }
    if (!name) {
      throw new Error("Vorlagenname fehlt");
    }

    const connector = kind === "connector" ? this.sanitizeConnectorTemplateDraft(input.connector) : undefined;
    const schedule = kind === "schedule" ? this.sanitizeScheduleTemplateDraft(input.schedule) : undefined;
    if (kind === "connector" && !connector) {
      throw new Error("Connector-Vorlage enthaelt keine gueltigen Daten");
    }
    if (kind === "schedule" && !schedule) {
      throw new Error("Scheduler-Vorlage enthaelt keine gueltigen Daten");
    }

    const templates = await this.readCustomTemplates();
    const existingIndex = templates.findIndex((item) => item.id === input.id);
    const createdAt = existingIndex >= 0 ? templates[existingIndex].createdAt : new Date().toISOString();
    const saved: TemplateDefinition = {
      id: existingIndex >= 0 ? templates[existingIndex].id : this.createTemplateId(kind, name),
      kind,
      name,
      description: String(input.description || "").trim() || undefined,
      scope: "custom",
      tags: Array.isArray(input.tags)
        ? input.tags.map((tag) => String(tag || "").trim()).filter(Boolean)
        : [],
      connector,
      schedule,
      createdAt,
      updatedAt: new Date().toISOString()
    };

    if (existingIndex >= 0) {
      templates[existingIndex] = saved;
    } else {
      templates.push(saved);
    }

    await this.writeCustomTemplates(templates);
    return saved;
  }

  private createTemplateId(kind: TemplateKind, name: string): string {
    const slug = String(name || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || kind;
    return `custom-${kind}-${slug}-${Date.now()}`;
  }

  private getTemplateLibraryPath(): string {
    return path.join(process.cwd(), "artifacts", "templates", "template-library.json");
  }

  private async readCustomTemplates(): Promise<TemplateDefinition[]> {
    try {
      const raw = await fs.promises.readFile(this.getTemplateLibraryPath(), "utf-8");
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        return [];
      }

      return parsed
        .filter((item) => item && typeof item === "object" && (item.kind === "connector" || item.kind === "schedule"))
        .map((item) => ({
          id: String(item.id || "").trim(),
          kind: item.kind,
          name: String(item.name || "").trim(),
          description: String(item.description || "").trim() || undefined,
          scope: "custom" as const,
          tags: Array.isArray(item.tags) ? item.tags.map((tag: unknown) => String(tag || "").trim()).filter(Boolean) : [],
          connector: this.sanitizeConnectorTemplateDraft(item.connector),
          schedule: this.sanitizeScheduleTemplateDraft(item.schedule),
          createdAt: String(item.createdAt || "").trim() || undefined,
          updatedAt: String(item.updatedAt || "").trim() || undefined
        }))
        .filter((item) => item.id && item.name);
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError?.code === "ENOENT") {
        return [];
      }
      throw error;
    }
  }

  private async writeCustomTemplates(templates: TemplateDefinition[]): Promise<void> {
    const filePath = this.getTemplateLibraryPath();
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    await fs.promises.writeFile(filePath, JSON.stringify(templates, null, 2), "utf-8");
  }

  private sanitizeConnectorTemplateDraft(input?: ConnectorTemplateDraft): ConnectorTemplateDraft | undefined {
    if (!input || typeof input !== "object") {
      return undefined;
    }

    const parameters = { ...(input.parameters || {}) };
    delete parameters.password;
    delete parameters.bearerToken;
    delete parameters.apiKeyValue;
    delete parameters.clientSecret;

    const draft: ConnectorTemplateDraft = {
      name: String(input.name || "").trim() || undefined,
      active: input.active === undefined ? true : !!input.active,
      connectorType: String(input.connectorType || "").trim() || undefined,
      targetSystem: String(input.targetSystem || "").trim() || undefined,
      direction: String(input.direction || "").trim() || undefined,
      secretKey: String(input.secretKey || "").trim() || undefined,
      timeoutMs: Number.isFinite(Number(input.timeoutMs)) ? Number(input.timeoutMs) : undefined,
      maxRetries: Number.isFinite(Number(input.maxRetries)) ? Number(input.maxRetries) : undefined,
      description: String(input.description || "").trim() || undefined,
      parameters: Object.keys(parameters).length ? parameters : undefined
    };

    return draft.connectorType ? draft : undefined;
  }

  private sanitizeScheduleTemplateDraft(input?: ScheduleTemplateDraft): ScheduleTemplateDraft | undefined {
    if (!input || typeof input !== "object") {
      return undefined;
    }

    const draft: ScheduleTemplateDraft = {
      name: String(input.name || "").trim() || undefined,
      active: input.active === undefined ? true : !!input.active,
      sourceSystem: String(input.sourceSystem || "").trim() || undefined,
      targetSystem: String(input.targetSystem || "").trim() || undefined,
      objectName: String(input.objectName || "").trim() || undefined,
      operation: String(input.operation || "").trim() || undefined,
      mappingDefinition: String(input.mappingDefinition || "").trim() || undefined,
      direction: String(input.direction || "").trim() || undefined,
      sourceType: String(input.sourceType || "").trim() || undefined,
      targetType: String(input.targetType || "").trim() || undefined,
      sourceDefinition: String(input.sourceDefinition || "").trim() || undefined,
      targetDefinition: String(input.targetDefinition || "").trim() || undefined,
      batchSize: Number.isFinite(Number(input.batchSize)) ? Number(input.batchSize) : undefined,
      timingDefinition: String(input.timingDefinition || "").trim() || undefined,
      inheritTimingFromParent: !!input.inheritTimingFromParent
    };

    return draft.objectName || draft.sourceType || draft.targetType ? draft : undefined;
  }

  private async applyBundleTemplate(template: TemplateDefinition, instanceId?: string): Promise<ApplyTemplateResult> {
    const bundle = this.sanitizeBundleTemplateDraft(template.bundle);
    if (!bundle) {
      throw new Error("Bundle-Vorlage ist unvollstaendig");
    }
    if (!bundle.connector.connectorType) {
      throw new Error("Bundle-Vorlage enthaelt keinen Connector-Typ");
    }
    if (!bundle.schedule.sourceSystem || !bundle.schedule.targetSystem || !bundle.schedule.objectName || !bundle.schedule.operation) {
      throw new Error("Bundle-Vorlage enthaelt keinen vollstaendigen Scheduler-Entwurf");
    }

    const resolvedInstance = this.resolveInstance(instanceId);
    const connectorName = await this.createUniqueConnectorName(bundle.connector.name || `${template.name} Connector`, resolvedInstance.id);
    const connectorResult = await this.saveConnector(
      {
        name: connectorName,
        active: bundle.connector.active !== false,
        connectorType: bundle.connector.connectorType,
        targetSystem: bundle.connector.targetSystem,
        direction: bundle.connector.direction,
        secretKey: bundle.connector.secretKey,
        timeoutMs: bundle.connector.timeoutMs,
        maxRetries: bundle.connector.maxRetries,
        description: bundle.connector.description,
        parameters: bundle.connector.parameters
      },
      resolvedInstance.id
    );

    const scheduleResult = await this.saveSchedule(
      {
        name: bundle.schedule.name || template.name,
        active: bundle.schedule.active !== false,
        sourceSystem: bundle.schedule.sourceSystem,
        targetSystem: bundle.schedule.targetSystem,
        objectName: bundle.schedule.objectName,
        operation: bundle.schedule.operation,
        connectorId: connectorResult.id,
        mappingDefinition: bundle.schedule.mappingDefinition,
        direction: bundle.schedule.direction,
        sourceType: bundle.schedule.sourceType,
        targetType: bundle.schedule.targetType,
        sourceDefinition: bundle.schedule.sourceDefinition,
        targetDefinition: bundle.schedule.targetDefinition,
        batchSize: bundle.schedule.batchSize,
        timingDefinition: bundle.schedule.timingDefinition,
        parentScheduleId: bundle.schedule.parentScheduleId,
        inheritTimingFromParent: bundle.schedule.inheritTimingFromParent
      },
      resolvedInstance.id
    );

    return {
      templateId: template.id,
      templateKind: template.kind,
      connector: {
        id: connectorResult.id,
        name: connectorName,
        action: connectorResult.action
      },
      schedule: {
        id: scheduleResult.id,
        name: bundle.schedule.name || template.name,
        action: scheduleResult.action
      }
    };
  }

  private sanitizeBundleTemplateDraft(input?: TemplateBundleDraft): TemplateBundleDraft | undefined {
    if (!input || typeof input !== "object") {
      return undefined;
    }

    const connector = this.sanitizeConnectorTemplateDraft(input.connector);
    const schedule = this.sanitizeScheduleTemplateDraft(input.schedule);
    if (!connector || !schedule) {
      return undefined;
    }

    return { connector, schedule };
  }

  private async createUniqueConnectorName(baseName: string, instanceId?: string): Promise<string> {
    const normalizedBaseName = String(baseName || "").trim() || "Template Connector";
    const existingNames = new Set((await this.listConnectors(instanceId)).map((item) => String(item.name || "").trim().toLowerCase()));
    if (!existingNames.has(normalizedBaseName.toLowerCase())) {
      return normalizedBaseName;
    }

    let counter = 2;
    while (existingNames.has(`${normalizedBaseName} ${counter}`.toLowerCase())) {
      counter += 1;
    }
    return `${normalizedBaseName} ${counter}`;
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
    const sourceType: FileScheduleType =
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

  public async analyzeMigrationImportFile(
    fileName: string,
    contentBase64: string,
    instanceId?: string
  ): Promise<MigrationImportAnalysisResult> {
    if (!fileName) {
      throw new Error("fileName ist erforderlich");
    }

    if (!contentBase64) {
      throw new Error("contentBase64 ist erforderlich");
    }

    const fileBuffer = Buffer.from(contentBase64, "base64");
    const parsed = this.analyzeFileBuffer(fileName, fileBuffer);
    const objects = await this.listSalesforceObjects(instanceId);
    const fileTokens = this.tokenizeSuggestionValue(path.basename(fileName).replace(/\.[^.]+$/, ""));
    const preferredObjectNames = new Set([
      "Account",
      "Contact",
      "Lead",
      "Opportunity",
      "Case",
      "Product2",
      "Order",
      "OrderItem",
      "PricebookEntry"
    ]);

    const candidateObjects = objects.filter((entry) => {
      const objectTokens = [
        ...this.tokenizeSuggestionValue(entry.name),
        ...this.tokenizeSuggestionValue(entry.label)
      ];

      return preferredObjectNames.has(entry.name) || fileTokens.some((token) => objectTokens.includes(token));
    });

    const effectiveCandidates = candidateObjects.length > 0
      ? candidateObjects
      : objects.filter((entry) => preferredObjectNames.has(entry.name));

    const describedFieldsByObject = new Map<string, Array<{ name: string; label: string }>>();
    await Promise.all(effectiveCandidates.slice(0, 12).map(async (entry) => {
      try {
        const fields = await this.describeSalesforceObjectFields(entry.name, instanceId);
        describedFieldsByObject.set(entry.name, fields.map((field) => ({ name: field.name, label: field.label })));
      } catch {
        describedFieldsByObject.set(entry.name, []);
      }
    }));

    const sheetAnalyses = parsed.format === "excel" && Array.isArray(parsed.availableSheetNames) && parsed.availableSheetNames.length
      ? parsed.availableSheetNames.map((sheetName) => {
          const fields = this.parseMigrationSourceBuffer(fileName, fileBuffer, { sheetName }).fields;
          const recordCount = this.parseMigrationSourceBuffer(fileName, fileBuffer, { sheetName }).recordCount;
          return {
            sheetName,
            headers: fields,
            recordCount,
            suggestions: this.buildMigrationImportSuggestions(
              `${fileName} ${sheetName}`,
              fields,
              effectiveCandidates.slice(0, 12),
              describedFieldsByObject
            )
          } satisfies MigrationImportSheetAnalysis;
        })
      : [];
    const primarySheetAnalysis = sheetAnalyses[0];
    const suggestions = primarySheetAnalysis
      ? primarySheetAnalysis.suggestions
      : this.buildMigrationImportSuggestions(fileName, parsed.fields, effectiveCandidates.slice(0, 12), describedFieldsByObject);

    return {
      fileName,
      format: parsed.format,
      charset: parsed.charset,
      delimiter: parsed.delimiter,
      headers: primarySheetAnalysis ? primarySheetAnalysis.headers : parsed.fields,
      recordCount: primarySheetAnalysis ? primarySheetAnalysis.recordCount : parsed.recordCount,
      suggestions,
      sheetName: parsed.sheetName,
      sheets: sheetAnalyses.length ? sheetAnalyses : undefined
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
    const resolved = this.resolveRuntimeInstance(instanceId);
    const client = new SalesforceClient(resolved.config);
    await client.login();
    return client;
  }

  private getAdaptiveSalesforceCacheTtlMs(instanceId: string): number {
    const usageRatio = this.salesforceApiUsageByInstance.get(instanceId);
    if (usageRatio === undefined || Number.isNaN(usageRatio)) {
      return 10_000;
    }

    if (usageRatio >= 0.95) {
      return 3 * 60_000;
    }
    if (usageRatio >= 0.90) {
      return 2 * 60_000;
    }
    if (usageRatio >= 0.80) {
      return 60_000;
    }
    if (usageRatio >= 0.65) {
      return 30_000;
    }

    return 10_000;
  }

  private updateApiUsageRatio(instanceId: string, overview?: SalesforceOrgOverview): void {
    const used = Number(overview?.apiUsage?.used || 0);
    const max = Number(overview?.apiUsage?.max || 0);
    if (!Number.isFinite(used) || !Number.isFinite(max) || max <= 0) {
      return;
    }

    const ratio = Math.max(0, Math.min(1, used / max));
    this.salesforceApiUsageByInstance.set(instanceId, ratio);
  }

  private async withAdaptiveSalesforceCache<T>(instanceId: string, bucket: string, loader: () => Promise<T>): Promise<T> {
    const key = `${instanceId}:${bucket}`;
    const now = Date.now();
    const cached = this.adaptiveSalesforceCache.get(key);
    if (cached && cached.expiresAt > now) {
      return cached.value as T;
    }

    const value = await loader();
    const ttlMs = this.getAdaptiveSalesforceCacheTtlMs(instanceId);
    this.adaptiveSalesforceCache.set(key, {
      value,
      expiresAt: now + ttlMs
    });
    return value;
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

  private getOverviewStatsRangeWindow(range: OverviewStatsRange): { from: Date; to: Date } {
    const to = new Date();
    const from = new Date(to);

    if (range === "day") {
      from.setHours(0, 0, 0, 0);
      return { from, to };
    }

    if (range === "year") {
      from.setMonth(0, 1);
      from.setHours(0, 0, 0, 0);
      return { from, to };
    }

    from.setDate(1);
    from.setHours(0, 0, 0, 0);
    return { from, to };
  }

  private createLogBuckets(range: LogChartRange, from: Date, to: Date): LogChartBucket[] {
    const buckets: LogChartBucket[] = [];

    if (range === "last_hour") {
      const aligned = new Date(from);
      aligned.setSeconds(0, 0);
      const minute = aligned.getMinutes();
      aligned.setMinutes(minute - (minute % 5));

      const cursor = new Date(aligned);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start.getTime() + 5 * 60_000);
        buckets.push({
          label: start.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          errors: 0,
          connectorErrors: {}
        });
        cursor.setTime(end.getTime());
      }
    }

    if (range === "last_24h") {
      const aligned = new Date(from);
      aligned.setMinutes(0, 0, 0);

      const cursor = new Date(aligned);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start.getTime() + 60 * 60_000);
        buckets.push({
          label: start.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          errors: 0,
          connectorErrors: {}
        });
        cursor.setTime(end.getTime());
      }
    } else if (range !== "last_hour") {
      const aligned = new Date(from);
      aligned.setHours(0, 0, 0, 0);

      const cursor = new Date(aligned);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start);
        end.setDate(start.getDate() + 1);
        buckets.push({
          label: start.toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          errors: 0,
          connectorErrors: {}
        });
        cursor.setTime(end.getTime());
      }
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

  private createRecordsChartBuckets(range: OverviewStatsRange, from: Date, to: Date): RecordsChartBucket[] {
    const buckets: RecordsChartBucket[] = [];

    if (range === "day") {
      const cursor = new Date(from);
      cursor.setMinutes(0, 0, 0);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start.getTime() + 60 * 60_000);
        buckets.push({
          label: start.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          succeeded: 0,
          failed: 0,
          connectorTotals: {},
          connectorFailures: {},
          connectorSchedules: {}
        });
        cursor.setTime(end.getTime());
      }
    } else if (range === "month") {
      const cursor = new Date(from);
      cursor.setHours(0, 0, 0, 0);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start);
        end.setDate(start.getDate() + 1);
        buckets.push({
          label: start.toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          succeeded: 0,
          failed: 0,
          connectorTotals: {},
          connectorFailures: {},
          connectorSchedules: {}
        });
        cursor.setTime(end.getTime());
      }
    } else {
      const cursor = new Date(from);
      cursor.setDate(1);
      cursor.setHours(0, 0, 0, 0);
      while (cursor < to) {
        const start = new Date(cursor);
        const end = new Date(start);
        end.setMonth(start.getMonth() + 1, 1);
        buckets.push({
          label: start.toLocaleDateString("de-DE", { month: "short" }),
          start: start.toISOString(),
          end: end.toISOString(),
          total: 0,
          succeeded: 0,
          failed: 0,
          connectorTotals: {},
          connectorFailures: {},
          connectorSchedules: {}
        });
        cursor.setTime(end.getTime());
      }
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

    const profileSchedulerDue = this.isSelectedImportProfileSchedulerDue(schedule.targetDefinition, schedule.lastRunAt);
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

  private isSelectedImportProfileSchedulerDue(targetDefinition?: string, lastRunAt?: string): boolean | undefined {
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
          nextRunAt?: unknown;
          scheduler?: {
            mode?: unknown;
            rules?: Array<{
              days?: unknown;
              startTime?: unknown;
              endTime?: unknown;
              intervalMinutes?: unknown;
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
        const nextRunAt = String(selectedProfile.nextRunAt || "").trim();
        if (!nextRunAt) {
          return true;
        }

        const nextRunTimestamp = new Date(nextRunAt).getTime();
        return Number.isNaN(nextRunTimestamp) ? true : nextRunTimestamp <= Date.now();
      }

      const normalizedRules = rules.map((rule) => ({
        days: Array.isArray(rule?.days)
          ? rule.days
              .map((day) => String(day || "").trim().toLowerCase())
              .filter((day): day is SchedulerDay => ["mon", "tue", "wed", "thu", "fri", "sat", "sun"].includes(day))
          : [],
        startTime: String(rule?.startTime || "").trim(),
        endTime: String(rule?.endTime || "").trim(),
        intervalMinutes: Number(rule?.intervalMinutes)
      }));

      return isImportProfileSchedulerRuleDue(normalizedRules, new Date(), lastRunAt);
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

  private resolveMssqlTargetTable(
    connector: Pick<ConnectorConfig, "connectorType" | "parameters">,
    requestedTargetObject?: string
  ): { schemaName: string; tableName: string } | null {
    const configuredSchema = String(connector.parameters?.schema || "dbo").trim() || "dbo";
    const configuredTable = String(connector.parameters?.table || "").trim();
    const requested = String(requestedTargetObject || "").trim();

    if (requested) {
      const [schemaPart, tablePart] = requested.includes(".")
        ? requested.split(".", 2)
        : [configuredSchema, requested];
      const schemaName = String(schemaPart || configuredSchema).trim() || configuredSchema;
      const tableName = String(tablePart || "").trim();
      if (tableName) {
        return { schemaName, tableName };
      }
    }

    if (configuredTable) {
      return {
        schemaName: configuredSchema,
        tableName: configuredTable
      };
    }

    return null;
  }

  private async getMssqlTargetFields(
    connectorId: string,
    targetObject?: string,
    instanceId?: string
  ): Promise<Array<{ name: string; type: string; label?: string }>> {
    const client = await this.createClient(instanceId);
    const connector = await client.queryConnector(connectorId);
    if (connector.connectorType.toLowerCase() !== 'mssql') {
      throw new Error(`MSSQL-Zielfelder werden nur für MSSQL-Connectoren unterstützt, erhalten: ${connector.connectorType}`);
    }

    const requestedTarget = this.resolveMssqlTargetTable(connector, targetObject);
    if (!requestedTarget) {
      return [];
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

    const loadColumns = async (schemaName: string, tableName: string) => {
      const result = await database.execute<{
        COLUMN_NAME: string;
        DATA_TYPE: string;
        CHARACTER_MAXIMUM_LENGTH: number | null;
      }>(
        `
          SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = @schemaName
            AND TABLE_NAME = @tableName
          ORDER BY ORDINAL_POSITION
        `,
        {
          schemaName,
          tableName
        }
      );

      return result.recordset.map((row) => ({
        name: String(row.COLUMN_NAME || '').trim(),
        type: row.CHARACTER_MAXIMUM_LENGTH && row.CHARACTER_MAXIMUM_LENGTH > 0
          ? `${String(row.DATA_TYPE || 'unknown').trim()}(${row.CHARACTER_MAXIMUM_LENGTH})`
          : String(row.DATA_TYPE || 'unknown').trim(),
        label: `${schemaName}.${tableName}.${String(row.COLUMN_NAME || '').trim()}`
      })).filter((field) => field.name);
    };

    try {
      let fields = await loadColumns(requestedTarget.schemaName, requestedTarget.tableName);
      const configuredTarget = this.resolveMssqlTargetTable(connector, undefined);
      const requestedDiffersFromConfigured = configuredTarget
        && (configuredTarget.schemaName !== requestedTarget.schemaName || configuredTarget.tableName !== requestedTarget.tableName);

      if (!fields.length && configuredTarget && requestedDiffersFromConfigured) {
        fields = await loadColumns(configuredTarget.schemaName, configuredTarget.tableName);
      }

      return fields;
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
  ): Promise<{ fields: Array<{ name: string; type: string; label?: string; requiredOnCreate?: boolean; nillable?: boolean; isExternalId?: boolean; createable?: boolean; updateable?: boolean; calculated?: boolean; autoNumber?: boolean; defaultedOnCreate?: boolean }> }> {
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
          label: field.label,
          requiredOnCreate: field.requiredOnCreate === true,
          nillable: field.nillable === true,
          isExternalId: field.isExternalId === true,
          createable: field.createable === true,
          updateable: field.updateable === true,
          calculated: field.calculated === true,
          autoNumber: field.autoNumber === true,
          defaultedOnCreate: field.defaultedOnCreate === true
        }))
      };
    }

    if (this.isMssqlTargetSystem(normalizedTargetSystem) && connectorId) {
      try {
        const fields = await this.getMssqlTargetFields(connectorId, targetObject, instanceId);
        return { fields };
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
    const resolvedInstance = this.resolveRuntimeInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, "salesforceOverview", async () => {
      const client = await this.createClient(resolvedInstance.id);
      const overview = await client.getOrgOverview();
      this.updateApiUsageRatio(resolvedInstance.id, overview);
      return overview;
    });
  }

  public async listSalesforceObjects(instanceId?: string): Promise<{ name: string; label: string }[]> {
    const resolvedInstance = this.resolveRuntimeInstance(instanceId);
    return this.withAdaptiveSalesforceCache(resolvedInstance.id, "listSalesforceObjects", async () => {
      const client = await this.createClient(resolvedInstance.id);
      return await client.listObjectMetadata();
    });
  }

  public async listSalesforcePricebooks(instanceId?: string): Promise<Array<{ id: string; name: string; isActive: boolean; isStandard: boolean }>> {
    const client = await this.createClient(instanceId);
    return await client.listPricebooks();
  }

  public async listSalesforceUsers(instanceId?: string): Promise<Array<{ id: string; name: string; username: string; isActive: boolean }>> {
    const client = await this.createClient(instanceId);
    return await client.listUsers();
  }

  public async describeSalesforceObjectFields(objectApiName: string, instanceId?: string): Promise<{ name: string; label: string; type: string; nillable: boolean; isExternalId: boolean; createable: boolean; updateable: boolean; defaultedOnCreate: boolean; calculated: boolean; autoNumber: boolean; requiredOnCreate: boolean }[]> {
    const resolvedInstance = this.resolveRuntimeInstance(instanceId);
    const normalizedObjectApiName = String(objectApiName || "").trim();
    return this.withAdaptiveSalesforceCache(
      resolvedInstance.id,
      `describeSalesforceObjectFields:${normalizedObjectApiName.toLowerCase()}`,
      async () => {
        const client = await this.createClient(resolvedInstance.id);
        return await client.describeObjectFields(normalizedObjectApiName);
      }
    );
  }

  public async createSalesforceCustomField(
    objectApiName: string,
    fieldApiName: string,
    fieldType: string,
    options?: { picklistValues?: string[]; externalId?: boolean; unique?: boolean },
    instanceId?: string
  ): Promise<unknown> {
    const client = await this.createClient(instanceId);
    const ensuredApiName = fieldApiName.endsWith("__c") ? fieldApiName : fieldApiName + "__c";
    const sfType = this.mapFieldTypeToSalesforceType(fieldType, options?.picklistValues);
    const metadata: Record<string, unknown> = {
      label: fieldApiName.replace(/__c$/, "").replace(/_/g, " "),
      type: sfType.type,
      ...sfType.extra
    };
    if (options?.externalId) {
      metadata.externalId = true;
      metadata.unique = options.unique !== false;
      if (metadata.type === "Text") {
        metadata.length = 100;
      }
    }
    try {
      const result = await client.createOrUpdateMetadata("CustomField", objectApiName + "." + ensuredApiName, metadata);
      const fieldAccess = await client.ensurePermissionSetFieldAccess("MSD_Integration_Agent", objectApiName, ensuredApiName);
      const visible = await client.waitForObjectFieldVisibility(objectApiName, ensuredApiName);
      if (!visible) {
        throw new Error(`Field ${objectApiName}.${ensuredApiName} is still not visible after granting permission set access`);
      }
      return {
        result,
        fieldAccess,
        visible
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes("DUPLICATE_DEVELOPER_NAME") && !message.includes("not yet visible on the object describe result")) {
        throw error;
      }

      const fieldAccess = await client.ensurePermissionSetFieldAccess("MSD_Integration_Agent", objectApiName, ensuredApiName);
      const visible = await client.waitForObjectFieldVisibility(objectApiName, ensuredApiName);
      if (!visible) {
        throw error;
      }

      return {
        success: true,
        action: "granted-access",
        type: "CustomField",
        fullName: objectApiName + "." + ensuredApiName,
        fieldAccess,
        visible
      };
    }
  }

  public async getMigrationSourceDistinctValues(
    migrationId: string,
    objectId: string,
    columnName: string
  ): Promise<string[]> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} wurde nicht gefunden`);
    }

    const obj = (migration.objects || []).find((entry) => entry.id === objectId);
    if (!obj) {
      throw new Error(`Objekt ${objectId} wurde in Migration ${migrationId} nicht gefunden`);
    }

    const column = String(columnName || "").trim();
    if (!column) {
      throw new Error("Spaltenname darf nicht leer sein");
    }

    const rows = await this.loadMigrationSourceRows(migrationId, obj);
    const distinctValues = new Set<string>();
    for (const entry of rows) {
      const rawValue = entry?.row?.[column];
      const normalizedValue = String(rawValue ?? "").trim();
      if (normalizedValue) {
        distinctValues.add(normalizedValue);
      }
    }

    return Array.from(distinctValues).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }));
  }

  public analyzeFileBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string; sheetName?: string }
  ): {
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
    sheetName?: string;
    availableSheetNames?: string[];
  } {
    const parsed = this.parseMigrationSourceBuffer(fileName, fileBuffer, options);
    return {
      format: parsed.format,
      charset: parsed.charset,
      delimiter: parsed.delimiter,
      textQualifier: parsed.textQualifier,
      fields: parsed.fields,
      rows: parsed.previewRows,
      recordCount: parsed.recordCount,
      sheetName: parsed.sheetName,
      availableSheetNames: parsed.availableSheetNames
    };
  }

  public async stageMigrationSourceFile(
    migrationId: string,
    objectId: string,
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string; sheetName?: string }
  ): Promise<{
    filePath: string;
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
    sheetName?: string;
    availableSheetNames?: string[];
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
        fileSheetName: parsed.sheetName,
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
    obj.fileSheetName = parsed.sheetName;
    obj.availableSheetNames = parsed.availableSheetNames;
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
      sheetName: parsed.sheetName,
      availableSheetNames: parsed.availableSheetNames,
      stagingMode: "sqlite",
      stagingDatabasePath,
      stagingImportedAt: importedAt,
      stagingStatus: "ready"
    };
  }

  public async analyzeMigrationObjectSource(
    migrationId: string,
    objectId: string,
    options?: { offset?: number; limit?: number; filter?: string; status?: string }
  ): Promise<{
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    rows: Record<string, unknown>[];
    recordCount: number;
    sheetName?: string;
    availableSheetNames?: string[];
    processingMode?: "file" | "sqlite";
    stagingMode?: "sqlite" | "file";
    stagingDatabasePath?: string;
    stagingImportedAt?: string;
    stagingStatus?: string;
    previewOffset?: number;
    previewLimit?: number;
    filteredRecordCount?: number;
    previewFilter?: string;
    previewStatusFilter?: string;
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
        let desiredCharset = String(obj.fileCharset || meta.fileCharset || "utf8");
        let desiredDelimiter = String(obj.fileDelimiter || meta.fileDelimiter || ";");
        const desiredTextQualifier = String(obj.fileTextQualifier ?? meta.fileTextQualifier ?? '"');
        const desiredSheetName = String(obj.fileSheetName || meta.fileSheetName || "").trim() || undefined;

        if (obj.filePath && !String(obj.fileCharset || "").trim()) {
          const absolutePath = path.isAbsolute(obj.filePath)
            ? obj.filePath
            : path.resolve(process.cwd(), obj.filePath);
          const fileBuffer = await fs.promises.readFile(absolutePath);
          const fileName = path.basename(absolutePath);
          const detectedAnalysis = analyzeUploadedFile(fileName, fileBuffer);
          desiredCharset = String(detectedAnalysis.charset || desiredCharset);
          if (!String(obj.fileDelimiter || "").trim()) {
            desiredDelimiter = String(detectedAnalysis.delimiter || desiredDelimiter);
          }
          if (!desiredSheetName) {
            obj.availableSheetNames = detectedAnalysis.sheets?.map((sheet) => sheet.sheetName) || obj.availableSheetNames;
          }
        }

        const stagingNeedsRefresh = desiredCharset !== meta.fileCharset
          || desiredDelimiter !== meta.fileDelimiter
          || desiredTextQualifier !== meta.fileTextQualifier
          || desiredSheetName !== (meta.fileSheetName || undefined);

        if (stagingNeedsRefresh && obj.filePath) {
          const absolutePath = path.isAbsolute(obj.filePath)
            ? obj.filePath
            : path.resolve(process.cwd(), obj.filePath);
          const fileBuffer = await fs.promises.readFile(absolutePath);
          const fileName = path.basename(absolutePath);
          await this.stageMigrationSourceFile(migrationId, objectId, fileName, fileBuffer, {
            charset: desiredCharset,
            delimiter: desiredDelimiter,
            textQualifier: desiredTextQualifier,
            sheetName: desiredSheetName
          });
          return this.analyzeMigrationObjectSource(migrationId, objectId, options);
        }

        const previewLimit = typeof options?.limit === "number" && options.limit > 0 ? Math.floor(options.limit) : 10;
        const previewOffset = typeof options?.offset === "number" && options.offset > 0 ? Math.floor(options.offset) : 0;
        const previewFilter = String(options?.filter || "").trim();
        const previewStatusFilter = String(options?.status || "").trim();
        const [stagedRows, filteredRecordCount, statusSummary] = await Promise.all([
          this.migrationStaging.listObjectRows(migrationId, objectId, {
            limit: previewLimit,
            offset: previewOffset,
            searchTerm: previewFilter,
            status: previewStatusFilter
          }),
          this.migrationStaging.countObjectRows(migrationId, objectId, {
            searchTerm: previewFilter,
            status: previewStatusFilter
          }),
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
          sheetName: meta.fileSheetName,
          availableSheetNames: obj.availableSheetNames,
          processingMode: effectiveProcessingMode,
          stagingMode: "sqlite",
          stagingDatabasePath: obj.stagingDatabasePath,
          stagingImportedAt: meta.uploadedAt,
          stagingStatus: obj.stagingStatus || "ready",
          previewOffset,
          previewLimit,
          filteredRecordCount,
          previewFilter,
          previewStatusFilter,
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
      sheetName: obj.fileSheetName,
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
      filteredRecordCount: analysis.recordCount,
      previewFilter: String(options?.filter || "").trim(),
      previewStatusFilter: String(options?.status || "").trim(),
      statusSummary: undefined
    };
  }

  private parseMigrationSourceBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string; sheetName?: string }
  ): {
    format: "csv" | "excel" | "json";
    charset: string;
    delimiter: string;
    textQualifier: string;
    fields: string[];
    previewRows: Record<string, unknown>[];
    allRows: Record<string, unknown>[];
    recordCount: number;
    sheetName?: string;
    availableSheetNames?: string[];
  } {
    const analysis = analyzeUploadedFile(fileName, fileBuffer);
    const format = analysis.format;
    const charset = String(options?.charset || analysis.charset || "utf8").trim() || "utf8";
    const delimiter = String(options?.delimiter || analysis.delimiter || ';');
    const textQualifier = String(options?.textQualifier || '"') || '"';
    const availableSheetNames = Array.isArray(analysis.sheets) ? analysis.sheets.map((sheet) => sheet.sheetName) : undefined;
    const selectedSheetName = format === "excel"
      ? String(options?.sheetName || analysis.primarySheetName || availableSheetNames?.[0] || "").trim()
      : undefined;
    const selectedSheet = selectedSheetName
      ? analysis.sheets?.find((sheet) => sheet.sheetName === selectedSheetName)
      : undefined;
    const fields = selectedSheet?.headers || analysis.headers || [];
    let allRows: Record<string, unknown>[] = [];
    let recordCount = 0;
    if (analysis.format === 'excel') {
      const XLSX = require('xlsx') as any;
      const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
      if (!selectedSheetName || !workbook.Sheets[selectedSheetName]) {
        throw new Error(`Excel-Datei enthaelt keine lesbare Mappe: ${selectedSheetName || '(leer)'}`);
      }

      const worksheetRows = XLSX.utils.sheet_to_json(workbook.Sheets[selectedSheetName], {
        defval: '',
        raw: false
      }) as Record<string, unknown>[];
      allRows = worksheetRows.map((row) => ({ ...(row || {}) }));
      recordCount = allRows.length;
    } else if (analysis.format === 'json') {
      let parsed: unknown;
      try {
        parsed = JSON.parse(decodeTextBuffer(fileBuffer, charset));
      } catch {
        throw new Error('JSON-Datei ist ungueltig');
      }

      const normalizedRows = Array.isArray(parsed)
        ? parsed
        : (parsed && typeof parsed === 'object' ? [parsed] : []);

      if (!normalizedRows.length && parsed !== null && parsed !== undefined) {
        throw new Error('JSON-Datei enthaelt keine gueltigen Datensaetze');
      }

      allRows = normalizedRows.map((entry) => {
        if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
          return { ...(entry as Record<string, unknown>) };
        }

        return { value: entry };
      });
      recordCount = allRows.length;
    } else {
      const parsedRows = parseDelimitedRows(
        decodeTextBuffer(fileBuffer, charset),
        delimiter,
        textQualifier
      );
      const headers = (parsedRows[0] || []).map((header, index) => {
        const normalized = String(header || '').trim();
        return normalized || `column_${index + 1}`;
      });

      for (let index = 1; index < parsedRows.length; index += 1) {
        const values = parsedRows[index] || [];
        const record: Record<string, unknown> = {};
        headers.forEach((header, valueIndex) => {
          record[header] = values[valueIndex] ?? '';
        });
        allRows.push(record);
      }

      recordCount = allRows.length;
    }

    return {
      format,
      charset,
      delimiter,
      textQualifier,
      fields,
      previewRows: allRows.slice(0, 10),
      allRows,
      recordCount,
      sheetName: selectedSheetName,
      availableSheetNames
    };
  }

  private mapFieldTypeToSalesforceType(fieldType: string, picklistValues?: string[]): { type: string; extra?: Record<string, unknown> } {
    const normalizedPicklistValues = Array.from(new Set((Array.isArray(picklistValues) ? picklistValues : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)));
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
      Url: { type: "Url" },
      Picklist: {
        type: "Picklist",
        extra: {
          valueSet: {
            restricted: true,
            valueSetDefinition: {
              sorted: false,
              value: normalizedPicklistValues.map((value, index) => ({
                fullName: value,
                default: index === 0,
                isActive: true,
                label: value
              }))
            }
          }
        }
      }
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

  private normalizeMigrationSalesforceRecord(
    obj: MigrationObjectConfig,
    record: Record<string, unknown>
  ): Record<string, unknown> {
    const normalized: Record<string, unknown> = { ...record };

    const emailFields = new Set((obj.fieldMappings || [])
      .filter((mapping) => String(mapping.targetFieldType || "").trim().toLowerCase() === "email")
      .map((mapping) => String(mapping.targetField || "").trim())
      .filter(Boolean));

    for (const fieldName of emailFields) {
      normalized[fieldName] = this.normalizeMigrationEmailValue(normalized[fieldName]);
    }

    if (obj.salesforceObject === "Account") {
      if (normalized.Name === undefined || normalized.Name === null || normalized.Name === "") {
        const fallbackName = [normalized.ERP_Account_Number__c, normalized.ERP_Address_Number__c]
          .map((value) => (typeof value === "string" ? value.trim() : ""))
          .find((value) => value.length > 0);
        if (fallbackName) {
          normalized.Name = fallbackName;
        }
      }

      if (typeof normalized.BillingPostalCode === "string") {
        const collapsedPostalCode = normalized.BillingPostalCode.replace(/\s+/g, " ").trim();
        if (!collapsedPostalCode) {
          normalized.BillingPostalCode = null;
        } else if (collapsedPostalCode.length > 20) {
          const firstToken = collapsedPostalCode.split(" ")[0] || collapsedPostalCode;
          normalized.BillingPostalCode = firstToken.slice(0, 20);
        } else {
          normalized.BillingPostalCode = collapsedPostalCode;
        }
      }
    }

    return normalized;
  }

  private normalizeMigrationEmailValue(value: unknown): string | null | unknown {
    if (value === undefined || value === null) {
      return value;
    }

    if (typeof value !== "string") {
      return value;
    }

    const trimmedValue = value.trim();
    if (!trimmedValue) {
      return null;
    }

    const loweredValue = trimmedValue.toLowerCase();
    if (
      loweredValue === "nicht vorhanden" ||
      loweredValue === "nicht vorh" ||
      loweredValue === "keine" ||
      loweredValue === "n/a"
    ) {
      return null;
    }

    if (trimmedValue.includes(";") || trimmedValue.includes(",") || /^www\./i.test(trimmedValue)) {
      return null;
    }

    const normalizedWhitespace = trimmedValue.replace(/\s*@\s*/g, "@").replace(/\s*\.\s*/g, ".");
    if (/\s/.test(normalizedWhitespace)) {
      return null;
    }

    return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(normalizedWhitespace)
      ? normalizedWhitespace
      : null;
  }

  private isUnsupportedSalesforceExternalIdError(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error || "");
    return message.includes("does not match an External ID, Salesforce Id, or indexed field");
  }

  private getSalesforceWriteOptions(): { allOrNone: boolean; headers: Record<string, string> } {
    return {
      allOrNone: false,
      headers: {
        "Sforce-Duplicate-Rule-Header": "allowSave=true"
      }
    };
  }

  private getMigrationExternalIdValue(record: Record<string, unknown>, externalIdField: string): unknown {
    const directValue = record[externalIdField];
    if (directValue !== undefined) {
      return directValue;
    }

    const matchedEntry = Object.entries(record).find(([key]) => key.toLowerCase() === externalIdField.toLowerCase());
    return matchedEntry ? matchedEntry[1] : undefined;
  }

  private async executeMigrationBatch(
    client: SalesforceClient,
    obj: MigrationObjectConfig,
    batch: Record<string, unknown>[]
  ): Promise<Array<{ success?: boolean; errors?: Array<{ message?: string }>; error?: { message?: string } }>> {
    if (obj.operation === "upsert" && !obj.externalIdField) {
      throw new Error(
        `Objekt ${obj.salesforceObject} ist auf Upsert gestellt, aber es ist kein External-ID-Feld konfiguriert. Der Lauf wurde zum Schutz vor unbeabsichtigten Inserts abgebrochen.`
      );
    }

    if (obj.operation === "upsert" && obj.externalIdField) {
      const normalizedBatch = batch.map((record) => {
        const directValue = record[obj.externalIdField!];
        if (directValue !== undefined) {
          return record;
        }

        const matchedEntry = Object.entries(record).find(([key]) => key.toLowerCase() === obj.externalIdField!.toLowerCase());
        if (!matchedEntry) {
          return record;
        }

        return {
          ...record,
          [obj.externalIdField!]: matchedEntry[1]
        };
      });

      try {
        const results = await (client as any).connection.sobject(obj.salesforceObject).upsert(
          normalizedBatch,
          obj.externalIdField,
          this.getSalesforceWriteOptions()
        );
        return Array.isArray(results) ? results : [results];
      } catch (error) {
        if (!this.isUnsupportedSalesforceExternalIdError(error)) {
          throw error;
        }

        const fallbackResults: Array<{ success?: boolean; errors?: Array<{ message?: string }>; error?: { message?: string } }> = [];
        for (const record of normalizedBatch) {
          const lookupValue = record[obj.externalIdField];
          if (lookupValue === undefined || lookupValue === null || lookupValue === "") {
            fallbackResults.push({
              success: false,
              errors: [{ message: `Missing lookup value for field ${obj.externalIdField}` }]
            });
            continue;
          }

          try {
            const lookupSoql = `SELECT Id FROM ${obj.salesforceObject} WHERE ${obj.externalIdField} = ${this.toSoqlLiteral(lookupValue)} LIMIT 1`;
            const existing = await client.queryGeneric(lookupSoql);
            const existingId = typeof existing[0]?.Id === "string" ? existing[0].Id : undefined;

            if (existingId) {
              const updatePayload = { ...record, Id: existingId };
              const updateResult = await (client as any).connection.sobject(obj.salesforceObject).update(updatePayload, this.getSalesforceWriteOptions());
              fallbackResults.push(updateResult);
            } else {
              const insertResult = await (client as any).connection.sobject(obj.salesforceObject).insert(record, this.getSalesforceWriteOptions());
              fallbackResults.push(insertResult);
            }
          } catch (recordError) {
            fallbackResults.push({
              success: false,
              errors: [{ message: recordError instanceof Error ? recordError.message : String(recordError || "Unknown Salesforce error") }]
            });
          }
        }

        return fallbackResults;
      }
    }

    if (obj.operation === "update") {
      const results = await (client as any).connection.sobject(obj.salesforceObject).update(batch, this.getSalesforceWriteOptions());
      return Array.isArray(results) ? results : [results];
    }

    const results = await (client as any).connection.sobject(obj.salesforceObject).insert(batch, this.getSalesforceWriteOptions());
    return Array.isArray(results) ? results : [results];
  }

  private resolveMigrationTargetFieldApiName(
    fieldName: string,
    availableFields: Array<{ name?: string; label?: string; type?: string }>
  ): { name: string; label?: string; type?: string; exists: boolean } {
    const rawName = String(fieldName || "").trim();
    if (!rawName) {
      return { name: "", exists: false };
    }

    const fieldMap = new Map(
      (Array.isArray(availableFields) ? availableFields : [])
        .map((field) => ({
          name: String(field?.name || "").trim(),
          label: field?.label,
          type: field?.type
        }))
        .filter((field) => field.name)
        .map((field) => [field.name.toLowerCase(), field] as const)
    );

    const exactMatch = fieldMap.get(rawName.toLowerCase());
    if (exactMatch) {
      return { ...exactMatch, exists: true };
    }

    if (!rawName.toLowerCase().endsWith("__c")) {
      const customFieldMatch = fieldMap.get((rawName + "__c").toLowerCase());
      if (customFieldMatch) {
        return { ...customFieldMatch, exists: true };
      }
    }

    return { name: rawName, exists: false };
  }

  private inferMigrationTargetFieldType(mapping: MigrationFieldMapping): string {
    const explicitPicklistValues = Array.isArray((mapping as MigrationFieldMapping & { picklistValues?: string[] }).picklistValues)
      ? ((mapping as MigrationFieldMapping & { picklistValues?: string[] }).picklistValues || []).map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    if (explicitPicklistValues.length) {
      return "Picklist";
    }

    const normalizedType = String(mapping.targetFieldType || "").trim().toLowerCase();
    if (normalizedType === "picklist") return "Picklist";
    if (normalizedType === "url") return "Url";
    if (normalizedType === "date") return "Date";
    if (normalizedType === "datetime") return "DateTime";
    if (normalizedType === "boolean") return "Checkbox";
    if (normalizedType === "email") return "Email";
    if (normalizedType === "phone") return "Phone";
    if (normalizedType === "currency") return "Currency";
    if (normalizedType === "percent") return "Percent";
    if (["double", "int", "integer", "number"].includes(normalizedType)) return "Number";

    const targetFieldName = String(mapping.targetField || "").trim().toLowerCase();
    if (targetFieldName.includes("currency")) return "Currency";
    if (targetFieldName.includes("percent")) return "Percent";
    if (targetFieldName.includes("email")) return "Email";
    if (targetFieldName.includes("phone") || targetFieldName.includes("mobile")) return "Phone";
    if (targetFieldName.includes("date")) return "Date";
    if (targetFieldName.includes("url") || targetFieldName.includes("website")) return "Url";

    return "Text";
  }

  private async autoCreateMissingMigrationTargetFields(
    client: SalesforceClient,
    obj: MigrationObjectConfig,
    objectFields: Array<{ name?: string; label?: string; type?: string }>
  ): Promise<Array<{ name: string }>> {
    const createdFields: Array<{ name: string }> = [];

    for (const mapping of obj.fieldMappings || []) {
      const resolvedField = this.resolveMigrationTargetFieldApiName(mapping.targetField, objectFields);
      if (resolvedField.exists) {
        continue;
      }

      const rawFieldName = String(resolvedField.name || "").trim();
      if (!rawFieldName) {
        continue;
      }

      const fieldType = this.inferMigrationTargetFieldType(mapping);
      const picklistValues = fieldType === "Picklist"
        ? Array.from(new Set((((mapping as MigrationFieldMapping & { picklistValues?: string[] }).picklistValues) || [])
            .map((value) => String(value || "").trim())
            .filter(Boolean)))
        : [];

      if (fieldType === "Picklist" && !picklistValues.length) {
        throw new Error(`Feld ${rawFieldName} kann nicht automatisch als Picklist angelegt werden, weil keine Werte hinterlegt sind.`);
      }

      const ensuredApiName = rawFieldName.endsWith("__c") ? rawFieldName : rawFieldName + "__c";
      const sfType = this.mapFieldTypeToSalesforceType(fieldType, picklistValues);
      const metadata: Record<string, unknown> = {
        label: ensuredApiName.replace(/__c$/, "").replace(/_/g, " "),
        type: sfType.type,
        ...sfType.extra
      };

      await client.createOrUpdateMetadata("CustomField", obj.salesforceObject + "." + ensuredApiName, metadata);
      createdFields.push({ name: ensuredApiName });
      objectFields.push({ name: ensuredApiName, label: String(metadata.label || ensuredApiName), type: String(sfType.type || "") });
    }

    return createdFields;
  }

  private async ensureMigrationTargetFieldsExist(
    client: SalesforceClient,
    obj: MigrationObjectConfig
  ): Promise<void> {
    const objectFields = await client.describeObjectFields(obj.salesforceObject);
    await this.autoCreateMissingMigrationTargetFields(client, obj, objectFields);
    const missingFields: string[] = [];

    for (const mapping of obj.fieldMappings || []) {
      const resolvedField = this.resolveMigrationTargetFieldApiName(mapping.targetField, objectFields);
      if (!resolvedField.exists) {
        if (resolvedField.name) {
          missingFields.push(resolvedField.name);
        }
        continue;
      }

      mapping.targetField = resolvedField.name;
      if (resolvedField.label) {
        mapping.targetFieldLabel = resolvedField.label;
      }
      if (resolvedField.type) {
        mapping.targetFieldType = resolvedField.type;
      }
    }

    if (!missingFields.length) {
      return;
    }

    const preview = Array.from(new Set(missingFields)).slice(0, 5).join(", ");
    throw new Error(
      `Folgende Zielfelder existieren in Salesforce nicht auf ${obj.salesforceObject}: ${preview}. Bitte in Schritt 6 anlegen oder das Mapping korrigieren.`
    );
  }

  private async syncMigrationTargetPicklistValues(
    client: SalesforceClient,
    obj: MigrationObjectConfig,
    recordStates: Array<{
      rowIndex: number;
      sourceRecord: Record<string, unknown>;
      sfRecord?: Record<string, unknown>;
      mappingError?: string;
    }>
  ): Promise<void> {
    const targetFields = await client.describeObjectFields(obj.salesforceObject);

    for (const mapping of obj.fieldMappings || []) {
      const fieldType = this.inferMigrationTargetFieldType(mapping);
      if (fieldType !== "Picklist") {
        continue;
      }

      const resolvedField = this.resolveMigrationTargetFieldApiName(mapping.targetField, targetFields);
      const targetFieldName = String(resolvedField.name || "").trim();
      if (!resolvedField.exists || !targetFieldName.endsWith("__c")) {
        continue;
      }

      const mappedValues = Array.from(new Set(recordStates
        .map((state) => state.sfRecord?.[targetFieldName])
        .map((value) => String(value ?? "").trim())
        .filter(Boolean)));

      if (mappedValues.length === 0) {
        continue;
      }

      await client.syncCustomFieldPicklistValues({
        objectApiName: obj.salesforceObject,
        fieldApiName: targetFieldName,
        values: mappedValues
      });
    }
  }

  private ensureUniqueMigrationExternalIds(
    obj: MigrationObjectConfig,
    recordStates: Array<{
      rowIndex: number;
      sourceRecord: Record<string, unknown>;
      sfRecord?: Record<string, unknown>;
      mappingError?: string;
    }>
  ): void {
    if (obj.operation !== "upsert" || !obj.externalIdField) {
      return;
    }

    const duplicates = new Map<string, number[]>();
    const firstRowByValue = new Map<string, number>();

    for (const state of recordStates) {
      if (!state.sfRecord || state.mappingError) {
        continue;
      }

      const rawValue = state.sfRecord[obj.externalIdField];
      const normalizedValue = rawValue === undefined || rawValue === null ? "" : String(rawValue).trim();
      if (!normalizedValue) {
        continue;
      }

      const firstRow = firstRowByValue.get(normalizedValue);
      if (firstRow === undefined) {
        firstRowByValue.set(normalizedValue, state.rowIndex);
        continue;
      }

      const rows = duplicates.get(normalizedValue) || [firstRow];
      rows.push(state.rowIndex);
      duplicates.set(normalizedValue, rows);
    }

    if (!duplicates.size) {
      return;
    }

    const preview = [...duplicates.entries()]
      .sort((a, b) => a[0].localeCompare(b[0], "de"))
      .slice(0, 5)
      .map(([value, rows]) => `${value} (Zeilen ${rows.join(", ")})`)
      .join("; ");

    for (const [value, rows] of duplicates.entries()) {
      const errorMessage = `Das Upsert-Feld ${obj.externalIdField} ist in der Quelldatei nicht eindeutig (${value}; Zeilen ${rows.join(", ")}).`;
      const duplicateRows = new Set(rows);
      for (const state of recordStates) {
        if (duplicateRows.has(state.rowIndex)) {
          state.mappingError = errorMessage;
        }
      }
    }
  }

  private async collectAmbiguousSalesforceExternalIds(
    client: SalesforceClient,
    obj: MigrationObjectConfig,
    recordStates: Array<{
      rowIndex: number;
      sourceRecord: Record<string, unknown>;
      sfRecord?: Record<string, unknown>;
      mappingError?: string;
    }>
  ): Promise<Map<string, string[]>> {
    if (obj.operation !== "upsert" || !obj.externalIdField) {
      return new Map<string, string[]>();
    }

    const externalIdValues = Array.from(new Set(recordStates
      .filter((state) => state.sfRecord && !state.mappingError)
      .map((state) => this.getMigrationExternalIdValue(state.sfRecord!, obj.externalIdField!))
      .map((value) => String(value ?? "").trim())
      .filter(Boolean)));

    if (!externalIdValues.length) {
      return new Map<string, string[]>();
    }

    const ambiguousValues = new Map<string, string[]>();
    for (let index = 0; index < externalIdValues.length; index += 200) {
      const chunk = externalIdValues.slice(index, index + 200);
      const soql = `SELECT Id, ${obj.externalIdField} FROM ${obj.salesforceObject} WHERE ${obj.externalIdField} IN (${chunk
        .map((value) => this.toSoqlLiteral(value))
        .join(", ")})`;
      const existingRecords = await client.queryGeneric(soql);
      const idsByValue = new Map<string, string[]>();

      for (const existingRecord of existingRecords) {
        const value = String(existingRecord[obj.externalIdField] ?? "").trim();
        const id = String(existingRecord.Id ?? "").trim();
        if (!value || !id) {
          continue;
        }

        const ids = idsByValue.get(value) || [];
        ids.push(id);
        idsByValue.set(value, ids);
      }

      for (const [value, ids] of idsByValue.entries()) {
        if (ids.length > 1) {
          ambiguousValues.set(value, ids);
        }
      }
    }

    return ambiguousValues;
  }

  private async markAmbiguousSalesforceExternalIds(
    client: SalesforceClient,
    obj: MigrationObjectConfig,
    recordStates: Array<{
      rowIndex: number;
      sourceRecord: Record<string, unknown>;
      sfRecord?: Record<string, unknown>;
      mappingError?: string;
    }>
  ): Promise<void> {
    if (obj.operation !== "upsert" || !obj.externalIdField) {
      return;
    }

    const ambiguousValues = await this.collectAmbiguousSalesforceExternalIds(client, obj, recordStates);

    if (!ambiguousValues.size) {
      return;
    }

    for (const state of recordStates) {
      if (!state.sfRecord || state.mappingError) {
        continue;
      }

      const externalIdValue = String(this.getMigrationExternalIdValue(state.sfRecord, obj.externalIdField) ?? "").trim();
      if (!externalIdValue) {
        continue;
      }

      const existingIds = ambiguousValues.get(externalIdValue);
      if (existingIds && existingIds.length > 1) {
        state.mappingError = `${obj.externalIdField.replace(/__c$/, "")}: more than one record found for external id field: [${existingIds.join(", ")}]`;
      }
    }
  }

  public async getMigrationPreflightWarnings(
    migrationId: string,
    instanceId?: string
  ): Promise<{
    items: Array<{
      objectId: string;
      salesforceObject: string;
      externalIdField: string;
      affectedRecordCount: number;
      conflictCount: number;
      conflicts: Array<{ value: string; rowIndexes: number[]; existingIds: string[] }>;
    }>;
  }> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} not found`);
    }

    const orderedObjects = [...migration.executionPlan]
      .sort((a, b) => a.order - b.order)
      .map((step) => migration.objects.find((o) => o.id === step.objectId))
      .filter((o): o is MigrationObjectConfig => !!o);

    for (const obj of migration.objects) {
      if (!orderedObjects.find((entry) => entry.id === obj.id)) {
        orderedObjects.push(obj);
      }
    }
    const client = await this.createClient(instanceId ?? migration.instanceId);
    const items: Array<{
      objectId: string;
      salesforceObject: string;
      externalIdField: string;
      affectedRecordCount: number;
      conflictCount: number;
      conflicts: Array<{ value: string; rowIndexes: number[]; existingIds: string[] }>;
    }> = [];

    for (const obj of orderedObjects) {
      if (obj.operation !== "upsert" || !obj.externalIdField) {
        continue;
      }

      const sourceRows = await this.loadMigrationSourceRows(migration.id, obj);
      const mappingLines = this.buildMigrationMappingLines(obj);
      const lookupResolver = await this.createMigrationLookupResolver(client, mappingLines, sourceRows.map((entry) => entry.row));
      const engine = new MappingDefinitionEngine(lookupResolver);

      const recordStates: Array<{
        rowIndex: number;
        sourceRecord: Record<string, unknown>;
        sfRecord?: Record<string, unknown>;
        mappingError?: string;
      }> = [];

      for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex += 1) {
        const sourceRow = sourceRows[rowIndex];
        const row = sourceRow.row;
        try {
          if (mappingLines.length > 0) {
            const mapped = await engine.mapRecord(row, mappingLines);
            const record: Record<string, unknown> = {};
            for (const [key, value] of Object.entries(mapped.values)) {
              record[key] = value !== undefined && value !== "" ? value : null;
            }
            recordStates.push({
              rowIndex: sourceRow.rowIndex,
              sourceRecord: row,
              sfRecord: this.normalizeMigrationSalesforceRecord(obj, record)
            });
          } else {
            const record: Record<string, unknown> = {};
            for (const mapping of obj.fieldMappings) {
              const rawValue = row[mapping.sourceColumn];
              record[mapping.targetField] = rawValue !== undefined && rawValue !== "" ? rawValue : null;
            }
            recordStates.push({
              rowIndex: sourceRow.rowIndex,
              sourceRecord: row,
              sfRecord: this.normalizeMigrationSalesforceRecord(obj, record)
            });
          }
        } catch (error) {
          recordStates.push({
            rowIndex: sourceRow.rowIndex,
            sourceRecord: row,
            mappingError: error instanceof Error ? error.message : String(error)
          });
        }
      }

      const ambiguousValues = await this.collectAmbiguousSalesforceExternalIds(client, obj, recordStates);
      if (!ambiguousValues.size) {
        continue;
      }

      const conflicts = [...ambiguousValues.entries()].map(([value, existingIds]) => ({
        value,
        existingIds,
        rowIndexes: recordStates
          .filter((state) => state.sfRecord && !state.mappingError)
          .filter((state) => String(this.getMigrationExternalIdValue(state.sfRecord!, obj.externalIdField!) ?? "").trim() === value)
          .map((state) => state.rowIndex)
      }));

      items.push({
        objectId: obj.id,
        salesforceObject: obj.salesforceObject,
        externalIdField: obj.externalIdField,
        affectedRecordCount: conflicts.reduce((sum, conflict) => sum + conflict.rowIndexes.length, 0),
        conflictCount: conflicts.length,
        conflicts
      });
    }

    return { items };
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

  public listMigrationsForUi(): MigrationConfig[] {
    return this.readMigrationsStore().map((migration) => this.sanitizeMigrationForUi(migration));
  }

  public getMigration(id: string): MigrationConfig | undefined {
    return this.readMigrationsStore().find((m) => m.id === id);
  }

  public getMigrationForUi(id: string): MigrationConfig | undefined {
    const migration = this.getMigration(id);
    return migration ? this.sanitizeMigrationForUi(migration) : undefined;
  }

  private sanitizeMigrationForUi(migration: MigrationConfig): MigrationConfig {
    return {
      ...migration,
      salesforceLogin: migration.salesforceLogin
        ? {
            ...migration.salesforceLogin,
            accessToken: undefined,
            refreshToken: undefined
          }
        : undefined
    };
  }

  public saveMigration(input: Omit<MigrationConfig, "createdAt" | "updatedAt"> & { createdAt?: string; updatedAt?: string }): MigrationConfig {
    const migrations = this.readMigrationsStore();
    const now = new Date().toISOString();
    const existing = migrations.find((m) => m.id === input.id);
    const normalizedLogin = input.salesforceLogin && String(input.salesforceLogin.loginUrl || "").trim()
      ? {
          ...input.salesforceLogin,
          id: input.id,
          name: String(input.salesforceLogin.name || input.name || input.id).trim() || input.id,
          loginUrl: normalizeMigrationSalesforceLoginUrl(
            String(input.salesforceLogin.loginUrl || this.getMigrationInstanceLoginUrl(input.salesforceLogin.environment)).trim(),
            input.salesforceLogin.environment
          ),
          authType: input.salesforceLogin.authType === "password"
            ? "password"
            : (input.salesforceLogin.authType === "client_credentials"
              ? "client_credentials"
              : (input.salesforceLogin.authType || existing?.salesforceLogin?.authType || "oauth_refresh_token")),
          username: input.salesforceLogin.authType === "password"
            ? String(input.salesforceLogin.username || existing?.salesforceLogin?.username || "").trim() || undefined
            : undefined,
          password: input.salesforceLogin.authType === "password"
            ? String(input.salesforceLogin.password || existing?.salesforceLogin?.password || "") || undefined
            : undefined,
          securityToken: input.salesforceLogin.authType === "password"
            ? String(input.salesforceLogin.securityToken || existing?.salesforceLogin?.securityToken || "").trim() || undefined
            : undefined,
          clientId: input.salesforceLogin.authType === "client_credentials"
            ? String(input.salesforceLogin.clientId || existing?.salesforceLogin?.clientId || "").trim() || undefined
            : undefined,
          clientSecret: input.salesforceLogin.authType === "client_credentials"
            ? String(input.salesforceLogin.clientSecret || existing?.salesforceLogin?.clientSecret || "").trim() || undefined
            : undefined,
          instanceUrl: input.salesforceLogin.instanceUrl || existing?.salesforceLogin?.instanceUrl,
          accessToken: input.salesforceLogin.authType === "oauth_refresh_token" ? (input.salesforceLogin.accessToken || existing?.salesforceLogin?.accessToken) : undefined,
          refreshToken: input.salesforceLogin.authType === "oauth_refresh_token" ? (input.salesforceLogin.refreshToken || existing?.salesforceLogin?.refreshToken) : undefined,
          tokenIssuedAt: input.salesforceLogin.authType === "oauth_refresh_token" ? (input.salesforceLogin.tokenIssuedAt || existing?.salesforceLogin?.tokenIssuedAt) : undefined,
          lastConnectionStatus: input.salesforceLogin.lastConnectionStatus || existing?.salesforceLogin?.lastConnectionStatus || "never",
          lastConnectedAt: input.salesforceLogin.lastConnectedAt || existing?.salesforceLogin?.lastConnectedAt,
          lastConnectionError: input.salesforceLogin.lastConnectionError || existing?.salesforceLogin?.lastConnectionError,
          orgOverview: input.salesforceLogin.orgOverview || existing?.salesforceLogin?.orgOverview,
          objectCount: input.salesforceLogin.objectCount ?? existing?.salesforceLogin?.objectCount,
          updatedAt: now
        }
      : undefined;
    const saved: MigrationConfig = {
      ...input,
      batchSize: this.resolveMigrationBatchSize(input.batchSize),
      instanceId: normalizedLogin ? `migration:${input.id}` : (String(input.instanceId || "").trim() || undefined),
      salesforceLogin: normalizedLogin,
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
    const batchSize = this.resolveMigrationBatchSize(migration);

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
    migration.lastRunResult = {
      startedAt,
      steps: orderedObjects.map((obj) => ({
        objectId: obj.id,
        salesforceObject: obj.salesforceObject,
        status: "pending",
        recordsProcessed: 0,
        recordsSucceeded: 0,
        recordsFailed: 0,
        failedRecordsId: undefined
      }))
    };
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
        const progressStep = migration.lastRunResult?.steps.find((step) => step.objectId === obj.id);
        if (progressStep) {
          progressStep.status = "running";
          progressStep.errorMessage = undefined;
          progressStep.failedRecordsId = undefined;
          this.saveMigration(migration);
        }

        try {
          await this.ensureMigrationTargetFieldsExist(client, obj);

          const sourceRows = await this.loadMigrationSourceRows(migration.id, obj);
          const rows = sourceRows.map((entry) => entry.row);

          stepResult.recordsProcessed = rows.length;
          if (progressStep) {
            progressStep.recordsProcessed = rows.length;
            this.saveMigration(migration);
          }

          const mappingLines = this.buildMigrationMappingLines(obj);
          const lookupResolver = await this.createMigrationLookupResolver(client, mappingLines, sourceRows.map((entry) => entry.row));
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
                const normalizedRecord = this.normalizeMigrationSalesforceRecord(obj, record);
                recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, sfRecord: normalizedRecord });
              } else {
                const record: Record<string, unknown> = {};
                for (const mapping of obj.fieldMappings) {
                  const rawValue = row[mapping.sourceColumn];
                  record[mapping.targetField] = rawValue !== undefined && rawValue !== "" ? rawValue : null;
                }
                const normalizedRecord = this.normalizeMigrationSalesforceRecord(obj, record);
                recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, sfRecord: normalizedRecord });
              }
            } catch (error) {
              const errorMsg = error instanceof Error ? error.message : String(error);
              recordStates.push({ rowIndex: sourceRow.rowIndex, sourceRecord: row, mappingError: errorMsg });
              if (mappingErrorsPreview.length < 3) {
                mappingErrorsPreview.push(errorMsg);
              }
            }
          }

          this.ensureUniqueMigrationExternalIds(obj, recordStates);
          await this.markAmbiguousSalesforceExternalIds(client, obj, recordStates);
          await this.syncMigrationTargetPicklistValues(client, obj, recordStates);

          const executableRecordStates = recordStates.filter((state) => state.sfRecord && !state.mappingError);
          const sfRecords = executableRecordStates.map((state) => state.sfRecord!);

          let succeeded = 0;
          let failed = 0;
          const sfRecordStateIndexes = recordStates
            .map((state, idx) => (state.sfRecord && !state.mappingError ? idx : -1))
            .filter((idx) => idx >= 0);

          for (let i = 0; i < sfRecords.length; i += batchSize) {
            const batch = sfRecords.slice(i, i + batchSize);
            const batchResults = await this.executeMigrationBatch(client, obj, batch);

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
            if (progressStep) {
              progressStep.recordsSucceeded = succeeded;
              progressStep.recordsFailed = recordStates.filter((state) => state.mappingError).length;
              this.saveMigration(migration);
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
            if (progressStep) {
              progressStep.failedRecordsId = failedRecordsId;
            }
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

        if (progressStep) {
          progressStep.status = stepResult.status;
          progressStep.recordsProcessed = stepResult.recordsProcessed;
          progressStep.recordsSucceeded = stepResult.recordsSucceeded;
          progressStep.recordsFailed = stepResult.recordsFailed;
          progressStep.errorMessage = stepResult.errorMessage;
          progressStep.failedRecordsId = stepResult.failedRecordsId;
          this.saveMigration(migration);
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
      const finalizedRunResult = {
        startedAt,
        finishedAt,
        reportPath,
        steps: stepResults.map((s) => ({ ...s }))
      };
      migration.lastRunResult = finalizedRunResult;
      migration.runHistory = [
        finalizedRunResult,
        ...(Array.isArray(migration.runHistory) ? migration.runHistory : [])
      ].slice(0, 10);
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

    if (obj.stagingMode === "sqlite" && editedByRow.size > 0) {
      await this.migrationStaging.updateRowPayloads(
        migrationId,
        objectId,
        [...editedByRow.entries()].map(([rowIndex, sourceRecord]) => ({
          rowIndex,
          payload: sourceRecord,
          status: "pending"
        }))
      );
      obj.stagingStatus = "ready";
      this.saveMigration(migration);
    }

    const client = await this.createClient(instanceId ?? migration.instanceId);
    await this.ensureMigrationTargetFieldsExist(client, obj);
    const mappingLines = this.buildMigrationMappingLines(obj);
    const lookupResolver = await this.createMigrationLookupResolver(
      client,
      mappingLines,
      previousFailed.map((failed) => editedByRow.get(Number(failed.rowIndex || 0)) || failed.sourceRecord || {})
    );
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
          const normalizedRecord = this.normalizeMigrationSalesforceRecord(obj, record);
          recordStates.push({ rowIndex: Math.max(1, rowIndex), sourceRecord, sfRecord: normalizedRecord });
        } else {
          const record: Record<string, unknown> = {};
          for (const mapping of obj.fieldMappings) {
            const rawValue = sourceRecord[mapping.sourceColumn];
            record[mapping.targetField] = rawValue !== undefined && rawValue !== "" ? rawValue : null;
          }
          const normalizedRecord = this.normalizeMigrationSalesforceRecord(obj, record);
          recordStates.push({ rowIndex: Math.max(1, rowIndex), sourceRecord, sfRecord: normalizedRecord });
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

    const batchSize = this.resolveMigrationBatchSize(migration);
    let succeeded = 0;
    let failed = 0;

    for (let i = 0; i < sfRecords.length; i += batchSize) {
      const batch = sfRecords.slice(i, i + batchSize);
      const batchResults = await this.executeMigrationBatch(client, obj, batch);

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

  public async saveFailedMigrationRecordCorrections(
    migrationId: string,
    objectId: string,
    editedRecords: Array<{ rowIndex: number; sourceRecord: Record<string, unknown> }> = []
  ): Promise<{ updatedRows: number; statusSummary?: Record<string, number> }> {
    const migration = this.getMigration(migrationId);
    if (!migration) {
      throw new Error(`Migration ${migrationId} not found`);
    }

    const obj = migration.objects.find((item) => item.id === objectId);
    if (!obj) {
      throw new Error(`Object ${objectId} not found in migration`);
    }

    if (obj.stagingMode !== "sqlite") {
      throw new Error(`Objekt ${obj.salesforceObject} verwendet kein SQLite-Staging`);
    }

    const normalizedUpdates = editedRecords.filter(
      (record) => record && typeof record.rowIndex === "number" && record.sourceRecord && typeof record.sourceRecord === "object"
    );

    if (!normalizedUpdates.length) {
      return { updatedRows: 0, statusSummary: (await this.migrationStaging.getObjectStatusSummary(migrationId, objectId)).byStatus };
    }

    await this.migrationStaging.updateRowPayloads(
      migrationId,
      objectId,
      normalizedUpdates.map((record) => ({
        rowIndex: record.rowIndex,
        payload: record.sourceRecord,
        status: "pending"
      }))
    );

    obj.stagingStatus = "ready";
    this.saveMigration(migration);

    return {
      updatedRows: normalizedUpdates.length,
      statusSummary: (await this.migrationStaging.getObjectStatusSummary(migrationId, objectId)).byStatus
    };
  }
}
