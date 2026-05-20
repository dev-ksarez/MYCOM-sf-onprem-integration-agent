import pino from "pino";
import fs from "node:fs";
import path from "node:path";
import { PassThrough } from "node:stream";
import archiver from "archiver";
import {
  CreateLogInput,
  ConnectorConfig,
  isOperationallyRelevantLog,
  SalesforceClient,
  SalesforceOrgOverview,
  SalesforceObjectFieldMetadata,
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
import { calculateNextRunAtFromTiming } from "../core/scheduler/schedule-timing";
import { getDefaultStaleRunInactivityThresholdMinutes, getStaleRunInactivityThresholdMinutesForSchedule } from "../core/scheduler/stale-run-policy";
import { MigrationStagingSqlite } from "../infrastructure/db/migration-staging-sqlite";
import { MssqlDatabase } from "../infrastructure/db/mssql";
import { SqliteDatabase } from "../infrastructure/db/sqlite";
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
import { analyzeUploadedFile, decodeTextBuffer, parseDelimitedRows, parseExcelBuffer, parseFileFromConnector } from "../utils/file-transfer";
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
  lookupCacheEnabled: boolean;
  lookupCacheTtlMinutes: number;
  logBatchingEnabled: boolean;
  logSyncIntervalMinutes: number;
  logBatchSize: number;
  logBufferMaxEntries: number;
  confluenceBaseUrl?: string;
  confluenceUsername?: string;
  confluenceApiToken?: string;
  confluenceSpaceKey?: string;
  confluenceParentPageId?: string;
  confluencePageTitlePrefix?: string;
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
  clientIdEnv?: string;
  clientSecretEnv?: string;
  queryLimit?: number;
}

export interface SalesforceProjectMutationInput {
  id?: string;
  name: string;
  description?: string;
  archived?: boolean;
  productionWriteProtection?: boolean;
  lookupCacheEnabled?: boolean;
  lookupCacheTtlMinutes?: number;
  logBatchingEnabled?: boolean;
  logSyncIntervalMinutes?: number;
  logBatchSize?: number;
  logBufferMaxEntries?: number;
  confluenceBaseUrl?: string;
  confluenceUsername?: string;
  confluenceApiToken?: string;
  confluenceSpaceKey?: string;
  confluenceParentPageId?: string;
  confluencePageTitlePrefix?: string;
}

export type SalesforceInstanceReadinessStatus = "ready" | "setup-required" | "setup-running" | "setup-failed";

export interface SalesforceReadinessMissingArtifact {
  type: "object" | "field" | "permission" | "capability";
  name: string;
  severity: "critical" | "warning";
  message?: string;
}

export interface SalesforceInstanceReadinessCheckInput {
  projectId?: string;
  targetEnv?: "test" | "production";
  mode?: "validate-only";
  requestedBy?: string;
}

export interface SalesforceInstanceReadinessResult {
  instanceId: string;
  projectId: string;
  status: SalesforceInstanceReadinessStatus;
  checkedAt: string;
  missingArtifacts: SalesforceReadinessMissingArtifact[];
  capabilities: {
    healthPulse: boolean;
    remoteCommands: boolean;
    logUpload: boolean;
  };
  nextAction?: "run-msd-setup" | "none";
}

export interface SalesforceInstanceMsdSetupInput {
  projectId?: string;
  targetEnv?: "test" | "production";
  mode?: "dry-run" | "apply";
  components?: string[];
  requestedBy?: string;
}

export interface SalesforceInstanceMsdSetupResult {
  instanceId: string;
  projectId: string;
  status: SalesforceInstanceReadinessStatus;
  startedAt: string;
  finishedAt: string;
  applied: string[];
  warnings: string[];
  missingArtifacts: SalesforceReadinessMissingArtifact[];
  capabilities: {
    healthPulse: boolean;
    remoteCommands: boolean;
    logUpload: boolean;
  };
  nextAction?: "run-msd-setup" | "none";
  auditId: string;
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

interface AgentObjectFieldDefinition {
  apiName: string;
  label: string;
  type: "Text" | "LongTextArea" | "DateTime" | "Number" | "Checkbox" | "Lookup" | "Picklist";
  length?: number;
  precision?: number;
  scale?: number;
  defaultValue?: boolean;
  referenceTo?: string;
  relationshipLabel?: string;
  relationshipName?: string;
  picklistValues?: Array<{ fullName: string; label: string; default?: boolean }>;
  visibleLines?: number;
  legacyApiNames?: string[];
}

interface AgentObjectDefinition {
  canonicalObjectApiName: string;
  label?: string;
  pluralLabel?: string;
  nameField?: Record<string, unknown>;
  capability?: "healthPulse" | "remoteCommands" | "logUpload";
  legacyObjectApiNames: string[];
  requiredFields: AgentObjectFieldDefinition[];
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
const PROJECTS_SQLITE_FILE = process.env.PROJECTS_SQLITE_FILE || path.resolve(process.cwd(), "data/projects.sqlite");
const METADATA_SQLITE_FILE = process.env.METADATA_SQLITE_FILE || PROJECTS_SQLITE_FILE;
const SAGE100_DB_DOC_INDEX_FILE = process.env.SAGE100_DB_DOC_INDEX_FILE || path.resolve(process.cwd(), "artifacts/sage100-db-doc-index.json");
const LOCAL_MIGRATION_INSTANCES_FILE = process.env.SF_MIGRATION_INSTANCES_FILE || path.resolve(process.cwd(), "artifacts/migration-instances.json");
const LOCAL_SCHEDULE_TIMING_FILE = process.env.SF_SCHEDULE_TIMING_FILE || path.resolve(process.cwd(), "artifacts/schedule-timing.json");
const LOCAL_SCHEDULE_HEALTH_FILE = process.env.SF_SCHEDULE_HEALTH_FILE || path.resolve(process.cwd(), "artifacts/schedule-health.json");
const LOCAL_MIGRATIONS_FILE = path.resolve(process.cwd(), "artifacts/migrations.json");
const LOCAL_INSTANCE_READINESS_FILE = process.env.SF_INSTANCE_READINESS_FILE || path.resolve(process.cwd(), "artifacts/instance-readiness.json");
const LOCAL_FAILED_RUN_RECORDS_DIR =
  process.env.FAILED_RUN_RECORDS_DIR || path.resolve(process.cwd(), "artifacts/runtime/failed-run-records");
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

interface LocalInstanceReadinessRecord {
  instanceId: string;
  projectId: string;
  status: SalesforceInstanceReadinessStatus;
  missingArtifacts: SalesforceReadinessMissingArtifact[];
  lastCheckedAt: string;
  lastSetupAt?: string;
}

export interface PersistedMetadataObject {
  systemType: "salesforce";
  objectName: string;
  label: string;
  kind?: string;
  queryable: boolean;
  fieldCount: number;
}

export interface PersistedMetadataField {
  objectName: string;
  name: string;
  label: string;
  type: string;
  required: boolean;
  externalId: boolean;
  createable: boolean;
  updateable: boolean;
  referenceTo?: string[];
  picklistValues?: Array<{ value: string; label: string }>;
}

export interface InstanceMetadataSnapshot {
  id: number;
  projectId: string;
  instanceId: string;
  systemType: "salesforce";
  status: "running" | "success" | "error";
  refreshedAt: string;
  objectCount: number;
  fieldCount: number;
  errorMessage?: string;
}

export interface InstanceMetadataContext {
  snapshot?: InstanceMetadataSnapshot;
  objects: PersistedMetadataObject[];
  fieldsByObject: Record<string, PersistedMetadataField[]>;
}

export interface Sage100DocumentationField {
  name: string;
  type: string;
  required?: boolean;
  description?: string;
}

export interface Sage100DocumentationTable {
  name: string;
  pages?: number[];
  primaryKey?: string[];
  fields: Sage100DocumentationField[];
  score?: number;
}

export interface Sage100DocumentationContext {
  sourceFile?: string;
  indexFile: string;
  generatedAt?: string;
  pageCount?: number;
  tableCount: number;
  matchedTables: Sage100DocumentationTable[];
}

async function withMetadataDatabase<T>(callback: (database: SqliteDatabase) => Promise<T>): Promise<T> {
  const database = new SqliteDatabase({ filePath: METADATA_SQLITE_FILE });
  try {
    await initializeMetadataDatabase(database);
    return await callback(database);
  } finally {
    await database.close();
  }
}

async function initializeMetadataDatabase(database: SqliteDatabase): Promise<void> {
  await database.run(`
    CREATE TABLE IF NOT EXISTS metadata_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      project_id TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      system_type TEXT NOT NULL,
      status TEXT NOT NULL,
      refreshed_at TEXT NOT NULL,
      object_count INTEGER NOT NULL DEFAULT 0,
      field_count INTEGER NOT NULL DEFAULT 0,
      error_message TEXT
    )
  `);

  await database.run(`
    CREATE TABLE IF NOT EXISTS metadata_objects (
      project_id TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      system_type TEXT NOT NULL,
      object_name TEXT NOT NULL,
      label TEXT NOT NULL,
      kind TEXT,
      queryable INTEGER NOT NULL DEFAULT 1,
      field_count INTEGER NOT NULL DEFAULT 0,
      refreshed_at TEXT NOT NULL,
      PRIMARY KEY (project_id, instance_id, system_type, object_name)
    )
  `);

  await database.run(`
    CREATE TABLE IF NOT EXISTS metadata_fields (
      project_id TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      system_type TEXT NOT NULL,
      object_name TEXT NOT NULL,
      field_name TEXT NOT NULL,
      label TEXT NOT NULL,
      type TEXT NOT NULL,
      required INTEGER NOT NULL DEFAULT 0,
      external_id INTEGER NOT NULL DEFAULT 0,
      createable INTEGER NOT NULL DEFAULT 0,
      updateable INTEGER NOT NULL DEFAULT 0,
      reference_to_json TEXT,
      picklist_values_json TEXT,
      refreshed_at TEXT NOT NULL,
      PRIMARY KEY (project_id, instance_id, system_type, object_name, field_name)
    )
  `);
}

function normalizeMetadataSnapshot(row: Record<string, unknown>): InstanceMetadataSnapshot {
  return {
    id: Number(row.id || 0),
    projectId: String(row.project_id || "").trim(),
    instanceId: String(row.instance_id || "").trim(),
    systemType: "salesforce",
    status: String(row.status || "error").trim() === "success" ? "success" : String(row.status || "").trim() === "running" ? "running" : "error",
    refreshedAt: String(row.refreshed_at || "").trim(),
    objectCount: Number(row.object_count || 0),
    fieldCount: Number(row.field_count || 0),
    errorMessage: String(row.error_message || "").trim() || undefined
  };
}

function parseJsonArrayField<T>(raw: unknown): T[] | undefined {
  const text = String(raw || "").trim();
  if (!text) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(text) as unknown;
    return Array.isArray(parsed) ? parsed as T[] : undefined;
  } catch {
    return undefined;
  }
}

function readLocalInstanceReadinessRecords(): LocalInstanceReadinessRecord[] {
  try {
    if (!fs.existsSync(LOCAL_INSTANCE_READINESS_FILE)) {
      return [];
    }

    const raw = fs.readFileSync(LOCAL_INSTANCE_READINESS_FILE, "utf8").trim();
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }

    const normalized: LocalInstanceReadinessRecord[] = [];

    for (const item of parsed) {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        continue;
      }

      const candidate = item as Record<string, unknown>;
      const instanceId = String(candidate.instanceId || "").trim();
      const projectId = String(candidate.projectId || "default-project").trim() || "default-project";
      if (!instanceId) {
        continue;
      }

      const statusCandidate = String(candidate.status || "setup-required").trim();
      const status: SalesforceInstanceReadinessStatus = (
        statusCandidate === "ready"
        || statusCandidate === "setup-running"
        || statusCandidate === "setup-failed"
        || statusCandidate === "setup-required"
      ) ? statusCandidate : "setup-required";

      const missingArtifacts: SalesforceReadinessMissingArtifact[] = [];
      if (Array.isArray(candidate.missingArtifacts)) {
        for (const entry of candidate.missingArtifacts) {
          if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
            continue;
          }

          const artifact = entry as Record<string, unknown>;
          const typeCandidate = String(artifact.type || "capability").trim();
          const type: SalesforceReadinessMissingArtifact["type"] = (
            typeCandidate === "object"
            || typeCandidate === "field"
            || typeCandidate === "permission"
            || typeCandidate === "capability"
          ) ? typeCandidate : "capability";
          const severityCandidate = String(artifact.severity || "warning").trim();
          const severity: SalesforceReadinessMissingArtifact["severity"] = severityCandidate === "critical" ? "critical" : "warning";
          const name = String(artifact.name || "").trim();
          if (!name) {
            continue;
          }

          missingArtifacts.push({
            type,
            name,
            severity,
            message: String(artifact.message || "").trim() || undefined
          });
        }
      }

      normalized.push({
        instanceId,
        projectId,
        status,
        missingArtifacts,
        lastCheckedAt: String(candidate.lastCheckedAt || "").trim() || new Date().toISOString(),
        lastSetupAt: String(candidate.lastSetupAt || "").trim() || undefined
      });
    }

    return normalized;
  } catch {
    return [];
  }
}

function writeLocalInstanceReadinessRecords(records: LocalInstanceReadinessRecord[]): void {
  const directory = path.dirname(LOCAL_INSTANCE_READINESS_FILE);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(LOCAL_INSTANCE_READINESS_FILE, JSON.stringify(records, null, 2), "utf8");
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
    lookupCacheEnabled: true,
    lookupCacheTtlMinutes: 15,
    logBatchingEnabled: true,
    logSyncIntervalMinutes: 5,
    logBatchSize: 200,
    logBufferMaxEntries: 10000,
    confluenceBaseUrl: undefined,
    confluenceUsername: undefined,
    confluenceApiToken: undefined,
    confluenceSpaceKey: undefined,
    confluenceParentPageId: undefined,
    confluencePageTitlePrefix: undefined,
    createdAt: now,
    updatedAt: now
  };
}

function normalizeDeploymentVariant(value: unknown): "customer" | "service-provider" | undefined {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "customer" || normalized === "customer-production" || normalized === "production-monitoring") {
    return "customer";
  }
  if (normalized === "service-provider" || normalized === "dienstleister" || normalized === "test-production") {
    return "service-provider";
  }
  return undefined;
}

function isOperationalProductionMutation(operation?: string): boolean {
  const normalized = String(operation || "").trim();
  return (
    normalized === "POST /api/schedules"
    || normalized === "POST /api/schedules/validate-config"
    || normalized === "POST /api/connectors"
    || normalized === "POST /api/runs/release-stale"
    || /^POST \/api\/schedules\/[^/]+\/(active|checkpoint|duplicate)$/.test(normalized)
    || /^POST \/api\/runs\/[^/]+\/cancel$/.test(normalized)
    || /^DELETE \/api\/schedules\/[^/]+$/.test(normalized)
    || /^DELETE \/api\/connectors\/[^/]+$/.test(normalized)
  );
}

function isCustomerProductionOperationsMode(projectId: string, instances: SalesforceInstanceEnvConfig[]): boolean {
  const explicitVariant = normalizeDeploymentVariant(
    process.env.AGENT_UI_DEPLOYMENT_VARIANT
    || process.env.ADMIN_UI_DEPLOYMENT_VARIANT
    || process.env.CUSTOMER_INSTALLATION_MODE
  );
  if (explicitVariant === "customer") {
    return true;
  }
  if (explicitVariant === "service-provider") {
    return false;
  }

  const normalizedProjectId = String(projectId || "default-project").trim() || "default-project";
  const projectInstances = instances.filter((item) =>
    String(item.projectId || "default-project").trim() === normalizedProjectId
  );
  const hasProduction = projectInstances.some((item) => item.role === "production");
  const hasTest = projectInstances.some((item) => item.role !== "production");
  return hasProduction && !hasTest;
}

function ensureProjectsSqliteColumns(db: any): void {
  try {
    const columns = new Set<string>((db.prepare("PRAGMA table_info(projects)").all() as Array<{ name: string }>).map((row) => row.name));
    const statements = [
      { name: "confluence_space_key", sql: "ALTER TABLE projects ADD COLUMN confluence_space_key TEXT" },
      { name: "confluence_parent_page_id", sql: "ALTER TABLE projects ADD COLUMN confluence_parent_page_id TEXT" },
      { name: "confluence_page_title_prefix", sql: "ALTER TABLE projects ADD COLUMN confluence_page_title_prefix TEXT" },
      { name: "confluence_base_url", sql: "ALTER TABLE projects ADD COLUMN confluence_base_url TEXT" },
      { name: "confluence_username", sql: "ALTER TABLE projects ADD COLUMN confluence_username TEXT" },
      { name: "confluence_api_token", sql: "ALTER TABLE projects ADD COLUMN confluence_api_token TEXT" },
      { name: "lookup_cache_enabled", sql: "ALTER TABLE projects ADD COLUMN lookup_cache_enabled INTEGER NOT NULL DEFAULT 1" },
      { name: "lookup_cache_ttl_minutes", sql: "ALTER TABLE projects ADD COLUMN lookup_cache_ttl_minutes INTEGER NOT NULL DEFAULT 15" },
      { name: "log_batching_enabled", sql: "ALTER TABLE projects ADD COLUMN log_batching_enabled INTEGER NOT NULL DEFAULT 1" },
      { name: "log_sync_interval_minutes", sql: "ALTER TABLE projects ADD COLUMN log_sync_interval_minutes INTEGER NOT NULL DEFAULT 5" },
      { name: "log_batch_size", sql: "ALTER TABLE projects ADD COLUMN log_batch_size INTEGER NOT NULL DEFAULT 200" },
      { name: "log_buffer_max_entries", sql: "ALTER TABLE projects ADD COLUMN log_buffer_max_entries INTEGER NOT NULL DEFAULT 10000" }
    ];

    for (const statement of statements) {
      if (!columns.has(statement.name)) {
        db.exec(statement.sql);
      }
    }
  } catch {
    // ignore schema migration errors; JSON fallback will still work
  }
}

function openProjectsSqliteSync(): any | null {
  try {
    // Use node builtin SQLite when available (Node 22+).
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const sqlite = require("node:sqlite");
    const DatabaseSync = sqlite?.DatabaseSync;
    if (!DatabaseSync) {
      return null;
    }

    const directory = path.dirname(PROJECTS_SQLITE_FILE);
    fs.mkdirSync(directory, { recursive: true });

    const db = new DatabaseSync(PROJECTS_SQLITE_FILE);
    db.exec(`CREATE TABLE IF NOT EXISTS projects (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      archived INTEGER NOT NULL DEFAULT 0,
      production_write_protection INTEGER NOT NULL DEFAULT 1,
      lookup_cache_enabled INTEGER NOT NULL DEFAULT 1,
      lookup_cache_ttl_minutes INTEGER NOT NULL DEFAULT 15,
      log_batching_enabled INTEGER NOT NULL DEFAULT 1,
      log_sync_interval_minutes INTEGER NOT NULL DEFAULT 5,
      log_batch_size INTEGER NOT NULL DEFAULT 200,
      log_buffer_max_entries INTEGER NOT NULL DEFAULT 10000,
      confluence_space_key TEXT,
      confluence_parent_page_id TEXT,
      confluence_page_title_prefix TEXT,
      confluence_base_url TEXT,
      confluence_username TEXT,
      confluence_api_token TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )`);
    ensureProjectsSqliteColumns(db);

    return db;
  } catch {
    return null;
  }
}

function readProjectsFromSqliteSync(): SalesforceProjectConfig[] | null {
  const db = openProjectsSqliteSync();
  if (!db) {
    return null;
  }

  try {
    const rows = db
      .prepare(`SELECT id, name, description, archived, production_write_protection, lookup_cache_enabled, lookup_cache_ttl_minutes, log_batching_enabled, log_sync_interval_minutes, log_batch_size, log_buffer_max_entries, confluence_space_key, confluence_parent_page_id, confluence_page_title_prefix, confluence_base_url, confluence_username, confluence_api_token, created_at, updated_at FROM projects`)
      .all() as Array<{
      id: string;
      name: string;
      description?: string;
      archived: number;
      production_write_protection: number;
      lookup_cache_enabled: number;
      lookup_cache_ttl_minutes: number;
      log_batching_enabled: number;
      log_sync_interval_minutes: number;
      log_batch_size: number;
      log_buffer_max_entries: number;
      confluence_space_key?: string;
      confluence_parent_page_id?: string;
      confluence_page_title_prefix?: string;
      confluence_base_url?: string;
      confluence_username?: string;
      confluence_api_token?: string;
      created_at: string;
      updated_at: string;
    }>;

    return rows
      .map((row) => ({
        id: String(row.id || "").trim(),
        name: String(row.name || "").trim(),
        description: String(row.description || "").trim() || undefined,
        archived: Number(row.archived || 0) === 1,
        productionWriteProtection: Number(row.production_write_protection || 0) !== 0,
        lookupCacheEnabled: Number(row.lookup_cache_enabled ?? 1) !== 0,
        lookupCacheTtlMinutes: Math.max(1, Number(row.lookup_cache_ttl_minutes ?? 15) || 15),
        logBatchingEnabled: Number(row.log_batching_enabled ?? 1) !== 0,
        logSyncIntervalMinutes: Math.max(1, Number(row.log_sync_interval_minutes ?? 5) || 5),
        logBatchSize: Math.max(1, Number(row.log_batch_size ?? 200) || 200),
        logBufferMaxEntries: Math.max(100, Number(row.log_buffer_max_entries ?? 10000) || 10000),
        confluenceBaseUrl: String(row.confluence_base_url || "").trim() || undefined,
        confluenceUsername: String(row.confluence_username || "").trim() || undefined,
        confluenceApiToken: String(row.confluence_api_token || "").trim() || undefined,
        confluenceSpaceKey: String(row.confluence_space_key || "").trim() || undefined,
        confluenceParentPageId: String(row.confluence_parent_page_id || "").trim() || undefined,
        confluencePageTitlePrefix: String(row.confluence_page_title_prefix || "").trim() || undefined,
        createdAt: String(row.created_at || "").trim() || new Date().toISOString(),
        updatedAt: String(row.updated_at || "").trim() || new Date().toISOString()
      }))
      .filter((item) => item.id && item.name);
  } catch {
    return null;
  } finally {
    try {
      db.close();
    } catch {
      // ignore close errors
    }
  }
}

function writeProjectsToSqliteSync(projects: SalesforceProjectConfig[]): void {
  const db = openProjectsSqliteSync();
  if (!db) {
    return;
  }

  try {
    db.exec("BEGIN");
    db.exec("DELETE FROM projects");

    const insert = db.prepare(
      `INSERT INTO projects (id, name, description, archived, production_write_protection, lookup_cache_enabled, lookup_cache_ttl_minutes, log_batching_enabled, log_sync_interval_minutes, log_batch_size, log_buffer_max_entries, confluence_space_key, confluence_parent_page_id, confluence_page_title_prefix, confluence_base_url, confluence_username, confluence_api_token, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );

    for (const project of projects) {
      insert.run(
        project.id,
        project.name,
        project.description || "",
        project.archived === true ? 1 : 0,
        project.productionWriteProtection === false ? 0 : 1,
        project.lookupCacheEnabled === false ? 0 : 1,
        Math.max(1, Number(project.lookupCacheTtlMinutes || 15) || 15),
        project.logBatchingEnabled === false ? 0 : 1,
        Math.max(1, Number(project.logSyncIntervalMinutes || 5) || 5),
        Math.max(1, Number(project.logBatchSize || 200) || 200),
        Math.max(100, Number(project.logBufferMaxEntries || 10000) || 10000),
        project.confluenceSpaceKey || "",
        project.confluenceParentPageId || "",
        project.confluencePageTitlePrefix || "",
        project.confluenceBaseUrl || "",
        project.confluenceUsername || "",
        project.confluenceApiToken || "",
        project.createdAt,
        project.updatedAt
      );
    }

    db.exec("COMMIT");
  } catch {
    try {
      db.exec("ROLLBACK");
    } catch {
      // ignore rollback errors
    }
  } finally {
    try {
      db.close();
    } catch {
      // ignore close errors
    }
  }
}

function readLocalProjects(): SalesforceProjectConfig[] {
  const sqliteItems = readProjectsFromSqliteSync();
  if (sqliteItems) {
    const items = [...sqliteItems];
    if (!items.some((entry) => entry.id === "default-project")) {
      items.unshift(defaultProjectConfig());
      writeProjectsToSqliteSync(items);
    }

    // Keep JSON mirror in sync for backward compatibility tools.
    const directory = path.dirname(LOCAL_PROJECTS_FILE);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(LOCAL_PROJECTS_FILE, JSON.stringify(items, null, 2), "utf8");
    return items;
  }

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
          lookupCacheEnabled: candidate.lookupCacheEnabled !== false,
          lookupCacheTtlMinutes: Math.max(1, Number(candidate.lookupCacheTtlMinutes || 15) || 15),
          logBatchingEnabled: candidate.logBatchingEnabled !== false,
          logSyncIntervalMinutes: Math.max(1, Number(candidate.logSyncIntervalMinutes || 5) || 5),
          logBatchSize: Math.max(1, Number(candidate.logBatchSize || 200) || 200),
          logBufferMaxEntries: Math.max(100, Number(candidate.logBufferMaxEntries || 10000) || 10000),
          confluenceBaseUrl: typeof candidate.confluenceBaseUrl === "string" ? candidate.confluenceBaseUrl.trim() || undefined : undefined,
          confluenceUsername: typeof candidate.confluenceUsername === "string" ? candidate.confluenceUsername.trim() || undefined : undefined,
          confluenceApiToken: typeof candidate.confluenceApiToken === "string" ? candidate.confluenceApiToken.trim() || undefined : undefined,
          confluenceSpaceKey: typeof candidate.confluenceSpaceKey === "string" ? candidate.confluenceSpaceKey.trim() || undefined : undefined,
          confluenceParentPageId: typeof candidate.confluenceParentPageId === "string" ? candidate.confluenceParentPageId.trim() || undefined : undefined,
          confluencePageTitlePrefix: typeof candidate.confluencePageTitlePrefix === "string" ? candidate.confluencePageTitlePrefix.trim() || undefined : undefined,
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

    writeProjectsToSqliteSync(items);

    return items;
  } catch {
    return [defaultProjectConfig()];
  }
}

function writeLocalProjects(projects: SalesforceProjectConfig[]): void {
  writeProjectsToSqliteSync(projects);
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
  loginUrl: string;
  projectId: string;
  projectName: string;
  role: "test" | "production";
  queryLimit?: number;
}

export interface SalesforceProjectOption {
  id: string;
  name: string;
  description?: string;
  archived?: boolean;
  productionWriteProtection: boolean;
  lookupCacheEnabled: boolean;
  lookupCacheTtlMinutes: number;
  logBatchingEnabled: boolean;
  logSyncIntervalMinutes: number;
  logBatchSize: number;
  logBufferMaxEntries: number;
  confluenceBaseUrl?: string;
  confluenceUsername?: string;
  confluenceApiTokenConfigured?: boolean;
  confluenceSpaceKey?: string;
  confluenceParentPageId?: string;
  confluencePageTitlePrefix?: string;
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

function isMonitorRelevantRun(run: Pick<RunListItem, "status" | "recordsRead" | "recordsProcessed" | "recordsSucceeded" | "recordsFailed" | "errorMessage">): boolean {
  const status = String(run.status || "").trim().toLowerCase();
  if (status && status !== "success") {
    return true;
  }
  if (String(run.errorMessage || "").trim()) {
    return true;
  }

  const recordsRead = Math.max(0, Number(run.recordsRead || 0));
  const recordsProcessed = Math.max(0, Number(run.recordsProcessed || 0));
  const recordsSucceeded = Math.max(0, Number(run.recordsSucceeded || 0));
  const recordsFailed = Math.max(0, Number(run.recordsFailed || 0));
  return recordsRead > 0 || recordsProcessed > 0 || recordsSucceeded > 0 || recordsFailed > 0;
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

function isMonitorRelevantLogItem(log: Pick<LogListItem, "level" | "step" | "message" | "recordKey">): boolean {
  const normalizedLevel = String(log.level || "").trim().toUpperCase();
  return isOperationallyRelevantLog({
    level: normalizedLevel === "ERROR" ? "ERROR" : normalizedLevel === "WARN" ? "WARN" : "INFO",
    step: log.step || "",
    message: log.message || "",
    recordKey: log.recordKey
  });
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
  daily: {
    date: string;
    total: number;
    succeeded: number;
    failed: number;
    previousSucceeded: number;
    growth: number;
    growthPercent: number | null;
  };
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

  const externalIdField = String(resolvedTargetDefinition?.externalIdField || "").trim().toLowerCase();
  if (
    objectName === "PricebookEntry" &&
    operation === "upsert" &&
    externalIdField === "productcode" &&
    providedTargetFields.has("productcode")
  ) {
    providedTargetFields.add("product2id");
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
  nextRunAt?: string;
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
  projectId?: string;
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
  private static readonly agentPermissionSetLegacyNames = ["MSD_Agent_Integration", "MSD_IntegrationAgent"];
  private static readonly defaultSalesforceMetadataFieldObjects = [
    "Account",
    "Contact",
    "Lead",
    "Opportunity",
    "Order",
    "Product2",
    "Pricebook2",
    "PricebookEntry"
  ];

  private static readonly agentObjectDefinitions: AgentObjectDefinition[] = [
    {
      canonicalObjectApiName: "MSD_Connector__c",
      label: "MSD Connector",
      pluralLabel: "MSD Connectors",
      nameField: {
        type: "Text",
        label: "Connector Name"
      },
      capability: "logUpload",
      legacyObjectApiNames: [],
      requiredFields: [
        { apiName: "MSD_Active__c", label: "Active", type: "Checkbox", defaultValue: true },
        { apiName: "MSD_ConnectorType__c", label: "Connector Type", type: "Text", length: 100 },
        { apiName: "MSD_TargetSystem__c", label: "Target System", type: "Text", length: 255 },
        { apiName: "MSD_Direction__c", label: "Direction", type: "Text", length: 50 },
        { apiName: "MSD_SecretKey__c", label: "Secret Key", type: "Text", length: 255 },
        { apiName: "MSD_TimeoutMs__c", label: "Timeout (ms)", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_MaxRetries__c", label: "Max Retries", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_Parameters__c", label: "Parameters (JSON)", type: "LongTextArea", length: 32768, visibleLines: 5 },
        { apiName: "MSD_Description__c", label: "Description", type: "LongTextArea", length: 32768, visibleLines: 5 }
      ]
    },
    {
      canonicalObjectApiName: "MSD_Schedule__c",
      label: "MSD Schedule",
      pluralLabel: "MSD Schedules",
      nameField: {
        type: "AutoNumber",
        label: "Schedule Number",
        displayFormat: "SCH-{0000}"
      },
      capability: "logUpload",
      legacyObjectApiNames: [],
      requiredFields: [
        { apiName: "Active__c", label: "Active", type: "Checkbox", defaultValue: true },
        { apiName: "SourceSystem__c", label: "Source System", type: "Text", length: 255 },
        { apiName: "TargetSystem__c", label: "Target System", type: "Text", length: 255 },
        { apiName: "ObjectName__c", label: "Object Name", type: "Text", length: 255 },
        { apiName: "Operation__c", label: "Operation", type: "Text", length: 100 },
        {
          apiName: "MSD_Connector__c",
          label: "Connector",
          type: "Lookup",
          referenceTo: "MSD_Connector__c",
          relationshipLabel: "Schedules",
          relationshipName: "Schedules"
        },
        { apiName: "MSD_MappingDefinition__c", label: "Mapping Definition (JSON)", type: "LongTextArea", length: 32768, visibleLines: 5 },
        { apiName: "MSD_Direction__c", label: "Direction", type: "Text", length: 50 },
        { apiName: "MSD_SourceType__c", label: "Source Type", type: "Text", length: 50 },
        {
          apiName: "MSD_TargetType__c",
          label: "Target Type",
          type: "Picklist",
          picklistValues: [
            { fullName: "SALESFORCE", label: "Salesforce", default: true },
            { fullName: "MSSQL", label: "MSSQL" },
            { fullName: "MOCK", label: "Mock" },
            { fullName: "SAGE100", label: "Sage100" },
            { fullName: "SALESFORCE_GLOBAL_PICKLIST", label: "Salesforce Global Picklist" },
            { fullName: "ORACLE", label: "Oracle" }
          ]
        },
        { apiName: "MSD_SourceDefinition__c", label: "Source Definition (JSON)", type: "LongTextArea", length: 32768, visibleLines: 5 },
        { apiName: "MSD_TargetDefinition__c", label: "Target Definition (JSON)", type: "LongTextArea", length: 32768, visibleLines: 5 },
        { apiName: "BatchSize__c", label: "Batch Size", type: "Number", precision: 18, scale: 0 },
        { apiName: "NextRunAt__c", label: "Next Run At", type: "DateTime" },
        { apiName: "LastRunAt__c", label: "Last Run At", type: "DateTime" }
      ]
    },
    {
      canonicalObjectApiName: "MSD_Run__c",
      label: "MSD Run",
      pluralLabel: "MSD Runs",
      nameField: {
        type: "AutoNumber",
        label: "Run Number",
        displayFormat: "RUN-{0000}"
      },
      capability: "logUpload",
      legacyObjectApiNames: [],
      requiredFields: [
        {
          apiName: "MSD_Schedule__c",
          label: "Schedule",
          type: "Lookup",
          referenceTo: "MSD_Schedule__c",
          relationshipLabel: "Runs",
          relationshipName: "Runs"
        },
        {
          apiName: "MSD_Status__c",
          label: "Status",
          type: "Picklist",
          picklistValues: [
            { fullName: "Running", label: "Running" },
            { fullName: "Success", label: "Success" },
            { fullName: "Partial Success", label: "Partial Success" },
            { fullName: "Failed", label: "Failed" }
          ]
        },
        { apiName: "MSD_StartedAt__c", label: "Started At", type: "DateTime" },
        { apiName: "MSD_FinishedAt__c", label: "Finished At", type: "DateTime" },
        { apiName: "MSD_CorrelationId__c", label: "Correlation ID", type: "Text", length: 255 },
        { apiName: "MSD_AgentId__c", label: "Agent ID", type: "Text", length: 255 },
        { apiName: "MSD_RecordsRead__c", label: "Records Read", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_RecordsProcessed__c", label: "Records Processed", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_RecordsSucceeded__c", label: "Records Succeeded", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_RecordsFailed__c", label: "Records Failed", type: "Number", precision: 18, scale: 0 },
        { apiName: "MSD_ErrorMessage__c", label: "Error Message", type: "LongTextArea", length: 32768, visibleLines: 5 }
      ]
    },
    {
      canonicalObjectApiName: "MSD_Log__c",
      label: "MSD Log",
      pluralLabel: "MSD Logs",
      nameField: {
        type: "AutoNumber",
        label: "Log Number",
        displayFormat: "LOG-{00000}"
      },
      capability: "logUpload",
      legacyObjectApiNames: [],
      requiredFields: [
        {
          apiName: "MSD_Run__c",
          label: "Run",
          type: "Lookup",
          referenceTo: "MSD_Run__c",
          relationshipLabel: "Logs",
          relationshipName: "Logs"
        },
        {
          apiName: "MSD_Level__c",
          label: "Level",
          type: "Picklist",
          picklistValues: [
            { fullName: "INFO", label: "INFO", default: true },
            { fullName: "WARN", label: "WARN" },
            { fullName: "ERROR", label: "ERROR" }
          ]
        },
        { apiName: "MSD_Step__c", label: "Step", type: "Text", length: 255 },
        { apiName: "MSD_Message__c", label: "Message", type: "LongTextArea", length: 32768, visibleLines: 5 },
        { apiName: "MSD_RecordKey__c", label: "Record Key", type: "Text", length: 255 },
        { apiName: "MSD_CorrelationId__c", label: "Correlation ID", type: "Text", length: 255 }
      ]
    },
    {
      canonicalObjectApiName: "MSD_Checkpoint__c",
      label: "MSD Checkpoint",
      pluralLabel: "MSD Checkpoints",
      nameField: {
        type: "AutoNumber",
        label: "Checkpoint Number",
        displayFormat: "CHK-{0000}"
      },
      capability: "logUpload",
      legacyObjectApiNames: [],
      requiredFields: [
        {
          apiName: "MSD_Schedule__c",
          label: "Schedule",
          type: "Lookup",
          referenceTo: "MSD_Schedule__c",
          relationshipLabel: "Checkpoints",
          relationshipName: "Checkpoints"
        },
        {
          apiName: "MSD_Run__c",
          label: "Run",
          type: "Lookup",
          referenceTo: "MSD_Run__c",
          relationshipLabel: "Checkpoints",
          relationshipName: "Checkpoints"
        },
        { apiName: "MSD_ObjectName__c", label: "Object Name", type: "Text", length: 255 },
        { apiName: "MSD_LastCheckpoint__c", label: "Last Checkpoint Value", type: "Text", length: 255 },
        { apiName: "MSD_LastRecordId__c", label: "Last Record ID", type: "Text", length: 255 }
      ]
    },
    {
      canonicalObjectApiName: "MSD_AgentHealth__c",
      capability: "healthPulse",
      legacyObjectApiNames: ["MSD_AgentPulse__c", "MSD_Heartbeat__c"],
      requiredFields: [
        {
          apiName: "MSD_InstanceId__c",
          label: "Instance Id",
          type: "Text",
          length: 120,
          legacyApiNames: ["InstanceId__c", "MSD_SourceInstance__c"]
        },
        {
          apiName: "MSD_ProjectId__c",
          label: "Project Id",
          type: "Text",
          length: 120,
          legacyApiNames: ["ProjectId__c"]
        },
        {
          apiName: "MSD_AgentVersion__c",
          label: "Agent Version",
          type: "Text",
          length: 120,
          legacyApiNames: ["Version__c", "AgentVersion__c"]
        },
        {
          apiName: "MSD_RuntimeStatus__c",
          label: "Runtime Status",
          type: "Text",
          length: 80,
          legacyApiNames: ["Status__c", "MSD_Status__c"]
        },
        {
          apiName: "MSD_LastSeenAt__c",
          label: "Last Seen At",
          type: "DateTime",
          legacyApiNames: ["LastSeenAt__c", "MSD_LastHeartbeat__c"]
        },
        {
          apiName: "MSD_HealthPayload__c",
          label: "Health Payload",
          type: "LongTextArea",
          length: 32768,
          visibleLines: 8,
          legacyApiNames: ["MSD_HealthJson__c", "HealthPayload__c", "Payload__c"]
        }
      ]
    },
    {
      canonicalObjectApiName: "MSD_AgentCommand__c",
      capability: "remoteCommands",
      legacyObjectApiNames: ["MSD_RemoteCommand__c", "MSD_AgentInstruction__c"],
      requiredFields: [
        {
          apiName: "MSD_CommandId__c",
          label: "Command Id",
          type: "Text",
          length: 120,
          legacyApiNames: ["CommandId__c"]
        },
        {
          apiName: "MSD_CommandType__c",
          label: "Command Type",
          type: "Text",
          length: 120,
          legacyApiNames: ["CommandType__c", "Type__c"]
        },
        {
          apiName: "MSD_CommandPayload__c",
          label: "Command Payload",
          type: "LongTextArea",
          length: 32768,
          visibleLines: 8,
          legacyApiNames: ["MSD_PayloadJson__c", "Payload__c"]
        },
        {
          apiName: "MSD_ResponseInstruction__c",
          label: "Response Instruction",
          type: "LongTextArea",
          length: 32768,
          visibleLines: 6,
          legacyApiNames: ["ResponseInstruction__c", "Instruction__c"]
        },
        {
          apiName: "MSD_Status__c",
          label: "Status",
          type: "Text",
          length: 80,
          legacyApiNames: ["Status__c"]
        },
        {
          apiName: "MSD_ExpiresAt__c",
          label: "Expires At",
          type: "DateTime",
          legacyApiNames: ["ExpiresAt__c"]
        },
        {
          apiName: "MSD_Signature__c",
          label: "Signature",
          type: "Text",
          length: 255,
          legacyApiNames: ["Signature__c"]
        }
      ]
    }
  ];

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

  private resolveProjectForInstance(instanceId?: string): SalesforceProjectConfig {
    if (!instanceId) {
      return defaultProjectConfig();
    }

    const configured = readConfiguredInstancesWithMetadata().find((item) => item.id === instanceId);
    const projectId = String(configured?.projectId || "default-project").trim() || "default-project";
    const projects = readLocalProjects();
    const projectById = new Map(projects.map((item) => [item.id, item]));
    return projectById.get(projectId) || projectById.get("default-project") || defaultProjectConfig();
  }

  private resolveLookupCacheRuntime(instanceId?: string): { enabled: boolean; ttlMs: number } {
    const project = this.resolveProjectForInstance(instanceId);
    if (project.lookupCacheEnabled === false) {
      return { enabled: false, ttlMs: 0 };
    }

    const ttlMinutes = Math.max(1, Number(project.lookupCacheTtlMinutes || 15) || 15);
    return {
      enabled: true,
      ttlMs: ttlMinutes * 60_000
    };
  }

  private async writeRunLogsWithProjectStrategy(
    client: SalesforceClient,
    inputs: CreateLogInput[],
    instanceId?: string
  ): Promise<void> {
    const project = this.resolveProjectForInstance(instanceId);
    const normalizedInputs = Array.isArray(inputs) ? inputs.filter((item) => !!item) : [];
    if (!normalizedInputs.length) {
      return;
    }

    const batchingEnabled = project.logBatchingEnabled !== false;
    const batchSize = Math.max(1, Number(project.logBatchSize || 200) || 200);

    if (!batchingEnabled || normalizedInputs.length === 1) {
      if (batchingEnabled) {
        await client.createLogsBulk([normalizedInputs[0]]);
        return;
      }
      await client.createLog(normalizedInputs[0]);
      return;
    }

    for (let offset = 0; offset < normalizedInputs.length; offset += batchSize) {
      const batch = normalizedInputs.slice(offset, offset + batchSize);
      if (!batch.length) {
        continue;
      }
      await client.createLogsBulk(batch);
    }
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
    sourceRecords: Array<Record<string, unknown>>,
    instanceId?: string
  ): Promise<LookupResolverFn> {
    const cacheRuntime = this.resolveLookupCacheRuntime(instanceId);
    const lookupLines = mappingLines.filter(
      (line) => line.transform.type === "LOOKUP" && line.transform.lookupObject && line.transform.lookupField
    );
    const lookupCache = new Map<string, { value: string | null; expiresAt: number }>();

    if (!cacheRuntime.enabled) {
      return async (lookupObject: string, lookupField: string, value: unknown): Promise<string | null> => {
        if (value === undefined || value === null || value === "") {
          return null;
        }

        const soql = `SELECT Id FROM ${lookupObject} WHERE ${lookupField} = ${this.toSoqlLiteral(value)} LIMIT 1`;
        const result = await client.queryGeneric(soql);
        return result.length && typeof result[0].Id === "string" ? result[0].Id : null;
      };
    }

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
            lookupCache.set(cacheKey, {
              value: typeof row.Id === "string" ? row.Id : null,
              expiresAt: Date.now() + cacheRuntime.ttlMs
            });
          }
        }

        for (const value of chunk) {
          const cacheKey = `${group.lookupObject}|${group.lookupField}|${value}`;
          if (!lookupCache.has(cacheKey)) {
            lookupCache.set(cacheKey, {
              value: null,
              expiresAt: Date.now() + cacheRuntime.ttlMs
            });
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
      const cachedEntry = lookupCache.get(cacheKey);
      if (cachedEntry && cachedEntry.expiresAt > Date.now()) {
        return cachedEntry.value;
      }

      const soql = `SELECT Id FROM ${lookupObject} WHERE ${lookupField} = ${this.toSoqlLiteral(value)} LIMIT 1`;
      const result = await client.queryGeneric(soql);
      const resolvedId = result.length && typeof result[0].Id === "string" ? result[0].Id : null;
      lookupCache.set(cacheKey, {
        value: resolvedId,
        expiresAt: Date.now() + cacheRuntime.ttlMs
      });
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
        loginUrl: instance.config.loginUrl,
        projectId,
        projectName,
        role,
        queryLimit: instance.config.queryLimit
      };
    });
  }

  private findInstanceOptionOrThrow(instanceId: string): SalesforceInstanceOption {
    const normalizedInstanceId = String(instanceId || "").trim();
    if (!normalizedInstanceId) {
      throw new Error("instanceId ist erforderlich");
    }

    const instance = this.listInstances().find((item) => item.id === normalizedInstanceId);
    if (!instance) {
      throw new Error(`Salesforce-Instanz ${normalizedInstanceId} nicht gefunden`);
    }

    return instance;
  }

  private buildDefaultReadinessMissingArtifacts(): SalesforceReadinessMissingArtifact[] {
    const defaults: SalesforceReadinessMissingArtifact[] = [];
    for (const definition of AdminDataService.agentObjectDefinitions) {
      defaults.push({
        type: "object",
        name: definition.canonicalObjectApiName,
        severity: "critical",
        message: `Objekt ${definition.canonicalObjectApiName} fehlt`
      });
      for (const field of definition.requiredFields) {
        defaults.push({
          type: "field",
          name: `${definition.canonicalObjectApiName}.${field.apiName}`,
          severity: "critical",
          message: `Feld ${definition.canonicalObjectApiName}.${field.apiName} fehlt`
        });
      }
    }

    defaults.push({
      type: "permission",
      name: "MSD_Integration_Agent",
      severity: "critical",
      message: "Berechtigungssatz fuer Agent-Betrieb fehlt"
    });
    return defaults;
  }

  private normalizeApiName(value: string): string {
    return String(value || "").trim().toLowerCase();
  }

  private escapeSoqlLiteral(value: string): string {
    return String(value || "").replace(/'/g, "\\'");
  }

  private buildCustomFieldMetadata(field: AgentObjectFieldDefinition): Record<string, unknown> {
    if (field.type === "Checkbox") {
      return {
        label: field.label,
        type: "Checkbox",
        defaultValue: field.defaultValue === true
      };
    }

    if (field.type === "DateTime") {
      return {
        label: field.label,
        type: "DateTime",
        required: false
      };
    }

    if (field.type === "LongTextArea") {
      return {
        label: field.label,
        type: "LongTextArea",
        length: field.length ?? 32768,
        visibleLines: field.visibleLines ?? 6,
        required: false
      };
    }

    if (field.type === "Number") {
      return {
        label: field.label,
        type: "Number",
        precision: field.precision ?? 18,
        scale: field.scale ?? 0,
        required: false,
        unique: false
      };
    }

    if (field.type === "Lookup") {
      return {
        label: field.label,
        type: "Lookup",
        referenceTo: field.referenceTo,
        relationshipLabel: field.relationshipLabel || field.label,
        relationshipName: field.relationshipName || field.apiName.replace(/__c$/, ""),
        deleteConstraint: "SetNull",
        required: false
      };
    }

    if (field.type === "Picklist") {
      return {
        label: field.label,
        type: "Picklist",
        required: false,
        valueSet: {
          valueSetDefinition: {
            sorted: false,
            value: (field.picklistValues || []).map((value) => ({
              fullName: value.fullName,
              default: value.default === true,
              label: value.label
            }))
          }
        }
      };
    }

    return {
      label: field.label,
      type: "Text",
      length: field.length ?? 255,
      required: false,
      unique: false
    };
  }

  private resolveKnownObjectName(
    knownObjects: Map<string, string>,
    names: string[]
  ): string | null {
    for (const name of names) {
      const match = knownObjects.get(this.normalizeApiName(name));
      if (match) {
        return match;
      }
    }
    return null;
  }

  private resolveKnownFieldName(
    knownFields: Set<string>,
    canonicalName: string,
    legacyNames: string[]
  ): string | null {
    const canonical = this.normalizeApiName(canonicalName);
    if (knownFields.has(canonical)) {
      return canonicalName;
    }

    for (const legacyName of legacyNames) {
      const legacy = this.normalizeApiName(legacyName);
      if (knownFields.has(legacy)) {
        return legacyName;
      }
    }

    return null;
  }

  private buildCapabilityFromArtifacts(
    missingArtifacts: SalesforceReadinessMissingArtifact[]
  ): { healthPulse: boolean; remoteCommands: boolean; logUpload: boolean } {
    const hasGlobalCritical = missingArtifacts.some(
      (artifact) => artifact.severity === "critical" && (artifact.name === "describeGlobal" || artifact.name === "MSD_Integration_Agent")
    );

    const hasHealthCritical = missingArtifacts.some(
      (artifact) => artifact.severity === "critical" && artifact.name.startsWith("MSD_AgentHealth__c")
    );
    const hasCommandCritical = missingArtifacts.some(
      (artifact) => artifact.severity === "critical" && artifact.name.startsWith("MSD_AgentCommand__c")
    );
    const hasLogUploadCritical = missingArtifacts.some(
      (artifact) => artifact.severity === "critical"
        && (
          artifact.name.startsWith("MSD_Connector__c")
          || artifact.name.startsWith("MSD_Schedule__c")
          || artifact.name.startsWith("MSD_Run__c")
          || artifact.name.startsWith("MSD_Log__c")
          || artifact.name.startsWith("MSD_Checkpoint__c")
        )
    );

    return {
      healthPulse: !(hasGlobalCritical || hasHealthCritical),
      remoteCommands: !(hasGlobalCritical || hasCommandCritical),
      logUpload: !(hasGlobalCritical || hasLogUploadCritical)
    };
  }

  private async ensureAgentPermissionSetSetup(client: SalesforceClient): Promise<{ applied: string[]; warnings: string[] }> {
    const applied: string[] = [];
    const warnings: string[] = [];
    const permissionSetName = "MSD_Integration_Agent";

    try {
      const existingPermissionSet = await client.queryGeneric(
        `SELECT Id, Name FROM PermissionSet WHERE Name = '${permissionSetName}' LIMIT 1`
      );
      if (!existingPermissionSet.length) {
        await client.createOrUpdateMetadata("PermissionSet", permissionSetName, {
          fullName: permissionSetName,
          label: "MSD Integration Agent",
          description: "Permissions for sf-onprem integration agent setup and runtime access.",
          hasActivationRequired: false
        });
        applied.push(`${permissionSetName}.permissionset`);

        let visible = false;
        for (let attempt = 0; attempt < 10; attempt += 1) {
          const permissionSet = await client.queryGeneric(
            `SELECT Id, Name FROM PermissionSet WHERE Name = '${permissionSetName}' LIMIT 1`
          );
          if (permissionSet.length) {
            visible = true;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        if (!visible) {
          warnings.push(`PermissionSet ${permissionSetName} wurde angelegt, ist aber noch nicht per SOQL sichtbar. Setup bitte erneut ausfuehren.`);
          return { applied, warnings };
        }
      }
    } catch (error) {
      warnings.push(`PermissionSet ${permissionSetName} konnte nicht angelegt/geprueft werden: ${error instanceof Error ? error.message : String(error)}`);
      return { applied, warnings };
    }

    try {
      const assignment = await client.ensurePermissionSetAssigned(permissionSetName);
      if (!assignment.alreadyExisted) {
        applied.push(`${permissionSetName}.assignment`);
      }
    } catch (error) {
      warnings.push(`PermissionSet ${permissionSetName} konnte dem aktuellen Salesforce-Benutzer nicht zugewiesen werden: ${error instanceof Error ? error.message : String(error)}`);
    }

    return { applied, warnings };
  }

  private async ensureAgentObjectSetup(
    client: SalesforceClient,
    knownObjects: Map<string, string>,
    definition: AgentObjectDefinition
  ): Promise<{ applied: string[]; warnings: string[] }> {
    const applied: string[] = [];
    const warnings: string[] = [];

    let targetObject = this.resolveKnownObjectName(knownObjects, [definition.canonicalObjectApiName]);
    if (!targetObject) {
      const legacyObject = this.resolveKnownObjectName(knownObjects, definition.legacyObjectApiNames);
      if (legacyObject) {
        targetObject = legacyObject;
        warnings.push(
          `Legacy-Objekt ${legacyObject} gefunden. Migration bleibt kompatibel; kanonisches Objekt ${definition.canonicalObjectApiName} wurde nicht erzwungen.`
        );
      } else {
        const canonical = definition.canonicalObjectApiName;
        const label = definition.label || canonical.replace(/__c$/, "").replace(/_/g, " ");
        await client.createOrUpdateMetadata("CustomObject", canonical, {
          fullName: canonical,
          label,
          pluralLabel: definition.pluralLabel || `${label}s`,
          deploymentStatus: "Deployed",
          sharingModel: "ReadWrite",
          nameField: definition.nameField || {
            type: "AutoNumber",
            label: "Name",
            displayFormat: `${canonical.replace(/__c$/, "")}-{0000}`
          }
        });
        if (await client.waitForObjectVisibility(canonical)) {
          knownObjects.set(this.normalizeApiName(canonical), canonical);
          targetObject = canonical;
          applied.push(canonical);
        } else {
          warnings.push(`Objekt ${canonical} wurde angelegt, ist aber noch nicht im Salesforce-Describe sichtbar. Bitte Setup erneut ausfuehren.`);
          return { applied, warnings };
        }
      }
    }

    if (!targetObject) {
      return { applied, warnings };
    }

    try {
      const objectAccess = await client.ensurePermissionSetObjectAccess("MSD_Integration_Agent", targetObject);
      if (!objectAccess.alreadyExisted) {
        applied.push(`MSD_Integration_Agent.objectPermissions.${targetObject}`);
      }
    } catch (error) {
      warnings.push(
        `Objektberechtigung fuer ${targetObject} konnte nicht gesetzt werden: ${error instanceof Error ? error.message : String(error)}`
      );
    }

    let knownFieldNames = new Set<string>();
    try {
      const fields = await client.describeObjectFields(targetObject, { forceRefresh: true });
      knownFieldNames = new Set(fields.map((field) => this.normalizeApiName(field.name)).filter(Boolean));
    } catch (error) {
      warnings.push(
        `Felder fuer ${targetObject} konnten nicht geladen werden: ${error instanceof Error ? error.message : String(error)}`
      );
      return { applied, warnings };
    }

    for (const field of definition.requiredFields) {
      const resolvedField = this.resolveKnownFieldName(knownFieldNames, field.apiName, field.legacyApiNames || []);
      if (resolvedField) {
        const resolvedFullName = `${targetObject}.${resolvedField}`;
        if (await client.waitForCustomFieldMetadataVisibility(resolvedFullName, 6, 1000)) {
          try {
            const fieldAccess = await client.ensurePermissionSetFieldAccess("MSD_Integration_Agent", targetObject, resolvedField);
            if (!fieldAccess.alreadyExisted) {
              applied.push(`MSD_Integration_Agent.fieldPermissions.${resolvedFullName}`);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("no CustomField named") || message.includes("bad value for restricted picklist field")) {
              warnings.push(
                `Feldberechtigung fuer ${resolvedFullName} wird beim naechsten Setup-Lauf erneut gesetzt; Salesforce kennt das Feld in PermissionSet-Metadaten noch nicht.`
              );
            } else {
              warnings.push(`Feldberechtigung fuer ${resolvedFullName} konnte nicht gesetzt werden: ${message}`);
            }
          }
        } else {
          warnings.push(
            `Feldberechtigung fuer ${resolvedFullName} wird beim naechsten Setup-Lauf gesetzt; Salesforce-Metadata-Read sieht das Feld noch nicht.`
          );
        }
        if (this.normalizeApiName(resolvedField) !== this.normalizeApiName(field.apiName)) {
          warnings.push(
            `Legacy-Feld ${targetObject}.${resolvedField} gefunden. Kanonisches Feld ${targetObject}.${field.apiName} wird fuer neue Installationen verwendet.`
          );
        }
        continue;
      }

      const fullName = `${targetObject}.${field.apiName}`;
      try {
        await client.createOrUpdateMetadata("CustomField", fullName, this.buildCustomFieldMetadata(field), {
          waitForCustomFieldDescribe: false
        });
        const metadataVisible = await client.waitForCustomFieldMetadataVisibility(fullName, 2, 500);
        if (metadataVisible) {
          try {
            const fieldAccess = await client.ensurePermissionSetFieldAccess("MSD_Integration_Agent", targetObject, field.apiName);
            if (!fieldAccess.alreadyExisted) {
              applied.push(`MSD_Integration_Agent.fieldPermissions.${fullName}`);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("no CustomField named") || message.includes("bad value for restricted picklist field")) {
              warnings.push(
                `Feldberechtigung fuer ${fullName} wird beim naechsten Setup-Lauf erneut gesetzt; Salesforce kennt das Feld in PermissionSet-Metadaten noch nicht.`
              );
            } else {
              warnings.push(`Feldberechtigung fuer ${fullName} konnte nicht gesetzt werden: ${message}`);
            }
          }
        } else {
          warnings.push(
            `Feld ${fullName} wurde angelegt, ist aber noch nicht per Salesforce-Metadata-Read sichtbar. Feldberechtigung wird beim naechsten Setup-Lauf gesetzt.`
          );
        }
        knownFieldNames.add(this.normalizeApiName(field.apiName));
        applied.push(fullName);
      } catch (error) {
        warnings.push(`${fullName}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    return { applied, warnings };
  }

  private async checkSalesforceAgentReadiness(
    instanceId: string,
    permissionSetName = "MSD_Integration_Agent"
  ): Promise<{
    missingArtifacts: SalesforceReadinessMissingArtifact[];
    capabilities: { healthPulse: boolean; remoteCommands: boolean; logUpload: boolean };
  }> {
    const resolved = this.resolveRuntimeInstance(instanceId);
    const client = new SalesforceClient(resolved.config);
    await client.login();

    const missingArtifacts: SalesforceReadinessMissingArtifact[] = [];
    const knownObjects = new Map<string, string>();
    try {
      const metadata = await client.listObjectMetadata();
      for (const entry of metadata) {
        const objectName = String(entry.name || "").trim();
        if (!objectName) {
          continue;
        }
        knownObjects.set(this.normalizeApiName(objectName), objectName);
      }
    } catch (error) {
      missingArtifacts.push({
        type: "capability",
        name: "describeGlobal",
        severity: "critical",
        message: error instanceof Error ? error.message : String(error)
      });
    }

    for (const definition of AdminDataService.agentObjectDefinitions) {
      const canonical = definition.canonicalObjectApiName;
      const objectApiName = this.resolveKnownObjectName(
        knownObjects,
        [canonical, ...definition.legacyObjectApiNames]
      );

      if (!objectApiName) {
        missingArtifacts.push({
          type: "object",
          name: canonical,
          severity: "critical",
          message: `Objekt ${canonical} fehlt oder ist fuer den aktuellen Benutzer nicht sichtbar.`
        });
        continue;
      }

      if (this.normalizeApiName(objectApiName) !== this.normalizeApiName(canonical)) {
        missingArtifacts.push({
          type: "object",
          name: canonical,
          severity: "warning",
          message: `Legacy-Objekt ${objectApiName} erkannt. Migration auf ${canonical} empfohlen, aber bestehendes System bleibt lauffaehig.`
        });
      }

      let knownFieldNames = new Set<string>();
      try {
        const fields = await client.describeObjectFields(objectApiName, { forceRefresh: true });
        knownFieldNames = new Set(fields.map((field) => this.normalizeApiName(field.name)).filter(Boolean));
      } catch (error) {
        missingArtifacts.push({
          type: "capability",
          name: canonical,
          severity: "critical",
          message: `Feld-Describe fuer ${objectApiName} fehlgeschlagen: ${error instanceof Error ? error.message : String(error)}`
        });
        continue;
      }

      for (const field of definition.requiredFields) {
        const matchedField = this.resolveKnownFieldName(knownFieldNames, field.apiName, field.legacyApiNames || []);
        if (!matchedField) {
          const fieldCandidates = [field.apiName, ...(field.legacyApiNames || [])];
          let metadataFieldName = "";
          for (const candidateFieldName of fieldCandidates) {
            const candidateFullName = `${objectApiName}.${candidateFieldName}`;
            if (await client.customFieldMetadataExists(candidateFullName)) {
              metadataFieldName = candidateFieldName;
              break;
            }
          }

          if (metadataFieldName) {
            missingArtifacts.push({
              type: "field",
              name: `${canonical}.${field.apiName}`,
              severity: "warning",
              message: `Feld ${metadataFieldName} existiert auf ${objectApiName}, ist fuer den aktuellen Benutzer aber nicht im Salesforce-Describe sichtbar. Field-Level-Security/PermissionSet pruefen.`
            });
            continue;
          }

          missingArtifacts.push({
            type: "field",
            name: `${canonical}.${field.apiName}`,
            severity: "critical",
            message: `Erforderliches Feld ${field.apiName} fehlt auf ${objectApiName}.`
          });
          continue;
        }

        if (this.normalizeApiName(matchedField) !== this.normalizeApiName(field.apiName)) {
          missingArtifacts.push({
            type: "field",
            name: `${canonical}.${field.apiName}`,
            severity: "warning",
            message: `Legacy-Feld ${matchedField} erkannt. Migration auf ${field.apiName} empfohlen.`
          });
        }
      }
    }

    try {
      const permissionCandidates = [
        permissionSetName,
        ...AdminDataService.agentPermissionSetLegacyNames
      ].map((entry) => String(entry || "").trim()).filter(Boolean);
      const quotedPermissionCandidates = permissionCandidates
        .map((entry) => `'${this.escapeSoqlLiteral(entry)}'`)
        .join(", ");
      const permissionSetResult = await client.queryGeneric(
        `SELECT Id, Name FROM PermissionSet WHERE Name IN (${quotedPermissionCandidates})`
      );
      const discoveredPermissionSets = new Set(
        permissionSetResult
          .map((entry) => String(entry.Name || "").trim())
          .filter(Boolean)
      );

      if (!discoveredPermissionSets.size) {
        missingArtifacts.push({
          type: "permission",
          name: permissionSetName,
          severity: "critical",
          message: "Erforderlicher Permission Set fehlt."
        });
      } else if (!discoveredPermissionSets.has(permissionSetName)) {
        const legacyPermissionName = permissionCandidates.find((entry) => discoveredPermissionSets.has(entry));
        missingArtifacts.push({
          type: "permission",
          name: permissionSetName,
          severity: "warning",
          message: `Legacy-PermissionSet ${legacyPermissionName || "unbekannt"} erkannt. Migration auf ${permissionSetName} empfohlen.`
        });
      }
    } catch (error) {
      missingArtifacts.push({
        type: "permission",
        name: permissionSetName,
        severity: "critical",
        message: error instanceof Error ? error.message : String(error)
      });
    }

    return {
      missingArtifacts,
      capabilities: this.buildCapabilityFromArtifacts(missingArtifacts)
    };
  }

  private async applySalesforceMsdSetup(
    instanceId: string,
    components: string[]
  ): Promise<{ applied: string[]; warnings: string[]; missingArtifacts: SalesforceReadinessMissingArtifact[] }> {
    const resolved = this.resolveRuntimeInstance(instanceId);
    const client = new SalesforceClient(resolved.config);
    await client.login();

    const knownObjects = new Map<string, string>();
    try {
      const metadata = await client.listObjectMetadata();
      for (const entry of metadata) {
        const objectName = String(entry.name || "").trim();
        if (!objectName) {
          continue;
        }
        knownObjects.set(this.normalizeApiName(objectName), objectName);
      }
    } catch {
      // Readiness liefert den eigentlichen Fehlertext; Setup versucht dennoch Best-Effort-Anlage.
    }

    const desiredComponents = components.length
      ? components
      : [
          "MSD_Integration_Agent.permissionset",
          "MSD_Connector__c",
          "MSD_Schedule__c",
          "MSD_Run__c",
          "MSD_Log__c",
          "MSD_Checkpoint__c",
          "MSD_AgentHealth__c",
          "MSD_AgentCommand__c"
        ];
    const applied: string[] = [];
    const warnings: string[] = [];

    for (const component of desiredComponents) {
      const normalized = String(component || "").trim();
      if (!normalized) {
        continue;
      }

      try {
        const definition = AdminDataService.agentObjectDefinitions.find(
          (entry) => this.normalizeApiName(entry.canonicalObjectApiName) === this.normalizeApiName(normalized)
        );
        if (definition) {
          const setupResult = await this.ensureAgentObjectSetup(client, knownObjects, definition);
          applied.push(...setupResult.applied);
          warnings.push(...setupResult.warnings);
          continue;
        }

        if (normalized === "MSD_Integration_Agent.permissionset") {
          const permissionSetResult = await this.ensureAgentPermissionSetSetup(client);
          applied.push(...permissionSetResult.applied);
          warnings.push(...permissionSetResult.warnings);
          continue;
        }

        warnings.push(`Komponente ${normalized} ist im MVP-Setup nicht automatisiert hinterlegt.`);
      } catch (error) {
        warnings.push(`${normalized}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    const readiness = await this.checkSalesforceAgentReadiness(instanceId);
    return {
      applied,
      warnings,
      missingArtifacts: readiness.missingArtifacts
    };
  }

  public async runInstanceReadinessCheck(
    instanceId: string,
    input: SalesforceInstanceReadinessCheckInput
  ): Promise<SalesforceInstanceReadinessResult> {
    const instance = this.findInstanceOptionOrThrow(instanceId);
    const projectId = String(input.projectId || instance.projectId || "default-project").trim() || "default-project";
    const now = new Date().toISOString();

    const records = readLocalInstanceReadinessRecords();
    const existing = records.find((entry) => entry.instanceId === instance.id);

    if (existing?.status === "setup-running") {
      throw new Error("Readiness-Check kann nicht ausgefuehrt werden, solange ein Setup-Lauf aktiv ist.");
    }

    const runtimeReadiness = await this.checkSalesforceAgentReadiness(instance.id);
    const missingArtifacts = runtimeReadiness.missingArtifacts;
    const criticalArtifactCount = missingArtifacts.filter((artifact) => artifact.severity === "critical").length;
    const status: SalesforceInstanceReadinessStatus = criticalArtifactCount ? "setup-required" : "ready";

    const nextRecord: LocalInstanceReadinessRecord = {
      instanceId: instance.id,
      projectId,
      status,
      missingArtifacts,
      lastCheckedAt: now,
      lastSetupAt: existing?.lastSetupAt
    };

    const nextRecords = records.filter((entry) => entry.instanceId !== instance.id);
    nextRecords.push(nextRecord);
    writeLocalInstanceReadinessRecords(nextRecords);

    return {
      instanceId: instance.id,
      projectId,
      status,
      checkedAt: now,
      missingArtifacts,
      capabilities: runtimeReadiness.capabilities,
      nextAction: status === "ready" ? "none" : "run-msd-setup"
    };
  }

  public async runInstanceMsdSetup(
    instanceId: string,
    input: SalesforceInstanceMsdSetupInput
  ): Promise<SalesforceInstanceMsdSetupResult> {
    const instance = this.findInstanceOptionOrThrow(instanceId);
    const projectId = String(input.projectId || instance.projectId || "default-project").trim() || "default-project";
    const mode = input.mode === "dry-run" ? "dry-run" : "apply";
    const startedAt = new Date().toISOString();
    const normalizedComponents = Array.isArray(input.components)
      ? input.components.map((entry) => String(entry || "").trim()).filter(Boolean)
      : [];

    let applied: string[] = [];
    let warnings: string[] = [];
    let missingArtifacts: SalesforceReadinessMissingArtifact[] = this.buildDefaultReadinessMissingArtifacts();
    if (mode === "apply") {
      const setupResult = await this.applySalesforceMsdSetup(instance.id, normalizedComponents);
      applied = setupResult.applied;
      warnings = setupResult.warnings;
      missingArtifacts = setupResult.missingArtifacts;
    } else {
      const readiness = await this.checkSalesforceAgentReadiness(instance.id);
      missingArtifacts = readiness.missingArtifacts;
      warnings = ["Dry-Run: Keine Aenderungen in Salesforce vorgenommen."];
    }
    const finishedAt = new Date().toISOString();

    const records = readLocalInstanceReadinessRecords();
    const criticalArtifactCount = missingArtifacts.filter((artifact) => artifact.severity === "critical").length;
    const nextStatus: SalesforceInstanceReadinessStatus = criticalArtifactCount
      ? (mode === "apply" && warnings.length ? "setup-failed" : "setup-required")
      : "ready";
    const nextRecord: LocalInstanceReadinessRecord = {
      instanceId: instance.id,
      projectId,
      status: nextStatus,
      missingArtifacts,
      lastCheckedAt: finishedAt,
      lastSetupAt: mode === "apply" ? finishedAt : records.find((entry) => entry.instanceId === instance.id)?.lastSetupAt
    };
    const nextRecords = records.filter((entry) => entry.instanceId !== instance.id);
    nextRecords.push(nextRecord);
    writeLocalInstanceReadinessRecords(nextRecords);

    return {
      instanceId: instance.id,
      projectId,
      status: nextStatus,
      startedAt,
      finishedAt,
      applied,
      warnings,
      missingArtifacts,
      capabilities: this.buildCapabilityFromArtifacts(missingArtifacts),
      nextAction: nextStatus === "ready" ? "none" : "run-msd-setup",
      auditId: `audit-${Date.now().toString(36)}`
    };
  }

  public assertInstanceWriteAllowed(instanceId?: string, operation?: string): void {
    const resolved = this.resolveRuntimeInstance(instanceId);
    const configuredInstances = readConfiguredInstancesWithMetadata();
    const metadataById = new Map(configuredInstances.map((item) => [item.id, item]));
    const instanceMeta = metadataById.get(resolved.id);

    const role: "test" | "production" = instanceMeta?.role === "production" ? "production" : "test";
    if (role !== "production") {
      return;
    }

    const projects = readLocalProjects();
    const projectById = new Map(projects.map((project) => [project.id, project]));
    const projectId = String(instanceMeta?.projectId || "default-project").trim() || "default-project";
    const project = projectById.get(projectId) || projectById.get("default-project") || defaultProjectConfig();

    if (project.productionWriteProtection) {
      if (
        isOperationalProductionMutation(operation)
        && isCustomerProductionOperationsMode(projectId, configuredInstances)
      ) {
        return;
      }

      const details = operation ? ` (${operation})` : "";
      throw new Error(`Schreibzugriff blockiert: Instanz ${resolved.name} (${resolved.id}) ist als Produktion im Projekt ${project.name} mit aktivem Produktionsschutz konfiguriert${details}.`);
    }
  }

  public listConfiguredInstanceConfigs(): SalesforceInstanceEnvConfig[] {
    return readConfiguredSalesforceInstances();
  }

  public listProjects(): SalesforceProjectOption[] {
    return readLocalProjects()
      .map((project) => {
        const { confluenceApiToken: _confluenceApiToken, ...safeProject } = project;
        return {
          ...safeProject,
          confluenceApiTokenConfigured: Boolean(_confluenceApiToken)
        };
      })
      .sort((left, right) => left.name.localeCompare(right.name, "de"));
  }

  public getProjectConfig(projectId: string): SalesforceProjectConfig | undefined {
    const normalizedProjectId = String(projectId || "").trim();
    if (!normalizedProjectId) {
      return undefined;
    }
    const project = readLocalProjects().find((item) => item.id === normalizedProjectId);
    return project ? { ...project } : undefined;
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

    const lookupCacheTtlMinutes = Math.max(
      1,
      Number(
        input.lookupCacheTtlMinutes
        ?? existingProject?.lookupCacheTtlMinutes
        ?? 15
      ) || 15
    );
    const logSyncIntervalMinutes = Math.max(
      1,
      Number(
        input.logSyncIntervalMinutes
        ?? existingProject?.logSyncIntervalMinutes
        ?? 5
      ) || 5
    );
    const logBatchSize = Math.max(
      1,
      Number(
        input.logBatchSize
        ?? existingProject?.logBatchSize
        ?? 200
      ) || 200
    );
    const logBufferMaxEntries = Math.max(
      100,
      Number(
        input.logBufferMaxEntries
        ?? existingProject?.logBufferMaxEntries
        ?? 10000
      ) || 10000
    );

    // Normalize and validate Confluence parent page id: accept numeric id or full Confluence URL
    let normalizedConfluenceParentPageId: string | undefined = undefined;
    const rawConfluenceParentPageId = String(input.confluenceParentPageId ?? existingProject?.confluenceParentPageId ?? "").trim();
    if (rawConfluenceParentPageId) {
      const urlMatch = rawConfluenceParentPageId.match(/(?:pages\/(\d+)|pageId=(\d+))/i);
      if (urlMatch && (urlMatch[1] || urlMatch[2])) {
        normalizedConfluenceParentPageId = String(urlMatch[1] || urlMatch[2]);
      } else if (/^[0-9]+$/.test(rawConfluenceParentPageId)) {
        normalizedConfluenceParentPageId = rawConfluenceParentPageId;
      } else {
        throw new Error('Ungültige Confluence Parent Page ID. Bitte nur numerische ID oder eine Confluence Page URL angeben.');
      }
    }

    const nextItem: SalesforceProjectConfig = {
      id,
      name,
      description: String(input.description || "").trim() || undefined,
      archived: input.archived === undefined ? (existingProject?.archived === true) : input.archived === true,
      productionWriteProtection: input.productionWriteProtection !== false,
      lookupCacheEnabled: input.lookupCacheEnabled === undefined ? (existingProject?.lookupCacheEnabled !== false) : input.lookupCacheEnabled !== false,
      lookupCacheTtlMinutes,
      logBatchingEnabled: input.logBatchingEnabled === undefined ? (existingProject?.logBatchingEnabled !== false) : input.logBatchingEnabled !== false,
      logSyncIntervalMinutes,
      logBatchSize,
      logBufferMaxEntries,
      confluenceBaseUrl: String(input.confluenceBaseUrl || existingProject?.confluenceBaseUrl || "").trim() || undefined,
      confluenceUsername: String(input.confluenceUsername || existingProject?.confluenceUsername || "").trim() || undefined,
      confluenceApiToken: String(input.confluenceApiToken || existingProject?.confluenceApiToken || "").trim() || undefined,
      confluenceSpaceKey: String(input.confluenceSpaceKey || existingProject?.confluenceSpaceKey || "").trim() || undefined,
      confluenceParentPageId: normalizedConfluenceParentPageId,
      confluencePageTitlePrefix: String(input.confluencePageTitlePrefix || existingProject?.confluencePageTitlePrefix || "").trim() || undefined,
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
    const { confluenceApiToken: _confluenceApiToken, ...safeProject } = nextItem;
    return { ...safeProject, confluenceApiTokenConfigured: Boolean(_confluenceApiToken) };
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

    const localInstances = readLocalInstances();
    const reassignedLocalInstances = localInstances.map((instance) =>
      String(instance.projectId || "default-project").trim() === normalizedProjectId
        ? { ...instance, projectId: "default-project" }
        : instance
    );
    const localAssignedIds = new Set(
      localInstances
        .filter((instance) => String(instance.projectId || "default-project").trim() === normalizedProjectId)
        .map((instance) => String(instance.id || "").trim())
        .filter(Boolean)
    );
    const assignedInstances = readConfiguredInstancesWithMetadata().filter((instance) =>
      String(instance.projectId || "default-project").trim() === normalizedProjectId
    );
    const nonLocalAssignedInstances = assignedInstances.filter((instance) => !localAssignedIds.has(String(instance.id || "").trim()));
    if (nonLocalAssignedInstances.length) {
      throw new Error(`Projekt ${normalizedProjectId} kann nicht geloescht werden, da noch ${nonLocalAssignedInstances.length} nicht lokal verwaltete Instanz(en) zugeordnet sind.`);
    }

    const projects = readLocalProjects();
    const filtered = projects.filter((entry) => entry.id !== normalizedProjectId);
    const deleted = filtered.length < projects.length;

    if (deleted) {
      if (localAssignedIds.size > 0) {
        writeLocalInstances(reassignedLocalInstances);
      }
      writeLocalProjects(filtered);
    }

    return { deleted, projectId: normalizedProjectId };
  }

  public saveInstance(input: SalesforceInstanceMutationInput): SalesforceInstanceOption {
    const id = String(input.id || "").trim();
    const projectId = String(input.projectId || "default-project").trim() || "default-project";
    const role = input.role === "production" ? "production" : "test";
    if (!id) {
      throw new Error("id ist erforderlich");
    }

    const projects = readLocalProjects();
    const project = projects.find((item) => item.id === projectId);
    if (!project) {
      throw new Error(`Projekt ${projectId} nicht gefunden`);
    }

    const localInstances = readLocalInstances();
    const configuredInstances = readConfiguredInstancesWithMetadata();
    const existingLocal = localInstances.find((item) => item.id === id);
    const existingConfigured = configuredInstances.find((item) => item.id === id);
    const existingResolved = resolveInstances().find((item) => item.id === id);

    const loginUrl = String(input.loginUrl || existingLocal?.loginUrl || existingConfigured?.loginUrl || existingResolved?.config.loginUrl || "").trim();
    const clientId = String(input.clientId || existingLocal?.clientId || existingConfigured?.clientId || existingResolved?.config.clientId || "").trim();
    const clientSecret = String(input.clientSecret || existingLocal?.clientSecret || existingConfigured?.clientSecret || existingResolved?.config.clientSecret || "").trim();
    const clientIdEnv = String(input.clientIdEnv || existingLocal?.clientIdEnv || existingConfigured?.clientIdEnv || "").trim();
    const clientSecretEnv = String(input.clientSecretEnv || existingLocal?.clientSecretEnv || existingConfigured?.clientSecretEnv || "").trim();

    if (!loginUrl || (!clientId && !clientIdEnv) || (!clientSecret && !clientSecretEnv)) {
      throw new Error("Instanzzuordnung konnte nicht gespeichert werden: loginUrl und Client-Credentials fehlen. Bestehende Instanzen muessen entweder clientId/clientSecret oder clientIdEnv/clientSecretEnv enthalten.");
    }

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
      name: input.name?.trim() || existingLocal?.name || existingConfigured?.name || existingResolved?.name || id,
      loginUrl,
      projectId,
      role,
      clientId: clientId || undefined,
      clientSecret: clientSecret || undefined,
      clientIdEnv: clientIdEnv || undefined,
      clientSecretEnv: clientSecretEnv || undefined,
      queryLimit: input.queryLimit || existingLocal?.queryLimit || existingConfigured?.queryLimit || existingResolved?.config.queryLimit
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
      loginUrl: nextItem.loginUrl,
      projectId,
      projectName: project.name,
      role,
      queryLimit: nextItem.queryLimit
    };
  }

  public listMigrationInstances(): MigrationSalesforceInstanceSummary[] {
    return [];
  }

  public saveMigrationInstance(input: MigrationSalesforceInstanceMutationInput): MigrationSalesforceInstanceSummary {
    throw new Error("Migration-Instanzen werden nicht mehr separat verwaltet. Nutze Projektzuordnung und Projektinstanzen.");
  }

  public getMigrationOAuthAuthorizationUrl(instanceId: string, state: string, redirectUri: string): string {
    throw new Error("OAuth fuer separate Migration-Instanzen ist nicht mehr verfuegbar.");
  }

  public async completeMigrationOAuth(instanceId: string, code: string, redirectUri: string): Promise<MigrationSalesforceInstanceSummary> {
    throw new Error("OAuth fuer separate Migration-Instanzen ist nicht mehr verfuegbar.");
  }

  public async connectMigrationInstance(instanceId: string): Promise<MigrationSalesforceInstanceSummary> {
    throw new Error("Migration-Instanzen werden nicht mehr separat verwaltet. Nutze Projektzuordnung und Projektinstanzen.");
  }

  public async listSchedules(instanceId?: string): Promise<ScheduleListItem[]> {
    const resolvedInstance = this.resolveInstance(instanceId);
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

    const checkpointLookupKeys = records.map((record) => {
      const schedule = this.toIntegrationSchedule(record);
      return { scheduleId: schedule.id, objectName: schedule.objectName };
    });
    const checkpointsByScheduleAndObject = await client.getCheckpoints(checkpointLookupKeys).catch(() => new Map());

    return records.map((record) => {
      const schedule = this.toIntegrationSchedule(record);
      const persistedTimingDefinition = localTiming[schedule.id] || schedule.timingDefinition;
      const effectiveNextRunAt = schedule.nextRunAt
        || calculateNextRunAtFromTiming(persistedTimingDefinition, new Date());
      const effectiveSchedule: IntegrationSchedule = {
        ...schedule,
        timingDefinition: persistedTimingDefinition,
        nextRunAt: effectiveNextRunAt
      };
      const checkpoint = checkpointsByScheduleAndObject.get(`${schedule.id}::${schedule.objectName}`) || null;

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
          nextRunAt: effectiveNextRunAt,
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
    const client = await this.createClient(resolvedInstance.id);
    const requestedLimit = Math.max(1, Math.min(limit, 200));
    const rawLimit = Math.max(requestedLimit, Math.min(200, requestedLimit * 5));
    const runs = await client.queryRuns(rawLimit);
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
    })).filter(isMonitorRelevantRun).slice(0, requestedLimit);
  }

  public async summarizeRecordsByRange(range: OverviewStatsRange, instanceId?: string): Promise<RecordsChartSummary> {
    const { from, to } = this.getOverviewStatsRangeWindow(range);
    const buckets = this.createRecordsChartBuckets(range, from, to);
    const client = await this.createClient(instanceId);
    const runs = await client.queryRunsByDateRange(from.toISOString(), to.toISOString(), 5000);
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const tomorrowStart = new Date(todayStart);
    tomorrowStart.setDate(todayStart.getDate() + 1);
    const yesterdayStart = new Date(todayStart);
    yesterdayStart.setDate(todayStart.getDate() - 1);
    const dailyRuns = await client.queryRunsByDateRange(yesterdayStart.toISOString(), tomorrowStart.toISOString(), 5000);
    const daily = dailyRuns.reduce(
      (summary, run) => {
        const startedAt = run.MSD_StartedAt__c ? new Date(run.MSD_StartedAt__c) : null;
        if (!startedAt || Number.isNaN(startedAt.getTime())) {
          return summary;
        }

        const succeeded = Math.max(0, Number(run.MSD_RecordsSucceeded__c || 0));
        const failed = Math.max(0, Number(run.MSD_RecordsFailed__c || 0));
        if (startedAt >= todayStart && startedAt < tomorrowStart) {
          summary.total += succeeded + failed;
          summary.succeeded += succeeded;
          summary.failed += failed;
        } else if (startedAt >= yesterdayStart && startedAt < todayStart) {
          summary.previousSucceeded += succeeded;
        }
        return summary;
      },
      {
        date: todayStart.toISOString(),
        total: 0,
        succeeded: 0,
        failed: 0,
        previousSucceeded: 0,
        growth: 0,
        growthPercent: null as number | null
      }
    );
    daily.growth = daily.succeeded - daily.previousSucceeded;
    daily.growthPercent = daily.previousSucceeded > 0 ? (daily.growth / daily.previousSucceeded) * 100 : null;
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
      connectors: Array.from(connectorNames).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" })),
      daily
    };
  }

  public async setScheduleActive(scheduleId: string, active: boolean, instanceId?: string): Promise<{ id: string; active: boolean }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    await client.updateScheduleRecord(scheduleId, {
      Active__c: active
    });
    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listSchedules"]);

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
      const latestLogsByRunId = await client.queryLatestLogsByRunIds(runs.map((run) => run.Id)).catch(() => new Map());

    const staleCandidates = runs.map((run) => {
        const startedAt = run.MSD_StartedAt__c;
        const startedAtMs = startedAt ? new Date(startedAt).getTime() : Number.NaN;
        const ageMinutes = Number.isNaN(startedAtMs)
          ? staleThresholdMinutes
          : Math.max(0, Math.round((now - startedAtMs) / 60000));
        const latestLogCreatedAt = latestLogsByRunId.get(run.Id)?.CreatedDate;
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
      });

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
    await this.writeRunLogsWithProjectStrategy(
      client,
      [{
        runId: normalizedRunId,
        level: "WARN",
        step: "RUN_ABORTED",
        message: errorMessage,
        correlationId: run.MSD_CorrelationId__c || `manual-abort-${Date.now()}`
      }],
      instanceId
    );

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
    })).filter(isMonitorRelevantLogItem);
  }

  public async getRunFailedRecords(runId: string, instanceId?: string): Promise<RunFailedRecordsResult> {
    const normalizedRunId = String(runId || "").trim();
    if (!normalizedRunId) {
      throw new Error("Run-ID fehlt");
    }

    const fallbackFromLogs = async (): Promise<RunFailedRecordsResult> => {
      const logs = await this.listLogs(normalizedRunId, 500, instanceId);
      const items = logs
        .filter((log) => String(log.level || "").trim().toUpperCase() === "ERROR")
        .map((log, index) => ({
          rowIndex: index,
          externalKey: String(log.recordKey || "").trim() || undefined,
          statusCode: String(log.step || "").trim() || undefined,
          message: String(log.message || "").trim() || undefined,
          retryable: undefined,
          sourceRecord: undefined,
          mappedRecord: undefined
        }));

      return {
        runId: normalizedRunId,
        scheduleName: String(logs[0]?.scheduleName || "").trim() || undefined,
        total: items.length,
        items
      };
    };

    const filePath = path.join(LOCAL_FAILED_RUN_RECORDS_DIR, `${normalizedRunId}.json`);
    if (!fs.existsSync(filePath)) {
      return await fallbackFromLogs();
    }

    try {
      const raw = fs.readFileSync(filePath, "utf8").trim();
      if (!raw) {
        return await fallbackFromLogs();
      }

      const parsed = JSON.parse(raw) as RunFailedRecordsResult;
      const parsedResult = {
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

      return parsedResult.items.length > 0 ? parsedResult : await fallbackFromLogs();
    } catch {
      return await fallbackFromLogs();
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
    const records = await client.queryLogsByDateRange(
      startIso,
      endIso,
      Math.max(limit * 4, 1000),
      type === "error" ? "ERROR" : undefined
    );

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
    })).filter(isMonitorRelevantLogItem);

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
  ): Promise<{
    id: string;
    action: "created" | "updated";
    sourceDefinition?: string;
    mappingDefinition?: string;
  }> {
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
      this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listSchedules", "scheduleFormOptions"]);
      this.saveLocalTimingDefinition(resolvedInstance.id, input.id, input.timingDefinition);
      if (input.active) {
        this.clearScheduleAutoDisabledFlag(input.id);
      }
      const persisted = await client.queryScheduleById(input.id);
      return {
        id: input.id,
        action: "updated",
        sourceDefinition: persisted.MSD_SourceDefinition__c,
        mappingDefinition: persisted.MSD_MappingDefinition__c
      };
    }

    // Create new record - Name field should not be set as it's auto-generated
    const id = await client.createScheduleRecord(fields);
    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listSchedules", "scheduleFormOptions"]);
    this.saveLocalTimingDefinition(resolvedInstance.id, id, input.timingDefinition);
    const persisted = await client.queryScheduleById(id);
    return {
      id,
      action: "created",
      sourceDefinition: persisted.MSD_SourceDefinition__c,
      mappingDefinition: persisted.MSD_MappingDefinition__c
    };
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

    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listSchedules", "scheduleFormOptions"]);

    return { id, action: "created" };
  }

  public async duplicateScheduleWithDirectionChange(
    scheduleId: string,
    newName?: string,
    instanceId?: string
  ): Promise<{ id: string; action: "created"; warnings: string[] }> {
    const draft = await this.buildDirectionChangedScheduleDraft(scheduleId, newName, instanceId);
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);

    const id = await client.createScheduleRecord({
      Name: draft.schedule.name,
      Active__c: false,
      SourceSystem__c: draft.schedule.sourceSystem,
      TargetSystem__c: draft.schedule.targetSystem,
      ObjectName__c: draft.schedule.objectName,
      Operation__c: draft.schedule.operation,
      MSD_Connector__c: draft.schedule.connectorId,
      MSD_MappingDefinition__c: draft.schedule.mappingDefinition,
      MSD_Direction__c: draft.schedule.direction,
      MSD_SourceType__c: draft.schedule.sourceType,
      MSD_TargetType__c: draft.schedule.targetType,
      MSD_SourceDefinition__c: draft.schedule.sourceDefinition,
      MSD_TargetDefinition__c: this.mergeScheduleEnvelope(draft.schedule.targetDefinition, {
        timingDefinition: draft.schedule.timingDefinition,
        parentScheduleId: undefined,
        inheritTimingFromParent: false
      }),
      BatchSize__c: draft.schedule.batchSize,
      NextRunAt__c: draft.schedule.nextRunAt
    });

    this.copyLocalTimingDefinition(resolvedInstance.id, scheduleId, id);
    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listSchedules", "scheduleFormOptions"]);

    return { id, action: "created", warnings: draft.warnings };
  }

  public async buildDirectionChangedScheduleDraft(
    scheduleId: string,
    newName?: string,
    instanceId?: string,
    draftInput?: ScheduleMutationInput
  ): Promise<{ schedule: Omit<IntegrationSchedule, "id"> & { id?: string }; warnings: string[] }> {
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
    const persistedRecord = scheduleId ? await client.queryScheduleById(scheduleId) : undefined;
    const record = draftInput
      ? this.scheduleInputToSalesforceScheduleRecord(draftInput, persistedRecord, scheduleId)
      : persistedRecord;
    if (!record) {
      throw new Error("Scheduler-Draft konnte nicht erzeugt werden: Scheduler nicht gefunden.");
    }
    const mapping = this.buildReversedScheduleMapping(record.MSD_MappingDefinition__c || "");
    const connector = record.MSD_Connector__c
      ? await client.queryConnector(record.MSD_Connector__c)
      : undefined;
    const reversed = this.buildDirectionChangedSchedule(record, mapping, connector);
    const cloneName = newName?.trim() || `${record.Name} (${reversed.direction})`;

    return {
      schedule: {
        name: cloneName,
        active: false,
        sourceSystem: reversed.sourceSystem,
        targetSystem: reversed.targetSystem,
        objectName: reversed.objectName,
        operation: reversed.operation,
        connectorId: record.MSD_Connector__c,
        mappingDefinition: reversed.mappingDefinition,
        direction: reversed.direction,
        sourceType: reversed.sourceType,
        targetType: reversed.targetType,
        sourceDefinition: reversed.sourceDefinition,
        targetDefinition: reversed.targetDefinition,
        batchSize: record.BatchSize__c || 100,
        nextRunAt: record.NextRunAt__c,
        timingDefinition: this.extractTimingDefinition(record.MSD_TargetDefinition__c),
        parentScheduleId: undefined,
        inheritTimingFromParent: false
      },
      warnings: reversed.warnings
    };
  }

  private scheduleInputToSalesforceScheduleRecord(
    input: ScheduleMutationInput,
    fallback: SalesforceScheduleRecord | undefined,
    scheduleId: string
  ): SalesforceScheduleRecord {
    return {
      Id: scheduleId || fallback?.Id || "",
      Name: String(input.name || fallback?.Name || "Scheduler").trim() || "Scheduler",
      Active__c: input.active ?? fallback?.Active__c ?? false,
      SourceSystem__c: input.sourceSystem || fallback?.SourceSystem__c,
      TargetSystem__c: input.targetSystem || fallback?.TargetSystem__c,
      ObjectName__c: input.objectName || fallback?.ObjectName__c,
      Operation__c: input.operation || fallback?.Operation__c,
      MSD_Connector__c: input.connectorId || fallback?.MSD_Connector__c,
      MSD_MappingDefinition__c: input.mappingDefinition || fallback?.MSD_MappingDefinition__c,
      MSD_Direction__c: input.direction || fallback?.MSD_Direction__c,
      MSD_SourceType__c: input.sourceType || fallback?.MSD_SourceType__c,
      MSD_TargetType__c: input.targetType || fallback?.MSD_TargetType__c,
      MSD_SourceDefinition__c: input.sourceDefinition || fallback?.MSD_SourceDefinition__c,
      MSD_TargetDefinition__c: this.mergeScheduleEnvelope(input.targetDefinition || fallback?.MSD_TargetDefinition__c, {
        timingDefinition: input.timingDefinition || this.extractTimingDefinition(fallback?.MSD_TargetDefinition__c),
        parentScheduleId: input.parentScheduleId,
        inheritTimingFromParent: input.inheritTimingFromParent
      }),
      BatchSize__c: input.batchSize || fallback?.BatchSize__c,
      NextRunAt__c: input.nextRunAt || fallback?.NextRunAt__c,
      LastRunAt__c: input.lastRunAt || fallback?.LastRunAt__c
    };
  }

  private buildReversedScheduleMapping(mappingDefinition: string): {
    mappingDefinition: string;
    sourceFields: string[];
    targetFields: string[];
    skippedRules: number;
  } {
    const parsed = new MappingDefinitionParser().parse(String(mappingDefinition || ""));
    const reversedRules: Array<{ sourceField: string; targetField: string; targetType: MappingTargetType; transformFunction: "NONE" }> = parsed.lines
      .flatMap((line) => {
        const sourceField = String(line.sourceField || "").trim();
        const targetField = String(line.targetField || "").trim();
        if (!sourceField || !targetField || line.transform.type === "STATIC") {
          return [];
        }

        return [{
          sourceField: targetField,
          targetField: sourceField,
          targetType: line.targetType || "string",
          transformFunction: "NONE"
        }];
      });

    const sourceFields = [...new Set(reversedRules.map((rule) => rule.sourceField).filter(Boolean))];
    const targetFields = [...new Set(reversedRules.map((rule) => rule.targetField).filter(Boolean))];

    return {
      mappingDefinition: JSON.stringify(reversedRules, null, 2),
      sourceFields,
      targetFields,
      skippedRules: parsed.lines.length - reversedRules.length
    };
  }

  private buildDirectionChangedSchedule(
    record: SalesforceScheduleRecord,
    mapping: {
      mappingDefinition: string;
      sourceFields: string[];
      targetFields: string[];
      skippedRules: number;
    },
    connector?: ConnectorConfig
  ): {
    sourceSystem: string;
    targetSystem: string;
    objectName: string;
    operation: string;
    direction: string;
    sourceType: string;
    targetType: string;
    sourceDefinition: string;
    targetDefinition: string;
    mappingDefinition: string;
    warnings: string[];
  } {
    const originalSourceType = normalizeScheduleType(record.MSD_SourceType__c).toUpperCase();
    const originalTargetType = normalizeScheduleType(record.MSD_TargetType__c).toUpperCase();
    const originalDirection = String(record.MSD_Direction__c || "").trim().toLowerCase();
    const nextDirection = originalDirection === "inbound" ? "Outbound" : "Inbound";
    const warnings: string[] = [];

    if (!mapping.sourceFields.length) {
      throw new Error("Richtung kann nicht gedreht werden: Das Mapping enthaelt keine invertierbaren Feldzuordnungen.");
    }

    if (mapping.skippedRules > 0) {
      warnings.push(`${mapping.skippedRules} Mapping-Regel(n) mit STATIC oder leerem Quell-/Zielfeld wurden nicht invertiert.`);
    }

    const sourceSystem = record.TargetSystem__c || (originalTargetType === "SALESFORCE" ? "Salesforce" : "");
    const targetSystem = record.SourceSystem__c || (originalSourceType === "SALESFORCE_SOQL" ? "Salesforce" : "");
    const sourceType = originalTargetType === "SALESFORCE"
      ? "SALESFORCE_SOQL"
      : originalTargetType;
    const targetType = originalSourceType === "SALESFORCE_SOQL"
      ? "SALESFORCE"
      : originalSourceType === "MSSQL_SQL"
        ? "MSSQL"
        : originalSourceType;

    let sourceDefinition = "";
    let targetDefinition = "{}";
    let objectName = record.ObjectName__c || "";
    let operation = record.Operation__c || "";

    if (sourceType === "SALESFORCE_SOQL") {
      const targetDefinitionRecord = resolveSelectedSalesforceTargetDefinition(String(record.MSD_TargetDefinition__c || "{}"));
      const objectApiName = String(targetDefinitionRecord.objectApiName || record.ObjectName__c || "").trim();
      if (!objectApiName) {
        throw new Error("Richtung kann nicht gedreht werden: Salesforce-Zielobjekt konnte nicht aus der TargetDefinition ermittelt werden.");
      }

      sourceDefinition = this.buildSalesforceSoqlSourceDefinition(objectApiName, mapping.sourceFields);
      objectName = this.resolveMssqlTargetObjectName(connector, record.ObjectName__c);
      operation = "Upsert";
      targetDefinition = JSON.stringify({
        upsertKey: this.resolveMssqlReverseUpsertKey(record, mapping)
      }, null, 2);
    } else if (sourceType === "MSSQL" || sourceType === "MSSQL_SQL") {
      sourceDefinition = this.buildMssqlSqlSourceDefinition(connector, mapping.sourceFields);
      const sourceObjectName = this.extractSalesforceObjectName(parseQuerySourceDefinition(String(record.MSD_SourceDefinition__c || "")).queryText);
      objectName = sourceObjectName || record.ObjectName__c || "";
      operation = record.Operation__c || "Upsert";
      targetDefinition = JSON.stringify({
        objectApiName: objectName,
        operation: String(operation || "Upsert").toLowerCase(),
        externalIdField: this.resolveSalesforceReverseExternalIdField(record, mapping)
      }, null, 2);
    } else {
      throw new Error(`Richtung kann fuer TargetType ${originalTargetType || "-"} noch nicht automatisch gedreht werden.`);
    }

    return {
      sourceSystem,
      targetSystem,
      objectName,
      operation,
      direction: nextDirection,
      sourceType,
      targetType,
      sourceDefinition,
      targetDefinition,
      mappingDefinition: mapping.mappingDefinition,
      warnings
    };
  }

  private buildSalesforceSoqlSourceDefinition(objectApiName: string, fields: string[]): string {
    const selectedFields = [...new Set(["Id", ...fields])]
      .map((field) => String(field || "").trim())
      .filter((field) => /^[A-Za-z_][A-Za-z0-9_.]*$/.test(field));
    if (!selectedFields.length) {
      throw new Error("SOQL konnte nicht erzeugt werden: keine gueltigen Salesforce-Felder im Mapping.");
    }
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(objectApiName)) {
      throw new Error(`SOQL konnte nicht erzeugt werden: ungueltiger Salesforce-Objektname ${objectApiName}.`);
    }

    return [
      "SELECT",
      `  ${selectedFields.join(",\n  ")}`,
      `FROM ${objectApiName}`
    ].join("\n");
  }

  private buildMssqlSqlSourceDefinition(connector: ConnectorConfig | undefined, fields: string[]): string {
    const parameters = connector?.parameters || {};
    const schemaName = String(parameters.schema || parameters.schemaName || "dbo").trim() || "dbo";
    const tableName = String(parameters.table || parameters.tableName || "").trim();
    if (!tableName) {
      throw new Error("SQL konnte nicht erzeugt werden: Der MSSQL-Connector enthaelt keinen table/tableName Parameter.");
    }
    const quoteIdentifier = (value: string, label: string): string => {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
        throw new Error(`SQL konnte nicht erzeugt werden: ungueltiger MSSQL-Identifier fuer ${label}: ${value}.`);
      }
      return `[${value}]`;
    };
    const selectedFields = [...new Set(fields)]
      .map((field) => String(field || "").trim())
      .filter(Boolean)
      .map((field) => quoteIdentifier(field, "Feld"));

    if (!selectedFields.length) {
      throw new Error("SQL konnte nicht erzeugt werden: keine gueltigen MSSQL-Felder im Mapping.");
    }

    return [
      "SELECT",
      `  ${selectedFields.join(",\n  ")}`,
      `FROM ${quoteIdentifier(schemaName, "Schema")}.${quoteIdentifier(tableName, "Tabelle")}`
    ].join("\n");
  }

  private resolveMssqlTargetObjectName(connector: ConnectorConfig | undefined, fallback?: string): string {
    const parameters = connector?.parameters || {};
    return String(parameters.table || parameters.tableName || fallback || "MSSQL_Target").trim() || "MSSQL_Target";
  }

  private resolveMssqlReverseUpsertKey(
    record: SalesforceScheduleRecord,
    mapping: { targetFields: string[] }
  ): string {
    const resolvedTargetDefinition = resolveSelectedSalesforceTargetDefinition(String(record.MSD_TargetDefinition__c || "{}"));
    const externalIdField = String(resolvedTargetDefinition.externalIdField || "").trim().toLowerCase();
    if (externalIdField) {
      const parsed = new MappingDefinitionParser().parse(String(record.MSD_MappingDefinition__c || ""));
      const externalIdMapping = parsed.lines.find((line) => String(line.targetField || "").trim().toLowerCase() === externalIdField);
      const sourceField = String(externalIdMapping?.sourceField || "").trim();
      if (sourceField) {
        return sourceField;
      }
    }

    return mapping.targetFields[0] || "external_key";
  }

  private resolveSalesforceReverseExternalIdField(
    record: SalesforceScheduleRecord,
    mapping: { sourceFields: string[] }
  ): string {
    const parsedSource = parseQuerySourceDefinition(String(record.MSD_SourceDefinition__c || "")).queryText;
    const selectedSourceFields = this.extractSalesforceSelectedFields(parsedSource);
    const idField = selectedSourceFields.find((field) => String(field.alias || field.expression || "").trim().toLowerCase() === "id");
    if (idField) {
      return String(idField.alias || idField.expression || "Id").trim();
    }

    return mapping.sourceFields.find((field) => String(field || "").trim().toLowerCase() === "id")
      || mapping.sourceFields[0]
      || "Id";
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
    const resolvedInstance = this.resolveInstance(instanceId);
    const client = await this.createClient(resolvedInstance.id);
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
      this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listConnectors", "scheduleFormOptions"]);
      return { id: input.id, action: "updated" };
    }

    const id = await client.createConnectorRecord(fields);
    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listConnectors", "scheduleFormOptions"]);
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
    this.invalidateAdaptiveSalesforceCache(resolvedInstance.id, ["listConnectors", "listSchedules", "scheduleFormOptions"]);

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
    const analysis = await analyzeUploadedFile(fileName, fileBuffer);
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
    const parsed = await this.analyzeFileBuffer(fileName, fileBuffer);
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
      ? await Promise.all(parsed.availableSheetNames.map(async (sheetName) => {
          const sheetParsed = await this.parseMigrationSourceBuffer(fileName, fileBuffer, { sheetName });
          const fields = sheetParsed.fields;
          const recordCount = sheetParsed.recordCount;
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
        }))
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
      nextRunAt: schedule.nextRunAt,
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
    const projectRuntime = this.resolveLookupCacheRuntime(instanceId);
    if (!projectRuntime.enabled) {
      return 0;
    }

    if (projectRuntime.ttlMs > 0) {
      return projectRuntime.ttlMs;
    }

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
    const ttlMs = this.getAdaptiveSalesforceCacheTtlMs(instanceId);
    if (ttlMs <= 0) {
      return await loader();
    }

    const key = `${instanceId}:${bucket}`;
    const now = Date.now();
    const cached = this.adaptiveSalesforceCache.get(key);
    if (cached && cached.expiresAt > now) {
      return cached.value as T;
    }

    const value = await loader();
    this.adaptiveSalesforceCache.set(key, {
      value,
      expiresAt: now + ttlMs
    });
    return value;
  }

  private invalidateAdaptiveSalesforceCache(instanceId: string, buckets?: string[]): void {
    const normalizedInstanceId = String(instanceId || "").trim();
    if (!normalizedInstanceId) {
      return;
    }

    const prefix = `${normalizedInstanceId}:`;
    if (!buckets || buckets.length === 0) {
      for (const key of Array.from(this.adaptiveSalesforceCache.keys())) {
        if (key.startsWith(prefix)) {
          this.adaptiveSalesforceCache.delete(key);
        }
      }
      return;
    }

    for (const bucket of buckets) {
      this.adaptiveSalesforceCache.delete(`${normalizedInstanceId}:${bucket}`);
    }
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
  ): Promise<{ fields: Array<{ name: string; type: string; label?: string; requiredOnCreate?: boolean; nillable?: boolean; isExternalId?: boolean; createable?: boolean; updateable?: boolean; calculated?: boolean; autoNumber?: boolean; defaultedOnCreate?: boolean; referenceTo?: string[]; picklistValues?: Array<{ value: string; label: string }> }> }> {
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
          defaultedOnCreate: field.defaultedOnCreate === true,
          referenceTo: Array.isArray(field.referenceTo) ? field.referenceTo : undefined,
          picklistValues: Array.isArray(field.picklistValues) ? field.picklistValues : undefined
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

  public async refreshInstanceMetadata(
    instanceId?: string,
    options?: { objectNames?: string[]; includeAllFields?: boolean; maxFieldObjects?: number }
  ): Promise<InstanceMetadataSnapshot> {
    const resolvedInstance = this.resolveRuntimeInstance(instanceId);
    const configured = readConfiguredInstancesWithMetadata().find((item) => item.id === resolvedInstance.id);
    const projectId = String(configured?.projectId || "default-project").trim() || "default-project";
    const refreshedAt = new Date().toISOString();
    const normalizedRequestedObjects = (options?.objectNames || [])
      .map((name) => String(name || "").trim())
      .filter(Boolean);

    try {
      const client = await this.createClient(resolvedInstance.id);
      const objects = await client.listObjectMetadata();
      const objectNameSet = new Set(objects.map((object) => object.name));
      const maxFieldObjects = Math.max(1, Math.min(Number(options?.maxFieldObjects || 40), 250));
      const existingScheduleObjects = (await this.listSchedules(resolvedInstance.id))
        .map((schedule) => String(schedule.objectName || "").trim())
        .filter(Boolean);
      const priorityObjects = [
        ...normalizedRequestedObjects,
        ...existingScheduleObjects,
        ...AdminDataService.defaultSalesforceMetadataFieldObjects
      ].filter((name, index, arr) => arr.indexOf(name) === index && objectNameSet.has(name));
      const fieldObjectNames = options?.includeAllFields === true
        ? objects.slice(0, maxFieldObjects).map((object) => object.name)
        : priorityObjects.slice(0, maxFieldObjects);

      const fieldsByObject = new Map<string, SalesforceObjectFieldMetadata[]>();
      for (const objectName of fieldObjectNames) {
        try {
          fieldsByObject.set(objectName, await client.describeObjectFields(objectName, { forceRefresh: true }));
        } catch {
          fieldsByObject.set(objectName, []);
        }
      }

      const fieldCount = Array.from(fieldsByObject.values()).reduce((sum, fields) => sum + fields.length, 0);

      return await withMetadataDatabase(async (database) => {
        const snapshotResult = await database.run(
          `INSERT INTO metadata_snapshots (
            project_id, instance_id, system_type, status, refreshed_at, object_count, field_count, error_message
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [projectId, resolvedInstance.id, "salesforce", "success", refreshedAt, objects.length, fieldCount, null]
        );

        await database.run(
          `DELETE FROM metadata_objects WHERE project_id = ? AND instance_id = ? AND system_type = ?`,
          [projectId, resolvedInstance.id, "salesforce"]
        );

        for (const object of objects) {
          const fields = fieldsByObject.get(object.name);
          await database.run(
            `INSERT OR REPLACE INTO metadata_objects (
              project_id, instance_id, system_type, object_name, label, kind, queryable, field_count, refreshed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              projectId,
              resolvedInstance.id,
              "salesforce",
              object.name,
              object.label || object.name,
              object.name.endsWith("__c") ? "customObject" : "standardObject",
              1,
              fields ? fields.length : 0,
              refreshedAt
            ]
          );
        }

        for (const objectName of fieldObjectNames) {
          await database.run(
            `DELETE FROM metadata_fields
             WHERE project_id = ? AND instance_id = ? AND system_type = ? AND object_name = ?`,
            [projectId, resolvedInstance.id, "salesforce", objectName]
          );

          for (const field of fieldsByObject.get(objectName) || []) {
            await database.run(
              `INSERT OR REPLACE INTO metadata_fields (
                project_id, instance_id, system_type, object_name, field_name, label, type,
                required, external_id, createable, updateable, reference_to_json, picklist_values_json, refreshed_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                projectId,
                resolvedInstance.id,
                "salesforce",
                objectName,
                field.name,
                field.label || field.name,
                field.type || "unknown",
                field.requiredOnCreate ? 1 : 0,
                field.isExternalId ? 1 : 0,
                field.createable ? 1 : 0,
                field.updateable ? 1 : 0,
                field.referenceTo?.length ? JSON.stringify(field.referenceTo) : null,
                field.picklistValues?.length ? JSON.stringify(field.picklistValues) : null,
                refreshedAt
              ]
            );
          }
        }

        return {
          id: snapshotResult.lastID,
          projectId,
          instanceId: resolvedInstance.id,
          systemType: "salesforce",
          status: "success",
          refreshedAt,
          objectCount: objects.length,
          fieldCount
        };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return await withMetadataDatabase(async (database) => {
        const snapshotResult = await database.run(
          `INSERT INTO metadata_snapshots (
            project_id, instance_id, system_type, status, refreshed_at, object_count, field_count, error_message
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [projectId, resolvedInstance.id, "salesforce", "error", refreshedAt, 0, 0, message]
        );

        return {
          id: snapshotResult.lastID,
          projectId,
          instanceId: resolvedInstance.id,
          systemType: "salesforce",
          status: "error",
          refreshedAt,
          objectCount: 0,
          fieldCount: 0,
          errorMessage: message
        };
      });
    }
  }

  public async getInstanceMetadataContext(instanceId?: string): Promise<InstanceMetadataContext> {
    const resolvedInstance = this.resolveRuntimeInstance(instanceId);
    const configured = readConfiguredInstancesWithMetadata().find((item) => item.id === resolvedInstance.id);
    const projectId = String(configured?.projectId || "default-project").trim() || "default-project";

    return await withMetadataDatabase(async (database) => {
      let snapshotRow = await database.get<Record<string, unknown>>(
        `SELECT *
         FROM metadata_snapshots
         WHERE project_id = ? AND instance_id = ? AND system_type = ?
         ORDER BY id DESC
         LIMIT 1`,
        [projectId, resolvedInstance.id, "salesforce"]
      );

      if (!snapshotRow) {
        snapshotRow = await database.get<Record<string, unknown>>(
          `SELECT *
           FROM metadata_snapshots
           WHERE instance_id = ? AND system_type = ?
           ORDER BY CASE WHEN status = 'success' THEN 0 ELSE 1 END, id DESC
           LIMIT 1`,
          [resolvedInstance.id, "salesforce"]
        );
      }

      if (!snapshotRow && !instanceId) {
        snapshotRow = await database.get<Record<string, unknown>>(
          `SELECT *
           FROM metadata_snapshots
           WHERE system_type = ?
           ORDER BY CASE WHEN status = 'success' THEN 0 ELSE 1 END, id DESC
           LIMIT 1`,
          ["salesforce"]
        );
      }

      const metadataProjectId = String(snapshotRow?.project_id || projectId).trim() || projectId;
      const metadataInstanceId = String(snapshotRow?.instance_id || resolvedInstance.id).trim() || resolvedInstance.id;

      const objectRows = await database.all<Record<string, unknown>>(
        `SELECT *
         FROM metadata_objects
         WHERE project_id = ? AND instance_id = ? AND system_type = ?
         ORDER BY object_name ASC`,
        [metadataProjectId, metadataInstanceId, "salesforce"]
      );

      const fieldRows = await database.all<Record<string, unknown>>(
        `SELECT *
         FROM metadata_fields
         WHERE project_id = ? AND instance_id = ? AND system_type = ?
         ORDER BY object_name ASC, field_name ASC`,
        [metadataProjectId, metadataInstanceId, "salesforce"]
      );

      const fieldsByObject: Record<string, PersistedMetadataField[]> = {};
      for (const row of fieldRows) {
        const objectName = String(row.object_name || "").trim();
        const fieldName = String(row.field_name || "").trim();
        if (!objectName || !fieldName) {
          continue;
        }

        if (!fieldsByObject[objectName]) {
          fieldsByObject[objectName] = [];
        }

        fieldsByObject[objectName].push({
          objectName,
          name: fieldName,
          label: String(row.label || fieldName).trim(),
          type: String(row.type || "unknown").trim(),
          required: Number(row.required || 0) === 1,
          externalId: Number(row.external_id || 0) === 1,
          createable: Number(row.createable || 0) === 1,
          updateable: Number(row.updateable || 0) === 1,
          referenceTo: parseJsonArrayField<string>(row.reference_to_json),
          picklistValues: parseJsonArrayField<{ value: string; label: string }>(row.picklist_values_json)
        });
      }

      return {
        snapshot: snapshotRow ? normalizeMetadataSnapshot(snapshotRow) : undefined,
        objects: objectRows
          .map((row) => {
            const objectName = String(row.object_name || "").trim();
            return {
              systemType: "salesforce" as const,
              objectName,
              label: String(row.label || objectName).trim(),
              kind: String(row.kind || "").trim() || undefined,
              queryable: Number(row.queryable || 0) === 1,
              fieldCount: Number(row.field_count || 0)
            };
          })
          .filter((object) => object.objectName),
        fieldsByObject
      };
    });
  }

  public getSage100DocumentationContext(prompt?: string, limit = 8): Sage100DocumentationContext | undefined {
    if (!fs.existsSync(SAGE100_DB_DOC_INDEX_FILE)) {
      return undefined;
    }

    try {
      const raw = fs.readFileSync(SAGE100_DB_DOC_INDEX_FILE, "utf8").trim();
      if (!raw) {
        return undefined;
      }

      const parsed = JSON.parse(raw) as {
        sourceFile?: string;
        generatedAt?: string;
        pageCount?: number;
        tableCount?: number;
        tables?: Sage100DocumentationTable[];
      };
      const tables = Array.isArray(parsed.tables) ? parsed.tables : [];
      const normalizedPrompt = this.normalizeDocumentationMatchText(prompt || "");
      const promptTokens = normalizedPrompt
        .split(" ")
        .filter((token) => token.length >= 3);
      const domainHints = this.getSage100DomainHints(normalizedPrompt);

      const scoredTables = tables
        .map((table) => {
          const tableName = String(table.name || "").trim();
          const searchable = [
            tableName,
            ...(table.fields || []).slice(0, 40).flatMap((field) => [field.name, field.description || ""])
          ].join(" ");
          const normalizedSearchable = this.normalizeDocumentationMatchText(searchable);
          let score = 0;

          const normalizedTableName = this.normalizeDocumentationMatchText(tableName);
          for (const hint of domainHints) {
            if (hint.tableName && normalizedTableName === hint.tableName) {
              score += hint.score;
            } else if (hint.tableContains && normalizedTableName.includes(hint.tableContains)) {
              score += hint.score;
            }
          }

          for (const token of promptTokens) {
            if (normalizedSearchable.includes(token)) {
              score += 1;
            }
          }

          return {
            ...table,
            fields: (table.fields || []).slice(0, 40),
            score
          };
        })
        .filter((table) => Number(table.score || 0) > 0)
        .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
        .slice(0, Math.max(1, Math.min(limit, 20)));

      return {
        sourceFile: parsed.sourceFile,
        indexFile: SAGE100_DB_DOC_INDEX_FILE,
        generatedAt: parsed.generatedAt,
        pageCount: parsed.pageCount,
        tableCount: Number(parsed.tableCount || tables.length),
        matchedTables: scoredTables
      };
    } catch {
      return undefined;
    }
  }

  private normalizeDocumentationMatchText(value: string): string {
    return String(value || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9_]+/g, " ")
      .trim();
  }

  private getSage100DomainHints(normalizedPrompt: string): Array<{ tableName?: string; tableContains?: string; score: number }> {
    const hints: Array<{ tableName?: string; tableContains?: string; score: number }> = [];
    if (/\b(order|orders|auftrag|auftraege|bestellung|beleg|erp)\b/.test(normalizedPrompt)) {
      hints.push(
        { tableName: "khkvkbelege", score: 50 },
        { tableName: "khkvkbelegepositionen", score: 35 },
        { tableContains: "khkvkbelege", score: 8 }
      );
    }
    if (/\b(opportunity|opportunities|chance|deal)\b/.test(normalizedPrompt)) {
      hints.push(
        { tableName: "khkvkbelege", score: 20 },
        { tableContains: "khkprojekte", score: 3 }
      );
    }
    if (/\b(account|accounts|kunde|kunden|adresse|adressen)\b/.test(normalizedPrompt)) {
      hints.push(
        { tableName: "khkadressen", score: 60 },
        { tableContains: "khkadressen", score: 8 }
      );
    }
    if (/\b(contact|kontakt|kontakte|ansprechpartner)\b/.test(normalizedPrompt)) {
      hints.push({ tableContains: "khkansprechpartner", score: 8 });
    }
    if (/\b(product|produkt|artikel|pricebook|preis)\b/.test(normalizedPrompt)) {
      hints.push(
        { tableContains: "khkartikel", score: 8 },
        { tableContains: "khkartikelvarianten", score: 5 }
      );
    }
    return hints;
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
    options?: {
      picklistValues?: string[];
      length?: number;
      precision?: number;
      scale?: number;
      externalId?: boolean;
      unique?: boolean;
    },
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
    if (metadata.type === "Text" && Number.isFinite(Number(options?.length))) {
      metadata.length = Math.max(1, Math.min(255, Math.trunc(Number(options?.length))));
    }
    if (["Number", "Currency", "Percent"].includes(String(metadata.type || ""))) {
      if (Number.isFinite(Number(options?.precision))) {
        metadata.precision = Math.max(1, Math.min(18, Math.trunc(Number(options?.precision))));
      }
      if (Number.isFinite(Number(options?.scale))) {
        metadata.scale = Math.max(0, Math.min(Number(metadata.precision || 18) - 1, Math.trunc(Number(options?.scale))));
      }
    }
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

  public async analyzeFileBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string; sheetName?: string }
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
  }> {
    const parsed = await this.parseMigrationSourceBuffer(fileName, fileBuffer, options);
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

    const parsed = await this.parseMigrationSourceBuffer(safeFileName, fileBuffer, options);
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
          const detectedAnalysis = await analyzeUploadedFile(fileName, fileBuffer);
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
    const analysis = await this.analyzeFileBuffer(fileName, fileBuffer, {
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

  private async parseMigrationSourceBuffer(
    fileName: string,
    fileBuffer: Buffer,
    options?: { charset?: string; delimiter?: string; textQualifier?: string; sheetName?: string }
  ): Promise<{
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
  }> {
    const analysis = await analyzeUploadedFile(fileName, fileBuffer);
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
      const excelPayload = await parseExcelBuffer(fileBuffer, selectedSheetName);
      allRows = excelPayload.rows.map((row) => ({ ...(row || {}) }));
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
    const parsed = await this.parseMigrationSourceBuffer(fileName, fileBuffer, {
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
      const lookupResolver = await this.createMigrationLookupResolver(
        client,
        mappingLines,
        sourceRows.map((entry) => entry.row),
        instanceId ?? migration.instanceId
      );
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
      salesforceLogin: undefined
    };
  }

  private resolveProjectContextForMigration(input: {
    projectId?: string;
    instanceId?: string;
    existing?: MigrationConfig;
  }): { projectId: string; instanceId: string } {
    let projectId = String(input.projectId || input.existing?.projectId || "").trim();
    const preferredInstanceId = String(input.instanceId || input.existing?.instanceId || "").trim();

    if (!projectId && preferredInstanceId && !preferredInstanceId.startsWith("migration:")) {
      const configured = readConfiguredInstancesWithMetadata().find((item) => item.id === preferredInstanceId);
      projectId = String(configured?.projectId || "").trim();
    }

    if (!projectId) {
      throw new Error("projectId ist erforderlich");
    }

    const projects = readLocalProjects();
    if (!projects.some((item) => item.id === projectId)) {
      throw new Error(`Projekt ${projectId} nicht gefunden`);
    }

    const projectInstances = readConfiguredInstancesWithMetadata()
      .filter((item) => String(item.projectId || "default-project").trim() === projectId);

    if (!projectInstances.length) {
      throw new Error(`Projekt ${projectId} hat keine zugeordnete Salesforce-Instanz`);
    }

    if (preferredInstanceId && !preferredInstanceId.startsWith("migration:")) {
      const belongsToProject = projectInstances.some((item) => item.id === preferredInstanceId);
      if (!belongsToProject) {
        throw new Error(`Instanz ${preferredInstanceId} gehoert nicht zum Projekt ${projectId}`);
      }
      return { projectId, instanceId: preferredInstanceId };
    }

    const production = projectInstances.find((item) => item.role === "production");
    return { projectId, instanceId: (production || projectInstances[0]).id };
  }

  public saveMigration(input: Omit<MigrationConfig, "createdAt" | "updatedAt"> & { createdAt?: string; updatedAt?: string }): MigrationConfig {
    const migrations = this.readMigrationsStore();
    const now = new Date().toISOString();
    const existing = migrations.find((m) => m.id === input.id);

    if (input.salesforceLogin && String(input.salesforceLogin.loginUrl || "").trim()) {
      throw new Error("Eigenstaendige Salesforce-Anbindung fuer Migrationen ist nicht mehr zulaessig. Nutze die Projektzuordnung.");
    }

    const resolvedContext = this.resolveProjectContextForMigration({
      projectId: input.projectId,
      instanceId: input.instanceId,
      existing
    });

    const saved: MigrationConfig = {
      ...input,
      batchSize: this.resolveMigrationBatchSize(input.batchSize),
      projectId: resolvedContext.projectId,
      instanceId: resolvedContext.instanceId,
      salesforceLogin: undefined,
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
