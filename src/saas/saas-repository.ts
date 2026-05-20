import crypto from "node:crypto";
import { Pool } from "pg";
import {
  SaasAgentHeartbeat,
  SaasAgentRegistrationRequest,
  SaasAgentRegistrationResponse,
  SaasRunReport
} from "../types/saas-control-plane";

export interface UserRecord {
  id: string;
  email: string;
  displayName: string;
  passwordHash: string;
  isSystemAdmin: boolean;
  status: string;
}

export interface TenantRecord {
  id: string;
  tenantKey: string;
  name: string;
  status: string;
  createdAt: string;
}

export interface TenantDetails extends TenantRecord {
  license: LicenseDetail | null;
  projectCount: number;
  agentCount: number;
  onlineAgentCount: number;
  members: Array<{ userId: string; email: string; displayName: string; role: string }>;
  projects: Array<{ id: string; projectKey: string; name: string; mode: string; status: string }>;
}

export interface LicenseDetail {
  id: string;
  contractReference: string | null;
  plan: string;
  status: string;
  validFrom: string | null;
  validUntil: string | null;
  maxConnectors: number;
  maxSchedulers: number;
  maxRecordsPerMonth: number;
  featureAi: boolean;
  featureMigration: boolean;
  featureCustomConnector: boolean;
  featureCustomScheduler: boolean;
}

export interface LicenseInput {
  contractReference?: string;
  plan: string;
  status: "trial" | "active" | "expired" | "suspended";
  validFrom?: string | null;
  validUntil?: string | null;
  maxConnectors: number;
  maxSchedulers: number;
  maxRecordsPerMonth: number;
  featureAi: boolean;
  featureMigration: boolean;
  featureCustomConnector: boolean;
  featureCustomScheduler: boolean;
}

export interface CustomerDashboard {
  tenant: TenantRecord;
  license: LicenseDetail | null;
  stats: {
    agentCount: number;
    onlineAgentCount: number;
    runs24h: number;
    failed24h: number;
  };
  agents: Array<{
    agentId: string;
    name: string;
    projectKey: string;
    mode: string;
    status: string;
    agentVersion: string | null;
    lastHeartbeatAt: string | null;
  }>;
  recentRuns: Array<{
    runId: string;
    schedulerKey: string;
    projectKey: string;
    direction: string;
    status: string;
    readRecords: number;
    writtenRecords: number;
    failedRecords: number;
    startedAt: string;
    finishedAt: string | null;
  }>;
}

export interface RunDetail {
  run: {
    runId: string; schedulerKey: string; projectKey: string; direction: string;
    status: string; readRecords: number; writtenRecords: number; failedRecords: number;
    startedAt: string; finishedAt: string | null; errorCategory: string | null; errorMessage: string | null;
  };
  logs: Array<{ id: string; level: string; code: string | null; message: string; occurredAt: string }>;
  failedRecords: Array<{ id: string; recordKey: string | null; status: string | null; errorCode: string | null; message: string | null; createdAt: string }>;
}

export interface ConnectorConfigRecord {
  id: string;
  projectId: string;
  projectKey: string;
  connectorKey: string;
  type: string;
  displayName: string;
  metadataJson: unknown;
  secretPolicy: string;
  status: string;
  createdAt: string;
}

export interface ConnectorInput {
  connectorKey: string;
  type: string;
  displayName: string;
  metadataJson?: unknown;
  secretPolicy?: string;
}

export interface SchedulerConfigRecord {
  id: string;
  projectId: string;
  projectKey: string;
  schedulerKey: string;
  name: string;
  direction: string;
  active: boolean;
  scheduleExpression: string | null;
  connectorKey: string | null;
  objectName: string | null;
  configJson: unknown;
  status: string;
  createdAt: string;
}

export interface SchedulerInput {
  schedulerKey: string;
  name: string;
  direction: string;
  active: boolean;
  scheduleExpression?: string | null;
  connectorKey?: string | null;
  objectName?: string | null;
  configJson?: unknown;
}

export interface RegistrationTokenInfo {
  id: string;
  projectKey: string;
  projectName: string;
  token: string | null;
  status: string;
  expiresAt: string;
  createdAt: string;
}

export interface AgentRunItem {
  runId: string;
  schedulerKey: string;
  schedulerName: string | null;
  connectorKey: string | null;
  connectorName: string | null;
  direction: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  readRecords: number;
  writtenRecords: number;
  failedRecords: number;
  errorMessage: string | null;
}

export interface AgentLogEvent {
  id: string;
  runId: string;
  schedulerKey: string | null;
  connectorKey: string | null;
  level: string;
  code: string | null;
  message: string;
  occurredAt: string;
}

export interface AgentFailedRecord {
  id: string;
  recordKey: string | null;
  errorCode: string | null;
  message: string | null;
  createdAt: string;
}

export interface AgentRecordsBucketScheduler {
  schedulerKey: string;
  schedulerName: string | null;
  connectorKey: string | null;
  connectorName: string | null;
  total: number;
  succeeded: number;
  failed: number;
}

export interface AgentRecordsBucket {
  label: string;
  periodStart: string;
  periodEnd: string;
  total: number;
  succeeded: number;
  failed: number;
  schedulerBreakdown: AgentRecordsBucketScheduler[];
}

export interface AgentRecordsSummary {
  range: string;
  buckets: AgentRecordsBucket[];
  schedulerKeys: string[];
}

export interface AgentLogBucket {
  label: string;
  periodStart: string;
  periodEnd: string;
  total: number;
  errors: number;
  schedulerErrors: Record<string, number>;
}

export interface AgentLogSummary {
  range: string;
  buckets: AgentLogBucket[];
  schedulerKeys: string[];
}

export interface SaasRepository {
  close(): Promise<void>;
  checkHealth(): Promise<void>;
  getPortalSnapshot(): Promise<SaasPortalSnapshot>;
  claimAgentRegistration(request: SaasAgentRegistrationRequest): Promise<SaasAgentRegistrationResponse>;
  acceptAgentHeartbeat(heartbeat: SaasAgentHeartbeat, bearerToken: string): Promise<void>;
  recordSchedulerRun(run: SaasRunReport, bearerToken: string): Promise<string>;
  // Auth
  findUserByEmail(email: string): Promise<UserRecord | null>;
  createUser(input: { email: string; displayName: string; passwordHash: string; isSystemAdmin: boolean }): Promise<string>;
  ensureSystemAdmin(email: string, passwordHash: string): Promise<void>;
  updateUserLastLogin(userId: string): Promise<void>;
  // Tenant management
  listAllTenants(): Promise<TenantRecord[]>;
  getTenantDetails(tenantId: string): Promise<TenantDetails | null>;
  createTenant(input: { tenantKey: string; name: string }): Promise<string>;
  updateTenantStatus(tenantId: string, status: string): Promise<void>;
  // User / membership
  addTenantMember(tenantId: string, userId: string, role: string): Promise<void>;
  getUserTenantMembership(userId: string): Promise<{ tenantId: string; tenantKey: string; tenantName: string; role: string } | null>;
  // License
  upsertLicense(tenantId: string, input: LicenseInput): Promise<void>;
  // Projects
  createProject(input: { tenantId: string; projectKey: string; name: string; mode: string; timezone: string }): Promise<string>;
  // Registration tokens
  createRegistrationToken(projectId: string, tenantId: string): Promise<string>;
  listProjectRegistrationTokens(projectId: string): Promise<RegistrationTokenInfo[]>;
  // Customer data
  getCustomerDashboard(tenantId: string): Promise<CustomerDashboard>;
  // Config bundle
  getActiveConfigBundle(agentId: string, bearerToken: string): Promise<{ version: string; config: unknown } | null>;
  // User list
  listAllUsers(): Promise<Array<{ id: string; email: string; displayName: string; isSystemAdmin: boolean; status: string }>>;
  findTenantByKey(tenantKey: string): Promise<{ id: string } | null>;
  listTenantRegistrationTokens(tenantId: string): Promise<RegistrationTokenInfo[]>;
  // Run detail
  getRunDetail(runId: string, tenantId: string): Promise<RunDetail | null>;
  // Connectors
  listTenantConnectors(tenantId: string): Promise<ConnectorConfigRecord[]>;
  upsertConnector(tenantId: string, projectId: string, input: ConnectorInput): Promise<void>;
  deleteConnector(tenantId: string, connectorId: string): Promise<void>;
  // Schedulers
  listTenantSchedulers(tenantId: string): Promise<SchedulerConfigRecord[]>;
  upsertScheduler(tenantId: string, projectId: string, input: SchedulerInput): Promise<void>;
  deleteScheduler(tenantId: string, schedulerId: string): Promise<void>;
  // Agent dashboard reads
  getAgentRuns(agentId: string, bearerToken: string, limit: number): Promise<AgentRunItem[]>;
  getAgentRunLogEvents(agentId: string, runId: string, bearerToken: string, limit: number): Promise<AgentLogEvent[]>;
  getAgentRunFailedRecords(agentId: string, runId: string, bearerToken: string): Promise<AgentFailedRecord[]>;
  getAgentRecordsSummary(agentId: string, bearerToken: string, range: "day" | "month" | "year"): Promise<AgentRecordsSummary>;
  getAgentLogSummary(agentId: string, bearerToken: string, range: "last_hour" | "last_24h" | "last_30d"): Promise<AgentLogSummary>;
  getAgentLogsByRange(agentId: string, bearerToken: string, start: string, end: string, type: "all" | "error", limit: number, schedulerKey?: string): Promise<AgentLogEvent[]>;
}

export interface SaasRepositoryOptions {
  databaseUrl: string;
  agentTokenPepper: string;
}

interface AgentAuthRecord {
  tenant_id: string;
  project_id: string;
  tenant_key: string;
  project_key: string;
}

interface RegistrationRecord {
  token_id: string;
  tenant_id: string;
  tenant_key: string;
  tenant_name: string;
  tenant_status: "active" | "suspended" | "deleted";
  project_id: string;
  project_key: string;
  project_name: string;
  project_mode: "legacy" | "hybrid" | "saas";
  default_timezone: string;
}

interface LicenseRecord {
  id: string;
  contract_reference: string | null;
  status: "trial" | "active" | "expired" | "suspended";
  plan: string;
  valid_from: Date | null;
  valid_until: Date | null;
  max_connectors: number;
  max_schedulers: number;
  max_records_per_month: string | number;
  feature_ai: boolean;
  feature_migration: boolean;
  feature_custom_connector: boolean;
  feature_custom_scheduler: boolean;
}

export interface SaasPortalSnapshot {
  generatedAt: string;
  totals: {
    tenants: number;
    projects: number;
    agents: number;
    onlineAgents: number;
    schedulerRuns24h: number;
    failedRuns24h: number;
  };
  tenants: Array<{
    tenantId: string;
    tenantKey: string;
    name: string;
    status: string;
    plan: string;
    projectCount: number;
    agentCount: number;
    onlineAgentCount: number;
    lastHeartbeatAt?: string;
  }>;
  projects: Array<{
    projectId: string;
    tenantKey: string;
    projectKey: string;
    name: string;
    mode: string;
    status: string;
    agentCount: number;
    onlineAgentCount: number;
    lastHeartbeatAt?: string;
  }>;
  recentAgents: Array<{
    agentId: string;
    tenantKey: string;
    projectKey: string;
    name: string;
    status: string;
    mode: string;
    agentVersion?: string;
    lastHeartbeatAt?: string;
  }>;
  recentRuns: Array<{
    runId: string;
    tenantKey: string;
    projectKey: string;
    schedulerKey: string;
    direction: string;
    status: string;
    readRecords: number;
    writtenRecords: number;
    failedRecords: number;
    startedAt: string;
    finishedAt?: string;
  }>;
}

function hashAgentToken(token: string, pepper: string): string {
  return crypto.createHash("sha256").update(`${pepper}:${token}`).digest("hex");
}

function createAgentToken(): string {
  return `aat_${crypto.randomBytes(32).toString("base64url")}`;
}

function asJson(value: unknown): string {
  return JSON.stringify(value ?? {});
}

function generateTimeBuckets(range: "day" | "month" | "year"): Array<{ label: string; start: Date; end: Date }> {
  const now = new Date();
  const buckets: Array<{ label: string; start: Date; end: Date }> = [];
  if (range === "day") {
    for (let i = 23; i >= 0; i--) {
      const start = new Date(now);
      start.setUTCMinutes(0, 0, 0);
      start.setUTCHours(start.getUTCHours() - i);
      const end = new Date(start);
      end.setUTCHours(end.getUTCHours() + 1);
      buckets.push({ label: `${String(start.getUTCHours()).padStart(2, "0")}:00`, start, end });
    }
  } else if (range === "month") {
    for (let i = 29; i >= 0; i--) {
      const start = new Date(now);
      start.setUTCHours(0, 0, 0, 0);
      start.setUTCDate(start.getUTCDate() - i);
      const end = new Date(start);
      end.setUTCDate(end.getUTCDate() + 1);
      buckets.push({ label: `${String(start.getUTCDate()).padStart(2, "0")}.${String(start.getUTCMonth() + 1).padStart(2, "0")}`, start, end });
    }
  } else {
    for (let i = 11; i >= 0; i--) {
      const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - i, 1));
      const end = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth() + 1, 1));
      buckets.push({ label: `${String(start.getUTCMonth() + 1).padStart(2, "0")}/${String(start.getUTCFullYear()).slice(-2)}`, start, end });
    }
  }
  return buckets;
}

function generateLogBuckets(range: "last_hour" | "last_24h" | "last_30d"): Array<{ label: string; start: Date; end: Date }> {
  const now = new Date();
  const buckets: Array<{ label: string; start: Date; end: Date }> = [];
  if (range === "last_hour") {
    for (let i = 11; i >= 0; i--) {
      const start = new Date(now);
      start.setUTCSeconds(0, 0);
      start.setUTCMinutes(Math.floor(start.getUTCMinutes() / 5) * 5 - i * 5);
      const end = new Date(start);
      end.setUTCMinutes(end.getUTCMinutes() + 5);
      buckets.push({ label: `${String(start.getUTCHours()).padStart(2, "0")}:${String(start.getUTCMinutes()).padStart(2, "0")}`, start, end });
    }
  } else if (range === "last_24h") {
    for (let i = 23; i >= 0; i--) {
      const start = new Date(now);
      start.setUTCMinutes(0, 0, 0);
      start.setUTCHours(start.getUTCHours() - i);
      const end = new Date(start);
      end.setUTCHours(end.getUTCHours() + 1);
      buckets.push({ label: `${String(start.getUTCHours()).padStart(2, "0")}:00`, start, end });
    }
  } else {
    for (let i = 29; i >= 0; i--) {
      const start = new Date(now);
      start.setUTCHours(0, 0, 0, 0);
      start.setUTCDate(start.getUTCDate() - i);
      const end = new Date(start);
      end.setUTCDate(end.getUTCDate() + 1);
      buckets.push({ label: `${String(start.getUTCDate()).padStart(2, "0")}.${String(start.getUTCMonth() + 1).padStart(2, "0")}`, start, end });
    }
  }
  return buckets;
}

export function createSaasRepository(options: SaasRepositoryOptions): SaasRepository {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    max: 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000
  });

  const resolveAgentProject = async (agentId: string, bearerToken: string): Promise<{ tenantId: string; projectId: string }> => {
    const credentialHash = hashAgentToken(bearerToken, options.agentTokenPepper);
    const result = await pool.query<{ tenant_id: string; project_id: string }>(
      `select a.tenant_id, a.project_id
       from agent_credentials c
       join agents a on a.id = c.agent_id
       where c.agent_id = $1 and c.credential_hash = $2 and c.status = 'active'
         and a.status <> 'revoked' and (c.expires_at is null or c.expires_at > now())
       limit 1`,
      [agentId, credentialHash]
    );
    const row = result.rows[0];
    if (!row) throw new Error("Invalid agent credential");
    return { tenantId: row.tenant_id, projectId: row.project_id };
  };

  return {
    async close(): Promise<void> {
      await pool.end();
    },

    async checkHealth(): Promise<void> {
      await pool.query("select 1");
    },

    async getPortalSnapshot(): Promise<SaasPortalSnapshot> {
      const [totalsResult, tenantsResult, projectsResult, agentsResult, runsResult] = await Promise.all([
        pool.query<{
          tenants: string;
          projects: string;
          agents: string;
          online_agents: string;
          scheduler_runs_24h: string;
          failed_runs_24h: string;
        }>(
          `
            select
              (select count(*) from tenants where status <> 'deleted') as tenants,
              (select count(*) from projects where status <> 'deleted') as projects,
              (select count(*) from agents where status <> 'revoked') as agents,
              (select count(*) from agents where status = 'online' and last_heartbeat_at > now() - interval '15 minutes') as online_agents,
              (select count(*) from scheduler_runs where started_at > now() - interval '24 hours') as scheduler_runs_24h,
              (select count(*) from scheduler_runs where started_at > now() - interval '24 hours' and status in ('failed', 'partial')) as failed_runs_24h
          `
        ),
        pool.query<{
          tenant_id: string;
          tenant_key: string;
          name: string;
          status: string;
          plan: string | null;
          project_count: string;
          agent_count: string;
          online_agent_count: string;
          last_heartbeat_at: Date | null;
        }>(
          `
            select
              t.id as tenant_id,
              t.tenant_key,
              t.name,
              t.status,
              coalesce(l.plan, 'unlicensed') as plan,
              count(distinct p.id) as project_count,
              count(distinct a.id) as agent_count,
              count(distinct a.id) filter (where a.status = 'online' and a.last_heartbeat_at > now() - interval '15 minutes') as online_agent_count,
              max(a.last_heartbeat_at) as last_heartbeat_at
            from tenants t
            left join projects p on p.tenant_id = t.id and p.status <> 'deleted'
            left join agents a on a.tenant_id = t.id and a.status <> 'revoked'
            left join lateral (
              select plan
              from licenses
              where tenant_id = t.id
              order by created_at desc
              limit 1
            ) l on true
            where t.status <> 'deleted'
            group by t.id, t.tenant_key, t.name, t.status, l.plan
            order by t.created_at desc
            limit 50
          `
        ),
        pool.query<{
          project_id: string;
          tenant_key: string;
          project_key: string;
          name: string;
          mode: string;
          status: string;
          agent_count: string;
          online_agent_count: string;
          last_heartbeat_at: Date | null;
        }>(
          `
            select
              p.id as project_id,
              t.tenant_key,
              p.project_key,
              p.name,
              p.mode,
              p.status,
              count(distinct a.id) as agent_count,
              count(distinct a.id) filter (where a.status = 'online' and a.last_heartbeat_at > now() - interval '15 minutes') as online_agent_count,
              max(a.last_heartbeat_at) as last_heartbeat_at
            from projects p
            join tenants t on t.id = p.tenant_id
            left join agents a on a.project_id = p.id and a.status <> 'revoked'
            where p.status <> 'deleted'
            group by p.id, t.tenant_key, p.project_key, p.name, p.mode, p.status
            order by p.updated_at desc
            limit 100
          `
        ),
        pool.query<{
          agent_id: string;
          tenant_key: string;
          project_key: string;
          name: string;
          status: string;
          mode: string;
          agent_version: string | null;
          last_heartbeat_at: Date | null;
        }>(
          `
            select
              a.id as agent_id,
              t.tenant_key,
              p.project_key,
              a.name,
              a.status,
              a.mode,
              a.agent_version,
              a.last_heartbeat_at
            from agents a
            join tenants t on t.id = a.tenant_id
            join projects p on p.id = a.project_id
            where a.status <> 'revoked'
            order by a.last_heartbeat_at desc nulls last, a.updated_at desc
            limit 25
          `
        ),
        pool.query<{
          run_id: string;
          tenant_key: string;
          project_key: string;
          scheduler_key: string;
          direction: string;
          status: string;
          read_records: string;
          written_records: string;
          failed_records: string;
          started_at: Date;
          finished_at: Date | null;
        }>(
          `
            select
              r.id as run_id,
              t.tenant_key,
              p.project_key,
              r.scheduler_key,
              r.direction,
              r.status,
              r.read_records,
              r.written_records,
              r.failed_records,
              r.started_at,
              r.finished_at
            from scheduler_runs r
            join tenants t on t.id = r.tenant_id
            join projects p on p.id = r.project_id
            order by r.started_at desc
            limit 50
          `
        )
      ]);

      const totals = totalsResult.rows[0];
      return {
        generatedAt: new Date().toISOString(),
        totals: {
          tenants: Number(totals?.tenants || 0),
          projects: Number(totals?.projects || 0),
          agents: Number(totals?.agents || 0),
          onlineAgents: Number(totals?.online_agents || 0),
          schedulerRuns24h: Number(totals?.scheduler_runs_24h || 0),
          failedRuns24h: Number(totals?.failed_runs_24h || 0)
        },
        tenants: tenantsResult.rows.map((row) => ({
          tenantId: row.tenant_id,
          tenantKey: row.tenant_key,
          name: row.name,
          status: row.status,
          plan: row.plan || "unlicensed",
          projectCount: Number(row.project_count || 0),
          agentCount: Number(row.agent_count || 0),
          onlineAgentCount: Number(row.online_agent_count || 0),
          lastHeartbeatAt: row.last_heartbeat_at?.toISOString()
        })),
        projects: projectsResult.rows.map((row) => ({
          projectId: row.project_id,
          tenantKey: row.tenant_key,
          projectKey: row.project_key,
          name: row.name,
          mode: row.mode,
          status: row.status,
          agentCount: Number(row.agent_count || 0),
          onlineAgentCount: Number(row.online_agent_count || 0),
          lastHeartbeatAt: row.last_heartbeat_at?.toISOString()
        })),
        recentAgents: agentsResult.rows.map((row) => ({
          agentId: row.agent_id,
          tenantKey: row.tenant_key,
          projectKey: row.project_key,
          name: row.name,
          status: row.status,
          mode: row.mode,
          agentVersion: row.agent_version || undefined,
          lastHeartbeatAt: row.last_heartbeat_at?.toISOString()
        })),
        recentRuns: runsResult.rows.map((row) => ({
          runId: row.run_id,
          tenantKey: row.tenant_key,
          projectKey: row.project_key,
          schedulerKey: row.scheduler_key,
          direction: row.direction,
          status: row.status,
          readRecords: Number(row.read_records || 0),
          writtenRecords: Number(row.written_records || 0),
          failedRecords: Number(row.failed_records || 0),
          startedAt: row.started_at.toISOString(),
          finishedAt: row.finished_at?.toISOString()
        }))
      };
    },

    async claimAgentRegistration(request: SaasAgentRegistrationRequest): Promise<SaasAgentRegistrationResponse> {
      const registrationHash = hashAgentToken(request.registrationToken, options.agentTokenPepper);
      const client = await pool.connect();

      try {
        await client.query("begin");
        const registrationResult = await client.query<RegistrationRecord>(
          `
            select
              rt.id as token_id,
              t.id as tenant_id,
              t.tenant_key,
              t.name as tenant_name,
              t.status as tenant_status,
              p.id as project_id,
              p.project_key,
              p.name as project_name,
              p.mode as project_mode,
              p.default_timezone
            from registration_tokens rt
            join tenants t on t.id = rt.tenant_id
            join projects p on p.id = rt.project_id
            where rt.token_hash = $1
              and rt.status = 'active'
              and rt.expires_at > now()
              and t.tenant_key = $2
              and p.project_key = $3
              and t.status = 'active'
              and p.status = 'active'
            for update of rt
            limit 1
          `,
          [registrationHash, request.tenantKey, request.projectKey]
        );
        const registration = registrationResult.rows[0];
        if (!registration) {
          throw new Error("Invalid registration token");
        }

        const agentResult = await client.query<{ id: string }>(
          `
            insert into agents (
              tenant_id,
              project_id,
              installation_id,
              name,
              mode,
              status,
              agent_version,
              host_fingerprint_hash,
              capabilities_json
            )
            values ($1, $2, $3, $4, $5, 'online', $6, $7, $8::jsonb)
            on conflict (tenant_id, installation_id)
            do update set
              project_id = excluded.project_id,
              mode = excluded.mode,
              status = 'online',
              agent_version = excluded.agent_version,
              host_fingerprint_hash = excluded.host_fingerprint_hash,
              capabilities_json = excluded.capabilities_json,
              updated_at = now()
            returning id
          `,
          [
            registration.tenant_id,
            registration.project_id,
            request.agentInstallationId,
            request.agentInstallationId,
            request.preferredMode,
            request.agentVersion,
            crypto.createHash("sha256").update(request.hostFingerprint).digest("hex"),
            asJson(request.capabilities)
          ]
        );
        const agentId = agentResult.rows[0]?.id;
        if (!agentId) {
          throw new Error("Agent registration failed");
        }

        const accessToken = createAgentToken();
        await client.query(
          `
            insert into agent_credentials (agent_id, credential_hash, status, expires_at)
            values ($1, $2, 'active', now() + interval '180 days')
          `,
          [agentId, hashAgentToken(accessToken, options.agentTokenPepper)]
        );

        await client.query(
          "update registration_tokens set status = 'claimed', claimed_at = now() where id = $1",
          [registration.token_id]
        );

        const licenseResult = await client.query<LicenseRecord>(
          `
            select *
            from licenses
            where tenant_id = $1
              and status in ('trial', 'active')
            order by valid_until nulls last, created_at desc
            limit 1
          `,
          [registration.tenant_id]
        );
        const license = licenseResult.rows[0];

        await client.query("commit");

        return {
          agentId,
          tenant: {
            tenantId: registration.tenant_id,
            tenantKey: registration.tenant_key,
            tenantName: registration.tenant_name,
            status: registration.tenant_status
          },
          project: {
            projectId: registration.project_id,
            projectKey: registration.project_key,
            projectName: registration.project_name,
            mode: registration.project_mode,
            timezone: registration.default_timezone
          },
          license: {
            status: license?.status || "trial",
            plan: license?.plan || "pilot",
            validFrom: license?.valid_from?.toISOString(),
            validUntil: license?.valid_until?.toISOString(),
            limits: {
              maxConnectors: license?.max_connectors || 0,
              maxSchedulers: license?.max_schedulers || 0,
              maxRecordsPerMonth: Number(license?.max_records_per_month || 0)
            },
            features: {
              ai: Boolean(license?.feature_ai),
              migration: Boolean(license?.feature_migration),
              customConnector: Boolean(license?.feature_custom_connector),
              customScheduler: Boolean(license?.feature_custom_scheduler)
            }
          },
          credential: {
            type: "bearer",
            accessToken,
            expiresAt: new Date(Date.now() + 180 * 24 * 60 * 60 * 1000).toISOString(),
            refreshToken: ""
          }
        };
      } catch (error) {
        await client.query("rollback").catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
    },

    async acceptAgentHeartbeat(heartbeat: SaasAgentHeartbeat, bearerToken: string): Promise<void> {
      const credentialHash = hashAgentToken(bearerToken, options.agentTokenPepper);
      const authResult = await pool.query<AgentAuthRecord>(
        `
          select a.tenant_id, a.project_id, t.tenant_key, p.project_key
          from agent_credentials c
          join agents a on a.id = c.agent_id
          join tenants t on t.id = a.tenant_id
          join projects p on p.id = a.project_id
          where c.agent_id = $1
            and c.credential_hash = $2
            and c.status = 'active'
            and a.status <> 'revoked'
            and (c.expires_at is null or c.expires_at > now())
          limit 1
        `,
        [heartbeat.agentId, credentialHash]
      );

      const auth = authResult.rows[0];
      if (!auth || auth.tenant_key !== heartbeat.tenantKey || auth.project_key !== heartbeat.projectKey) {
        throw new Error("Invalid agent credential");
      }

      await pool.query(
        `
          insert into agent_heartbeats (
            tenant_id,
            project_id,
            agent_id,
            status,
            mode,
            agent_version,
            runtime_json,
            config_json,
            sent_at
          )
          values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9)
        `,
        [
          auth.tenant_id,
          auth.project_id,
          heartbeat.agentId,
          heartbeat.status,
          heartbeat.mode,
          heartbeat.agentVersion,
          asJson(heartbeat.runtime),
          asJson(heartbeat.config),
          heartbeat.sentAt
        ]
      );

      await pool.query(
        `
          update agents
          set
            status = $2,
            mode = $3,
            agent_version = $4,
            capabilities_json = $5::jsonb,
            last_heartbeat_at = now(),
            updated_at = now()
          where id = $1
        `,
        [heartbeat.agentId, heartbeat.status, heartbeat.mode, heartbeat.agentVersion, asJson(heartbeat.capabilities)]
      );

      await pool.query(
        "update agent_credentials set last_used_at = now() where agent_id = $1 and credential_hash = $2",
        [heartbeat.agentId, credentialHash]
      );
    },

    async recordSchedulerRun(run: SaasRunReport, bearerToken: string): Promise<string> {
      const credentialHash = hashAgentToken(bearerToken, options.agentTokenPepper);
      const authResult = await pool.query<AgentAuthRecord>(
        `
          select a.tenant_id, a.project_id, t.tenant_key, p.project_key
          from agent_credentials c
          join agents a on a.id = c.agent_id
          join tenants t on t.id = a.tenant_id
          join projects p on p.id = a.project_id
          where c.agent_id = $1
            and c.credential_hash = $2
            and c.status = 'active'
            and a.status <> 'revoked'
            and (c.expires_at is null or c.expires_at > now())
          limit 1
        `,
        [run.agentId, credentialHash]
      );

      const auth = authResult.rows[0];
      if (!auth || auth.tenant_key !== run.tenantKey || auth.project_key !== run.projectKey) {
        throw new Error("Invalid agent credential");
      }

      const runResult = await pool.query<{ id: string }>(
        `
          insert into scheduler_runs (
            tenant_id, project_id, agent_id, scheduler_key, direction, status,
            total_records, read_records, written_records, skipped_records, failed_records,
            started_at, finished_at, error_category, error_message
          )
          values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
          returning id
        `,
        [
          auth.tenant_id,
          auth.project_id,
          run.agentId,
          run.schedulerId,
          run.direction,
          run.status,
          run.counters?.total ?? null,
          run.counters?.read ?? 0,
          run.counters?.written ?? 0,
          run.counters?.skipped ?? 0,
          run.counters?.failed ?? 0,
          run.startedAt,
          run.finishedAt ?? null,
          run.errorSummary?.category ?? null,
          run.errorSummary?.message ?? null
        ]
      );

      const runId = runResult.rows[0]?.id;
      if (!runId) {
        throw new Error("Failed to record scheduler run");
      }
      return runId;
    },

    // ── Auth ─────────────────────────────────────────────────────────────

    async findUserByEmail(email: string): Promise<UserRecord | null> {
      const result = await pool.query<{
        id: string; email: string; display_name: string;
        password_hash: string; is_system_admin: boolean; status: string;
      }>(
        "select id, email, display_name, password_hash, is_system_admin, status from users where email = $1 limit 1",
        [email]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      return {
        id: row.id,
        email: row.email,
        displayName: row.display_name,
        passwordHash: row.password_hash,
        isSystemAdmin: row.is_system_admin,
        status: row.status
      };
    },

    async createUser(input: { email: string; displayName: string; passwordHash: string; isSystemAdmin: boolean }): Promise<string> {
      const result = await pool.query<{ id: string }>(
        `insert into users (email, display_name, password_hash, is_system_admin, status)
         values ($1, $2, $3, $4, 'active')
         on conflict (email) do nothing
         returning id`,
        [input.email, input.displayName, input.passwordHash, input.isSystemAdmin]
      );
      return result.rows[0]?.id || "";
    },

    async ensureSystemAdmin(email: string, passwordHash: string): Promise<void> {
      await pool.query(
        `insert into users (email, display_name, password_hash, is_system_admin, status)
         values ($1, $2, $3, true, 'active')
         on conflict (email) do nothing`,
        [email, email, passwordHash]
      );
    },

    async updateUserLastLogin(userId: string): Promise<void> {
      await pool.query(
        "update users set last_login_at = now(), updated_at = now() where id = $1",
        [userId]
      );
    },

    // ── Tenant management ────────────────────────────────────────────────

    async listAllTenants(): Promise<TenantRecord[]> {
      const result = await pool.query<{
        id: string; tenant_key: string; name: string; status: string; created_at: Date;
      }>(
        "select id, tenant_key, name, status, created_at from tenants where status <> 'deleted' order by created_at desc"
      );
      return result.rows.map((r) => ({
        id: r.id, tenantKey: r.tenant_key, name: r.name, status: r.status, createdAt: r.created_at.toISOString()
      }));
    },

    async getTenantDetails(tenantId: string): Promise<TenantDetails | null> {
      const [tenantResult, licenseResult, statsResult, membersResult, projectsResult] = await Promise.all([
        pool.query<{ id: string; tenant_key: string; name: string; status: string; created_at: Date }>(
          "select id, tenant_key, name, status, created_at from tenants where id = $1 limit 1",
          [tenantId]
        ),
        pool.query<LicenseRecord>(
          "select * from licenses where tenant_id = $1 order by created_at desc limit 1",
          [tenantId]
        ),
        pool.query<{
          agent_count: string; online_agent_count: string; runs_24h: string; failed_24h: string;
        }>(
          `select
            (select count(*) from agents where tenant_id = $1 and status <> 'revoked') as agent_count,
            (select count(*) from agents where tenant_id = $1 and status = 'online' and last_heartbeat_at > now() - interval '15 minutes') as online_agent_count,
            (select count(*) from scheduler_runs where tenant_id = $1 and started_at > now() - interval '24 hours') as runs_24h,
            (select count(*) from scheduler_runs where tenant_id = $1 and started_at > now() - interval '24 hours' and status in ('failed','partial')) as failed_24h`,
          [tenantId]
        ),
        pool.query<{ user_id: string; email: string; display_name: string; role: string }>(
          `select tm.user_id, u.email, u.display_name, tm.role
           from tenant_memberships tm join users u on u.id = tm.user_id
           where tm.tenant_id = $1 order by u.email`,
          [tenantId]
        ),
        pool.query<{ id: string; project_key: string; name: string; mode: string; status: string }>(
          "select id, project_key, name, mode, status from projects where tenant_id = $1 and status <> 'deleted' order by name",
          [tenantId]
        )
      ]);

      const tenant = tenantResult.rows[0];
      if (!tenant) {
        return null;
      }
      const lic = licenseResult.rows[0];
      const stats = statsResult.rows[0];
      return {
        id: tenant.id, tenantKey: tenant.tenant_key, name: tenant.name, status: tenant.status,
        createdAt: tenant.created_at.toISOString(),
        license: lic ? {
          id: lic.id, contractReference: lic.contract_reference ?? null, plan: lic.plan,
          status: lic.status, validFrom: lic.valid_from ? new Date(lic.valid_from).toISOString() : null,
          validUntil: lic.valid_until ? new Date(lic.valid_until).toISOString() : null,
          maxConnectors: lic.max_connectors, maxSchedulers: lic.max_schedulers,
          maxRecordsPerMonth: Number(lic.max_records_per_month),
          featureAi: Boolean(lic.feature_ai), featureMigration: Boolean(lic.feature_migration),
          featureCustomConnector: Boolean(lic.feature_custom_connector),
          featureCustomScheduler: Boolean(lic.feature_custom_scheduler)
        } : null,
        projectCount: projectsResult.rows.length,
        agentCount: Number(stats?.agent_count || 0),
        onlineAgentCount: Number(stats?.online_agent_count || 0),
        members: membersResult.rows.map((r) => ({
          userId: r.user_id, email: r.email, displayName: r.display_name, role: r.role
        })),
        projects: projectsResult.rows.map((r) => ({
          id: r.id, projectKey: r.project_key, name: r.name, mode: r.mode, status: r.status
        }))
      };
    },

    async createTenant(input: { tenantKey: string; name: string }): Promise<string> {
      const result = await pool.query<{ id: string }>(
        "insert into tenants (tenant_key, name) values ($1, $2) returning id",
        [input.tenantKey, input.name]
      );
      return result.rows[0]?.id || "";
    },

    async updateTenantStatus(tenantId: string, status: string): Promise<void> {
      await pool.query(
        "update tenants set status = $2, updated_at = now() where id = $1",
        [tenantId, status]
      );
    },

    // ── User / membership ────────────────────────────────────────────────

    async addTenantMember(tenantId: string, userId: string, role: string): Promise<void> {
      await pool.query(
        `insert into tenant_memberships (tenant_id, user_id, role)
         values ($1, $2, $3)
         on conflict (tenant_id, user_id) do update set role = excluded.role`,
        [tenantId, userId, role]
      );
    },

    async getUserTenantMembership(userId: string): Promise<{ tenantId: string; tenantKey: string; tenantName: string; role: string } | null> {
      const result = await pool.query<{ tenant_id: string; tenant_key: string; name: string; role: string }>(
        `select t.id as tenant_id, t.tenant_key, t.name, tm.role
         from tenant_memberships tm join tenants t on t.id = tm.tenant_id
         where tm.user_id = $1 and t.status = 'active'
         order by tm.created_at asc limit 1`,
        [userId]
      );
      const row = result.rows[0];
      if (!row) {
        return null;
      }
      return { tenantId: row.tenant_id, tenantKey: row.tenant_key, tenantName: row.name, role: row.role };
    },

    // ── License ──────────────────────────────────────────────────────────

    async upsertLicense(tenantId: string, input: LicenseInput): Promise<void> {
      await pool.query(
        `insert into licenses (
           tenant_id, contract_reference, plan, status, valid_from, valid_until,
           max_connectors, max_schedulers, max_records_per_month,
           feature_ai, feature_migration, feature_custom_connector, feature_custom_scheduler
         )
         values ($1,$2,$3,$4,$5::timestamptz,$6::timestamptz,$7,$8,$9,$10,$11,$12,$13)
         on conflict do nothing`,
        [
          tenantId, input.contractReference || null, input.plan, input.status,
          input.validFrom || null, input.validUntil || null,
          input.maxConnectors, input.maxSchedulers, input.maxRecordsPerMonth,
          input.featureAi, input.featureMigration, input.featureCustomConnector, input.featureCustomScheduler
        ]
      );
    },

    // ── Projects ─────────────────────────────────────────────────────────

    async createProject(input: { tenantId: string; projectKey: string; name: string; mode: string; timezone: string }): Promise<string> {
      const result = await pool.query<{ id: string }>(
        `insert into projects (tenant_id, project_key, name, mode, default_timezone)
         values ($1, $2, $3, $4, $5)
         returning id`,
        [input.tenantId, input.projectKey, input.name, input.mode, input.timezone]
      );
      return result.rows[0]?.id || "";
    },

    // ── Registration tokens ──────────────────────────────────────────────

    async createRegistrationToken(projectId: string, tenantId: string): Promise<string> {
      const rawToken = `rt_${crypto.randomBytes(32).toString("base64url")}`;
      const tokenHash = hashAgentToken(rawToken, options.agentTokenPepper);
      await pool.query(
        `insert into registration_tokens (tenant_id, project_id, token_hash, status, expires_at)
         values ($1, $2, $3, 'active', now() + interval '7 days')`,
        [tenantId, projectId, tokenHash]
      );
      return rawToken;
    },

    async listProjectRegistrationTokens(projectId: string): Promise<RegistrationTokenInfo[]> {
      const result = await pool.query<{
        id: string; project_key: string; project_name: string;
        status: string; expires_at: Date; created_at: Date;
      }>(
        `select rt.id, p.project_key, p.name as project_name, rt.status, rt.expires_at, rt.created_at
         from registration_tokens rt join projects p on p.id = rt.project_id
         where rt.project_id = $1
         order by rt.created_at desc limit 20`,
        [projectId]
      );
      return result.rows.map((r) => ({
        id: r.id, projectKey: r.project_key, projectName: r.project_name,
        token: null, status: r.status,
        expiresAt: r.expires_at.toISOString(), createdAt: r.created_at.toISOString()
      }));
    },

    // ── Customer dashboard ───────────────────────────────────────────────

    async getCustomerDashboard(tenantId: string): Promise<CustomerDashboard> {
      const [tenantResult, licenseResult, statsResult, agentsResult, runsResult] = await Promise.all([
        pool.query<{ id: string; tenant_key: string; name: string; status: string; created_at: Date }>(
          "select id, tenant_key, name, status, created_at from tenants where id = $1 limit 1",
          [tenantId]
        ),
        pool.query<LicenseRecord>(
          "select * from licenses where tenant_id = $1 and status in ('trial','active') order by valid_until nulls last, created_at desc limit 1",
          [tenantId]
        ),
        pool.query<{ agent_count: string; online_agent_count: string; runs_24h: string; failed_24h: string }>(
          `select
            (select count(*) from agents where tenant_id = $1 and status <> 'revoked') as agent_count,
            (select count(*) from agents where tenant_id = $1 and status = 'online' and last_heartbeat_at > now() - interval '15 minutes') as online_agent_count,
            (select count(*) from scheduler_runs where tenant_id = $1 and started_at > now() - interval '24 hours') as runs_24h,
            (select count(*) from scheduler_runs where tenant_id = $1 and started_at > now() - interval '24 hours' and status in ('failed','partial')) as failed_24h`,
          [tenantId]
        ),
        pool.query<{ agent_id: string; name: string; project_key: string; mode: string; status: string; agent_version: string | null; last_heartbeat_at: Date | null }>(
          `select a.id as agent_id, a.name, p.project_key, a.mode, a.status, a.agent_version, a.last_heartbeat_at
           from agents a join projects p on p.id = a.project_id
           where a.tenant_id = $1 and a.status <> 'revoked'
           order by a.last_heartbeat_at desc nulls last limit 50`,
          [tenantId]
        ),
        pool.query<{ run_id: string; scheduler_key: string; project_key: string; direction: string; status: string; read_records: string; written_records: string; failed_records: string; started_at: Date; finished_at: Date | null }>(
          `select r.id as run_id, r.scheduler_key, p.project_key, r.direction, r.status,
                  r.read_records, r.written_records, r.failed_records, r.started_at, r.finished_at
           from scheduler_runs r join projects p on p.id = r.project_id
           where r.tenant_id = $1 order by r.started_at desc limit 50`,
          [tenantId]
        )
      ]);

      const tenant = tenantResult.rows[0];
      const lic = licenseResult.rows[0];
      const stats = statsResult.rows[0];
      return {
        tenant: { id: tenant.id, tenantKey: tenant.tenant_key, name: tenant.name, status: tenant.status, createdAt: tenant.created_at.toISOString() },
        license: lic ? {
          id: lic.id, contractReference: lic.contract_reference ?? null, plan: lic.plan, status: lic.status,
          validFrom: lic.valid_from ? new Date(lic.valid_from).toISOString() : null,
          validUntil: lic.valid_until ? new Date(lic.valid_until).toISOString() : null,
          maxConnectors: lic.max_connectors, maxSchedulers: lic.max_schedulers,
          maxRecordsPerMonth: Number(lic.max_records_per_month),
          featureAi: Boolean(lic.feature_ai), featureMigration: Boolean(lic.feature_migration),
          featureCustomConnector: Boolean(lic.feature_custom_connector),
          featureCustomScheduler: Boolean(lic.feature_custom_scheduler)
        } : null,
        stats: {
          agentCount: Number(stats?.agent_count || 0),
          onlineAgentCount: Number(stats?.online_agent_count || 0),
          runs24h: Number(stats?.runs_24h || 0),
          failed24h: Number(stats?.failed_24h || 0)
        },
        agents: agentsResult.rows.map((r) => ({
          agentId: r.agent_id, name: r.name, projectKey: r.project_key, mode: r.mode, status: r.status,
          agentVersion: r.agent_version, lastHeartbeatAt: r.last_heartbeat_at?.toISOString() ?? null
        })),
        recentRuns: runsResult.rows.map((r) => ({
          runId: r.run_id, schedulerKey: r.scheduler_key, projectKey: r.project_key,
          direction: r.direction, status: r.status,
          readRecords: Number(r.read_records || 0), writtenRecords: Number(r.written_records || 0),
          failedRecords: Number(r.failed_records || 0),
          startedAt: r.started_at.toISOString(), finishedAt: r.finished_at?.toISOString() ?? null
        }))
      };
    },

    // ── Config bundle ────────────────────────────────────────────────────

    async getActiveConfigBundle(agentId: string, bearerToken: string): Promise<{ version: string; config: unknown } | null> {
      const credentialHash = hashAgentToken(bearerToken, options.agentTokenPepper);
      const authResult = await pool.query<{ project_id: string }>(
        `select a.project_id from agent_credentials c
         join agents a on a.id = c.agent_id
         where c.agent_id = $1 and c.credential_hash = $2 and c.status = 'active'
           and a.status <> 'revoked' and (c.expires_at is null or c.expires_at > now())
         limit 1`,
        [agentId, credentialHash]
      );
      if (!authResult.rows[0]) {
        return null;
      }
      const { project_id } = authResult.rows[0];
      const cvResult = await pool.query<{ id: string; version_no: number; config_json: unknown }>(
        `select id, version_no, config_json from config_versions
         where project_id = $1 and status = 'released'
         order by version_no desc limit 1`,
        [project_id]
      );
      if (!cvResult.rows[0]) {
        return null;
      }
      const cv = cvResult.rows[0];
      return { version: String(cv.version_no), config: cv.config_json };
    },

    async listAllUsers(): Promise<Array<{ id: string; email: string; displayName: string; isSystemAdmin: boolean; status: string }>> {
      const result = await pool.query<{ id: string; email: string; display_name: string; is_system_admin: boolean; status: string }>(
        "select id, email, display_name, is_system_admin, status from users order by email"
      );
      return result.rows.map((r) => ({
        id: r.id, email: r.email, displayName: r.display_name,
        isSystemAdmin: r.is_system_admin, status: r.status
      }));
    },

    async findTenantByKey(tenantKey: string): Promise<{ id: string } | null> {
      const result = await pool.query<{ id: string }>(
        "select id from tenants where tenant_key = $1 and status <> 'deleted' limit 1",
        [tenantKey]
      );
      return result.rows[0] ?? null;
    },

    async listTenantRegistrationTokens(tenantId: string): Promise<RegistrationTokenInfo[]> {
      const result = await pool.query<{
        id: string; project_key: string; project_name: string;
        status: string; expires_at: Date; created_at: Date;
      }>(
        `select rt.id, p.project_key, p.name as project_name, rt.status, rt.expires_at, rt.created_at
         from registration_tokens rt join projects p on p.id = rt.project_id
         where p.tenant_id = $1
         order by rt.created_at desc limit 50`,
        [tenantId]
      );
      return result.rows.map((r) => ({
        id: r.id, projectKey: r.project_key, projectName: r.project_name,
        token: null, status: r.status,
        expiresAt: r.expires_at.toISOString(), createdAt: r.created_at.toISOString()
      }));
    },

    // ── Run detail ───────────────────────────────────────────────────────

    async getRunDetail(runId: string, tenantId: string): Promise<RunDetail | null> {
      const [runResult, logsResult, failedResult] = await Promise.all([
        pool.query<{
          run_id: string; scheduler_key: string; project_key: string; direction: string;
          status: string; read_records: string; written_records: string; failed_records: string;
          started_at: Date; finished_at: Date | null; error_category: string | null; error_message: string | null;
        }>(
          `select r.id as run_id, r.scheduler_key, p.project_key, r.direction, r.status,
                  r.read_records, r.written_records, r.failed_records,
                  r.started_at, r.finished_at, r.error_category, r.error_message
           from scheduler_runs r join projects p on p.id = r.project_id
           where r.id = $1 and r.tenant_id = $2 limit 1`,
          [runId, tenantId]
        ),
        pool.query<{ id: string; level: string; code: string | null; message: string; occurred_at: Date }>(
          `select id, level, code, message, occurred_at
           from log_events where run_id = $1 and tenant_id = $2
           order by occurred_at asc limit 500`,
          [runId, tenantId]
        ),
        pool.query<{ id: string; record_key: string | null; status: string | null; error_code: string | null; message: string | null; created_at: Date }>(
          `select id, record_key, status, error_code, message, created_at
           from failed_records where run_id = $1 and tenant_id = $2
           order by created_at asc limit 200`,
          [runId, tenantId]
        )
      ]);

      const row = runResult.rows[0];
      if (!row) return null;
      return {
        run: {
          runId: row.run_id, schedulerKey: row.scheduler_key, projectKey: row.project_key,
          direction: row.direction, status: row.status,
          readRecords: Number(row.read_records || 0), writtenRecords: Number(row.written_records || 0),
          failedRecords: Number(row.failed_records || 0),
          startedAt: row.started_at.toISOString(), finishedAt: row.finished_at?.toISOString() ?? null,
          errorCategory: row.error_category, errorMessage: row.error_message
        },
        logs: logsResult.rows.map((r) => ({
          id: r.id, level: r.level, code: r.code, message: r.message, occurredAt: r.occurred_at.toISOString()
        })),
        failedRecords: failedResult.rows.map((r) => ({
          id: r.id, recordKey: r.record_key, status: r.status,
          errorCode: r.error_code, message: r.message, createdAt: r.created_at.toISOString()
        }))
      };
    },

    // ── Connectors ───────────────────────────────────────────────────────

    async listTenantConnectors(tenantId: string): Promise<ConnectorConfigRecord[]> {
      const result = await pool.query<{
        id: string; project_id: string; project_key: string; connector_key: string;
        type: string; display_name: string; metadata_json: unknown;
        secret_policy: string; status: string; created_at: Date;
      }>(
        `select c.id, c.project_id, p.project_key, c.connector_key, c.type,
                c.display_name, c.metadata_json, c.secret_policy, c.status, c.created_at
         from connector_configs c join projects p on p.id = c.project_id
         where c.tenant_id = $1 and c.status <> 'deleted'
         order by p.project_key, c.display_name`,
        [tenantId]
      );
      return result.rows.map((r) => ({
        id: r.id, projectId: r.project_id, projectKey: r.project_key,
        connectorKey: r.connector_key, type: r.type, displayName: r.display_name,
        metadataJson: r.metadata_json, secretPolicy: r.secret_policy,
        status: r.status, createdAt: r.created_at.toISOString()
      }));
    },

    async upsertConnector(tenantId: string, projectId: string, input: ConnectorInput): Promise<void> {
      await pool.query(
        `insert into connector_configs (tenant_id, project_id, connector_key, type, display_name, metadata_json, secret_policy)
         values ($1, $2, $3, $4, $5, $6::jsonb, $7)
         on conflict (tenant_id, project_id, connector_key) do update set
           type = excluded.type, display_name = excluded.display_name,
           metadata_json = excluded.metadata_json, secret_policy = excluded.secret_policy,
           status = 'active', updated_at = now()`,
        [
          tenantId, projectId, input.connectorKey, input.type, input.displayName,
          JSON.stringify(input.metadataJson ?? {}),
          input.secretPolicy || "local-only"
        ]
      );
    },

    async deleteConnector(tenantId: string, connectorId: string): Promise<void> {
      await pool.query(
        "update connector_configs set status = 'deleted', updated_at = now() where id = $1 and tenant_id = $2",
        [connectorId, tenantId]
      );
    },

    // ── Schedulers ───────────────────────────────────────────────────────

    async listTenantSchedulers(tenantId: string): Promise<SchedulerConfigRecord[]> {
      const result = await pool.query<{
        id: string; project_id: string; project_key: string; scheduler_key: string;
        name: string; direction: string; active: boolean; schedule_expression: string | null;
        connector_key: string | null; object_name: string | null; config_json: unknown;
        status: string; created_at: Date;
      }>(
        `select s.id, s.project_id, p.project_key, s.scheduler_key, s.name, s.direction,
                s.active, s.schedule_expression, s.connector_key, s.object_name,
                s.config_json, s.status, s.created_at
         from scheduler_configs s join projects p on p.id = s.project_id
         where s.tenant_id = $1 and s.status <> 'deleted'
         order by p.project_key, s.name`,
        [tenantId]
      );
      return result.rows.map((r) => ({
        id: r.id, projectId: r.project_id, projectKey: r.project_key,
        schedulerKey: r.scheduler_key, name: r.name, direction: r.direction,
        active: r.active, scheduleExpression: r.schedule_expression,
        connectorKey: r.connector_key, objectName: r.object_name,
        configJson: r.config_json, status: r.status, createdAt: r.created_at.toISOString()
      }));
    },

    async upsertScheduler(tenantId: string, projectId: string, input: SchedulerInput): Promise<void> {
      await pool.query(
        `insert into scheduler_configs
           (tenant_id, project_id, scheduler_key, name, direction, active,
            schedule_expression, connector_key, object_name, config_json)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
         on conflict (tenant_id, project_id, scheduler_key) do update set
           name = excluded.name, direction = excluded.direction, active = excluded.active,
           schedule_expression = excluded.schedule_expression, connector_key = excluded.connector_key,
           object_name = excluded.object_name, config_json = excluded.config_json,
           status = 'active', updated_at = now()`,
        [
          tenantId, projectId, input.schedulerKey, input.name, input.direction,
          input.active, input.scheduleExpression ?? null, input.connectorKey ?? null,
          input.objectName ?? null, JSON.stringify(input.configJson ?? {})
        ]
      );
    },

    async deleteScheduler(tenantId: string, schedulerId: string): Promise<void> {
      await pool.query(
        "update scheduler_configs set status = 'deleted', updated_at = now() where id = $1 and tenant_id = $2",
        [schedulerId, tenantId]
      );
    },

    // ── Agent dashboard reads ─────────────────────────────────────────────

    async getAgentRuns(agentId: string, bearerToken: string, limit: number): Promise<AgentRunItem[]> {
      const { tenantId, projectId } = await resolveAgentProject(agentId, bearerToken);
      const result = await pool.query<{
        run_id: string; scheduler_key: string; scheduler_name: string | null;
        connector_key: string | null; connector_name: string | null;
        direction: string; status: string; started_at: Date; finished_at: Date | null;
        read_records: string; written_records: string; failed_records: string; error_message: string | null;
      }>(
        `select r.id as run_id, r.scheduler_key,
                sc.name as scheduler_name, sc.connector_key,
                cc.display_name as connector_name,
                r.direction, r.status, r.started_at, r.finished_at,
                r.read_records, r.written_records, r.failed_records, r.error_message
         from scheduler_runs r
         left join scheduler_configs sc on sc.project_id = r.project_id
           and sc.scheduler_key = r.scheduler_key and sc.status <> 'deleted'
         left join connector_configs cc on cc.project_id = r.project_id
           and cc.connector_key = sc.connector_key and cc.status <> 'deleted'
         where r.tenant_id = $1 and r.project_id = $2
         order by r.started_at desc limit $3`,
        [tenantId, projectId, limit]
      );
      return result.rows.map((r) => ({
        runId: r.run_id, schedulerKey: r.scheduler_key, schedulerName: r.scheduler_name,
        connectorKey: r.connector_key, connectorName: r.connector_name,
        direction: r.direction, status: r.status,
        startedAt: r.started_at.toISOString(), finishedAt: r.finished_at?.toISOString() ?? null,
        readRecords: Number(r.read_records || 0), writtenRecords: Number(r.written_records || 0),
        failedRecords: Number(r.failed_records || 0), errorMessage: r.error_message
      }));
    },

    async getAgentRunLogEvents(agentId: string, runId: string, bearerToken: string, limit: number): Promise<AgentLogEvent[]> {
      const { tenantId } = await resolveAgentProject(agentId, bearerToken);
      const result = await pool.query<{
        id: string; run_id: string; scheduler_key: string | null; connector_key: string | null;
        level: string; code: string | null; message: string; occurred_at: Date;
      }>(
        `select l.id, l.run_id, r.scheduler_key, sc.connector_key,
                l.level, l.code, l.message, l.occurred_at
         from log_events l
         join scheduler_runs r on r.id = l.run_id and r.tenant_id = $2
         left join scheduler_configs sc on sc.project_id = r.project_id
           and sc.scheduler_key = r.scheduler_key and sc.status <> 'deleted'
         where l.run_id = $1 and l.tenant_id = $2
         order by l.occurred_at limit $3`,
        [runId, tenantId, limit]
      );
      return result.rows.map((r) => ({
        id: r.id, runId: r.run_id, schedulerKey: r.scheduler_key, connectorKey: r.connector_key,
        level: r.level, code: r.code, message: r.message, occurredAt: r.occurred_at.toISOString()
      }));
    },

    async getAgentRunFailedRecords(agentId: string, runId: string, bearerToken: string): Promise<AgentFailedRecord[]> {
      const { tenantId } = await resolveAgentProject(agentId, bearerToken);
      const result = await pool.query<{
        id: string; record_key: string | null; error_code: string | null; message: string | null; created_at: Date;
      }>(
        `select id, record_key, error_code, message, created_at
         from failed_records where run_id = $1 and tenant_id = $2 order by created_at`,
        [runId, tenantId]
      );
      return result.rows.map((r) => ({
        id: r.id, recordKey: r.record_key, errorCode: r.error_code,
        message: r.message, createdAt: r.created_at.toISOString()
      }));
    },

    async getAgentRecordsSummary(agentId: string, bearerToken: string, range: "day" | "month" | "year"): Promise<AgentRecordsSummary> {
      const { tenantId, projectId } = await resolveAgentProject(agentId, bearerToken);
      const buckets = generateTimeBuckets(range);
      const since = buckets[0].start;
      const result = await pool.query<{
        scheduler_key: string; scheduler_name: string | null;
        connector_key: string | null; connector_name: string | null;
        status: string; started_at: Date; written_records: string; failed_records: string;
      }>(
        `select r.scheduler_key, sc.name as scheduler_name, sc.connector_key,
                cc.display_name as connector_name,
                r.status, r.started_at, r.written_records, r.failed_records
         from scheduler_runs r
         left join scheduler_configs sc on sc.project_id = r.project_id
           and sc.scheduler_key = r.scheduler_key and sc.status <> 'deleted'
         left join connector_configs cc on cc.project_id = r.project_id
           and cc.connector_key = sc.connector_key and cc.status <> 'deleted'
         where r.tenant_id = $1 and r.project_id = $2 and r.started_at >= $3
         order by r.started_at`,
        [tenantId, projectId, since]
      );

      const schedulerKeySet = new Set<string>();
      type SchedulerAgg = { schedulerKey: string; schedulerName: string | null; connectorKey: string | null; connectorName: string | null; total: number; succeeded: number; failed: number };
      const filledBuckets: AgentRecordsBucket[] = buckets.map((b) => ({
        label: b.label, periodStart: b.start.toISOString(), periodEnd: b.end.toISOString(),
        total: 0, succeeded: 0, failed: 0, schedulerBreakdown: []
      }));
      const schedulerByBucket = new Map<number, Map<string, SchedulerAgg>>();

      for (const run of result.rows) {
        const idx = buckets.findIndex((b) => run.started_at >= b.start && run.started_at < b.end);
        if (idx < 0) continue;
        const succeeded = Number(run.written_records || 0);
        const failed = Number(run.failed_records || 0);
        const bucket = filledBuckets[idx];
        bucket.total += succeeded + failed;
        bucket.succeeded += succeeded;
        bucket.failed += failed;
        if (run.scheduler_key) {
          schedulerKeySet.add(run.scheduler_key);
          if (!schedulerByBucket.has(idx)) schedulerByBucket.set(idx, new Map());
          const sbMap = schedulerByBucket.get(idx)!;
          if (!sbMap.has(run.scheduler_key)) {
            sbMap.set(run.scheduler_key, {
              schedulerKey: run.scheduler_key, schedulerName: run.scheduler_name,
              connectorKey: run.connector_key, connectorName: run.connector_name,
              total: 0, succeeded: 0, failed: 0
            });
          }
          const agg = sbMap.get(run.scheduler_key)!;
          agg.total += succeeded + failed;
          agg.succeeded += succeeded;
          agg.failed += failed;
        }
      }

      for (const [idx, sbMap] of schedulerByBucket) {
        filledBuckets[idx].schedulerBreakdown = Array.from(sbMap.values());
      }

      return { range, buckets: filledBuckets, schedulerKeys: Array.from(schedulerKeySet) };
    },

    async getAgentLogSummary(agentId: string, bearerToken: string, range: "last_hour" | "last_24h" | "last_30d"): Promise<AgentLogSummary> {
      const { tenantId } = await resolveAgentProject(agentId, bearerToken);
      const buckets = generateLogBuckets(range);
      const since = buckets[0].start;
      const result = await pool.query<{
        level: string; occurred_at: Date; scheduler_key: string | null;
      }>(
        `select l.level, l.occurred_at, r.scheduler_key
         from log_events l
         join scheduler_runs r on r.id = l.run_id and r.tenant_id = $1
         where l.tenant_id = $1 and l.occurred_at >= $2
         order by l.occurred_at`,
        [tenantId, since]
      );

      const schedulerKeySet = new Set<string>();
      const filledBuckets: AgentLogBucket[] = buckets.map((b) => ({
        label: b.label, periodStart: b.start.toISOString(), periodEnd: b.end.toISOString(),
        total: 0, errors: 0, schedulerErrors: {}
      }));

      for (const row of result.rows) {
        const idx = buckets.findIndex((b) => row.occurred_at >= b.start && row.occurred_at < b.end);
        if (idx < 0) continue;
        const bucket = filledBuckets[idx];
        bucket.total++;
        const isError = row.level === "ERROR" || row.level === "WARN";
        if (isError) {
          bucket.errors++;
          if (row.scheduler_key) {
            schedulerKeySet.add(row.scheduler_key);
            bucket.schedulerErrors[row.scheduler_key] = (bucket.schedulerErrors[row.scheduler_key] || 0) + 1;
          }
        }
      }

      return { range, buckets: filledBuckets, schedulerKeys: Array.from(schedulerKeySet) };
    },

    async getAgentLogsByRange(agentId: string, bearerToken: string, start: string, end: string, type: "all" | "error", limit: number, schedulerKey?: string): Promise<AgentLogEvent[]> {
      const { tenantId } = await resolveAgentProject(agentId, bearerToken);
      const params: unknown[] = [tenantId, new Date(start), new Date(end), limit];
      const levelFilter = type === "error" ? "and l.level in ('ERROR', 'WARN')" : "";
      let schedulerFilter = "";
      if (schedulerKey) {
        params.push(schedulerKey);
        schedulerFilter = `and r.scheduler_key = $${params.length}`;
      }
      const result = await pool.query<{
        id: string; run_id: string; scheduler_key: string | null; connector_key: string | null;
        level: string; code: string | null; message: string; occurred_at: Date;
      }>(
        `select l.id, l.run_id, r.scheduler_key, sc.connector_key,
                l.level, l.code, l.message, l.occurred_at
         from log_events l
         join scheduler_runs r on r.id = l.run_id and r.tenant_id = $1
         left join scheduler_configs sc on sc.project_id = r.project_id
           and sc.scheduler_key = r.scheduler_key and sc.status <> 'deleted'
         where l.tenant_id = $1 and l.occurred_at >= $2 and l.occurred_at <= $3
           ${levelFilter} ${schedulerFilter}
         order by l.occurred_at desc limit $4`,
        params
      );
      return result.rows.map((r) => ({
        id: r.id, runId: r.run_id, schedulerKey: r.scheduler_key, connectorKey: r.connector_key,
        level: r.level, code: r.code, message: r.message, occurredAt: r.occurred_at.toISOString()
      }));
    }
  };
}
