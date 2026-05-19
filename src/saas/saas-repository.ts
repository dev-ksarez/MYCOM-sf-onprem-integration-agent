import crypto from "node:crypto";
import { Pool } from "pg";
import {
  SaasAgentHeartbeat,
  SaasAgentRegistrationRequest,
  SaasAgentRegistrationResponse
} from "../types/saas-control-plane";

export interface SaasRepository {
  close(): Promise<void>;
  checkHealth(): Promise<void>;
  getPortalSnapshot(): Promise<SaasPortalSnapshot>;
  claimAgentRegistration(request: SaasAgentRegistrationRequest): Promise<SaasAgentRegistrationResponse>;
  acceptAgentHeartbeat(heartbeat: SaasAgentHeartbeat, bearerToken: string): Promise<void>;
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

export function createSaasRepository(options: SaasRepositoryOptions): SaasRepository {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    max: 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000
  });

  return {
    async close(): Promise<void> {
      await pool.end();
    },

    async checkHealth(): Promise<void> {
      await pool.query("select 1");
    },

    async getPortalSnapshot(): Promise<SaasPortalSnapshot> {
      const [totalsResult, tenantsResult, projectsResult, agentsResult] = await Promise.all([
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
    }
  };
}
