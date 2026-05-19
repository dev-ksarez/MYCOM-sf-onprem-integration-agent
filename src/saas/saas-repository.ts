import crypto from "node:crypto";
import { Pool } from "pg";
import { SaasAgentHeartbeat } from "../types/saas-control-plane";

export interface SaasRepository {
  close(): Promise<void>;
  checkHealth(): Promise<void>;
  acceptAgentHeartbeat(heartbeat: SaasAgentHeartbeat, bearerToken: string): Promise<void>;
}

export interface SaasRepositoryOptions {
  databaseUrl: string;
  agentTokenPepper: string;
}

interface AgentAuthRecord {
  tenant_id: string;
  project_id: string;
}

function hashAgentToken(token: string, pepper: string): string {
  return crypto.createHash("sha256").update(`${pepper}:${token}`).digest("hex");
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

    async acceptAgentHeartbeat(heartbeat: SaasAgentHeartbeat, bearerToken: string): Promise<void> {
      const credentialHash = hashAgentToken(bearerToken, options.agentTokenPepper);
      const authResult = await pool.query<AgentAuthRecord>(
        `
          select a.tenant_id, a.project_id
          from agent_credentials c
          join agents a on a.id = c.agent_id
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
      if (!auth || auth.tenant_id !== heartbeat.tenantId || auth.project_id !== heartbeat.projectId) {
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
          heartbeat.tenantId,
          heartbeat.projectId,
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
