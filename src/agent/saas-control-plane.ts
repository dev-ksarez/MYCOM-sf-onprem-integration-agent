import pino from "pino";
import {
  assertUsableSaasControlPlaneConfig,
  getSaasControlPlaneConfig,
  SaasControlPlaneConfig
} from "../infrastructure/config/saas-control-plane-config";
import { HealthSnapshot } from "../server/health-snapshot";
import { SaasAgentHeartbeat } from "../types/saas-control-plane";

export interface SaasControlPlaneRuntimeOptions {
  logger: pino.Logger;
  getHealthSnapshot: () => HealthSnapshot;
  config?: SaasControlPlaneConfig;
}

export interface SaasControlPlaneRuntime {
  start(): void;
  stop(): void;
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, "");
}

function buildHeartbeat(config: SaasControlPlaneConfig, snapshot: HealthSnapshot): SaasAgentHeartbeat {
  const now = new Date().toISOString();
  const schedulerCount = Number(snapshot.schedulesFound || 0);

  return {
    idempotencyKey: `hb-${config.agentId}-${now}`,
    agentId: config.agentId,
    tenantId: config.tenantKey,
    projectId: config.projectKey,
    mode: "hybrid",
    status: "online",
    agentVersion: process.env.npm_package_version || "unknown",
    startedAt: snapshot.startedAt,
    sentAt: now,
    capabilities: ["heartbeat", "run-reporting", "config-fetch"],
    config: {
      desiredVersion: null,
      appliedVersion: null,
      lastRunVersion: null
    },
    runtime: {
      os: process.platform,
      nodeVersion: process.version,
      schedulerCount
    }
  };
}

async function postHeartbeat(config: SaasControlPlaneConfig, heartbeat: SaasAgentHeartbeat): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.requestTimeoutMs);

  try {
    const response = await fetch(`${normalizeBaseUrl(config.baseUrl)}/api/agent/v1/heartbeats`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${config.accessToken}`,
        "content-type": "application/json",
        "x-tenant-key": config.tenantKey,
        "x-project-key": config.projectKey
      },
      body: JSON.stringify(heartbeat),
      signal: controller.signal
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(`SaaS heartbeat failed with HTTP ${response.status}${text ? `: ${text.slice(0, 300)}` : ""}`);
    }
  } finally {
    clearTimeout(timeout);
  }
}

export function createSaasControlPlaneRuntime(options: SaasControlPlaneRuntimeOptions): SaasControlPlaneRuntime {
  const config = options.config || getSaasControlPlaneConfig();
  let timer: NodeJS.Timeout | undefined;
  let running = false;

  const publishHeartbeat = async (): Promise<void> => {
    if (!config.enabled || running) {
      return;
    }

    running = true;
    try {
      const heartbeat = buildHeartbeat(config, options.getHealthSnapshot());
      await postHeartbeat(config, heartbeat);
      options.logger.debug({ tenantKey: config.tenantKey, projectKey: config.projectKey }, "SaaS heartbeat published");
    } catch (error) {
      options.logger.warn({ err: error }, "SaaS heartbeat failed");
    } finally {
      running = false;
    }
  };

  return {
    start(): void {
      if (!config.enabled) {
        options.logger.debug("SaaS control plane disabled");
        return;
      }

      try {
        assertUsableSaasControlPlaneConfig(config);
      } catch (error) {
        options.logger.warn({ err: error }, "SaaS control plane not started");
        return;
      }

      void publishHeartbeat();
      timer = setInterval(() => void publishHeartbeat(), config.heartbeatIntervalMs);
      options.logger.info(
        { baseUrl: config.baseUrl, tenantKey: config.tenantKey, projectKey: config.projectKey },
        "SaaS control plane started"
      );
    },
    stop(): void {
      if (timer) {
        clearInterval(timer);
      }
      timer = undefined;
    }
  };
}
