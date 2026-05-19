import fs from "node:fs";
import path from "node:path";

export interface SaasControlPlaneConfig {
  enabled: boolean;
  baseUrl: string;
  tenantKey: string;
  projectKey: string;
  agentId: string;
  accessToken: string;
  heartbeatIntervalMs: number;
  requestTimeoutMs: number;
}

interface SaasBootstrapFile {
  baseUrl?: string;
  tenantKey?: string;
  projectKey?: string;
  agentId?: string;
  accessToken?: string;
}

function readBootstrapFile(filePath: string): SaasBootstrapFile {
  if (!fs.existsSync(filePath)) {
    return {};
  }

  const raw = fs.readFileSync(filePath, "utf8");
  const parsed = JSON.parse(raw) as SaasBootstrapFile;
  return parsed && typeof parsed === "object" ? parsed : {};
}

function trim(value: unknown): string {
  return String(value || "").trim();
}

function positiveNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function isEnabled(value: string | undefined, defaultValue: boolean): boolean {
  const normalized = String(value ?? (defaultValue ? "1" : "0")).trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "off";
}

export function getSaasControlPlaneConfig(env: NodeJS.ProcessEnv = process.env): SaasControlPlaneConfig {
  const bootstrapPath = path.resolve(
    trim(env.AGENT_SAAS_BOOTSTRAP_FILE) || path.resolve(process.cwd(), "artifacts/saas-bootstrap.json")
  );
  const bootstrap = readBootstrapFile(bootstrapPath);

  return {
    enabled: isEnabled(env.AGENT_SAAS_CONTROL_PLANE_ENABLED, false),
    baseUrl: trim(env.AGENT_SAAS_BASE_URL) || trim(bootstrap.baseUrl),
    tenantKey: trim(env.AGENT_SAAS_TENANT_KEY) || trim(bootstrap.tenantKey),
    projectKey: trim(env.AGENT_SAAS_PROJECT_KEY) || trim(bootstrap.projectKey),
    agentId: trim(env.AGENT_SAAS_AGENT_ID) || trim(bootstrap.agentId),
    accessToken: trim(env.AGENT_SAAS_ACCESS_TOKEN) || trim(bootstrap.accessToken),
    heartbeatIntervalMs: positiveNumber(env.AGENT_SAAS_HEARTBEAT_INTERVAL_MS, 300_000),
    requestTimeoutMs: positiveNumber(env.AGENT_SAAS_REQUEST_TIMEOUT_MS, 15_000)
  };
}

export function assertUsableSaasControlPlaneConfig(config: SaasControlPlaneConfig): void {
  if (!config.enabled) {
    return;
  }

  const missing = [
    ["AGENT_SAAS_BASE_URL", config.baseUrl],
    ["AGENT_SAAS_TENANT_KEY", config.tenantKey],
    ["AGENT_SAAS_PROJECT_KEY", config.projectKey],
    ["AGENT_SAAS_AGENT_ID", config.agentId],
    ["AGENT_SAAS_ACCESS_TOKEN", config.accessToken]
  ].filter(([, value]) => !value);

  if (missing.length > 0) {
    throw new Error(`SaaS control plane enabled but missing configuration: ${missing.map(([key]) => key).join(", ")}`);
  }
}
