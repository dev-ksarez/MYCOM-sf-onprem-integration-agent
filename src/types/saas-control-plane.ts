export type SaasMode = "legacy" | "hybrid" | "saas";

export type TenantStatus = "active" | "suspended" | "deleted";

export type LicenseStatus = "trial" | "active" | "expired" | "suspended";

export type TenantRole = "owner" | "admin" | "operator" | "viewer" | "support";

export type AgentStatus = "pending" | "online" | "offline" | "revoked";

export interface SaasLicenseLimits {
  maxConnectors: number;
  maxSchedulers: number;
  maxRecordsPerMonth: number;
}

export interface SaasLicenseFeatures {
  ai: boolean;
  migration: boolean;
  customConnector: boolean;
  customScheduler: boolean;
}

export interface SaasTenantIdentity {
  tenantId: string;
  tenantKey: string;
  tenantName: string;
  status: TenantStatus;
}

export interface SaasProjectIdentity {
  projectId: string;
  projectKey: string;
  projectName: string;
  mode: SaasMode;
  timezone: string;
}

export interface SaasLicenseSnapshot {
  status: LicenseStatus;
  plan: string;
  validFrom?: string;
  validUntil?: string;
  limits: SaasLicenseLimits;
  features: SaasLicenseFeatures;
}

export interface SaasAgentRegistrationRequest {
  tenantKey: string;
  registrationToken: string;
  agentInstallationId: string;
  agentVersion: string;
  hostFingerprint: string;
  preferredMode: Exclude<SaasMode, "legacy">;
  capabilities: string[];
}

export interface SaasAgentCredential {
  type: "bearer";
  accessToken: string;
  expiresAt: string;
  refreshToken: string;
}

export interface SaasAgentRegistrationResponse {
  agentId: string;
  tenant: SaasTenantIdentity;
  project: SaasProjectIdentity;
  license: SaasLicenseSnapshot;
  credential: SaasAgentCredential;
}

export interface SaasAgentHeartbeat {
  idempotencyKey: string;
  agentId: string;
  tenantId: string;
  projectId: string;
  mode: SaasMode;
  status: AgentStatus;
  agentVersion: string;
  startedAt?: string;
  sentAt: string;
  capabilities: string[];
  config: {
    desiredVersion?: string | null;
    appliedVersion?: string | null;
    lastRunVersion?: string | null;
  };
  runtime: {
    os: string;
    nodeVersion: string;
    schedulerCount: number;
    connectorCount?: number;
  };
}

export interface SaasRunCounters {
  total?: number;
  read: number;
  written: number;
  skipped: number;
  failed: number;
}

export interface SaasRunReport {
  idempotencyKey: string;
  agentId: string;
  tenantId: string;
  projectId: string;
  schedulerId: string;
  configVersion?: string;
  direction: "inbound" | "outbound";
  status: "queued" | "running" | "success" | "partial" | "failed" | "cancelled";
  startedAt: string;
  finishedAt?: string;
  counters?: SaasRunCounters;
  errorSummary?: {
    category: "technical" | "validation" | "auth" | "license" | "timeout" | "unknown";
    message: string;
  };
}
