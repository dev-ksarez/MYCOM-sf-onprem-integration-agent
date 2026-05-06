import type { HealthSnapshot } from "../server/health-snapshot";
import type { DashboardUpdateStatus, DashboardUpdateTriggerResult } from "../server/dashboard-update-service";
import type { SalesforceInstanceEnvConfig } from "../server/admin-data-service";

function getRemoteBaseUrl(): string {
  return String(process.env.AGENT_REMOTE_BASE_URL || "").trim().replace(/\/$/, "");
}

function getRemoteToken(): string {
  return String(process.env.AGENT_REMOTE_TOKEN || "").trim();
}

export function isRemoteAgentConfigured(): boolean {
  return Boolean(getRemoteBaseUrl());
}

async function remoteRequest<T>(pathname: string, options: RequestInit = {}): Promise<T> {
  const baseUrl = getRemoteBaseUrl();
  if (!baseUrl) {
    throw new Error("AGENT_REMOTE_BASE_URL ist nicht konfiguriert.");
  }

  const token = getRemoteToken();
  const response = await fetch(`${baseUrl}${pathname}`, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    }
  });

  const payload = await response.json().catch(() => ({ error: `Remote request failed: ${response.status}` }));
  if (!response.ok) {
    throw new Error(String((payload as { error?: string }).error || `Remote request failed: ${response.status}`));
  }

  return payload as T;
}

export async function fetchRemoteAgentHealth(): Promise<HealthSnapshot> {
  return await remoteRequest<HealthSnapshot>("/api/agent/health");
}

export async function fetchRemoteAgentUpdateStatus(): Promise<DashboardUpdateStatus> {
  return await remoteRequest<DashboardUpdateStatus>("/api/agent/update-status");
}

export async function triggerRemoteAgentUpdate(): Promise<DashboardUpdateTriggerResult> {
  return await remoteRequest<DashboardUpdateTriggerResult>("/api/agent/update-now", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}"
  });
}

export async function syncRemoteAgentInstances(instances: SalesforceInstanceEnvConfig[]): Promise<{ ok: true; count: number }> {
  return await remoteRequest<{ ok: true; count: number }>("/api/agent/instances", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items: instances })
  });
}
