import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { HealthSnapshot } from "../server/health-snapshot";

export const AGENT_HEALTH_FILE = path.resolve(process.cwd(), "artifacts", "runtime", "agent-health.json");

export interface AgentHealthSnapshotReadResult {
  snapshot: HealthSnapshot | null;
  modifiedAtMs?: number;
  ageMs?: number;
  stale: boolean;
}

export async function writeAgentHealthSnapshot(snapshot: HealthSnapshot): Promise<void> {
  await fsp.mkdir(path.dirname(AGENT_HEALTH_FILE), { recursive: true });
  await fsp.writeFile(AGENT_HEALTH_FILE, JSON.stringify(snapshot, null, 2), "utf8");
}

export function readAgentHealthSnapshot(): HealthSnapshot | null {
  try {
    const raw = fs.readFileSync(AGENT_HEALTH_FILE, "utf8");
    const parsed = JSON.parse(raw) as HealthSnapshot;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

export function readAgentHealthSnapshotStatus(maxAgeMs: number): AgentHealthSnapshotReadResult {
  const snapshot = readAgentHealthSnapshot();
  if (!snapshot) {
    return { snapshot: null, stale: true };
  }

  try {
    const stat = fs.statSync(AGENT_HEALTH_FILE);
    const modifiedAtMs = stat.mtimeMs;
    const ageMs = Date.now() - modifiedAtMs;
    return {
      snapshot,
      modifiedAtMs,
      ageMs,
      stale: Number.isFinite(ageMs) && ageMs > maxAgeMs
    };
  } catch {
    return { snapshot, stale: true };
  }
}

export function getDefaultAgentHealthSnapshot(): HealthSnapshot {
  return {
    service: "degraded",
    scheduler: "error",
    startedAt: new Date(0).toISOString(),
    uptimeSeconds: 0,
    lastRunError: "Agent-Dienst hat noch keinen Health-Status geschrieben."
  };
}
