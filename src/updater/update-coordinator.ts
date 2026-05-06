import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export const DEFAULT_UPDATE_MANIFEST_URL =
  process.env.UPDATE_MANIFEST_URL ||
  "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json";
export const DEFAULT_WINDOWS_AGENT_SERVICE_NAME = process.env.AGENT_SERVICE_NAME || "SfOnpremIntegrationAgent";
export const DEFAULT_WINDOWS_WEB_SERVICE_NAME = process.env.WEB_SERVICE_NAME || "SfOnpremIntegrationWeb";
export const DEFAULT_WINDOWS_UPDATER_SERVICE_NAME = process.env.UPDATER_SERVICE_NAME || "SfOnpremIntegrationUpdater";
export const UPDATE_STATUS_FILE = path.resolve(process.cwd(), "logs", "dashboard-update-status.json");
export const UPDATE_REQUEST_FILE = path.resolve(process.cwd(), "artifacts", "runtime", "update-request.json");

export interface UpdateServiceStatus {
  currentVersion: string;
  targetVersion?: string;
  updateAvailable: boolean;
  supported: boolean;
  hostPlatform: string;
  manifestUrl: string;
  message: string;
  inProgress?: boolean;
  progressPercent?: number;
  stage?: string;
  updatedAt?: string;
}

export interface UpdateRequest {
  action: "apply-update";
  requestedAt: string;
  requestedBy?: string;
}

export function getHostPlatformLabel(): string {
  if (process.platform === "win32") {
    return "Windows";
  }

  if (process.platform === "darwin") {
    return "macOS";
  }

  if (process.platform === "linux") {
    return "Linux";
  }

  return process.platform;
}

export function compareVersions(left: string, right: string): number {
  const parse = (value: string): number[] =>
    String(value || "0")
      .split(".")
      .map((segment) => Number(segment.replace(/[^0-9].*$/, "") || 0));

  const leftParts = parse(left);
  const rightParts = parse(right);
  const length = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < length; index += 1) {
    const diff = Number(leftParts[index] || 0) - Number(rightParts[index] || 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return 0;
}

export async function getCurrentPackageVersion(): Promise<string> {
  const packageJsonPath = path.resolve(process.cwd(), "package.json");
  const raw = await fsp.readFile(packageJsonPath, "utf8");
  const parsed = JSON.parse(raw) as { version?: string };
  return String(parsed.version || "0.0.0").trim() || "0.0.0";
}

export async function readUpdateServiceStatus(): Promise<UpdateServiceStatus | null> {
  try {
    const raw = await fsp.readFile(UPDATE_STATUS_FILE, "utf8");
    const parsed = JSON.parse(raw) as UpdateServiceStatus;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

export async function writeUpdateServiceStatus(status: UpdateServiceStatus): Promise<void> {
  await fsp.mkdir(path.dirname(UPDATE_STATUS_FILE), { recursive: true });
  await fsp.writeFile(UPDATE_STATUS_FILE, JSON.stringify(status, null, 2), "utf8");
}

export async function queueUpdateRequest(requestedBy = "web-dashboard"): Promise<void> {
  const request: UpdateRequest = {
    action: "apply-update",
    requestedAt: new Date().toISOString(),
    requestedBy
  };
  await fsp.mkdir(path.dirname(UPDATE_REQUEST_FILE), { recursive: true });
  await fsp.writeFile(UPDATE_REQUEST_FILE, JSON.stringify(request, null, 2), "utf8");
}

export async function consumeUpdateRequest(): Promise<UpdateRequest | null> {
  try {
    const raw = await fsp.readFile(UPDATE_REQUEST_FILE, "utf8");
    await fsp.rm(UPDATE_REQUEST_FILE, { force: true });
    const parsed = JSON.parse(raw) as UpdateRequest;
    return parsed && parsed.action === "apply-update" ? parsed : null;
  } catch {
    return null;
  }
}

export function isAutoUpdaterEnabled(): boolean {
  const raw = String(process.env.AUTO_UPDATER_ENABLED || "1").trim().toLowerCase();
  return raw !== "0" && raw !== "false" && raw !== "off";
}

export function getUpdateCheckIntervalMs(): number {
  const configured = Number(process.env.UPDATE_CHECK_INTERVAL_MS || 15 * 60 * 1000);
  return Number.isFinite(configured) && configured >= 60_000 ? configured : 15 * 60 * 1000;
}

export function getUpdaterHostnameLabel(): string {
  return os.hostname() || "unknown-host";
}

export function readUpdateServiceStatusSync(): UpdateServiceStatus | null {
  try {
    const raw = fs.readFileSync(UPDATE_STATUS_FILE, "utf8");
    const parsed = JSON.parse(raw) as UpdateServiceStatus;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}
