import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_UPDATE_MANIFEST_URL =
  process.env.UPDATE_MANIFEST_URL ||
  "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json";
const DEFAULT_WINDOWS_SERVICE_NAME = process.env.AGENT_SERVICE_NAME || "SfOnpremIntegrationAgent";
const UPDATE_PROGRESS_FILE = path.resolve(process.cwd(), "logs", "dashboard-update-status.json");
const UPDATE_PREPARING_STALE_MS = 90 * 1000;
const UPDATE_RUNNING_STALE_MS = 15 * 60 * 1000;

export interface DashboardUpdateStatus {
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

export interface DashboardUpdateTriggerResult {
  ok: boolean;
  message: string;
  output?: string;
}

interface DashboardUpdateProgressState {
  state: "idle" | "running" | "completed" | "failed";
  message: string;
  progressPercent?: number;
  stage?: string;
  targetVersion?: string;
  updatedAt: string;
}

async function readUpdateProgress(): Promise<DashboardUpdateProgressState | null> {
  try {
    const raw = await fs.readFile(UPDATE_PROGRESS_FILE, "utf8");
    const parsed = JSON.parse(raw) as DashboardUpdateProgressState;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

async function writeUpdateProgress(progress: DashboardUpdateProgressState): Promise<void> {
  await fs.mkdir(path.dirname(UPDATE_PROGRESS_FILE), { recursive: true });
  await fs.writeFile(UPDATE_PROGRESS_FILE, JSON.stringify(progress, null, 2), "utf8");
}

function normalizeProgressPercent(value: unknown): number | undefined {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return undefined;
  }

  return Math.max(0, Math.min(100, Math.round(numericValue)));
}

function getProgressAgeMs(progress: DashboardUpdateProgressState | null): number | null {
  if (!progress?.updatedAt) {
    return null;
  }

  const timestamp = Date.parse(progress.updatedAt);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return Math.max(0, Date.now() - timestamp);
}

function isRunningProgressStale(progress: DashboardUpdateProgressState | null): boolean {
  if (!progress || progress.state !== "running") {
    return false;
  }

  const ageMs = getProgressAgeMs(progress);
  if (ageMs === null) {
    return false;
  }

  const stage = String(progress.stage || "").trim().toLowerCase();
  if (stage === "start" || stage === "init") {
    return ageMs > UPDATE_PREPARING_STALE_MS;
  }

  return ageMs > UPDATE_RUNNING_STALE_MS;
}

function isTerminalProgressState(progress: DashboardUpdateProgressState | null): boolean {
  return progress?.state === "completed" || progress?.state === "failed";
}

function getHostPlatformLabel(): string {
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

function compareVersions(left: string, right: string): number {
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

async function getCurrentPackageVersion(): Promise<string> {
  const packageJsonPath = path.resolve(process.cwd(), "package.json");
  const raw = await fs.readFile(packageJsonPath, "utf8");
  const parsed = JSON.parse(raw) as { version?: string };
  return String(parsed.version || "0.0.0").trim() || "0.0.0";
}

export async function getDashboardUpdateStatus(): Promise<DashboardUpdateStatus> {
  const currentVersion = await getCurrentPackageVersion();
  const supported = process.platform === "win32";
  const hostPlatform = getHostPlatformLabel();
  let progress = await readUpdateProgress();

  if (isRunningProgressStale(progress)) {
    progress = {
      ...progress,
      state: "failed",
      stage: "failed",
      progressPercent: normalizeProgressPercent(progress?.progressPercent) ?? 100,
      updatedAt: new Date().toISOString(),
      message: "Der Update-Status ist veraltet. Bitte Update erneut pruefen oder neu starten."
    };
    await writeUpdateProgress(progress);
  }

  if (progress?.state === "running") {
    return {
      currentVersion,
      targetVersion: progress.targetVersion,
      updateAvailable: false,
      supported,
      hostPlatform,
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: progress.message || "Update wird ausgefuehrt.",
      inProgress: true,
      progressPercent: normalizeProgressPercent(progress.progressPercent),
      stage: progress.stage,
      updatedAt: progress.updatedAt
    };
  }

  const terminalProgress = isTerminalProgressState(progress) ? progress : null;

  try {
    const response = await fetch(DEFAULT_UPDATE_MANIFEST_URL, {
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      return {
        currentVersion,
        updateAvailable: false,
        supported,
        hostPlatform,
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: `Manifest nicht erreichbar (${response.status})`
      };
    }

    const manifest = (await response.json()) as { version?: string };
    const targetVersion = String(manifest.version || "").trim();
    if (!targetVersion) {
      return {
        currentVersion,
        updateAvailable: false,
        supported,
        hostPlatform,
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: "Manifest enthaelt keine Zielversion"
      };
    }

    const updateAvailable = compareVersions(targetVersion, currentVersion) > 0;
    const baseMessage = updateAvailable
      ? `Update verfuegbar: ${currentVersion} -> ${targetVersion}`
      : `Kein Update erforderlich (${currentVersion})`;
    return {
      currentVersion,
      targetVersion,
      updateAvailable,
      supported,
      hostPlatform,
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: !supported
        ? `${baseMessage}. Der Direktstart richtet sich nach dem Agent-Host, nicht nach dem Browser-Client. Aktueller Agent-Host: ${hostPlatform}.`
        : baseMessage,
      inProgress: false,
      progressPercent: undefined,
      stage: undefined,
      updatedAt: terminalProgress?.updatedAt
    };
  } catch (error) {
    return {
      currentVersion,
      updateAvailable: false,
      supported,
      hostPlatform,
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: terminalProgress?.message || (error instanceof Error ? error.message : String(error)),
      inProgress: false,
      progressPercent: undefined,
      stage: undefined,
      updatedAt: terminalProgress?.updatedAt
    };
  }
}

export async function triggerDashboardUpdate(): Promise<DashboardUpdateTriggerResult> {
  if (process.platform !== "win32") {
    return {
      ok: false,
      message: `Dashboard-Update kann nur direkt gestartet werden, wenn der Agent-Host auf Windows laeuft. Aktueller Agent-Host: ${getHostPlatformLabel()}.`
    };
  }

  const scriptPath = path.resolve(process.cwd(), "scripts/windows/update-agent.ps1");
  await writeUpdateProgress({
    state: "running",
    message: "Update wird vorbereitet.",
    progressPercent: 5,
    stage: "start",
    updatedAt: new Date().toISOString()
  });

  return await new Promise((resolve) => {
    let settled = false;
    const child = spawn(
      "powershell.exe",
      [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        scriptPath,
        "-ServiceName",
        DEFAULT_WINDOWS_SERVICE_NAME,
        "-AppRoot",
        process.cwd(),
        "-UpdateManifestUrl",
        DEFAULT_UPDATE_MANIFEST_URL,
        "-StatusFilePath",
        UPDATE_PROGRESS_FILE
      ],
      {
        cwd: process.cwd(),
        windowsHide: true,
        detached: true,
        stdio: "ignore"
      }
    );

    child.once("error", (error) => {
      if (settled) {
        return;
      }

      settled = true;
      resolve({
        ok: false,
        message: error instanceof Error ? error.message : String(error)
      });
    });

    child.unref();

    if (!settled) {
      settled = true;
      resolve({
        ok: true,
        message: "Update-Prozess im Hintergrund gestartet. Der Agent-Dienst kann dabei kurz beendet und neu gestartet werden."
      });
    }
  });
}