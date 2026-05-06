import {
  compareVersions,
  DEFAULT_UPDATE_MANIFEST_URL,
  getCurrentPackageVersion,
  getHostPlatformLabel,
  queueUpdateRequest,
  readUpdateServiceStatus,
  readUpdateServiceStatusSync,
  UpdateServiceStatus,
  writeUpdateServiceStatus
} from "../updater/update-coordinator";
import { fetchRemoteAgentUpdateStatus, isRemoteAgentConfigured, triggerRemoteAgentUpdate } from "../runtime/remote-agent-client";

const UPDATE_PREPARING_STALE_MS = 90 * 1000;
const UPDATE_RUNNING_STALE_MS = 15 * 60 * 1000;

export type DashboardUpdateStatus = UpdateServiceStatus;

export interface DashboardUpdateTriggerResult {
  ok: boolean;
  message: string;
  output?: string;
}

function normalizeProgressPercent(value: unknown): number | undefined {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return undefined;
  }

  return Math.max(0, Math.min(100, Math.round(numericValue)));
}

function getProgressAgeMs(progress: DashboardUpdateStatus | null): number | null {
  if (!progress?.updatedAt) {
    return null;
  }

  const timestamp = Date.parse(progress.updatedAt);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return Math.max(0, Date.now() - timestamp);
}

function isRunningProgressStale(progress: DashboardUpdateStatus | null): boolean {
  if (!progress || progress.inProgress !== true) {
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

function isTerminalProgressState(progress: DashboardUpdateStatus | null): boolean {
  return Boolean(progress && progress.inProgress !== true);
}

export async function getDashboardUpdateStatus(): Promise<DashboardUpdateStatus> {
  if (isRemoteAgentConfigured()) {
    return await fetchRemoteAgentUpdateStatus();
  }

  const currentVersion = await getCurrentPackageVersion();
  const supported = process.platform === "win32";
  const hostPlatform = getHostPlatformLabel();
  let progress = await readUpdateServiceStatus();

  if (isRunningProgressStale(progress)) {
    progress = {
      ...(progress || {
        currentVersion,
        updateAvailable: false,
        supported,
        hostPlatform,
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: ""
      }),
      stage: "failed",
      progressPercent: normalizeProgressPercent(progress?.progressPercent) ?? 100,
      updatedAt: new Date().toISOString(),
      message: "Der Update-Status ist veraltet. Bitte Update erneut pruefen oder neu starten.",
      inProgress: false
    };
    await writeUpdateServiceStatus(progress);
  }

  if (progress?.inProgress) {
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
  if (isRemoteAgentConfigured()) {
    return await triggerRemoteAgentUpdate();
  }

  const currentStatus = readUpdateServiceStatusSync();
  if (currentStatus?.inProgress) {
    return {
      ok: false,
      message: "Es laeuft bereits ein Update."
    };
  }

  await queueUpdateRequest("web-dashboard");
  await writeUpdateServiceStatus({
    currentVersion: await getCurrentPackageVersion(),
    targetVersion: currentStatus?.targetVersion,
    updateAvailable: Boolean(currentStatus?.updateAvailable),
    supported: process.platform === "win32",
    hostPlatform: getHostPlatformLabel(),
    manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
    message: "Update wurde beim AutoUpdater-Dienst angefordert.",
    inProgress: false,
    updatedAt: new Date().toISOString()
  });

  return {
    ok: true,
    message: "Update wurde beim AutoUpdater-Dienst angefordert."
  };
}
