import { spawn } from "node:child_process";
import pino from "pino";
import {
  compareVersions,
  consumeUpdateRequest,
  DEFAULT_UPDATE_MANIFEST_URL,
  DEFAULT_WINDOWS_AGENT_SERVICE_NAME,
  DEFAULT_WINDOWS_UPDATER_SERVICE_NAME,
  DEFAULT_WINDOWS_WEB_SERVICE_NAME,
  getCurrentPackageVersion,
  getHostPlatformLabel,
  getUpdateCheckIntervalMs,
  isAutoUpdaterEnabled,
  UPDATE_STATUS_FILE,
  writeUpdateServiceStatus
} from "./update-coordinator";

export interface UpdaterServiceRuntime {
  start(): Promise<void>;
  stop(): void;
}

export function createUpdaterServiceRuntime(logger: pino.Logger): UpdaterServiceRuntime {
  let timer: NodeJS.Timeout | undefined;
  let running = false;

  const runUpdateScript = async (targetVersion: string, reason: string): Promise<void> => {
    await writeUpdateServiceStatus({
      currentVersion: await getCurrentPackageVersion(),
      targetVersion,
      updateAvailable: false,
      supported: process.platform === "win32",
      hostPlatform: getHostPlatformLabel(),
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: reason,
      inProgress: true,
      progressPercent: 5,
      stage: "start",
      updatedAt: new Date().toISOString()
    });

    if (process.platform !== "win32") {
      await writeUpdateServiceStatus({
        currentVersion: await getCurrentPackageVersion(),
        targetVersion,
        updateAvailable: true,
        supported: false,
        hostPlatform: getHostPlatformLabel(),
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: "Direkte automatische Updates sind derzeit nur unter Windows implementiert.",
        inProgress: false,
        updatedAt: new Date().toISOString()
      });
      return;
    }

    await new Promise<void>((resolve, reject) => {
      let stderrOutput = "";
      
      const child = spawn(
        "powershell.exe",
        [
          "-NoProfile",
          "-ExecutionPolicy",
          "Bypass",
          "-File",
          "scripts/windows/update-agent.ps1",
          "-ServiceName",
          DEFAULT_WINDOWS_AGENT_SERVICE_NAME,
          "-WebServiceName",
          DEFAULT_WINDOWS_WEB_SERVICE_NAME,
          "-UpdaterServiceName",
          DEFAULT_WINDOWS_UPDATER_SERVICE_NAME,
          "-AppRoot",
          process.cwd(),
          "-UpdateManifestUrl",
          DEFAULT_UPDATE_MANIFEST_URL,
          "-StatusFilePath",
          UPDATE_STATUS_FILE
        ],
        {
          cwd: process.cwd(),
          windowsHide: true,
          stdio: ["ignore", "ignore", "pipe"]
        }
      );

      if (child.stderr) {
        child.stderr.on("data", (data: Buffer) => {
          stderrOutput += data.toString();
        });
      }

      child.once("error", reject);
      child.once("exit", (code) => {
        if (code === 0) {
          resolve();
          return;
        }

        const errorMsg = stderrOutput.trim() 
          ? `Updater script exited with code ${code ?? -1}: ${stderrOutput}`
          : `Updater script exited with code ${code ?? -1}`;
        
        reject(new Error(errorMsg));
      });
    });
  };

  const runCycle = async (): Promise<void> => {
    if (running) {
      return;
    }

    running = true;
    try {
      const currentVersion = await getCurrentPackageVersion();
      const hostPlatform = getHostPlatformLabel();
      const supported = process.platform === "win32";
      const request = await consumeUpdateRequest();
      const autoUpdaterEnabled = isAutoUpdaterEnabled();

      if (!request && !autoUpdaterEnabled) {
        await writeUpdateServiceStatus({
          currentVersion,
          updateAvailable: false,
          supported,
          hostPlatform,
          manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
          message: "Automatische Update-Pruefung ist deaktiviert.",
          inProgress: false,
          updatedAt: new Date().toISOString()
        });
        return;
      }

      const response = await fetch(DEFAULT_UPDATE_MANIFEST_URL, {
        headers: { Accept: "application/json" }
      });

      if (!response.ok) {
        await writeUpdateServiceStatus({
          currentVersion,
          updateAvailable: false,
          supported,
          hostPlatform,
          manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
          message: `Manifest nicht erreichbar (${response.status})`,
          inProgress: false,
          updatedAt: new Date().toISOString()
        });
        return;
      }

      const manifest = (await response.json()) as { version?: string };
      const targetVersion = String(manifest.version || "").trim();
      const updateAvailable = Boolean(targetVersion) && compareVersions(targetVersion, currentVersion) > 0;

      if (!targetVersion) {
        await writeUpdateServiceStatus({
          currentVersion,
          updateAvailable: false,
          supported,
          hostPlatform,
          manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
          message: "Manifest enthaelt keine Zielversion",
          inProgress: false,
          updatedAt: new Date().toISOString()
        });
        return;
      }

      const shouldApply = supported && updateAvailable && (Boolean(request) || autoUpdaterEnabled);
      if (shouldApply) {
        const reason = request
          ? `Update wird auf Anforderung von ${request.requestedBy || "unknown"} gestartet.`
          : `Automatisches Update auf ${targetVersion} wird gestartet.`;
        await runUpdateScript(targetVersion, reason);
        return;
      }

      await writeUpdateServiceStatus({
        currentVersion,
        targetVersion,
        updateAvailable,
        supported,
        hostPlatform,
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: updateAvailable
          ? `Update verfuegbar: ${currentVersion} -> ${targetVersion}`
          : `Kein Update erforderlich (${currentVersion})`,
        inProgress: false,
        updatedAt: new Date().toISOString()
      });
    } catch (error) {
      logger.error({ err: error }, "Updater cycle failed");
      await writeUpdateServiceStatus({
        currentVersion: await getCurrentPackageVersion().catch(() => "0.0.0"),
        updateAvailable: false,
        supported: process.platform === "win32",
        hostPlatform: getHostPlatformLabel(),
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: error instanceof Error ? error.message : String(error),
        inProgress: false,
        updatedAt: new Date().toISOString()
      });
    } finally {
      running = false;
    }
  };

  return {
    async start(): Promise<void> {
      await runCycle();
      timer = setInterval(() => {
        void runCycle();
      }, getUpdateCheckIntervalMs());
      logger.info({ intervalMs: getUpdateCheckIntervalMs() }, "Updater service started");
    },
    stop(): void {
      if (timer) {
        clearInterval(timer);
      }
      timer = undefined;
    }
  };
}
