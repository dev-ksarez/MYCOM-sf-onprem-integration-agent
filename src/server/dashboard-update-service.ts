import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_UPDATE_MANIFEST_URL =
  process.env.UPDATE_MANIFEST_URL ||
  "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json";
const DEFAULT_WINDOWS_SERVICE_NAME = process.env.AGENT_SERVICE_NAME || "SfOnpremIntegrationAgent";

export interface DashboardUpdateStatus {
  currentVersion: string;
  targetVersion?: string;
  updateAvailable: boolean;
  supported: boolean;
  manifestUrl: string;
  message: string;
}

export interface DashboardUpdateTriggerResult {
  ok: boolean;
  message: string;
  output?: string;
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

  try {
    const response = await fetch(DEFAULT_UPDATE_MANIFEST_URL, {
      headers: { Accept: "application/json" }
    });
    if (!response.ok) {
      return {
        currentVersion,
        updateAvailable: false,
        supported: process.platform === "win32",
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
        supported: process.platform === "win32",
        manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
        message: "Manifest enthaelt keine Zielversion"
      };
    }

    const updateAvailable = compareVersions(targetVersion, currentVersion) > 0;
    return {
      currentVersion,
      targetVersion,
      updateAvailable,
      supported: process.platform === "win32",
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: updateAvailable
        ? `Update verfuegbar: ${currentVersion} -> ${targetVersion}`
        : `Kein Update erforderlich (${currentVersion})`
    };
  } catch (error) {
    return {
      currentVersion,
      updateAvailable: false,
      supported: process.platform === "win32",
      manifestUrl: DEFAULT_UPDATE_MANIFEST_URL,
      message: error instanceof Error ? error.message : String(error)
    };
  }
}

export async function triggerDashboardUpdate(): Promise<DashboardUpdateTriggerResult> {
  if (process.platform !== "win32") {
    return {
      ok: false,
      message: "Dashboard-Update kann nur auf Windows-Agenten direkt gestartet werden."
    };
  }

  const scriptPath = path.resolve(process.cwd(), "scripts/windows/update-agent.ps1");
  return await new Promise((resolve) => {
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
        DEFAULT_UPDATE_MANIFEST_URL
      ],
      {
        cwd: process.cwd(),
        windowsHide: true
      }
    );

    let output = "";
    child.stdout.on("data", (chunk) => {
      output += String(chunk || "");
    });
    child.stderr.on("data", (chunk) => {
      output += String(chunk || "");
    });
    child.on("close", (code) => {
      resolve({
        ok: code === 0,
        message: code === 0 ? "Update-Prozess erfolgreich gestartet oder abgeschlossen." : "Update-Prozess fehlgeschlagen.",
        output: output.trim() || undefined
      });
    });
    child.on("error", (error) => {
      resolve({
        ok: false,
        message: error instanceof Error ? error.message : String(error),
        output: output.trim() || undefined
      });
    });
  });
}