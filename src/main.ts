import "dotenv/config";
import pino from "pino";
import { createAgentServiceRuntime } from "./agent/agent-service-runtime";
import { buildSystemHealthSnapshot } from "./server/health-snapshot";
import { createAppServer } from "./server/app";
import { HealthSnapshot } from "./server/health-snapshot";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

function isEnabled(value: string | undefined, defaultValue: boolean): boolean {
  const normalized = String(value ?? (defaultValue ? "1" : "0")).trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "off";
}

const webUiEnabled = process.env.WEB_UI_ENABLED === "1" || process.env.WEB_UI_ENABLED === "true";
const webUiPort = Number(process.env.WEB_UI_PORT || 8080);
const agentRuntime = createAgentServiceRuntime({
  logger,
  agentId: process.env.AGENT_ID || "local-agent-01",
  schedulerIntervalMs: Number(process.env.SCHEDULER_INTERVAL_MS || 60_000),
  schedulerEnabled: isEnabled(process.env.AGENT_SCHEDULER_ENABLED, true),
  logRetentionDays: (() => {
    const configured = Number(process.env.SF_LOG_RETENTION_DAYS?.trim() || "30");
    return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 0;
  })(),
  salesforceControlPlaneEnabled: isEnabled(process.env.AGENT_SALESFORCE_CONTROL_PLANE_ENABLED, true),
  salesforceHealthIntervalMs: Number(process.env.AGENT_HEALTH_PULSE_INTERVAL_MS || 300_000),
  salesforceCommandPollIntervalMs: Number(process.env.AGENT_COMMAND_POLL_INTERVAL_MS || 60_000),
  saasControlPlaneEnabled: isEnabled(process.env.AGENT_SAAS_CONTROL_PLANE_ENABLED, false)
});

async function main(): Promise<void> {
  if (webUiEnabled) {
    const server = createAppServer(async (): Promise<HealthSnapshot> => await buildSystemHealthSnapshot(agentRuntime.getHealthSnapshot()));
    await new Promise<void>((resolve, reject) => {
      server.once("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "EADDRINUSE") {
          reject(
            new Error(
              `WEB_UI_PORT ${webUiPort} ist bereits belegt. Bitte laufenden Prozess beenden oder WEB_UI_PORT auf einen freien Port setzen.`
            )
          );
          return;
        }

        reject(error);
      });

      server.listen(webUiPort, () => {
        logger.info({ port: webUiPort }, "Web UI and health API started");
        resolve();
      });
    });
  }

  await agentRuntime.start();
  logger.info({ webUiEnabled }, "Legacy combined service started");
}

function shutdown(signal: string): void {
  logger.info({ signal }, "Shutdown requested");
  agentRuntime.stop();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch((error) => {
  logger.error({ err: error }, "Application failed");
  process.exit(1);
});
