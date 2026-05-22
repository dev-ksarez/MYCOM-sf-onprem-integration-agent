import "dotenv/config";
import fs from "node:fs";
import https from "node:https";
import pino from "pino";
import { createAgentApiServer, createAgentRequestListener, isAgentApiEnabled } from "./agent/agent-api-server";
import { createAgentServiceRuntime } from "./agent/agent-service-runtime";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

function isEnabled(value: string | undefined, defaultValue: boolean): boolean {
  const normalized = String(value ?? (defaultValue ? "1" : "0")).trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "off";
}

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
  geraeteakteIndexEnabled: isEnabled(process.env.FILE_INDEX_PUSH_ENABLED, false),
  geraeteakteIndexIntervalMs: Number(process.env.FILE_INDEX_PUSH_INTERVAL_MS || 300_000),
  geraeteakteBasePath: process.env.FILE_BROWSE_BASE_PATH
});

async function main(): Promise<void> {
  await agentRuntime.start();

  if (isAgentApiEnabled()) {
    const agentApiPort = Number(process.env.AGENT_API_PORT || 8090);
    const agentApiServer = createAgentApiServer(() => agentRuntime.getHealthSnapshot());
    await new Promise<void>((resolve, reject) => {
      agentApiServer.once("error", reject);
      agentApiServer.listen(agentApiPort, () => {
        logger.info({ port: agentApiPort }, "Agent API service started");
        resolve();
      });
    });

    // Optionaler HTTPS-Listener (für Thumbnail-<img> aus Salesforce nötig:
    // Mixed-Content erfordert https). Aktiv, wenn Cert + Key konfiguriert sind.
    const tlsCertPath = String(process.env.AGENT_API_TLS_CERT || "").trim();
    const tlsKeyPath = String(process.env.AGENT_API_TLS_KEY || "").trim();
    if (tlsCertPath && tlsKeyPath) {
      try {
        const httpsPort = Number(process.env.AGENT_API_HTTPS_PORT || 8443);
        const tlsServer = https.createServer(
          { cert: fs.readFileSync(tlsCertPath), key: fs.readFileSync(tlsKeyPath) },
          createAgentRequestListener(() => agentRuntime.getHealthSnapshot())
        );
        await new Promise<void>((resolve, reject) => {
          tlsServer.once("error", reject);
          tlsServer.listen(httpsPort, () => {
            logger.info({ port: httpsPort }, "Agent API service started (HTTPS)");
            resolve();
          });
        });
      } catch (error) {
        logger.error({ err: error }, "HTTPS-Listener konnte nicht gestartet werden");
      }
    }
  }
}

function shutdown(signal: string): void {
  logger.info({ signal }, "Agent shutdown requested");
  agentRuntime.stop();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch((error) => {
  logger.error({ err: error }, "Agent service failed");
  process.exit(1);
});
