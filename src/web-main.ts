import "dotenv/config";
import pino from "pino";
import { readAgentHealthSnapshotStatus } from "./runtime/agent-health-store";
import { fetchRemoteAgentHealth, isRemoteAgentConfigured, syncRemoteAgentInstances } from "./runtime/remote-agent-client";
import { readConfiguredSalesforceInstances } from "./server/admin-data-service";
import { createAppServer } from "./server/app";
import { buildSystemHealthSnapshot, HealthSnapshot } from "./server/health-snapshot";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const webUiPort = Number(process.env.WEB_UI_PORT || 8080);
const webStartedAt = new Date();
const agentHealthMaxAgeMs = Number(process.env.AGENT_HEALTH_MAX_AGE_MS || 3 * 60 * 1000);

function getWebOnlyHealthSnapshot(): HealthSnapshot {
  return {
    service: "ok",
    scheduler: "idle",
    startedAt: webStartedAt.toISOString(),
    uptimeSeconds: process.uptime(),
    lastRunError: undefined
  };
}

async function getHealthSnapshot() {
  if (isRemoteAgentConfigured()) {
    return await fetchRemoteAgentHealth();
  }

  const agentHealth = readAgentHealthSnapshotStatus(agentHealthMaxAgeMs);
  if (agentHealth.snapshot && !agentHealth.stale) {
    return await buildSystemHealthSnapshot(agentHealth.snapshot);
  }

  return await buildSystemHealthSnapshot(getWebOnlyHealthSnapshot());
}

async function main(): Promise<void> {
  if (isRemoteAgentConfigured()) {
    try {
      const instances = readConfiguredSalesforceInstances();
      await syncRemoteAgentInstances(instances);
      logger.info({ count: instances.length }, "Remote agent instance configuration synchronized");
    } catch (error) {
      logger.warn({ err: error }, "Remote agent instance configuration sync failed");
    }
  }

  const server = createAppServer(getHealthSnapshot);
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
      logger.info({ port: webUiPort }, "Web dashboard service started");
      resolve();
    });
  });
}

main().catch((error) => {
  logger.error({ err: error }, "Web dashboard service failed");
  process.exit(1);
});
