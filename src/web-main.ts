import "dotenv/config";
import pino from "pino";
import { readAgentHealthSnapshot, getDefaultAgentHealthSnapshot } from "./runtime/agent-health-store";
import { fetchRemoteAgentHealth, isRemoteAgentConfigured, syncRemoteAgentInstances } from "./runtime/remote-agent-client";
import { readConfiguredSalesforceInstances } from "./server/admin-data-service";
import { createAppServer } from "./server/app";
import { buildSystemHealthSnapshot } from "./server/health-snapshot";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const webUiPort = Number(process.env.WEB_UI_PORT || 8080);

async function getHealthSnapshot() {
  if (isRemoteAgentConfigured()) {
    return await fetchRemoteAgentHealth();
  }

  return await buildSystemHealthSnapshot(readAgentHealthSnapshot() || getDefaultAgentHealthSnapshot());
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
