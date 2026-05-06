import "dotenv/config";
import pino from "pino";
import { createAgentApiServer, isAgentApiEnabled } from "./agent/agent-api-server";
import { createAgentServiceRuntime } from "./agent/agent-service-runtime";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const agentRuntime = createAgentServiceRuntime({
  logger,
  agentId: process.env.AGENT_ID || "local-agent-01",
  schedulerIntervalMs: Number(process.env.SCHEDULER_INTERVAL_MS || 60_000),
  schedulerEnabled: (() => {
    const configured = String(process.env.AGENT_SCHEDULER_ENABLED || "1").trim().toLowerCase();
    return configured !== "0" && configured !== "false" && configured !== "off";
  })(),
  logRetentionDays: (() => {
    const configured = Number(process.env.SF_LOG_RETENTION_DAYS?.trim() || "30");
    return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 0;
  })()
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
