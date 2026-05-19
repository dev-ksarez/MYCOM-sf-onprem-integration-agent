import "dotenv/config";
import pino from "pino";
import { createSaasApiServer } from "./saas/saas-api-server";
import { getSaasServerConfig } from "./saas/saas-config";
import { createSaasRepository } from "./saas/saas-repository";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const config = getSaasServerConfig();
const repository = createSaasRepository({
  databaseUrl: config.databaseUrl,
  agentTokenPepper: config.agentTokenPepper
});
const server = createSaasApiServer(repository);

server.listen(config.port, () => {
  logger.info({ port: config.port }, "SaaS API started");
});

function shutdown(signal: string): void {
  logger.info({ signal }, "SaaS API shutdown requested");
  server.close(() => {
    void repository.close().finally(() => process.exit(0));
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
