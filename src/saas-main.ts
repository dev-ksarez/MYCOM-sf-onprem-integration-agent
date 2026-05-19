import "dotenv/config";
import pino from "pino";
import { createSaasApiServer } from "./saas/saas-api-server";
import { getSaasServerConfig } from "./saas/saas-config";
import { createSaasRepository } from "./saas/saas-repository";
import { hashPassword } from "./saas/saas-auth";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const config = getSaasServerConfig();
const repository = createSaasRepository({
  databaseUrl: config.databaseUrl,
  agentTokenPepper: config.agentTokenPepper
});

if (config.adminPassword) {
  hashPassword(config.adminPassword)
    .then((hash) => repository.ensureSystemAdmin(config.adminEmail, hash))
    .then(() => logger.info({ email: config.adminEmail }, "system admin ensured"))
    .catch((err) => logger.error({ err }, "failed to ensure system admin"));
}

const server = createSaasApiServer(repository, config);

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
