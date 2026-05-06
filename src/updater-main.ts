import "dotenv/config";
import pino from "pino";
import { createUpdaterServiceRuntime } from "./updater/updater-service-runtime";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const updaterRuntime = createUpdaterServiceRuntime(logger);

async function main(): Promise<void> {
  await updaterRuntime.start();
}

function shutdown(signal: string): void {
  logger.info({ signal }, "Updater shutdown requested");
  updaterRuntime.stop();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch((error) => {
  logger.error({ err: error }, "Updater service failed");
  process.exit(1);
});
