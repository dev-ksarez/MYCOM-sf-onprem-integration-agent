import "dotenv/config";
import pino from "pino";
import { runDueSchedulesOnce } from "./agent/agent-runner";
import { createAppServer, HealthSnapshot } from "./server/app";

const logger = pino({
  level: process.env.LOG_LEVEL || "info"
});

const agentId = process.env.AGENT_ID || "local-agent-01";
const startedAt = new Date();
const schedulerIntervalMs = Number(process.env.SCHEDULER_INTERVAL_MS || 60_000);
const webUiEnabled = process.env.WEB_UI_ENABLED === "1" || process.env.WEB_UI_ENABLED === "true";
const webUiPort = Number(process.env.WEB_UI_PORT || 8080);

let schedulerTimer: NodeJS.Timeout | undefined;
let isSchedulerRunning = false;
let lastRunStartedAt: string | undefined;
let lastRunFinishedAt: string | undefined;
let lastRunStatus: "success" | "error" | undefined;
let lastRunError: string | undefined;
let schedulesFound: number | undefined;
let dueSchedules: number | undefined;
let processedSchedules: number | undefined;

function getHealthSnapshot(): HealthSnapshot {
  const service = lastRunStatus === "error" ? "degraded" : "ok";
  const scheduler = isSchedulerRunning
    ? "running"
    : lastRunStatus === "error"
      ? "error"
      : "idle";

  return {
    service,
    scheduler,
    startedAt: startedAt.toISOString(),
    uptimeSeconds: (Date.now() - startedAt.getTime()) / 1000,
    lastRunStartedAt,
    lastRunFinishedAt,
    lastRunStatus,
    lastRunError,
    schedulesFound,
    dueSchedules,
    processedSchedules
  };
}

async function runSchedulerCycle(): Promise<void> {
  if (isSchedulerRunning) {
    logger.warn("Scheduler cycle already running, skipping overlapping trigger");
    return;
  }

  isSchedulerRunning = true;
  lastRunStartedAt = new Date().toISOString();
  lastRunError = undefined;

  try {
    const summary = await runDueSchedulesOnce(logger, agentId);
    schedulesFound = summary.schedulesFound;
    dueSchedules = summary.dueSchedules;
    processedSchedules = summary.processedSchedules;
    lastRunStatus = "success";
  } catch (error) {
    lastRunStatus = "error";
    lastRunError = error instanceof Error ? error.message : "Unknown error";
    logger.error({ err: error }, "Scheduler cycle failed");
  } finally {
    lastRunFinishedAt = new Date().toISOString();
    isSchedulerRunning = false;
  }
}

async function main(): Promise<void> {
  if (webUiEnabled) {
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
        logger.info({ port: webUiPort }, "Web UI and health API started");
        resolve();
      });
    });
  }

  await runSchedulerCycle();
  schedulerTimer = setInterval(() => {
    void runSchedulerCycle();
  }, schedulerIntervalMs);

  logger.info({ schedulerIntervalMs, webUiEnabled }, "Agent service started");
}

function shutdown(signal: string): void {
  logger.info({ signal }, "Shutdown requested");
  if (schedulerTimer) {
    clearInterval(schedulerTimer);
  }
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch((error) => {
  logger.error({ err: error }, "Application failed");
  process.exit(1);
});