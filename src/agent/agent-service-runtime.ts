import pino from "pino";
import { runDueSchedulesOnce } from "../agent/agent-runner";
import { writeAgentHealthSnapshot } from "../runtime/agent-health-store";
import { HealthSnapshot } from "../server/health-snapshot";

interface AgentServiceRuntimeOptions {
  logger: pino.Logger;
  agentId: string;
  schedulerIntervalMs: number;
  logRetentionDays: number;
  schedulerEnabled: boolean;
}

export interface AgentServiceRuntime {
  start(): Promise<void>;
  stop(): void;
  getHealthSnapshot(): HealthSnapshot;
}

export function createAgentServiceRuntime(options: AgentServiceRuntimeOptions): AgentServiceRuntime {
  const startedAt = new Date();
  let schedulerTimer: NodeJS.Timeout | undefined;
  let isSchedulerRunning = false;
  let lastRunStartedAt: string | undefined;
  let lastRunFinishedAt: string | undefined;
  let lastRunStatus: "success" | "error" | undefined;
  let lastRunError: string | undefined;
  let schedulesFound: number | undefined;
  let dueSchedules: number | undefined;
  let processedSchedules: number | undefined;

  const getHealthSnapshot = (): HealthSnapshot => {
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
      processedSchedules,
      logRetentionDays: options.logRetentionDays
    };
  };

  const persistSnapshot = async (): Promise<void> => {
    await writeAgentHealthSnapshot(getHealthSnapshot());
  };

  const runSchedulerCycle = async (): Promise<void> => {
    if (isSchedulerRunning) {
      options.logger.warn("Scheduler cycle already running, skipping overlapping trigger");
      return;
    }

    isSchedulerRunning = true;
    lastRunStartedAt = new Date().toISOString();
    lastRunError = undefined;
    await persistSnapshot();

    try {
      const summary = await runDueSchedulesOnce(options.logger, options.agentId);
      schedulesFound = summary.schedulesFound;
      dueSchedules = summary.dueSchedules;
      processedSchedules = summary.processedSchedules;
      lastRunStatus = "success";
    } catch (error) {
      lastRunStatus = "error";
      lastRunError = error instanceof Error ? error.message : "Unknown error";
      options.logger.error({ err: error }, "Scheduler cycle failed");
    } finally {
      lastRunFinishedAt = new Date().toISOString();
      isSchedulerRunning = false;
      await persistSnapshot();
    }
  };

  return {
    async start(): Promise<void> {
      await persistSnapshot();
      if (!options.schedulerEnabled) {
        options.logger.info("Agent scheduler disabled");
        return;
      }
      await runSchedulerCycle();
      schedulerTimer = setInterval(() => {
        void runSchedulerCycle();
      }, options.schedulerIntervalMs);
      options.logger.info({ schedulerIntervalMs: options.schedulerIntervalMs }, "Agent service started");
    },
    stop(): void {
      if (schedulerTimer) {
        clearInterval(schedulerTimer);
      }
      schedulerTimer = undefined;
      void persistSnapshot();
    },
    getHealthSnapshot
  };
}
