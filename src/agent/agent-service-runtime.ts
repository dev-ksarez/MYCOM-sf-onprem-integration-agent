import pino from "pino";
import { runDueSchedulesOnce } from "../agent/agent-runner";
import { writeAgentHealthSnapshot } from "../runtime/agent-health-store";
import { HealthSnapshot } from "../server/health-snapshot";
import { createSalesforceControlPlaneRuntime, SalesforceControlPlaneRuntime } from "./salesforce-control-plane";

interface AgentServiceRuntimeOptions {
  logger: pino.Logger;
  agentId: string;
  schedulerIntervalMs: number;
  logRetentionDays: number;
  schedulerEnabled: boolean;
  salesforceControlPlaneEnabled: boolean;
  salesforceHealthIntervalMs: number;
  salesforceCommandPollIntervalMs: number;
}

export interface AgentServiceRuntime {
  start(): Promise<void>;
  stop(): void;
  getHealthSnapshot(): HealthSnapshot;
}

// Wenn wiederholt keine aktiven Schedules gefunden werden, wird der Poll-Interval
// schrittweise erhoeht, um unnoetige Salesforce-API-Calls zu vermeiden.
// Sobald wieder Schedules gefunden werden, kehrt der Interval zum Normalwert zurueck.
const BACKOFF_STEP_MULTIPLIER = 2;
const BACKOFF_MAX_CYCLES_WITHOUT_SCHEDULES = 4; // nach 4 Leerzyklen maximaler Backoff
const BACKOFF_MAX_MULTIPLIER = 8; // maximal 8x Normalintervall

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
  let consecutiveEmptyCycles = 0;
  let salesforceControlPlane: SalesforceControlPlaneRuntime | undefined;

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

  const scheduleNextCycle = (): void => {
    // Backoff: bei wiederholten Leerzyklen Poll-Interval verdoppeln (bis zum Maximum)
    const backoffFactor = consecutiveEmptyCycles === 0
      ? 1
      : Math.min(
          Math.pow(BACKOFF_STEP_MULTIPLIER, Math.min(consecutiveEmptyCycles, BACKOFF_MAX_CYCLES_WITHOUT_SCHEDULES)),
          BACKOFF_MAX_MULTIPLIER
        );
    const nextIntervalMs = Math.round(options.schedulerIntervalMs * backoffFactor);
    if (backoffFactor > 1) {
      options.logger.debug(
        { emptyCycles: consecutiveEmptyCycles, nextIntervalMs, normalIntervalMs: options.schedulerIntervalMs },
        "No active schedules – polling less frequently (backoff)"
      );
    }

    schedulerTimer = setTimeout(() => {
      void runSchedulerCycle();
    }, nextIntervalMs);
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

      if (summary.schedulesFound === 0) {
        consecutiveEmptyCycles += 1;
      } else {
        consecutiveEmptyCycles = 0;
      }
    } catch (error) {
      lastRunStatus = "error";
      lastRunError = error instanceof Error ? error.message : "Unknown error";
      options.logger.error({ err: error }, "Scheduler cycle failed");
    } finally {
      lastRunFinishedAt = new Date().toISOString();
      isSchedulerRunning = false;
      await persistSnapshot();
    }
    // Naechsten Zyklus planen (ggf. mit Backoff wenn keine Schedules gefunden)
    scheduleNextCycle();
  };

  return {
    async start(): Promise<void> {
      await persistSnapshot();
      salesforceControlPlane = createSalesforceControlPlaneRuntime({
        logger: options.logger,
        agentId: options.agentId,
        getHealthSnapshot,
        healthIntervalMs: options.salesforceHealthIntervalMs,
        commandPollIntervalMs: options.salesforceCommandPollIntervalMs,
        enabled: options.salesforceControlPlaneEnabled
      });
      salesforceControlPlane.start();

      if (!options.schedulerEnabled) {
        options.logger.info("Agent scheduler disabled");
        return;
      }
      await runSchedulerCycle(); // plant nach Abschluss selbst den naechsten Zyklus via scheduleNextCycle()
      options.logger.info({ schedulerIntervalMs: options.schedulerIntervalMs }, "Agent service started");
    },
    stop(): void {
      if (schedulerTimer) {
        clearTimeout(schedulerTimer);
      }
      schedulerTimer = undefined;
      salesforceControlPlane?.stop();
      salesforceControlPlane = undefined;
      void persistSnapshot();
    },
    getHealthSnapshot
  };
}
