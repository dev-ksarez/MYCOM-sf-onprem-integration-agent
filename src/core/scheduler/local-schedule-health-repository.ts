import fs from "node:fs";
import path from "node:path";

export interface LocalScheduleHealthItem {
  consecutiveFailures: number;
  lastError?: string;
  lastFailedAt?: string;
  autoDisabled?: boolean;
  autoDisabledAt?: string;
}

export interface LocalScheduleHealthDocument {
  version: number;
  updatedAt: string;
  schedules: Record<string, LocalScheduleHealthItem>;
}

export class LocalScheduleHealthRepository {
  private readonly filePath: string;

  constructor(filePath?: string) {
    this.filePath =
      filePath ||
      process.env.SF_SCHEDULE_HEALTH_FILE ||
      path.resolve(process.cwd(), "artifacts/schedule-health.json");
  }

  public read(): Record<string, LocalScheduleHealthItem> {
    try {
      if (!fs.existsSync(this.filePath)) {
        return {};
      }

      const raw = fs.readFileSync(this.filePath, "utf8").trim();
      if (!raw) {
        return {};
      }

      const parsed = JSON.parse(raw) as unknown;
      const schedulesCandidate =
        parsed && typeof parsed === "object" && !Array.isArray(parsed) && "schedules" in parsed
          ? (parsed as { schedules?: unknown }).schedules
          : parsed;

      if (!schedulesCandidate || typeof schedulesCandidate !== "object" || Array.isArray(schedulesCandidate)) {
        return {};
      }

      return Object.entries(schedulesCandidate).reduce<Record<string, LocalScheduleHealthItem>>((acc, [scheduleId, item]) => {
        if (!item || typeof item !== "object" || Array.isArray(item)) {
          return acc;
        }

        const candidate = item as Record<string, unknown>;
        acc[scheduleId] = {
          consecutiveFailures: Math.max(0, Number(candidate.consecutiveFailures || 0) || 0),
          lastError: typeof candidate.lastError === "string" ? candidate.lastError : undefined,
          lastFailedAt: typeof candidate.lastFailedAt === "string" ? candidate.lastFailedAt : undefined,
          autoDisabled: candidate.autoDisabled === true,
          autoDisabledAt: typeof candidate.autoDisabledAt === "string" ? candidate.autoDisabledAt : undefined
        };
        return acc;
      }, {});
    } catch {
      return {};
    }
  }

  public write(store: Record<string, LocalScheduleHealthItem>): void {
    const directory = path.dirname(this.filePath);
    fs.mkdirSync(directory, { recursive: true });
    const document: LocalScheduleHealthDocument = {
      version: 1,
      updatedAt: new Date().toISOString(),
      schedules: store
    };
    fs.writeFileSync(this.filePath, JSON.stringify(document, null, 2), "utf8");
  }

  public markSuccess(scheduleId: string): void {
    const store = this.read();
    const existing = store[scheduleId];
    if (!existing) {
      return;
    }

    if ((existing.consecutiveFailures || 0) === 0 && existing.autoDisabled !== true) {
      return;
    }

    store[scheduleId] = {
      ...existing,
      consecutiveFailures: 0,
      autoDisabled: false,
      autoDisabledAt: undefined,
      lastError: undefined
    };
    this.write(store);
  }

  public markFailure(scheduleId: string, errorMessage: string): LocalScheduleHealthItem {
    const store = this.read();
    const existing = store[scheduleId] || { consecutiveFailures: 0 };
    const updated: LocalScheduleHealthItem = {
      ...existing,
      consecutiveFailures: Math.max(0, Number(existing.consecutiveFailures || 0) || 0) + 1,
      lastError: errorMessage,
      lastFailedAt: new Date().toISOString()
    };
    store[scheduleId] = updated;
    this.write(store);
    return updated;
  }

  public markAutoDisabled(scheduleId: string): void {
    const store = this.read();
    const existing = store[scheduleId] || { consecutiveFailures: 0 };
    store[scheduleId] = {
      ...existing,
      autoDisabled: true,
      autoDisabledAt: new Date().toISOString()
    };
    this.write(store);
  }
}
