export interface ParsedScheduleTiming {
  days: number[];
  intervalMinutes?: number;
  startTime?: string;
}

function parseStartTime(value: unknown): { hours: number; minutes: number } | null {
  const raw = String(value || "09:00").trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    return null;
  }

  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (
    !Number.isInteger(hours)
    || !Number.isInteger(minutes)
    || hours < 0
    || hours > 23
    || minutes < 0
    || minutes > 59
  ) {
    return null;
  }

  return { hours, minutes };
}

export function parseScheduleTiming(timingDefinition?: string): ParsedScheduleTiming | null {
  const raw = String(timingDefinition || "").trim();
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }

    const record = parsed as Record<string, unknown>;
    const days = Array.isArray(record.days)
      ? record.days
          .map((value) => Number(value))
          .filter((value) => Number.isInteger(value) && value >= 0 && value <= 6)
      : [];
    const uniqueDays = Array.from(new Set(days)).sort((a, b) => a - b);

    const intervalMinutes = Number(record.intervalMinutes || 0);
    const normalizedInterval = Number.isInteger(intervalMinutes) && intervalMinutes > 0
      ? intervalMinutes
      : undefined;

    const startTime = typeof record.startTime === "string" && parseStartTime(record.startTime)
      ? record.startTime.trim()
      : undefined;

    return {
      days: uniqueDays,
      intervalMinutes: normalizedInterval,
      startTime
    };
  } catch {
    return null;
  }
}

export function calculateNextRunAtFromTiming(timingDefinition?: string, after: Date = new Date()): string | undefined {
  const timing = parseScheduleTiming(timingDefinition);
  if (!timing) {
    return undefined;
  }

  const intervalMinutes = timing.intervalMinutes;
  const start = parseStartTime(timing.startTime || "09:00");

  if (!timing.days.length) {
    if (!intervalMinutes) {
      return undefined;
    }
    const nextRun = new Date(after);
    nextRun.setMinutes(nextRun.getMinutes() + intervalMinutes);
    return nextRun.toISOString();
  }

  if (!start) {
    return undefined;
  }

  const intervalMs = intervalMinutes && intervalMinutes > 0
    ? intervalMinutes * 60 * 1000
    : 24 * 60 * 60 * 1000;

  for (let offset = 0; offset <= 60; offset += 1) {
    const dayStart = new Date(after);
    dayStart.setDate(after.getDate() + offset);
    dayStart.setHours(start.hours, start.minutes, 0, 0);

    if (!timing.days.includes(dayStart.getDay())) {
      continue;
    }

    if (intervalMs >= 24 * 60 * 60 * 1000) {
      if (dayStart.getTime() > after.getTime()) {
        return dayStart.toISOString();
      }
      continue;
    }

    const endOfDay = new Date(dayStart);
    endOfDay.setHours(23, 59, 59, 999);

    if (dayStart.getTime() > after.getTime()) {
      return dayStart.toISOString();
    }

    const elapsedMs = after.getTime() - dayStart.getTime();
    const intervalsElapsed = Math.floor(elapsedMs / intervalMs) + 1;
    const candidate = new Date(dayStart.getTime() + intervalsElapsed * intervalMs);
    if (candidate.getTime() > after.getTime() && candidate.getTime() <= endOfDay.getTime()) {
      return candidate.toISOString();
    }
  }

  return undefined;
}
