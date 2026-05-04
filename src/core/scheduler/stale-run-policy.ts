const DEFAULT_STALE_RUN_INACTIVITY_MINUTES = 10;
const defaultScheduleInactivityOverrides = new Map<string, number>([
  ["SCH6", 3]
]);

function normalizeSchedulePolicyKey(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/\d+/g, (digits) => String(Number(digits)))
    .replace(/[^A-Z0-9]+/g, "");
}

function parseScheduleInactivityOverrides(): Map<string, number> {
  const raw = String(process.env.SF_STALE_RUN_INACTIVITY_OVERRIDES || "").trim();
  const overrides = new Map<string, number>();
  if (!raw) {
    return overrides;
  }

  raw
    .split(/[;,]/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .forEach((entry) => {
      const separatorIndex = entry.includes("=") ? entry.indexOf("=") : entry.indexOf(":");
      if (separatorIndex <= 0) {
        return;
      }

      const key = normalizeSchedulePolicyKey(entry.slice(0, separatorIndex));
      const minutes = Number(entry.slice(separatorIndex + 1).trim());
      if (!key || !Number.isFinite(minutes) || minutes <= 0) {
        return;
      }

      overrides.set(key, minutes);
    });

  return overrides;
}

const scheduleInactivityOverrides = parseScheduleInactivityOverrides();

export function getDefaultStaleRunInactivityThresholdMinutes(): number {
  const configured = Number(process.env.SF_STALE_RUN_INACTIVITY_MINUTES?.trim() || String(DEFAULT_STALE_RUN_INACTIVITY_MINUTES));
  return Number.isFinite(configured) && configured > 0 ? configured : DEFAULT_STALE_RUN_INACTIVITY_MINUTES;
}

export function getStaleRunInactivityThresholdMinutesForSchedule(scheduleId?: string, scheduleName?: string): number {
  const scheduleKeys = [scheduleName, scheduleId]
    .map((value) => normalizeSchedulePolicyKey(value))
    .filter(Boolean);

  for (const key of scheduleKeys) {
    const override = scheduleInactivityOverrides.get(key);
    if (override) {
      return override;
    }

    const defaultOverride = defaultScheduleInactivityOverrides.get(key);
    if (defaultOverride) {
      return defaultOverride;
    }
  }

  return getDefaultStaleRunInactivityThresholdMinutes();
}