export type SchedulerDay = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";

export interface ImportProfileScheduleRuleLike {
  days: SchedulerDay[];
  startTime: string;
  endTime: string;
  intervalMinutes: number;
}

function parseTimeOfDayToMinutes(value: string, label: string): number {
  if (!/^([01]\d|2[0-3]):([0-5]\d)$/.test(value)) {
    throw new Error(`Invalid time format for ${label}: ${value}. Expected HH:mm`);
  }

  const [hourPart, minutePart] = value.split(":");
  return Number(hourPart) * 60 + Number(minutePart);
}

function getDayName(dayIndex: number): SchedulerDay {
  if (dayIndex === 0) {
    return "sun";
  }
  if (dayIndex === 1) {
    return "mon";
  }
  if (dayIndex === 2) {
    return "tue";
  }
  if (dayIndex === 3) {
    return "wed";
  }
  if (dayIndex === 4) {
    return "thu";
  }
  if (dayIndex === 5) {
    return "fri";
  }

  return "sat";
}

function getPreviousDayName(day: SchedulerDay): SchedulerDay {
  if (day === "sun") {
    return "sat";
  }
  if (day === "mon") {
    return "sun";
  }
  if (day === "tue") {
    return "mon";
  }
  if (day === "wed") {
    return "tue";
  }
  if (day === "thu") {
    return "wed";
  }
  if (day === "fri") {
    return "thu";
  }

  return "fri";
}

export function isImportProfileSchedulerRuleDue(
  rules: ImportProfileScheduleRuleLike[],
  now = new Date()
): boolean {
  const currentDay = getDayName(now.getDay());
  const previousDay = getPreviousDayName(currentDay);
  const currentMinutesOfDay = now.getHours() * 60 + now.getMinutes();

  return rules.some((rule, ruleIndex) => {
    const startMinutes = parseTimeOfDayToMinutes(
      rule.startTime,
      `scheduler.rules[${ruleIndex}].startTime`
    );
    const endMinutes = parseTimeOfDayToMinutes(
      rule.endTime,
      `scheduler.rules[${ruleIndex}].endTime`
    );
    const isOvernight = endMinutes < startMinutes;
    let minutesSinceStart: number | undefined;

    if (!isOvernight) {
      if (!rule.days.includes(currentDay)) {
        return false;
      }

      if (currentMinutesOfDay < startMinutes || currentMinutesOfDay > endMinutes) {
        return false;
      }

      minutesSinceStart = currentMinutesOfDay - startMinutes;
    } else {
      const inLateWindow = currentMinutesOfDay >= startMinutes;
      const inEarlyWindow = currentMinutesOfDay <= endMinutes;

      if (!inLateWindow && !inEarlyWindow) {
        return false;
      }

      if (inLateWindow && !rule.days.includes(currentDay)) {
        return false;
      }

      if (inEarlyWindow && !rule.days.includes(previousDay)) {
        return false;
      }

      minutesSinceStart = inLateWindow
        ? currentMinutesOfDay - startMinutes
        : 1440 - startMinutes + currentMinutesOfDay;
    }

    if (minutesSinceStart < 0 || !Number.isInteger(rule.intervalMinutes) || rule.intervalMinutes <= 0) {
      return false;
    }

    return minutesSinceStart % rule.intervalMinutes === 0;
  });
}