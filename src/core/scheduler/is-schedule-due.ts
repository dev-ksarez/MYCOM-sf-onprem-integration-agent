import { IntegrationSchedule } from "../../types/integration-schedule";

export function isScheduleDue(schedule: IntegrationSchedule): boolean {
  if (!schedule.active) {
    return false;
  }

  if (!schedule.nextRunAt) {
    return true;
  }

  return new Date(schedule.nextRunAt).getTime() <= Date.now();
}