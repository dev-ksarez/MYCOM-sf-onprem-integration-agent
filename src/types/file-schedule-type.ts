export const FILE_SCHEDULE_TYPES = ["FILE_CSV", "FILE_EXCEL", "FILE_JSON"] as const;

export type FileScheduleType = (typeof FILE_SCHEDULE_TYPES)[number];

export function normalizeScheduleType(value: unknown): string {
  return String(value || "").trim().toUpperCase();
}

export function isFileScheduleType(value: unknown): value is FileScheduleType {
  return FILE_SCHEDULE_TYPES.includes(normalizeScheduleType(value) as FileScheduleType);
}
