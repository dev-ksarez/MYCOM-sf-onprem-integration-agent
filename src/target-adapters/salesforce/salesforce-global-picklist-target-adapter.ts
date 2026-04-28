import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";
import { SalesforceClient } from "../../clients/salesforce/salesforce-client";

type SchedulerDay = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";

interface ImportProfileScheduleRule {
  days: SchedulerDay[];
  startTime: string;
  endTime: string;
  intervalMinutes: number;
}

interface ImportProfileSchedulerConfig {
  mode: "rules";
  rules: ImportProfileScheduleRule[];
}

interface GlobalPicklistTarget {
  globalValueSetApiName: string;
  externalIdField: string;
  labelField: string;
}

interface GlobalPicklistImportProfile {
  name: string;
  active: boolean;
  schedulerEnabled: boolean;
  nextRunAt?: string;
  scheduler?: ImportProfileSchedulerConfig;
  mode: "picklist";
  target: GlobalPicklistTarget;
}

interface GlobalPicklistTargetDefinition {
  selectedImportProfileName?: string;
  importProfiles: GlobalPicklistImportProfile[];
}

function parseTimeOfDayToMinutes(value: string, label: string): number {
  if (!/^([01]\d|2[0-3]):([0-5]\d)$/.test(value)) {
    throw new Error(`Invalid time format for ${label}: ${value}. Expected HH:mm`);
  }

  const [hourPart, minutePart] = value.split(":");
  const hours = Number(hourPart);
  const minutes = Number(minutePart);
  return hours * 60 + minutes;
}

function parseTargetDefinition(rawDefinition: string): GlobalPicklistTargetDefinition {
  const trimmed = rawDefinition.trim();
  if (!trimmed) {
    throw new Error("Salesforce global picklist target definition must not be empty");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown JSON parsing error";
    throw new Error(`Invalid JSON in Salesforce global picklist target definition: ${message}`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Salesforce global picklist target definition must be a JSON object");
  }

  const definition = parsed as Record<string, unknown>;
  const selectedImportProfileNameRaw = definition.selectedImportProfileName;
  const importProfilesRaw = definition.importProfiles;

  if (!Array.isArray(importProfilesRaw) || importProfilesRaw.length === 0) {
    throw new Error("importProfiles must be a non-empty array for global picklist target definition");
  }

  const importProfiles = importProfilesRaw.map((profile, index) => {
    if (!profile || typeof profile !== "object" || Array.isArray(profile)) {
      throw new Error(`importProfiles[${index}] must be an object`);
    }

    const candidate = profile as Record<string, unknown>;
    const name = candidate.name;
    const active = candidate.active;
    const schedulerEnabled = candidate.schedulerEnabled;
    const nextRunAt = candidate.nextRunAt;
    const mode = candidate.mode;
    const scheduler = candidate.scheduler;
    const target = candidate.target;

    if (typeof name !== "string" || !name.trim()) {
      throw new Error(`importProfiles[${index}].name is required`);
    }

    if (mode !== "picklist") {
      throw new Error(`importProfiles[${index}].mode must be 'picklist'`);
    }

    if (!target || typeof target !== "object" || Array.isArray(target)) {
      throw new Error(`importProfiles[${index}].target must be an object`);
    }

    const targetCandidate = target as Record<string, unknown>;
    const globalValueSetApiName = targetCandidate.globalValueSetApiName;
    const externalIdField = targetCandidate.externalIdField;
    const labelField = targetCandidate.labelField;

    if (typeof globalValueSetApiName !== "string" || !globalValueSetApiName.trim()) {
      throw new Error(`importProfiles[${index}].target.globalValueSetApiName is required`);
    }

    if (typeof externalIdField !== "string" || !externalIdField.trim()) {
      throw new Error(`importProfiles[${index}].target.externalIdField is required`);
    }

    if (typeof labelField !== "string" || !labelField.trim()) {
      throw new Error(`importProfiles[${index}].target.labelField is required`);
    }

    let parsedScheduler: ImportProfileSchedulerConfig | undefined;
    if (scheduler !== undefined) {
      if (!scheduler || typeof scheduler !== "object" || Array.isArray(scheduler)) {
        throw new Error(`importProfiles[${index}].scheduler must be an object`);
      }

      const schedulerCandidate = scheduler as Record<string, unknown>;
      if (schedulerCandidate.mode !== "rules") {
        throw new Error(`importProfiles[${index}].scheduler.mode must be 'rules'`);
      }

      const rules = schedulerCandidate.rules;
      if (!Array.isArray(rules) || rules.length === 0) {
        throw new Error(`importProfiles[${index}].scheduler.rules must be a non-empty array`);
      }

      const parsedRules = rules.map((rule, ruleIndex) => {
        if (!rule || typeof rule !== "object" || Array.isArray(rule)) {
          throw new Error(`importProfiles[${index}].scheduler.rules[${ruleIndex}] must be an object`);
        }

        const ruleCandidate = rule as Record<string, unknown>;
        const days = ruleCandidate.days;
        const startTime = ruleCandidate.startTime;
        const endTime = ruleCandidate.endTime;
        const intervalMinutes = ruleCandidate.intervalMinutes;

        if (!Array.isArray(days) || days.length === 0) {
          throw new Error(`importProfiles[${index}].scheduler.rules[${ruleIndex}].days must be a non-empty array`);
        }

        const parsedDays = days.map((day, dayIndex) => {
          if (
            day !== "mon" &&
            day !== "tue" &&
            day !== "wed" &&
            day !== "thu" &&
            day !== "fri" &&
            day !== "sat" &&
            day !== "sun"
          ) {
            throw new Error(
              `importProfiles[${index}].scheduler.rules[${ruleIndex}].days[${dayIndex}] is invalid`
            );
          }

          return day as SchedulerDay;
        });

        if (typeof startTime !== "string" || !startTime.trim()) {
          throw new Error(`importProfiles[${index}].scheduler.rules[${ruleIndex}].startTime is required`);
        }

        if (typeof endTime !== "string" || !endTime.trim()) {
          throw new Error(`importProfiles[${index}].scheduler.rules[${ruleIndex}].endTime is required`);
        }

        if (typeof intervalMinutes !== "number" || !Number.isInteger(intervalMinutes) || intervalMinutes <= 0) {
          throw new Error(`importProfiles[${index}].scheduler.rules[${ruleIndex}].intervalMinutes must be a positive integer`);
        }

        const normalizedStartTime = startTime.trim();
        const normalizedEndTime = endTime.trim();
        parseTimeOfDayToMinutes(normalizedStartTime, `importProfiles[${index}].scheduler.rules[${ruleIndex}].startTime`);
        parseTimeOfDayToMinutes(normalizedEndTime, `importProfiles[${index}].scheduler.rules[${ruleIndex}].endTime`);

        return {
          days: parsedDays,
          startTime: normalizedStartTime,
          endTime: normalizedEndTime,
          intervalMinutes
        };
      });

      parsedScheduler = {
        mode: "rules",
        rules: parsedRules
      };
    }

    return {
      name: name.trim(),
      active: active === undefined ? true : Boolean(active),
      schedulerEnabled: schedulerEnabled === undefined ? true : Boolean(schedulerEnabled),
      nextRunAt: typeof nextRunAt === "string" ? nextRunAt.trim() : undefined,
      scheduler: parsedScheduler,
      mode: "picklist" as const,
      target: {
        globalValueSetApiName: globalValueSetApiName.trim(),
        externalIdField: externalIdField.trim(),
        labelField: labelField.trim()
      }
    };
  });

  const selectedImportProfileName =
    typeof selectedImportProfileNameRaw === "string" && selectedImportProfileNameRaw.trim()
      ? selectedImportProfileNameRaw.trim()
      : undefined;

  return {
    selectedImportProfileName,
    importProfiles
  };
}

export class SalesforceGlobalPicklistTargetAdapter implements TargetAdapter {
  private readonly salesforceClient: SalesforceClient;
  private readonly targetDefinition: GlobalPicklistTargetDefinition;
  private readonly activeProfile: GlobalPicklistImportProfile;

  public constructor(salesforceClient: SalesforceClient, targetDefinition: string) {
    this.salesforceClient = salesforceClient;
    this.targetDefinition = parseTargetDefinition(targetDefinition);
    this.activeProfile = this.resolveActiveImportProfile();
  }

  public isProfileSchedulerDue(now = Date.now()): boolean {
    if (!this.activeProfile.active) {
      return false;
    }

    if (!this.activeProfile.schedulerEnabled) {
      return false;
    }

    if (this.activeProfile.scheduler?.mode === "rules") {
      return this.isRuleBasedSchedulerDue(new Date(now));
    }

    if (!this.activeProfile.nextRunAt) {
      return true;
    }

    return new Date(this.activeProfile.nextRunAt).getTime() <= now;
  }

  public getActiveProfileName(): string {
    return this.activeProfile.name;
  }

  public async writeRecords(records: GenericRecord[], context: TransferContext): Promise<ConnectorResult[]> {
    const target = this.activeProfile.target;
    const preparedEntries: Array<{ apiName: string; label: string }> = [];
    const preflightResults: ConnectorResult[] = [];

    for (const record of records) {
      const apiNameRaw = record.values[target.externalIdField];
      const labelRaw = record.values[target.labelField];
      const apiName = String(apiNameRaw ?? "").trim();
      const label = String(labelRaw ?? apiNameRaw ?? "").trim();

      if (!apiName) {
        preflightResults.push({
          externalKey: "UNKNOWN",
          success: false,
          statusCode: "VALIDATION_ERROR",
          message: `Missing mapped field ${target.externalIdField} for global picklist sync`,
          retryable: false
        });
        continue;
      }

      preparedEntries.push({ apiName, label: label || apiName });
      preflightResults.push({
        externalKey: apiName,
        success: true,
        targetId: target.globalValueSetApiName,
        statusCode: "PREPARED",
        message: `Prepared value ${apiName} for global value set ${target.globalValueSetApiName}`,
        retryable: false
      });
    }

    const validEntries = preparedEntries;
    if (validEntries.length === 0) {
      return preflightResults;
    }

    try {
      const syncResult = await this.salesforceClient.syncGlobalValueSetValues({
        globalValueSetApiName: target.globalValueSetApiName,
        entries: validEntries
      });

      return preflightResults.map((result) => {
        if (!result.success) {
          return result;
        }

        return {
          externalKey: result.externalKey,
          success: true,
          targetId: target.globalValueSetApiName,
          statusCode: "UPSERT_OK",
          message: `Global picklist synced in run ${context.runId} (added=${syncResult.added}, updated=${syncResult.updated})`,
          retryable: false
        };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown global picklist sync error";

      return preflightResults.map((result) => ({
        externalKey: result.externalKey,
        success: false,
        targetId: target.globalValueSetApiName,
        statusCode: "TECHNICAL_ERROR",
        message,
        retryable: true
      }));
    }
  }

  private resolveActiveImportProfile(): GlobalPicklistImportProfile {
    const selectedName = this.targetDefinition.selectedImportProfileName;

    if (selectedName) {
      const selected = this.targetDefinition.importProfiles.find((profile) => profile.name === selectedName);
      if (!selected) {
        throw new Error(`Selected import profile was not found: ${selectedName}`);
      }

      return selected;
    }

    const firstActive = this.targetDefinition.importProfiles.find((profile) => profile.active);
    if (firstActive) {
      return firstActive;
    }

    throw new Error("No active import profile found in global picklist target definition");
  }

  private isRuleBasedSchedulerDue(now: Date): boolean {
    const scheduler = this.activeProfile.scheduler;
    if (!scheduler || scheduler.mode !== "rules") {
      return false;
    }

    const currentDay = this.getDayName(now.getDay());
    const previousDay = this.getPreviousDayName(currentDay);
    const currentMinutesOfDay = now.getHours() * 60 + now.getMinutes();

    return scheduler.rules.some((rule) => {
      const startMinutes = parseTimeOfDayToMinutes(rule.startTime, "scheduler.startTime");
      const endMinutes = parseTimeOfDayToMinutes(rule.endTime, "scheduler.endTime");

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

      if (minutesSinceStart < 0) {
        return false;
      }

      return minutesSinceStart % rule.intervalMinutes === 0;
    });
  }

  private getDayName(dayIndex: number): SchedulerDay {
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

  private getPreviousDayName(day: SchedulerDay): SchedulerDay {
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
}
