
import { ConnectorConfig, SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { MssqlDatabase } from "../../infrastructure/db/mssql";
import { ConnectorResult } from "../../types/connector-result";
import { GenericRecord } from "../../types/generic-record";
import { TargetAdapter } from "../../types/target-adapter";
import { TransferContext } from "../../types/transfer-context";

type PicklistSource = "global" | "object";
type TargetMode = "object" | "picklist";
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

interface SalesforcePicklistSqlMapping {
  query: string;
  matchColumn: string;
  valueColumn: string;
}

interface SalesforcePicklistDefinition {
  fieldApiName: string;
  source: PicklistSource;
  globalValueSetApiName?: string;
  sqlMapping?: SalesforcePicklistSqlMapping;
}

type SalesforceOperation = "upsert" | "insert" | "update";

interface SalesforceObjectTargetDefinition {
  objectApiName: string;
  operation: SalesforceOperation;
  externalIdField: string;
  picklists: SalesforcePicklistDefinition[];
}

interface SalesforceImportProfile {
  name: string;
  active: boolean;
  schedulerEnabled: boolean;
  nextRunAt?: string;
  scheduler?: ImportProfileSchedulerConfig;
  mode: TargetMode;
  target: SalesforceObjectTargetDefinition;
}

interface SalesforceTargetDefinition {
  selectedImportProfileName?: string;
  importProfiles: SalesforceImportProfile[];
}

function validateIdentifier(value: string, label: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_.]*$/.test(value)) {
    throw new Error(`Invalid Salesforce identifier for ${label}: ${value}`);
  }

  return value;
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

function parseTargetDefinition(rawDefinition: string): SalesforceTargetDefinition {
  const trimmedDefinition = rawDefinition.trim();
  if (!trimmedDefinition) {
    throw new Error("Salesforce target definition must not be empty");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmedDefinition);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown JSON parsing error";
    throw new Error(`Invalid JSON in Salesforce target definition: ${message}`);
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Salesforce target definition must be a JSON object");
  }

  const definition = parsed as Record<string, unknown>;

  const parsePicklists = (
    picklists: unknown,
    path: string
  ): SalesforcePicklistDefinition[] => {
    if (picklists === undefined) {
      return [];
    }

    if (!Array.isArray(picklists)) {
      throw new Error(`${path} must be an array`);
    }

    return picklists.map((picklist, index) => {
      if (!picklist || typeof picklist !== "object" || Array.isArray(picklist)) {
        throw new Error(`${path}[${index}] must be an object`);
      }

      const candidate = picklist as Record<string, unknown>;
      const fieldApiName = candidate.fieldApiName;
      const source = candidate.source;
      const globalValueSetApiName = candidate.globalValueSetApiName;
      const sqlMapping = candidate.sqlMapping;

      if (typeof fieldApiName !== "string" || !fieldApiName.trim()) {
        throw new Error(`${path}[${index}] is missing fieldApiName`);
      }

      if (source !== "global" && source !== "object") {
        throw new Error(`${path}[${index}] must use source='global' or source='object'`);
      }

      let parsedSqlMapping: SalesforcePicklistSqlMapping | undefined;
      if (sqlMapping !== undefined) {
        if (!sqlMapping || typeof sqlMapping !== "object" || Array.isArray(sqlMapping)) {
          throw new Error(`${path}[${index}].sqlMapping must be an object`);
        }

        const sqlCandidate = sqlMapping as Record<string, unknown>;
        const query = sqlCandidate.query;
        const matchColumn = sqlCandidate.matchColumn;
        const valueColumn = sqlCandidate.valueColumn;

        if (typeof query !== "string" || !query.trim()) {
          throw new Error(`${path}[${index}].sqlMapping.query is required`);
        }

        if (typeof matchColumn !== "string" || !matchColumn.trim()) {
          throw new Error(`${path}[${index}].sqlMapping.matchColumn is required`);
        }

        if (typeof valueColumn !== "string" || !valueColumn.trim()) {
          throw new Error(`${path}[${index}].sqlMapping.valueColumn is required`);
        }

        parsedSqlMapping = {
          query: query.trim(),
          matchColumn: matchColumn.trim(),
          valueColumn: valueColumn.trim()
        };
      }

      if (source === "global") {
        if (typeof globalValueSetApiName !== "string" || !globalValueSetApiName.trim()) {
          throw new Error(`${path}[${index}] with source='global' requires globalValueSetApiName`);
        }

        return {
          fieldApiName: validateIdentifier(fieldApiName.trim(), `${path}[${index}].fieldApiName`),
          source,
          globalValueSetApiName: validateIdentifier(
            globalValueSetApiName.trim(),
            `${path}[${index}].globalValueSetApiName`
          ),
          sqlMapping: parsedSqlMapping
        };
      }

      return {
        fieldApiName: validateIdentifier(fieldApiName.trim(), `${path}[${index}].fieldApiName`),
        source,
        sqlMapping: parsedSqlMapping
      };
    });
  };

  const parseObjectTarget = (
    candidate: Record<string, unknown>,
    path: string
  ): SalesforceObjectTargetDefinition => {
    const objectApiName = candidate.objectApiName;
    const operation = candidate.operation;
    const externalIdField = candidate.externalIdField;
    const picklists = candidate.picklists;

    if (typeof objectApiName !== "string" || !objectApiName.trim()) {
      throw new Error(`${path}.objectApiName is required`);
    }

    const validOperations: SalesforceOperation[] = ["upsert", "insert", "update"];
    if (!validOperations.includes(operation as SalesforceOperation)) {
      throw new Error(`${path}.operation must be one of: ${validOperations.join(", ")}. Got: '${String(operation)}'`);
    }

    const resolvedOperation = operation as SalesforceOperation;
    const needsExternalId = resolvedOperation === "upsert" || resolvedOperation === "update";

    if (needsExternalId && (typeof externalIdField !== "string" || !externalIdField.trim())) {
      throw new Error(`${path}.externalIdField is required for operation '${resolvedOperation}'`);
    }

    const resolvedExternalIdField =
      typeof externalIdField === "string" && externalIdField.trim()
        ? validateIdentifier(externalIdField.trim(), `${path}.externalIdField`)
        : "Id";

    return {
      objectApiName: validateIdentifier(objectApiName.trim(), `${path}.objectApiName`),
      operation: resolvedOperation,
      externalIdField: resolvedExternalIdField,
      picklists: parsePicklists(picklists, `${path}.picklists`)
    };
  };

  const importProfilesRaw = definition.importProfiles;
  const selectedImportProfileNameRaw = definition.selectedImportProfileName;

  if (importProfilesRaw !== undefined) {
    if (!Array.isArray(importProfilesRaw) || importProfilesRaw.length === 0) {
      throw new Error("Salesforce target definition importProfiles must be a non-empty array");
    }

    const importProfiles = importProfilesRaw.map((profile, index) => {
      if (!profile || typeof profile !== "object" || Array.isArray(profile)) {
        throw new Error(`Salesforce target definition importProfiles[${index}] must be an object`);
      }

      const candidate = profile as Record<string, unknown>;
      const name = candidate.name;
      const active = candidate.active;
      const schedulerEnabled = candidate.schedulerEnabled;
      const nextRunAt = candidate.nextRunAt;
      const mode = candidate.mode;
      const scheduler = candidate.scheduler;

      if (typeof name !== "string" || !name.trim()) {
        throw new Error(`Salesforce target definition importProfiles[${index}] is missing name`);
      }

      if (mode !== "object" && mode !== "picklist") {
        throw new Error(`Salesforce target definition importProfiles[${index}] must use mode='object' or mode='picklist'`);
      }

      if (nextRunAt !== undefined && (typeof nextRunAt !== "string" || !nextRunAt.trim())) {
        throw new Error(`Salesforce target definition importProfiles[${index}].nextRunAt must be a non-empty string when provided`);
      }

      let parsedScheduler: ImportProfileSchedulerConfig | undefined;
      if (scheduler !== undefined) {
        if (!scheduler || typeof scheduler !== "object" || Array.isArray(scheduler)) {
          throw new Error(`Salesforce target definition importProfiles[${index}].scheduler must be an object`);
        }

        const schedulerCandidate = scheduler as Record<string, unknown>;
        const schedulerMode = schedulerCandidate.mode;
        const rules = schedulerCandidate.rules;

        if (schedulerMode !== "rules") {
          throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.mode must be 'rules'`);
        }

        if (!Array.isArray(rules) || rules.length === 0) {
          throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules must be a non-empty array`);
        }

        const parsedRules = rules.map((rule, ruleIndex) => {
          if (!rule || typeof rule !== "object" || Array.isArray(rule)) {
            throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}] must be an object`);
          }

          const ruleCandidate = rule as Record<string, unknown>;
          const days = ruleCandidate.days;
          const startTime = ruleCandidate.startTime;
          const endTime = ruleCandidate.endTime;
          const intervalMinutes = ruleCandidate.intervalMinutes;

          if (!Array.isArray(days) || days.length === 0) {
            throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}].days must be a non-empty array`);
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
                `Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}].days[${dayIndex}] is invalid`
              );
            }

            return day as SchedulerDay;
          });

          if (typeof startTime !== "string" || !startTime.trim()) {
            throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}].startTime is required`);
          }

          if (typeof endTime !== "string" || !endTime.trim()) {
            throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}].endTime is required`);
          }

          const normalizedStartTime = startTime.trim();
          const normalizedEndTime = endTime.trim();
          parseTimeOfDayToMinutes(normalizedStartTime, `importProfiles[${index}].scheduler.rules[${ruleIndex}].startTime`);
          parseTimeOfDayToMinutes(normalizedEndTime, `importProfiles[${index}].scheduler.rules[${ruleIndex}].endTime`);

          if (typeof intervalMinutes !== "number" || !Number.isInteger(intervalMinutes) || intervalMinutes <= 0) {
            throw new Error(`Salesforce target definition importProfiles[${index}].scheduler.rules[${ruleIndex}].intervalMinutes must be a positive integer`);
          }

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
        mode: mode as TargetMode,
        target: parseObjectTarget(candidate, `importProfiles[${index}]`)
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

  // Backward compatibility: existing flat target definition becomes one default profile.
  return {
    selectedImportProfileName: "default",
    importProfiles: [
      {
        name: "default",
        active: true,
        schedulerEnabled: true,
        mode: "object",
        target: parseObjectTarget(definition, "targetDefinition")
      }
    ]
  };
}

// Pattern: JS Date toString / toLocaleString / toISOString values with timezone info
const JS_DATE_STRING_RE = /^\w{3}\s+\w{3}\s+\d{2}\s+\d{4}|^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

function normalizeDateValue(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }

  const trimmed = value.trim();

  // Already ISO date YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }

  // ISO datetime – strip to date only if it has timezone or T separator
  if (/^\d{4}-\d{2}-\d{2}T/.test(trimmed)) {
    return trimmed.slice(0, 10);
  }

  // JS Date.toString() e.g. "Tue May 12 2026 02:00:00 GMT+0200 (Central European Summer Time)"
  if (/^\w{3}\s+\w{3}\s+\d{2}\s+\d{4}/.test(trimmed)) {
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 10);
    }
  }

  return value;
}

function normalizeRecordValues(values: Record<string, unknown>): Record<string, unknown> {
  const normalizedEntries = Object.entries(values)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => [validateIdentifier(key, `field ${key}`), normalizeDateValue(value)]);

  return Object.fromEntries(normalizedEntries);
}

function validateRequiredRelationshipLookups(
  objectApiName: string,
  values: Record<string, unknown>
): void {
  if (
    objectApiName === "Contact" &&
    Object.prototype.hasOwnProperty.call(values, "AccountId") &&
    (values.AccountId === null || values.AccountId === undefined || values.AccountId === "")
  ) {
    throw new Error("Missing required Account lookup for Contact.AccountId");
  }
}

function isDuplicateSalesforceError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("duplicate_value") ||
    normalized.includes("duplicates_detected") ||
    normalized.includes("duplicate record") ||
    normalized.includes("duplicate value") ||
    normalized.includes("you're creating a duplicate record")
  );
}

function isExternalIdConfigurationError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("provided external id field does not exist or is not accessible") ||
    normalized.includes("missing external id value for field") ||
    normalized.includes("externalidfield is required") ||
    normalized.includes("pricebookentry upsert missing required key field") ||
    normalized.includes("unable to create/update fields")
  );
}

function formatUnknownError(error: unknown): string {
  if (!(error instanceof Error)) {
    return "Unknown error";
  }

  const errorWithData = error as Error & {
    data?: Array<Record<string, unknown>> | Record<string, unknown>;
  };

  const details = errorWithData.data;
  if (!details) {
    return error.message;
  }

  try {
    if (Array.isArray(details)) {
      const summarized = details
        .map((entry) => {
          const statusCode = typeof entry.statusCode === "string" ? entry.statusCode : undefined;
          const message = typeof entry.message === "string" ? entry.message : JSON.stringify(entry);
          return statusCode ? `${statusCode}: ${message}` : message;
        })
        .join(" | ");

      return `${error.message} | Salesforce details: ${summarized}`;
    }

    return `${error.message} | Salesforce details: ${JSON.stringify(details)}`;
  } catch {
    return error.message;
  }
}

export class SalesforceTargetAdapter implements TargetAdapter {
  private readonly salesforceClient: SalesforceClient;
  private readonly connectorConfig?: ConnectorConfig;
  private readonly targetDefinition: SalesforceTargetDefinition;
  private readonly activeProfile: SalesforceImportProfile;
  private picklistSqlDatabase?: MssqlDatabase;
  private readonly sqlMappingCache: Map<string, Map<string, string>>;

  public constructor(
    salesforceClient: SalesforceClient,
    targetDefinition: string,
    connectorConfig?: ConnectorConfig
  ) {
    this.salesforceClient = salesforceClient;
    this.connectorConfig = connectorConfig;
    this.targetDefinition = parseTargetDefinition(targetDefinition);
    this.activeProfile = this.resolveActiveImportProfile();
    this.sqlMappingCache = new Map();
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

  public async writeRecords(
    records: GenericRecord[],
    context: TransferContext
  ): Promise<ConnectorResult[]> {
    const results: ConnectorResult[] = [];
    const target = this.activeProfile.target;
    const mode = this.activeProfile.mode;

    for (const record of records) {
      const normalizedValues = await this.normalizePicklistValues(normalizeRecordValues(record.values));
      const externalKeyValue = normalizedValues[target.externalIdField];
      const externalKey =
        typeof externalKeyValue === "string"
          ? externalKeyValue
          : String(externalKeyValue ?? "UNKNOWN");

      try {
        if (externalKeyValue === undefined || externalKeyValue === null || externalKeyValue === "") {
          if (target.operation !== "insert") {
            throw new Error(
              `Mapped record is missing required external id field: ${target.externalIdField}`
            );
          }
        }

        const valuesToWrite =
          mode === "picklist"
            ? this.filterPicklistValuesForWrite(normalizedValues)
            : normalizedValues;

        validateRequiredRelationshipLookups(target.objectApiName, valuesToWrite);

        let targetId: string;

        if (target.operation === "insert") {
          targetId = await this.salesforceClient.createGenericRecord(target.objectApiName, valuesToWrite);
        } else if (target.operation === "update") {
          targetId = await this.salesforceClient.updateGenericRecord(target.objectApiName, valuesToWrite);
        } else {
          // upsert (default)
          const shouldUsePricebookCompositeKey =
            target.objectApiName === "PricebookEntry" &&
            target.externalIdField === "ProductCode" &&
            valuesToWrite.Product2Id !== undefined &&
            valuesToWrite.Pricebook2Id !== undefined;

          const shouldUseProductCodeLookupForProduct2 =
            target.objectApiName === "Product2" && target.externalIdField === "ProductCode";

          targetId = shouldUsePricebookCompositeKey
            ? await this.salesforceClient.upsertPricebookEntryByCompositeKey(valuesToWrite)
            : shouldUseProductCodeLookupForProduct2
              ? await this.salesforceClient.upsertProduct2ByProductCode(valuesToWrite)
              : await this.salesforceClient.upsertGenericRecord({
                  objectApiName: target.objectApiName,
                  externalIdField: target.externalIdField,
                  values: valuesToWrite
                });
        }

        const statusLabel =
          target.operation === "insert"
            ? "INSERT_OK"
            : target.operation === "update"
              ? "UPDATE_OK"
              : "UPSERT_OK";

        results.push({
          externalKey,
          success: true,
          targetId,
          statusCode: statusLabel,
          message: `Salesforce record ${target.operation}ed in run ${context.runId}`,
          retryable: false
        });
      } catch (error) {
        const message = formatUnknownError(error);
        const isDuplicateError = isDuplicateSalesforceError(message);
        const isExternalIdConfigError = isExternalIdConfigurationError(message);
        const isNonRetryable = isDuplicateError || isExternalIdConfigError;

        results.push({
          externalKey,
          success: false,
          statusCode: isDuplicateError
            ? "DUPLICATE_ERROR"
            : isExternalIdConfigError
              ? "CONFIGURATION_ERROR"
              : "TECHNICAL_ERROR",
          message,
          retryable: !isNonRetryable
        });
      }
    }

    return results;
  }

  private async normalizePicklistValues(
    values: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const target = this.activeProfile.target;
    if (target.picklists.length === 0) {
      return values;
    }

    const normalizedValues = { ...values };

    for (const picklist of target.picklists) {
      const currentValue = normalizedValues[picklist.fieldApiName];
      if (currentValue === undefined || currentValue === null || currentValue === "") {
        continue;
      }

      let serializedValue = String(currentValue).trim();
      if (!serializedValue) {
        continue;
      }

      if (picklist.sqlMapping) {
        serializedValue = await this.resolveValueFromSqlMapping(picklist, serializedValue);
      }

      const allowedValues =
        picklist.source === "global"
          ? await this.salesforceClient.getGlobalPicklistValues(picklist.globalValueSetApiName!)
          : await this.salesforceClient.getObjectPicklistValues(
              target.objectApiName,
              picklist.fieldApiName
            );

      const matchedValue = allowedValues.find(
        (entry) => entry.value === serializedValue || entry.label === serializedValue
      );

      if (!matchedValue) {
        const optionsPreview = allowedValues
          .slice(0, 10)
          .map((entry) => entry.value)
          .join(", ");
        const suffix = allowedValues.length > 10 ? ", ..." : "";

        throw new Error(
          `Invalid picklist value '${serializedValue}' for field ${picklist.fieldApiName}. Allowed values: ${optionsPreview}${suffix}`
        );
      }

      normalizedValues[picklist.fieldApiName] = matchedValue.value;
    }

    return normalizedValues;
  }

  private resolveActiveImportProfile(): SalesforceImportProfile {
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

    throw new Error("No active import profile found in Salesforce target definition");
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

  private filterPicklistValuesForWrite(values: Record<string, unknown>): Record<string, unknown> {
    const target = this.activeProfile.target;
    const picklistFieldNames = new Set(target.picklists.map((picklist) => picklist.fieldApiName));

    return Object.fromEntries(
      Object.entries(values).filter(([key]) => key === target.externalIdField || picklistFieldNames.has(key))
    );
  }

  private async resolveValueFromSqlMapping(
    picklist: SalesforcePicklistDefinition,
    rawValue: string
  ): Promise<string> {
    if (!picklist.sqlMapping) {
      return rawValue;
    }

    const mapping = await this.loadSqlMapping(picklist);
    const mappedValue = mapping.get(rawValue);
    if (!mappedValue) {
      throw new Error(
        `No SQL mapping result for picklist field ${picklist.fieldApiName} and source value '${rawValue}'`
      );
    }

    return mappedValue;
  }

  private async loadSqlMapping(picklist: SalesforcePicklistDefinition): Promise<Map<string, string>> {
    if (!picklist.sqlMapping) {
      return new Map();
    }

    const cacheKey = `${picklist.fieldApiName}::${picklist.sqlMapping.query}`;
    const cached = this.sqlMappingCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    const database = this.getOrCreateSqlMappingDatabase();
    const result = await database.query<Record<string, unknown>>(picklist.sqlMapping.query);

    const mapping = new Map<string, string>();
    for (const row of result.recordset) {
      const matchValue = row[picklist.sqlMapping.matchColumn];
      const targetValue = row[picklist.sqlMapping.valueColumn];

      if (matchValue === undefined || matchValue === null || targetValue === undefined || targetValue === null) {
        continue;
      }

      const matchKey = String(matchValue).trim();
      const mappedValue = String(targetValue).trim();
      if (!matchKey || !mappedValue) {
        continue;
      }

      mapping.set(matchKey, mappedValue);
    }

    if (mapping.size === 0) {
      throw new Error(
        `SQL mapping for picklist field ${picklist.fieldApiName} returned no usable rows`
      );
    }

    this.sqlMappingCache.set(cacheKey, mapping);
    return mapping;
  }

  private getOrCreateSqlMappingDatabase(): MssqlDatabase {
    if (this.picklistSqlDatabase) {
      return this.picklistSqlDatabase;
    }

    if (!this.connectorConfig) {
      throw new Error("Picklist SQL mapping requires connector configuration");
    }

    const params = this.connectorConfig.parameters;
    const server = this.getRequiredString(params, "server", "MSSQL source parameter for picklist SQL mapping");
    const database = this.getRequiredString(params, "database", "MSSQL source parameter for picklist SQL mapping");
    const user = this.getRequiredString(params, "user", "MSSQL source parameter for picklist SQL mapping");
    const password = this.resolvePassword(this.connectorConfig);

    this.picklistSqlDatabase = new MssqlDatabase({
      server,
      database,
      user,
      password,
      port: this.getOptionalNumber(params, "port"),
      encrypt: this.getOptionalBoolean(params, "encrypt"),
      trustServerCertificate: this.getOptionalBoolean(params, "trustServerCertificate"),
      connectionTimeout: this.connectorConfig.timeoutMs,
      requestTimeout: this.connectorConfig.timeoutMs
    });

    return this.picklistSqlDatabase;
  }

  private getRequiredString(
    parameters: Record<string, unknown>,
    key: string,
    context: string
  ): string {
    const value = parameters[key];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(`Missing required ${context}: ${key}`);
    }

    return value.trim();
  }

  private getOptionalNumber(parameters: Record<string, unknown>, key: string): number | undefined {
    const value = parameters[key];
    if (value === undefined || value === null || value === "") {
      return undefined;
    }

    if (typeof value === "number") {
      return value;
    }

    if (typeof value === "string") {
      const parsed = Number(value);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
    }

    throw new Error(`Invalid numeric MSSQL parameter for picklist SQL mapping: ${key}`);
  }

  private getOptionalBoolean(parameters: Record<string, unknown>, key: string): boolean | undefined {
    const value = parameters[key];
    if (value === undefined || value === null || value === "") {
      return undefined;
    }

    if (typeof value === "boolean") {
      return value;
    }

    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (normalized === "true") {
        return true;
      }
      if (normalized === "false") {
        return false;
      }
    }

    throw new Error(`Invalid boolean MSSQL parameter for picklist SQL mapping: ${key}`);
  }

  private resolvePassword(config: ConnectorConfig): string {
    if (!config.secretKey) {
      throw new Error(`MSSQL source connector ${config.name} is missing MSD_SecretKey__c`);
    }

    const password = process.env[config.secretKey];
    if (!password) {
      throw new Error(
        `Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`
      );
    }

    return password;
  }
}