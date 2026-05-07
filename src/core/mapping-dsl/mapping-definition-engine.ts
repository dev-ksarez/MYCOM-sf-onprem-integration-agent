

import {
  MappingDefinitionLine,
  MappingPicklistEntry,
  MappingTargetType,
  MappingTransformType
} from "./mapping-definition-types";

export interface MappingSourceRecord {
  [key: string]: unknown;
}

export interface MappingDefinitionEngineResult {
  values: Record<string, unknown>;
}

/**
 * Resolves a LOOKUP transform at runtime.
 * Returns the Salesforce ID for the matching record, or null if not found.
 */
export type LookupResolverFn = (
  objectName: string,
  field: string,
  value: unknown
) => Promise<string | null>;

function isEmptyValue(value: unknown): boolean {
  return value === undefined || value === null || value === "";
}

function readSourceValue(record: MappingSourceRecord, sourceField: string): unknown {
  if (!sourceField) {
    return undefined;
  }

  if (Object.prototype.hasOwnProperty.call(record, sourceField)) {
    return record[sourceField];
  }

  if (!sourceField.includes(".")) {
    return record[sourceField];
  }

  return sourceField.split(".").reduce<unknown>((currentValue, keyPart) => {
    if (currentValue === undefined || currentValue === null || typeof currentValue !== "object") {
      return undefined;
    }

    return (currentValue as Record<string, unknown>)[keyPart];
  }, record);
}

function applyPicklistMappings(value: unknown, mappings?: MappingPicklistEntry[]): unknown {
  if (isEmptyValue(value) || !Array.isArray(mappings) || mappings.length === 0) {
    return value;
  }

  const normalizedValue = String(value).trim();
  const directMatch = mappings.find((entry) => entry.source === normalizedValue);
  if (directMatch) {
    return directMatch.target;
  }

  const lowercaseValue = normalizedValue.toLowerCase();
  const relaxedMatch = mappings.find((entry) => entry.source.trim().toLowerCase() === lowercaseValue);
  return relaxedMatch ? relaxedMatch.target : value;
}

function normalizeEmailValue(value: unknown): string | null | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === null) {
    return null;
  }

  const normalized = String(value)
    .trim()
    .replace(/\s*@\s*/g, "@")
    .replace(/\s*\.\s*/g, ".");

  if (!normalized) {
    return null;
  }

  return normalized;
}

function isValidEmailValue(value: string): boolean {
  if (value.includes(";") || value.includes(",") || /^www\./i.test(value) || /\s/.test(value)) {
    return false;
  }

  return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(value);
}

function applyEmailValidation(value: unknown, line: MappingDefinitionLine): unknown {
  if (line.emailValidation?.enabled !== true) {
    return value;
  }

  const normalized = normalizeEmailValue(value);
  if (normalized === undefined || normalized === null) {
    return normalized;
  }

  if (isValidEmailValue(normalized)) {
    return normalized;
  }

  if (line.emailValidation.invalidAction === "ERROR") {
    throw new Error(`Invalid email value: ${value}`);
  }

  return null;
}

function parseSupportedDateValue(value: unknown): Date | undefined {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return undefined;
  }

  const nativeParsed = new Date(normalized);
  if (!Number.isNaN(nativeParsed.getTime())) {
    return nativeParsed;
  }

  const germanDateMatch = normalized.match(
    /^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (!germanDateMatch) {
    return undefined;
  }

  const [, dayText, monthText, yearText, hourText, minuteText, secondText] = germanDateMatch;
  const day = Number.parseInt(dayText, 10);
  const month = Number.parseInt(monthText, 10);
  const year = Number.parseInt(yearText, 10);
  const hours = Number.parseInt(hourText || "0", 10);
  const minutes = Number.parseInt(minuteText || "0", 10);
  const seconds = Number.parseInt(secondText || "0", 10);

  const parsed = new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day ||
    parsed.getUTCHours() !== hours ||
    parsed.getUTCMinutes() !== minutes ||
    parsed.getUTCSeconds() !== seconds
  ) {
    return undefined;
  }

  return parsed;
}

function applySimpleTransform(value: unknown, transformType: MappingTransformType): unknown {
  if (isEmptyValue(value)) {
    return value;
  }

  switch (transformType) {
    case "NONE":
      return value;
    case "TRIM":
      return String(value).trim();
    case "UPPERCASE":
      return String(value).toUpperCase();
    case "LOWERCASE":
      return String(value).toLowerCase();
    case "TO_INTEGER": {
      const parsed = Number.parseInt(String(value), 10);
      if (Number.isNaN(parsed)) {
        throw new Error(`Cannot convert value to integer: ${value}`);
      }
      return parsed;
    }
    case "TO_BOOLEAN": {
      if (typeof value === "boolean") {
        return value;
      }

      const normalized = String(value).trim().toLowerCase();
      if (["true", "1", "yes", "y", "ja", "j"].includes(normalized)) {
        return true;
      }
      if (["false", "0", "no", "n", "nein"].includes(normalized)) {
        return false;
      }

      throw new Error(`Cannot convert value to boolean: ${value}`);
    }
    case "DATETIME_ISO": {
      const parsedDate = parseSupportedDateValue(value);
      if (!parsedDate) {
        throw new Error(`Cannot convert value to ISO datetime: ${value}`);
      }
      return parsedDate.toISOString();
    }
    default:
      return value;
  }
}

function castToTargetType(value: unknown, targetType: MappingTargetType): unknown {
  if (isEmptyValue(value)) {
    return value;
  }

  switch (targetType) {
    case "string":
      return String(value);
    case "integer": {
      const parsed = typeof value === "number" ? Math.trunc(value) : Number.parseInt(String(value), 10);
      if (Number.isNaN(parsed)) {
        throw new Error(`Cannot cast value to integer: ${value}`);
      }
      return parsed;
    }
    case "number": {
      const parsed = typeof value === "number" ? value : Number.parseFloat(String(value));
      if (Number.isNaN(parsed)) {
        throw new Error(`Cannot cast value to number: ${value}`);
      }
      return parsed;
    }
    case "boolean": {
      if (typeof value === "boolean") {
        return value;
      }

      const normalized = String(value).trim().toLowerCase();
      if (["true", "1", "yes", "y"].includes(normalized)) {
        return true;
      }
      if (["false", "0", "no", "n"].includes(normalized)) {
        return false;
      }

      throw new Error(`Cannot cast value to boolean: ${value}`);
    }
    case "datetime": {
      const parsedDate = parseSupportedDateValue(value);
      if (!parsedDate) {
        throw new Error(`Cannot cast value to datetime: ${value}`);
      }
      return parsedDate.toISOString();
    }
    default:
      return value;
  }
}

function applyTransform(line: MappingDefinitionLine, record: MappingSourceRecord): unknown {
  const transform = line.transform;

  if (transform.type === "STATIC") {
    return applyEmailValidation(castToTargetType(transform.argument ?? "", line.targetType), line);
  }

  if (transform.type === "LOOKUP") {
    // Handled separately in applyTransformAsync
    throw new Error(
      `LOOKUP transform is not implemented yet for target field ${line.targetField} at line ${line.lineNumber}`
    );
  }

  const sourceValue = readSourceValue(record, line.sourceField);
  const transformedValue = applySimpleTransform(sourceValue, transform.type);
  const picklistMappedValue = applyPicklistMappings(transformedValue, line.picklistMappings);
  const castedValue = castToTargetType(picklistMappedValue, line.targetType);
  return applyEmailValidation(castedValue, line);
}

async function applyTransformAsync(
  line: MappingDefinitionLine,
  record: MappingSourceRecord,
  lookupResolver: LookupResolverFn,
  lookupCache: Map<string, string | null>
): Promise<unknown> {
  const transform = line.transform;

  if (transform.type === "STATIC") {
    return applyEmailValidation(castToTargetType(transform.argument ?? "", line.targetType), line);
  }

  if (transform.type === "LOOKUP") {
    if (!transform.lookupObject || !transform.lookupField) {
      throw new Error(
        `LOOKUP transform at line ${line.lineNumber} is missing lookupObject or lookupField`
      );
    }

    const rawValue = readSourceValue(record, line.sourceField);
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      return null;
    }

    const cacheKey = `${transform.lookupObject}|${transform.lookupField}|${rawValue}`;
    if (lookupCache.has(cacheKey)) {
      return lookupCache.get(cacheKey) ?? null;
    }

    const resolvedId = await lookupResolver(transform.lookupObject, transform.lookupField, rawValue);
    lookupCache.set(cacheKey, resolvedId);
    return resolvedId;
  }

  const sourceValue = readSourceValue(record, line.sourceField);
  const transformedValue = applySimpleTransform(sourceValue, transform.type);
  const picklistMappedValue = applyPicklistMappings(transformedValue, line.picklistMappings);
  const castedValue = castToTargetType(picklistMappedValue, line.targetType);
  return applyEmailValidation(castedValue, line);
}

export class MappingDefinitionEngine {
  private readonly lookupResolver?: LookupResolverFn;
  private readonly lookupCache: Map<string, string | null>;

  public constructor(lookupResolver?: LookupResolverFn) {
    this.lookupResolver = lookupResolver;
    this.lookupCache = new Map();
  }

  public async mapRecord(
    record: MappingSourceRecord,
    lines: MappingDefinitionLine[]
  ): Promise<MappingDefinitionEngineResult> {
    const values: Record<string, unknown> = {};
    const hasLookup = lines.some((line) => line.transform.type === "LOOKUP");

    if (hasLookup && !this.lookupResolver) {
      const firstLookupLine = lines.find((line) => line.transform.type === "LOOKUP")!;
      throw new Error(
        `Mapping error at line ${firstLookupLine.lineNumber} for target field ${firstLookupLine.targetField}: LOOKUP transform requires a lookup resolver but none was provided`
      );
    }

    for (const line of lines) {
      try {
        if (line.transform.type === "LOOKUP" && this.lookupResolver) {
          values[line.targetField] = await applyTransformAsync(
            line,
            record,
            this.lookupResolver,
            this.lookupCache
          );
        } else {
          values[line.targetField] = applyTransform(line, record);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown mapping error";
        throw new Error(
          `Mapping error at line ${line.lineNumber} for target field ${line.targetField}: ${message}`
        );
      }
    }

    return { values };
  }
}
