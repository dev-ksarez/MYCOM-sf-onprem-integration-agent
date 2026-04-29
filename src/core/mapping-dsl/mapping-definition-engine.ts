

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

  return record[sourceField];
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
      if (["true", "1", "yes", "y"].includes(normalized)) {
        return true;
      }
      if (["false", "0", "no", "n"].includes(normalized)) {
        return false;
      }

      throw new Error(`Cannot convert value to boolean: ${value}`);
    }
    case "DATETIME_ISO": {
      const parsedDate = new Date(String(value));
      if (Number.isNaN(parsedDate.getTime())) {
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
      const parsedDate = new Date(String(value));
      if (Number.isNaN(parsedDate.getTime())) {
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
    return castToTargetType(transform.argument ?? "", line.targetType);
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
  return castToTargetType(picklistMappedValue, line.targetType);
}

async function applyTransformAsync(
  line: MappingDefinitionLine,
  record: MappingSourceRecord,
  lookupResolver: LookupResolverFn,
  lookupCache: Map<string, string | null>
): Promise<unknown> {
  const transform = line.transform;

  if (transform.type === "STATIC") {
    return castToTargetType(transform.argument ?? "", line.targetType);
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
  return castToTargetType(picklistMappedValue, line.targetType);
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