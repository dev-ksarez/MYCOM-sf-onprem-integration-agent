

import {
  MappingDefinitionLine,
  MappingTargetType,
  MappingTransformType
} from "./mapping-definition-types";

export interface MappingSourceRecord {
  [key: string]: unknown;
}

export interface MappingDefinitionEngineResult {
  values: Record<string, unknown>;
}

function isEmptyValue(value: unknown): boolean {
  return value === undefined || value === null || value === "";
}

function readSourceValue(record: MappingSourceRecord, sourceField: string): unknown {
  if (!sourceField) {
    return undefined;
  }

  return record[sourceField];
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
    throw new Error(
      `LOOKUP transform is not implemented yet for target field ${line.targetField} at line ${line.lineNumber}`
    );
  }

  const sourceValue = readSourceValue(record, line.sourceField);
  const transformedValue = applySimpleTransform(sourceValue, transform.type);
  return castToTargetType(transformedValue, line.targetType);
}

export class MappingDefinitionEngine {
  public mapRecord(
    record: MappingSourceRecord,
    lines: MappingDefinitionLine[]
  ): MappingDefinitionEngineResult {
    const values: Record<string, unknown> = {};

    for (const line of lines) {
      try {
        values[line.targetField] = applyTransform(line, record);
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