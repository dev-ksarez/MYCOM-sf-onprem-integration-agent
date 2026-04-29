

import {
  MappingDefinitionLine,
  MappingPicklistEntry,
  MappingDefinitionParseResult,
  MappingTargetType,
  MappingTransformType,
  ParsedTransform
} from "./mapping-definition-types";

const SUPPORTED_TARGET_TYPES: MappingTargetType[] = ["string", "integer", "number", "boolean", "datetime"];

const SIMPLE_TRANSFORMS: MappingTransformType[] = [
  "NONE",
  "TRIM",
  "UPPERCASE",
  "LOWERCASE",
  "TO_INTEGER",
  "TO_BOOLEAN",
  "DATETIME_ISO"
];

interface StoredMappingRule {
  sourceField?: unknown;
  targetField?: unknown;
  targetType?: unknown;
  sourceType?: unknown;
  transformFunction?: unknown;
  lookupEnabled?: unknown;
  lookupObject?: unknown;
  lookupField?: unknown;
  picklistMappings?: unknown;
}

function normalizeStoredPicklistMappings(value: unknown): MappingPicklistEntry[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((entry) => entry && typeof entry === "object" && !Array.isArray(entry))
    .map((entry) => {
      const candidate = entry as { source?: unknown; target?: unknown };
      return {
        source: String(candidate.source ?? "").trim(),
        target: String(candidate.target ?? "").trim()
      };
    })
    .filter((entry) => entry.source.length > 0 || entry.target.length > 0);
}

function isSupportedTargetType(value: string): value is MappingTargetType {
  return SUPPORTED_TARGET_TYPES.includes(value as MappingTargetType);
}

function parseTransform(rawTransform: string, lineNumber: number): ParsedTransform {
  const trimmedTransform = rawTransform.trim();

  if (SIMPLE_TRANSFORMS.includes(trimmedTransform as MappingTransformType)) {
    return {
      type: trimmedTransform as MappingTransformType,
      raw: trimmedTransform
    };
  }

  const staticMatch = trimmedTransform.match(/^STATIC\[(.*)\]$/);
  if (staticMatch) {
    return {
      type: "STATIC",
      raw: trimmedTransform,
      argument: staticMatch[1]
    };
  }

  const lookupMatch = trimmedTransform.match(/^LOOKUP\[([^|\]]+)\|([^\]]+)\]$/);
  if (lookupMatch) {
    return {
      type: "LOOKUP",
      raw: trimmedTransform,
      lookupObject: lookupMatch[1].trim(),
      lookupField: lookupMatch[2].trim()
    };
  }

  throw new Error(`Invalid transform at line ${lineNumber}: ${trimmedTransform}`);
}

function parseLine(rawLine: string, lineNumber: number): MappingDefinitionLine {
  const equalsIndex = rawLine.indexOf("=");
  if (equalsIndex === -1) {
    throw new Error(
      `Invalid mapping line ${lineNumber}: expected format <targetField>;<targetType>=<sourceField>;<transform>`
    );
  }

  const leftPart = rawLine.slice(0, equalsIndex).trim();
  const rightPart = rawLine.slice(equalsIndex + 1).trim();

  const leftTokens = leftPart.split(";");
  if (leftTokens.length !== 2) {
    throw new Error(`Invalid target definition at line ${lineNumber}: ${leftPart}`);
  }

  const targetField = leftTokens[0].trim();
  const targetTypeRaw = leftTokens[1].trim();

  if (!targetField) {
    throw new Error(`Missing target field at line ${lineNumber}`);
  }

  if (!isSupportedTargetType(targetTypeRaw)) {
    throw new Error(`Unsupported target type at line ${lineNumber}: ${targetTypeRaw}`);
  }

  const transformSeparatorIndex = rightPart.lastIndexOf(";");
  if (transformSeparatorIndex === -1) {
    throw new Error(`Missing transform definition at line ${lineNumber}: ${rightPart}`);
  }

  const sourceField = rightPart.slice(0, transformSeparatorIndex).trim();
  const rawTransform = rightPart.slice(transformSeparatorIndex + 1).trim();

  if (!rawTransform) {
    throw new Error(`Missing transform at line ${lineNumber}`);
  }

  const transform = parseTransform(rawTransform, lineNumber);

  return {
    lineNumber,
    rawLine,
    targetField,
    targetType: targetTypeRaw,
    sourceField,
    transform
  };
}

function parseStoredRule(rule: StoredMappingRule, lineNumber: number): MappingDefinitionLine | null {
  const sourceField = String(rule.sourceField ?? "").trim();
  const targetField = String(rule.targetField ?? "").trim();

  if (!sourceField && !targetField) {
    return null;
  }

  if (!targetField) {
    throw new Error(`Invalid JSON mapping at index ${lineNumber - 1}: missing targetField`);
  }

  const rawTargetType = String(rule.targetType ?? rule.sourceType ?? "string").trim() || "string";
  const targetType = isSupportedTargetType(rawTargetType) ? rawTargetType : "string";
  const lookupEnabled = Boolean(rule.lookupEnabled);
  const lookupObject = String(rule.lookupObject ?? "").trim();
  const lookupField = String(rule.lookupField ?? "").trim();
  const rawTransformFunction = String(rule.transformFunction ?? "NONE").trim() || "NONE";
  const rawTransform = lookupEnabled && lookupObject && lookupField
    ? `LOOKUP[${lookupObject}|${lookupField}]`
    : rawTransformFunction;
  const transform = parseTransform(rawTransform, lineNumber);

  return {
    lineNumber,
    rawLine: JSON.stringify(rule),
    targetField,
    targetType,
    sourceField,
    transform,
    picklistMappings: normalizeStoredPicklistMappings(rule.picklistMappings)
  };
}

export class MappingDefinitionParser {
  public parse(definition: string): MappingDefinitionParseResult {
    const trimmedDefinition = definition.trim();
    if (!trimmedDefinition) {
      return { lines: [] };
    }

    if (trimmedDefinition.startsWith("[")) {
      const parsed = JSON.parse(trimmedDefinition) as unknown;
      if (!Array.isArray(parsed)) {
        throw new Error("Invalid JSON mapping definition: expected an array of mapping rules");
      }

      const lines = parsed
        .map((entry, index) => parseStoredRule((entry ?? {}) as StoredMappingRule, index + 1))
        .filter((line): line is MappingDefinitionLine => line !== null);

      return { lines };
    }

    const rawLines = definition
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !line.startsWith("#"));

    const lines = rawLines.map((line, index) => parseLine(line, index + 1));

    return { lines };
  }
}