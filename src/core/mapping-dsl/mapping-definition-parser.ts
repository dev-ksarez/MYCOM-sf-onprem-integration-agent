

import {
  MappingDefinitionLine,
  MappingDefinitionParseResult,
  MappingTargetType,
  MappingTransformType,
  ParsedTransform
} from "./mapping-definition-types";

const SUPPORTED_TARGET_TYPES: MappingTargetType[] = ["string", "integer", "boolean", "datetime"];

const SIMPLE_TRANSFORMS: MappingTransformType[] = [
  "NONE",
  "TRIM",
  "UPPERCASE",
  "LOWERCASE",
  "TO_INTEGER",
  "TO_BOOLEAN",
  "DATETIME_ISO"
];

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

export class MappingDefinitionParser {
  public parse(definition: string): MappingDefinitionParseResult {
    const rawLines = definition
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !line.startsWith("#"));

    const lines = rawLines.map((line, index) => parseLine(line, index + 1));

    return { lines };
  }
}