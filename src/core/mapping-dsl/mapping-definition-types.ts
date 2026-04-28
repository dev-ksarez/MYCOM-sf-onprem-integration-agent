export type MappingTargetType = "string" | "integer" | "boolean" | "datetime";

export type MappingTransformType =
  | "NONE"
  | "TRIM"
  | "UPPERCASE"
  | "LOWERCASE"
  | "TO_INTEGER"
  | "TO_BOOLEAN"
  | "DATETIME_ISO"
  | "STATIC"
  | "LOOKUP";

export interface ParsedTransform {
  type: MappingTransformType;
  raw: string;
  argument?: string;
  lookupObject?: string;
  lookupField?: string;
}

export interface MappingDefinitionLine {
  lineNumber: number;
  rawLine: string;
  targetField: string;
  targetType: MappingTargetType;
  sourceField: string;
  transform: ParsedTransform;
}

export interface MappingDefinitionParseResult {
  lines: MappingDefinitionLine[];
}
