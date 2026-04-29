export type MappingTargetType = "string" | "integer" | "number" | "boolean" | "datetime";

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

export interface MappingPicklistEntry {
  source: string;
  target: string;
}

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
  picklistMappings?: MappingPicklistEntry[];
}

export interface MappingDefinitionParseResult {
  lines: MappingDefinitionLine[];
}
