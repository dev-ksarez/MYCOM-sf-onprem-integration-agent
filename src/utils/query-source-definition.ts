export type DeltaStrategy = "datetime" | "timestamp" | "id";

export interface DeltaConfig {
  strategy: DeltaStrategy;
  field: string;
}

export interface AfterExportConfig {
  updates: Record<string, string>;
}

export interface DeltaCheckpoint {
  value?: string;
  recordId?: string;
}

export interface SourceRecordCheckpoint {
  value: string;
  recordId?: string;
}

interface QuerySourceDefinitionEnvelope {
  queryText?: unknown;
  soql?: unknown;
  query?: unknown;
  delta?: unknown;
  afterExport?: unknown;
}

export interface ParsedQuerySourceDefinition {
  queryText: string;
  delta?: DeltaConfig;
  afterExport?: AfterExportConfig;
}

export function normalizeAfterExportConfig(value: unknown): AfterExportConfig | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const updates = Object.entries(value as Record<string, unknown>).reduce<Record<string, string>>((acc, [key, raw]) => {
    const fieldName = String(key || "").trim();
    const fieldValue = String(raw || "").trim();
    if (fieldName && fieldValue) {
      acc[fieldName] = fieldValue;
    }
    return acc;
  }, {});

  return Object.keys(updates).length ? { updates } : undefined;
}

export function normalizeDeltaStrategy(value: unknown): DeltaStrategy | undefined {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "datetime" || normalized === "timestamp" || normalized === "id") {
    return normalized;
  }
  return undefined;
}

export function normalizeDeltaConfig(value: unknown): DeltaConfig | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const candidate = value as Record<string, unknown>;
  const strategy = normalizeDeltaStrategy(candidate.strategy);
  const field = typeof candidate.field === "string" ? candidate.field.trim() : "";
  if (!strategy || !field) {
    return undefined;
  }

  return { strategy, field };
}

export function parseQuerySourceDefinition(rawDefinition: string): ParsedQuerySourceDefinition {
  const trimmed = String(rawDefinition || "").trim();
  if (!trimmed) {
    return { queryText: "" };
  }

  try {
    const parsed = JSON.parse(trimmed) as QuerySourceDefinitionEnvelope;
    const queryText = parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? typeof parsed.queryText === "string"
        ? parsed.queryText.trim()
        : typeof parsed.soql === "string"
          ? parsed.soql.trim()
          : typeof parsed.query === "string"
            ? parsed.query.trim()
            : ""
      : "";

    if (queryText) {
      return {
        queryText,
        delta: normalizeDeltaConfig(parsed.delta),
        afterExport: normalizeAfterExportConfig(parsed.afterExport)
      };
    }
  } catch {
    // Backward compatible: plain query text.
  }

  return { queryText: trimmed };
}

export function resolveAfterExportValue(value: string, exportDate: string, runId: string): string {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return normalized;
  }

  if (normalized.toLowerCase() === "exportdate") {
    return exportDate;
  }

  if (normalized.toLowerCase() === "runid") {
    return runId;
  }

  return normalized;
}

export function normalizeCheckpointValue(value: unknown): string | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
    return `0x${value.toString("hex")}`;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed || undefined;
  }

  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }

  return undefined;
}

export function getRecordValueByField(record: Record<string, unknown>, field: string): unknown {
  const trimmed = String(field || "").trim();
  if (!trimmed) {
    return undefined;
  }

  if (trimmed in record) {
    return record[trimmed];
  }

  const directMatch = Object.keys(record).find((key) => key.toLowerCase() === trimmed.toLowerCase());
  if (directMatch) {
    return record[directMatch];
  }

  const tokens = trimmed.split(".").map((token) => token.trim()).filter(Boolean);
  let current: unknown = record;
  for (const token of tokens) {
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }

    const currentRecord = current as Record<string, unknown>;
    if (token in currentRecord) {
      current = currentRecord[token];
      continue;
    }

    const tokenMatch = Object.keys(currentRecord).find((key) => key.toLowerCase() === token.toLowerCase());
    if (!tokenMatch) {
      return undefined;
    }

    current = currentRecord[tokenMatch];
  }

  return current;
}

export function getRecordIdentifier(record: Record<string, unknown>): string | undefined {
  for (const key of ["sourceId", "SourceId", "sourceID", "Id", "ID", "id", "externalKey", "ExternalKey"]) {
    const normalized = normalizeCheckpointValue(record[key]);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
}