import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";

export interface EndpointSourceDefinition {
  method: string;
  path: string;
  contentType: string;
  recordMode: "single" | "array" | "envelope";
  bodyPath?: string;
  queryFields: string[];
  headerFields: string[];
  response: {
    successStatus: number;
    errorStatus: number;
  };
  validation: {
    requiredBodyFields: string[];
  };
}

export interface EndpointRequestRecordInput {
  body: unknown;
  query: Record<string, unknown>;
  headers: Record<string, unknown>;
  request: Record<string, unknown>;
  auth: Record<string, unknown>;
}

function normalizePath(value: unknown): string {
  const raw = String(value || "").trim();
  if (!raw) {
    return "/";
  }
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function normalizeStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean);
  }
  return String(value || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function normalizePathArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function getByPath(value: unknown, path: string | undefined): unknown {
  const tokens = String(path || "").split(".").map((token) => token.trim()).filter(Boolean);
  let current = value;
  for (const token of tokens) {
    if (current === null || current === undefined) {
      return undefined;
    }
    if (Array.isArray(current)) {
      const index = Number(token);
      if (!Number.isInteger(index) || index < 0 || index >= current.length) {
        return undefined;
      }
      current = current[index];
      continue;
    }
    if (typeof current !== "object") {
      return undefined;
    }
    current = (current as Record<string, unknown>)[token];
  }
  return current;
}

export function parseEndpointSourceDefinition(rawDefinition: string): EndpointSourceDefinition {
  const trimmed = String(rawDefinition || "").trim();
  if (!trimmed) {
    throw new Error("Endpoint SourceDefinition darf nicht leer sein");
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error("Endpoint SourceDefinition muss gueltiges JSON sein");
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Endpoint SourceDefinition muss ein JSON-Objekt sein");
  }

  const candidate = parsed as Record<string, unknown>;
  const method = String(candidate.method || "POST").trim().toUpperCase();
  const recordModeRaw = String(candidate.recordMode || "single").trim().toLowerCase();
  const recordMode = recordModeRaw === "array" || recordModeRaw === "envelope" ? recordModeRaw : "single";
  const response = candidate.response && typeof candidate.response === "object" && !Array.isArray(candidate.response)
    ? candidate.response as Record<string, unknown>
    : {};
  const validation = candidate.validation && typeof candidate.validation === "object" && !Array.isArray(candidate.validation)
    ? candidate.validation as Record<string, unknown>
    : {};

  return {
    method,
    path: normalizePath(candidate.path),
    contentType: String(candidate.contentType || "application/json").trim().toLowerCase(),
    recordMode,
    bodyPath: typeof candidate.bodyPath === "string" ? candidate.bodyPath.trim() : undefined,
    queryFields: normalizeStringArray(candidate.queryFields),
    headerFields: normalizeStringArray(candidate.headerFields),
    response: {
      successStatus: Math.max(200, Math.min(299, Math.trunc(Number(response.successStatus || 202)) || 202)),
      errorStatus: Math.max(400, Math.min(599, Math.trunc(Number(response.errorStatus || 422)) || 422))
    },
    validation: {
      requiredBodyFields: normalizePathArray(validation.requiredBodyFields)
    }
  };
}

export function createEndpointRecords(
  definition: EndpointSourceDefinition,
  input: EndpointRequestRecordInput
): GenericRecord[] {
  const scopedBody = definition.bodyPath ? getByPath(input.body, definition.bodyPath) : input.body;
  const items = definition.recordMode === "array" || definition.recordMode === "envelope"
    ? Array.isArray(scopedBody) ? scopedBody : []
    : [scopedBody];

  if ((definition.recordMode === "array" || definition.recordMode === "envelope") && !Array.isArray(scopedBody)) {
    throw new Error(`Endpoint bodyPath ${definition.bodyPath || "<root>"} enthaelt kein Array`);
  }

  return items.map((item) => ({
    values: {
      body: item,
      query: input.query,
      headers: input.headers,
      request: input.request,
      auth: input.auth
    }
  }));
}

export function validateEndpointRequestBody(definition: EndpointSourceDefinition, body: unknown): void {
  if (!definition.validation.requiredBodyFields.length) {
    return;
  }

  for (const field of definition.validation.requiredBodyFields) {
    const value = getByPath(body, field);
    if (value === undefined || value === null || value === "") {
      throw new Error(`Pflichtfeld fehlt im Endpoint Request: ${field}`);
    }
  }
}

export class EndpointSourceAdapter implements SourceAdapter {
  private readonly records: GenericRecord[];

  public constructor(records: GenericRecord[]) {
    this.records = records;
  }

  public async readRecords(_context: TransferContext): Promise<GenericRecord[]> {
    return this.records;
  }
}
