import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { DatabaseMetadata, DatabaseTableMetadata } from "../../types/database-metadata";

export interface FileMakerSourceDefinition {
  layout?: string;
  query?: Array<Record<string, unknown>>;
  sort?: Array<Record<string, unknown>>;
  limit?: number;
  offset?: number;
  fields?: string[];
}

function getRequiredString(parameters: Record<string, unknown>, key: string): string {
  const value = parameters[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`Missing required FileMaker connector parameter: ${key}`);
  }
  return value.trim();
}

function resolvePassword(config: ConnectorConfig): string {
  const inlinePassword = config.parameters.password;
  if (typeof inlinePassword === "string" && inlinePassword.trim() !== "") {
    return inlinePassword;
  }

  if (!config.secretKey) {
    throw new Error(`FileMaker connector ${config.name} is missing MSD_SecretKey__c`);
  }

  const password = process.env[config.secretKey];
  if (!password) {
    throw new Error(`Environment variable for secret key ${config.secretKey} is not set for connector ${config.name}`);
  }

  return password;
}

function normalizeBaseUrl(value: string): string {
  return value.replace(/\/+$/, "");
}

function parseSourceDefinition(rawDefinition: string): FileMakerSourceDefinition {
  const trimmed = String(rawDefinition || "").trim();
  if (!trimmed) {
    throw new Error("FileMaker SourceDefinition darf nicht leer sein");
  }

  if (!trimmed.startsWith("{")) {
    return { layout: trimmed };
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("FileMaker SourceDefinition muss ein JSON-Objekt sein");
    }
    return parsed as FileMakerSourceDefinition;
  } catch {
    throw new Error("FileMaker SourceDefinition muss gueltiges JSON sein oder einen Layout-Namen enthalten");
  }
}

function extractResponseData(payload: unknown): Record<string, unknown> {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return {};
  }
  const data = (payload as Record<string, unknown>).response;
  return data && typeof data === "object" && !Array.isArray(data)
    ? data as Record<string, unknown>
    : payload as Record<string, unknown>;
}

function normalizeFileMakerRecord(record: unknown, fields?: string[]): Record<string, unknown> {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    return {};
  }

  const raw = record as Record<string, unknown>;
  const fieldData = raw.fieldData && typeof raw.fieldData === "object" && !Array.isArray(raw.fieldData)
    ? raw.fieldData as Record<string, unknown>
    : raw;
  const row = { ...fieldData };

  if (raw.recordId !== undefined && row.recordId === undefined) {
    row.recordId = raw.recordId;
  }
  if (raw.modId !== undefined && row.modId === undefined) {
    row.modId = raw.modId;
  }

  const selectedFields = Array.isArray(fields)
    ? fields.map((field) => String(field || "").trim()).filter(Boolean)
    : [];
  if (!selectedFields.length) {
    return row;
  }

  const filtered: Record<string, unknown> = {};
  for (const field of selectedFields) {
    filtered[field] = row[field];
  }
  return filtered;
}

export class FileMakerDataApiClient {
  private readonly config: ConnectorConfig;
  private readonly baseUrl: string;
  private readonly databaseName: string;
  private readonly username: string;
  private readonly password: string;

  public constructor(config: ConnectorConfig) {
    this.config = config;
    const parameters = config.parameters || {};
    this.baseUrl = normalizeBaseUrl(
      String(parameters.baseUrl || parameters.serverUrl || parameters.server || "").trim()
    );
    if (!this.baseUrl) {
      throw new Error("Missing required FileMaker connector parameter: baseUrl");
    }
    this.databaseName = getRequiredString(parameters, "database");
    this.username = getRequiredString(parameters, "user");
    this.password = resolvePassword(config);
  }

  private buildUrl(path: string): string {
    const encodedDatabase = encodeURIComponent(this.databaseName);
    return `${this.baseUrl}/fmi/data/vLatest/databases/${encodedDatabase}${path}`;
  }

  private async request(path: string, init: RequestInit = {}, token?: string): Promise<unknown> {
    const response = await fetch(this.buildUrl(path), {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(init.headers || {})
      }
    });
    const rawText = await response.text();
    let payload: unknown = {};
    if (rawText) {
      try {
        payload = JSON.parse(rawText);
      } catch {
        payload = { rawText };
      }
    }

    if (!response.ok) {
      const messages = payload && typeof payload === "object"
        ? (payload as Record<string, any>).messages
        : undefined;
      const detail = Array.isArray(messages)
        ? messages.map((message) => message?.message || message?.code).filter(Boolean).join("; ")
        : rawText;
      throw new Error(`FileMaker Data API request failed (${response.status} ${response.statusText}): ${detail || "no details"}`);
    }

    return payload;
  }

  public async withSession<T>(callback: (token: string) => Promise<T>): Promise<T> {
    const auth = Buffer.from(`${this.username}:${this.password}`, "utf8").toString("base64");
    const loginPayload = await this.request("/sessions", {
      method: "POST",
      headers: { Authorization: `Basic ${auth}` },
      body: "{}"
    });
    const token = String(extractResponseData(loginPayload).token || "").trim();
    if (!token) {
      throw new Error("FileMaker Data API login did not return a token");
    }

    try {
      return await callback(token);
    } finally {
      await this.request(`/sessions/${encodeURIComponent(token)}`, { method: "DELETE" }, token).catch(() => {});
    }
  }

  public async testConnection(): Promise<boolean> {
    await this.withSession(async (token) => {
      await this.request("/layouts", { method: "GET" }, token);
    });
    return true;
  }

  public async listLayouts(token: string): Promise<Array<{ name: string }>> {
    const payload = await this.request("/layouts", { method: "GET" }, token);
    const layouts = extractResponseData(payload).layouts;
    if (!Array.isArray(layouts)) {
      return [];
    }
    return layouts
      .map((layout) => ({
        name: String((layout as Record<string, unknown>)?.name || "").trim()
      }))
      .filter((layout) => layout.name);
  }

  public async getLayoutMetadata(token: string, layoutName: string): Promise<DatabaseTableMetadata> {
    const payload = await this.request(`/layouts/${encodeURIComponent(layoutName)}/metadata`, { method: "GET" }, token);
    const data = extractResponseData(payload);
    const fieldMeta = Array.isArray(data.fieldMetaData) ? data.fieldMetaData : [];

    return {
      name: layoutName,
      label: layoutName,
      type: "layout",
      columns: fieldMeta.map((field, index) => {
        const raw = field as Record<string, unknown>;
        const name = String(raw.name || "").trim();
        return {
          name,
          label: String(raw.displayName || name).trim() || name,
          type: String(raw.type || raw.result || "unknown").trim() || "unknown",
          nullable: raw.notEmpty !== true,
          ordinal: index + 1
        };
      }).filter((field) => field.name)
    };
  }

  public async getDatabaseMetadata(): Promise<DatabaseMetadata> {
    return await this.withSession(async (token) => {
      const layouts = await this.listLayouts(token);
      const limitedLayouts = layouts.slice(0, 50);
      const tables: DatabaseTableMetadata[] = [];

      for (const layout of limitedLayouts) {
        try {
          tables.push(await this.getLayoutMetadata(token, layout.name));
        } catch {
          tables.push({
            name: layout.name,
            label: layout.name,
            type: "layout",
            columns: []
          });
        }
      }

      return {
        connectorId: this.config.id,
        connectorName: this.config.name,
        connectorType: this.config.connectorType,
        databaseName: this.databaseName,
        refreshedAt: new Date().toISOString(),
        tables
      };
    });
  }

  public async readRecords(rawDefinition: string, limit = 100): Promise<Record<string, unknown>[]> {
    const definition = parseSourceDefinition(rawDefinition);
    const layout = String(definition.layout || "").trim();
    if (!layout) {
      throw new Error("FileMaker SourceDefinition benoetigt layout");
    }

    const cappedLimit = Math.max(1, Math.min(limit, 500));
    return await this.withSession(async (token) => {
      const hasQuery = Array.isArray(definition.query) && definition.query.length > 0;
      const payload = hasQuery
        ? await this.request(`/layouts/${encodeURIComponent(layout)}/_find`, {
            method: "POST",
            body: JSON.stringify({
              query: definition.query,
              sort: Array.isArray(definition.sort) ? definition.sort : undefined,
              limit: cappedLimit,
              offset: Math.max(1, Number(definition.offset || 1) || 1)
            })
          }, token)
        : await this.request(
            `/layouts/${encodeURIComponent(layout)}/records?_limit=${cappedLimit}&_offset=${Math.max(1, Number(definition.offset || 1) || 1)}`,
            { method: "GET" },
            token
          );

      const records = extractResponseData(payload).data;
      return Array.isArray(records)
        ? records.map((record) => normalizeFileMakerRecord(record, definition.fields))
        : [];
    });
  }
}

export function parseFileMakerSourceDefinition(rawDefinition: string): FileMakerSourceDefinition {
  return parseSourceDefinition(rawDefinition);
}
