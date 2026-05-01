import { ConnectorConfig } from "../../clients/salesforce/salesforce-client";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";

interface RestSourceDefinition {
  endpoint?: string;
  method?: string;
  headers?: Record<string, string>;
  query?: Record<string, string | number | boolean>;
  body?: unknown;
  resultPath?: string;
  transform?: string;
}

function parseRestSourceDefinition(rawDefinition: string): RestSourceDefinition {
  const trimmed = String(rawDefinition || "").trim();
  if (!trimmed) {
    throw new Error("REST SourceDefinition darf nicht leer sein");
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("REST SourceDefinition muss ein JSON-Objekt sein");
    }
    return parsed as RestSourceDefinition;
  } catch {
    throw new Error("REST SourceDefinition muss gueltiges JSON sein");
  }
}

function resolvePasswordOrToken(config: ConnectorConfig): string {
  const inlinePassword = typeof config.parameters?.password === "string"
    ? config.parameters.password.trim()
    : "";
  if (inlinePassword) {
    return inlinePassword;
  }

  if (!config.secretKey) {
    return "";
  }

  return String(process.env[config.secretKey] || "").trim();
}

function toAbsoluteUrl(baseUrl: string, endpoint: string): string {
  const trimmedEndpoint = String(endpoint || "").trim();
  if (!trimmedEndpoint) {
    throw new Error("REST endpoint fehlt in SourceDefinition");
  }

  if (/^https?:\/\//i.test(trimmedEndpoint)) {
    return trimmedEndpoint;
  }

  const normalizedBase = String(baseUrl || "").trim().replace(/\/+$/, "");
  if (!normalizedBase) {
    throw new Error("REST Connector benoetigt parameters.baseUrl oder einen absoluten endpoint in SourceDefinition");
  }

  const normalizedEndpoint = trimmedEndpoint.startsWith("/") ? trimmedEndpoint : `/${trimmedEndpoint}`;
  return `${normalizedBase}${normalizedEndpoint}`;
}

function getByPath(value: unknown, path: string): unknown {
  const tokens = String(path || "")
    .split(".")
    .map((token) => token.trim())
    .filter(Boolean);

  let current: unknown = value;
  for (const token of tokens) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (Array.isArray(current)) {
      const index = Number(token);
      if (Number.isNaN(index) || index < 0 || index >= current.length) {
        return undefined;
      }
      current = current[index];
      continue;
    }

    if (typeof current === "object") {
      current = (current as Record<string, unknown>)[token];
      continue;
    }

    return undefined;
  }

  return current;
}

function normalizeRows(payload: unknown, resultPath?: string, limit?: number): Record<string, unknown>[] {
  const scopedPayload = String(resultPath || "").trim() ? getByPath(payload, String(resultPath || "").trim()) : payload;

  let items: unknown[];
  if (Array.isArray(scopedPayload)) {
    items = scopedPayload;
  } else if (scopedPayload && typeof scopedPayload === "object") {
    items = [scopedPayload];
  } else {
    throw new Error("REST Antwort enthaelt keine Datensaetze (Array/Objekt)");
  }

  const normalized = items.map((item) => {
    if (item && typeof item === "object" && !Array.isArray(item)) {
      return item as Record<string, unknown>;
    }
    return { value: item };
  });

  if (Number.isFinite(limit) && Number(limit) > 0) {
    return normalized.slice(0, Number(limit));
  }

  return normalized;
}

function normalizeEcbExrSeriesRows(payload: unknown, limit?: number): Record<string, unknown>[] {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("ECB JSON-Antwort ist ungueltig");
  }

  const root = payload as Record<string, unknown>;
  const seriesDimensions = getByPath(root, "structure.dimensions.series");
  const observationValues = getByPath(root, "structure.dimensions.observation.0.values");
  const series = getByPath(root, "dataSets.0.series");

  if (!Array.isArray(seriesDimensions) || !series || typeof series !== "object" || Array.isArray(series)) {
    throw new Error("ECB JSON-Antwort enthaelt keine gueltige EXR series-Struktur");
  }

  const seriesDims = seriesDimensions as Array<{ id?: string; values?: Array<{ id?: string }> }>;
  const currencyDimIndex = seriesDims.findIndex((dim) => String(dim?.id || "").toUpperCase() === "CURRENCY");
  const denomDimIndex = seriesDims.findIndex((dim) => String(dim?.id || "").toUpperCase() === "CURRENCY_DENOM");

  if (currencyDimIndex < 0 || denomDimIndex < 0) {
    throw new Error("ECB EXR series enthaelt keine CURRENCY/CURRENCY_DENOM Dimension");
  }

  const obsLookup = Array.isArray(observationValues)
    ? (observationValues as Array<{ id?: string }>).map((v) => String(v?.id || ""))
    : [];

  const rows: Record<string, unknown>[] = [];
  for (const [seriesKey, seriesValue] of Object.entries(series as Record<string, unknown>)) {
    if (!seriesValue || typeof seriesValue !== "object" || Array.isArray(seriesValue)) {
      continue;
    }

    const keyParts = seriesKey.split(":");
    const currencyIndex = Number(keyParts[currencyDimIndex]);
    const denomIndex = Number(keyParts[denomDimIndex]);

    const currency = seriesDims[currencyDimIndex]?.values?.[currencyIndex]?.id;
    const denominator = seriesDims[denomDimIndex]?.values?.[denomIndex]?.id;

    const observations = (seriesValue as Record<string, unknown>).observations;
    if (!observations || typeof observations !== "object" || Array.isArray(observations)) {
      continue;
    }

    const obsEntries = Object.entries(observations as Record<string, unknown>);
    if (!obsEntries.length) {
      continue;
    }

    const [obsIndex, obsValue] = obsEntries[0];
    const valueArray = Array.isArray(obsValue) ? obsValue : [];
    const rate = valueArray[0];
    if (typeof rate !== "number") {
      continue;
    }

    const obsIdx = Number(obsIndex);
    const timePeriod = Number.isInteger(obsIdx) && obsIdx >= 0 ? obsLookup[obsIdx] : undefined;
    const currencyPair = `${String(currency || "")}/${String(denominator || "")}`;
    const currencyPairKey = `${String(currency || "")}-${String(denominator || "")}`;

    rows.push({
      seriesKey,
      currency,
      denominator,
      currencyPair,
      currencyPairKey,
      rate,
      timePeriod,
      source: "ECB"
    });
  }

  if (!rows.length) {
    throw new Error("ECB EXR series enthaelt keine verwertbaren Beobachtungen");
  }

  if (Number.isFinite(limit) && Number(limit) > 0) {
    return rows.slice(0, Number(limit));
  }

  return rows;
}

function applyAuthHeaders(config: ConnectorConfig, headers: Record<string, string>): void {
  const authType = String(config.parameters?.authType || "none").trim().toLowerCase();
  const fallbackSecret = resolvePasswordOrToken(config);

  if (authType === "bearer") {
    const token = String(config.parameters?.token || fallbackSecret || "").trim();
    if (!token) {
      throw new Error(`REST Connector ${config.name} hat kein Token konfiguriert (parameters.token oder Secret Key)`);
    }
    headers.Authorization = `Bearer ${token}`;
    return;
  }

  if (authType === "basic") {
    const user = String(config.parameters?.user || config.parameters?.username || "").trim();
    const password = String(config.parameters?.password || fallbackSecret || "").trim();
    if (!user || !password) {
      throw new Error(`REST Connector ${config.name} benoetigt user/username und Passwort fuer Basic Auth`);
    }
    const encoded = Buffer.from(`${user}:${password}`, "utf8").toString("base64");
    headers.Authorization = `Basic ${encoded}`;
    return;
  }

  if (authType === "apikey") {
    const apiKeyHeader = String(config.parameters?.apiKeyHeader || "X-API-Key").trim() || "X-API-Key";
    const apiKey = String(config.parameters?.apiKey || fallbackSecret || "").trim();
    if (!apiKey) {
      throw new Error(`REST Connector ${config.name} benoetigt API-Key (parameters.apiKey oder Secret Key)`);
    }
    headers[apiKeyHeader] = apiKey;
  }
}

export async function fetchRestRows(
  connectorConfig: ConnectorConfig,
  rawDefinition: string,
  limit?: number
): Promise<Record<string, unknown>[]> {
  const definition = parseRestSourceDefinition(rawDefinition);
  const method = String(definition.method || "GET").trim().toUpperCase() || "GET";
  const baseUrl = String(connectorConfig.parameters?.baseUrl || "").trim();
  const url = new URL(toAbsoluteUrl(baseUrl, String(definition.endpoint || "").trim()));

  Object.entries(definition.query || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return;
    }
    url.searchParams.set(key, String(value));
  });

  const headers: Record<string, string> = {
    Accept: "application/json"
  };

  const parameterHeaders = connectorConfig.parameters?.headers;
  if (parameterHeaders && typeof parameterHeaders === "object" && !Array.isArray(parameterHeaders)) {
    Object.entries(parameterHeaders as Record<string, unknown>).forEach(([key, value]) => {
      if (typeof value === "string" && value.trim()) {
        headers[key] = value.trim();
      }
    });
  }

  Object.entries(definition.headers || {}).forEach(([key, value]) => {
    if (typeof value === "string" && value.trim()) {
      headers[key] = value.trim();
    }
  });

  applyAuthHeaders(connectorConfig, headers);

  const hasBody = method !== "GET" && method !== "HEAD" && definition.body !== undefined;
  if (hasBody && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(url.toString(), {
    method,
    headers,
    body: hasBody ? JSON.stringify(definition.body) : undefined
  });

  if (!response.ok) {
    const rawText = await response.text();
    throw new Error(`REST Request fehlgeschlagen (${response.status} ${response.statusText}): ${rawText.slice(0, 300)}`);
  }

  const rawText = await response.text();
  let payload: unknown;
  try {
    payload = rawText ? JSON.parse(rawText) : [];
  } catch {
    throw new Error("REST Antwort ist kein gueltiges JSON");
  }

  const transform = String(definition.transform || "").trim().toUpperCase();
  if (transform === "ECB_EXR_SERIES") {
    return normalizeEcbExrSeriesRows(payload, limit);
  }

  return normalizeRows(payload, definition.resultPath, limit);
}

export class RestApiSourceAdapter implements SourceAdapter {
  private readonly connectorConfig: ConnectorConfig;
  private readonly sourceDefinition: string;

  public constructor(connectorConfig: ConnectorConfig, sourceDefinition: string) {
    this.connectorConfig = connectorConfig;
    this.sourceDefinition = sourceDefinition;
  }

  public async readRecords(context: TransferContext): Promise<GenericRecord[]> {
    const rows = await fetchRestRows(this.connectorConfig, this.sourceDefinition, context.batchSize);
    return rows.map((row) => ({ values: { ...row } }));
  }
}
