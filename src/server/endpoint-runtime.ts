import http from "node:http";
import { Logger } from "pino";
import { ConnectorConfig, SalesforceClient } from "../clients/salesforce/salesforce-client";
import { getSalesforceConfig } from "../infrastructure/config/salesforce-config";
import { runEndpointScheduleRequest } from "../agent/agent-runner";
import { IntegrationSchedule } from "../types/integration-schedule";
import {
  createEndpointRecords,
  parseEndpointSourceDefinition,
  validateEndpointRequestBody
} from "../source-adapters/endpoint/endpoint-source-adapter";

interface EndpointRoute {
  connector: ConnectorConfig;
  schedule: IntegrationSchedule;
  method: string;
  fullPath: string;
  successStatus: number;
  errorStatus: number;
}

interface EndpointRouteCache {
  expiresAt: number;
  routes: EndpointRoute[];
}

interface EndpointAuthResult {
  ok: boolean;
  statusCode?: number;
  error?: string;
  claims?: Record<string, unknown>;
}

const ROUTE_CACHE_TTL_MS = Math.max(0, Number(process.env.AGENT_ENDPOINT_ROUTE_CACHE_MS || 15_000) || 15_000);
const DEFAULT_MAX_BODY_BYTES = Math.max(1024, Number(process.env.AGENT_ENDPOINT_MAX_BODY_BYTES || 1024 * 1024) || 1024 * 1024);
let routeCache: EndpointRouteCache | undefined;

function normalizePath(value: unknown): string {
  const raw = String(value || "").trim().replace(/\/+$/, "");
  if (!raw) {
    return "";
  }
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function joinPath(rootPath: string, relativePath: string): string {
  const root = normalizePath(rootPath);
  const relative = String(relativePath || "").trim();
  const normalizedRelative = relative.startsWith("/") ? relative : `/${relative}`;
  return `${root}${normalizedRelative}`.replace(/\/{2,}/g, "/") || "/";
}

function isEndpointConnector(connector: ConnectorConfig): boolean {
  const normalized = String(connector.connectorType || "").trim().toUpperCase();
  return normalized === "ENDPOINT" || normalized === "AGENT_ENDPOINT";
}

function mapScheduleRecord(record: Awaited<ReturnType<SalesforceClient["querySchedules"]>>[number]): IntegrationSchedule {
  return {
    id: record.Id,
    name: record.Name,
    active: record.Active__c,
    sourceSystem: record.SourceSystem__c || "",
    targetSystem: record.TargetSystem__c || "",
    objectName: record.ObjectName__c || "",
    operation: record.Operation__c || "",
    connectorId: record.MSD_Connector__c,
    mappingDefinition: record.MSD_MappingDefinition__c,
    direction: record.MSD_Direction__c,
    sourceType: record.MSD_SourceType__c,
    targetType: record.MSD_TargetType__c,
    sourceDefinition: record.MSD_SourceDefinition__c,
    targetDefinition: record.MSD_TargetDefinition__c,
    batchSize: record.BatchSize__c || 100,
    nextRunAt: record.NextRunAt__c,
    lastRunAt: record.LastRunAt__c
  };
}

async function loadEndpointRoutes(logger: Logger): Promise<EndpointRoute[]> {
  const now = Date.now();
  if (routeCache && routeCache.expiresAt > now) {
    return routeCache.routes;
  }

  const client = new SalesforceClient(getSalesforceConfig());
  await client.login();

  const [connectors, scheduleRecords] = await Promise.all([
    client.queryConnectors(),
    client.querySchedules(true)
  ]);

  const connectorById = new Map(connectors.filter((connector) => connector.active).map((connector) => [connector.id, connector]));
  const routes: EndpointRoute[] = [];

  for (const record of scheduleRecords) {
    const schedule = mapScheduleRecord(record);
    if (String(schedule.sourceType || "").trim().toUpperCase() !== "ENDPOINT") {
      continue;
    }
    if (!schedule.connectorId) {
      continue;
    }

    const connector = connectorById.get(schedule.connectorId);
    if (!connector || !isEndpointConnector(connector)) {
      continue;
    }

    try {
      const connectorRootPath = normalizePath(connector.parameters.rootPath || connector.parameters.basePath || connector.parameters.path);
      const definition = parseEndpointSourceDefinition(schedule.sourceDefinition || "");
      const fullPath = joinPath(connectorRootPath, definition.path);
      routes.push({
        connector,
        schedule,
        method: definition.method,
        fullPath,
        successStatus: definition.response.successStatus,
        errorStatus: definition.response.errorStatus
      });
    } catch (error) {
      logger.warn(
        {
          scheduleId: schedule.id,
          connectorId: connector.id,
          error: error instanceof Error ? error.message : String(error)
        },
        "Endpoint route configuration ignored"
      );
    }
  }

  routeCache = {
    expiresAt: now + ROUTE_CACHE_TTL_MS,
    routes
  };
  return routes;
}

function getHeader(req: http.IncomingMessage, name: string): string {
  const value = req.headers[name.toLowerCase()];
  if (Array.isArray(value)) {
    return value[0] || "";
  }
  return String(value || "");
}

function getBearerToken(req: http.IncomingMessage): string {
  const authorization = getHeader(req, "authorization");
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match?.[1] ? match[1].trim() : "";
}

async function introspectToken(token: string, oauth2: Record<string, unknown>): Promise<Record<string, unknown> | null> {
  const introspectionUrl = String(oauth2.introspectionUrl || oauth2.tokenIntrospectionUrl || "").trim();
  if (!introspectionUrl) {
    return null;
  }

  const body = new URLSearchParams({ token });
  const headers: Record<string, string> = {
    Accept: "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
  };

  const clientId = String(oauth2.clientId || "").trim();
  const clientSecret = String(oauth2.clientSecret || "").trim();
  if (clientId && clientSecret) {
    headers.Authorization = `Basic ${Buffer.from(`${clientId}:${clientSecret}`, "utf8").toString("base64")}`;
  }

  const response = await fetch(introspectionUrl, {
    method: "POST",
    headers,
    body: body.toString()
  });
  if (!response.ok) {
    throw new Error(`OAuth2 Introspection failed: ${response.status} ${response.statusText}`);
  }

  const payload = await response.json() as Record<string, unknown>;
  return payload.active === true ? payload : null;
}

async function authenticateEndpointRequest(req: http.IncomingMessage, connector: ConnectorConfig): Promise<EndpointAuthResult> {
  const parameters = connector.parameters || {};
  const authType = String(parameters.authType || "none").trim().toLowerCase();
  if (!authType || authType === "none") {
    return { ok: true, claims: {} };
  }

  if (authType !== "oauth2" && authType !== "bearer") {
    return { ok: false, statusCode: 403, error: `Auth Type ${authType} wird fuer Endpoint Connectoren nicht unterstuetzt` };
  }

  const token = getBearerToken(req);
  if (!token) {
    return { ok: false, statusCode: 401, error: "Bearer Token fehlt" };
  }

  const oauth2 = parameters.oauth2 && typeof parameters.oauth2 === "object" && !Array.isArray(parameters.oauth2)
    ? parameters.oauth2 as Record<string, unknown>
    : parameters;
  const introspected = await introspectToken(token, oauth2);
  if (introspected) {
    return { ok: true, claims: introspected };
  }

  const configuredToken = String(parameters.token || parameters.bearerToken || "").trim()
    || (connector.secretKey ? String(process.env[connector.secretKey] || "").trim() : "");
  if (configuredToken && token === configuredToken) {
    return { ok: true, claims: { subject: connector.name, authType } };
  }

  return { ok: false, statusCode: 403, error: "Bearer Token ist ungueltig" };
}

async function readBody(req: http.IncomingMessage, maxBytes: number): Promise<unknown> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buffer.length;
    if (total > maxBytes) {
      throw new Error(`Request Body ist groesser als ${maxBytes} Bytes`);
    }
    chunks.push(buffer);
  }

  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw.trim()) {
    return {};
  }

  const contentType = getHeader(req, "content-type").toLowerCase();
  if (contentType.includes("application/json")) {
    return JSON.parse(raw);
  }
  return raw;
}

function selectQuery(searchParams: URLSearchParams, fields: string[]): Record<string, unknown> {
  const selected: Record<string, unknown> = {};
  const source = fields.length ? fields : Array.from(searchParams.keys());
  for (const field of source) {
    if (searchParams.has(field)) {
      selected[field] = searchParams.get(field);
    }
  }
  return selected;
}

function selectHeaders(req: http.IncomingMessage, fields: string[]): Record<string, unknown> {
  const selected: Record<string, unknown> = {};
  for (const field of fields) {
    if (field === "authorization") {
      continue;
    }
    const value = getHeader(req, field);
    if (value) {
      selected[field] = value;
    }
  }
  return selected;
}

function sendJson(res: http.ServerResponse, statusCode: number, payload: unknown): void {
  res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

export async function handleEndpointRuntimeRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  requestUrl: URL,
  logger: Logger
): Promise<boolean> {
  let routes: EndpointRoute[];
  try {
    routes = await loadEndpointRoutes(logger);
  } catch (error) {
    logger.debug(
      { error: error instanceof Error ? error.message : String(error) },
      "Endpoint routes could not be loaded"
    );
    return false;
  }
  const route = routes.find((item) => item.method === String(req.method || "GET").toUpperCase() && item.fullPath === requestUrl.pathname);
  if (!route) {
    return false;
  }

  const auth = await authenticateEndpointRequest(req, route.connector);
  if (!auth.ok) {
    sendJson(res, auth.statusCode || 403, { error: auth.error || "Endpoint Auth fehlgeschlagen" });
    return true;
  }

  const definition = parseEndpointSourceDefinition(route.schedule.sourceDefinition || "");
  try {
    const maxBodyBytes = Math.max(1024, Number(route.connector.parameters?.limits && typeof route.connector.parameters.limits === "object"
      ? (route.connector.parameters.limits as Record<string, unknown>).maxBodyBytes
      : route.connector.parameters?.maxBodyBytes) || DEFAULT_MAX_BODY_BYTES);
    const body = await readBody(req, maxBodyBytes);
    validateEndpointRequestBody(definition, body);

    const correlationId = getHeader(req, "x-correlation-id") || getHeader(req, "x-request-id") || `ENDPOINT-${Date.now()}`;
    const records = createEndpointRecords(definition, {
      body,
      query: selectQuery(requestUrl.searchParams, definition.queryFields),
      headers: selectHeaders(req, definition.headerFields),
      request: {
        method: req.method,
        path: requestUrl.pathname,
        receivedAt: new Date().toISOString(),
        remoteAddress: req.socket.remoteAddress
      },
      auth: auth.claims || {}
    });

    const outcome = await runEndpointScheduleRequest(
      logger,
      process.env.AGENT_ID || "local-web-endpoint",
      route.schedule.id,
      records,
      correlationId
    );

    sendJson(res, outcome.status === "Success" ? definition.response.successStatus : definition.response.errorStatus, {
      status: outcome.status || "Failed",
      runId: outcome.runId,
      correlationId: outcome.correlationId || correlationId,
      scheduleId: route.schedule.id
    });
    return true;
  } catch (error) {
    logger.warn(
      {
        scheduleId: route.schedule.id,
        connectorId: route.connector.id,
        error: error instanceof Error ? error.message : String(error)
      },
      "Endpoint request failed"
    );
    sendJson(res, definition.response.errorStatus, {
      status: "Failed",
      error: error instanceof Error ? error.message : String(error),
      scheduleId: route.schedule.id
    });
    return true;
  }
}
