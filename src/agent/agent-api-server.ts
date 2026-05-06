import http from "node:http";
import { triggerDashboardUpdate, getDashboardUpdateStatus } from "../server/dashboard-update-service";
import { buildSystemHealthSnapshot, HealthSnapshot } from "../server/health-snapshot";
import { readConfiguredSalesforceInstances, writeConfiguredSalesforceInstances, type SalesforceInstanceEnvConfig } from "../server/admin-data-service";

function getAgentApiToken(): string {
  return String(process.env.AGENT_API_TOKEN || "").trim();
}

function isAuthorized(req: http.IncomingMessage): boolean {
  const configuredToken = getAgentApiToken();
  if (!configuredToken) {
    return false;
  }

  const authorization = Array.isArray(req.headers.authorization) ? req.headers.authorization[0] : req.headers.authorization;
  const token = String(authorization || "").replace(/^Bearer\s+/i, "").trim();
  return token === configuredToken;
}

export function isAgentApiEnabled(): boolean {
  const enabled = String(process.env.AGENT_API_ENABLED || "1").trim().toLowerCase();
  return enabled !== "0" && enabled !== "false" && enabled !== "off";
}

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  return raw ? (JSON.parse(raw) as unknown) : {};
}

function normalizeAgentInstancesPayload(input: unknown): SalesforceInstanceEnvConfig[] {
  const items = (
    input && typeof input === "object" && !Array.isArray(input) && "items" in input
      ? (input as { items?: unknown }).items
      : input
  );

  if (!Array.isArray(items)) {
    throw new Error("Instanzliste fehlt oder ist ungueltig.");
  }

  return items.map((item, index) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      throw new Error(`Instanz ${index + 1} ist ungueltig.`);
    }

    const candidate = item as Record<string, unknown>;
    const id = String(candidate.id || "").trim();
    const loginUrl = String(candidate.loginUrl || "").trim();
    const name = String(candidate.name || id).trim();
    const clientId = typeof candidate.clientId === "string" ? candidate.clientId.trim() : undefined;
    const clientSecret = typeof candidate.clientSecret === "string" ? candidate.clientSecret.trim() : undefined;
    const clientIdEnv = typeof candidate.clientIdEnv === "string" ? candidate.clientIdEnv.trim() : undefined;
    const clientSecretEnv = typeof candidate.clientSecretEnv === "string" ? candidate.clientSecretEnv.trim() : undefined;
    const queryLimit = Number(candidate.queryLimit);

    if (!id || !loginUrl) {
      throw new Error(`Instanz ${index + 1} benoetigt mindestens id und loginUrl.`);
    }

    if (!clientId && !clientIdEnv) {
      throw new Error(`Instanz ${id} benoetigt clientId oder clientIdEnv.`);
    }

    if (!clientSecret && !clientSecretEnv) {
      throw new Error(`Instanz ${id} benoetigt clientSecret oder clientSecretEnv.`);
    }

    return {
      id,
      name,
      loginUrl,
      clientId,
      clientSecret,
      clientIdEnv,
      clientSecretEnv,
      queryLimit: Number.isFinite(queryLimit) && queryLimit > 0 ? Math.floor(queryLimit) : undefined
    } satisfies SalesforceInstanceEnvConfig;
  });
}

export function createAgentApiServer(getHealthSnapshot: () => HealthSnapshot): http.Server {
  return http.createServer((req, res) => {
    void (async () => {
      const requestUrl = new URL(req.url || "/", "http://localhost");
      const sendJson = (statusCode: number, payload: unknown): void => {
        res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify(payload));
      };

      if (!isAuthorized(req)) {
        sendJson(401, { error: "Agent-API Token fehlt oder ist ungueltig" });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/agent/health") {
        sendJson(200, await buildSystemHealthSnapshot(getHealthSnapshot()));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/agent/update-status") {
        sendJson(200, await getDashboardUpdateStatus());
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/agent/instances") {
        sendJson(200, {
          items: readConfiguredSalesforceInstances().map((item) => ({
            id: item.id,
            name: item.name,
            loginUrl: item.loginUrl,
            clientIdConfigured: Boolean(item.clientId || item.clientIdEnv),
            clientSecretConfigured: Boolean(item.clientSecret || item.clientSecretEnv),
            queryLimit: item.queryLimit
          }))
        });
        return;
      }

      if (req.method === "PUT" && requestUrl.pathname === "/api/agent/instances") {
        const body = await readJsonBody(req);
        const items = normalizeAgentInstancesPayload(body);
        writeConfiguredSalesforceInstances(items);
        sendJson(200, { ok: true, count: items.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/agent/update-now") {
        const result = await triggerDashboardUpdate();
        sendJson(result.ok ? 200 : 500, result);
        return;
      }

      sendJson(404, { error: "Not Found" });
    })().catch((error) => {
      res.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown server error" }));
    });
  });
}
