import http from "node:http";
import { SaasAgentHeartbeat, SaasAgentRegistrationRequest } from "../types/saas-control-plane";
import { SaasRepository } from "./saas-repository";
import { renderSaasPortal } from "./saas-portal";

const MAX_JSON_BYTES = 512 * 1024;

function sendJson(res: http.ServerResponse, statusCode: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(payload);
}

function sendHtml(res: http.ServerResponse, statusCode: number, html: string): void {
  res.writeHead(statusCode, {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(html);
}

function extractBearerToken(req: http.IncomingMessage): string {
  const authorization = String(req.headers.authorization || "").trim();
  const match = /^Bearer\s+(.+)$/i.exec(authorization);
  return match ? match[1].trim() : "";
}

function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_JSON_BYTES) {
        reject(new Error("Request body too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8").trim();
        resolve(raw ? JSON.parse(raw) : {});
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function isHeartbeat(value: unknown): value is SaasAgentHeartbeat {
  const candidate = value as Partial<SaasAgentHeartbeat>;
  return !!candidate
    && typeof candidate === "object"
    && typeof candidate.idempotencyKey === "string"
    && typeof candidate.agentId === "string"
    && typeof candidate.tenantKey === "string"
    && typeof candidate.projectKey === "string"
    && typeof candidate.sentAt === "string"
    && typeof candidate.status === "string"
    && typeof candidate.mode === "string";
}

function isRegistrationClaim(value: unknown): value is SaasAgentRegistrationRequest {
  const candidate = value as Partial<SaasAgentRegistrationRequest>;
  return !!candidate
    && typeof candidate === "object"
    && typeof candidate.tenantKey === "string"
    && typeof candidate.projectKey === "string"
    && typeof candidate.registrationToken === "string"
    && typeof candidate.agentInstallationId === "string"
    && typeof candidate.agentVersion === "string"
    && typeof candidate.hostFingerprint === "string"
    && typeof candidate.preferredMode === "string"
    && Array.isArray(candidate.capabilities);
}

export function createSaasApiServer(repository: SaasRepository): http.Server {
  return http.createServer(async (req, res) => {
    const requestUrl = new URL(req.url || "/", "http://127.0.0.1");

    try {
      if (req.method === "GET" && requestUrl.pathname === "/health") {
        await repository.checkHealth();
        sendJson(res, 200, { ok: true });
        return;
      }

      if (req.method === "GET" && (requestUrl.pathname === "/" || requestUrl.pathname === "/portal")) {
        const snapshot = await repository.getPortalSnapshot();
        sendHtml(res, 200, renderSaasPortal(snapshot));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/saas/v1/overview") {
        const snapshot = await repository.getPortalSnapshot();
        sendJson(res, 200, snapshot);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/agent/v1/heartbeats") {
        const bearerToken = extractBearerToken(req);
        if (!bearerToken) {
          sendJson(res, 401, { error: "missing_bearer_token" });
          return;
        }

        const body = await readJsonBody(req);
        if (!isHeartbeat(body)) {
          sendJson(res, 400, { error: "invalid_heartbeat" });
          return;
        }

        await repository.acceptAgentHeartbeat(body, bearerToken);
        sendJson(res, 202, { accepted: true, serverTime: new Date().toISOString() });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/agent/v1/registrations/claim") {
        const body = await readJsonBody(req);
        if (!isRegistrationClaim(body)) {
          sendJson(res, 400, { error: "invalid_registration_claim" });
          return;
        }

        const registration = await repository.claimAgentRegistration(body);
        sendJson(res, 201, registration);
        return;
      }

      sendJson(res, 404, { error: "not_found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Internal server error";
      const status = message === "Invalid agent credential"
        ? 403
        : message === "Invalid registration token"
          ? 403
          : 500;
      sendJson(res, status, { error: status === 403 ? "forbidden" : "internal_error" });
    }
  });
}
