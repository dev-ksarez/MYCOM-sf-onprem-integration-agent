import http from "node:http";
import { SaasAgentHeartbeat, SaasAgentRegistrationRequest, SaasRunReport } from "../types/saas-control-plane";
import { SaasRepository } from "./saas-repository";
import { SaasServerConfig } from "./saas-config";
import {
  getSessionFromRequest, setSessionCookie, clearSessionCookie,
  createSessionToken, verifyPassword, hashPassword, PortalSession
} from "./saas-auth";
import { renderAdminDashboard, renderAdminTenants, renderAdminTenantDetail, renderAdminUsers } from "./portal-admin";
import {
  renderCustomerDashboard, renderCustomerAgents, renderCustomerRuns,
  renderCustomerContract, renderCustomerTokens,
  renderCustomerRunDetail, renderCustomerConnectors, renderCustomerSchedulers
} from "./portal-customer";
import { loginPage } from "./portal-layout";

const MAX_JSON_BYTES = 512 * 1024;
const MAX_FORM_BYTES = 64 * 1024;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function sendJson(res: http.ServerResponse, statusCode: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  res.end(payload);
}

function sendHtml(res: http.ServerResponse, statusCode: number, html: string): void {
  res.writeHead(statusCode, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
  res.end(html);
}

function redirect(res: http.ServerResponse, location: string): void {
  res.writeHead(302, { location });
  res.end();
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
      if (size > MAX_JSON_BYTES) { reject(new Error("Request body too large")); req.destroy(); return; }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8").trim();
        resolve(raw ? JSON.parse(raw) : {});
      } catch (error) { reject(error); }
    });
    req.on("error", reject);
  });
}

function readRawBody(req: http.IncomingMessage, maxBytes: number): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > maxBytes) { reject(new Error("Request body too large")); req.destroy(); return; }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

async function readFormBody(req: http.IncomingMessage): Promise<URLSearchParams> {
  const raw = await readRawBody(req, MAX_FORM_BYTES);
  return new URLSearchParams(raw);
}

function flashUrl(base: string, type: "ok" | "err", msg: string): string {
  return `${base}?flash=${type}&msg=${encodeURIComponent(msg)}`;
}

function getFlash(url: URL): { type: "ok" | "err"; msg: string } | undefined {
  const type = url.searchParams.get("flash");
  const msg = url.searchParams.get("msg");
  if ((type === "ok" || type === "err") && msg) return { type, msg };
  return undefined;
}

function isHeartbeat(value: unknown): value is SaasAgentHeartbeat {
  const c = value as Partial<SaasAgentHeartbeat>;
  return !!c && typeof c === "object"
    && typeof c.idempotencyKey === "string" && typeof c.agentId === "string"
    && typeof c.tenantKey === "string" && typeof c.projectKey === "string"
    && typeof c.sentAt === "string" && typeof c.status === "string" && typeof c.mode === "string";
}

function isRunReport(value: unknown): value is SaasRunReport {
  const c = value as Partial<SaasRunReport>;
  return !!c && typeof c === "object"
    && typeof c.idempotencyKey === "string" && typeof c.agentId === "string"
    && typeof c.tenantKey === "string" && typeof c.projectKey === "string"
    && typeof c.schedulerId === "string" && typeof c.direction === "string"
    && typeof c.status === "string" && typeof c.startedAt === "string";
}

function isRegistrationClaim(value: unknown): value is SaasAgentRegistrationRequest {
  const c = value as Partial<SaasAgentRegistrationRequest>;
  return !!c && typeof c === "object"
    && typeof c.tenantKey === "string" && typeof c.projectKey === "string"
    && typeof c.registrationToken === "string" && typeof c.agentInstallationId === "string"
    && typeof c.agentVersion === "string" && typeof c.hostFingerprint === "string"
    && typeof c.preferredMode === "string" && Array.isArray(c.capabilities);
}

function requireSession(req: http.IncomingMessage, jwtSecret: string): PortalSession | null {
  return getSessionFromRequest(req, jwtSecret);
}

function requireAdmin(session: PortalSession | null): session is PortalSession {
  return !!session && session.isSystemAdmin;
}

function requireCustomer(session: PortalSession | null): session is PortalSession {
  return !!session && !session.isSystemAdmin && !!session.tenantId;
}

export function createSaasApiServer(repository: SaasRepository, config: SaasServerConfig): http.Server {
  const { portalJwtSecret } = config;
  const secure = false; // Caddy terminates TLS, so cookies don't need Secure flag here

  return http.createServer(async (req, res) => {
    const requestUrl = new URL(req.url || "/", "http://127.0.0.1");
    const path = requestUrl.pathname;
    const method = req.method || "GET";

    try {

      // ── Health ──────────────────────────────────────────────────────────
      if (method === "GET" && path === "/health") {
        await repository.checkHealth();
        sendJson(res, 200, { ok: true });
        return;
      }

      // ── Portal root → redirect ──────────────────────────────────────────
      if (method === "GET" && (path === "/" || path === "/portal")) {
        const session = requireSession(req, portalJwtSecret);
        if (!session) { redirect(res, "/portal/login"); return; }
        redirect(res, session.isSystemAdmin ? "/portal/admin" : "/portal/customer");
        return;
      }

      // ── Login ───────────────────────────────────────────────────────────
      if (method === "GET" && path === "/portal/login") {
        const session = requireSession(req, portalJwtSecret);
        if (session) { redirect(res, session.isSystemAdmin ? "/portal/admin" : "/portal/customer"); return; }
        const errMsg = requestUrl.searchParams.get("err") || undefined;
        sendHtml(res, 200, loginPage(errMsg));
        return;
      }

      if (method === "POST" && path === "/api/portal/v1/auth/login") {
        const form = await readFormBody(req);
        const email = (form.get("email") || "").trim().toLowerCase();
        const password = form.get("password") || "";
        const user = email ? await repository.findUserByEmail(email) : null;
        if (!user || user.status !== "active" || !(await verifyPassword(password, user.passwordHash))) {
          redirect(res, "/portal/login?err=" + encodeURIComponent("Ungültige Anmeldedaten"));
          return;
        }
        await repository.updateUserLastLogin(user.id);

        let tenantId: string | null = null;
        let tenantKey: string | null = null;
        let role = null;
        if (!user.isSystemAdmin) {
          const membership = await repository.getUserTenantMembership(user.id);
          if (!membership) {
            redirect(res, "/portal/login?err=" + encodeURIComponent("Kein Mandant zugewiesen"));
            return;
          }
          tenantId = membership.tenantId;
          tenantKey = membership.tenantKey;
          role = membership.role as PortalSession["role"];
        }

        const token = createSessionToken({
          userId: user.id, email: user.email, displayName: user.displayName,
          isSystemAdmin: user.isSystemAdmin, tenantId, tenantKey, role
        }, portalJwtSecret);
        setSessionCookie(res, token, secure);
        redirect(res, user.isSystemAdmin ? "/portal/admin" : "/portal/customer");
        return;
      }

      if ((method === "GET" || method === "POST") && path === "/portal/logout") {
        clearSessionCookie(res);
        redirect(res, "/portal/login");
        return;
      }

      // ── Admin portal ────────────────────────────────────────────────────

      if (path.startsWith("/portal/admin")) {
        const session = requireSession(req, portalJwtSecret);
        if (!requireAdmin(session)) { redirect(res, "/portal/login"); return; }

        // GET /portal/admin → dashboard
        if (method === "GET" && path === "/portal/admin") {
          const snapshot = await repository.getPortalSnapshot();
          sendHtml(res, 200, renderAdminDashboard(snapshot, session));
          return;
        }

        // GET/POST /portal/admin/tenants
        if (path === "/portal/admin/tenants") {
          if (method === "POST") {
            const form = await readFormBody(req);
            const tenantKey = (form.get("tenantKey") || "").trim().toLowerCase();
            const name = (form.get("name") || "").trim();
            if (!tenantKey || !name) {
              const tenants = await repository.listAllTenants();
              sendHtml(res, 400, renderAdminTenants(tenants, session, { type: "err", msg: "Key und Name sind Pflichtfelder" }));
              return;
            }
            await repository.createTenant({ tenantKey, name });
            redirect(res, flashUrl("/portal/admin/tenants", "ok", `Mandant „${name}" angelegt`));
            return;
          }
          const tenants = await repository.listAllTenants();
          sendHtml(res, 200, renderAdminTenants(tenants, session, getFlash(requestUrl)));
          return;
        }

        // GET/POST /portal/admin/tenants/:id/*
        const tenantDetailMatch = /^\/portal\/admin\/tenants\/([^/]+)(\/.*)?$/.exec(path);
        if (tenantDetailMatch) {
          const tenantId = tenantDetailMatch[1];
          const sub = tenantDetailMatch[2] || "";

          if (!UUID_RE.test(tenantId)) { sendJson(res, 404, { error: "not_found" }); return; }

          if (method === "POST" && sub === "/license") {
            const form = await readFormBody(req);
            await repository.upsertLicense(tenantId, {
              contractReference: form.get("contractReference") || undefined,
              plan: form.get("plan") || "pilot",
              status: (form.get("status") || "trial") as "trial" | "active" | "expired" | "suspended",
              validFrom: form.get("validFrom") || null,
              validUntil: form.get("validUntil") || null,
              maxConnectors: Number(form.get("maxConnectors") || 5),
              maxSchedulers: Number(form.get("maxSchedulers") || 10),
              maxRecordsPerMonth: Number(form.get("maxRecordsPerMonth") || 10000),
              featureAi: form.has("featureAi"),
              featureMigration: form.has("featureMigration"),
              featureCustomConnector: form.has("featureCustomConnector"),
              featureCustomScheduler: form.has("featureCustomScheduler")
            });
            redirect(res, flashUrl(`/portal/admin/tenants/${tenantId}`, "ok", "Lizenz gespeichert"));
            return;
          }

          if (method === "POST" && sub === "/members") {
            const form = await readFormBody(req);
            const email = (form.get("email") || "").trim().toLowerCase();
            const role = (form.get("role") || "viewer").trim();
            const user = email ? await repository.findUserByEmail(email) : null;
            if (!user) {
              const details = await repository.getTenantDetails(tenantId);
              if (!details) { sendJson(res, 404, { error: "not_found" }); return; }
              sendHtml(res, 400, renderAdminTenantDetail(details, session, { type: "err", msg: `Benutzer mit E-Mail „${email}" nicht gefunden` }));
              return;
            }
            await repository.addTenantMember(tenantId, user.id, role);
            redirect(res, flashUrl(`/portal/admin/tenants/${tenantId}`, "ok", `${email} als ${role} hinzugefügt`));
            return;
          }

          if (method === "POST" && sub === "/projects") {
            const form = await readFormBody(req);
            const projectKey = (form.get("projectKey") || "").trim().toLowerCase();
            const name = (form.get("name") || "").trim();
            const mode = (form.get("mode") || "hybrid").trim();
            const timezone = (form.get("timezone") || "Europe/Berlin").trim();
            if (!projectKey || !name) {
              const details = await repository.getTenantDetails(tenantId);
              if (!details) { sendJson(res, 404, { error: "not_found" }); return; }
              sendHtml(res, 400, renderAdminTenantDetail(details, session, { type: "err", msg: "Projekt-Key und Name sind Pflichtfelder" }));
              return;
            }
            await repository.createProject({ tenantId, projectKey, name, mode, timezone });
            redirect(res, flashUrl(`/portal/admin/tenants/${tenantId}`, "ok", `Projekt „${name}" angelegt`));
            return;
          }

          if (method === "POST" && sub === "/status") {
            const form = await readFormBody(req);
            const status = (form.get("status") || "").trim();
            if (status === "active" || status === "suspended") {
              await repository.updateTenantStatus(tenantId, status);
            }
            redirect(res, flashUrl(`/portal/admin/tenants/${tenantId}`, "ok", "Status aktualisiert"));
            return;
          }

          if (method === "GET" && sub === "") {
            const details = await repository.getTenantDetails(tenantId);
            if (!details) { sendJson(res, 404, { error: "not_found" }); return; }
            sendHtml(res, 200, renderAdminTenantDetail(details, session, getFlash(requestUrl)));
            return;
          }
        }

        // GET/POST /portal/admin/users
        if (path === "/portal/admin/users") {
          if (method === "POST") {
            const form = await readFormBody(req);
            const email = (form.get("email") || "").trim().toLowerCase();
            const displayName = (form.get("displayName") || "").trim();
            const password = form.get("password") || "";
            const tenantKey = (form.get("tenantKey") || "").trim();
            const role = (form.get("role") || "viewer").trim();
            if (!email || !displayName || password.length < 8) {
              const users = await repository.listAllUsers();
              sendHtml(res, 400, renderAdminUsers(users, session, { type: "err", msg: "E-Mail, Name und Passwort (mind. 8 Zeichen) sind Pflichtfelder" }));
              return;
            }
            const passwordHash = await hashPassword(password);
            const userId = await repository.createUser({ email, displayName, passwordHash, isSystemAdmin: false });
            if (!userId) {
              const users = await repository.listAllUsers();
              sendHtml(res, 400, renderAdminUsers(users, session, { type: "err", msg: `E-Mail „${email}" ist bereits vergeben` }));
              return;
            }
            if (tenantKey) {
              const tenant = await repository.findTenantByKey(tenantKey);
              if (tenant) await repository.addTenantMember(tenant.id, userId, role);
            }
            redirect(res, flashUrl("/portal/admin/users", "ok", `Benutzer „${email}" angelegt`));
            return;
          }
          const users = await repository.listAllUsers();
          sendHtml(res, 200, renderAdminUsers(users, session, getFlash(requestUrl)));
          return;
        }

        sendJson(res, 404, { error: "not_found" });
        return;
      }

      // ── Customer portal ─────────────────────────────────────────────────

      if (path.startsWith("/portal/customer")) {
        const session = requireSession(req, portalJwtSecret);
        if (!requireCustomer(session)) { redirect(res, "/portal/login"); return; }
        const tenantId = session.tenantId!;

        if (method === "GET" && path === "/portal/customer") {
          const data = await repository.getCustomerDashboard(tenantId);
          sendHtml(res, 200, renderCustomerDashboard(data, session));
          return;
        }

        if (method === "GET" && path === "/portal/customer/agents") {
          const data = await repository.getCustomerDashboard(tenantId);
          sendHtml(res, 200, renderCustomerAgents(data.agents, session));
          return;
        }

        if (method === "GET" && path === "/portal/customer/runs") {
          const data = await repository.getCustomerDashboard(tenantId);
          sendHtml(res, 200, renderCustomerRuns(data.recentRuns, session));
          return;
        }

        if (method === "GET" && path === "/portal/customer/contract") {
          const data = await repository.getCustomerDashboard(tenantId);
          sendHtml(res, 200, renderCustomerContract(data, session));
          return;
        }

        if (path === "/portal/customer/tokens") {
          const tenantDetails = await repository.getTenantDetails(tenantId);
          const projects = tenantDetails?.projects.map((p) => ({ id: p.id, projectKey: p.projectKey, name: p.name })) ?? [];

          if (method === "POST") {
            const form = await readFormBody(req);
            const projectId = (form.get("projectId") || "").trim();
            if (!projectId || !UUID_RE.test(projectId)) {
              const tokens = await repository.listTenantRegistrationTokens(tenantId);
              sendHtml(res, 400, renderCustomerTokens(tokens, projects, session, { type: "err", msg: "Ungültige Projekt-ID" }));
              return;
            }
            const newToken = await repository.createRegistrationToken(projectId, tenantId);
            const tokens = await repository.listTenantRegistrationTokens(tenantId);
            sendHtml(res, 200, renderCustomerTokens(tokens, projects, session, undefined, newToken));
            return;
          }

          const tokens = await repository.listTenantRegistrationTokens(tenantId);
          sendHtml(res, 200, renderCustomerTokens(tokens, projects, session, getFlash(requestUrl)));
          return;
        }

        // ── Run detail ────────────────────────────────────────────────────
        const runDetailMatch = /^\/portal\/customer\/runs\/([0-9a-f-]{36})$/.exec(path);
        if (method === "GET" && runDetailMatch) {
          const detail = await repository.getRunDetail(runDetailMatch[1], tenantId);
          if (!detail) { sendJson(res, 404, { error: "not_found" }); return; }
          sendHtml(res, 200, renderCustomerRunDetail(detail, session));
          return;
        }

        // ── Connectors ────────────────────────────────────────────────────
        if (path === "/portal/customer/connectors") {
          const tenantDetails = await repository.getTenantDetails(tenantId);
          const projects = tenantDetails?.projects.map((p) => ({ id: p.id, projectKey: p.projectKey, name: p.name })) ?? [];
          if (method === "POST") {
            const form = await readFormBody(req);
            const projectId = (form.get("projectId") || "").trim();
            const connectorKey = (form.get("connectorKey") || "").trim().toLowerCase();
            const type = (form.get("type") || "other").trim();
            const displayName = (form.get("displayName") || "").trim();
            const secretPolicy = (form.get("secretPolicy") || "local-only").trim();
            let metadataJson: unknown = {};
            try { metadataJson = JSON.parse(form.get("metadataJson") || "{}"); } catch { /* ignore */ }
            if (!projectId || !connectorKey || !displayName) {
              const connectors = await repository.listTenantConnectors(tenantId);
              sendHtml(res, 400, renderCustomerConnectors(connectors, projects, session, { type: "err", msg: "Projekt, Key und Name sind Pflichtfelder" }));
              return;
            }
            await repository.upsertConnector(tenantId, projectId, { connectorKey, type, displayName, metadataJson, secretPolicy });
            redirect(res, flashUrl("/portal/customer/connectors", "ok", `Connector „${displayName}" gespeichert`));
            return;
          }
          const connectors = await repository.listTenantConnectors(tenantId);
          sendHtml(res, 200, renderCustomerConnectors(connectors, projects, session, getFlash(requestUrl)));
          return;
        }

        const connectorDeleteMatch = /^\/portal\/customer\/connectors\/([0-9a-f-]{36})\/delete$/.exec(path);
        if (method === "POST" && connectorDeleteMatch) {
          await repository.deleteConnector(tenantId, connectorDeleteMatch[1]);
          redirect(res, flashUrl("/portal/customer/connectors", "ok", "Connector gelöscht"));
          return;
        }

        // ── Schedulers ────────────────────────────────────────────────────
        if (path === "/portal/customer/schedulers") {
          const tenantDetails = await repository.getTenantDetails(tenantId);
          const projects = tenantDetails?.projects.map((p) => ({ id: p.id, projectKey: p.projectKey, name: p.name })) ?? [];
          const connectors = await repository.listTenantConnectors(tenantId);

          if (method === "POST") {
            const form = await readFormBody(req);
            const projectId = (form.get("projectId") || "").trim();
            const schedulerKey = (form.get("schedulerKey") || "").trim().toLowerCase();
            const name = (form.get("name") || "").trim();
            const direction = (form.get("direction") || "outbound").trim();
            const active = form.get("active") !== "false";
            const scheduleExpression = (form.get("scheduleExpression") || "").trim() || null;
            const connectorKey = (form.get("connectorKey") || "").trim() || null;
            const objectName = (form.get("objectName") || "").trim() || null;
            if (!projectId || !schedulerKey || !name) {
              sendHtml(res, 400, renderCustomerSchedulers(await repository.listTenantSchedulers(tenantId), connectors, projects, session, { type: "err", msg: "Projekt, Key und Name sind Pflichtfelder" }));
              return;
            }
            await repository.upsertScheduler(tenantId, projectId, { schedulerKey, name, direction, active, scheduleExpression, connectorKey, objectName });
            redirect(res, flashUrl("/portal/customer/schedulers", "ok", `Scheduler „${name}" gespeichert`));
            return;
          }
          const schedulers = await repository.listTenantSchedulers(tenantId);
          sendHtml(res, 200, renderCustomerSchedulers(schedulers, connectors, projects, session, getFlash(requestUrl)));
          return;
        }

        const schedulerDeleteMatch = /^\/portal\/customer\/schedulers\/([0-9a-f-]{36})\/delete$/.exec(path);
        if (method === "POST" && schedulerDeleteMatch) {
          await repository.deleteScheduler(tenantId, schedulerDeleteMatch[1]);
          redirect(res, flashUrl("/portal/customer/schedulers", "ok", "Scheduler gelöscht"));
          return;
        }

        sendJson(res, 404, { error: "not_found" });
        return;
      }

      // ── Legacy portal snapshot ──────────────────────────────────────────
      if (method === "GET" && path === "/api/saas/v1/overview") {
        const snapshot = await repository.getPortalSnapshot();
        sendJson(res, 200, snapshot);
        return;
      }

      // ── Agent API ───────────────────────────────────────────────────────

      if (method === "POST" && path === "/api/agent/v1/heartbeats") {
        const bearerToken = extractBearerToken(req);
        if (!bearerToken) { sendJson(res, 401, { error: "missing_bearer_token" }); return; }
        const body = await readJsonBody(req);
        if (!isHeartbeat(body)) { sendJson(res, 400, { error: "invalid_heartbeat" }); return; }
        await repository.acceptAgentHeartbeat(body, bearerToken);
        sendJson(res, 202, { accepted: true, serverTime: new Date().toISOString(), commandsAvailable: false, minimumSupportedAgentVersion: "0.2.50" });
        return;
      }

      if (method === "POST" && path === "/api/agent/v1/runs") {
        const bearerToken = extractBearerToken(req);
        if (!bearerToken) { sendJson(res, 401, { error: "missing_bearer_token" }); return; }
        const body = await readJsonBody(req);
        if (!isRunReport(body)) { sendJson(res, 400, { error: "invalid_run_report" }); return; }
        const runId = await repository.recordSchedulerRun(body, bearerToken);
        sendJson(res, 201, { runId, accepted: true });
        return;
      }

      if (method === "POST" && path === "/api/agent/v1/registrations/claim") {
        const body = await readJsonBody(req);
        if (!isRegistrationClaim(body)) { sendJson(res, 400, { error: "invalid_registration_claim" }); return; }
        const registration = await repository.claimAgentRegistration(body);
        sendJson(res, 201, registration);
        return;
      }

      if (method === "GET" && path === "/api/agent/v1/config-bundle") {
        const bearerToken = extractBearerToken(req);
        if (!bearerToken) { sendJson(res, 401, { error: "missing_bearer_token" }); return; }
        const agentId = requestUrl.searchParams.get("agentId") || "";
        if (!agentId) { sendJson(res, 400, { error: "missing_agentId" }); return; }
        const bundle = await repository.getActiveConfigBundle(agentId, bearerToken);
        if (!bundle) { sendJson(res, 404, { error: "no_config_bundle" }); return; }
        sendJson(res, 200, bundle);
        return;
      }

      sendJson(res, 404, { error: "not_found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Internal server error";
      const status = message === "Invalid agent credential" || message === "Invalid registration token" ? 403 : 500;
      sendJson(res, status, { error: status === 403 ? "forbidden" : "internal_error" });
    }
  });
}
