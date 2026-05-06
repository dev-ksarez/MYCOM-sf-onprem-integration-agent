import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";

const ADMIN_SESSION_COOKIE_NAME = "sf_agent_session";
const ADMIN_CSRF_COOKIE_NAME = "sf_agent_csrf";
const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const STATE_TTL_MS = 10 * 60 * 1000;

const adminSessions = new Map<string, AdminSession>();
const migrationOauthStates = new Map<string, { migrationId: string; redirectUri: string; expiresAt: number }>();
const salesforceLoginStates = new Map<string, { redirectUri: string; expiresAt: number }>();

export type AdminPermission = "read" | "write" | "delete" | "admin";
export type AdminAuthMode = "local" | "salesforce_oidc";

export interface AdminUserRecord {
  id: string;
  username: string;
  password?: string;
  displayName?: string;
  roles: string[];
  permissions: AdminPermission[];
}

export interface AdminSession {
  token: string;
  userId: string;
  username: string;
  displayName?: string;
  roles: string[];
  permissions: AdminPermission[];
  authProvider: "local" | "salesforce_oidc";
  expiresAt: number;
}

interface SalesforceOidcConfig {
  loginUrl: string;
  clientId: string;
  clientSecret: string;
  scopes: string;
  enabled: boolean;
}

export interface AdminAuthConfig {
  mode: AdminAuthMode;
  enabled: boolean;
  users: AdminUserRecord[];
  salesforceOidc?: SalesforceOidcConfig;
}

function formatSalesforceOauthError(error?: string, description?: string, loginUrl?: string): string {
  const normalizedError = String(error || "").trim();
  const normalizedDescription = String(description || "").trim();
  const normalizedLoginUrl = String(loginUrl || "").trim();

  if (normalizedError === "invalid_client_id") {
    return [
      "Salesforce Client ID ist ungueltig.",
      normalizedLoginUrl ? `Pruefe, ob die Connected App in ${normalizedLoginUrl} existiert.` : "",
      "Pruefe SF_IDP_CLIENT_ID und ob Production/Sandbox zur Connected App passt."
    ]
      .filter(Boolean)
      .join(" ");
  }

  return normalizedDescription || normalizedError || "Salesforce-Login fehlgeschlagen.";
}

function normalizePermissions(value: unknown, roles: string[]): AdminPermission[] {
  const roleSet = new Set(roles.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean));
  const permissions = new Set<AdminPermission>();
  const addPermission = (permission: string): void => {
    if (permission === "read" || permission === "write" || permission === "delete" || permission === "admin") {
      permissions.add(permission);
    }
  };

  if (Array.isArray(value)) {
    value.forEach((item) => addPermission(String(item || "").trim().toLowerCase()));
  }

  if (roleSet.has("admin")) {
    permissions.add("admin");
    permissions.add("read");
    permissions.add("write");
    permissions.add("delete");
  }
  if (roleSet.has("editor")) {
    permissions.add("read");
    permissions.add("write");
  }
  if (roleSet.has("viewer")) {
    permissions.add("read");
  }
  if (permissions.has("admin")) {
    permissions.add("read");
    permissions.add("write");
    permissions.add("delete");
  }
  if (permissions.has("write")) {
    permissions.add("read");
  }

  return Array.from(permissions);
}

function normalizeUsers(input: unknown): AdminUserRecord[] {
  if (!Array.isArray(input)) {
    return [];
  }

  const normalized: AdminUserRecord[] = [];
  input.forEach((item, index) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        return;
      }

      const candidate = item as Record<string, unknown>;
      const username = String(candidate.username || candidate.email || "").trim();
      if (!username) {
        return;
      }

      const roles = Array.isArray(candidate.roles)
        ? candidate.roles.map((role) => String(role || "").trim()).filter(Boolean)
        : [];
      const permissions = normalizePermissions(candidate.permissions, roles);
      normalized.push({
        id: String(candidate.id || `user-${index + 1}`),
        username,
        password: candidate.password ? String(candidate.password) : undefined,
        displayName: candidate.displayName ? String(candidate.displayName) : undefined,
        roles,
        permissions: permissions.length ? permissions : ["read"]
      } satisfies AdminUserRecord);
    });
  return normalized;
}

function loadUsersFromConfig(): AdminUserRecord[] {
  const usersJson = String(process.env.ADMIN_UI_USERS_JSON || "").trim();
  if (usersJson) {
    try {
      return normalizeUsers(JSON.parse(usersJson));
    } catch {
      return [];
    }
  }

  const usersFile = String(process.env.ADMIN_UI_USERS_FILE || "").trim();
  if (usersFile && fs.existsSync(usersFile)) {
    try {
      return normalizeUsers(JSON.parse(fs.readFileSync(usersFile, "utf8")));
    } catch {
      return [];
    }
  }

  const username = String(process.env.ADMIN_UI_USERNAME || "").trim();
  const password = String(process.env.ADMIN_UI_PASSWORD || "");
  if (!username || !password) {
    return [];
  }

  return [{
    id: "local-admin",
    username,
    password,
    displayName: username,
    roles: ["admin"],
    permissions: ["admin", "read", "write", "delete"]
  }];
}

function getSalesforceOidcConfig(): SalesforceOidcConfig | undefined {
  const loginUrl = String(process.env.SF_IDP_LOGIN_URL || "").trim();
  const clientId = String(process.env.SF_IDP_CLIENT_ID || "").trim();
  const clientSecret = String(process.env.SF_IDP_CLIENT_SECRET || "").trim();
  const scopes = String(process.env.SF_IDP_SCOPES || "openid email profile").trim();
  const enabled = Boolean(loginUrl && clientId && clientSecret);
  return enabled ? { loginUrl, clientId, clientSecret, scopes, enabled } : undefined;
}

export function getAdminAuthConfig(): AdminAuthConfig {
  const users = loadUsersFromConfig();
  const mode = String(process.env.ADMIN_AUTH_MODE || "local").trim().toLowerCase() === "salesforce_oidc"
    ? "salesforce_oidc"
    : "local";
  const salesforceOidc = getSalesforceOidcConfig();

  if (mode === "salesforce_oidc") {
    return {
      mode,
      enabled: Boolean(salesforceOidc && users.length),
      users,
      salesforceOidc
    };
  }

  return {
    mode: "local",
    enabled: Boolean(users.length),
    users,
    salesforceOidc
  };
}

function parseCookies(headerValue: string | undefined): Record<string, string> {
  return String(headerValue || "")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .reduce<Record<string, string>>((cookies, part) => {
      const separatorIndex = part.indexOf("=");
      if (separatorIndex <= 0) {
        return cookies;
      }

      const key = part.slice(0, separatorIndex).trim();
      const value = part.slice(separatorIndex + 1).trim();
      cookies[key] = decodeURIComponent(value);
      return cookies;
    }, {});
}

export function appendSetCookie(existing: string | string[] | undefined, cookie: string): string[] {
  const values = Array.isArray(existing) ? existing.slice() : existing ? [existing] : [];
  values.push(cookie);
  return values;
}

function pruneExpiredAdminSessions(now = Date.now()): void {
  for (const [token, session] of adminSessions.entries()) {
    if (session.expiresAt <= now) {
      adminSessions.delete(token);
    }
  }
}

export function createAdminSession(user: Omit<AdminSession, "token" | "expiresAt">): string {
  pruneExpiredAdminSessions();
  const token = crypto.randomBytes(32).toString("hex");
  adminSessions.set(token, {
    ...user,
    token,
    expiresAt: Date.now() + ADMIN_SESSION_TTL_MS
  });
  return token;
}

export function createCsrfToken(): string {
  return crypto.randomBytes(24).toString("hex");
}

function pruneExpiringMap<T extends { expiresAt: number }>(store: Map<string, T>, now = Date.now()): void {
  for (const [key, entry] of store.entries()) {
    if (entry.expiresAt <= now) {
      store.delete(key);
    }
  }
}

export function createMigrationOauthState(migrationId: string, redirectUri: string): string {
  pruneExpiringMap(migrationOauthStates);
  const state = crypto.randomBytes(32).toString("hex");
  migrationOauthStates.set(state, {
    migrationId,
    redirectUri,
    expiresAt: Date.now() + STATE_TTL_MS
  });
  return state;
}

export function consumeMigrationOauthState(state: string): { migrationId: string; redirectUri: string } | null {
  pruneExpiringMap(migrationOauthStates);
  const entry = migrationOauthStates.get(state);
  if (!entry) {
    return null;
  }

  migrationOauthStates.delete(state);
  return {
    migrationId: entry.migrationId,
    redirectUri: entry.redirectUri
  };
}

function isSecureRequest(req: http.IncomingMessage): boolean {
  const forwardedProto = req.headers["x-forwarded-proto"];
  const value = Array.isArray(forwardedProto) ? forwardedProto[0] : forwardedProto;
  return String(value || "").toLowerCase() === "https";
}

export function buildSessionCookie(req: http.IncomingMessage, token: string): string {
  const securePart = isSecureRequest(req) ? "; Secure" : "";
  return `${ADMIN_SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${Math.floor(ADMIN_SESSION_TTL_MS / 1000)}${securePart}`;
}

export function buildCsrfCookie(req: http.IncomingMessage, token: string): string {
  const securePart = isSecureRequest(req) ? "; Secure" : "";
  return `${ADMIN_CSRF_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; SameSite=Strict; Max-Age=${Math.floor(ADMIN_SESSION_TTL_MS / 1000)}${securePart}`;
}

export function buildExpiredSessionCookie(req: http.IncomingMessage): string {
  const securePart = isSecureRequest(req) ? "; Secure" : "";
  return `${ADMIN_SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${securePart}`;
}

export function getOrCreateCsrfToken(req: http.IncomingMessage): string {
  const token = parseCookies(req.headers.cookie)[ADMIN_CSRF_COOKIE_NAME];
  return token || createCsrfToken();
}

export function constantTimeEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(String(left || ""), "utf8");
  const rightBuffer = Buffer.from(String(right || ""), "utf8");
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

export function isMutatingMethod(method: string | undefined): boolean {
  const normalized = String(method || "GET").toUpperCase();
  return normalized === "POST" || normalized === "PUT" || normalized === "PATCH" || normalized === "DELETE";
}

export function hasAllowedRequestOrigin(req: http.IncomingMessage): boolean {
  const originHeader = Array.isArray(req.headers.origin) ? req.headers.origin[0] : req.headers.origin;
  const refererHeader = Array.isArray(req.headers.referer) ? req.headers.referer[0] : req.headers.referer;
  const source = String(originHeader || refererHeader || "").trim();
  if (!source) {
    return true;
  }

  try {
    const parsed = new URL(source);
    const forwardedHost = Array.isArray(req.headers["x-forwarded-host"]) ? req.headers["x-forwarded-host"][0] : req.headers["x-forwarded-host"];
    const host = String(forwardedHost || req.headers.host || "").split(",")[0].trim().toLowerCase();
    return Boolean(host) && parsed.host.toLowerCase() === host;
  } catch {
    return false;
  }
}

function buildRequestOrigin(req: http.IncomingMessage): string {
  const forwardedProto = Array.isArray(req.headers["x-forwarded-proto"]) ? req.headers["x-forwarded-proto"][0] : req.headers["x-forwarded-proto"];
  const forwardedHost = Array.isArray(req.headers["x-forwarded-host"]) ? req.headers["x-forwarded-host"][0] : req.headers["x-forwarded-host"];
  const protocol = String(forwardedProto || (isSecureRequest(req) ? "https" : "http")).split(",")[0].trim().toLowerCase() || "http";
  const host = String(forwardedHost || req.headers.host || "").split(",")[0].trim();
  if (!host) {
    throw new Error("Host Header fehlt fuer OAuth Redirect");
  }

  return `${protocol}://${host}`;
}

export function buildMigrationOauthRedirectUri(req: http.IncomingMessage): string {
  const configuredRedirectUri = String(process.env.SF_MIGRATION_OAUTH_REDIRECT_URI || "").trim();
  if (configuredRedirectUri) {
    return configuredRedirectUri;
  }

  const origin = new URL(buildRequestOrigin(req));
  if (origin.hostname === "127.0.0.1" || origin.hostname === "::1") {
    origin.hostname = "localhost";
  }

  origin.pathname = "/auth/migration-salesforce/callback";
  origin.search = "";
  origin.hash = "";
  return origin.toString();
}

export function buildAdminSalesforceOidcRedirectUri(req: http.IncomingMessage): string {
  const configuredRedirectUri = String(process.env.SF_IDP_REDIRECT_URI || "").trim();
  if (configuredRedirectUri) {
    return configuredRedirectUri;
  }

  const origin = new URL(buildRequestOrigin(req));
  origin.pathname = "/auth/salesforce/callback";
  origin.search = "";
  origin.hash = "";
  return origin.toString();
}

export function hasValidCsrfToken(req: http.IncomingMessage): boolean {
  const cookieToken = parseCookies(req.headers.cookie)[ADMIN_CSRF_COOKIE_NAME];
  const headerValue = Array.isArray(req.headers["x-csrf-token"]) ? req.headers["x-csrf-token"][0] : req.headers["x-csrf-token"];
  const headerToken = String(headerValue || "").trim();
  if (!cookieToken || !headerToken) {
    return false;
  }

  return constantTimeEquals(cookieToken, headerToken);
}

export function getAdminSession(req: http.IncomingMessage): AdminSession | null {
  pruneExpiredAdminSessions();
  const token = parseCookies(req.headers.cookie)[ADMIN_SESSION_COOKIE_NAME];
  if (!token) {
    return null;
  }

  const session = adminSessions.get(token);
  if (!session || session.expiresAt <= Date.now()) {
    adminSessions.delete(token);
    return null;
  }

  session.expiresAt = Date.now() + ADMIN_SESSION_TTL_MS;
  return session;
}

export function hasValidAdminSession(req: http.IncomingMessage): boolean {
  return Boolean(getAdminSession(req));
}

export function clearAdminSession(req: http.IncomingMessage): void {
  const token = parseCookies(req.headers.cookie)[ADMIN_SESSION_COOKIE_NAME];
  if (token) {
    adminSessions.delete(token);
  }
}

export function hasPermission(session: AdminSession | null | undefined, permission: AdminPermission): boolean {
  if (!session) {
    return false;
  }

  return session.permissions.includes("admin") || session.permissions.includes(permission);
}

export function authenticateLocalAdminUser(username: string, password: string, config = getAdminAuthConfig()): AdminUserRecord | null {
  const normalizedUsername = String(username || "").trim().toLowerCase();
  const candidate = config.users.find((user) => user.username.trim().toLowerCase() === normalizedUsername);
  if (!candidate || !candidate.password) {
    return null;
  }

  return constantTimeEquals(password, candidate.password) ? candidate : null;
}

function createState(store: Map<string, { redirectUri: string; expiresAt: number }>, redirectUri: string): string {
  pruneExpiringMap(store);
  const state = crypto.randomBytes(32).toString("hex");
  store.set(state, { redirectUri, expiresAt: Date.now() + STATE_TTL_MS });
  return state;
}

function consumeState(store: Map<string, { redirectUri: string; expiresAt: number }>, state: string): { redirectUri: string } | null {
  pruneExpiringMap(store);
  const entry = store.get(state);
  if (!entry) {
    return null;
  }

  store.delete(state);
  return { redirectUri: entry.redirectUri };
}

export function createSalesforceLoginState(redirectUri: string): string {
  return createState(salesforceLoginStates, redirectUri);
}

export function buildSalesforceLoginAuthorizationUrl(state: string, redirectUri: string, config = getAdminAuthConfig()): string {
  if (!config.salesforceOidc?.enabled) {
    throw new Error("Salesforce-Identitätsprovider ist nicht konfiguriert.");
  }

  const authorizationUrl = new URL("/services/oauth2/authorize", config.salesforceOidc.loginUrl);
  authorizationUrl.searchParams.set("response_type", "code");
  authorizationUrl.searchParams.set("client_id", config.salesforceOidc.clientId);
  authorizationUrl.searchParams.set("redirect_uri", redirectUri);
  authorizationUrl.searchParams.set("scope", config.salesforceOidc.scopes);
  authorizationUrl.searchParams.set("state", state);
  return authorizationUrl.toString();
}

export async function completeSalesforceLogin(code: string, state: string, config = getAdminAuthConfig()): Promise<AdminUserRecord> {
  if (!config.salesforceOidc?.enabled) {
    throw new Error("Salesforce-Identitätsprovider ist nicht konfiguriert.");
  }

  const pendingState = consumeState(salesforceLoginStates, state);
  if (!pendingState) {
    throw new Error("Der Login-Status ist abgelaufen oder ungueltig.");
  }

  const tokenEndpoint = new URL("/services/oauth2/token", config.salesforceOidc.loginUrl);
  const tokenResponse = await fetch(tokenEndpoint, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code,
      client_id: config.salesforceOidc.clientId,
      client_secret: config.salesforceOidc.clientSecret,
      redirect_uri: pendingState.redirectUri
    })
  });

  const tokenPayload = (await tokenResponse.json()) as {
    access_token?: string;
    id?: string;
    instance_url?: string;
    error?: string;
    error_description?: string;
  };

  if (!tokenResponse.ok || !tokenPayload.access_token) {
    throw new Error(
      formatSalesforceOauthError(
        tokenPayload.error,
        tokenPayload.error_description,
        config.salesforceOidc.loginUrl
      )
    );
  }

  const identityUrl = String(tokenPayload.id || "").trim();
  if (!identityUrl) {
    throw new Error("Salesforce-Identity-URL fehlt in der Token-Antwort.");
  }

  const identityResponse = await fetch(identityUrl, {
    headers: { Authorization: `Bearer ${tokenPayload.access_token}` }
  });
  const identityPayload = (await identityResponse.json()) as {
    username?: string;
    email?: string;
    display_name?: string;
    user_id?: string;
  };

  if (!identityResponse.ok) {
    throw new Error("Salesforce-Identitaet konnte nicht geladen werden.");
  }

  const lookupKeys = [
    String(identityPayload.username || "").trim().toLowerCase(),
    String(identityPayload.email || "").trim().toLowerCase()
  ].filter(Boolean);
  const user = config.users.find((candidate) =>
    lookupKeys.includes(candidate.username.trim().toLowerCase())
  );

  if (!user) {
    throw new Error("Salesforce-Benutzer ist in der lokalen Rollen-/Berechtigungskonfiguration nicht freigegeben.");
  }

  return {
    ...user,
    displayName: user.displayName || String(identityPayload.display_name || identityPayload.username || user.username).trim() || user.username
  };
}
