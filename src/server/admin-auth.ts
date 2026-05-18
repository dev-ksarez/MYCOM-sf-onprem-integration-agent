import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

const ADMIN_SESSION_COOKIE_NAME = "sf_agent_session";
const ADMIN_CSRF_COOKIE_NAME = "sf_agent_csrf";
const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const STATE_TTL_MS = 10 * 60 * 1000;

interface OAuthStateEntry {
  redirectUri: string;
  expiresAt: number;
  codeVerifier?: string;
}

const adminSessions = new Map<string, AdminSession>();
const migrationOauthStates = new Map<string, { migrationId: string; redirectUri: string; expiresAt: number }>();
const salesforceLoginStates = new Map<string, OAuthStateEntry>();

export type AdminPermission = "read" | "write" | "delete" | "admin";
export type AdminAuthMode = "local" | "salesforce_oidc";
export type AdminModule = "migration" | "projects" | "deployment";

export interface AdminUserRecord {
  id: string;
  username: string;
  password?: string;
  displayName?: string;
  roles: string[];
  permissions: AdminPermission[];
  modules: AdminModule[];
}

export interface AdminSession {
  token: string;
  userId: string;
  username: string;
  displayName?: string;
  roles: string[];
  permissions: AdminPermission[];
  modules: AdminModule[];
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

export type AdminUserMutationInput = {
  id?: string;
  username?: string;
  password?: string;
  displayName?: string;
  roles?: string[];
  permissions?: AdminPermission[];
  modules?: AdminModule[];
};

export type ProjectMembershipRole = "viewer" | "operator" | "release-manager" | "owner";

export interface ProjectMembershipRecord {
  projectId: string;
  userId: string;
  roleInProject: ProjectMembershipRole;
  assignedAt: string;
  assignedBy?: string;
}

export interface ProjectMembershipDetail extends ProjectMembershipRecord {
  username: string;
  displayName?: string;
  userRoles: string[];
  userPermissions: AdminPermission[];
}

const DEFAULT_ADMIN_USERS_FILE = path.resolve(process.cwd(), "artifacts/admin-users.json");
const DEFAULT_PROJECT_MEMBERSHIPS_FILE = path.resolve(process.cwd(), "artifacts/project-memberships.json");

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

function normalizeModules(value: unknown, roles: string[]): AdminModule[] {
  const roleSet = new Set(roles.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean));
  const modules = new Set<AdminModule>();
  const addModule = (item: unknown): void => {
    const normalized = String(item || "").trim().toLowerCase();
    if (normalized === "migration" || normalized === "projects" || normalized === "deployment") {
      modules.add(normalized);
    }
  };

  if (Array.isArray(value)) {
    value.forEach(addModule);
  }
  if (roleSet.has("admin")) {
    modules.add("migration");
    modules.add("projects");
    modules.add("deployment");
  }
  return Array.from(modules);
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
      const modules = normalizeModules(candidate.modules, roles);
      normalized.push({
        id: String(candidate.id || `user-${index + 1}`),
        username,
        password: candidate.password ? String(candidate.password) : undefined,
        displayName: candidate.displayName ? String(candidate.displayName) : undefined,
        roles,
        permissions: permissions.length ? permissions : ["read"],
        modules
      } satisfies AdminUserRecord);
    });
  return normalized;
}

function getAdminUsersFilePath(): string {
  return path.resolve(String(process.env.ADMIN_UI_USERS_FILE || DEFAULT_ADMIN_USERS_FILE).trim() || DEFAULT_ADMIN_USERS_FILE);
}

function readUsersFile(filePath = getAdminUsersFilePath()): AdminUserRecord[] {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  try {
    return normalizeUsers(JSON.parse(fs.readFileSync(filePath, "utf8")));
  } catch {
    return [];
  }
}

function writeUsersFile(users: AdminUserRecord[], filePath = getAdminUsersFilePath()): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(users, null, 2), "utf8");
}

function createBootstrapAdminUser(): AdminUserRecord {
  return {
    id: "local-admin",
    username: "admin",
    password: "admin123!",
    displayName: "Lokaler Admin",
    roles: ["admin"],
    permissions: ["admin", "read", "write", "delete"],
    modules: ["migration", "projects", "deployment"]
  };
}

function loadUsersFromConfig(): AdminUserRecord[] {
  const usersFromDefaultFile = readUsersFile();
  if (usersFromDefaultFile.length) {
    return usersFromDefaultFile;
  }

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
    const bootstrapUser = createBootstrapAdminUser();
    writeUsersFile([bootstrapUser]);
    return [bootstrapUser];
  }

  return [{
    id: "local-admin",
    username,
    password,
    displayName: username,
    roles: ["admin"],
    permissions: ["admin", "read", "write", "delete"],
    modules: ["migration", "projects", "deployment"]
  }];
}

function normalizeProjectRole(value: unknown): ProjectMembershipRole {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "owner") {
    return "owner";
  }
  if (normalized === "release-manager") {
    return "release-manager";
  }
  if (normalized === "operator") {
    return "operator";
  }
  return "viewer";
}

function getProjectMembershipsFilePath(): string {
  const configured = String(process.env.ADMIN_UI_PROJECT_MEMBERSHIPS_FILE || "").trim();
  return path.resolve(configured || DEFAULT_PROJECT_MEMBERSHIPS_FILE);
}

function normalizeProjectMemberships(input: unknown): ProjectMembershipRecord[] {
  if (!Array.isArray(input)) {
    return [];
  }

  const seen = new Set<string>();
  const items: ProjectMembershipRecord[] = [];

  input.forEach((rawItem) => {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      return;
    }
    const item = rawItem as Record<string, unknown>;
    const projectId = String(item.projectId || "").trim();
    const userId = String(item.userId || "").trim();
    if (!projectId || !userId) {
      return;
    }

    const dedupeKey = `${projectId}::${userId}`;
    if (seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);

    items.push({
      projectId,
      userId,
      roleInProject: normalizeProjectRole(item.roleInProject),
      assignedAt: String(item.assignedAt || "").trim() || new Date().toISOString(),
      assignedBy: String(item.assignedBy || "").trim() || undefined
    });
  });

  return items;
}

function readProjectMembershipsFile(filePath = getProjectMembershipsFilePath()): ProjectMembershipRecord[] {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  try {
    return normalizeProjectMemberships(JSON.parse(fs.readFileSync(filePath, "utf8")));
  } catch {
    return [];
  }
}

function writeProjectMembershipsFile(items: ProjectMembershipRecord[], filePath = getProjectMembershipsFilePath()): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(items, null, 2), "utf8");
}

function getSalesforceOidcConfig(): SalesforceOidcConfig | undefined {
  const loginUrl = String(process.env.SF_IDP_LOGIN_URL || "").trim();
  const clientId = String(process.env.SF_IDP_CLIENT_ID || "").trim();
  const clientSecret = String(process.env.SF_IDP_CLIENT_SECRET || "").trim();
  const scopes = String(process.env.SF_IDP_SCOPES || "openid").trim();
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

function base64UrlEncode(buffer: Buffer): string {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function createPkceVerifier(): string {
  return base64UrlEncode(crypto.randomBytes(32));
}

function createPkceChallenge(verifier: string): string {
  return base64UrlEncode(crypto.createHash("sha256").update(verifier).digest());
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
  if (origin.hostname === "127.0.0.1" || origin.hostname === "::1") {
    origin.hostname = "localhost";
  }

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

export function hasModuleAccess(session: AdminSession | null | undefined, module: AdminModule): boolean {
  if (!session) {
    return false;
  }

  return session.permissions.includes("admin") || (session.modules || []).includes(module);
}

export function listAdminUsers(): AdminUserRecord[] {
  return getAdminAuthConfig().users.map((user) => ({
    ...user,
    password: undefined
  }));
}

export function isProjectMembershipRestricted(): boolean {
  return readProjectMembershipsFile().length > 0;
}

export function listProjectIdsForUser(userId: string): string[] {
  const normalizedUserId = String(userId || "").trim();
  if (!normalizedUserId) {
    return [];
  }

  return readProjectMembershipsFile()
    .filter((item) => item.userId === normalizedUserId)
    .map((item) => item.projectId);
}

export function hasProjectAccess(
  session: AdminSession | null | undefined,
  projectId: string,
  access: "read" | "write" = "read"
): boolean {
  if (!session) {
    return false;
  }

  if (hasPermission(session, "admin")) {
    return true;
  }

  const normalizedProjectId = String(projectId || "").trim();
  if (!normalizedProjectId) {
    return false;
  }

  const memberships = readProjectMembershipsFile();
  if (!memberships.length) {
    return true;
  }

  const membership = memberships.find((item) => item.projectId === normalizedProjectId && item.userId === session.userId);
  if (!membership) {
    return false;
  }

  if (access === "read") {
    return true;
  }

  return membership.roleInProject === "operator"
    || membership.roleInProject === "release-manager"
    || membership.roleInProject === "owner";
}

export function listProjectMembers(projectId: string): ProjectMembershipDetail[] {
  const normalizedProjectId = String(projectId || "").trim();
  if (!normalizedProjectId) {
    throw new Error("projectId ist erforderlich");
  }

  const users = readUsersFile().length ? readUsersFile() : getAdminAuthConfig().users;
  const usersById = new Map(users.map((user) => [user.id, user]));
  return readProjectMembershipsFile()
    .filter((item) => item.projectId === normalizedProjectId)
    .map((item) => {
      const user = usersById.get(item.userId);
      return {
        ...item,
        username: user?.username || item.userId,
        displayName: user?.displayName,
        userRoles: user?.roles || [],
        userPermissions: user?.permissions || []
      };
    })
    .sort((a, b) => a.username.localeCompare(b.username, "de"));
}

export function saveProjectMember(input: {
  projectId: string;
  userId: string;
  roleInProject?: ProjectMembershipRole | string;
  assignedBy?: string;
}): ProjectMembershipRecord {
  const projectId = String(input.projectId || "").trim();
  const userId = String(input.userId || "").trim();
  if (!projectId) {
    throw new Error("projectId ist erforderlich");
  }
  if (!userId) {
    throw new Error("userId ist erforderlich");
  }

  const users = readUsersFile().length ? readUsersFile() : getAdminAuthConfig().users;
  if (!users.some((item) => item.id === userId)) {
    throw new Error(`Benutzer ${userId} nicht gefunden`);
  }

  const now = new Date().toISOString();
  const roleInProject = normalizeProjectRole(input.roleInProject);
  const memberships = readProjectMembershipsFile();
  const existing = memberships.find((item) => item.projectId === projectId && item.userId === userId);
  const saved: ProjectMembershipRecord = existing
    ? {
        ...existing,
        roleInProject,
        assignedBy: String(input.assignedBy || "").trim() || existing.assignedBy,
        assignedAt: now
      }
    : {
        projectId,
        userId,
        roleInProject,
        assignedAt: now,
        assignedBy: String(input.assignedBy || "").trim() || undefined
      };

  const next = existing
    ? memberships.map((item) => (item.projectId === projectId && item.userId === userId ? saved : item))
    : [...memberships, saved];

  writeProjectMembershipsFile(next);
  return saved;
}

export function deleteProjectMember(projectId: string, userId: string): boolean {
  const normalizedProjectId = String(projectId || "").trim();
  const normalizedUserId = String(userId || "").trim();
  if (!normalizedProjectId) {
    throw new Error("projectId ist erforderlich");
  }
  if (!normalizedUserId) {
    throw new Error("userId ist erforderlich");
  }

  const memberships = readProjectMembershipsFile();
  const next = memberships.filter((item) => !(item.projectId === normalizedProjectId && item.userId === normalizedUserId));
  if (next.length === memberships.length) {
    return false;
  }
  writeProjectMembershipsFile(next);
  return true;
}

export function saveAdminUser(input: AdminUserMutationInput): AdminUserRecord {
  const users = readUsersFile().length ? readUsersFile() : getAdminAuthConfig().users;
  const id = String(input.id || `user-${Date.now()}`).trim();
  const username = String(input.username || "").trim();
  if (!username) {
    throw new Error("Benutzername ist erforderlich.");
  }

  const existing = users.find((user) => user.id === id || user.username.trim().toLowerCase() === username.toLowerCase());
  const roles = Array.isArray(input.roles) ? input.roles.map((role) => String(role || "").trim()).filter(Boolean) : existing?.roles || ["viewer"];
  const permissions = normalizePermissions(input.permissions, roles);
  const modules = normalizeModules(input.modules, roles);
  const saved: AdminUserRecord = {
    id: existing?.id || id,
    username,
    password: input.password ? String(input.password) : existing?.password,
    displayName: String(input.displayName || existing?.displayName || username).trim(),
    roles,
    permissions: permissions.length ? permissions : ["read"],
    modules
  };

  if (!saved.password) {
    throw new Error("Passwort ist fuer lokale Benutzer erforderlich.");
  }

  const nextUsers = existing
    ? users.map((user) => user.id === existing.id ? saved : user)
    : [...users, saved];
  writeUsersFile(nextUsers);
  return { ...saved, password: undefined };
}

export function deleteAdminUser(id: string): boolean {
  const normalizedId = String(id || "").trim();
  const users = readUsersFile().length ? readUsersFile() : getAdminAuthConfig().users;
  if (users.length <= 1) {
    throw new Error("Der letzte Admin-Benutzer kann nicht geloescht werden.");
  }
  const target = users.find((user) => user.id === normalizedId);
  if (!target) {
    return false;
  }
  const nextUsers = users.filter((user) => user.id !== normalizedId);
  if (!nextUsers.some((user) => user.permissions.includes("admin"))) {
    throw new Error("Mindestens ein Benutzer mit Admin-Recht ist erforderlich.");
  }
  writeUsersFile(nextUsers);
  return true;
}

export function authenticateLocalAdminUser(username: string, password: string, config = getAdminAuthConfig()): AdminUserRecord | null {
  const normalizedUsername = String(username || "").trim().toLowerCase();
  const candidate = config.users.find((user) => user.username.trim().toLowerCase() === normalizedUsername);
  if (!candidate || !candidate.password) {
    return null;
  }

  return constantTimeEquals(password, candidate.password) ? candidate : null;
}

function createState(store: Map<string, OAuthStateEntry>, redirectUri: string, codeVerifier?: string): string {
  pruneExpiringMap(store);
  const state = crypto.randomBytes(32).toString("hex");
  store.set(state, { redirectUri, codeVerifier, expiresAt: Date.now() + STATE_TTL_MS });
  return state;
}

function consumeState(store: Map<string, OAuthStateEntry>, state: string): { redirectUri: string; codeVerifier?: string } | null {
  pruneExpiringMap(store);
  const entry = store.get(state);
  if (!entry) {
    return null;
  }

  store.delete(state);
  return { redirectUri: entry.redirectUri, codeVerifier: entry.codeVerifier };
}

export function createSalesforceLoginState(redirectUri: string): string {
  return createState(salesforceLoginStates, redirectUri, createPkceVerifier());
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
  const pendingState = salesforceLoginStates.get(state);
  if (pendingState?.codeVerifier) {
    authorizationUrl.searchParams.set("code_challenge", createPkceChallenge(pendingState.codeVerifier));
    authorizationUrl.searchParams.set("code_challenge_method", "S256");
  }
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
  const tokenRequestBody = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    client_id: config.salesforceOidc.clientId,
    client_secret: config.salesforceOidc.clientSecret,
    redirect_uri: pendingState.redirectUri
  });
  if (pendingState.codeVerifier) {
    tokenRequestBody.set("code_verifier", pendingState.codeVerifier);
  }

  const tokenResponse = await fetch(tokenEndpoint, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: tokenRequestBody
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
