import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import sharp from "sharp";
import { triggerDashboardUpdate, getDashboardUpdateStatus } from "../server/dashboard-update-service";
import { buildSystemHealthSnapshot, HealthSnapshot } from "../server/health-snapshot";
import { readConfiguredSalesforceInstances, writeConfiguredSalesforceInstances, type SalesforceInstanceEnvConfig } from "../server/admin-data-service";
import { isSafeGeraeteaktePathSegment, resolveGeraeteakteSerialDirectory } from "./geraeteakte-paths";

const failedAuthAttempts = new Map<string, { count: number; resetAt: number }>();

// ─── Geräteakte: Datei-Browse & Download-Token ───────────────────────────────

const DOWNLOAD_TOKEN_TTL_MS = 2 * 60 * 1000; // 2 Minuten
const DOWNLOAD_TOKEN_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;

interface DownloadTokenEntry {
  seriennummer: string;
  expiresAt: number;
}

const downloadTokenStore = new Map<string, DownloadTokenEntry>();

setInterval(() => {
  const now = Date.now();
  for (const [token, entry] of downloadTokenStore.entries()) {
    if (entry.expiresAt <= now) downloadTokenStore.delete(token);
  }
}, DOWNLOAD_TOKEN_CLEANUP_INTERVAL_MS).unref();

function createDownloadToken(seriennummer: string): string {
  const token = crypto.randomBytes(32).toString("hex");
  downloadTokenStore.set(token, { seriennummer, expiresAt: Date.now() + DOWNLOAD_TOKEN_TTL_MS });
  return token;
}

function isValidDownloadToken(token: string, seriennummer: string): boolean {
  if (!token) return false;
  const entry = downloadTokenStore.get(token);
  if (!entry) return false;
  if (entry.expiresAt <= Date.now()) { downloadTokenStore.delete(token); return false; }
  return entry.seriennummer === seriennummer;
}

function getFileBrowseBasePath(): string {
  return String(process.env.FILE_BROWSE_BASE_PATH ?? "").trim();
}

// HMAC-signierte Download-URL (Weg 2: kein SF→Agent-Callout nötig).
// Salesforce-Apex signiert lokal mit demselben Secret; der Agent verifiziert.
function getDownloadSigningSecret(): string {
  return String(process.env.FILE_DOWNLOAD_SIGNING_SECRET ?? "").trim();
}

function isValidSignedDownload(seriennummer: string, filename: string, exp: string, sig: string): boolean {
  const secret = getDownloadSigningSecret();
  if (!secret || !exp || !sig) return false;

  const expSeconds = Number(exp);
  if (!Number.isFinite(expSeconds) || expSeconds * 1000 < Date.now()) return false; // abgelaufen

  const signingString = `${seriennummer}\n${filename}\n${exp}`;
  const expected = crypto.createHmac("sha256", secret).update(signingString).digest("base64");

  const sigBuffer = Buffer.from(sig);
  const expectedBuffer = Buffer.from(expected);
  return sigBuffer.length === expectedBuffer.length && crypto.timingSafeEqual(sigBuffer, expectedBuffer);
}

function isSafePathSegment(segment: string): boolean {
  return isSafeGeraeteaktePathSegment(segment);
}

interface FileEntry {
  name: string;
  extension: string;
  size: number;
  createdAt: string;
  modifiedAt: string;
}

const JUNK_FILES = new Set([".DS_Store", "Thumbs.db", "desktop.ini"]);

function isIgnoredFileName(name: string): boolean {
  return name.startsWith(".") || JUNK_FILES.has(name);
}

function walkDir(dir: string, relPrefix: string, out: { rel: string; abs: string }[]): void {
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const entry of entries) {
    if (!isSafePathSegment(entry.name)) continue;
    const abs = path.join(dir, entry.name);
    const rel = relPrefix ? `${relPrefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      walkDir(abs, rel, out);
    } else if (entry.isFile() && !isIgnoredFileName(entry.name)) {
      out.push({ rel, abs });
    }
  }
}

function listFilesForSerial(seriennummer: string): { seriennummer: string; files: FileEntry[] } {
  if (!isSafePathSegment(seriennummer)) throw new HttpError(400, "Ungueltige Seriennummer");

  const basePath = getFileBrowseBasePath();
  if (!basePath) throw new HttpError(500, "FILE_BROWSE_BASE_PATH ist nicht konfiguriert");

  const targetDir = resolveSerialDirectoryOrHttpError(basePath, seriennummer).absolutePath;

  if (!fs.existsSync(targetDir)) return { seriennummer, files: [] };

  const found: { rel: string; abs: string }[] = [];
  walkDir(targetDir, "", found);
  const files: FileEntry[] = [];

  for (const { rel, abs } of found) {
    try {
      const stat = fs.statSync(abs);
      files.push({
        name: rel,
        extension: path.extname(rel).replace(".", "").toLowerCase(),
        size: stat.size,
        createdAt: stat.birthtime.toISOString(),
        modifiedAt: stat.mtime.toISOString(),
      });
    } catch { /* nicht lesbare Datei ueberspringen */ }
  }

  files.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  return { seriennummer, files };
}

function getMimeType(ext: string): string {
  const map: Record<string, string> = {
    pdf: "application/pdf",
    doc: "application/msword",
    docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    xls: "application/vnd.ms-excel",
    xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ppt: "application/vnd.ms-powerpoint",
    pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png",
    gif: "image/gif", svg: "image/svg+xml", webp: "image/webp", bmp: "image/bmp",
    mp4: "video/mp4", avi: "video/x-msvideo", mov: "video/quicktime",
    wmv: "video/x-ms-wmv", mkv: "video/x-matroska",
    mp3: "audio/mpeg", wav: "audio/wav", ogg: "audio/ogg", flac: "audio/flac",
    zip: "application/zip", rar: "application/vnd.rar",
    "7z": "application/x-7z-compressed", tar: "application/x-tar", gz: "application/gzip",
    txt: "text/plain", csv: "text/csv", xml: "application/xml",
    html: "text/html", htm: "text/html", json: "application/json",
    msg: "application/vnd.ms-outlook", eml: "message/rfc822",
  };
  return map[ext] ?? "application/octet-stream";
}

// Validiert Seriennummer + relativen Pfad und liefert den absoluten Dateipfad.
function resolveSafeFilePath(seriennummer: string, relativePath: string): { filePath: string; baseName: string } {
  const segments = relativePath.split("/").filter(Boolean);
  if (!isSafePathSegment(seriennummer) || segments.length === 0 || !segments.every(isSafePathSegment)) {
    throw new HttpError(400, "Ungueltige Seriennummer oder Dateiname");
  }

  const basePath = getFileBrowseBasePath();
  if (!basePath) throw new HttpError(500, "FILE_BROWSE_BASE_PATH ist nicht konfiguriert");

  const targetDir = resolveSerialDirectoryOrHttpError(basePath, seriennummer).absolutePath;

  const filePath = path.resolve(path.join(targetDir, ...segments));
  if (!filePath.startsWith(targetDir + path.sep)) {
    throw new HttpError(400, "Ungueltiger Pfad (Pfad-Traversal)");
  }

  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    throw new HttpError(404, "Datei nicht gefunden");
  }

  return { filePath, baseName: segments[segments.length - 1] };
}

function resolveSerialDirectoryOrHttpError(basePath: string, seriennummer: string): { absolutePath: string } {
  try {
    return resolveGeraeteakteSerialDirectory(basePath, seriennummer);
  } catch (error) {
    throw new HttpError(400, error instanceof Error ? error.message : "Ungueltige Seriennummer");
  }
}

function streamFile(res: http.ServerResponse, seriennummer: string, relativePath: string, inline = false): Promise<void> {
  return new Promise((resolve, reject) => {
    let resolved: { filePath: string; baseName: string };
    try {
      resolved = resolveSafeFilePath(seriennummer, relativePath);
    } catch (error) {
      return reject(error);
    }

    const stat = fs.statSync(resolved.filePath);
    const ext = path.extname(resolved.baseName).replace(".", "").toLowerCase();
    const encodedFilename = encodeURIComponent(resolved.baseName);
    // inline → Browser zeigt die Datei an (Vorschau); sonst Download erzwingen
    const disposition = inline ? "inline" : "attachment";

    res.writeHead(200, {
      "Content-Type": getMimeType(ext),
      "Content-Length": stat.size,
      "Content-Disposition": `${disposition}; filename*=UTF-8''${encodedFilename}`,
      "Cache-Control": inline ? "private, max-age=300" : "no-store",
    });

    const stream = fs.createReadStream(resolved.filePath);
    stream.on("error", reject);
    stream.on("end", resolve);
    stream.pipe(res);
  });
}

// ─── Thumbnails (server-seitig verkleinert) ──────────────────────────────────

const THUMBNAIL_IMAGE_EXTENSIONS = new Set(["jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif"]);

function isThumbnailableExtension(ext: string): boolean {
  return THUMBNAIL_IMAGE_EXTENSIONS.has(ext.toLowerCase());
}

interface ThumbnailCacheEntry { buffer: Buffer; key: string; }
const THUMBNAIL_CACHE_MAX = 500;
const thumbnailCache = new Map<string, ThumbnailCacheEntry>();

function getThumbnailWidth(raw: string | null): number {
  const value = Number(raw ?? "");
  if (!Number.isFinite(value) || value <= 0) return 120;
  return Math.min(Math.max(Math.floor(value), 32), 400); // 32..400 px
}

async function streamThumbnail(
  res: http.ServerResponse,
  seriennummer: string,
  relativePath: string,
  width: number
): Promise<void> {
  const { filePath, baseName } = resolveSafeFilePath(seriennummer, relativePath);
  const ext = path.extname(baseName).replace(".", "").toLowerCase();
  if (!isThumbnailableExtension(ext)) {
    throw new HttpError(415, "Kein unterstuetztes Bildformat fuer Thumbnails");
  }

  const stat = fs.statSync(filePath);
  const cacheKey = `${filePath}:${stat.mtimeMs}:${stat.size}:${width}`;
  let buffer = thumbnailCache.get(cacheKey)?.buffer;

  if (!buffer) {
    buffer = await sharp(filePath)
      .rotate() // EXIF-Orientierung beruecksichtigen
      .resize(width, width, { fit: "inside", withoutEnlargement: true })
      .jpeg({ quality: 70 })
      .toBuffer();

    if (thumbnailCache.size >= THUMBNAIL_CACHE_MAX) {
      const oldestKey = thumbnailCache.keys().next().value;
      if (oldestKey) thumbnailCache.delete(oldestKey);
    }
    thumbnailCache.set(cacheKey, { buffer, key: cacheKey });
  }

  res.writeHead(200, {
    "Content-Type": "image/jpeg",
    "Content-Length": buffer.length,
    "Cache-Control": "private, max-age=3600",
  });
  res.end(buffer);
}

function getAgentApiToken(): string {
  return String(process.env.AGENT_API_TOKEN || "").trim();
}

class HttpError extends Error {
  public constructor(public readonly statusCode: number, message: string) {
    super(message);
  }
}

function getAgentJsonBodyLimitBytes(): number {
  const configured = Number(process.env.AGENT_API_JSON_BODY_LIMIT_BYTES || "");
  return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 512 * 1024;
}

function constantTimeEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(String(left || ""), "utf8");
  const rightBuffer = Buffer.from(String(right || ""), "utf8");
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }
  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function getClientKey(req: http.IncomingMessage): string {
  const forwardedFor = Array.isArray(req.headers["x-forwarded-for"]) ? req.headers["x-forwarded-for"][0] : req.headers["x-forwarded-for"];
  return String(forwardedFor || req.socket.remoteAddress || "unknown").split(",")[0].trim() || "unknown";
}

function getAuthRateLimitWindowMs(): number {
  const configured = Number(process.env.AGENT_API_AUTH_RATE_LIMIT_WINDOW_MS || "");
  return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 60_000;
}

function getAuthRateLimitMaxFailures(): number {
  const configured = Number(process.env.AGENT_API_AUTH_RATE_LIMIT_MAX_FAILURES || "");
  return Number.isFinite(configured) && configured > 0 ? Math.floor(configured) : 20;
}

function isAuthRateLimited(req: http.IncomingMessage): boolean {
  const entry = failedAuthAttempts.get(getClientKey(req));
  if (!entry) {
    return false;
  }
  if (entry.resetAt <= Date.now()) {
    failedAuthAttempts.delete(getClientKey(req));
    return false;
  }
  return entry.count >= getAuthRateLimitMaxFailures();
}

function recordAuthFailure(req: http.IncomingMessage): void {
  const key = getClientKey(req);
  const now = Date.now();
  const existing = failedAuthAttempts.get(key);
  if (!existing || existing.resetAt <= now) {
    failedAuthAttempts.set(key, { count: 1, resetAt: now + getAuthRateLimitWindowMs() });
    return;
  }
  failedAuthAttempts.set(key, { count: existing.count + 1, resetAt: existing.resetAt });
}

function clearAuthFailures(req: http.IncomingMessage): void {
  failedAuthAttempts.delete(getClientKey(req));
}

function isAuthorized(req: http.IncomingMessage): boolean {
  const configuredToken = getAgentApiToken();
  if (!configuredToken) {
    return false;
  }

  const authorization = Array.isArray(req.headers.authorization) ? req.headers.authorization[0] : req.headers.authorization;
  const token = String(authorization || "").replace(/^Bearer\s+/i, "").trim();
  return constantTimeEquals(token, configuredToken);
}

export function isAgentApiEnabled(): boolean {
  const enabled = String(process.env.AGENT_API_ENABLED || "1").trim().toLowerCase();
  return enabled !== "0" && enabled !== "false" && enabled !== "off";
}

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const contentType = String(req.headers["content-type"] || "").toLowerCase();
  if (contentType && !contentType.includes("application/json")) {
    throw new HttpError(415, "Content-Type muss application/json sein.");
  }

  const limitBytes = getAgentJsonBodyLimitBytes();
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    totalBytes += buffer.length;
    if (totalBytes > limitBytes) {
      throw new HttpError(413, "Request Body ist zu gross.");
    }
    chunks.push(buffer);
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  try {
    return raw ? (JSON.parse(raw) as unknown) : {};
  } catch {
    throw new HttpError(400, "JSON Body ist ungueltig.");
  }
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

export function createAgentRequestListener(getHealthSnapshot: () => HealthSnapshot): http.RequestListener {
  return (req, res) => {
    void (async () => {
      const requestUrl = new URL(req.url || "/", "http://localhost");
      const sendJson = (statusCode: number, payload: unknown): void => {
        res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify(payload));
      };

      // CORS + Private Network Access (PNA): erlaubt der Salesforce-HTTPS-Seite den
      // Zugriff auf den Loopback-Agent (z. B. für <img>-Thumbnails). Chrome schickt
      // dafür einen Preflight (OPTIONS) mit Access-Control-Request-Private-Network.
      const requestOrigin = Array.isArray(req.headers.origin) ? req.headers.origin[0] : req.headers.origin;
      res.setHeader("Access-Control-Allow-Origin", requestOrigin || "*");
      res.setHeader("Access-Control-Allow-Private-Network", "true");
      res.setHeader("Vary", "Origin");

      if (req.method === "OPTIONS") {
        res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
        res.setHeader("Access-Control-Max-Age", "86400");
        res.writeHead(204);
        res.end();
        return;
      }

      if (isAuthRateLimited(req)) {
        sendJson(429, { error: "Zu viele ungueltige Agent-API Token-Versuche" });
        return;
      }

      // Datei-Download erlaubt Bearer ODER kurzlebigen Download-Token
      const urlParts = requestUrl.pathname.split("/").filter(Boolean);
      const isFileDownloadRoute = (
        req.method === "GET" &&
        urlParts.length >= 5 &&
        urlParts[0] === "api" && urlParts[1] === "agent" && urlParts[2] === "files"
      );
      const downloadSerial = isFileDownloadRoute ? decodeURIComponent(urlParts[3]) : "";
      const downloadRelPath = isFileDownloadRoute
        ? urlParts.slice(4).map((part) => decodeURIComponent(part)).join("/")
        : "";
      const downloadTokenParam = requestUrl.searchParams.get("downloadToken") ?? "";
      const bearerOk = isAuthorized(req);
      const downloadTokenOk = isFileDownloadRoute &&
        isValidDownloadToken(downloadTokenParam, downloadSerial);
      const signedDownloadOk = isFileDownloadRoute &&
        isValidSignedDownload(
          downloadSerial,
          downloadRelPath,
          requestUrl.searchParams.get("exp") ?? "",
          requestUrl.searchParams.get("sig") ?? ""
        );

      if (!bearerOk && !downloadTokenOk && !signedDownloadOk) {
        recordAuthFailure(req);
        sendJson(401, { error: "Agent-API Token fehlt oder ist ungueltig" });
        return;
      }
      clearAuthFailures(req);

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

      // ─── Geräteakte Endpunkte ──────────────────────────────────────────────

      // GET /api/agent/files/:seriennummer — Dateiliste
      const filesListMatch = requestUrl.pathname.match(/^\/api\/agent\/files\/([^/]+)$/);
      if (req.method === "GET" && filesListMatch) {
        const seriennummer = decodeURIComponent(filesListMatch[1]);
        sendJson(200, listFilesForSerial(seriennummer));
        return;
      }

      // POST /api/agent/files/:seriennummer/download-token — Kurzzeit-Token
      const downloadTokenMatch = requestUrl.pathname.match(/^\/api\/agent\/files\/([^/]+)\/download-token$/);
      if (req.method === "POST" && downloadTokenMatch) {
        const seriennummer = decodeURIComponent(downloadTokenMatch[1]);
        if (!isSafePathSegment(seriennummer)) throw new HttpError(400, "Ungueltige Seriennummer");
        const token = createDownloadToken(seriennummer);
        sendJson(200, { token, expiresAt: new Date(Date.now() + DOWNLOAD_TOKEN_TTL_MS).toISOString() });
        return;
      }

      // GET /api/agent/files/:seriennummer/:relativePath — Datei-Download (Unterordner erlaubt)
      // Optional ?thumb=1[&w=120] liefert ein verkleinertes JPEG-Thumbnail (nur Bilder).
      const fileDownloadMatch = requestUrl.pathname.match(/^\/api\/agent\/files\/([^/]+)\/(.+)$/);
      if (req.method === "GET" && fileDownloadMatch) {
        const seriennummer = decodeURIComponent(fileDownloadMatch[1]);
        const relativePath = fileDownloadMatch[2].split("/").map((part) => decodeURIComponent(part)).join("/");
        if (requestUrl.searchParams.get("thumb")) {
          await streamThumbnail(res, seriennummer, relativePath, getThumbnailWidth(requestUrl.searchParams.get("w")));
        } else {
          await streamFile(res, seriennummer, relativePath, Boolean(requestUrl.searchParams.get("inline")));
        }
        return;
      }

      sendJson(404, { error: "Not Found" });
    })().catch((error) => {
      const statusCode = error instanceof HttpError ? error.statusCode : 500;
      res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown server error" }));
    });
  };
}

export function createAgentApiServer(getHealthSnapshot: () => HealthSnapshot): http.Server {
  return http.createServer(createAgentRequestListener(getHealthSnapshot));
}
