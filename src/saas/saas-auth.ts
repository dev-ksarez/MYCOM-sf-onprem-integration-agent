import crypto from "node:crypto";
import http from "node:http";

export type PortalRole = "system_admin" | "owner" | "admin" | "operator" | "viewer" | "support";

export interface PortalSession {
  userId: string;
  email: string;
  displayName: string;
  isSystemAdmin: boolean;
  tenantId: string | null;
  tenantKey: string | null;
  role: PortalRole | null;
  exp: number;
}

const COOKIE_NAME = "sf_saas_session";
const TOKEN_TTL_SECONDS = 8 * 60 * 60; // 8h

export async function hashPassword(password: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const salt = crypto.randomBytes(16).toString("hex");
    crypto.scrypt(password, salt, 64, (err, derived) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(`${salt}:${derived.toString("hex")}`);
    });
  });
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const [salt, hash] = stored.split(":");
    if (!salt || !hash) {
      resolve(false);
      return;
    }
    crypto.scrypt(password, salt, 64, (err, derived) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(derived.toString("hex") === hash);
    });
  });
}

function b64url(s: string): string {
  return Buffer.from(s).toString("base64url");
}

function fromB64url(s: string): string {
  return Buffer.from(s, "base64url").toString("utf8");
}

export function createSessionToken(session: Omit<PortalSession, "exp">, jwtSecret: string): string {
  const payload: PortalSession = {
    ...session,
    exp: Math.floor(Date.now() / 1000) + TOKEN_TTL_SECONDS
  };
  const encoded = b64url(JSON.stringify(payload));
  const sig = crypto.createHmac("sha256", jwtSecret).update(encoded).digest("hex");
  return `${encoded}.${sig}`;
}

export function verifySessionToken(token: string, jwtSecret: string): PortalSession | null {
  try {
    const lastDot = token.lastIndexOf(".");
    if (lastDot < 0) {
      return null;
    }
    const encoded = token.slice(0, lastDot);
    const sig = token.slice(lastDot + 1);
    const expected = crypto.createHmac("sha256", jwtSecret).update(encoded).digest("hex");
    if (!crypto.timingSafeEqual(Buffer.from(sig, "hex"), Buffer.from(expected, "hex"))) {
      return null;
    }
    const session = JSON.parse(fromB64url(encoded)) as PortalSession;
    if (session.exp < Math.floor(Date.now() / 1000)) {
      return null;
    }
    return session;
  } catch {
    return null;
  }
}

export function getSessionFromRequest(req: http.IncomingMessage, jwtSecret: string): PortalSession | null {
  const cookieHeader = req.headers.cookie || "";
  const match = new RegExp(`(?:^|;\\s*)${COOKIE_NAME}=([^;]+)`).exec(cookieHeader);
  if (!match) {
    return null;
  }
  return verifySessionToken(decodeURIComponent(match[1]), jwtSecret);
}

export function setSessionCookie(res: http.ServerResponse, token: string, secure: boolean): void {
  const flags = [
    `${COOKIE_NAME}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Strict",
    `Max-Age=${TOKEN_TTL_SECONDS}`
  ];
  if (secure) {
    flags.push("Secure");
  }
  res.setHeader("Set-Cookie", flags.join("; "));
}

export function clearSessionCookie(res: http.ServerResponse): void {
  res.setHeader("Set-Cookie", `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`);
}
