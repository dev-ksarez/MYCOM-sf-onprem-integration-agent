export interface SaasServerConfig {
  port: number;
  databaseUrl: string;
  agentTokenPepper: string;
  portalJwtSecret: string;
  adminEmail: string;
  adminPassword: string;
}

function required(name: string): string {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
  return value;
}

function positiveNumber(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

export function getSaasServerConfig(): SaasServerConfig {
  return {
    port: positiveNumber(process.env.SAAS_PORT, 9010),
    databaseUrl: required("DATABASE_URL"),
    agentTokenPepper: required("SAAS_AGENT_TOKEN_PEPPER"),
    portalJwtSecret: required("PORTAL_JWT_SECRET"),
    adminEmail: String(process.env.SAAS_ADMIN_EMAIL || "admin@mycom.de").trim(),
    adminPassword: String(process.env.SAAS_ADMIN_PASSWORD || "").trim()
  };
}
