import { ConnectorConfig, SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { SalesforceConfig } from "../../infrastructure/config/salesforce-config";
import { CanonicalAccount } from "../../types/canonical-account";
import { ConnectorResult } from "../../types/connector-result";
import { JobContext } from "../../types/job-context";
import { MappedRecord } from "../../types/mapped-record";
import { TargetConnector } from "../../types/target-connector";

function stringParam(parameters: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    const value = parameters[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function secretValue(config: ConnectorConfig): string | undefined {
  const inlineSecret = stringParam(config.parameters, "clientSecret", "password", "secret");
  if (inlineSecret) {
    return inlineSecret;
  }

  const secretKey = String(config.secretKey || "").trim();
  if (!secretKey) {
    return undefined;
  }

  const value = process.env[secretKey];
  if (!value) {
    throw new Error(`Environment variable for secret key ${secretKey} is not set for connector ${config.name}`);
  }
  return value;
}

function normalizeAuthType(value?: string): SalesforceConfig["authType"] {
  const normalized = String(value || "client_credentials").trim().toLowerCase();
  if (normalized === "password" || normalized === "oauth_refresh_token" || normalized === "client_credentials") {
    return normalized;
  }
  throw new Error(`Unsupported Salesforce connector authType: ${value}`);
}

function normalizeEnvironment(value: unknown): "sandbox" | "production" | undefined {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "sandbox" || normalized === "test") {
    return "sandbox";
  }
  if (normalized === "production" || normalized === "prod") {
    return "production";
  }
  return undefined;
}

function resolveLoginUrl(parameters: Record<string, unknown>): string {
  const configuredLoginUrl = stringParam(parameters, "loginUrl", "sfLoginUrl", "url");
  if (configuredLoginUrl) {
    return configuredLoginUrl;
  }
  return normalizeEnvironment(parameters.environment || parameters.salesforceEnvironment || parameters.orgEnvironment) === "sandbox"
    ? "https://test.salesforce.com"
    : "https://login.salesforce.com";
}

function buildSalesforceConfig(config: ConnectorConfig): SalesforceConfig {
  const parameters = config.parameters || {};
  const loginUrl = resolveLoginUrl(parameters);
  const authType = normalizeAuthType(stringParam(parameters, "authType"));
  const secret = secretValue(config);
  const queryLimit = Number(stringParam(parameters, "queryLimit") || parameters.queryLimit || 100);

  if (authType === "password") {
    return {
      loginUrl,
      authType,
      username: stringParam(parameters, "username", "user"),
      password: stringParam(parameters, "password") || secret,
      securityToken: stringParam(parameters, "securityToken"),
      queryLimit: Number.isFinite(queryLimit) && queryLimit > 0 ? queryLimit : 100
    };
  }

  if (authType === "oauth_refresh_token") {
    return {
      loginUrl,
      authType,
      clientId: stringParam(parameters, "clientId", "consumerKey"),
      clientSecret: stringParam(parameters, "clientSecret") || secret,
      refreshToken: stringParam(parameters, "refreshToken"),
      accessToken: stringParam(parameters, "accessToken"),
      instanceUrl: stringParam(parameters, "instanceUrl"),
      queryLimit: Number.isFinite(queryLimit) && queryLimit > 0 ? queryLimit : 100
    };
  }

  return {
    loginUrl,
    authType,
    clientId: stringParam(parameters, "clientId", "consumerKey"),
    clientSecret: stringParam(parameters, "clientSecret") || secret,
    queryLimit: Number.isFinite(queryLimit) && queryLimit > 0 ? queryLimit : 100
  };
}

export class SalesforceConnector implements TargetConnector {
  private readonly config: ConnectorConfig;
  private readonly salesforceConfig: SalesforceConfig;
  private client?: SalesforceClient;

  public constructor(config: ConnectorConfig) {
    this.config = config;
    this.salesforceConfig = buildSalesforceConfig(config);
  }

  public systemName(): string {
    return "salesforce";
  }

  public async getClient(): Promise<SalesforceClient> {
    if (!this.client) {
      this.client = new SalesforceClient(this.salesforceConfig);
      await this.client.login();
    }
    return this.client;
  }

  public async testConnection(): Promise<boolean> {
    const client = await this.getClient();
    const rows = await client.queryGeneric("SELECT Id FROM User LIMIT 1");
    return Array.isArray(rows);
  }

  public async upsertAccounts(_records: CanonicalAccount[], _context: JobContext): Promise<ConnectorResult[]> {
    throw new Error("SalesforceConnector is used as a Salesforce org connection; use SalesforceTargetAdapter for writes.");
  }

  public async upsertMappedRecords(_records: MappedRecord[], _context: JobContext): Promise<ConnectorResult[]> {
    throw new Error("SalesforceConnector is used as a Salesforce org connection; use SalesforceTargetAdapter for writes.");
  }
}
