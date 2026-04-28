export interface SalesforceConfig {
  loginUrl: string;
  clientId: string;
  clientSecret: string;
  queryLimit: number;
}

export function getSalesforceConfig(): SalesforceConfig {
  const loginUrl = process.env.SF_LOGIN_URL?.trim();
  const clientId = process.env.SF_CLIENT_ID?.trim();
  const clientSecret = process.env.SF_CLIENT_SECRET?.trim();
  const queryLimit = Number(process.env.SF_QUERY_LIMIT?.trim() || "100");

  if (!loginUrl) {
    throw new Error("Missing environment variable: SF_LOGIN_URL");
  }

  if (!clientId) {
    throw new Error("Missing environment variable: SF_CLIENT_ID");
  }

  if (!clientSecret) {
    throw new Error("Missing environment variable: SF_CLIENT_SECRET");
  }

  return {
    loginUrl,
    clientId,
    clientSecret,
    queryLimit
  };
}