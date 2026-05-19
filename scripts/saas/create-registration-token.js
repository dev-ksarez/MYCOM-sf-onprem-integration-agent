#!/usr/bin/env node

const crypto = require("node:crypto");
const { Client } = require("pg");

function readArg(name, fallback = "") {
  const prefix = `--${name}=`;
  const match = process.argv.find((arg) => arg.startsWith(prefix));
  return match ? match.slice(prefix.length).trim() : fallback;
}

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
  return value;
}

function hashToken(token, pepper) {
  return crypto.createHash("sha256").update(`${pepper}:${token}`).digest("hex");
}

async function main() {
  const databaseUrl = requiredEnv("DATABASE_URL");
  const pepper = requiredEnv("SAAS_AGENT_TOKEN_PEPPER");
  const tenantKey = readArg("tenant", "tenant-pilot");
  const projectKey = readArg("project", "project-pilot");
  const tenantName = readArg("tenant-name", tenantKey);
  const projectName = readArg("project-name", projectKey);
  const token = `rt_${crypto.randomBytes(32).toString("base64url")}`;

  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    await client.query("begin");
    const tenant = await client.query(
      `
        insert into tenants (tenant_key, name, status)
        values ($1, $2, 'active')
        on conflict (tenant_key) do update set name = excluded.name, updated_at = now()
        returning id
      `,
      [tenantKey, tenantName]
    );
    const tenantId = tenant.rows[0].id;

    const project = await client.query(
      `
        insert into projects (tenant_id, project_key, name, mode, status)
        values ($1, $2, $3, 'hybrid', 'active')
        on conflict (tenant_id, project_key) do update set name = excluded.name, updated_at = now()
        returning id
      `,
      [tenantId, projectKey, projectName]
    );
    const projectId = project.rows[0].id;

    await client.query(
      `
        insert into licenses (
          tenant_id,
          plan,
          status,
          max_connectors,
          max_schedulers,
          max_records_per_month,
          feature_ai,
          feature_migration,
          feature_custom_connector,
          feature_custom_scheduler
        )
        values ($1, 'pilot', 'trial', 10, 25, 250000, true, true, false, false)
      `,
      [tenantId]
    );

    await client.query(
      `
        insert into registration_tokens (tenant_id, project_id, token_hash, expires_at)
        values ($1, $2, $3, now() + interval '24 hours')
      `,
      [tenantId, projectId, hashToken(token, pepper)]
    );
    await client.query("commit");

    console.log(JSON.stringify({ tenantKey, projectKey, registrationToken: token, expiresInHours: 24 }, null, 2));
  } catch (error) {
    await client.query("rollback").catch(() => undefined);
    throw error;
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
