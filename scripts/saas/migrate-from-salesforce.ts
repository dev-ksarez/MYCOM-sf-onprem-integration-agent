/**
 * Migrates existing Salesforce data (schedulers, connectors, runs, logs) into PostgreSQL.
 *
 * Usage:
 *   npx ts-node scripts/saas/migrate-from-salesforce.ts \
 *     --tenantKey acme \
 *     --projectKey default \
 *     --startDate 2024-01-01 \
 *     [--dryRun]
 *
 * Required env vars:
 *   DATABASE_URL            - PostgreSQL connection URL
 *   SF_LOGIN_URL            - Salesforce login URL
 *   SF_CLIENT_ID            - Salesforce connected app client ID
 *   SF_CLIENT_SECRET        - Salesforce connected app client secret
 */

import "dotenv/config";
import { Pool } from "pg";
import { SalesforceClient } from "../../src/clients/salesforce/salesforce-client";
import { getSalesforceConfig } from "../../src/infrastructure/config/salesforce-config";

const args = process.argv.slice(2);
function getArg(name: string): string | undefined {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 ? args[idx + 1] : undefined;
}

const TENANT_KEY = getArg("tenantKey") || "";
const PROJECT_KEY = getArg("projectKey") || "";
const START_DATE = getArg("startDate") || "2024-01-01";
const END_DATE = getArg("endDate") || new Date().toISOString().slice(0, 10);
const DRY_RUN = args.includes("--dryRun");

if (!TENANT_KEY || !PROJECT_KEY) {
  console.error("Usage: ... --tenantKey <key> --projectKey <key> [--startDate YYYY-MM-DD] [--endDate YYYY-MM-DD] [--dryRun]");
  process.exit(1);
}

const DATABASE_URL = process.env.DATABASE_URL || "";
if (!DATABASE_URL) { console.error("DATABASE_URL is required"); process.exit(1); }

function mapRunStatus(sfStatus?: string): string {
  const s = (sfStatus || "").toLowerCase();
  if (s === "success" || s === "completed") return "success";
  if (s === "partial success" || s === "partial") return "partial";
  if (s === "failed" || s === "error") return "failed";
  if (s === "running" || s === "in progress") return "running";
  return "unknown";
}

function mapLogLevel(sfLevel?: string): "info" | "warn" | "error" {
  const l = (sfLevel || "").toLowerCase();
  if (l === "error") return "error";
  if (l === "warn" || l === "warning") return "warn";
  return "info";
}

async function main(): Promise<void> {
  console.log(`Migrating SF data → PostgreSQL`);
  console.log(`  Tenant: ${TENANT_KEY}  Project: ${PROJECT_KEY}`);
  console.log(`  Range: ${START_DATE} – ${END_DATE}`);
  if (DRY_RUN) console.log("  DRY RUN – no writes");

  const pool = new Pool({ connectionString: DATABASE_URL });

  // Resolve tenant + project IDs
  const tenantResult = await pool.query<{ id: string }>(
    "select id from tenants where tenant_key = $1 and status <> 'deleted' limit 1",
    [TENANT_KEY]
  );
  const tenantRow = tenantResult.rows[0];
  if (!tenantRow) { console.error(`Tenant '${TENANT_KEY}' not found`); await pool.end(); process.exit(1); }
  const tenantId = tenantRow.id;

  const projectResult = await pool.query<{ id: string }>(
    "select id from projects where tenant_id = $1 and project_key = $2 and status <> 'deleted' limit 1",
    [tenantId, PROJECT_KEY]
  );
  const projectRow = projectResult.rows[0];
  if (!projectRow) { console.error(`Project '${PROJECT_KEY}' not found for tenant '${TENANT_KEY}'`); await pool.end(); process.exit(1); }
  const projectId = projectRow.id;

  console.log(`  Tenant ID: ${tenantId}  Project ID: ${projectId}`);

  // Connect to Salesforce
  const sfConfig = getSalesforceConfig();
  const sfClient = new SalesforceClient(sfConfig);
  await sfClient.login();
  console.log("Connected to Salesforce");

  // 1. Fetch and store connectors as JSON metadata
  console.log("\n── Connectors ──");
  const connectors = await sfClient.queryConnectors();
  console.log(`  Found ${connectors.length} connectors`);
  const connectorMap = new Map<string, string>(); // sfId → name
  for (const c of connectors) {
    connectorMap.set(c.id, c.name);
    if (!DRY_RUN) {
      await pool.query(
        `insert into connector_configs (tenant_id, project_id, connector_key, name, connector_type, config_json, status)
         values ($1, $2, $3, $4, $5, $6::jsonb, 'active')
         on conflict (tenant_id, project_id, connector_key) do update set
           name = excluded.name,
           connector_type = excluded.connector_type,
           config_json = excluded.config_json,
           updated_at = now()`,
        [tenantId, projectId, c.name.toLowerCase().replace(/\s+/g, "_"), c.name, c.connectorType || "unknown", JSON.stringify(c)]
      ).catch((err: Error) => {
        // Table might not exist yet – log but continue
        if (!String(err.message).includes("does not exist")) throw err;
        console.warn(`  Skipping connector_configs insert (table not found)`);
      });
    }
    console.log(`  [${DRY_RUN ? "DRY" : "OK"}] Connector: ${c.name} (${c.connectorType || "?"})`);
  }

  // 2. Fetch and store schedulers
  console.log("\n── Schedulers ──");
  const schedules = await sfClient.querySchedules(false);
  console.log(`  Found ${schedules.length} schedules`);
  const schedulerKeyMap = new Map<string, string>(); // sfId → schedulerKey (Name)
  for (const s of schedules) {
    schedulerKeyMap.set(s.Id, s.Name);
    if (!DRY_RUN) {
      await pool.query(
        `insert into scheduler_configs (tenant_id, project_id, scheduler_key, name, direction, active, config_json)
         values ($1, $2, $3, $4, $5, $6, $7::jsonb)
         on conflict (tenant_id, project_id, scheduler_key) do update set
           name = excluded.name,
           direction = excluded.direction,
           active = excluded.active,
           config_json = excluded.config_json,
           updated_at = now()`,
        [
          tenantId, projectId,
          s.Name.toLowerCase().replace(/\s+/g, "_"),
          s.Name,
          (s.MSD_Direction__c || "outbound").toLowerCase(),
          s.Active__c,
          JSON.stringify(s)
        ]
      ).catch((err: Error) => {
        if (!String(err.message).includes("does not exist")) throw err;
        console.warn(`  Skipping scheduler_configs insert (table not found)`);
      });
    }
    console.log(`  [${DRY_RUN ? "DRY" : "OK"}] Scheduler: ${s.Name} (active=${s.Active__c})`);
  }

  // 3. Fetch runs in date range
  console.log("\n── Runs ──");
  const startIso = new Date(START_DATE + "T00:00:00Z").toISOString();
  const endIso = new Date(END_DATE + "T23:59:59Z").toISOString();
  const runs = await sfClient.queryRunsByDateRange(startIso, endIso, 5000);
  console.log(`  Found ${runs.length} runs in range`);

  let runsImported = 0;
  let runsSkipped = 0;
  const pgRunIdBySfId = new Map<string, string>(); // sfId → pg run id

  for (const run of runs) {
    const schedulerKey = run.MSD_Schedule__r?.Name || run.MSD_Schedule__c || "unknown";
    const direction = "outbound";
    const status = mapRunStatus(run.MSD_Status__c);
    const startedAt = run.MSD_StartedAt__c || run.MSD_FinishedAt__c || new Date().toISOString();
    const finishedAt = run.MSD_FinishedAt__c || null;
    const readRecords = Math.floor(run.MSD_RecordsRead__c || 0);
    const writtenRecords = Math.floor(run.MSD_RecordsSucceeded__c || run.MSD_RecordsProcessed__c || 0);
    const failedRecords = Math.floor(run.MSD_RecordsFailed__c || 0);

    if (!DRY_RUN) {
      const existingResult = await pool.query<{ id: string }>(
        "select id from scheduler_runs where tenant_id = $1 and project_id = $2 and sf_source_id = $3 limit 1",
        [tenantId, projectId, run.Id]
      ).catch(() => ({ rows: [] as Array<{ id: string }> }));

      if (existingResult.rows[0]) {
        pgRunIdBySfId.set(run.Id, existingResult.rows[0].id);
        runsSkipped++;
        continue;
      }

      const result = await pool.query<{ id: string }>(
        `insert into scheduler_runs (
           tenant_id, project_id, scheduler_key, direction, status,
           read_records, written_records, skipped_records, failed_records,
           started_at, finished_at, error_message, sf_source_id
         )
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         on conflict do nothing
         returning id`,
        [
          tenantId, projectId,
          schedulerKey.toLowerCase().replace(/\s+/g, "_"),
          direction, status,
          readRecords, writtenRecords, 0, failedRecords,
          startedAt, finishedAt,
          run.MSD_ErrorMessage__c || null,
          run.Id
        ]
      ).catch((err: Error) => {
        // sf_source_id column might not exist – retry without it
        if (String(err.message).includes("sf_source_id")) {
          return pool.query<{ id: string }>(
            `insert into scheduler_runs (
               tenant_id, project_id, scheduler_key, direction, status,
               read_records, written_records, skipped_records, failed_records,
               started_at, finished_at, error_message
             )
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
             on conflict do nothing
             returning id`,
            [tenantId, projectId, schedulerKey.toLowerCase().replace(/\s+/g, "_"), direction, status,
             readRecords, writtenRecords, 0, failedRecords, startedAt, finishedAt, run.MSD_ErrorMessage__c || null]
          );
        }
        throw err;
      });
      if (result.rows[0]) {
        pgRunIdBySfId.set(run.Id, result.rows[0].id);
        runsImported++;
      }
    } else {
      pgRunIdBySfId.set(run.Id, `dry-run-${run.Id}`);
      runsImported++;
    }
  }
  console.log(`  Imported: ${runsImported}  Skipped (already exist): ${runsSkipped}`);

  // 4. Fetch logs for imported runs
  console.log("\n── Logs ──");
  const sfRunIds = Array.from(pgRunIdBySfId.keys());
  let logsImported = 0;
  let logBatch = 0;

  for (let i = 0; i < sfRunIds.length; i += 50) {
    const chunk = sfRunIds.slice(i, i + 50);
    const logsMap = await sfClient.queryLatestLogsByRunIds(chunk);

    for (const [sfRunId, log] of logsMap.entries()) {
      const pgRunId = pgRunIdBySfId.get(sfRunId);
      if (!pgRunId || pgRunId.startsWith("dry-run-")) continue;
      const level = mapLogLevel(log.MSD_Level__c);
      const message = log.MSD_Message__c || log.MSD_Step__c || "(no message)";
      const occurredAt = log.CreatedDate || new Date().toISOString();

      if (!DRY_RUN) {
        await pool.query(
          `insert into log_events (tenant_id, project_id, run_id, occurred_at, level, code, message)
           values ($1,$2,$3,$4,$5,$6,$7)
           on conflict do nothing`,
          [tenantId, projectId, pgRunId, occurredAt, level, log.MSD_Step__c || null, message]
        );
      }
      logsImported++;
    }
    logBatch++;
    if (logBatch % 10 === 0) process.stdout.write(".");
  }
  console.log(`\n  Log events imported: ${logsImported}${DRY_RUN ? " (dry run)" : ""}`);

  await pool.end();
  console.log("\nMigration complete.");
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
