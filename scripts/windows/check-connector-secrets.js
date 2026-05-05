#!/usr/bin/env node

const path = require("path");
const dotenv = require("dotenv");

async function main() {
  const appRoot = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(__dirname, "..", "..");
  dotenv.config({ path: path.join(appRoot, ".env") });

  const requiredSalesforceVars = ["SF_LOGIN_URL", "SF_CLIENT_ID", "SF_CLIENT_SECRET"];
  const missingSalesforceVars = requiredSalesforceVars.filter((key) => !String(process.env[key] || "").trim());
  if (missingSalesforceVars.length > 0) {
    console.log(
      `SKIPPED Missing Salesforce configuration for connector preflight: ${missingSalesforceVars.join(", ")}`
    );
    return;
  }

  const { getSalesforceConfig } = require(path.join(appRoot, "dist", "infrastructure", "config", "salesforce-config.js"));
  const { SalesforceClient } = require(path.join(appRoot, "dist", "clients", "salesforce", "salesforce-client.js"));

  const client = new SalesforceClient(getSalesforceConfig());
  await client.login();

  const schedules = await client.querySchedules(true);
  const activeConnectorIds = Array.from(
    new Set(
      schedules
        .map((schedule) => String(schedule.MSD_Connector__c || "").trim())
        .filter(Boolean)
    )
  );

  const missingSecrets = [];
  for (const connectorId of activeConnectorIds) {
    const connector = await client.queryConnector(connectorId);
    const secretKey = String(connector.secretKey || "").trim();
    if (!secretKey) {
      continue;
    }

    if (!String(process.env[secretKey] || "").trim()) {
      missingSecrets.push({
        connectorId,
        connectorName: connector.name,
        secretKey,
      });
    }
  }

  if (missingSecrets.length > 0) {
    console.error("MISSING_CONNECTOR_SECRETS");
    for (const item of missingSecrets) {
      console.error(`${item.connectorName} [${item.connectorId}] -> ${item.secretKey}`);
    }
    process.exit(2);
  }

  console.log(`OK Checked ${activeConnectorIds.length} active connector(s); all required secret keys are present.`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});