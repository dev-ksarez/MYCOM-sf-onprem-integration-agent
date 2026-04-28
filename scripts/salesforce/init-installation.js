#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const readline = require("readline/promises");
const { stdin: input, stdout: output } = require("process");
const dotenv = require("dotenv");
const jsforce = require("jsforce");

dotenv.config();

function parseArgs(argv) {
  const args = {
    mode: "",
    activate: false,
    interactive: true,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--mode") {
      args.mode = String(argv[i + 1] || "").trim().toUpperCase();
      i += 1;
      continue;
    }

    if (arg === "--activate") {
      args.activate = true;
      continue;
    }

    if (arg === "--no-interactive") {
      args.interactive = false;
      continue;
    }
  }

  return args;
}

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
  return value;
}

function appRoot() {
  return path.resolve(__dirname, "..", "..");
}

function envFilePath() {
  return path.join(appRoot(), ".env");
}

function parseEnvFile(content) {
  const map = new Map();
  const lines = content.split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const idx = line.indexOf("=");
    if (idx <= 0) {
      continue;
    }

    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1);
    map.set(key, value);
  }

  return map;
}

function quoteEnvValue(value) {
  if (/\s/.test(value) || value.includes("#") || value.includes("\"")) {
    return `"${value.replace(/\"/g, '\\\"')}"`;
  }

  return value;
}

function upsertEnvValues(newValues) {
  const file = envFilePath();
  const current = fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";
  const map = parseEnvFile(current);

  for (const [key, value] of Object.entries(newValues)) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      map.set(key, String(value));
      process.env[key] = String(value);
    }
  }

  const lines = [];
  for (const [key, value] of map.entries()) {
    lines.push(`${key}=${quoteEnvValue(value)}`);
  }

  fs.writeFileSync(file, `${lines.join("\n")}\n`, "utf8");
}

async function askQuestion(rl, label, options = {}) {
  const required = options.required !== false;
  const defaultValue = options.defaultValue ?? "";

  while (true) {
    const suffix = defaultValue ? ` [${defaultValue}]` : "";
    const answer = (await rl.question(`${label}${suffix}: `)).trim();

    if (answer) {
      return answer;
    }

    if (defaultValue) {
      return String(defaultValue);
    }

    if (!required) {
      return "";
    }

    console.log("Value is required.");
  }
}

async function resolveValue({ key, label, rl, interactive, defaultValue, required = true }) {
  const current = String(process.env[key] || "").trim();
  if (current) {
    return current;
  }

  if (!interactive || !rl) {
    if (!required) {
      return "";
    }

    throw new Error(`Missing environment variable: ${key}`);
  }

  return askQuestion(rl, label, { required, defaultValue });
}

function escapeSoql(value) {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function login(loginUrl, clientId, clientSecret) {

  const tokenUrl = `${loginUrl.replace(/\/$/, "")}/services/oauth2/token`;
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: clientId,
    client_secret: clientSecret,
  });

  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  if (!response.ok) {
    const errorText = await response.text();
    const errorMsg = `Salesforce token request failed: ${response.status} ${errorText}`;
    
    // Provide diagnostic hint for common errors
    if (errorText.includes("unsupported_grant_type")) {
      console.error(`\n✗ ${errorMsg}`);
      console.error("\n⚠️  Your Salesforce Connected App is NOT configured for Client Credentials Flow");
      console.error("\nTo diagnose this issue, run:");
      console.error("  npm run sf:debug-oauth");
      console.error("\nFor detailed instructions, see: SALESFORCE_OAUTH_TROUBLESHOOTING.md");
      console.error("");
    }
    
    throw new Error(errorMsg);
  }

  const tokenData = await response.json();
  if (!tokenData.access_token || !tokenData.instance_url) {
    throw new Error("Salesforce token response is missing access_token or instance_url");
  }

  return new jsforce.Connection({
    instanceUrl: tokenData.instance_url,
    accessToken: tokenData.access_token,
  });
}

async function deployMetadata(connection) {
  const metadataDir = path.join(appRoot(), "salesforce", "metadata");
  const packageXml = path.join(metadataDir, "package.xml");
  const objectsDir = path.join(metadataDir, "objects");

  if (!fs.existsSync(packageXml)) {
    console.warn(`⚠️  Metadata package.xml not found at: ${packageXml}`);
    console.warn("   Skipping metadata deployment. Custom objects may not be created.");
    return;
  }

  if (!fs.existsSync(objectsDir)) {
    console.warn(`⚠️  Metadata objects directory not found at: ${objectsDir}`);
    console.warn("   Skipping metadata deployment. Custom objects may not be created.");
    return;
  }

  console.log("📦 Deploying Salesforce metadata...");

  // Build deploy ZIP in memory
  const Archiver = require("archiver");
  let zipBuffer = Buffer.alloc(0);

  const zipData = await new Promise((resolve, reject) => {
    const chunks = [];
    const archive = Archiver("zip", { zlib: { level: 9 } });

    archive.on("data", (chunk) => chunks.push(chunk));
    archive.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    archive.on("error", reject);

    // Add package.xml at root
    archive.file(packageXml, { name: "package.xml" });

    // Add all .object files inside objects/
    const objectFiles = fs.readdirSync(objectsDir).filter((f) => f.endsWith(".object"));
    for (const objFile of objectFiles) {
      archive.file(path.join(objectsDir, objFile), { name: `objects/${objFile}` });
    }

    archive.finalize();
  });

  try {
    const deployOptions = {
      rollbackOnError: true,
      checkOnly: false,
      singlePackage: true,
      runTests: [],
    };

    console.log(`  Deploying ZIP (${(zipData.length / 1024).toFixed(1)} KB)...`);
    const deployResult = await connection.metadata.deploy(
      zipData.toString("base64"),
      deployOptions
    );
    const deployId = deployResult.id;
    console.log(`  Deploy ID: ${deployId}`);

    // Poll for result
    const POLL_INTERVAL = 3000;
    const MAX_WAIT = 5 * 60 * 1000; // 5 minutes
    const startTime = Date.now();

    while (true) {
      const status = await connection.metadata.checkDeployStatus(deployId, true);
      console.log(
        `  Status: ${status.status} | Done: ${status.done} | Errors: ${
          status.numberComponentErrors ?? 0
        }`
      );

      if (status.done) {
        if (status.success) {
          console.log(
            `  ✅ Metadata deployed successfully! (${status.numberComponentsDeployed} components)`
          );
          
          // Wait for Salesforce to make fields available and refresh metadata cache
          console.log("  ⏳ Waiting for metadata to become available in org...");
          await new Promise((r) => setTimeout(r, 3000)); // 3 second delay
          
          // Refresh org metadata by describing a custom object
          try {
            await connection.describe("MSD_Connector__c");
            console.log("  ✓ Metadata cache refreshed");
          } catch (err) {
            console.warn(`  ⚠️  Could not refresh metadata: ${err.message}`);
          }
        } else {
          const failures = status.details?.componentFailures ?? [];
          const failList = Array.isArray(failures) ? failures : [failures];
          console.error("❌ Metadata deployment had errors:");
          for (const f of failList) {
            console.error(`  [${f.componentType}] ${f.fullName}: ${f.problem}`);
          }
          throw new Error(`Metadata deployment failed: ${failList.length} error(s) occurred`);
        }
        break;
      }

      if (Date.now() - startTime > MAX_WAIT) {
        throw new Error("Metadata deployment timed out after 5 minutes");
      }

      await new Promise((r) => setTimeout(r, POLL_INTERVAL));
    }
  } catch (err) {
    console.error(`❌ Metadata deployment failed: ${err.message}`);
    throw err;
  }
}

async function validateCustomObjectFields(connection, objectName, requiredFields) {
  try {
    const describe = await connection.describe(objectName);
    const existingFields = new Map(describe.fields.map((f) => [f.name, f]));

    const missing = requiredFields.filter((fieldName) => !existingFields.has(fieldName));
    
    if (missing.length > 0) {
      console.error(`❌ Custom object '${objectName}' is missing required fields:`);
      for (const field of missing) {
        console.error(`  - ${field}`);
      }
      throw new Error(`Missing ${missing.length} field(s) on ${objectName}. Metadata deployment may have failed.`);
    }

    console.log(`  ✓ All required fields exist on ${objectName}`);
    return true;
  } catch (err) {
    if (err.message?.includes("not found")) {
      throw new Error(`Custom object '${objectName}' does not exist. Metadata deployment failed.`);
    }
    throw err;
  }
}

function buildSage100Templates() {
  const accountExternalId =
    String(process.env.SAGE100_ACCOUNT_EXTERNAL_ID_FIELD || "AccountNumber").trim() || "AccountNumber";
  const contactExternalId =
    String(process.env.SAGE100_CONTACT_EXTERNAL_ID_FIELD || "Email").trim() || "Email";

  const accountTargetDefinition = JSON.stringify(
    {
      selectedImportProfileName: "basis",
      importProfiles: [
        {
          name: "basis",
          active: true,
          schedulerEnabled: true,
          mode: "object",
          objectApiName: "Account",
          operation: "upsert",
          externalIdField: accountExternalId,
          picklists: [],
        },
      ],
    },
    null,
    2
  );

  const contactTargetDefinition = JSON.stringify(
    {
      selectedImportProfileName: "basis",
      importProfiles: [
        {
          name: "basis",
          active: true,
          schedulerEnabled: true,
          mode: "object",
          objectApiName: "Contact",
          operation: "upsert",
          externalIdField: contactExternalId,
          picklists: [],
        },
      ],
    },
    null,
    2
  );

  return [
    {
      name: "SAGE100 - KHKAdressen -> Account",
      objectName: "KHKAdressen",
      sourceDefinition: [
        "SELECT",
        "  Kundennummer AS ExternalKey,",
        "  Name1 AS AccountName,",
        "  Strasse AS BillingStreet,",
        "  PLZ AS BillingPostalCode,",
        "  Ort AS BillingCity,",
        "  Land AS BillingCountry,",
        "  Telefon AS Phone,",
        "  Webseite AS Website",
        "FROM KHKAdressen",
      ].join("\n"),
      mappingDefinition: [
        "AccountNumber;string=ExternalKey;TRIM",
        "Name;string=AccountName;TRIM",
        "BillingStreet;string=BillingStreet;TRIM",
        "BillingPostalCode;string=BillingPostalCode;TRIM",
        "BillingCity;string=BillingCity;TRIM",
        "BillingCountry;string=BillingCountry;TRIM",
        "Phone;string=Phone;TRIM",
        "Website;string=Website;TRIM",
      ].join("\n"),
      targetDefinition: accountTargetDefinition,
    },
    {
      name: "SAGE100 - KHKAnsprechpartner -> Contact",
      objectName: "KHKAnsprechpartner",
      sourceDefinition: [
        "SELECT",
        "  AnsprechpartnerNr AS ExternalKey,",
        "  Vorname AS FirstName,",
        "  Nachname AS LastName,",
        "  Email AS Email,",
        "  Telefon AS Phone,",
        "  Mobil AS MobilePhone",
        "FROM KHKAnsprechpartner",
      ].join("\n"),
      mappingDefinition: [
        "Email;string=Email;LOWERCASE",
        "FirstName;string=FirstName;TRIM",
        "LastName;string=LastName;TRIM",
        "Phone;string=Phone;TRIM",
        "MobilePhone;string=MobilePhone;TRIM",
      ].join("\n"),
      targetDefinition: contactTargetDefinition,
    },
  ];
}

async function upsertConnectorByName(connection, connectorTemplate, activate) {
  const escapedName = escapeSoql(connectorTemplate.name);
  const soql = `
    SELECT Id, Name
    FROM MSD_Connector__c
    WHERE Name = '${escapedName}'
    ORDER BY CreatedDate DESC
    LIMIT 1
  `;

  const result = await connection.query(soql);
  const payload = {
    Name: connectorTemplate.name,
    MSD_Active__c: activate,
    MSD_ConnectorType__c: "mssql",
    MSD_TargetSystem__c: "salesforce",
    MSD_Direction__c: "Inbound",
    MSD_SecretKey__c: connectorTemplate.secretKey,
    MSD_Parameters__c: JSON.stringify(connectorTemplate.parameters),
    MSD_Description__c: "Generated by installation mode SAGE100",
  };

  if (result.records.length > 0) {
    const existingId = result.records[0].Id;
    const updateResult = await connection.sobject("MSD_Connector__c").update({
      Id: existingId,
      ...payload,
    });

    if (!updateResult.success) {
      const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
      throw new Error(`Failed to update connector ${connectorTemplate.name}: ${details}`);
    }

    return { action: "updated", id: existingId };
  }

  const createResult = await connection.sobject("MSD_Connector__c").create(payload);
  if (!createResult.success || !createResult.id) {
    const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
    throw new Error(`Failed to create connector ${connectorTemplate.name}: ${details}`);
  }

  return { action: "created", id: createResult.id };
}

async function upsertScheduleByName(connection, scheduleTemplate, connectorId, activate) {
  const escapedName = escapeSoql(scheduleTemplate.name);
  const soql = `
    SELECT Id, Name, Active__c
    FROM MSD_Schedule__c
    WHERE Name = '${escapedName}'
    ORDER BY CreatedDate DESC
    LIMIT 1
  `;

  const result = await connection.query(soql);
  const payload = {
    Name: scheduleTemplate.name,
    Active__c: activate,
    SourceSystem__c: "SAGE100",
    TargetSystem__c: "salesforce",
    ObjectName__c: scheduleTemplate.objectName,
    Operation__c: "upsert",
    MSD_Direction__c: "Inbound",
    MSD_SourceType__c: "MSSQL_SQL",
    MSD_TargetType__c: "SALESFORCE",
    MSD_SourceDefinition__c: scheduleTemplate.sourceDefinition,
    MSD_MappingDefinition__c: scheduleTemplate.mappingDefinition,
    MSD_Connector__c: connectorId,
    MSD_TargetDefinition__c: scheduleTemplate.targetDefinition,
    BatchSize__c: 200,
  };

  if (result.records.length > 0) {
    const existingId = result.records[0].Id;
    const updateResult = await connection.sobject("MSD_Schedule__c").update({
      Id: existingId,
      ...payload,
    });

    if (!updateResult.success) {
      const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
      throw new Error(`Failed to update schedule ${scheduleTemplate.name}: ${details}`);
    }

    return { action: "updated", id: existingId };
  }

  const createResult = await connection.sobject("MSD_Schedule__c").create(payload);
  if (!createResult.success || !createResult.id) {
    const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
    throw new Error(`Failed to create schedule ${scheduleTemplate.name}: ${details}`);
  }

  return { action: "created", id: createResult.id };
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.mode) {
    throw new Error("Missing required argument: --mode <MODE>. Example: --mode SAGE100");
  }

  if (args.mode !== "SAGE100") {
    throw new Error(`Unsupported installation mode: ${args.mode}. Supported: SAGE100`);
  }

  const rl = args.interactive && input.isTTY
    ? readline.createInterface({ input, output })
    : null;

  const loginUrl = await resolveValue({
    key: "SF_LOGIN_URL",
    label: "Salesforce Login URL",
    rl,
    interactive: args.interactive,
  });
  const clientId = await resolveValue({
    key: "SF_CLIENT_ID",
    label: "Salesforce Client ID",
    rl,
    interactive: args.interactive,
  });
  const clientSecret = await resolveValue({
    key: "SF_CLIENT_SECRET",
    label: "Salesforce Client Secret",
    rl,
    interactive: args.interactive,
  });

  const sqlServer = await resolveValue({
    key: "SAGE100_SQL_SERVER",
    label: "SAGE100 SQL Server host",
    rl,
    interactive: args.interactive,
  });
  const sqlPortRaw = await resolveValue({
    key: "SAGE100_SQL_PORT",
    label: "SAGE100 SQL Server port",
    rl,
    interactive: args.interactive,
    defaultValue: "1433",
  });
  const sqlDatabase = await resolveValue({
    key: "SAGE100_SQL_DATABASE",
    label: "SAGE100 SQL Database",
    rl,
    interactive: args.interactive,
  });
  const sqlUser = await resolveValue({
    key: "SAGE100_SQL_USER",
    label: "SAGE100 SQL User",
    rl,
    interactive: args.interactive,
  });
  const sqlPassword = await resolveValue({
    key: "SAGE100_SQL_PASSWORD",
    label: "SAGE100 SQL Password",
    rl,
    interactive: args.interactive,
  });

  const sqlPort = Number.parseInt(sqlPortRaw, 10);
  if (!Number.isInteger(sqlPort) || sqlPort <= 0) {
    throw new Error(`Invalid SAGE100_SQL_PORT: ${sqlPortRaw}`);
  }

  upsertEnvValues({
    SF_LOGIN_URL: loginUrl,
    SF_CLIENT_ID: clientId,
    SF_CLIENT_SECRET: clientSecret,
    SAGE100_SQL_SERVER: sqlServer,
    SAGE100_SQL_PORT: String(sqlPort),
    SAGE100_SQL_DATABASE: sqlDatabase,
    SAGE100_SQL_USER: sqlUser,
    SAGE100_SQL_PASSWORD: sqlPassword,
  });

  if (rl) {
    rl.close();
  }

  const connection = await login(loginUrl, clientId, clientSecret);
  const templates = buildSage100Templates();

  const connectorTemplate = {
    name: "SAGE100 MSSQL Connector",
    secretKey: "SAGE100_SQL_PASSWORD",
    parameters: {
      server: sqlServer,
      port: sqlPort,
      database: sqlDatabase,
      user: sqlUser,
      encrypt: false,
      trustServerCertificate: true,
    },
  };

  console.log(`Initializing installation mode ${args.mode} ...`);
  
  // Deploy metadata first (creates custom objects if they don't exist)
  await deployMetadata(connection);

  // Validate that required fields were created
  console.log("🔍 Validating deployed metadata...");
  await validateCustomObjectFields(connection, "MSD_Connector__c", [
    "Name",
    "MSD_Active__c",
    "MSD_ConnectorType__c",
    "MSD_TargetSystem__c",
    "MSD_Direction__c",
    "MSD_Parameters__c",
  ]);
  await validateCustomObjectFields(connection, "MSD_Schedule__c", [
    "Name",
    "Active__c",
    "MSD_Connector__c",
    "MSD_SourceDefinition__c",
    "MSD_TargetDefinition__c",
  ]);
  console.log("  ✓ All metadata validated successfully\n");

  const connectorResult = await upsertConnectorByName(connection, connectorTemplate, true);
  console.log(`- ${connectorResult.action.toUpperCase()}: ${connectorTemplate.name} (${connectorResult.id})`);

  for (const template of templates) {
    const { action, id } = await upsertScheduleByName(
      connection,
      template,
      connectorResult.id,
      args.activate
    );
    console.log(`- ${action.toUpperCase()}: ${template.name} (${id})`);
  }

  if (!args.activate) {
    console.log("Schedules were created/updated as inactive. Use --activate if they should start immediately.");
  }

  console.log("Installation profile bootstrap finished.");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
