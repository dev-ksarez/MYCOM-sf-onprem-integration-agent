#!/usr/bin/env node

const dotenv = require("dotenv");
const jsforce = require("jsforce");

dotenv.config();

function parseArgs(argv) {
  const args = {
    mode: "",
    activate: false,
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

function escapeSoql(value) {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function login() {
  const loginUrl = requiredEnv("SF_LOGIN_URL");
  const clientId = requiredEnv("SF_CLIENT_ID");
  const clientSecret = requiredEnv("SF_CLIENT_SECRET");

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
    throw new Error(`Salesforce token request failed: ${response.status} ${await response.text()}`);
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

async function upsertScheduleByName(connection, scheduleTemplate, activate) {
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

  const connection = await login();
  const templates = buildSage100Templates();

  console.log(`Initializing installation mode ${args.mode} ...`);
  for (const template of templates) {
    const { action, id } = await upsertScheduleByName(connection, template, args.activate);
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
