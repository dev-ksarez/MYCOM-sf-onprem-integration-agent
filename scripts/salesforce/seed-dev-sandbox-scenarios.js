#!/usr/bin/env node

const dotenv = require("dotenv");
const jsforce = require("jsforce");
const sql = require("mssql");

dotenv.config();

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`);
  }
  return value;
}

function optionalEnv(name, fallback = "") {
  const value = String(process.env[name] || "").trim();
  return value || fallback;
}

function escapeSoql(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
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
    const text = await response.text();
    throw new Error(`Salesforce token request failed: ${response.status} ${text}`);
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

function getSqlConfig() {
  return {
    server: requiredEnv("SAGE100_SQL_SERVER"),
    port: Number(requiredEnv("SAGE100_SQL_PORT")),
    database: requiredEnv("SAGE100_SQL_DATABASE"),
    user: requiredEnv("SAGE100_SQL_USER"),
    password: requiredEnv("SAGE100_SQL_PASSWORD"),
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  };
}

function tryGetSqlConfigFromEnv() {
  try {
    return getSqlConfig();
  } catch {
    return null;
  }
}

async function getSqlConfigFromConnector(connection, connectorId) {
  const escapedId = escapeSoql(connectorId);
  const result = await connection.query(`
    SELECT Id, Name, MSD_SecretKey__c, MSD_Parameters__c
    FROM MSD_Connector__c
    WHERE Id = '${escapedId}'
    LIMIT 1
  `);

  const connector = (result.records || [])[0];
  if (!connector) {
    throw new Error(`Connector not found: ${connectorId}`);
  }

  const rawParameters = String(connector.MSD_Parameters__c || "{}").trim();
  let parameters;
  try {
    parameters = JSON.parse(rawParameters || "{}");
  } catch {
    throw new Error(`Invalid JSON in MSD_Parameters__c for connector ${connector.Name}`);
  }

  const secretKey = String(connector.MSD_SecretKey__c || "").trim();
  const password = secretKey ? String(process.env[secretKey] || "").trim() : "";

  const server = String(parameters.server || "").trim();
  const database = String(parameters.database || "").trim();
  const user = String(parameters.user || "").trim();
  const port = Number(parameters.port || 1433);

  if (!server || !database || !user || !password) {
    throw new Error(
      `Connector ${connector.Name} is missing SQL values (server/database/user/password). ` +
      `Expected password env variable: ${secretKey || "<none>"}`
    );
  }

  return {
    server,
    port,
    database,
    user,
    password,
    options: {
      encrypt: Boolean(parameters.encrypt),
      trustServerCertificate:
        parameters.trustServerCertificate === undefined ? true : Boolean(parameters.trustServerCertificate),
    },
  };
}

async function ensureScenarioTables(sqlConfig) {
  let pool;
  const defaultPricebookEntryId = optionalEnv("DEV_SCENARIO_PRICEBOOK_ENTRY_ID", "01sWz000001506rIAA");

  try {
    pool = await sql.connect(sqlConfig);

    await pool.request().query(`
IF OBJECT_ID('dbo.msd_opportunities_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_opportunities_scenario (
    opportunity_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    account_external_key NVARCHAR(80) NULL,
    name NVARCHAR(255) NOT NULL,
    stage_name NVARCHAR(80) NOT NULL,
    close_date DATE NOT NULL,
    amount DECIMAL(18, 2) NULL
  );
END

IF OBJECT_ID('dbo.msd_opportunity_items_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_opportunity_items_scenario (
    item_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    opportunity_name NVARCHAR(255) NOT NULL,
    quantity DECIMAL(18, 2) NOT NULL,
    unit_price DECIMAL(18, 2) NOT NULL,
    pricebook_entry_id NVARCHAR(18) NULL
  );
END

IF OBJECT_ID('dbo.msd_quotes_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_quotes_scenario (
    quote_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    opportunity_name NVARCHAR(255) NOT NULL,
    quote_number NVARCHAR(80) NOT NULL,
    status NVARCHAR(80) NOT NULL,
    expiration_date DATE NULL
  );
END

IF OBJECT_ID('dbo.msd_quote_items_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_quote_items_scenario (
    item_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    quote_number NVARCHAR(80) NOT NULL,
    quantity DECIMAL(18, 2) NOT NULL,
    unit_price DECIMAL(18, 2) NOT NULL,
    pricebook_entry_id NVARCHAR(18) NULL
  );
END

IF OBJECT_ID('dbo.msd_orders_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_orders_scenario (
    order_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    account_external_key NVARCHAR(80) NULL,
    order_number NVARCHAR(80) NOT NULL,
    status NVARCHAR(80) NOT NULL,
    effective_date DATE NOT NULL
  );
END

IF OBJECT_ID('dbo.msd_order_items_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_order_items_scenario (
    item_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    order_number NVARCHAR(80) NOT NULL,
    quantity DECIMAL(18, 2) NOT NULL,
    unit_price DECIMAL(18, 2) NOT NULL,
    pricebook_entry_id NVARCHAR(18) NULL
  );
END

IF OBJECT_ID('dbo.msd_invoices_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_invoices_scenario (
    invoice_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    account_external_key NVARCHAR(80) NULL,
    invoice_number NVARCHAR(80) NOT NULL,
    status NVARCHAR(80) NOT NULL,
    invoice_date DATE NOT NULL,
    total_amount DECIMAL(18, 2) NULL
  );
END

IF OBJECT_ID('dbo.msd_invoice_items_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_invoice_items_scenario (
    item_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    invoice_number NVARCHAR(80) NOT NULL,
    quantity DECIMAL(18, 2) NOT NULL,
    unit_price DECIMAL(18, 2) NOT NULL
  );
END
`);

    await pool.request().query(`
DECLARE @SampleCount INT = 100;
DECLARE @DefaultPricebookEntryId NVARCHAR(18) = '${defaultPricebookEntryId}';

IF OBJECT_ID('dbo.msd_accounts_scenario', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.msd_accounts_scenario (
    external_key NVARCHAR(80) NOT NULL PRIMARY KEY,
    name NVARCHAR(255) NULL,
    account_number NVARCHAR(80) NULL,
    billing_city NVARCHAR(100) NULL,
    billing_country NVARCHAR(100) NULL,
    phone NVARCHAR(80) NULL,
    general_email NVARCHAR(255) NULL
  );
END

IF OBJECT_ID('tempdb..#nums') IS NOT NULL DROP TABLE #nums;
SELECT TOP (@SampleCount)
  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
INTO #nums
FROM sys.all_objects;

;WITH account_seed AS (
  SELECT
    CONCAT('ACC-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS external_key,
    CONCAT('Scenario Account ', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS name,
    CONCAT('A', RIGHT('000000' + CAST(n AS VARCHAR(10)), 6)) AS account_number,
    CONCAT('City ', ((n - 1) % 25) + 1) AS billing_city,
    'DE' AS billing_country,
    CONCAT('+49-30-', RIGHT('0000000' + CAST(1000000 + n AS VARCHAR(10)), 7)) AS phone,
    CONCAT('account', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4), '@scenario.local') AS general_email
  FROM #nums
)
MERGE dbo.msd_accounts_scenario AS target
USING account_seed AS source
ON target.external_key = source.external_key
WHEN MATCHED THEN
  UPDATE SET
    name = source.name,
    account_number = source.account_number,
    billing_city = source.billing_city,
    billing_country = source.billing_country,
    phone = source.phone,
    general_email = source.general_email
WHEN NOT MATCHED THEN
  INSERT (external_key, name, account_number, billing_city, billing_country, phone, general_email)
  VALUES (source.external_key, source.name, source.account_number, source.billing_city, source.billing_country, source.phone, source.general_email);

IF OBJECT_ID('tempdb..#accounts') IS NOT NULL DROP TABLE #accounts;
SELECT
  ROW_NUMBER() OVER (ORDER BY external_key) AS rn,
  external_key
INTO #accounts
FROM (
  SELECT TOP (@SampleCount) external_key
  FROM dbo.msd_accounts_scenario
  WHERE external_key IS NOT NULL AND LTRIM(RTRIM(external_key)) <> ''
  ORDER BY external_key
) src;

DECLARE @AccountCount INT = (SELECT COUNT(*) FROM #accounts);
IF @AccountCount = 0
BEGIN
  THROW 50000, 'No account keys found in dbo.msd_accounts_scenario for scenario linkage.', 1;
END

DELETE FROM dbo.msd_opportunity_items_scenario;
DELETE FROM dbo.msd_quote_items_scenario;
DELETE FROM dbo.msd_order_items_scenario;
DELETE FROM dbo.msd_invoice_items_scenario;
DELETE FROM dbo.msd_quotes_scenario;
DELETE FROM dbo.msd_orders_scenario;
DELETE FROM dbo.msd_invoices_scenario;
DELETE FROM dbo.msd_opportunities_scenario;

INSERT INTO dbo.msd_opportunities_scenario (opportunity_key, account_external_key, name, stage_name, close_date, amount)
SELECT
  CONCAT('OPP-', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS opportunity_key,
  acc.external_key AS account_external_key,
  CONCAT('Opportunity ', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS name,
  CASE n.n % 5
    WHEN 0 THEN 'Neu'
    WHEN 1 THEN 'Qualifiziert'
    WHEN 2 THEN 'Bedarfsanalyse'
    WHEN 3 THEN 'Angebot erstellt'
    ELSE 'In Verhandlung'
  END AS stage_name,
  DATEADD(DAY, n.n % 45, CONVERT(date, GETDATE())) AS close_date,
  CAST(1000 + (n.n * 135.75) AS DECIMAL(18,2)) AS amount
FROM #nums n
JOIN #accounts acc ON acc.rn = ((n.n - 1) % @AccountCount) + 1;

INSERT INTO dbo.msd_opportunity_items_scenario (item_key, opportunity_name, quantity, unit_price, pricebook_entry_id)
SELECT
  CONCAT('OPPITEM-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4), '-1') AS item_key,
  CONCAT('Opportunity ', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS opportunity_name,
  CAST((n % 5) + 1 AS DECIMAL(18,2)) AS quantity,
  CAST(50 + (n * 12.5) AS DECIMAL(18,2)) AS unit_price,
  @DefaultPricebookEntryId AS pricebook_entry_id
FROM #nums;

INSERT INTO dbo.msd_quotes_scenario (quote_key, opportunity_name, quote_number, status, expiration_date)
SELECT
  CONCAT('QUO-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS quote_key,
  CONCAT('Opportunity ', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS opportunity_name,
  CONCAT('Q-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS quote_number,
  CASE WHEN n % 3 = 0 THEN 'Approved' WHEN n % 3 = 1 THEN 'Draft' ELSE 'Presented' END AS status,
  DATEADD(DAY, 7 + (n % 30), CONVERT(date, GETDATE())) AS expiration_date
FROM #nums;

INSERT INTO dbo.msd_quote_items_scenario (item_key, quote_number, quantity, unit_price, pricebook_entry_id)
SELECT
  CONCAT('QUOITEM-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4), '-1') AS item_key,
  CONCAT('Q-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS quote_number,
  CAST((n % 4) + 1 AS DECIMAL(18,2)) AS quantity,
  CAST(80 + (n * 9.75) AS DECIMAL(18,2)) AS unit_price,
  @DefaultPricebookEntryId AS pricebook_entry_id
FROM #nums;

INSERT INTO dbo.msd_orders_scenario (order_key, account_external_key, order_number, status, effective_date)
SELECT
  CONCAT('ORD-', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS order_key,
  acc.external_key AS account_external_key,
  CONCAT('O-', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS order_number,
  CASE WHEN n.n % 2 = 0 THEN 'Activated' ELSE 'Draft' END AS status,
  DATEADD(DAY, -(n.n % 20), CONVERT(date, GETDATE())) AS effective_date
FROM #nums n
JOIN #accounts acc ON acc.rn = ((n.n - 1) % @AccountCount) + 1;

INSERT INTO dbo.msd_order_items_scenario (item_key, order_number, quantity, unit_price, pricebook_entry_id)
SELECT
  CONCAT('ORDITEM-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4), '-1') AS item_key,
  CONCAT('O-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS order_number,
  CAST((n % 7) + 1 AS DECIMAL(18,2)) AS quantity,
  CAST(65 + (n * 7.25) AS DECIMAL(18,2)) AS unit_price,
  @DefaultPricebookEntryId AS pricebook_entry_id
FROM #nums;

INSERT INTO dbo.msd_invoices_scenario (invoice_key, account_external_key, invoice_number, status, invoice_date, total_amount)
SELECT
  CONCAT('INV-', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS invoice_key,
  acc.external_key AS account_external_key,
  CONCAT('I-', RIGHT('0000' + CAST(n.n AS VARCHAR(10)), 4)) AS invoice_number,
  CASE WHEN n.n % 4 = 0 THEN 'Paid' WHEN n.n % 4 = 1 THEN 'Open' WHEN n.n % 4 = 2 THEN 'Posted' ELSE 'Overdue' END AS status,
  DATEADD(DAY, -(n.n % 35), CONVERT(date, GETDATE())) AS invoice_date,
  CAST(250 + (n.n * 49.95) AS DECIMAL(18,2)) AS total_amount
FROM #nums n
JOIN #accounts acc ON acc.rn = ((n.n - 1) % @AccountCount) + 1;

INSERT INTO dbo.msd_invoice_items_scenario (item_key, invoice_number, quantity, unit_price)
SELECT
  CONCAT('INVITEM-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4), '-1') AS item_key,
  CONCAT('I-', RIGHT('0000' + CAST(n AS VARCHAR(10)), 4)) AS invoice_number,
  CAST((n % 6) + 1 AS DECIMAL(18,2)) AS quantity,
  CAST(40 + (n * 8.8) AS DECIMAL(18,2)) AS unit_price
FROM #nums;
`);
  } finally {
    if (pool) {
      await pool.close();
    }
    await sql.close();
  }
}

function buildObjectTargetDefinition({ profileName, objectApiName, operation }) {
  return JSON.stringify({
    selectedImportProfileName: profileName,
    importProfiles: [
      {
        name: profileName,
        active: true,
        schedulerEnabled: true,
        mode: "object",
        scheduler: {
          mode: "rules",
          rules: [
            {
              days: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
              startTime: "00:00",
              endTime: "23:59",
              intervalMinutes: 2,
            },
          ],
        },
        objectApiName,
        operation,
        picklists: [],
      },
    ],
  });
}

function buildScenarioTemplates() {
  const invoiceObject = optionalEnv("DEV_SCENARIO_INVOICE_OBJECT", "Invoice__c");
  const invoiceItemObject = optionalEnv("DEV_SCENARIO_INVOICE_ITEM_OBJECT", "Invoice_Item__c");
  const opportunityPicklistExampleMappingDefinition = JSON.stringify(
    [
      {
        sourceField: "name",
        sourceType: "string",
        targetField: "Name",
        targetType: "string",
        lookupEnabled: false,
        lookupObject: "",
        lookupField: "",
        transformFunction: "TRIM",
        picklistMappings: []
      },
      {
        sourceField: "stage_name",
        sourceType: "string",
        targetField: "StageName",
        targetType: "string",
        lookupEnabled: false,
        lookupObject: "",
        lookupField: "",
        transformFunction: "TRIM",
        picklistMappings: [
          { source: "Neu", target: "Prospecting" },
          { source: "Qualifiziert", target: "Qualification" },
          { source: "Bedarfsanalyse", target: "Needs Analysis" },
          { source: "Angebot erstellt", target: "Proposal/Price Quote" },
          { source: "In Verhandlung", target: "Negotiation/Review" }
        ]
      },
      {
        sourceField: "close_date",
        sourceType: "string",
        targetField: "CloseDate",
        targetType: "string",
        lookupEnabled: false,
        lookupObject: "",
        lookupField: "",
        transformFunction: "NONE",
        picklistMappings: []
      },
      {
        sourceField: "amount",
        sourceType: "string",
        targetField: "Amount",
        targetType: "number",
        lookupEnabled: false,
        lookupObject: "",
        lookupField: "",
        transformFunction: "NONE",
        picklistMappings: []
      },
      {
        sourceField: "account_external_key",
        sourceType: "string",
        targetField: "AccountId",
        targetType: "string",
        lookupEnabled: true,
        lookupObject: "Account",
        lookupField: "ERP_Account_Number__c",
        transformFunction: "NONE",
        picklistMappings: []
      }
    ],
    null,
    2
  );

  return [
    {
      name: "Szenario MSSQL->SF Opportunity",
      objectName: "Opportunity",
      operation: "Insert",
      sourceDefinition: "SELECT opportunity_key, account_external_key, name, stage_name, close_date, amount FROM dbo.msd_opportunities_scenario",
      mappingDefinition: opportunityPicklistExampleMappingDefinition,
      targetDefinition: buildObjectTargetDefinition({
        profileName: "opportunities-every-2min",
        objectApiName: "Opportunity",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Opportunity Items",
      objectName: "OpportunityLineItem",
      operation: "Insert",
      sourceDefinition: "SELECT item_key, opportunity_name, quantity, unit_price, pricebook_entry_id FROM dbo.msd_opportunity_items_scenario",
      mappingDefinition: [
        "OpportunityId;string=opportunity_name;LOOKUP[Opportunity|Name]",
        "Quantity;string=quantity;NONE",
        "UnitPrice;string=unit_price;NONE",
        "PricebookEntryId;string=pricebook_entry_id;TRIM",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "opportunity-items-every-2min",
        objectApiName: "OpportunityLineItem",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Angebote",
      objectName: "Quote",
      operation: "Insert",
      sourceDefinition: "SELECT quote_key, opportunity_name, quote_number, status, expiration_date FROM dbo.msd_quotes_scenario",
      mappingDefinition: [
        "Name;string=quote_number;TRIM",
        "OpportunityId;string=opportunity_name;LOOKUP[Opportunity|Name]",
        "Status;string=status;TRIM",
        "ExpirationDate;string=expiration_date;NONE",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "quotes-every-2min",
        objectApiName: "Quote",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Angebotspositionen",
      objectName: "QuoteLineItem",
      operation: "Insert",
      sourceDefinition: "SELECT item_key, quote_number, quantity, unit_price, pricebook_entry_id FROM dbo.msd_quote_items_scenario",
      mappingDefinition: [
        "QuoteId;string=quote_number;LOOKUP[Quote|Name]",
        "Quantity;string=quantity;NONE",
        "UnitPrice;string=unit_price;NONE",
        "PricebookEntryId;string=pricebook_entry_id;TRIM",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "quote-items-every-2min",
        objectApiName: "QuoteLineItem",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Bestellungen",
      objectName: "Order",
      operation: "Insert",
      sourceDefinition: "SELECT order_key, account_external_key, order_number, status, effective_date FROM dbo.msd_orders_scenario",
      mappingDefinition: [
        "OrderNumber;string=order_number;TRIM",
        "Status;string=status;TRIM",
        "EffectiveDate;string=effective_date;NONE",
        "AccountId;string=account_external_key;LOOKUP[Account|ERP_Account_Number__c]",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "orders-every-2min",
        objectApiName: "Order",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Bestellpositionen",
      objectName: "OrderItem",
      operation: "Insert",
      sourceDefinition: "SELECT item_key, order_number, quantity, unit_price, pricebook_entry_id FROM dbo.msd_order_items_scenario",
      mappingDefinition: [
        "OrderId;string=order_number;LOOKUP[Order|OrderNumber]",
        "Quantity;string=quantity;NONE",
        "UnitPrice;string=unit_price;NONE",
        "PricebookEntryId;string=pricebook_entry_id;TRIM",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "order-items-every-2min",
        objectApiName: "OrderItem",
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Rechnungen",
      objectName: invoiceObject,
      operation: "Insert",
      sourceDefinition: "SELECT invoice_key, account_external_key, invoice_number, status, invoice_date, total_amount FROM dbo.msd_invoices_scenario",
      mappingDefinition: [
        "Name;string=invoice_number;TRIM",
        "Invoice_Number__c;string=invoice_number;TRIM",
        "Status__c;string=status;TRIM",
        "Invoice_Date__c;string=invoice_date;NONE",
        "Total_Amount__c;string=total_amount;NONE",
        "Account__c;string=account_external_key;LOOKUP[Account|ERP_Account_Number__c]",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "invoices-every-2min",
        objectApiName: invoiceObject,
        operation: "insert",
      }),
    },
    {
      name: "Szenario MSSQL->SF Rechnungspositionen",
      objectName: invoiceItemObject,
      operation: "Insert",
      sourceDefinition: "SELECT item_key, invoice_number, quantity, unit_price FROM dbo.msd_invoice_items_scenario",
      mappingDefinition: [
        "Name;string=item_key;TRIM",
        "Invoice__c;string=invoice_number;LOOKUP[Invoice__c|Invoice_Number__c]",
        "Quantity__c;string=quantity;NONE",
        "UnitPrice__c;string=unit_price;NONE",
      ].join("\n"),
      targetDefinition: buildObjectTargetDefinition({
        profileName: "invoice-items-every-2min",
        objectApiName: invoiceItemObject,
        operation: "insert",
      }),
    },
  ];
}

async function sanitizePayloadForWrite(connection, objectName, payload, operation) {
  try {
    const describe = await connection.describe(objectName);
    const fields = Array.isArray(describe.fields) ? describe.fields : [];
    const allowed = new Set(
      fields
        .filter((field) => {
          if (operation === "create") {
            return field.createable;
          }
          return field.updateable;
        })
        .map((field) => field.name)
    );

    const sanitized = {};
    const skipped = [];

    for (const [key, value] of Object.entries(payload)) {
      if (key === "Id" || allowed.has(key)) {
        sanitized[key] = value;
      } else {
        skipped.push(key);
      }
    }

    if (skipped.length > 0) {
      console.warn(`  ⚠️  ${objectName}: skipping non-${operation}able fields: ${skipped.join(", ")}`);
    }

    return sanitized;
  } catch (error) {
    console.warn(`  ⚠️  Could not describe ${objectName}: ${error instanceof Error ? error.message : error}`);
    return payload;
  }
}

async function resolveConnectorId(connection) {
  const explicit = optionalEnv("DEV_SCENARIO_CONNECTOR_ID");
  if (explicit) {
    return explicit;
  }

  const connectorName = optionalEnv("DEV_SCENARIO_CONNECTOR_NAME", "SAGE100 MSSQL Connector");
  const escapedName = escapeSoql(connectorName);

  const byName = await connection.query(`
    SELECT Id
    FROM MSD_Connector__c
    WHERE Name = '${escapedName}'
    ORDER BY CreatedDate DESC
    LIMIT 1
  `);

  if ((byName.records || []).length > 0) {
    return byName.records[0].Id;
  }

  const fallback = await connection.query(`
    SELECT MSD_Connector__c
    FROM MSD_Schedule__c
    WHERE MSD_SourceType__c = 'MSSQL_SQL'
      AND MSD_Connector__c != null
    ORDER BY LastModifiedDate DESC
    LIMIT 1
  `);

  if ((fallback.records || []).length > 0) {
    return fallback.records[0].MSD_Connector__c;
  }

  throw new Error("Could not resolve MSSQL connector. Set DEV_SCENARIO_CONNECTOR_ID in .env.");
}

async function upsertSchedule(connection, template, connectorId, activate) {
  const existing = await connection.query(`
    SELECT Id, Name, MSD_Connector__c, ObjectName__c, MSD_SourceDefinition__c, MSD_MappingDefinition__c, MSD_TargetDefinition__c
    FROM MSD_Schedule__c
    ORDER BY LastModifiedDate DESC
    LIMIT 200
  `);

  const records = existing.records || [];
  const matches = records.filter((record) => {
    const connectorMatches = String(record.MSD_Connector__c || "").trim() === String(connectorId).trim();
    const objectMatches = String(record.ObjectName__c || "").trim() === String(template.objectName).trim();
    const sourceMatches = String(record.MSD_SourceDefinition__c || "").trim() === String(template.sourceDefinition).trim();
    const targetMatches = String(record.MSD_TargetDefinition__c || "").trim() === String(template.targetDefinition).trim();
    const mappingMatches = String(record.MSD_MappingDefinition__c || "").trim() === String(template.mappingDefinition).trim();

    return connectorMatches && objectMatches && sourceMatches && targetMatches && mappingMatches;
  });

  const payload = {
    Active__c: !!activate,
    SourceSystem__c: "MSSQL",
    TargetSystem__c: "SALESFORCE",
    ObjectName__c: template.objectName,
    Operation__c: template.operation,
    MSD_Direction__c: "Inbound",
    MSD_SourceType__c: "MSSQL_SQL",
    MSD_TargetType__c: "SALESFORCE",
    MSD_SourceDefinition__c: template.sourceDefinition,
    MSD_MappingDefinition__c: template.mappingDefinition,
    MSD_Connector__c: connectorId,
    MSD_TargetDefinition__c: template.targetDefinition,
    BatchSize__c: 200,
    Name: template.name,
  };

  if (matches.length > 0) {
    const updatePayload = await sanitizePayloadForWrite(connection, "MSD_Schedule__c", payload, "update");
    const updateResult = await connection.sobject("MSD_Schedule__c").update({
      Id: matches[0].Id,
      ...updatePayload,
    });

    if (!updateResult.success) {
      const details = "errors" in updateResult ? JSON.stringify(updateResult.errors) : "unknown update error";
      throw new Error(`Failed to update schedule ${template.name}: ${details}`);
    }

    return { action: "updated", id: matches[0].Id };
  }

  const createPayload = await sanitizePayloadForWrite(connection, "MSD_Schedule__c", payload, "create");
  const createResult = await connection.sobject("MSD_Schedule__c").create(createPayload);
  if (!createResult.success || !createResult.id) {
    const details = "errors" in createResult ? JSON.stringify(createResult.errors) : "unknown create error";
    throw new Error(`Failed to create schedule ${template.name}: ${details}`);
  }

  return { action: "created", id: createResult.id };
}

async function main() {
  const activate = optionalEnv("DEV_SCENARIO_ACTIVATE", "true").toLowerCase() !== "false";

  console.log("1) Logging in to Salesforce...");
  const connection = await login();
  const connectorId = await resolveConnectorId(connection);
  console.log(`   ✓ Connector: ${connectorId}`);

  console.log("2) Creating/updating MSSQL example tables...");
  const sqlConfig = tryGetSqlConfigFromEnv() || (await getSqlConfigFromConnector(connection, connectorId));
  await ensureScenarioTables(sqlConfig);
  console.log("   ✓ MSSQL scenario tables ready");

  console.log("3) Upserting scenario schedules...");
  const templates = buildScenarioTemplates();
  for (const template of templates) {
    const result = await upsertSchedule(connection, template, connectorId, activate);
    console.log(`   - ${result.action.toUpperCase()}: ${template.name} (${result.id})`);
  }

  console.log("Done. DEV sandbox scenario (Opportunity/Quote/Order/Invoice + Items) is ready.");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
