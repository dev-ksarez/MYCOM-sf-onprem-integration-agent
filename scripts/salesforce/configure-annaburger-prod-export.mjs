import { execFileSync } from "node:child_process";

const targetOrg = process.env.SF_TARGET_ORG || "AnnaburgerPROD1";
const apiVersion = "v67.0";

function parseCliJson(output) {
  const text = String(output || "");
  const start = text.indexOf("{");
  if (start < 0) {
    throw new Error(`CLI output did not contain JSON: ${text}`);
  }
  return JSON.parse(text.slice(start));
}

function sfJson(args) {
  return parseCliJson(execFileSync("sf", [...args, "--json"], { encoding: "utf8" }));
}

const orgDisplay = sfJson(["org", "display", "--target-org", targetOrg]);
const tokenResult = sfJson(["org", "auth", "show-access-token", "--target-org", targetOrg]);
const instanceUrl = orgDisplay.result.instanceUrl;
const accessToken = tokenResult.result.accessToken || tokenResult.result.token;

if (!instanceUrl || !accessToken) {
  throw new Error("Could not resolve Salesforce instance URL or access token from sf CLI");
}

async function salesforceRequest(method, path, body) {
  const response = await fetch(`${instanceUrl}/services/data/${apiVersion}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json"
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : undefined;

  if (!response.ok) {
    throw new Error(`${method} ${path} failed with ${response.status}: ${text}`);
  }

  return payload;
}

async function query(soql) {
  const payload = await salesforceRequest("GET", `/query?q=${encodeURIComponent(soql)}`);
  return payload.records || [];
}

async function upsertByName(sobjectName, name, values, options = {}) {
  const records = await query(`SELECT Id, Name FROM ${sobjectName} WHERE Name = '${name.replace(/'/g, "\\'")}' LIMIT 1`);
  if (records.length) {
    await salesforceRequest("PATCH", `/sobjects/${sobjectName}/${records[0].Id}`, values);
    return { id: records[0].Id, name: records[0].Name, action: "updated" };
  }

  const createPayload = options.includeNameOnCreate === false ? values : { Name: name, ...values };
  const created = await salesforceRequest("POST", `/sobjects/${sobjectName}`, createPayload);
  const [createdRecord] = await query(`SELECT Id, Name FROM ${sobjectName} WHERE Id = '${created.id}'`);
  return { id: created.id, name: createdRecord?.Name || name, action: "inserted" };
}

const connectorParameters = {
  server: "anf-sql",
  port: 1433,
  database: "Salesforce",
  user: "MyCom",
  schema: "sf",
  encrypt: false,
  trustServerCertificate: true
};

const connector = await upsertByName("MSD_Connector__c", "SAGE100 MSSQL Salesforce Export Connector", {
  MSD_Active__c: true,
  MSD_ConnectorType__c: "MSSQL",
  MSD_TargetSystem__c: "MS SQL",
  MSD_Direction__c: "Outbound",
  MSD_SecretKey__c: "SAGE100_SQL_PASSWORD",
  MSD_TimeoutMs__c: 30000,
  MSD_MaxRetries__c: 3,
  MSD_Description__c: "Annaburger PROD Salesforce Export/Rueckmeldung in MSSQL DB Salesforce, Schema sf.",
  MSD_Parameters__c: JSON.stringify(connectorParameters)
});

const delta = { strategy: "datetime", field: "LastModifiedDate" };
const schedulerActive = true;

const schedules = [
  {
    name: "SCH-0033",
    objectName: "Account",
    direction: "Outbound",
    sourceType: "SALESFORCE_SOQL",
    targetType: "MSSQL",
    sourceSystem: "Salesforce",
    targetSystem: "MS SQL",
    operation: "Upsert",
    sourceDefinition: {
      queryText: "SELECT Id, AccountNumber, Name, AccountName2__c, Type, Type__c, Description, Phone, Mobile__c, General_Email__c, Website, Industry, NumberOfEmployees, BillingStreet, BillingPostalCode, BillingCity, BillingCountry, BillingCountryCode, ShippingStreet, ShippingPostalCode, ShippingCity, ShippingCountry, ShippingCountryCode, ERP_Address_Number__c, ERP_Account_Number__c, Customer_MatchCode__c, Customer_Group__c, VAT_ID__c, Taxation_Type__c, Price_list__c, Last_Dunning_Date__c, last_sale_Date__c, Ranking__c, InterestedIn__c, InterestedIn_Other_c__c, DealerGroup__c, Dealer__c, ERP_Representative__c, Bill_to_Account__c, ParentId, OwnerId, is_Active__c, LastModifiedDate, LastModifiedBy.Username FROM Account WHERE AccountNumber != null",
      delta
    },
    targetDefinition: { schema: "sf", table: "KHKAdressen_Staging", upsertKey: "AccountNumber" },
    mappingDefinition: [
      "Id;string=Id;NONE",
      "AccountNumber;string=AccountNumber;TRIM",
      "Name;string=Name;TRIM",
      "AccountName2__c;string=AccountName2__c;TRIM",
      "Type;string=Type;TRIM",
      "Type__c;string=Type__c;TRIM",
      "Description;string=Description;TRIM",
      "Phone;string=Phone;TRIM",
      "Mobile__c;string=Mobile__c;TRIM",
      "General_Email__c;string=General_Email__c;TRIM",
      "Website;string=Website;TRIM",
      "Industry;string=Industry;TRIM",
      "NumberOfEmployees;number=NumberOfEmployees;NONE",
      "BillingStreet;string=BillingStreet;TRIM",
      "BillingPostalCode;string=BillingPostalCode;TRIM",
      "BillingCity;string=BillingCity;TRIM",
      "BillingCountry;string=BillingCountry;TRIM",
      "BillingCountryCode;string=BillingCountryCode;TRIM",
      "ShippingStreet;string=ShippingStreet;TRIM",
      "ShippingPostalCode;string=ShippingPostalCode;TRIM",
      "ShippingCity;string=ShippingCity;TRIM",
      "ShippingCountry;string=ShippingCountry;TRIM",
      "ShippingCountryCode;string=ShippingCountryCode;TRIM",
      "ERP_Address_Number__c;string=ERP_Address_Number__c;TRIM",
      "ERP_Account_Number__c;string=ERP_Account_Number__c;TRIM",
      "Customer_MatchCode__c;string=Customer_MatchCode__c;TRIM",
      "Customer_Group__c;string=Customer_Group__c;TRIM",
      "VAT_ID__c;string=VAT_ID__c;TRIM",
      "Taxation_Type__c;string=Taxation_Type__c;TRIM",
      "Price_list__c;string=Price_list__c;TRIM",
      "Last_Dunning_Date__c;datetime=Last_Dunning_Date__c;NONE",
      "last_sale_Date__c;datetime=last_sale_Date__c;NONE",
      "Ranking__c;string=Ranking__c;TRIM",
      "InterestedIn__c;string=InterestedIn__c;TRIM",
      "InterestedIn_Other_c__c;string=InterestedIn_Other_c__c;TRIM",
      "DealerGroup__c;string=DealerGroup__c;TRIM",
      "Dealer__c;string=Dealer__c;TRIM",
      "ERP_Representative__c;string=ERP_Representative__c;TRIM",
      "Bill_to_Account__c;string=Bill_to_Account__c;TRIM",
      "ParentId;string=ParentId;TRIM",
      "OwnerId;string=OwnerId;TRIM",
      "is_Active__c;boolean=is_Active__c;NONE",
      "LastModifiedDate;datetime=LastModifiedDate;NONE",
      "LastModifiedByUsername;string=LastModifiedBy.Username;TRIM"
    ].join("\n")
  },
  {
    name: "SCH-0034",
    objectName: "Contact",
    direction: "Outbound",
    sourceType: "SALESFORCE_SOQL",
    targetType: "MSSQL",
    sourceSystem: "Salesforce",
    targetSystem: "MS SQL",
    operation: "Upsert",
    sourceDefinition: { queryText: "SELECT Id, AccountId, FirstName, LastName, Email, Phone, Title, LastModifiedDate, LastModifiedBy.Username FROM Contact", delta },
    targetDefinition: { schema: "sf", table: "KHKAnsprechpartner_Staging", upsertKey: "Id" },
    mappingDefinition: "Id;string=Id;NONE\nAccountId;string=AccountId;TRIM\nFirstName;string=FirstName;TRIM\nLastName;string=LastName;TRIM\nEmail;string=Email;TRIM\nPhone;string=Phone;TRIM\nTitle;string=Title;TRIM\nLastModifiedDate;datetime=LastModifiedDate;NONE\nLastModifiedByUsername;string=LastModifiedBy.Username;TRIM"
  },
  {
    name: "SCH-0035",
    objectName: "Opportunity",
    direction: "Outbound",
    sourceType: "SALESFORCE_SOQL",
    targetType: "MSSQL",
    sourceSystem: "Salesforce",
    targetSystem: "MS SQL",
    operation: "Upsert",
    sourceDefinition: { queryText: "SELECT Id, Name, AccountId, CloseDate, Amount, StageName, LastModifiedDate, LastModifiedBy.Username FROM Opportunity", delta },
    targetDefinition: { schema: "sf", table: "KHKVerkaufsprojekte_Staging", upsertKey: "Id" },
    mappingDefinition: "Id;string=Id;NONE\nName;string=Name;TRIM\nAccountId;string=AccountId;TRIM\nCloseDate;datetime=CloseDate;NONE\nAmount;number=Amount;NONE\nStageName;string=StageName;TRIM\nLastModifiedDate;datetime=LastModifiedDate;NONE\nLastModifiedByUsername;string=LastModifiedBy.Username;TRIM"
  },
  {
    name: "SCH-0036",
    objectName: "Quote",
    direction: "Outbound",
    sourceType: "SALESFORCE_SOQL",
    targetType: "MSSQL",
    sourceSystem: "Salesforce",
    targetSystem: "MS SQL",
    operation: "Upsert",
    sourceDefinition: { queryText: "SELECT Id, QuoteNumber, OpportunityId, AccountId, Status, ExpirationDate, GrandTotal, LastModifiedDate, LastModifiedBy.Username FROM Quote", delta },
    targetDefinition: { schema: "sf", table: "KHKAngebote_Staging", upsertKey: "QuoteNumber" },
    mappingDefinition: "Id;string=Id;NONE\nQuoteNumber;string=QuoteNumber;TRIM\nOpportunityId;string=OpportunityId;TRIM\nAccountId;string=AccountId;TRIM\nStatus;string=Status;TRIM\nExpirationDate;datetime=ExpirationDate;NONE\nGrandTotal;number=GrandTotal;NONE\nLastModifiedDate;datetime=LastModifiedDate;NONE\nLastModifiedByUsername;string=LastModifiedBy.Username;TRIM"
  },
  {
    name: "SCH-0040",
    objectName: "QuoteLineItem",
    direction: "Outbound",
    sourceType: "SALESFORCE_SOQL",
    targetType: "MSSQL",
    sourceSystem: "Salesforce",
    targetSystem: "MS SQL",
    operation: "Upsert",
    sourceDefinition: { queryText: "SELECT Id, QuoteId, Quote.QuoteNumber, PricebookEntryId, Product2Id, Product2.ProductCode, Description, Quantity, UnitPrice, ListPrice, Discount, TotalPrice, LastModifiedDate, LastModifiedBy.Username FROM QuoteLineItem", delta },
    targetDefinition: { schema: "sf", table: "KHKAngebotePositionen_Staging", upsertKey: "Id" },
    mappingDefinition: "Id;string=Id;NONE\nQuoteId;string=QuoteId;TRIM\nQuoteNumber;string=Quote.QuoteNumber;TRIM\nPricebookEntryId;string=PricebookEntryId;TRIM\nProduct2Id;string=Product2Id;TRIM\nProductCode;string=Product2.ProductCode;TRIM\nDescription;string=Description;TRIM\nQuantity;number=Quantity;NONE\nUnitPrice;number=UnitPrice;NONE\nListPrice;number=ListPrice;NONE\nDiscount;number=Discount;NONE\nTotalPrice;number=TotalPrice;NONE\nLastModifiedDate;datetime=LastModifiedDate;NONE\nLastModifiedByUsername;string=LastModifiedBy.Username;TRIM"
  }
];

const feedbackSchedules = [
  {
    name: "SCH-0041",
    objectName: "Account",
    table: "KHKAdressen_Staging",
    queryText: "SELECT Id, SAGEAddressId, SAGECustomerNo, PostStatus, PostMessage, UpdatedAt FROM sf.KHKAdressen_Staging WHERE PostFlag = 1 AND PostStatus IN ('SAGE_CREATED', 'SAGE_UPDATED', 'SAGE_ERROR')",
    mappingDefinition: "Id;string=Id;NONE\nSAGE_Address_ID__c;number=SAGEAddressId;NONE\nSAGE_Customer_No__c;string=SAGECustomerNo;TRIM\nPOST_STATUS__c;string=PostStatus;TRIM\nPOST_MESSAGE__c;string=PostMessage;TRIM"
  },
  {
    name: "SCH-0042",
    objectName: "Contact",
    table: "KHKAnsprechpartner_Staging",
    queryText: "SELECT Id, SAGEContactId, SAGEAddressId, SAGECustomerNo, PostStatus, PostMessage, UpdatedAt FROM sf.KHKAnsprechpartner_Staging WHERE PostFlag = 1 AND PostStatus IN ('SAGE_CREATED', 'SAGE_UPDATED', 'SAGE_ERROR')",
    mappingDefinition: "Id;string=Id;NONE\nSAGE_Contact_ID__c;number=SAGEContactId;NONE\nSAGE_Address_ID__c;number=SAGEAddressId;NONE\nSAGE_Customer_No__c;string=SAGECustomerNo;TRIM\nPOST_STATUS__c;string=PostStatus;TRIM\nPOST_MESSAGE__c;string=PostMessage;TRIM"
  },
  {
    name: "SCH-0043",
    objectName: "Opportunity",
    table: "KHKVerkaufsprojekte_Staging",
    queryText: "SELECT Id, SAGEOpportunityId, SAGEProjectNo, PostStatus, PostMessage, UpdatedAt FROM sf.KHKVerkaufsprojekte_Staging WHERE PostFlag = 1 AND PostStatus IN ('SAGE_CREATED', 'SAGE_UPDATED', 'SAGE_ERROR')",
    mappingDefinition: "Id;string=Id;NONE\nSAGE_Opportunity_ID__c;number=SAGEOpportunityId;NONE\nSAGE_Project_No__c;string=SAGEProjectNo;TRIM\nPOST_STATUS__c;string=PostStatus;TRIM\nPOST_MESSAGE__c;string=PostMessage;TRIM"
  },
  {
    name: "SCH-0044",
    objectName: "Quote",
    table: "KHKAngebote_Staging",
    queryText: "SELECT Id, SAGEQuoteId, SAGEQuoteNo, PostStatus, PostMessage, UpdatedAt FROM sf.KHKAngebote_Staging WHERE PostFlag = 1 AND PostStatus IN ('SAGE_CREATED', 'SAGE_UPDATED', 'SAGE_ERROR')",
    mappingDefinition: "Id;string=Id;NONE\nSAGE_Quote_ID__c;number=SAGEQuoteId;NONE\nSAGE_Quote_No__c;string=SAGEQuoteNo;TRIM\nPOST_STATUS__c;string=PostStatus;TRIM\nPOST_MESSAGE__c;string=PostMessage;TRIM"
  },
  {
    name: "SCH-0045",
    objectName: "QuoteLineItem",
    table: "KHKAngebotePositionen_Staging",
    queryText: "SELECT Id, SAGEQuoteItemId, SAGEArticleNo, PostStatus, PostMessage, UpdatedAt FROM sf.KHKAngebotePositionen_Staging WHERE PostFlag = 1 AND PostStatus IN ('SAGE_CREATED', 'SAGE_UPDATED', 'SAGE_ERROR')",
    mappingDefinition: "Id;string=Id;NONE\nSAGE_Quote_Item_ID__c;number=SAGEQuoteItemId;NONE\nSAGE_Article_No__c;string=SAGEArticleNo;TRIM\nPOST_STATUS__c;string=PostStatus;TRIM\nPOST_MESSAGE__c;string=PostMessage;TRIM"
  }
].map((schedule) => ({
  ...schedule,
  direction: "Inbound",
  sourceType: "MSSQL_SQL",
  targetType: "SALESFORCE",
  sourceSystem: "MS SQL",
  targetSystem: "Salesforce",
  operation: "Update",
  sourceDefinition: {
    queryText: schedule.queryText,
    delta: { strategy: "datetime", field: "UpdatedAt" },
    afterExport: { PostStatus: "SF_SYNCED", PostFlag: "0" },
    afterExportTable: `sf.${schedule.table}`,
    afterExportKey: "Id"
  },
  targetDefinition: { objectApiName: schedule.objectName, operation: "update", externalIdField: "Id" }
}));

const results = [];
for (const schedule of [...schedules, ...feedbackSchedules]) {
  const values = {
    Active__c: schedulerActive,
    BatchSize__c: 200,
    MSD_Connector__c: connector.id,
    MSD_Direction__c: schedule.direction,
    MSD_SourceType__c: schedule.sourceType,
    MSD_TargetType__c: schedule.targetType,
    SourceSystem__c: schedule.sourceSystem,
    TargetSystem__c: schedule.targetSystem,
    ObjectName__c: schedule.objectName,
    Operation__c: schedule.operation,
    MSD_SourceDefinition__c: JSON.stringify(schedule.sourceDefinition),
    MSD_TargetDefinition__c: JSON.stringify(schedule.targetDefinition),
    MSD_MappingDefinition__c: schedule.mappingDefinition
  };
  results.push({ requestedName: schedule.name, ...(await upsertByName("MSD_Schedule__c", schedule.name, values, { includeNameOnCreate: false })) });
}

console.log(JSON.stringify({
  targetOrg,
  connector,
  schedules: results
}, null, 2));
