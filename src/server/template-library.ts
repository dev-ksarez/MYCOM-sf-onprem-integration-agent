export type TemplateKind = "connector" | "schedule" | "bundle";

export interface ConnectorTemplateDraft {
  name?: string;
  active?: boolean;
  connectorType?: string;
  targetSystem?: string;
  direction?: string;
  secretKey?: string;
  timeoutMs?: number;
  maxRetries?: number;
  description?: string;
  parameters?: Record<string, unknown>;
}

export interface ScheduleTemplateDraft {
  name?: string;
  active?: boolean;
  sourceSystem?: string;
  targetSystem?: string;
  objectName?: string;
  operation?: string;
  connectorId?: string;
  mappingDefinition?: string;
  direction?: string;
  sourceType?: string;
  targetType?: string;
  sourceDefinition?: string;
  targetDefinition?: string;
  batchSize?: number;
  timingDefinition?: string;
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
}

export interface TemplateBundleDraft {
  connector: ConnectorTemplateDraft;
  schedule: ScheduleTemplateDraft;
}

export interface TemplateDefinition {
  id: string;
  kind: TemplateKind;
  name: string;
  description?: string;
  scope: "system" | "custom";
  tags: string[];
  connector?: ConnectorTemplateDraft;
  schedule?: ScheduleTemplateDraft;
  bundle?: TemplateBundleDraft;
  createdAt?: string;
  updatedAt?: string;
}

export interface TemplateMutationInput {
  id?: string;
  kind: TemplateKind;
  name: string;
  description?: string;
  tags?: string[];
  connector?: ConnectorTemplateDraft;
  schedule?: ScheduleTemplateDraft;
  bundle?: TemplateBundleDraft;
}

const builtInTemplates: TemplateDefinition[] = [
  {
    id: "system-connector-ezb-rest",
    kind: "connector",
    name: "EZB REST Connector",
    description: "Vorkonfigurierter REST Connector fuer Wechselkurse und Zeitreihen der EZB.",
    scope: "system",
    tags: ["EZB", "REST", "Inbound"],
    connector: {
      name: "EZB REST API",
      active: true,
      connectorType: "REST_API",
      targetSystem: "EZB",
      direction: "Inbound",
      timeoutMs: 30000,
      maxRetries: 2,
      description: "Liest Daten von der European Central Bank Data API.",
      parameters: {
        baseUrl: "https://data-api.ecb.europa.eu",
        resourcePath: "/service/data/EXR",
        authType: "none",
        method: "GET",
        extraHeaders: {
          Accept: "application/json"
        }
      }
    }
  },
  {
    id: "system-connector-brevo-rest",
    kind: "connector",
    name: "Brevo Newsletter Connector",
    description: "REST Vorlage fuer Versand- und Kontaktprozesse mit Brevo.",
    scope: "system",
    tags: ["Brevo", "REST", "Outbound", "Newsletter"],
    connector: {
      name: "Brevo API",
      active: true,
      connectorType: "REST_API",
      targetSystem: "Brevo",
      direction: "Outbound",
      timeoutMs: 30000,
      maxRetries: 3,
      description: "Spricht die Brevo API fuer E-Mail und Kontaktprozesse an.",
      parameters: {
        baseUrl: "https://api.brevo.com/v3",
        resourcePath: "/smtp/email",
        authType: "api_key",
        apiKeyName: "api-key",
        apiKeyLocation: "header",
        method: "POST",
        extraHeaders: {
          "Content-Type": "application/json"
        }
      }
    }
  },
  {
    id: "system-schedule-ezb-rates",
    kind: "schedule",
    name: "EZB Wechselkurse nach Salesforce",
    description: "Inbound Scheduler fuer EZB Kursdaten mit Salesforce als Ziel.",
    scope: "system",
    tags: ["EZB", "Inbound", "Salesforce"],
    schedule: {
      name: "EZB Wechselkurse",
      active: true,
      sourceSystem: "EZB",
      targetSystem: "Salesforce",
      objectName: "EZB__c",
      operation: "Upsert",
      direction: "Inbound",
      sourceType: "REST_API",
      targetType: "SALESFORCE",
      batchSize: 50,
      sourceDefinition: JSON.stringify({
        endpoint: "/service/data/EXR/D.USD.EUR.SP00.A",
        responseType: "json",
        flattenMode: "exchangeRates",
        lastNObservations: 30
      }, null, 2),
      targetDefinition: JSON.stringify({
        objectApiName: "EZB__c",
        externalIdField: "External_Id__c"
      }, null, 2),
      mappingDefinition: [
        "External_Id__c;string=seriesKey;NONE",
        "Currency__c;string=currency;NONE",
        "Rate__c;number=rate;NONE",
        "Observed_On__c;date=observedOn;NONE"
      ].join("\n"),
      timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: "08:00" })
    }
  },
  {
    id: "system-schedule-brevo-newsletter",
    kind: "schedule",
    name: "Salesforce Kontakte nach Brevo",
    description: "Outbound Scheduler fuer Newsletter-Empfaenger nach Brevo.",
    scope: "system",
    tags: ["Brevo", "Outbound", "Newsletter", "Salesforce"],
    schedule: {
      name: "Brevo Newsletter Sync",
      active: true,
      sourceSystem: "Salesforce",
      targetSystem: "Brevo",
      objectName: "Contact",
      operation: "Upsert",
      direction: "Outbound",
      sourceType: "SALESFORCE",
      targetType: "REST_API",
      batchSize: 200,
      sourceDefinition: JSON.stringify({
        soql: "SELECT Id, Email, FirstName, LastName, LastModifiedDate FROM Contact WHERE Email != null"
      }, null, 2),
      targetDefinition: JSON.stringify({
        endpoint: "/contacts/import",
        method: "POST",
        operation: "upsert"
      }, null, 2),
      mappingDefinition: [
        "email;string=Email;NONE",
        "attributes.FIRSTNAME;string=FirstName;NONE",
        "attributes.LASTNAME;string=LastName;NONE",
        "ext_id;string=Id;NONE"
      ].join("\n"),
      timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 30, startTime: "09:00" })
    }
  },
  {
    id: "system-bundle-ezb-salesforce",
    kind: "bundle",
    name: "Komplettvorlage EZB nach Salesforce",
    description: "Legt EZB REST Connector und EZB Wechselkurs-Scheduler in einem Schritt an.",
    scope: "system",
    tags: ["EZB", "REST", "Inbound", "Bundle", "Salesforce"],
    bundle: {
      connector: {
        name: "EZB REST API",
        active: true,
        connectorType: "REST_API",
        targetSystem: "EZB",
        direction: "Inbound",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Liest Daten von der European Central Bank Data API.",
        parameters: {
          baseUrl: "https://data-api.ecb.europa.eu",
          resourcePath: "/service/data/EXR",
          authType: "none",
          method: "GET",
          extraHeaders: {
            Accept: "application/json"
          }
        }
      },
      schedule: {
        active: true,
        sourceSystem: "EZB",
        targetSystem: "Salesforce",
        objectName: "EZB__c",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "REST_API",
        targetType: "SALESFORCE",
        batchSize: 50,
        sourceDefinition: JSON.stringify({
          endpoint: "/service/data/EXR/D.USD.EUR.SP00.A",
          responseType: "json",
          flattenMode: "exchangeRates",
          lastNObservations: 30
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "EZB__c",
          externalIdField: "External_Id__c"
        }, null, 2),
        mappingDefinition: [
          "External_Id__c;string=seriesKey;NONE",
          "Currency__c;string=currency;NONE",
          "Rate__c;number=rate;NONE",
          "Observed_On__c;date=observedOn;NONE"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-brevo-newsletter",
    kind: "bundle",
    name: "Komplettvorlage Salesforce nach Brevo",
    description: "Legt Brevo Connector und Newsletter-Scheduler in einem Schritt an.",
    scope: "system",
    tags: ["Brevo", "REST", "Outbound", "Bundle", "Newsletter"],
    bundle: {
      connector: {
        name: "Brevo API",
        active: true,
        connectorType: "REST_API",
        targetSystem: "Brevo",
        direction: "Outbound",
        timeoutMs: 30000,
        maxRetries: 3,
        description: "Spricht die Brevo API fuer E-Mail und Kontaktprozesse an.",
        parameters: {
          baseUrl: "https://api.brevo.com/v3",
          resourcePath: "/smtp/email",
          authType: "api_key",
          apiKeyName: "api-key",
          apiKeyLocation: "header",
          method: "POST",
          extraHeaders: {
            "Content-Type": "application/json"
          }
        }
      },
      schedule: {
        active: true,
        sourceSystem: "Salesforce",
        targetSystem: "Brevo",
        objectName: "Contact",
        operation: "Upsert",
        direction: "Outbound",
        sourceType: "SALESFORCE",
        targetType: "REST_API",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          soql: "SELECT Id, Email, FirstName, LastName, LastModifiedDate FROM Contact WHERE Email != null"
        }, null, 2),
        targetDefinition: JSON.stringify({
          endpoint: "/contacts/import",
          method: "POST",
          operation: "upsert"
        }, null, 2),
        mappingDefinition: [
          "email;string=Email;NONE",
          "attributes.FIRSTNAME;string=FirstName;NONE",
          "attributes.LASTNAME;string=LastName;NONE",
          "ext_id;string=Id;NONE"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 30, startTime: "09:00" })
      }
    }
  },
  {
    id: "system-bundle-sage100-salesforce-account",
    kind: "bundle",
    name: "SAGE100 direkt per MSSQL nach Salesforce",
    description: "Direkte SAGE100-Anbindung per MSSQL mit KHKAdressen als Quelle fuer Salesforce Accounts.",
    scope: "system",
    tags: ["SAGE100", "Direkt", "MSSQL", "Inbound", "Bundle", "Salesforce", "Account"],
    bundle: {
      connector: {
        name: "SAGE100 Direkt-MSSQL Connector",
        active: true,
        connectorType: "MSSQL",
        targetSystem: "SAGE100 Direkt/MSSQL",
        direction: "Inbound",
        secretKey: "SAGE100_SQL_PASSWORD",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Basis-Connector fuer SAGE100 SQL-Tabellen ueber Microsoft SQL Server.",
        parameters: {
          server: "${SAGE100_SQL_SERVER}",
          port: 1433,
          database: "${SAGE100_SQL_DATABASE}",
          user: "${SAGE100_SQL_USER}",
          encrypt: false,
          trustServerCertificate: true
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAGE100",
        targetSystem: "Salesforce",
        objectName: "Account",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "MSSQL",
        targetType: "SALESFORCE",
        batchSize: 200,
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
          "FROM KHKAdressen"
        ].join("\n"),
        targetDefinition: JSON.stringify({
          objectApiName: "Account",
          externalIdField: "AccountNumber"
        }, null, 2),
        mappingDefinition: [
          "AccountNumber;string=ExternalKey;TRIM",
          "Name;string=AccountName;TRIM",
          "BillingStreet;string=BillingStreet;TRIM",
          "BillingPostalCode;string=BillingPostalCode;TRIM",
          "BillingCity;string=BillingCity;TRIM",
          "BillingCountry;string=BillingCountry;TRIM",
          "Phone;string=Phone;TRIM",
          "Website;string=Website;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 2, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-sage100-api-salesforce-account",
    kind: "bundle",
    name: "SAGE100 offizielle API nach Salesforce",
    description: "Anbindung ueber die offizielle Sage-100-Kunden-API via OpenID Connect und SData-Endpunkt fuer Salesforce Accounts.",
    scope: "system",
    tags: ["SAGE100", "API", "OpenID", "SData", "Inbound", "Bundle", "Salesforce", "Account"],
    bundle: {
      connector: {
        name: "SAGE100 Offizielle API Connector",
        active: true,
        connectorType: "REST_API",
        targetSystem: "SAGE100 Offizielle API",
        direction: "Inbound",
        secretKey: "SAGE100_API_TOKEN",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Basis-Connector fuer die offizielle Sage-100-Kunden-API. Nutzt Bearer Token aus Sage ID / OpenID Connect gegen den dokumentierten Connectivity-Endpunkt.",
        parameters: {
          baseUrl: "https://connectivity.sage.de/ws/1037474",
          resourcePath: "/sdata/ol/apiKunden.Sage.API/DEMO;100/eptKunden.Sage.API",
          authType: "bearer",
          method: "GET",
          extraHeaders: {
            Accept: "application/json"
          }
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAGE100",
        targetSystem: "Salesforce",
        objectName: "Account",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "REST_API",
        targetType: "SALESFORCE",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          endpoint: "/sdata/ol/apiKunden.Sage.API/DEMO;100/eptKunden.Sage.API",
          method: "GET",
          headers: {
            Accept: "application/json",
            Prefer: "compact",
            "X-Sage-ConnectivityVersion": "1.3"
          },
          query: {
            startIndex: 1,
            count: 200,
            select: "kto,name1,name2,lieferStrasse,lieferPLZ,lieferOrt,lieferLand"
          },
          resultPath: "$resources"
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "Account",
          externalIdField: "AccountNumber"
        }, null, 2),
        mappingDefinition: [
          "AccountNumber;string=kto;TRIM",
          "Name;string=name1;TRIM",
          "BillingStreet;string=lieferStrasse;TRIM",
          "BillingPostalCode;string=lieferPLZ;TRIM",
          "BillingCity;string=lieferOrt;TRIM",
          "BillingCountry;string=lieferLand;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 5, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-sap-file-salesforce-account",
    kind: "bundle",
    name: "SAP Datei/CSV nach Salesforce",
    description: "Generische SAP-Dateiimport-Vorlage fuer CSV-Exporte aus SAP oder Middleware nach Salesforce Accounts.",
    scope: "system",
    tags: ["SAP", "Datei", "CSV", "Inbound", "Bundle", "Salesforce", "Account"],
    bundle: {
      connector: {
        name: "SAP Datei Connector",
        active: true,
        connectorType: "FILE",
        targetSystem: "SAP Datei/CSV",
        direction: "Inbound",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Liest SAP-CSV-Dateien aus einem Importverzeichnis und archiviert sie nach dem Einlesen.",
        parameters: {
          basePath: "artifacts/files",
          importPath: "sap/inbound",
          exportPath: "sap/outbound",
          archivePath: "sap/archive",
          defaultCharset: "utf8",
          defaultDelimiter: ";",
          archiveOnRead: true,
          archiveOnWrite: false
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAP",
        targetSystem: "Salesforce",
        objectName: "Account",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "FILE_CSV",
        targetType: "SALESFORCE",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          fileName: "sap-business-partner.csv",
          format: "csv",
          delimiter: ";",
          hasHeader: true
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "Account",
          externalIdField: "AccountNumber"
        }, null, 2),
        mappingDefinition: [
          "AccountNumber;string=BusinessPartner;TRIM",
          "Name;string=OrganizationBPName1;TRIM",
          "BillingStreet;string=StreetName;TRIM",
          "BillingPostalCode;string=PostalCode;TRIM",
          "BillingCity;string=CityName;TRIM",
          "BillingCountry;string=Country;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 15, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-sap-api-salesforce-account",
    kind: "bundle",
    name: "SAP OData/API nach Salesforce",
    description: "Generische SAP-API-Vorlage fuer OData oder REST-Endpunkte mit Business-Partner-Daten nach Salesforce Accounts.",
    scope: "system",
    tags: ["SAP", "API", "OData", "REST", "Inbound", "Bundle", "Salesforce", "Account"],
    bundle: {
      connector: {
        name: "SAP OData API Connector",
        active: true,
        connectorType: "REST_API",
        targetSystem: "SAP OData/API",
        direction: "Inbound",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Generischer SAP-API-Connector fuer OData- oder REST-Endpunkte. OAuth2 client_credentials ist vorkonfiguriert, Result-Path und Feldauswahl muessen je SAP-System verifiziert werden.",
        parameters: {
          baseUrl: "https://sap.example.com",
          resourcePath: "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner",
          authType: "oauth2",
          tokenUrl: "https://sap.example.com/oauth/token",
          grantType: "client_credentials",
          clientIdEnv: "SAP_API_CLIENT_ID",
          clientSecretEnv: "SAP_API_CLIENT_SECRET",
          scope: "BusinessPartner.Read",
          method: "GET",
          extraHeaders: {
            Accept: "application/json"
          }
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAP",
        targetSystem: "Salesforce",
        objectName: "Account",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "REST_API",
        targetType: "SALESFORCE",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          endpoint: "/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner",
          method: "GET",
          headers: {
            Accept: "application/json"
          },
          query: {
            "$top": 200,
            "$select": "BusinessPartner,OrganizationBPName1,StreetName,PostalCode,CityName,Country"
          },
          resultPath: "value"
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "Account",
          externalIdField: "AccountNumber"
        }, null, 2),
        mappingDefinition: [
          "AccountNumber;string=BusinessPartner;TRIM",
          "Name;string=OrganizationBPName1;TRIM",
          "BillingStreet;string=StreetName;TRIM",
          "BillingPostalCode;string=PostalCode;TRIM",
          "BillingCity;string=CityName;TRIM",
          "BillingCountry;string=Country;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 15, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-sap-vendor-file-salesforce-account",
    kind: "bundle",
    name: "SAP Kreditor Datei/CSV nach Salesforce",
    description: "Generische SAP-Kreditor-Vorlage fuer CSV-Exporte aus SAP oder Middleware nach Salesforce Accounts.",
    scope: "system",
    tags: ["SAP", "Kreditor", "Lieferant", "Datei", "CSV", "Inbound", "Bundle", "Salesforce", "Account"],
    bundle: {
      connector: {
        name: "SAP Kreditor Datei Connector",
        active: true,
        connectorType: "FILE",
        targetSystem: "SAP Kreditor Datei/CSV",
        direction: "Inbound",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Liest SAP-Kreditoren aus CSV-Dateien und archiviert sie nach dem Einlesen.",
        parameters: {
          basePath: "artifacts/files",
          importPath: "sap-vendor/inbound",
          exportPath: "sap-vendor/outbound",
          archivePath: "sap-vendor/archive",
          defaultCharset: "utf8",
          defaultDelimiter: ";",
          archiveOnRead: true,
          archiveOnWrite: false
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAP",
        targetSystem: "Salesforce",
        objectName: "Account",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "FILE_CSV",
        targetType: "SALESFORCE",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          fileName: "sap-supplier.csv",
          format: "csv",
          delimiter: ";",
          hasHeader: true
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "Account",
          externalIdField: "AccountNumber"
        }, null, 2),
        mappingDefinition: [
          "AccountNumber;string=Supplier;TRIM",
          "Name;string=SupplierName;TRIM",
          "BillingStreet;string=StreetName;TRIM",
          "BillingPostalCode;string=PostalCode;TRIM",
          "BillingCity;string=CityName;TRIM",
          "BillingCountry;string=Country;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 15, startTime: "08:00" })
      }
    }
  },
  {
    id: "system-bundle-sap-contact-file-salesforce-contact",
    kind: "bundle",
    name: "SAP Ansprechpartner Datei/CSV nach Salesforce",
    description: "Generische SAP-Ansprechpartner-Vorlage fuer CSV-Exporte aus SAP oder Middleware nach Salesforce Contacts mit Account-Zuordnung ueber AccountNumber.",
    scope: "system",
    tags: ["SAP", "Ansprechpartner", "Contact", "Datei", "CSV", "Inbound", "Bundle", "Salesforce"],
    bundle: {
      connector: {
        name: "SAP Ansprechpartner Datei Connector",
        active: true,
        connectorType: "FILE",
        targetSystem: "SAP Ansprechpartner Datei/CSV",
        direction: "Inbound",
        timeoutMs: 30000,
        maxRetries: 2,
        description: "Liest SAP-Ansprechpartner aus CSV-Dateien und verknuepft sie ueber Account.AccountNumber mit bestehenden Salesforce Accounts.",
        parameters: {
          basePath: "artifacts/files",
          importPath: "sap-contact/inbound",
          exportPath: "sap-contact/outbound",
          archivePath: "sap-contact/archive",
          defaultCharset: "utf8",
          defaultDelimiter: ";",
          archiveOnRead: true,
          archiveOnWrite: false
        }
      },
      schedule: {
        active: true,
        sourceSystem: "SAP",
        targetSystem: "Salesforce",
        objectName: "Contact",
        operation: "Upsert",
        direction: "Inbound",
        sourceType: "FILE_CSV",
        targetType: "SALESFORCE",
        batchSize: 200,
        sourceDefinition: JSON.stringify({
          fileName: "sap-contact.csv",
          format: "csv",
          delimiter: ";",
          hasHeader: true
        }, null, 2),
        targetDefinition: JSON.stringify({
          objectApiName: "Contact",
          externalIdField: "Email"
        }, null, 2),
        mappingDefinition: [
          "Account.AccountNumber;string=AccountReference;TRIM",
          "Email;string=Email;LOWERCASE",
          "FirstName;string=FirstName;TRIM",
          "LastName;string=LastName;TRIM",
          "Phone;string=Phone;TRIM",
          "MobilePhone;string=MobilePhone;TRIM"
        ].join("\n"),
        timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 15, startTime: "08:00" })
      }
    }
  }
];

export function listBuiltInTemplates(): TemplateDefinition[] {
  return JSON.parse(JSON.stringify(builtInTemplates)) as TemplateDefinition[];
}