/*
  Annaburger PROD1 - Outbound Staging Tables (Salesforce -> MSSQL)
  Creates staging tables referenced by outbound scheduler spec.
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.KHKAdressen_Staging', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.KHKAdressen_Staging (
    AccountNumber NVARCHAR(255) NOT NULL,
    SalesforceId NVARCHAR(18) NULL,
    Name NVARCHAR(255) NULL,
    Phone NVARCHAR(64) NULL,
    BillingStreet NVARCHAR(255) NULL,
    BillingPostalCode NVARCHAR(32) NULL,
    BillingCity NVARCHAR(128) NULL,
    BillingCountry NVARCHAR(64) NULL,
    LastModifiedDate DATETIME2(0) NOT NULL,
    LastModifiedByUsername NVARCHAR(255) NULL,
    ImportedAt DATETIME2(0) NOT NULL CONSTRAINT DF_KHKAdressen_Staging_ImportedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_KHKAdressen_Staging PRIMARY KEY (AccountNumber)
  );
END;

IF OBJECT_ID(N'dbo.KHKAnsprechpartner_Staging', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.KHKAnsprechpartner_Staging (
    SalesforceId NVARCHAR(18) NOT NULL,
    AccountId NVARCHAR(18) NULL,
    FirstName NVARCHAR(100) NULL,
    LastName NVARCHAR(100) NULL,
    Email NVARCHAR(255) NULL,
    Phone NVARCHAR(64) NULL,
    Title NVARCHAR(128) NULL,
    LastModifiedDate DATETIME2(0) NOT NULL,
    LastModifiedByUsername NVARCHAR(255) NULL,
    ImportedAt DATETIME2(0) NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_ImportedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_KHKAnsprechpartner_Staging PRIMARY KEY (SalesforceId)
  );
END;

IF OBJECT_ID(N'dbo.KHKVerkaufsprojekte_Staging', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.KHKVerkaufsprojekte_Staging (
    SalesforceId NVARCHAR(18) NOT NULL,
    Name NVARCHAR(255) NULL,
    AccountId NVARCHAR(18) NULL,
    CloseDate DATETIME2(0) NULL,
    Amount DECIMAL(18, 2) NULL,
    StageName NVARCHAR(80) NULL,
    LastModifiedDate DATETIME2(0) NOT NULL,
    LastModifiedByUsername NVARCHAR(255) NULL,
    ImportedAt DATETIME2(0) NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_ImportedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_KHKVerkaufsprojekte_Staging PRIMARY KEY (SalesforceId)
  );
END;

IF OBJECT_ID(N'dbo.KHKAngebote_Staging', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.KHKAngebote_Staging (
    QuoteNumber NVARCHAR(255) NOT NULL,
    SalesforceId NVARCHAR(18) NULL,
    OpportunityId NVARCHAR(18) NULL,
    AccountId NVARCHAR(18) NULL,
    Status NVARCHAR(80) NULL,
    ExpirationDate DATETIME2(0) NULL,
    GrandTotal DECIMAL(18, 2) NULL,
    LastModifiedDate DATETIME2(0) NOT NULL,
    LastModifiedByUsername NVARCHAR(255) NULL,
    ImportedAt DATETIME2(0) NOT NULL CONSTRAINT DF_KHKAngebote_Staging_ImportedAt DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_KHKAngebote_Staging PRIMARY KEY (QuoteNumber)
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_KHKAdressen_Staging_LastModifiedDate' AND object_id = OBJECT_ID(N'dbo.KHKAdressen_Staging'))
BEGIN
  CREATE INDEX IX_KHKAdressen_Staging_LastModifiedDate
    ON dbo.KHKAdressen_Staging (LastModifiedDate);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_KHKAnsprechpartner_Staging_LastModifiedDate' AND object_id = OBJECT_ID(N'dbo.KHKAnsprechpartner_Staging'))
BEGIN
  CREATE INDEX IX_KHKAnsprechpartner_Staging_LastModifiedDate
    ON dbo.KHKAnsprechpartner_Staging (LastModifiedDate);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_KHKVerkaufsprojekte_Staging_LastModifiedDate' AND object_id = OBJECT_ID(N'dbo.KHKVerkaufsprojekte_Staging'))
BEGIN
  CREATE INDEX IX_KHKVerkaufsprojekte_Staging_LastModifiedDate
    ON dbo.KHKVerkaufsprojekte_Staging (LastModifiedDate);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_KHKAngebote_Staging_LastModifiedDate' AND object_id = OBJECT_ID(N'dbo.KHKAngebote_Staging'))
BEGIN
  CREATE INDEX IX_KHKAngebote_Staging_LastModifiedDate
    ON dbo.KHKAngebote_Staging (LastModifiedDate);
END;
