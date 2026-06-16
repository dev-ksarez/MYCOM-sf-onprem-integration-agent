/*
  Annaburger PROD1 - Salesforce -> SAGE100 MSSQL staging tables.

  Grundlage:
    Live-Abfrage AnnaburgerPROD1 am 2026-06-16.
    Relevante Outbound-Scheduler: SCH-0033 Account, SCH-0034 Contact,
    SCH-0035 Opportunity, SCH-0036 Quote.
    Account-Oberflaechenfelder geprueft ueber PROD Layout "Account Layout".

  Die Spalten der Account-, Contact- und Quote-Tabellen orientieren sich an den
  dort hinterlegten Mapping-Zielen. QuoteLineItems sind in diesem Artefakt noch
  nicht als Outbound-Scheduler vorhanden; die Positions-Tabelle ist daher auf
  die ueblichen Salesforce QuoteLineItem-Felder vorbereitet.
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'sf') IS NULL
BEGIN
  EXEC(N'CREATE SCHEMA sf');
END
GO

CREATE TABLE sf.KHKAdressen_Staging (
  Id nvarchar(18) NOT NULL,
  AccountNumber nvarchar(50) NOT NULL,
  Name nvarchar(255) NULL,
  AccountName2__c nvarchar(100) NULL,
  Type nvarchar(255) NULL,
  Type__c nvarchar(255) NULL,
  Description nvarchar(max) NULL,
  Phone nvarchar(40) NULL,
  Mobile__c nvarchar(40) NULL,
  General_Email__c nvarchar(80) NULL,
  Website nvarchar(255) NULL,
  Industry nvarchar(255) NULL,
  NumberOfEmployees int NULL,
  BillingStreet nvarchar(255) NULL,
  BillingPostalCode nvarchar(20) NULL,
  BillingCity nvarchar(40) NULL,
  BillingCountry nvarchar(80) NULL,
  BillingCountryCode nvarchar(10) NULL,
  ShippingStreet nvarchar(255) NULL,
  ShippingPostalCode nvarchar(20) NULL,
  ShippingCity nvarchar(40) NULL,
  ShippingCountry nvarchar(80) NULL,
  ShippingCountryCode nvarchar(10) NULL,
  ERP_Address_Number__c nvarchar(20) NULL,
  ERP_Account_Number__c nvarchar(20) NULL,
  Customer_MatchCode__c nvarchar(100) NULL,
  Customer_Group__c nvarchar(255) NULL,
  VAT_ID__c nvarchar(20) NULL,
  Taxation_Type__c nvarchar(255) NULL,
  Price_list__c nvarchar(255) NULL,
  Last_Dunning_Date__c date NULL,
  last_sale_Date__c date NULL,
  Ranking__c nvarchar(255) NULL,
  InterestedIn__c nvarchar(max) NULL,
  InterestedIn_Other_c__c nvarchar(255) NULL,
  DealerGroup__c nvarchar(255) NULL,
  Dealer__c nvarchar(18) NULL,
  ERP_Representative__c nvarchar(18) NULL,
  Bill_to_Account__c nvarchar(18) NULL,
  ParentId nvarchar(18) NULL,
  OwnerId nvarchar(18) NULL,
  is_Active__c bit NULL,
  LastModifiedDate datetime2(0) NULL,
  LastModifiedByUsername nvarchar(255) NULL,
  SAGEAddressId int NULL,
  SAGECustomerNo nvarchar(20) NULL,
  PostFlag bit NOT NULL CONSTRAINT DF_KHKAdressen_Staging_PostFlag DEFAULT (1),
  PostStatus nvarchar(30) NOT NULL CONSTRAINT DF_KHKAdressen_Staging_PostStatus DEFAULT (N'OPEN'),
  PostMessage nvarchar(max) NULL,
  PostTryCount int NOT NULL CONSTRAINT DF_KHKAdressen_Staging_PostTryCount DEFAULT (0),
  LastPostAttemptAt datetime2(0) NULL,
  PostedAt datetime2(0) NULL,
  CreatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAdressen_Staging_CreatedAt DEFAULT (sysdatetime()),
  UpdatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAdressen_Staging_UpdatedAt DEFAULT (sysdatetime()),
  CONSTRAINT PK_KHKAdressen_Staging PRIMARY KEY (AccountNumber)
);
GO

CREATE UNIQUE INDEX UX_KHKAdressen_Staging_Id
  ON sf.KHKAdressen_Staging (Id);

CREATE INDEX IX_KHKAdressen_Staging_ERP_Address_Number
  ON sf.KHKAdressen_Staging (ERP_Address_Number__c);

CREATE INDEX IX_KHKAdressen_Staging_ERP_Account_Number
  ON sf.KHKAdressen_Staging (ERP_Account_Number__c);

CREATE INDEX IX_KHKAdressen_Staging_PostQueue
  ON sf.KHKAdressen_Staging (PostFlag, PostStatus, UpdatedAt)
  INCLUDE (SAGEAddressId, SAGECustomerNo, ERP_Address_Number__c, ERP_Account_Number__c);
GO

CREATE TABLE sf.KHKAnsprechpartner_Staging (
  Id nvarchar(18) NOT NULL,
  AccountId nvarchar(18) NULL,
  FirstName nvarchar(80) NULL,
  LastName nvarchar(80) NULL,
  Email nvarchar(255) NULL,
  Phone nvarchar(40) NULL,
  Title nvarchar(80) NULL,
  LastModifiedDate datetime2(0) NULL,
  LastModifiedByUsername nvarchar(255) NULL,
  SAGEContactId int NULL,
  SAGEAddressId int NULL,
  SAGECustomerNo nvarchar(20) NULL,
  PostFlag bit NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_PostFlag DEFAULT (1),
  PostStatus nvarchar(30) NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_PostStatus DEFAULT (N'OPEN'),
  PostMessage nvarchar(max) NULL,
  PostTryCount int NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_PostTryCount DEFAULT (0),
  LastPostAttemptAt datetime2(0) NULL,
  PostedAt datetime2(0) NULL,
  CreatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_CreatedAt DEFAULT (sysdatetime()),
  UpdatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Staging_UpdatedAt DEFAULT (sysdatetime()),
  CONSTRAINT PK_KHKAnsprechpartner_Staging PRIMARY KEY (Id)
);
GO

CREATE INDEX IX_KHKAnsprechpartner_Staging_AccountId
  ON sf.KHKAnsprechpartner_Staging (AccountId);

CREATE INDEX IX_KHKAnsprechpartner_Staging_PostQueue
  ON sf.KHKAnsprechpartner_Staging (PostFlag, PostStatus, UpdatedAt)
  INCLUDE (AccountId, SAGEContactId, SAGEAddressId, SAGECustomerNo);
GO

CREATE TABLE sf.KHKVerkaufsprojekte_Staging (
  Id nvarchar(18) NOT NULL,
  Name nvarchar(255) NULL,
  AccountId nvarchar(18) NULL,
  CloseDate date NULL,
  Amount decimal(19,4) NULL,
  StageName nvarchar(255) NULL,
  LastModifiedDate datetime2(0) NULL,
  LastModifiedByUsername nvarchar(255) NULL,
  SAGEOpportunityId int NULL,
  SAGEProjectNo nvarchar(50) NULL,
  PostFlag bit NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_PostFlag DEFAULT (1),
  PostStatus nvarchar(30) NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_PostStatus DEFAULT (N'OPEN'),
  PostMessage nvarchar(max) NULL,
  PostTryCount int NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_PostTryCount DEFAULT (0),
  LastPostAttemptAt datetime2(0) NULL,
  PostedAt datetime2(0) NULL,
  CreatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_CreatedAt DEFAULT (sysdatetime()),
  UpdatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKVerkaufsprojekte_Staging_UpdatedAt DEFAULT (sysdatetime()),
  CONSTRAINT PK_KHKVerkaufsprojekte_Staging PRIMARY KEY (Id)
);
GO

CREATE INDEX IX_KHKVerkaufsprojekte_Staging_AccountId
  ON sf.KHKVerkaufsprojekte_Staging (AccountId);

CREATE INDEX IX_KHKVerkaufsprojekte_Staging_PostQueue
  ON sf.KHKVerkaufsprojekte_Staging (PostFlag, PostStatus, UpdatedAt)
  INCLUDE (AccountId, SAGEOpportunityId, SAGEProjectNo);
GO

CREATE TABLE sf.KHKAngebote_Staging (
  Id nvarchar(18) NOT NULL,
  QuoteNumber nvarchar(30) NOT NULL,
  OpportunityId nvarchar(18) NULL,
  AccountId nvarchar(18) NULL,
  Status nvarchar(80) NULL,
  ExpirationDate date NULL,
  GrandTotal decimal(19,4) NULL,
  LastModifiedDate datetime2(0) NULL,
  LastModifiedByUsername nvarchar(255) NULL,
  SAGEQuoteId int NULL,
  SAGEQuoteNo nvarchar(30) NULL,
  PostFlag bit NOT NULL CONSTRAINT DF_KHKAngebote_Staging_PostFlag DEFAULT (1),
  PostStatus nvarchar(30) NOT NULL CONSTRAINT DF_KHKAngebote_Staging_PostStatus DEFAULT (N'OPEN'),
  PostMessage nvarchar(max) NULL,
  PostTryCount int NOT NULL CONSTRAINT DF_KHKAngebote_Staging_PostTryCount DEFAULT (0),
  LastPostAttemptAt datetime2(0) NULL,
  PostedAt datetime2(0) NULL,
  CreatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAngebote_Staging_CreatedAt DEFAULT (sysdatetime()),
  UpdatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAngebote_Staging_UpdatedAt DEFAULT (sysdatetime()),
  CONSTRAINT PK_KHKAngebote_Staging PRIMARY KEY (QuoteNumber)
);
GO

CREATE UNIQUE INDEX UX_KHKAngebote_Staging_Id
  ON sf.KHKAngebote_Staging (Id);

CREATE INDEX IX_KHKAngebote_Staging_AccountId
  ON sf.KHKAngebote_Staging (AccountId);

CREATE INDEX IX_KHKAngebote_Staging_OpportunityId
  ON sf.KHKAngebote_Staging (OpportunityId);

CREATE INDEX IX_KHKAngebote_Staging_PostQueue
  ON sf.KHKAngebote_Staging (PostFlag, PostStatus, UpdatedAt)
  INCLUDE (OpportunityId, SAGEQuoteId, SAGEQuoteNo);
GO

CREATE TABLE sf.KHKAngebotePositionen_Staging (
  Id nvarchar(18) NOT NULL,
  QuoteId nvarchar(18) NOT NULL,
  QuoteNumber nvarchar(30) NULL,
  PricebookEntryId nvarchar(18) NULL,
  Product2Id nvarchar(18) NULL,
  ProductCode nvarchar(255) NULL,
  Description nvarchar(max) NULL,
  Quantity decimal(19,4) NULL,
  UnitPrice decimal(19,4) NULL,
  ListPrice decimal(19,4) NULL,
  Discount decimal(9,4) NULL,
  TotalPrice decimal(19,4) NULL,
  LastModifiedDate datetime2(0) NULL,
  LastModifiedByUsername nvarchar(255) NULL,
  SAGEQuoteId int NULL,
  SAGEQuoteItemId int NULL,
  SAGEArticleNo nvarchar(31) NULL,
  PostFlag bit NOT NULL CONSTRAINT DF_KHKAngebotePositionen_Staging_PostFlag DEFAULT (1),
  PostStatus nvarchar(30) NOT NULL CONSTRAINT DF_KHKAngebotePositionen_Staging_PostStatus DEFAULT (N'OPEN'),
  PostMessage nvarchar(max) NULL,
  PostTryCount int NOT NULL CONSTRAINT DF_KHKAngebotePositionen_Staging_PostTryCount DEFAULT (0),
  LastPostAttemptAt datetime2(0) NULL,
  PostedAt datetime2(0) NULL,
  CreatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAngebotePositionen_Staging_CreatedAt DEFAULT (sysdatetime()),
  UpdatedAt datetime2(0) NOT NULL CONSTRAINT DF_KHKAngebotePositionen_Staging_UpdatedAt DEFAULT (sysdatetime()),
  CONSTRAINT PK_KHKAngebotePositionen_Staging PRIMARY KEY (Id)
);
GO

CREATE INDEX IX_KHKAngebotePositionen_Staging_QuoteId
  ON sf.KHKAngebotePositionen_Staging (QuoteId);

CREATE INDEX IX_KHKAngebotePositionen_Staging_QuoteNumber
  ON sf.KHKAngebotePositionen_Staging (QuoteNumber);

CREATE INDEX IX_KHKAngebotePositionen_Staging_PostQueue
  ON sf.KHKAngebotePositionen_Staging (PostFlag, PostStatus, UpdatedAt)
  INCLUDE (QuoteId, QuoteNumber, SAGEQuoteId, SAGEQuoteItemId, SAGEArticleNo);
GO

CREATE TRIGGER sf.TR_KHKAdressen_Staging_SetUpdatedAt
ON sf.KHKAdressen_Staging
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  IF UPDATE(UpdatedAt) RETURN;

  UPDATE target
     SET UpdatedAt = sysdatetime()
    FROM sf.KHKAdressen_Staging target
    JOIN inserted source ON source.AccountNumber = target.AccountNumber;
END;
GO

CREATE TRIGGER sf.TR_KHKAnsprechpartner_Staging_SetUpdatedAt
ON sf.KHKAnsprechpartner_Staging
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  IF UPDATE(UpdatedAt) RETURN;

  UPDATE target
     SET UpdatedAt = sysdatetime()
    FROM sf.KHKAnsprechpartner_Staging target
    JOIN inserted source ON source.Id = target.Id;
END;
GO

CREATE TRIGGER sf.TR_KHKVerkaufsprojekte_Staging_SetUpdatedAt
ON sf.KHKVerkaufsprojekte_Staging
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  IF UPDATE(UpdatedAt) RETURN;

  UPDATE target
     SET UpdatedAt = sysdatetime()
    FROM sf.KHKVerkaufsprojekte_Staging target
    JOIN inserted source ON source.Id = target.Id;
END;
GO

CREATE TRIGGER sf.TR_KHKAngebote_Staging_SetUpdatedAt
ON sf.KHKAngebote_Staging
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  IF UPDATE(UpdatedAt) RETURN;

  UPDATE target
     SET UpdatedAt = sysdatetime()
    FROM sf.KHKAngebote_Staging target
    JOIN inserted source ON source.QuoteNumber = target.QuoteNumber;
END;
GO

CREATE TRIGGER sf.TR_KHKAngebotePositionen_Staging_SetUpdatedAt
ON sf.KHKAngebotePositionen_Staging
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;
  IF UPDATE(UpdatedAt) RETURN;

  UPDATE target
     SET UpdatedAt = sysdatetime()
    FROM sf.KHKAngebotePositionen_Staging target
    JOIN inserted source ON source.Id = target.Id;
END;
GO
