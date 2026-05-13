/*
  Annaburger PROD1 - Outbound Staging Tables rollback
*/

SET NOCOUNT ON;

IF OBJECT_ID(N'dbo.KHKAngebote_Staging', N'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.KHKAngebote_Staging;
END;

IF OBJECT_ID(N'dbo.KHKVerkaufsprojekte_Staging', N'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.KHKVerkaufsprojekte_Staging;
END;

IF OBJECT_ID(N'dbo.KHKAnsprechpartner_Staging', N'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.KHKAnsprechpartner_Staging;
END;

IF OBJECT_ID(N'dbo.KHKAdressen_Staging', N'U') IS NOT NULL
BEGIN
  DROP TABLE dbo.KHKAdressen_Staging;
END;
