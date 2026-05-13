SET NOCOUNT ON;

IF DB_ID(N'MySmartData') IS NULL
BEGIN
  CREATE DATABASE MySmartData;
END;
GO

USE MySmartData;
GO

IF OBJECT_ID(N'dbo.KHKProdukt', N'V') IS NOT NULL DROP VIEW dbo.KHKProdukt;
IF OBJECT_ID(N'dbo.KHKProdukt', N'U') IS NOT NULL DROP TABLE dbo.KHKProdukt;

IF OBJECT_ID(N'dbo.KHKProduktTexte', N'V') IS NOT NULL DROP VIEW dbo.KHKProduktTexte;
IF OBJECT_ID(N'dbo.KHKProduktTexte', N'U') IS NOT NULL DROP TABLE dbo.KHKProduktTexte;

IF OBJECT_ID(N'dbo.KHKArtikelRelations', N'V') IS NOT NULL DROP VIEW dbo.KHKArtikelRelations;
IF OBJECT_ID(N'dbo.KHKArtikelRelations', N'U') IS NOT NULL DROP TABLE dbo.KHKArtikelRelations;

IF OBJECT_ID(N'dbo.KHKBelege', N'V') IS NOT NULL DROP VIEW dbo.KHKBelege;
IF OBJECT_ID(N'dbo.KHKBelege', N'U') IS NOT NULL DROP TABLE dbo.KHKBelege;

IF OBJECT_ID(N'dbo.KHKAngebote', N'V') IS NOT NULL DROP VIEW dbo.KHKAngebote;
IF OBJECT_ID(N'dbo.KHKAngebote', N'U') IS NOT NULL DROP TABLE dbo.KHKAngebote;
GO

DROP TABLE IF EXISTS dbo.KHKVerkausprojekte;
DROP TABLE IF EXISTS dbo.KHKArchivEKBelege;
DROP TABLE IF EXISTS dbo.KHKArchivVKBelege;
DROP TABLE IF EXISTS dbo.KHKArtikelZubehoer;
DROP TABLE IF EXISTS dbo.KHKKontokorrent;
DROP TABLE IF EXISTS dbo.KHKArtikelgruppen;
DROP TABLE IF EXISTS dbo.KHKArtikelBezeichnung;
DROP TABLE IF EXISTS dbo.KHKArtikel;
DROP TABLE IF EXISTS dbo.KHKAnsprechpartner;
DROP TABLE IF EXISTS dbo.KHKAdressen;
GO

CREATE TABLE dbo.KHKAdressen (
  Adresse NVARCHAR(40) NOT NULL PRIMARY KEY,
  Matchcode NVARCHAR(120) NULL,
  Name1 NVARCHAR(255) NOT NULL,
  Name2 NVARCHAR(255) NULL,
  LieferStrasse NVARCHAR(255) NULL,
  LieferPLZ NVARCHAR(20) NULL,
  LieferOrt NVARCHAR(120) NULL,
  LieferLand NVARCHAR(80) NULL,
  Telefon NVARCHAR(64) NULL,
  EMail NVARCHAR(255) NULL,
  Sprache NVARCHAR(10) NULL,
  Gruppe NVARCHAR(40) NULL,
  Referenz NVARCHAR(80) NULL,
  Aktiv SMALLINT NOT NULL
);

CREATE TABLE dbo.KHKAnsprechpartner (
  Nummer NVARCHAR(40) NOT NULL PRIMARY KEY,
  Adresse NVARCHAR(40) NOT NULL,
  Ansprechpartner NVARCHAR(160) NULL,
  Titel NVARCHAR(120) NULL,
  Vorname NVARCHAR(100) NULL,
  Nachname NVARCHAR(100) NULL,
  Position NVARCHAR(120) NULL,
  Abteilung NVARCHAR(120) NULL,
  Telefon NVARCHAR(64) NULL,
  EMail NVARCHAR(255) NULL,
  Gruppe NVARCHAR(40) NULL
);

CREATE TABLE dbo.KHKArtikel (
  Artikelnummer NVARCHAR(60) NOT NULL PRIMARY KEY,
  Bezeichnung1 NVARCHAR(255) NOT NULL,
  Bezeichnung2 NVARCHAR(255) NULL,
  Langtext NVARCHAR(MAX) NULL,
  Artikelgruppe NVARCHAR(40) NULL,
  Hauptartikelgruppe NVARCHAR(40) NULL,
  Vaterartikelgruppe NVARCHAR(40) NULL,
  Aktiv SMALLINT NOT NULL
);

CREATE TABLE dbo.KHKArtikelBezeichnung (
  Artikelnummer NVARCHAR(60) NOT NULL,
  Sprache NVARCHAR(10) NOT NULL,
  Bezeichnung1 NVARCHAR(255) NULL,
  Bezeichnung2 NVARCHAR(255) NULL,
  Dimensionstext NVARCHAR(255) NULL,
  Langtext NVARCHAR(MAX) NULL,
  CONSTRAINT PK_KHKArtikelBezeichnung PRIMARY KEY (Artikelnummer, Sprache)
);

CREATE TABLE dbo.KHKArtikelgruppen (
  Hauptartikelgruppe NVARCHAR(40) NOT NULL,
  Artikelgruppe NVARCHAR(40) NOT NULL,
  VaterArtikelgruppe NVARCHAR(40) NULL,
  Bezeichnung NVARCHAR(255) NOT NULL,
  CONSTRAINT PK_KHKArtikelgruppen PRIMARY KEY (Hauptartikelgruppe, Artikelgruppe)
);

CREATE TABLE dbo.KHKKontokorrent (
  Kto NVARCHAR(60) NOT NULL PRIMARY KEY,
  KtoArt NVARCHAR(10) NOT NULL,
  Matchcode NVARCHAR(120) NULL,
  Adresse NVARCHAR(40) NOT NULL,
  WKz NVARCHAR(10) NULL,
  Zahlungskond NVARCHAR(80) NULL,
  Nettobedingung NVARCHAR(80) NULL,
  Gruppe NVARCHAR(40) NULL,
  Kreditlimit DECIMAL(18,2) NOT NULL
);

CREATE TABLE dbo.KHKArtikelZubehoer (
  Ursprungsnummer NVARCHAR(60) NOT NULL,
  Zubehoernummer NVARCHAR(60) NOT NULL,
  UrsprungsArt NVARCHAR(40) NULL,
  ZubehoerArt NVARCHAR(40) NULL,
  Automatisch SMALLINT NOT NULL,
  Mengenberechnung NVARCHAR(40) NULL,
  MengenFaktor DECIMAL(18,6) NOT NULL,
  Basismengeneinheit NVARCHAR(20) NULL,
  CONSTRAINT PK_KHKArtikelZubehoer PRIMARY KEY (Ursprungsnummer, Zubehoernummer)
);

CREATE TABLE dbo.KHKArchivVKBelege (
  AenderungsNr INT NOT NULL,
  BelID NVARCHAR(40) NOT NULL,
  Belegkennzeichen NVARCHAR(20) NULL,
  Belegart NVARCHAR(40) NULL,
  Belegjahr INT NOT NULL,
  Belegnummer NVARCHAR(60) NOT NULL,
  Matchcode NVARCHAR(120) NULL,
  Belegstatus NVARCHAR(80) NULL,
  A0Empfaenger NVARCHAR(255) NULL,
  A0AdressNr NVARCHAR(40) NULL,
  Belegdatum DATETIME2(0) NULL,
  CONSTRAINT PK_KHKArchivVKBelege PRIMARY KEY (BelID)
);

CREATE TABLE dbo.KHKArchivEKBelege (
  AenderungsNr INT NOT NULL,
  BelID NVARCHAR(40) NOT NULL,
  Belegkennzeichen NVARCHAR(20) NULL,
  Belegart NVARCHAR(40) NULL,
  Belegjahr INT NOT NULL,
  Belegnummer NVARCHAR(60) NOT NULL,
  Belegdatum DATETIME2(0) NULL,
  A0Empfaenger NVARCHAR(255) NULL,
  A4Name1 NVARCHAR(255) NULL,
  A4Ort NVARCHAR(120) NULL,
  CONSTRAINT PK_KHKArchivEKBelege PRIMARY KEY (BelID)
);

CREATE TABLE dbo.KHKVerkausprojekte (
  Projektname NVARCHAR(255) NOT NULL,
  Projektdatum DATETIME2(0) NOT NULL,
  Wert DECIMAL(18,2) NOT NULL,
  Projektstatus NVARCHAR(80) NOT NULL,
  Kundennummer NVARCHAR(40) NOT NULL,
  CONSTRAINT PK_KHKVerkausprojekte PRIMARY KEY (Projektname, Kundennummer)
);
GO

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKAdressen (Adresse, Matchcode, Name1, Name2, LieferStrasse, LieferPLZ, LieferOrt, LieferLand, Telefon, EMail, Sprache, Gruppe, Referenz, Aktiv)
SELECT
  CONCAT('KD', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('MATCH-', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('Musterfirma ', n),
  CONCAT('Filiale ', ((n - 1) % 10) + 1),
  CONCAT('Lieferstrasse ', n),
  CONCAT('10', RIGHT('000' + CAST(n AS VARCHAR(3)), 3)),
  CONCAT('Ort-', ((n - 1) % 25) + 1),
  'DE',
  CONCAT('+49-30-', RIGHT('000000' + CAST(100000 + n AS VARCHAR(6)), 6)),
  CONCAT('kunde', n, '@example.org'),
  CASE WHEN n % 3 = 0 THEN 'EN' ELSE 'DE' END,
  CONCAT('GRP', RIGHT('00' + CAST(((n - 1) % 20) + 1 AS VARCHAR(3)), 3)),
  CONCAT('REF-', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  -1
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKAnsprechpartner (Nummer, Adresse, Ansprechpartner, Titel, Vorname, Nachname, Position, Abteilung, Telefon, EMail, Gruppe)
SELECT
  CONCAT('AP', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('KD', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('Ansprechpartner ', n),
  CASE WHEN n % 2 = 0 THEN 'Herr' ELSE 'Frau' END,
  CONCAT('Vorname', n),
  CONCAT('Nachname', n),
  CASE WHEN n % 3 = 0 THEN 'Leitung' ELSE 'Sachbearbeitung' END,
  CASE WHEN n % 4 = 0 THEN 'Vertrieb' ELSE 'Service' END,
  CONCAT('+49-40-', RIGHT('000000' + CAST(200000 + n AS VARCHAR(6)), 6)),
  CONCAT('kontakt', n, '@example.org'),
  CONCAT('GRP', RIGHT('00' + CAST(((n - 1) % 20) + 1 AS VARCHAR(3)), 3))
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArtikelgruppen (Hauptartikelgruppe, Artikelgruppe, VaterArtikelgruppe, Bezeichnung)
SELECT
  CONCAT('HGRP', RIGHT('00' + CAST(((n - 1) % 10) + 1 AS VARCHAR(3)), 3)),
  CONCAT('GRP', RIGHT('000' + CAST(n AS VARCHAR(3)), 3)),
  CONCAT('VGRP', RIGHT('00' + CAST(((n - 1) % 5) + 1 AS VARCHAR(3)), 3)),
  CONCAT('Artikelgruppe ', n)
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArtikel (Artikelnummer, Bezeichnung1, Bezeichnung2, Langtext, Artikelgruppe, Hauptartikelgruppe, Vaterartikelgruppe, Aktiv)
SELECT
  CONCAT('ART', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('Artikel ', n),
  CONCAT('Variante ', ((n - 1) % 6) + 1),
  CONCAT('Langtext fuer Artikel ', n, ' (SAGE100-Demo)'),
  CONCAT('GRP', RIGHT('00' + CAST(((n - 1) % 20) + 1 AS VARCHAR(3)), 3)),
  CONCAT('HGRP', RIGHT('00' + CAST(((n - 1) % 10) + 1 AS VARCHAR(3)), 3)),
  CONCAT('VGRP', RIGHT('00' + CAST(((n - 1) % 5) + 1 AS VARCHAR(3)), 3)),
  -1
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArtikelBezeichnung (Artikelnummer, Sprache, Bezeichnung1, Bezeichnung2, Dimensionstext, Langtext)
SELECT
  CONCAT('ART', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CASE WHEN n % 3 = 0 THEN 'EN' ELSE 'DE' END,
  CONCAT('Artikel ', n, CASE WHEN n % 3 = 0 THEN ' EN' ELSE ' DE' END),
  CONCAT('Zusatz ', n),
  CONCAT('Dimension ', ((n - 1) % 8) + 1),
  CONCAT('Mehrsprachiger Langtext fuer Artikel ', n)
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKKontokorrent (Kto, KtoArt, Matchcode, Adresse, WKz, Zahlungskond, Nettobedingung, Gruppe, Kreditlimit)
SELECT
  CONCAT('KTO', RIGHT('000000' + CAST(n AS VARCHAR(6)), 6)),
  'D',
  CONCAT('MATCH-', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('KD', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CASE WHEN n % 5 = 0 THEN 'USD' ELSE 'EUR' END,
  CASE WHEN n % 3 = 0 THEN '30T' ELSE '14T' END,
  CASE WHEN n % 2 = 0 THEN 'Netto' ELSE 'Skonto' END,
  CONCAT('GRP', RIGHT('00' + CAST(((n - 1) % 20) + 1 AS VARCHAR(3)), 3)),
  CAST(500 + n * 25.50 AS DECIMAL(18,2))
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArtikelZubehoer (Ursprungsnummer, Zubehoernummer, UrsprungsArt, ZubehoerArt, Automatisch, Mengenberechnung, MengenFaktor, Basismengeneinheit)
SELECT
  CONCAT('ART', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CONCAT('ART', RIGHT('00000' + CAST(((n + 7 - 1) % 100) + 1 AS VARCHAR(5)), 5)),
  'Hauptartikel',
  'Zubehoer',
  CASE WHEN n % 2 = 0 THEN -1 ELSE 0 END,
  CASE WHEN n % 3 = 0 THEN 'FAKTOR' ELSE 'STUECK' END,
  CAST(1 + (n % 5) * 0.25 AS DECIMAL(18,6)),
  'STK'
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArchivVKBelege (AenderungsNr, BelID, Belegkennzeichen, Belegart, Belegjahr, Belegnummer, Matchcode, Belegstatus, A0Empfaenger, A0AdressNr, Belegdatum)
SELECT
  n,
  CONCAT('VK', RIGHT('000000' + CAST(n AS VARCHAR(6)), 6)),
  CASE WHEN n % 2 = 0 THEN 'VK' ELSE 'AN' END,
  CASE WHEN n % 2 = 0 THEN 'Rechnung' ELSE 'Angebot' END,
  2026,
  CONCAT('VK-', RIGHT('000000' + CAST(n AS VARCHAR(6)), 6)),
  CONCAT('MATCH-', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  CASE WHEN n % 4 = 0 THEN 'Geschlossen' ELSE 'Offen' END,
  CONCAT('Empfaenger ', n),
  CONCAT('KD', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5)),
  DATEADD(DAY, -n % 90, SYSUTCDATETIME())
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKArchivEKBelege (AenderungsNr, BelID, Belegkennzeichen, Belegart, Belegjahr, Belegnummer, Belegdatum, A0Empfaenger, A4Name1, A4Ort)
SELECT
  n,
  CONCAT('EK', RIGHT('000000' + CAST(n AS VARCHAR(6)), 6)),
  'EK',
  CASE WHEN n % 2 = 0 THEN 'Bestellung' ELSE 'Auftragsbestaetigung' END,
  2026,
  CONCAT('EK-', RIGHT('000000' + CAST(n AS VARCHAR(6)), 6)),
  DATEADD(DAY, -n % 120, SYSUTCDATETIME()),
  CONCAT('Lieferant ', n),
  CONCAT('Firma ', n),
  CONCAT('Ort-', ((n - 1) % 25) + 1)
FROM nums;

;WITH nums AS (
  SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects
)
INSERT INTO dbo.KHKVerkausprojekte (Projektname, Projektdatum, Wert, Projektstatus, Kundennummer)
SELECT
  CONCAT('Projekt ', n),
  DATEADD(DAY, -n % 60, SYSUTCDATETIME()),
  CAST(1000 + n * 75 AS DECIMAL(18,2)),
  CASE WHEN n % 3 = 0 THEN 'Won' WHEN n % 3 = 1 THEN 'Open' ELSE 'In Progress' END,
  CONCAT('KD', RIGHT('00000' + CAST(n AS VARCHAR(5)), 5))
FROM nums;
GO

CREATE VIEW dbo.KHKProdukt AS
SELECT
  Artikelnummer,
  Bezeichnung1 AS Produktname,
  Bezeichnung2 AS Kurztext,
  Artikelgruppe,
  CASE WHEN Aktiv = -1 THEN 1 ELSE 0 END AS Aktiv
FROM dbo.KHKArtikel;
GO

CREATE VIEW dbo.KHKProduktTexte AS
SELECT
  CONCAT(Artikelnummer, '-', Sprache) AS TextId,
  Artikelnummer,
  Sprache,
  Langtext
FROM dbo.KHKArtikelBezeichnung;
GO

CREATE VIEW dbo.KHKArtikelRelations AS
SELECT
  CONCAT(Ursprungsnummer, '-', Zubehoernummer) AS ZeilenId,
  Ursprungsnummer AS HauptNr,
  Zubehoernummer AS ZubehoerArtikelnummer,
  CAST(MengenFaktor AS DECIMAL(18,2)) AS Menge,
  NULL AS Artikelgruppe
FROM dbo.KHKArtikelZubehoer;
GO

CREATE VIEW dbo.KHKBelege AS
SELECT
  Belegnummer,
  Belegstatus,
  Belegdatum,
  A0AdressNr AS Kundennummer,
  Belegart AS Belegtyp
FROM dbo.KHKArchivVKBelege;
GO

CREATE VIEW dbo.KHKAngebote AS
SELECT
  Belegnummer AS Angebotsnummer,
  DATEADD(DAY, 30, COALESCE(Belegdatum, SYSUTCDATETIME())) AS GueltigBis,
  A0AdressNr AS Kundennummer
FROM dbo.KHKArchivVKBelege
WHERE Belegart = 'Angebot';
GO

SELECT
  (SELECT COUNT(*) FROM dbo.KHKAdressen) AS KHKAdressen,
  (SELECT COUNT(*) FROM dbo.KHKAnsprechpartner) AS KHKAnsprechpartner,
  (SELECT COUNT(*) FROM dbo.KHKArtikel) AS KHKArtikel,
  (SELECT COUNT(*) FROM dbo.KHKArtikelBezeichnung) AS KHKArtikelBezeichnung,
  (SELECT COUNT(*) FROM dbo.KHKArtikelgruppen) AS KHKArtikelgruppen,
  (SELECT COUNT(*) FROM dbo.KHKKontokorrent) AS KHKKontokorrent,
  (SELECT COUNT(*) FROM dbo.KHKArtikelZubehoer) AS KHKArtikelZubehoer,
  (SELECT COUNT(*) FROM dbo.KHKArchivVKBelege) AS KHKArchivVKBelege,
  (SELECT COUNT(*) FROM dbo.KHKArchivEKBelege) AS KHKArchivEKBelege,
  (SELECT COUNT(*) FROM dbo.KHKVerkausprojekte) AS KHKVerkausprojekte;
