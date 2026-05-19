/*
  Sage 100 sample database for MSSQL integration tests.

  Basis: artifacts/sage100-db-doc-index.json, generated from the Sage 100
  database documentation. The script intentionally contains only the relevant
  operational tables for customer, contact, product, accessory, price, document
  and text-block scenarios.

  Usage:
    sqlcmd -S localhost -U sa -P "<password>" -i scripts/mssql/create-sage100-sample-db.sql
*/

IF DB_ID(N'Sage100Sample') IS NULL
BEGIN
  CREATE DATABASE [Sage100Sample];
END
GO

USE [Sage100Sample];
GO

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'dbo.KHKTextbausteineGruppen', N'U') IS NOT NULL DROP TABLE dbo.KHKTextbausteineGruppen;
IF OBJECT_ID(N'dbo.KHKTextbausteineBelegarten', N'U') IS NOT NULL DROP TABLE dbo.KHKTextbausteineBelegarten;
IF OBJECT_ID(N'dbo.KHKVKBelegePositionen', N'U') IS NOT NULL DROP TABLE dbo.KHKVKBelegePositionen;
IF OBJECT_ID(N'dbo.KHKVKBelege', N'U') IS NOT NULL DROP TABLE dbo.KHKVKBelege;
IF OBJECT_ID(N'dbo.KHKPreislistenArtikel', N'U') IS NOT NULL DROP TABLE dbo.KHKPreislistenArtikel;
IF OBJECT_ID(N'dbo.KHKPreislisten', N'U') IS NOT NULL DROP TABLE dbo.KHKPreislisten;
IF OBJECT_ID(N'dbo.KHKArtikelZubehoerStaffel', N'U') IS NOT NULL DROP TABLE dbo.KHKArtikelZubehoerStaffel;
IF OBJECT_ID(N'dbo.KHKArtikelZubehoer', N'U') IS NOT NULL DROP TABLE dbo.KHKArtikelZubehoer;
IF OBJECT_ID(N'dbo.KHKZubehoergruppen', N'U') IS NOT NULL DROP TABLE dbo.KHKZubehoergruppen;
IF OBJECT_ID(N'dbo.KHKTextbausteine', N'U') IS NOT NULL DROP TABLE dbo.KHKTextbausteine;
IF OBJECT_ID(N'dbo.KHKArtikel', N'U') IS NOT NULL DROP TABLE dbo.KHKArtikel;
IF OBJECT_ID(N'dbo.KHKArtikelgruppen', N'U') IS NOT NULL DROP TABLE dbo.KHKArtikelgruppen;
IF OBJECT_ID(N'dbo.KHKAnsprechpartner', N'U') IS NOT NULL DROP TABLE dbo.KHKAnsprechpartner;
IF OBJECT_ID(N'dbo.KHKKontokorrent', N'U') IS NOT NULL DROP TABLE dbo.KHKKontokorrent;
IF OBJECT_ID(N'dbo.KHKAdressen', N'U') IS NOT NULL DROP TABLE dbo.KHKAdressen;
IF OBJECT_ID(N'dbo.KHKGruppen', N'U') IS NOT NULL DROP TABLE dbo.KHKGruppen;
GO

CREATE TABLE dbo.KHKAdressen (
  Adresse int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKAdressen_Mandant DEFAULT (0),
  Kategorie smallint NULL,
  Matchcode nvarchar(50) NOT NULL,
  Anrede nvarchar(50) NULL,
  Name1 nvarchar(50) NULL,
  Name2 nvarchar(50) NULL,
  LieferZusatz nvarchar(64) NULL,
  LieferStrasse nvarchar(64) NULL,
  LieferLand nvarchar(3) NULL,
  LieferPLZ nvarchar(24) NULL,
  LieferOrt nvarchar(40) NULL,
  Telefon nvarchar(40) NULL,
  Telefax nvarchar(40) NULL,
  EMail nvarchar(255) NULL,
  Homepage nvarchar(255) NULL,
  Memo nvarchar(max) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKAdressen PRIMARY KEY (Adresse, Mandant)
);

CREATE TABLE dbo.KHKKontokorrent (
  Kto nvarchar(20) NOT NULL,
  KtoArt nvarchar(1) NOT NULL CONSTRAINT DF_KHKKontokorrent_KtoArt DEFAULT (N'D'),
  Mandant smallint NOT NULL CONSTRAINT DF_KHKKontokorrent_Mandant DEFAULT (0),
  KtoTyp smallint NULL CONSTRAINT DF_KHKKontokorrent_KtoTyp DEFAULT (0),
  Matchcode nvarchar(50) NOT NULL,
  Adresse int NULL,
  StandardAdresse int NULL,
  WKz nvarchar(3) NOT NULL CONSTRAINT DF_KHKKontokorrent_WKz DEFAULT (N'EUR'),
  Sammelkto smallint NOT NULL CONSTRAINT DF_KHKKontokorrent_Sammelkto DEFAULT (0),
  Bebuchbarkeit int NULL,
  Besteuerung smallint NOT NULL CONSTRAINT DF_KHKKontokorrent_Besteuerung DEFAULT (0),
  EULand nvarchar(2) NULL,
  Kundengruppe nvarchar(10) NULL,
  Vertreter nvarchar(20) NULL,
  Zahlungsbedingung nvarchar(10) NULL,
  Kreditlimit decimal(19,4) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKKontokorrent PRIMARY KEY (Kto, Mandant),
  CONSTRAINT FK_KHKKontokorrent_Adresse FOREIGN KEY (Adresse, Mandant) REFERENCES dbo.KHKAdressen (Adresse, Mandant)
);

CREATE TABLE dbo.KHKAnsprechpartner (
  Nummer int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKAnsprechpartner_Mandant DEFAULT (0),
  Adresse int NOT NULL,
  Ansprechpartner nvarchar(50) NULL,
  Gruppe nvarchar(10) NULL,
  Titel nvarchar(50) NULL,
  Vorname nvarchar(50) NULL,
  Nachname nvarchar(50) NULL,
  Position nvarchar(64) NULL,
  Abteilung nvarchar(50) NULL,
  Anrede nvarchar(50) NULL,
  Briefanrede nvarchar(64) NULL,
  Telefon nvarchar(40) NULL,
  MobilTelefon nvarchar(40) NULL,
  EMail nvarchar(255) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKAnsprechpartner PRIMARY KEY (Mandant, Nummer),
  CONSTRAINT FK_KHKAnsprechpartner_Adresse FOREIGN KEY (Adresse, Mandant) REFERENCES dbo.KHKAdressen (Adresse, Mandant)
);

CREATE TABLE dbo.KHKGruppen (
  Typ int NOT NULL,
  Gruppe nvarchar(10) NOT NULL,
  GruppeInt int NOT NULL CONSTRAINT DF_KHKGruppen_GruppeInt DEFAULT (0),
  Mandant smallint NOT NULL CONSTRAINT DF_KHKGruppen_Mandant DEFAULT (0),
  Bezeichnung nvarchar(50) NULL,
  Tag nvarchar(50) NULL,
  Memo nvarchar(max) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKGruppen PRIMARY KEY (Gruppe, GruppeInt, Mandant, Typ)
);

CREATE TABLE dbo.KHKArtikelgruppen (
  Mandant smallint NOT NULL CONSTRAINT DF_KHKArtikelgruppen_Mandant DEFAULT (0),
  Hauptartikelgruppe nvarchar(15) NOT NULL,
  Artikelgruppe nvarchar(15) NOT NULL,
  VaterArtikelgruppe nvarchar(15) NOT NULL,
  Bezeichnung nvarchar(50) NULL,
  Gruppenebene smallint NULL CONSTRAINT DF_KHKArtikelgruppen_Gruppenebene DEFAULT (0),
  HatUntergruppen smallint NOT NULL CONSTRAINT DF_KHKArtikelgruppen_HatUntergruppen DEFAULT (0),
  Lieferant nvarchar(20) NULL,
  TempSelectKey int NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKArtikelgruppen PRIMARY KEY (Artikelgruppe, Hauptartikelgruppe, Mandant, VaterArtikelgruppe)
);

CREATE TABLE dbo.KHKArtikel (
  Artikelnummer nvarchar(31) NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKArtikel_Mandant DEFAULT (0),
  Bezeichnung1 nvarchar(50) NULL,
  Bezeichnung2 nvarchar(50) NULL,
  Matchcode nvarchar(50) NULL,
  Langtext nvarchar(max) NULL,
  Dimensionstext nvarchar(max) NULL,
  DimensionstextHTML nvarchar(max) NULL,
  DimensionstextRTF nvarchar(max) NULL,
  Memo nvarchar(max) NULL,
  Artikelgruppe nvarchar(15) NULL,
  Ersatzartikelnummer nvarchar(31) NULL,
  Mengeneinheit nvarchar(5) NULL,
  Steuercode nvarchar(10) NULL,
  Aktiv smallint NOT NULL CONSTRAINT DF_KHKArtikel_Aktiv DEFAULT (-1),
  EANNummer nvarchar(40) NULL,
  Gewicht decimal(19,4) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKArtikel PRIMARY KEY (Artikelnummer, Mandant)
);

CREATE TABLE dbo.KHKZubehoergruppen (
  Zubehoergruppe nvarchar(15) NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKZubehoergruppen_Mandant DEFAULT (0),
  Bezeichnung nvarchar(50) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKZubehoergruppen PRIMARY KEY (Mandant, Zubehoergruppe)
);

CREATE TABLE dbo.KHKArtikelZubehoer (
  ZubehoerID int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKArtikelZubehoer_Mandant DEFAULT (0),
  UrsprungsArt smallint NULL CONSTRAINT DF_KHKArtikelZubehoer_UrsprungsArt DEFAULT (1),
  Ursprungsnummer nvarchar(31) NULL,
  UrsprungsAuspraegungID int NULL,
  ZubehoerArt smallint NULL CONSTRAINT DF_KHKArtikelZubehoer_ZubehoerArt DEFAULT (1),
  Zubehoernummer nvarchar(31) NULL,
  ZubehoerAuspraegungID int NULL,
  Automatisch smallint NOT NULL CONSTRAINT DF_KHKArtikelZubehoer_Automatisch DEFAULT (0),
  Mengenberechnung smallint NULL CONSTRAINT DF_KHKArtikelZubehoer_Mengenberechnung DEFAULT (0),
  MengenFaktor decimal(19,4) NULL CONSTRAINT DF_KHKArtikelZubehoer_MengenFaktor DEFAULT (0),
  Basismengeneinheit nvarchar(5) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKArtikelZubehoer PRIMARY KEY (Mandant, ZubehoerID),
  CONSTRAINT FK_KHKArtikelZubehoer_Ursprung FOREIGN KEY (Ursprungsnummer, Mandant) REFERENCES dbo.KHKArtikel (Artikelnummer, Mandant),
  CONSTRAINT FK_KHKArtikelZubehoer_Zubehoer FOREIGN KEY (Zubehoernummer, Mandant) REFERENCES dbo.KHKArtikel (Artikelnummer, Mandant)
);

CREATE TABLE dbo.KHKArtikelZubehoerStaffel (
  ZubehoerID int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKArtikelZubehoerStaffel_Mandant DEFAULT (0),
  Staffelart smallint NOT NULL,
  UrsprungMengePreis decimal(19,4) NOT NULL CONSTRAINT DF_KHKArtikelZubehoerStaffel_Ursprung DEFAULT (0),
  ZubehoerMenge decimal(19,4) NULL CONSTRAINT DF_KHKArtikelZubehoerStaffel_Menge DEFAULT (0),
  Timesparmp rowversion,
  CONSTRAINT PK_KHKArtikelZubehoerStaffel PRIMARY KEY (Mandant, Staffelart, UrsprungMengePreis, ZubehoerID),
  CONSTRAINT FK_KHKArtikelZubehoerStaffel_Zubehoer FOREIGN KEY (Mandant, ZubehoerID) REFERENCES dbo.KHKArtikelZubehoer (Mandant, ZubehoerID)
);

CREATE TABLE dbo.KHKPreislisten (
  ID int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKPreislisten_Mandant DEFAULT (0),
  Bezeichnung nvarchar(20) NULL,
  IstAktion smallint NOT NULL CONSTRAINT DF_KHKPreislisten_IstAktion DEFAULT (0),
  Basisliste int NOT NULL CONSTRAINT DF_KHKPreislisten_Basisliste DEFAULT (0),
  Aktionsliste int NOT NULL CONSTRAINT DF_KHKPreislisten_Aktionsliste DEFAULT (0),
  GueltigVon datetime2 NULL,
  GueltigBis datetime2 NULL,
  IstBruttopreis smallint NOT NULL CONSTRAINT DF_KHKPreislisten_IstBruttopreis DEFAULT (0),
  IstRabattfaehig smallint NOT NULL CONSTRAINT DF_KHKPreislisten_IstRabattfaehig DEFAULT (-1),
  Memo nvarchar(max) NULL,
  WKz nvarchar(3) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKPreislisten PRIMARY KEY (ID, Mandant)
);

CREATE TABLE dbo.KHKPreislistenArtikel (
  Mandant smallint NOT NULL CONSTRAINT DF_KHKPreislistenArtikel_Mandant DEFAULT (0),
  ListeID int NULL CONSTRAINT DF_KHKPreislistenArtikel_ListeID DEFAULT (0),
  Artikelnummer nvarchar(31) NULL,
  AuspraegungID int NULL CONSTRAINT DF_KHKPreislistenArtikel_AuspraegungID DEFAULT (0),
  AbMenge decimal(19,4) NULL CONSTRAINT DF_KHKPreislistenArtikel_AbMenge DEFAULT (0),
  Einzelpreis decimal(19,4) NULL CONSTRAINT DF_KHKPreislistenArtikel_Einzelpreis DEFAULT (0),
  ID int NOT NULL,
  IstStaffelPreis smallint NOT NULL CONSTRAINT DF_KHKPreislistenArtikel_IstStaffelPreis DEFAULT (0),
  Timesparmp rowversion,
  CONSTRAINT PK_KHKPreislistenArtikel PRIMARY KEY (ID, Mandant),
  CONSTRAINT FK_KHKPreislistenArtikel_Liste FOREIGN KEY (ListeID, Mandant) REFERENCES dbo.KHKPreislisten (ID, Mandant),
  CONSTRAINT FK_KHKPreislistenArtikel_Artikel FOREIGN KEY (Artikelnummer, Mandant) REFERENCES dbo.KHKArtikel (Artikelnummer, Mandant)
);

CREATE TABLE dbo.KHKVKBelege (
  BelID int NOT NULL,
  Mandant smallint NOT NULL,
  Belegkennzeichen nvarchar(3) NULL,
  Belegart nvarchar(50) NULL,
  Belegjahr smallint NULL CONSTRAINT DF_KHKVKBelege_Belegjahr DEFAULT (0),
  Belegnummer int NULL CONSTRAINT DF_KHKVKBelege_Belegnummer DEFAULT (0),
  Matchcode nvarchar(50) NULL,
  Periode int NULL CONSTRAINT DF_KHKVKBelege_Periode DEFAULT (0),
  Belegstatus smallint NULL CONSTRAINT DF_KHKVKBelege_Belegstatus DEFAULT (0),
  Interessent smallint NOT NULL CONSTRAINT DF_KHKVKBelege_Interessent DEFAULT (0),
  A0AdressNr int NULL CONSTRAINT DF_KHKVKBelege_A0AdressNr DEFAULT (0),
  A0Empfaenger nvarchar(20) NULL,
  Belegdatum datetime2 NULL,
  Liefertermin datetime2 NULL,
  NettoBetrag decimal(19,4) NULL,
  SteuerBetrag decimal(19,4) NULL,
  BruttoBetrag decimal(19,4) NULL,
  WKz nvarchar(3) NULL CONSTRAINT DF_KHKVKBelege_WKz DEFAULT (N'EUR'),
  Kopftext nvarchar(max) NULL,
  Fusstext nvarchar(max) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKVKBelege PRIMARY KEY (BelID, Mandant),
  CONSTRAINT FK_KHKVKBelege_Kunde FOREIGN KEY (A0Empfaenger, Mandant) REFERENCES dbo.KHKKontokorrent (Kto, Mandant)
);

CREATE TABLE dbo.KHKVKBelegePositionen (
  BelPosID int NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKVKBelegePositionen_Mandant DEFAULT (0),
  BelID int NULL CONSTRAINT DF_KHKVKBelegePositionen_BelID DEFAULT (0),
  VorPosID int NULL CONSTRAINT DF_KHKVKBelegePositionen_VorPosID DEFAULT (0),
  [Index] int NULL CONSTRAINT DF_KHKVKBelegePositionen_Index DEFAULT (0),
  Position nvarchar(50) NULL,
  PosType smallint NULL CONSTRAINT DF_KHKVKBelegePositionen_PosType DEFAULT (1),
  Status smallint NULL CONSTRAINT DF_KHKVKBelegePositionen_Status DEFAULT (0),
  Artikelnummer nvarchar(31) NULL,
  AuspraegungID int NULL CONSTRAINT DF_KHKVKBelegePositionen_AuspraegungID DEFAULT (0),
  Artikelgruppe nvarchar(15) NULL,
  Hauptartikelgruppe nvarchar(15) NULL,
  Bezeichnung1 nvarchar(50) NULL,
  Bezeichnung2 nvarchar(50) NULL,
  Menge decimal(19,4) NULL,
  Mengeneinheit nvarchar(5) NULL,
  Einzelpreis decimal(19,4) NULL,
  Rabatt decimal(19,4) NULL,
  Gesamtpreis decimal(19,4) NULL,
  Langtext nvarchar(max) NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKVKBelegePositionen PRIMARY KEY (BelPosID, Mandant),
  CONSTRAINT FK_KHKVKBelegePositionen_Beleg FOREIGN KEY (BelID, Mandant) REFERENCES dbo.KHKVKBelege (BelID, Mandant),
  CONSTRAINT FK_KHKVKBelegePositionen_Artikel FOREIGN KEY (Artikelnummer, Mandant) REFERENCES dbo.KHKArtikel (Artikelnummer, Mandant)
);

CREATE TABLE dbo.KHKTextbausteine (
  Textbaustein nvarchar(30) NOT NULL,
  Sprache nvarchar(3) NOT NULL,
  Klasse smallint NOT NULL CONSTRAINT DF_KHKTextbausteine_Klasse DEFAULT (0),
  Mandant smallint NOT NULL CONSTRAINT DF_KHKTextbausteine_Mandant DEFAULT (0),
  Text nvarchar(max) NOT NULL,
  TextHTML nvarchar(max) NULL,
  TextRTF nvarchar(max) NULL,
  Obligatorisch smallint NULL CONSTRAINT DF_KHKTextbausteine_Obligatorisch DEFAULT (0),
  Timesparmp rowversion,
  CONSTRAINT PK_KHKTextbausteine PRIMARY KEY (Klasse, Mandant, Sprache, Textbaustein)
);

CREATE TABLE dbo.KHKTextbausteineBelegarten (
  Textbaustein nvarchar(30) NOT NULL,
  Belegart nvarchar(3) NOT NULL,
  Mandant smallint NOT NULL CONSTRAINT DF_KHKTextbausteineBelegarten_Mandant DEFAULT (0),
  Art smallint NULL CONSTRAINT DF_KHKTextbausteineBelegarten_Art DEFAULT (1),
  Timesparmp rowversion,
  CONSTRAINT PK_KHKTextbausteineBelegarten PRIMARY KEY (Belegart, Mandant, Textbaustein)
);

CREATE TABLE dbo.KHKTextbausteineGruppen (
  Textbaustein nvarchar(30) NOT NULL,
  Gruppe nvarchar(10) NOT NULL,
  Typ int NOT NULL,
  Mandant smallint NOT NULL,
  Timesparmp rowversion,
  CONSTRAINT PK_KHKTextbausteineGruppen PRIMARY KEY (Gruppe, Mandant, Textbaustein, Typ)
);
GO

INSERT INTO dbo.KHKAdressen
  (Adresse, Mandant, Kategorie, Matchcode, Anrede, Name1, Name2, LieferZusatz, LieferStrasse, LieferLand, LieferPLZ, LieferOrt, Telefon, Telefax, EMail, Homepage, Memo)
VALUES
  (10001, 1, 1, N'MUSTERMANN MASCHINENBAU', N'Firma', N'Mustermann Maschinenbau GmbH', N'Einkauf', N'Werk 1', N'Industriestrasse 12', N'D', N'86150', N'Augsburg', N'+49 821 1000-0', N'+49 821 1000-99', N'einkauf@mustermann.example', N'https://mustermann.example', N'Key Account fuer Ersatzteile und Wartung.'),
  (10002, 1, 1, N'BAYER AGRARTECHNIK', N'Firma', N'Bayer Agrartechnik KG', NULL, N'Zentrallager', N'Feldweg 8', N'D', N'93047', N'Regensburg', N'+49 941 2000-0', NULL, N'bestellung@bayer-agrar.example', N'https://bayer-agrar.example', N'Preisgruppe Handel.'),
  (10003, 1, 1, N'NORDHANDEL GMBH', N'Firma', N'Nordhandel GmbH', N'Logistik', NULL, N'Hafenstrasse 44', N'D', N'20457', N'Hamburg', N'+49 40 3000-0', NULL, N'orders@nordhandel.example', NULL, N'Regelmaessige Belegimporte.');

INSERT INTO dbo.KHKGruppen
  (Typ, Gruppe, GruppeInt, Mandant, Bezeichnung, Tag, Memo)
VALUES
  (11, N'HANDEL', 100, 1, N'Handelskunden', N'B2B', N'Kunden mit Standard-Handelskonditionen.'),
  (11, N'OEM', 200, 1, N'OEM Kunden', N'B2B', N'Kunden mit projektspezifischen Preisen.'),
  (11, N'SERVICE', 300, 1, N'Servicekunden', N'B2B', N'Kunden mit Wartungsvertraegen.');

INSERT INTO dbo.KHKKontokorrent
  (Kto, KtoArt, Mandant, KtoTyp, Matchcode, Adresse, StandardAdresse, WKz, Sammelkto, Bebuchbarkeit, Besteuerung, EULand, Kundengruppe, Vertreter, Zahlungsbedingung, Kreditlimit)
VALUES
  (N'D10001', N'D', 1, 0, N'MUSTERMANN MASCHINENBAU', 10001, 10001, N'EUR', 0, 1, 0, N'DE', N'OEM', N'V001', N'30T', 75000.00),
  (N'D10002', N'D', 1, 0, N'BAYER AGRARTECHNIK', 10002, 10002, N'EUR', 0, 1, 0, N'DE', N'HANDEL', N'V002', N'14T', 45000.00),
  (N'D10003', N'D', 1, 0, N'NORDHANDEL GMBH', 10003, 10003, N'EUR', 0, 1, 0, N'DE', N'SERVICE', N'V001', N'30T', 30000.00);

INSERT INTO dbo.KHKAnsprechpartner
  (Nummer, Mandant, Adresse, Ansprechpartner, Gruppe, Titel, Vorname, Nachname, Position, Abteilung, Anrede, Briefanrede, Telefon, MobilTelefon, EMail)
VALUES
  (20001, 1, 10001, N'Anna Schmidt', N'EK', NULL, N'Anna', N'Schmidt', N'Leiterin Einkauf', N'Einkauf', N'Frau', N'Sehr geehrte Frau Schmidt,', N'+49 821 1000-11', N'+49 170 100011', N'anna.schmidt@mustermann.example'),
  (20002, 1, 10002, N'Markus Weber', N'EK', NULL, N'Markus', N'Weber', N'Disponent', N'Logistik', N'Herr', N'Sehr geehrter Herr Weber,', N'+49 941 2000-22', NULL, N'markus.weber@bayer-agrar.example'),
  (20003, 1, 10003, N'Lea Hoffmann', N'SV', NULL, N'Lea', N'Hoffmann', N'Servicekoordination', N'Service', N'Frau', N'Sehr geehrte Frau Hoffmann,', N'+49 40 3000-33', N'+49 171 300033', N'lea.hoffmann@nordhandel.example');

INSERT INTO dbo.KHKArtikelgruppen
  (Mandant, Hauptartikelgruppe, Artikelgruppe, VaterArtikelgruppe, Bezeichnung, Gruppenebene, HatUntergruppen, Lieferant, TempSelectKey)
VALUES
  (1, N'MASCH', N'MASCH', N'', N'Maschinen', 0, -1, NULL, 0),
  (1, N'ERSATZ', N'ERSATZ', N'', N'Ersatzteile', 0, -1, NULL, 0),
  (1, N'SERVICE', N'SERVICE', N'', N'Serviceartikel', 0, 0, NULL, 0);

INSERT INTO dbo.KHKArtikel
  (Artikelnummer, Mandant, Bezeichnung1, Bezeichnung2, Matchcode, Langtext, Dimensionstext, DimensionstextHTML, DimensionstextRTF, Memo, Artikelgruppe, Ersatzartikelnummer, Mengeneinheit, Steuercode, Aktiv, EANNummer, Gewicht)
VALUES
  (N'1000006', 1, N'Produkt ID', N'Basisprodukt', N'PRODUKT-1000006', N'Basisprodukt fuer Preislisten- und PricebookEntry-Tests.', NULL, NULL, NULL, N'Salesforce ProductCode 1000006.', N'MASCH', NULL, N'ST', N'19', -1, N'4000001000006', 1250.0000),
  (N'1000007', 1, N'Zubehoer Kit A', N'Anbausatz', N'ZUBEHOER-1000007', N'Zubehoer fuer Produkt 1000006.', NULL, NULL, NULL, N'Automatisches Zubehoer.', N'ERSATZ', NULL, N'ST', N'19', -1, N'4000001000007', 12.5000),
  (N'1000008', 1, N'Wartungspaket S', N'Jahresservice', N'SERVICE-1000008', N'Serviceartikel fuer Belegpositionen.', NULL, NULL, NULL, N'Nicht lagergefuehrter Serviceartikel.', N'SERVICE', NULL, N'H', N'19', -1, NULL, 0.0000),
  (N'1000009', 1, N'Ersatzmesser Set', N'Verschleissteil', N'ERSATZ-1000009', N'Ersatzteilset als optionales Zubehoer.', NULL, NULL, NULL, N'Verschleissteil.', N'ERSATZ', NULL, N'ST', N'19', -1, N'4000001000009', 3.7500);

INSERT INTO dbo.KHKZubehoergruppen
  (Zubehoergruppe, Mandant, Bezeichnung)
VALUES
  (N'STDKIT', 1, N'Standard-Zubehoer'),
  (N'SERVICE', 1, N'Service-Zubehoer');

INSERT INTO dbo.KHKArtikelZubehoer
  (ZubehoerID, Mandant, UrsprungsArt, Ursprungsnummer, UrsprungsAuspraegungID, ZubehoerArt, Zubehoernummer, ZubehoerAuspraegungID, Automatisch, Mengenberechnung, MengenFaktor, Basismengeneinheit)
VALUES
  (30001, 1, 1, N'1000006', 0, 1, N'1000007', 0, -1, 2, 1.0000, N'ST'),
  (30002, 1, 1, N'1000006', 0, 1, N'1000009', 0, 0, 1, 1.0000, N'ST'),
  (30003, 1, 1, N'1000006', 0, 1, N'1000008', 0, 0, 5, 0.0000, N'H');

INSERT INTO dbo.KHKArtikelZubehoerStaffel
  (ZubehoerID, Mandant, Staffelart, UrsprungMengePreis, ZubehoerMenge)
VALUES
  (30002, 1, 4, 1.0000, 1.0000),
  (30002, 1, 4, 5.0000, 2.0000),
  (30003, 1, 5, 5000.0000, 2.0000);

INSERT INTO dbo.KHKPreislisten
  (ID, Mandant, Bezeichnung, IstAktion, Basisliste, Aktionsliste, GueltigVon, GueltigBis, IstBruttopreis, IstRabattfaehig, Memo, WKz)
VALUES
  (1, 1, N'Standard', 0, 0, 0, '2026-01-01', '2026-12-31', 0, -1, N'Standard-Netto-Preisliste 2026.', N'EUR'),
  (2, 1, N'OEM 2026', 0, 1, 0, '2026-01-01', '2026-12-31', 0, -1, N'Projektpreise fuer OEM-Kunden.', N'EUR'),
  (3, 1, N'Aktion Q2', -1, 1, 0, '2026-04-01', '2026-06-30', 0, -1, N'Aktionspreise fuer Q2.', N'EUR');

INSERT INTO dbo.KHKPreislistenArtikel
  (Mandant, ListeID, Artikelnummer, AuspraegungID, AbMenge, Einzelpreis, ID, IstStaffelPreis)
VALUES
  (1, 1, N'1000006', 0, 0.0000, 572.9900, 40001, 0),
  (1, 1, N'1000007', 0, 0.0000, 89.9000, 40002, 0),
  (1, 1, N'1000008', 0, 0.0000, 120.0000, 40003, 0),
  (1, 1, N'1000009', 0, 0.0000, 49.5000, 40004, 0),
  (1, 2, N'1000006', 0, 0.0000, 535.0000, 40005, 0),
  (1, 2, N'1000006', 0, 10.0000, 510.0000, 40006, -1),
  (1, 3, N'1000007', 0, 0.0000, 79.9000, 40007, 0);

INSERT INTO dbo.KHKTextbausteine
  (Textbaustein, Sprache, Klasse, Mandant, Text, TextHTML, TextRTF, Obligatorisch)
VALUES
  (N'VK-KOPF', N'DEU', 1, 1, N'Vielen Dank fuer Ihre Bestellung. Die Lieferung erfolgt gemaess unseren Verkaufsbedingungen.', NULL, NULL, 0),
  (N'VK-FUSS', N'DEU', 1, 1, N'Zahlbar netto innerhalb der vereinbarten Zahlungsfrist.', NULL, NULL, 0),
  (N'ART-HINWEIS', N'DEU', 2, 1, N'Artikel ist fuer den Export in Salesforce als Produkt geeignet.', NULL, NULL, 0),
  (N'ZUB-HINWEIS', N'DEU', 3, 1, N'Zubehoer wird bei Belegerfassung vorgeschlagen.', NULL, NULL, 0);

INSERT INTO dbo.KHKTextbausteineBelegarten
  (Textbaustein, Belegart, Mandant, Art)
VALUES
  (N'VK-KOPF', N'RE', 1, 1),
  (N'VK-FUSS', N'RE', 1, 2),
  (N'VK-KOPF', N'AN', 1, 1);

INSERT INTO dbo.KHKTextbausteineGruppen
  (Textbaustein, Gruppe, Typ, Mandant)
VALUES
  (N'VK-KOPF', N'OEM', 11, 1),
  (N'VK-FUSS', N'HANDEL', 11, 1);

INSERT INTO dbo.KHKVKBelege
  (BelID, Mandant, Belegkennzeichen, Belegart, Belegjahr, Belegnummer, Matchcode, Periode, Belegstatus, Interessent, A0AdressNr, A0Empfaenger, Belegdatum, Liefertermin, NettoBetrag, SteuerBetrag, BruttoBetrag, WKz, Kopftext, Fusstext)
VALUES
  (50001, 1, N'RE', N'Rechnung', 2026, 100045, N'RE-2026-100045 MUSTERMANN', 202605, 0, 0, 10001, N'D10001', '2026-05-10', '2026-05-14', 1267.9800, 240.9162, 1508.8962, N'EUR', N'Vielen Dank fuer Ihre Bestellung.', N'Zahlbar netto innerhalb von 30 Tagen.'),
  (50002, 1, N'AN', N'Angebot', 2026, 200012, N'AN-2026-200012 BAYER', 202605, 1, 0, 10002, N'D10002', '2026-05-15', '2026-06-01', 662.8900, 125.9491, 788.8391, N'EUR', N'Angebot freibleibend.', N'Gueltig bis 30.06.2026.'),
  (50003, 1, N'RE', N'Rechnung', 2026, 100046, N'RE-2026-100046 NORDHANDEL', 202605, 0, 0, 10003, N'D10003', '2026-05-18', '2026-05-20', 360.0000, 68.4000, 428.4000, N'EUR', N'Serviceeinsatz gemaess Auftrag.', N'Vielen Dank.');

INSERT INTO dbo.KHKVKBelegePositionen
  (BelPosID, Mandant, BelID, VorPosID, [Index], Position, PosType, Status, Artikelnummer, AuspraegungID, Artikelgruppe, Hauptartikelgruppe, Bezeichnung1, Bezeichnung2, Menge, Mengeneinheit, Einzelpreis, Rabatt, Gesamtpreis, Langtext)
VALUES
  (51001, 1, 50001, 0, 1, N'1', 1, 0, N'1000006', 0, N'MASCH', N'MASCH', N'Produkt ID', N'Basisprodukt', 2.0000, N'ST', 572.9900, 0.0000, 1145.9800, N'Basisprodukt fuer Mustermann.'),
  (51002, 1, 50001, 0, 2, N'2', 1, 0, N'1000007', 0, N'ERSATZ', N'ERSATZ', N'Zubehoer Kit A', N'Anbausatz', 1.0000, N'ST', 89.9000, 0.0000, 89.9000, N'Automatisch eingefuegtes Zubehoer.'),
  (51003, 1, 50001, 0, 3, N'3', 1, 0, N'1000009', 0, N'ERSATZ', N'ERSATZ', N'Ersatzmesser Set', N'Verschleissteil', 1.0000, N'ST', 49.5000, 35.0000, 32.1000, N'Rabattiertes Ersatzteil.'),
  (52001, 1, 50002, 0, 1, N'1', 1, 0, N'1000006', 0, N'MASCH', N'MASCH', N'Produkt ID', N'Basisprodukt', 1.0000, N'ST', 572.9900, 0.0000, 572.9900, N'Angebotsposition fuer Bayer Agrartechnik.'),
  (52002, 1, 50002, 0, 2, N'2', 1, 0, N'1000007', 0, N'ERSATZ', N'ERSATZ', N'Zubehoer Kit A', N'Anbausatz', 1.0000, N'ST', 89.9000, 0.0000, 89.9000, N'Optionales Kit.'),
  (53001, 1, 50003, 0, 1, N'1', 1, 0, N'1000008', 0, N'SERVICE', N'SERVICE', N'Wartungspaket S', N'Jahresservice', 3.0000, N'H', 120.0000, 0.0000, 360.0000, N'Serviceeinsatz vor Ort.');
GO

SELECT 'KHKAdressen' AS Tabelle, COUNT(*) AS Datensaetze FROM dbo.KHKAdressen
UNION ALL SELECT 'KHKKontokorrent', COUNT(*) FROM dbo.KHKKontokorrent
UNION ALL SELECT 'KHKAnsprechpartner', COUNT(*) FROM dbo.KHKAnsprechpartner
UNION ALL SELECT 'KHKArtikel', COUNT(*) FROM dbo.KHKArtikel
UNION ALL SELECT 'KHKArtikelZubehoer', COUNT(*) FROM dbo.KHKArtikelZubehoer
UNION ALL SELECT 'KHKPreislistenArtikel', COUNT(*) FROM dbo.KHKPreislistenArtikel
UNION ALL SELECT 'KHKVKBelege', COUNT(*) FROM dbo.KHKVKBelege
UNION ALL SELECT 'KHKVKBelegePositionen', COUNT(*) FROM dbo.KHKVKBelegePositionen
UNION ALL SELECT 'KHKTextbausteine', COUNT(*) FROM dbo.KHKTextbausteine;
GO
