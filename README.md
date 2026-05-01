# SF On-Prem Integration Agent

Salesforce-gesteuerter On-Prem Integration Agent auf Basis von Node.js und TypeScript.

Der Agent holt Konfigurationen aus Salesforce, liest Daten aus Quellsystemen (z. B. MSSQL), mapped sie in ein kanonisches Format und schreibt sie in Zielsysteme (Salesforce Objekt oder Picklist).

## Features

- Salesforce als Steuerungsebene fuer Schedules, Runs und Logging
- Flexible Import-Profile pro Ziel (Objekt oder Picklist)
- Picklist-Unterstuetzung:
  - Global Value Set
  - Objektbezogene Picklist
  - Optionales SQL-Mapping fuer Picklist-Werte
- Scheduler pro Import-Profil mit Regeln:
  - Tage (Mo-So)
  - Zeitfenster (inkl. Overnight-Faelle)
  - Intervall in Minuten
- Overlap-Schutz ueber laufende Runs (`MSD_Run__c` mit Status `Running`)
- Windows-Dienst-Installation
- Auto-Update mit Manifest, SHA256-Pruefung, Backup und Rollback
- Release-Paketerstellung fuer Kundeninstallation

## Voraussetzungen

- Node.js 22+
- npm
- Zugriff auf Salesforce und ggf. MSSQL

## Lokale Entwicklung

```bash
npm ci
npm run build
npm start
```

Entwicklung mit ts-node:

```bash
npm run dev
```

## Wichtige Skripte

- `npm run build` - TypeScript Build
- `npm run start` - Start aus `dist/main.js`
- `npm run dev` - Start aus TypeScript-Quellen
- `npm run win:install-service` - Windows-Dienst installieren
- `npm run win:uninstall-service` - Windows-Dienst deinstallieren
- `npm run win:update-now` - Manuelles Update ausfuehren
- `npm run win:register-updater` - Scheduled Task fuer Auto-Update registrieren
- `npm run win:build-package` - Kunden-ZIP erzeugen
- `npm run win:build-package:with-node-modules` - Kunden-ZIP inkl. `node_modules`
- `npm run init:installation -- --mode SAGE100` - Interaktive Erstinstallation (Salesforce + SAGE100 SQL + Basis-Importprofile)
- `npm run sf:deploy-metadata` - Salesforce Metadaten deployen

## Kunden-Deployment (Windows)

Das detaillierte Runbook steht in [WINDOWS_DEPLOYMENT.md](WINDOWS_DEPLOYMENT.md).

Kurzablauf:

1. Release-Paket auf Zielserver entpacken
2. `.env` konfigurieren
3. Optional: `npm run init:installation -- --mode SAGE100` (fragt Salesforce URL/Client und SQL Server fuer SAGE100 ab)
4. Dienst installieren
5. Auto-Updater registrieren

## Paket fuer Kundeninstallation erstellen

```bash
npm run build
npm run win:build-package
```

Artefakt:

- `artifacts/sf-onprem-integration-agent-customer-installer-<version>.zip`

Optional (groesser, dafuer ohne npm-Install auf Kundensystem):

```bash
npm run win:build-package:with-node-modules
```

## Salesforce Metadaten

Metadaten liegen unter:

- `salesforce/metadata/objects/`
- `salesforce/metadata/package.xml`

Deployment:

```bash
npm run sf:deploy-metadata
```

Noetige Umgebungsvariablen:

- `SF_LOGIN_URL`
- `SF_CLIENT_ID`
- `SF_CLIENT_SECRET`
- `SF_USERNAME`
- `SF_PASSWORD`

## Update-Manifest URL

Der Updater ist auf dieses Repository konfiguriert:

- `https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json`

## Datei-Import/Export Beispiele (CSV und Excel)

Fuer schnelle Tests sind fertige Beispielartefakte vorhanden unter:

- [artifacts/file-examples/account-import.csv](artifacts/file-examples/account-import.csv)
- [artifacts/file-examples/contact-import.csv](artifacts/file-examples/contact-import.csv)
- [artifacts/file-examples/account-import.xlsx](artifacts/file-examples/account-import.xlsx)
- [artifacts/file-examples/contact-import.xlsx](artifacts/file-examples/contact-import.xlsx)
- [artifacts/file-examples/account-export-example.csv](artifacts/file-examples/account-export-example.csv)
- [artifacts/file-examples/contact-export-example.csv](artifacts/file-examples/contact-export-example.csv)
- [artifacts/file-examples/account-export-example.xlsx](artifacts/file-examples/account-export-example.xlsx)
- [artifacts/file-examples/contact-export-example.xlsx](artifacts/file-examples/contact-export-example.xlsx)
- [artifacts/file-examples/setup-file-import-export.example.json](artifacts/file-examples/setup-file-import-export.example.json)

Enthaltene Szenarien:

- Account Import (Datei nach Salesforce)
- Ansprechpartner Import (Datei nach Salesforce)
- Account Export (Salesforce nach Datei)
- Ansprechpartner Export (Salesforce nach Datei)

### Setup mit einem Klick importieren

1. Web UI oeffnen
2. Quellinstanz im Instanz-Dropdown waehlen und Setup exportieren klicken (JSON wird heruntergeladen)
3. Zielinstanz im Instanz-Dropdown waehlen
4. Setup importieren klicken
5. Exportierte JSON-Datei auswaehlen

Die Datei legt einen Beispiel-Datei-Connector und vier deaktivierte Scheduler an.

Hinweis: Parent-Child Scheduler-Hierarchien werden beim Import instanzuebergreifend ueber Namen aufgeloest.

### CSV Beispiele direkt anlegen

1. Zielinstanz im Instanz-Dropdown waehlen
2. Oben in der Leiste auf CSV Beispiele anlegen klicken

Dadurch wird automatisch artifacts/file-examples/setup-file-import-export.example.json importiert und die vier CSV Beispiel-Scheduler (Account/Ansprechpartner Import/Export) werden sofort angelegt.

### DragDrop Schnelltest

1. In der Uebersicht einen Datei-Connector oeffnen
2. Eine CSV/XLSX Datei auf den Connector-Knoten ziehen
3. Es wird automatisch ein neuer Scheduler erzeugt und mit Header-Mapping vorbelegt

### Beispielpfade im Connector

Das Beispiel-Setup nutzt folgende Parameter:

- basePath: artifacts/file-examples
- importPath: .
- exportPath: .
- archivePath: archive

Damit werden die Importdateien direkt aus [artifacts/file-examples](artifacts/file-examples) gelesen und Exporte dort geschrieben.
