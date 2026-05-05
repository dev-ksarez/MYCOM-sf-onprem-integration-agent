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

Fuer geschuetzte Admin-Zugaenge der Web UI:

- ADMIN_UI_USERNAME
- ADMIN_UI_PASSWORD

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

## Spec-Driven Development

Fuer nicht-triviale Aenderungen wird zuerst eine Spec unter `docs/specs/` angelegt und erst danach implementiert.

Schnellstart:

```bash
npm run spec:new -- "Kurzer Feature-Titel"
npm run spec:validate
```

Die ausfuehrliche Einfuehrung steht in [SPEC_DRIVEN_DEVELOPMENT.md](SPEC_DRIVEN_DEVELOPMENT.md), die laufenden Specs und die Vorlage in [docs/specs/README.md](docs/specs/README.md).

## Modulgrenzen Web UI

Die grosse Server- und UI-Datei wird inkrementell aufgeteilt.

- Dashboard-Update-Logik: `src/server/dashboard-update-service.ts`
- Migration Listen-/Import-UI: `src/server/migration-ui-module.ts`
- Zentrale Aggregation und Mutationen: `src/server/admin-data-service.ts`

Neue serverseitige und UI-nahe Logik sollte bevorzugt in diesen Modulen oder weiteren kleinen Nachfolgemodulen landen, statt `src/server/app.ts` weiter aufzublähen.

## Wichtige Skripte

- `npm run build` - TypeScript Build
- `npm run start` - Start aus `dist/main.js`
- `npm run dev` - Start aus TypeScript-Quellen
- `npm run spec:new -- "Titel"` - Neue Spec fuer nicht-triviale Aenderungen anlegen
- `npm run spec:validate` - Spec-Struktur in `docs/specs/` pruefen
- `npm run docker:test:ubuntu` - Ubuntu-basierter Docker-Verifikationstest fuer Build + Specs
- `npm run win:install-service` - Windows-Dienst installieren
- `npm run win:uninstall-service` - Windows-Dienst deinstallieren
- `npm run win:update-now` - Manuelles Update ausfuehren
- `npm run win:register-updater` - Scheduled Task fuer Auto-Update registrieren
- `npm run win:build-package` - Kunden-ZIP erzeugen
- `npm run win:build-package:with-node-modules` - Kunden-ZIP inkl. `node_modules`
- `npm run init:installation -- --mode SAGE100` - Interaktive Erstinstallation (Salesforce + SAGE100 SQL + Basis-Importprofile)
- `npm run sf:deploy-metadata` - Salesforce Metadaten deployen

## Setup-Szenarien

Es gibt jetzt drei verbindliche Setup-Szenarien:

1. [WINDOWS_DEPLOYMENT.md](WINDOWS_DEPLOYMENT.md): Windows Server als Dienst im lokalen Netz
2. [UBUNTU_LOCAL_DEPLOYMENT.md](UBUNTU_LOCAL_DEPLOYMENT.md): Ubuntu-Server im lokalen Netz
3. [LINUX_DEPLOYMENT.md](LINUX_DEPLOYMENT.md): öffentlicher Linux-Server mit Reverse Proxy und TLS

Die Web UI im Tab Installation kann zwischen diesen drei Szenarien umschalten und dafür passende Artefakte erzeugen.

## Kunden-Deployment (Windows)

Das detaillierte Runbook steht in [WINDOWS_DEPLOYMENT.md](WINDOWS_DEPLOYMENT.md).

Kurzablauf:

1. Release-Paket auf Zielserver entpacken
2. `.env` konfigurieren
3. Optional: `npm run init:installation -- --mode SAGE100` (fragt Salesforce URL/Client und SQL Server fuer SAGE100 ab)
4. Dienst installieren
5. Auto-Updater registrieren

## Linux-Deployment und gehaerteter Betrieb

Die Linux-Runbooks sind jetzt getrennt:

- [UBUNTU_LOCAL_DEPLOYMENT.md](UBUNTU_LOCAL_DEPLOYMENT.md) fuer Ubuntu im lokalen Netz
- [LINUX_DEPLOYMENT.md](LINUX_DEPLOYMENT.md) fuer den öffentlichen Linux-Server

Kurzablauf:

1. App unter einem dedizierten Service-User nach `/opt/sf-integration-agent` bereitstellen
2. Environment-Datei unter `/etc/sf-integration-agent/agent.env` pflegen
3. systemd-Service und nginx-Reverse-Proxy aus [scripts/linux](scripts/linux) aktivieren
4. TLS am Reverse Proxy terminieren und nur HTTPS oeffentlich exponieren
5. Optional: abgesicherten SFTP-Drop-User fuer Datei-Connectoren mit `npm run linux:setup-sftp -- --sftp-user <name>` einrichten

Wichtige Security-Variablen fuer die Web UI:

- `ADMIN_UI_USERNAME` und `ADMIN_UI_PASSWORD` aktivieren den Login-Schutz fuer Root und Admin-API.
- Im Produktionsmodus bleibt die Web UI ohne diese beiden Variablen gesperrt.
- Mutierende Requests der Web UI werden zusaetzlich durch CSRF-Token und Origin-Pruefung abgesichert.
- MSSQL-Verbindungen nutzen jetzt sichere Defaults: `encrypt=true`, `trustServerCertificate=false`.

Erster webbasierter Installer-Einstieg:

- In der Web UI steht ein neuer Tab Installation zur Verfuegung.
- Dort werden jetzt drei Setup-Szenarien angeboten: Windows Server / Dienst, Linux (Ubuntu) und öffentlicher Linux Server.
- Dort werden je Szenario Zielpfade, sichere Default-Variablen, SFTP-Setup und empfohlene Betriebsbefehle zusammengefasst.
- Dort koennen jetzt auch szenariospezifische Vorlagendateien unter `artifacts/installer/generated/...` erzeugt werden.
- Nach der Erzeugung steht das jeweilige Installer-Paket auch direkt als ZIP-Download aus der Web UI bereit.

Ubuntu-Docker-Verifikation:

```bash
npm run docker:test:ubuntu
```

Hinweis fuer Datei-Connectoren:

- Die Datei-Connectoren arbeiten lokal mit `basePath`, `importPath`, `exportPath` und `archivePath`.
- Das Linux-Setup legt dafuer standardmaessig einen SFTP-Drop unter `/var/lib/sf-integration-agent/sftp/<user>/drop` an und verlinkt ihn nach `artifacts/files`.
- Damit koennen externe Systeme Dateien per SFTP ablegen, waehrend der Agent unveraendert ueber seine vorhandenen Datei-Pfade arbeitet.

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
