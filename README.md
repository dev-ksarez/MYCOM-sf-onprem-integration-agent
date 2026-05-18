# SF On-Prem Integration Agent

Salesforce-gesteuerter On-Prem Integration Agent auf Basis von Node.js und TypeScript.

Der Agent holt Konfigurationen aus Salesforce, liest Daten aus Quellsystemen (z. B. MSSQL), mapped sie in ein kanonisches Format und schreibt sie in Zielsysteme (Salesforce Objekt oder Picklist).

## Features

- Salesforce als Steuerungsebene fuer Schedules, Runs und Logging
- KI-gestuetzte Dashboard-Gesamtanalyse (Laufzeiten, Fehler, Datenwuchs, Health-Score, Empfehlungen)
- KI-gestuetzte Migrationsanalyse mit Salesforce-Zielobjektvorschlaegen
- Direktes Erstellen eines Migrationsprofils aus der KI-Analyse (als Draft im Wizard)
- Pflichtfeld-Validierung fuer Salesforce-Zielobjekte mit Ampelstatus (GRUEN/GELB/ROT)
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

## KI-Unterstuetzung in der Web UI

Die Web UI enthaelt einen KI-Assistenten fuer zwei Kernbereiche:

1. Scheduler-Assistent
- Generiert Scheduler-Konfigurationen aus natuerlicher Sprache.
- Ermittelt Quelle, Ziel, Objekt, Richtung, Operation und Timing.

2. Dashboard- und Migrationsanalyse
- Dashboard: Gesamtbewertung des Agent-Zustands mit Score, Status, Zusammenfassung und Empfehlungen.
- Migration: Salesforce-fokussierte Quellanalyse inkl. Mapping-Vorschlaegen und Datenschutz-Hinweisen.
- Zielobjekte: Account, Contact, Lead, Opportunity, Order, Product (Product2), ProductPrice (PricebookEntry).
- Ein Klick erstellt aus der Analyse direkt ein Migrationsprofil als Entwurf.
- Vor Profilerstellung zeigt die UI einen Pflichtfeld-Check mit Ampelstatus und fehlenden Pflichtfeldern.

Detaildokumentation zum Scheduler-Assistenten und den KI-Erweiterungen:

- [FEATURE_AI_SCHEDULER_ASSISTANT.md](FEATURE_AI_SCHEDULER_ASSISTANT.md)
- [RELEASE_NOTES_0.2.44.md](RELEASE_NOTES_0.2.44.md)

## Voraussetzungen

- Node.js 22+
- npm
- Zugriff auf Salesforce und ggf. MSSQL

Fuer geschuetzte Admin-Zugaenge der Web UI:

- ADMIN_UI_USERNAME
- ADMIN_UI_PASSWORD

Optional fuer Rollenmodell und Salesforce-Login:

- `ADMIN_AUTH_MODE=local|salesforce_oidc`
- `ADMIN_UI_USERS_JSON` oder `ADMIN_UI_USERS_FILE`
- `SF_IDP_LOGIN_URL`
- `SF_IDP_CLIENT_ID`
- `SF_IDP_CLIENT_SECRET`
- `SF_IDP_REDIRECT_URI`

## Lokale Entwicklung

```bash
npm ci
npm run build
npm run dev:agent
npm run dev:web
npm run dev:updater
```

Entwicklung mit ts-node:

```bash
npm run dev
```

Hinweis:

- `npm run dev` bzw. `npm run start` starten aus Rueckwaertskompatibilitaetsgruenden weiter den Legacy-Kombiprozess.
- Fuer die Zielarchitektur mit getrennten Diensten stehen bereit:
  - `npm run start:agent`
  - `npm run start:web`
  - `npm run start:updater`
  - `npm run dev:agent`
  - `npm run dev:web`
  - `npm run dev:updater`

## Spec-Driven Development

Fuer nicht-triviale Aenderungen wird zuerst eine Spec unter `docs/specs/` angelegt und erst danach implementiert.

Schnellstart:

```bash
npm run spec:new -- "Kurzer Feature-Titel"
npm run spec:validate
```

Die ausfuehrliche Einfuehrung steht in [SPEC_DRIVEN_DEVELOPMENT.md](SPEC_DRIVEN_DEVELOPMENT.md), die laufenden Specs und die Vorlage in [docs/specs/README.md](docs/specs/README.md).

## Modulgrenzen Web UI

Die grosse Server- und UI-Datei wird inkrementell aufgeteilt. Neue Funktionen sollen als andockbare Module auf gemeinsame Services, Templates und Assets zugreifen, statt neue Inline-Bloecke in `src/server/app.ts` aufzubauen.

- Dashboard-Update-Logik: `src/server/dashboard-update-service.ts`
- Migration Listen-/Import-UI: `src/server/migration-ui-module.ts`
- Zentrale Aggregation und Mutationen: `src/server/admin-data-service.ts`
- App-Modulregistrierung und Navigation: `src/server/app-modules.ts`
- Statische Web-Assets: `src/server/asset-server.ts`
- Wiederverwendbarer HTML-Dokumentrahmen: `src/server/ui-template.ts`
- Admin-UI-JavaScript-Auslieferung: `src/server/admin-ui-script.ts`
- Audit-Historie: `src/server/audit-history-service.ts`

Neue serverseitige und UI-nahe Logik sollte bevorzugt in diesen Modulen oder weiteren kleinen Nachfolgemodulen landen. Das gilt insbesondere fuer kuenftige Migration-, Admin-, Reporting- oder PDF-Module.

## Zielarchitektur Dienste

Das Projekt wird kuenftig entlang von drei fachlichen und betrieblichen Rollen geschnitten:

1. `Agent-Dienst`
   Fuehrt Scheduler, Datenimporte, Exporte, Mapping und Connector-Ausfuehrung aus.
2. `WebServer-Dienst`
   Stellt Web UI, Admin-API, Dashboard, Installer-UI und Betriebsansichten bereit.
3. `AutoUpdater-Dienst`
   Prueft Releases, migriert Bestandsinstallationen und fuehrt Updates kontrolliert mit Backup und Rollback aus.

Wichtig:

- Die Dreiteilung soll nicht nur im Code sichtbar sein, sondern auch in Installer, Release-Paket und Upgrade-Workflow.
- Bestehende Installationen muessen per Updater von der heutigen kombinierten Struktur auf die Zielstruktur migriert werden.
- Die laufende Spezifikation dazu steht unter [docs/specs/2026-05-06-drei-dienste-release-und-updater-zielarchitektur/spec.md](docs/specs/2026-05-06-drei-dienste-release-und-updater-zielarchitektur/spec.md).
- Agent und Web koennen jetzt auch auf unterschiedlichen Hosts laufen, wenn der Web-Host gegen die Agent-API konfiguriert wird.

## Rollen und Berechtigungen

Die Web-Authentifizierung unterstuetzt jetzt serverseitig durchgesetzte Rechte:

- `read`
- `write`
- `delete`
- `admin`

Benutzer koennen lokal in der Web UI im Tab `Admin` gepflegt werden. Persistiert wird standardmaessig in `artifacts/admin-users.json` oder in der per `ADMIN_UI_USERS_FILE` gesetzten Datei. Fuer Docker ist `./artifacts:/app/artifacts` gemountet, damit Benutzer und Historie Neustarts ueberleben.

Neuinstallationen und Windows-Updates legen bei Bedarf automatisch eine Bootstrap-Datei `artifacts/admin-users.json` an und ergaenzen `.env` um `ADMIN_UI_USERS_FILE=artifacts/admin-users.json`. Vorhandene Benutzerdateien und Betriebsdaten bleiben dabei erhalten.

Standardzuordnung:

- `viewer` -> `read`
- `editor` -> `read`, `write`
- `admin` -> `read`, `write`, `delete`, `admin`

Beispiel fuer `ADMIN_UI_USERS_JSON`:

```json
[
  {
    "id": "ops-admin",
    "username": "admin@example.com",
    "password": "starkes-passwort",
    "roles": ["admin"],
    "modules": ["migration", "projects", "deployment"]
  },
  {
    "id": "ops-viewer",
    "username": "viewer@example.com",
    "roles": ["viewer"],
    "permissions": ["read"],
    "modules": []
  }
]
```

Aktuell verfuegbare Modulberechtigung:

- `migration` fuer das Migrationsmodul

Konfigurationsaenderungen an Instanzen, Templates, Schedulern, Connectoren, Migrationen und Admin-Benutzern werden mit Benutzer und Zeitstempel in `artifacts/audit-history.json` protokolliert und im Admin-Tab angezeigt.

Im Modus `salesforce_oidc` wird das Passwort lokal nicht verwendet. Die lokale Benutzerliste dient dann als Rollen- und Berechtigungszuordnung fuer Salesforce-Benutzer.

## Salesforce als Identitätsprovider

Ja, Salesforce kann hier als Identitätsprovider genutzt werden.

Aktuell umgesetzt ist eine OIDC-basierte Admin-Anmeldung fuer die Web UI:

- `ADMIN_AUTH_MODE=salesforce_oidc`
- `SF_IDP_LOGIN_URL=https://login.salesforce.com` oder Sandbox-Login-URL
- `SF_IDP_CLIENT_ID`
- `SF_IDP_CLIENT_SECRET`
- optional `SF_IDP_REDIRECT_URI`

Wichtig:

- Der Benutzer muss in `ADMIN_UI_USERS_JSON` oder `ADMIN_UI_USERS_FILE` hinterlegt sein.
- Die lokale Benutzerdefinition mappt Salesforce-Benutzer auf Rollen und Rechte.
- Ohne lokale Freigabe wird ein erfolgreicher Salesforce-Login trotzdem nicht autorisiert.

## Agent und Web auf getrennten Servern

Fuer getrennte Hosts gibt es jetzt eine vorbereitete Agent-API.

Auf dem Agent-Host:

- `AGENT_API_ENABLED=1`
- `AGENT_API_PORT=8090`
- `AGENT_API_TOKEN=<shared-secret>`

Auf dem Web-Host:

- `AGENT_REMOTE_BASE_URL=http://agent-host:8090`
- `AGENT_REMOTE_TOKEN=<shared-secret>`

Darueber laufen:

- Health-Abfrage des Agenten
- Update-Status
- Update-Anforderung an den Agent-/Updater-Host

## Installationsprofile fuer getrennte Hosts

Windows:

- Vollinstallation:
  - `powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile all`
- Agent-Host:
  - installiert nur den Agent-Dienst `SfOnpremIntegrationAgent`
  - `powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile agent-host`
- Web-Host:
  - installiert Web UI und AutoUpdater als `SfOnpremIntegrationWeb` und `SfOnpremIntegrationUpdater`
  - `powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile web-host`

Linux:

- Vollinstallation:
  - `sudo bash scripts/linux/install-linux-agent.sh --roles agent,web,updater`
- Agent-Host:
  - `sudo bash scripts/linux/install-linux-agent.sh --roles agent,updater`
- Web-Host:
  - `sudo bash scripts/linux/install-linux-agent.sh --roles web`

Persistenz:

- Das gewaehlte Rollenprofil wird lokal unter `artifacts/runtime/install-profile.json` gespeichert.
- Der Updater verwendet dieses Profil spaeter fuer die erneute Dienstkonfiguration.

## Wichtige Skripte

- `npm run build` - TypeScript Build
- `npm run start` - Legacy-Kombistart aus `dist/main.js`
- `npm run start:agent` - Getrennter Start des Agent-Dienstes
- `npm run start:web` - Getrennter Start des WebServer-Dienstes
- `npm run start:updater` - Getrennter Start des AutoUpdater-Dienstes
- `npm run dev` - Legacy-Kombistart aus TypeScript-Quellen
- `npm run dev:agent` - Agent-Dienst aus TypeScript-Quellen
- `npm run dev:web` - WebServer-Dienst aus TypeScript-Quellen
- `npm run dev:updater` - AutoUpdater-Dienst aus TypeScript-Quellen
- `npm run spec:new -- "Titel"` - Neue Spec fuer nicht-triviale Aenderungen anlegen
- `npm run spec:validate` - Spec-Struktur in `docs/specs/` pruefen
- `npm run docker:test:ubuntu` - Ubuntu-basierter Docker-Verifikationstest fuer Build + Specs
- `npm run win:install-service` - Windows-Dienst installieren
- `npm run win:uninstall-service` - Windows-Dienst deinstallieren
- `npm run win:update-now` - Manuelles Update ausfuehren
- `npm run win:update-existing` - Bestehende Windows-Installation auf Latest oder Ziel-Release aktualisieren
- `npm run win:register-updater` - Rueckwaertskompatibler Alias, konfiguriert jetzt den AutoUpdater-Dienst
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
4. Dienste installieren
5. Agent-, Web- und AutoUpdater-Dienst pruefen

Fuer spaetere Bestandsupdates:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-existing -- -AppRoot "C:\apps\sf-onprem-integration-agent" -ReleaseVersion "0.2.2"
```

Hinweis fuer Altstaende vor 0.2.2:

- das neue npm-Skript ist dort lokal noch nicht vorhanden
- in diesem Fall das Release-ZIP entpacken und den Launcher `update-existing-installation.ps1` oder `update-existing-installation.cmd` aus dem ZIP-Wurzelverzeichnis starten

## Linux-Deployment und gehaerteter Betrieb

Die Linux-Runbooks sind jetzt getrennt:

- [UBUNTU_LOCAL_DEPLOYMENT.md](UBUNTU_LOCAL_DEPLOYMENT.md) fuer Ubuntu im lokalen Netz
- [LINUX_DEPLOYMENT.md](LINUX_DEPLOYMENT.md) fuer den öffentlichen Linux-Server

Kurzablauf:

1. App unter einem dedizierten Service-User nach `/opt/sf-integration-agent` bereitstellen
2. Environment-Datei unter `/etc/sf-integration-agent/agent.env` pflegen
3. systemd-Dienste fuer Agent, Web und Updater sowie den nginx-Reverse-Proxy aus [scripts/linux](scripts/linux) aktivieren
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

Getrennte Host-Verifikation per Docker:

```bash
npm run docker:test:separated-hosts
```

Das Compose-Szenario gibt Secrets gezielt pro Container weiter und laedt nicht mehr die komplette lokale `.env` in beide Dienste. Vor dem Start muessen mindestens `AGENT_API_TOKEN` und `AGENT_REMOTE_TOKEN` gesetzt sein; beide Werte muessen identisch sein.

Dabei werden zwei Container gestartet:

- `agent-host`
  - startet `npm run start:agent`
  - laeuft im Test ohne Scheduler (`AGENT_SCHEDULER_ENABLED=0`), damit keine echte Salesforce-Konfiguration noetig ist
  - exponiert die Agent-API auf Port `8090`
- `web-host`
  - startet `npm run start:web` und `npm run start:updater`
  - nutzt den Agenten remote ueber `AGENT_REMOTE_BASE_URL`
  - laeuft mit deaktivierter automatischer Update-Pruefung (`AUTO_UPDATER_ENABLED=0`)
  - exponiert die Web UI auf Host-Port `18080`

Test-Login im Docker-Szenario:

- Im Produktionsmodus erzeugt Docker keinen Default-Admin mehr.
- Lege vor dem Start `artifacts/admin-users.json` an oder setze `ADMIN_UI_USERS_JSON`/`ADMIN_UI_USERS_FILE`.
- Neue oder ueber die UI gespeicherte lokale Passwoerter werden als `scrypt`-Hash persistiert.

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

Fuer lokale Admin-Anmeldung muss eine Benutzerdatei `artifacts/admin-users.json` vorhanden sein oder ueber `ADMIN_UI_USERS_JSON`/`ADMIN_UI_USERS_FILE` bereitgestellt werden. Neue oder ueber die UI gespeicherte lokale Passwoerter werden als `scrypt`-Hash persistiert.

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
