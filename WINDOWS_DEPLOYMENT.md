# Windows Deployment Runbook

Dieses Runbook beschreibt die Kundeninstallation als Windows-Dienst inklusive Auto-Update mit Rollback.

## Zielbild

- Dienstname: `SfOnpremIntegrationAgent`
- Starttyp: automatisch
- Laufzeit: `node dist/main.js`
- Updates: über GitHub Releases
- Fallback: automatischer Rollback bei fehlerhaftem Update

## Voraussetzungen beim Kunden

- Windows Server oder Windows 10/11
- Lokale Administratorrechte
- Node.js 22+ in `PATH`
- Ausgehender Zugriff auf:
	- `https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases`
	- Salesforce (`login`/`instance` URL)
	- ggf. MSSQL-Server
- AppRoot-Ordner, z. B. `C:\apps\sf-onprem-integration-agent`
- Konfigurationsdatei `.env` (aus `.env.example` ableiten)

## Variante A (empfohlen): Deployment mit `node_modules`

Diese Variante ist robuster in abgeschotteten Netzen, da kein `npm install` beim Kunden notwendig ist.

Das Kundenpaket enthaelt jetzt auch `nssm.exe`, damit die Dienstinstallation ohne separate NSSM-Vorinstallation laeuft.

### One-Click Start aus dem Kundenpaket

Das erzeugte Kundenpaket enthaelt im ZIP-Wurzelverzeichnis jetzt zusaetzlich:

- `install-customer-package.cmd`
- `install-customer-package.ps1`

Nach dem Entpacken kann der Kunde direkt `install-customer-package.cmd` starten. Falls der Start nicht bereits mit Administratorrechten erfolgt, fordert das Skript diese automatisch per UAC an. Danach kopiert es die Anwendung nach `C:\apps\sf-onprem-integration-agent`, oeffnet auf Wunsch eine interaktive `.env`-Konfiguration und startet die eigentliche Dienstinstallation.

### 1) Release-Inhalt auf Zielserver kopieren

Mindestens enthalten:

- `dist/`
- `package.json`
- `scripts/windows/`
- `node_modules/`

### 2) `.env` konfigurieren

Beispielbasis siehe `.env.example`.

Optional fuer den Basis-Installationsmodus SAGE100:

- `SAGE100_ACCOUNT_EXTERNAL_ID_FIELD` (Default: `AccountNumber`)
- `SAGE100_CONTACT_EXTERNAL_ID_FIELD` (Default: `Email`)

### 2.1) Basis-Importprofile fuer SAGE100 einrichten

Dieser Schritt legt bei der Erstinstallation zwei Schedule-Templates in Salesforce an:

- `SAGE100 - KHKAdressen -> Account`
- `SAGE100 - KHKAnsprechpartner -> Contact`

**Wichtig:** Dieser Schritt deployed automatisch auch die erforderlichen Salesforce Metadaten (Custom Objects, Fields) in Ihre Org. Sie benötigen dafür kein separates `npm run sf:deploy-metadata` Skript.

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run init:installation -- --mode SAGE100
```

Bei diesem Schritt werden interaktiv abgefragt:

- Salesforce Login URL, Client ID, Client Secret
- SAGE100 SQL Server Host, Port, Datenbank, User, Passwort

Während der Ausführung:

1. ✓ Authentifizierung bei Salesforce erfolgt
2. ✓ Salesforce Metadaten werden deployed (Custom Objects erstellt/aktualisiert)
3. ✓ `MSD_Connector__c` fuer MSSQL wird angelegt/aktualisiert
4. ✓ Schedule-Templates werden erstellt und mit dem Connector verknuepft

Die Werte werden in `.env` gespeichert.

Optional direkt aktivieren (Schedules starten sofort):

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run init:installation -- --mode SAGE100 --activate
```

### 3) Dienst installieren

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install-service -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Oder als kompletter Installationslauf in einem Schritt:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Das Skript prueft die wichtigsten Voraussetzungen, installiert bei Bedarf Abhaengigkeiten, legt optional eine `.env` aus `.env.example` an, installiert den Windows-Dienst und registriert den Auto-Updater.

### 3.1) Schnelle Neuinstallation eines vorhandenen Dienstes

Wenn ein bereits installierter Dienst mit aelteren Installer-Staenden angelegt wurde, ist die schnellste saubere Korrektur meist:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:uninstall-service
npm run win:install-service -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Danach optional den Updater erneut setzen:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:register-updater -- -EveryMinutes 15 -AppRoot "C:\apps\sf-onprem-integration-agent"
```

### 3.2) Direkte Reparatur eines bereits installierten NSSM-Dienstes

Wenn nur die Umgebungsvariablen des Windows-Dienstes korrigiert werden sollen, ohne komplette Neuinstallation:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:repair-service-env -- -AppRoot "C:\apps\sf-onprem-integration-agent" -WebUiPort 8080 -SchedulerIntervalMs 60000 -RestartService
```

Dieses Skript setzt `WEB_UI_ENABLED=1`, `WEB_UI_PORT` und `SCHEDULER_INTERVAL_MS` direkt am vorhandenen NSSM-Dienst neu.

Zusatz: Mit `-PromptForEnv` koennen die wichtigsten Betriebswerte direkt im Installer abgefragt werden, zum Beispiel:

- `AGENT_ID`
- `LOG_LEVEL`
- `WEB_UI_PORT`
- `SCHEDULER_INTERVAL_MS`
- `SF_LOGIN_URL`, `SF_CLIENT_ID`, `SF_CLIENT_SECRET`
- `SAGE100_SQL_SERVER`, `SAGE100_SQL_PORT`, `SAGE100_SQL_DATABASE`, `SAGE100_SQL_USER`, `SAGE100_SQL_PASSWORD`
- `MSSQL_DEV_PASSWORD` fuer bestehende Connectoren mit Secret Key `MSSQL_DEV_PASSWORD`

Die Abfragen sind jetzt fuer Endkunden in deutscher Sprache formuliert und enthalten kurze Erklaerungen sowie sinnvolle Defaults.

Vor dem Start des Windows-Dienstes prueft der Installer ausserdem die Secret Keys der aktiven Salesforce-Connectoren. Fehlt z. B. ein benoetigter Wert wie `MSSQL_DEV_PASSWORD`, stoppt die Installation vor dem Dienststart mit einem klaren Hinweis im Installer-Log.

### 4) Auto-Updater als Scheduled Task registrieren

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:register-updater -- -EveryMinutes 15 -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Die Standard-Manifest-URL ist bereits gesetzt auf:

`https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json`

## Variante B: Deployment ohne `node_modules`

Diese Variante ist kleiner, benötigt aber einmalig Paketinstallation beim Kunden.

### 1) Release-Inhalt auf Zielserver kopieren

Mindestens enthalten:

- `dist/`
- `package.json`
- `package-lock.json`
- `scripts/windows/`

### 2) Abhängigkeiten auf dem Zielsystem installieren

```powershell
cd C:\apps\sf-onprem-integration-agent
npm ci --omit=dev
```

### 3) `.env` konfigurieren

Beispielbasis siehe `.env.example`.

### 4) Dienst installieren und Updater registrieren

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install -- -AppRoot "C:\apps\sf-onprem-integration-agent" -InstallDependencies
```

Mit interaktiver `.env`-Erfassung:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install -- -AppRoot "C:\apps\sf-onprem-integration-agent" -InstallDependencies -PromptForEnv
```

Alternativ weiterhin getrennt:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install-service -- -AppRoot "C:\apps\sf-onprem-integration-agent"
npm run win:register-updater -- -EveryMinutes 15 -AppRoot "C:\apps\sf-onprem-integration-agent"
```

## Manuelles Update (on demand)

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-now -- -ServiceName "SfOnpremIntegrationAgent" -UpdateManifestUrl "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json" -AppRoot "C:\apps\sf-onprem-integration-agent"
```

## Update einer bereits vorhandenen Windows-Installation

Fuer eine bestehende Kundeninstallation gibt es jetzt einen vereinfachten Wrapper, der standardmaessig auf das neueste GitHub Release zeigt:

Wichtig fuer bestehende Installationen vor Version 0.2.2:

- der Befehl `npm run win:update-existing` ist dort noch nicht im bereits installierten `package.json` vorhanden
- deshalb das aktuelle Release-ZIP herunterladen und den enthaltenen Launcher aus dem ZIP-Wurzelverzeichnis starten

Direkt aus dem entpackten Release-Paket:

```powershell
cd <entpacktes-release-verzeichnis>
.\update-existing-installation.ps1 -AppRoot "C:\apps\sf-onprem-integration-agent" -ReleaseVersion "0.2.2"
```

Oder per CMD-Launcher:

```cmd
update-existing-installation.cmd -AppRoot "C:\apps\sf-onprem-integration-agent" -ReleaseVersion "0.2.2"
```

Sobald die Installation auf 0.2.2 oder neuer ist, funktioniert auch der npm-Aufruf direkt aus dem installierten Verzeichnis:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-existing -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Gezielt auf eine bestimmte Release-Version aktualisieren:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-existing -- -AppRoot "C:\apps\sf-onprem-integration-agent" -ReleaseVersion "0.2.2"
```

Wenn nach dem Update auch der geplante Auto-Updater auf dieselbe Manifest-URL umgestellt werden soll:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-existing -- -AppRoot "C:\apps\sf-onprem-integration-agent" -ReleaseVersion "0.2.2" -ReRegisterUpdaterTask
```

## Was beim Update automatisch passiert

- Manifest laden (`version`, `packageUrl`, `sha256`)
- ZIP herunterladen
- SHA256 prüfen (falls vorhanden)
- Dienst stoppen
- Backup nach `backups/<timestamp>` erstellen
- Dateien ersetzen (`dist`, `package.json`, optional `node_modules`)
- Dienst starten und Running prüfen
- Bei Fehler: automatischer Rollback

## Betriebschecks nach Installation

```powershell
Get-Service SfOnpremIntegrationAgent
Get-ScheduledTask -TaskName "SfOnpremIntegrationAgent-Updater"
```

Erwartung:

- Service-Status: `Running`
- Scheduled Task vorhanden und aktiviert

## Optional: Salesforce Metadaten einmalig bereitstellen

Wenn die benötigten Custom Objects im Ziel-Org noch nicht existieren:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run sf:deploy-metadata
```

Benötigte Umgebungsvariablen für diesen Schritt:

- `SF_LOGIN_URL`
- `SF_CLIENT_ID`
- `SF_CLIENT_SECRET`
- `SF_USERNAME`
- `SF_PASSWORD`

## Deinstallation

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:uninstall-service
```

## Paket-Erstellung fuer den Vertrieb

Im Projektverzeichnis kannst du das Kunden-Installationspaket lokal erstellen:

```powershell
npm run build
npm run win:build-package
```

Ausgabe:

- `artifacts/sf-onprem-integration-agent-customer-installer-<version>.zip`

Das ZIP enthaelt jetzt einen Startpunkt fuer Kundeninstallationen direkt im Wurzelverzeichnis.

Optional mit bereits enthaltenen Abhaengigkeiten (groesseres Paket):

```powershell
npm run win:build-package:with-node-modules
```
