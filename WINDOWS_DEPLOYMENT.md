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

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run init:installation -- --mode SAGE100
```

Optional direkt aktivieren:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run init:installation -- --mode SAGE100 --activate
```

### 3) Dienst installieren

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:install-service -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

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
npm run win:install-service -- -AppRoot "C:\apps\sf-onprem-integration-agent"
npm run win:register-updater -- -EveryMinutes 15 -AppRoot "C:\apps\sf-onprem-integration-agent"
```

## Manuelles Update (on demand)

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-now -- -ServiceName "SfOnpremIntegrationAgent" -UpdateManifestUrl "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json" -AppRoot "C:\apps\sf-onprem-integration-agent"
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

Optional mit bereits enthaltenen Abhaengigkeiten (groesseres Paket):

```powershell
npm run win:build-package:with-node-modules
```
