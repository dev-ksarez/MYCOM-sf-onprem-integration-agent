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
- `npm run init:installation -- --mode SAGE100` - Basis-Importprofile fuer Erstinstallation anlegen
- `npm run sf:deploy-metadata` - Salesforce Metadaten deployen

## Kunden-Deployment (Windows)

Das detaillierte Runbook steht in [WINDOWS_DEPLOYMENT.md](WINDOWS_DEPLOYMENT.md).

Kurzablauf:

1. Release-Paket auf Zielserver entpacken
2. `.env` konfigurieren
3. Optional: `npm run init:installation -- --mode SAGE100`
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
