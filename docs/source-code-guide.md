# Source Code Guide

Dieses Dokument ist die technische Einstiegskarte fuer Entwickler, die am SF On-Prem Integration Agent arbeiten. Es beschreibt die wichtigsten Module, Laufzeitfluesse, Erweiterungspunkte und Regeln fuer sichere Aenderungen.

## Runtime-Ueberblick

Der Code ist in drei Prozesse geschnitten:

- `src/agent-main.ts`: startet den Agent-Dienst fuer Scheduler, Jobs, Connectoren und Agent-API.
- `src/web-main.ts`: startet die Web UI und Admin-API.
- `src/updater-main.ts`: startet den Update-Dienst.

Der Legacy-Einstieg `src/main.ts` bleibt aus Kompatibilitaetsgruenden erhalten. Neue Betriebslogik sollte aber entlang der getrennten Prozesse entstehen.

## Modulkarte

| Pfad | Verantwortung |
| --- | --- |
| `src/agent/` | Scheduler-Laufzeit, Job-Ausfuehrung, Agent-API, Health-Pulse und Salesforce-Control-Plane. |
| `src/clients/salesforce/` | Salesforce-Zugriff, SOQL, Run-/Log-/Checkpoint-Objekte, Metadata-Operationen. |
| `src/core/job-runner/` | Fachlicher Transferablauf von Quelle ueber Mapping zum Ziel. |
| `src/core/mapping-dsl/` | Parser, Typen und Engine fuer Mapping-Regeln. |
| `src/core/scheduler/` | Timing-Regeln, Due-Pruefung, Importprofil-Scheduler und Stale-Run-Policy. |
| `src/source-adapters/` | Quellen: Salesforce SOQL, MSSQL SQL, REST, Datei. |
| `src/target-adapters/` | Ziele: Salesforce, globale Picklists, MSSQL, Datei. |
| `src/connectors/` | Niedrigere Connector-Implementierungen fuer Mock und MSSQL. |
| `src/server/` | Web UI, Admin-API, Auth, Projekt-/Migrationsdaten, Installer, Template-Store. |
| `src/runtime/` | Laufzeit-Hilfen zwischen Prozessen, z. B. Remote-Agent-Client und Health-Store. |
| `src/infrastructure/` | Konfiguration und Datenbankzugriffe. |
| `src/types/` | Gemeinsame Datenvertraege fuer Jobs, Adapter, Records und Ergebnisse. |
| `src/utils/` | Gemeinsame Parser und Datei-/Query-Hilfen. |
| `scripts/` | Installations-, Release-, Update- und Salesforce-Hilfsskripte. |
| `docs/specs/` | Feature-Spezifikationen vor nicht-trivialen Aenderungen. |

## Kritische Flows

### Scheduler und Transfer

1. `src/agent/agent-service-runtime.ts` startet den Scheduler-Timer.
2. `src/agent/agent-runner.ts` laedt Salesforce-Konfigurationen, prueft Faelligkeit und startet Runs.
3. Source- und Target-Adapter werden ueber Registry/Factory-Auswahl erzeugt.
4. `src/core/job-runner/data-transfer-job.ts` fuehrt den Transfer aus:
   - Source lesen oder streamen
   - Mapping-DSL parsen
   - Lookups optional vorladen
   - Ziel in Batches schreiben
   - Checkpoint, erfolgreiche Records und Fehlerrecords zurueckgeben
5. `agent-runner.ts` persistiert Runs, Logs, Checkpoints, failed-records und optionale After-Export-Aktionen.

Neue Quellen sollten `SourceAdapter` implementieren. Wenn moeglich, zusaetzlich `readRecordStream()` anbieten, damit grosse Datenmengen nicht komplett im Speicher liegen. Neue Ziele implementieren `TargetAdapter.writeRecords()` und muessen Batch-Groessen respektieren.

### Web UI und Admin-API

`src/server/app.ts` ist noch der zentrale HTTP-Router. Neue serverseitige Funktionen sollen nicht weiter in diese Datei wachsen, sondern in kleine Module ausgelagert werden.

Bestehende Zielmodule:

- `admin-data-service.ts`: fachliche Aggregation und Mutationen fuer Projekte, Instanzen, Scheduler, Connectoren und Migrationen.
- `admin-auth.ts`: lokale und Salesforce-OIDC-Authentifizierung, Sessions, Rollen und Projektmitgliedschaften.
- `dashboard-update-service.ts`: Update-Status und Update-Trigger.
- `installer-generator.ts`: generierte Installationspakete.
- `template-library.ts`: eingebaute Vorlagen.
- `migration-ui-module.ts`, `scheduler-ui-module.ts`, `connector-ui-module.ts`, `ai-scheduler-ui-module.ts`: UI-nahe HTML/JS-Modulteile.
- `admin-ui-script.ts`: grosser bestehender Client-Script-Bestand. Neue UI-Funktionen sollten bevorzugt in kleinere Module wandern.

### Update-Flow

`src/updater/update-coordinator.ts` definiert Status- und Request-Dateien sowie Manifest-Handling. `src/updater/updater-service-runtime.ts` fuehrt Updates aus. Windows-Update-Skripte validieren SHA256, erstellen Backups und fuehren Rollback aus.

## Sicherheitsregeln

- Keine Default-Credentials in produktiver Konfiguration. Lokale Admin-Passwoerter werden als `scrypt`-Hash gespeichert.
- Secrets gehoeren in Env-Dateien, Secret Stores oder Docker Secrets, nicht in Git.
- `docker-compose.separated-hosts.yml` erwartet explizite Agent-Tokens und laedt nicht die komplette `.env` in beide Container.
- Mutierende Web-Routen muessen Auth, Rollen/Projektzugriff und CSRF beachten.
- Agent-API ist ein interner Betriebs-Endpunkt. Neue Agent-API-Funktionen brauchen Tokenpruefung, Rate Limit und moeglichst engen Scope.
- JSON-Body-Groessen werden zentral begrenzt. Neue Body-Reader duerfen nicht unlimitiert puffern.
- Datei-Connectoren muessen innerhalb der konfigurierten Roots bleiben. Pfadvalidierung nicht umgehen.
- Salesforce-/HTTP-Calls brauchen Timeouts. Fuer neue externe Calls einen Timeout-Wrapper verwenden.
- Bei SQL/SOQL dynamische Identifier nur aus kontrollierten Feldern zulassen; Werte immer escapen oder parameterisieren.

## Performance-Regeln

- Grosse Quellen sollen `readRecordStream()` implementieren.
- Batch-Groessen aus `TransferContext.batchSize` respektieren.
- Keine Salesforce-Abfragen in Schleifen, wenn Bulk-SOQL moeglich ist.
- UI-Polling-Endpunkte sollten cachen oder aggregieren, statt mehrere teure Salesforce-Calls pro Refresh zu erzeugen.
- Datei- und Upload-Verarbeitung muss `FILE_CONNECTOR_MAX_FILE_BYTES` respektieren.
- `DataTransferJob` puffert weiterhin, wenn Lookup-Preload fuer Mapping-Lookups noetig ist. Fuer sehr grosse Lookup-Jobs sollte eine Lookup-Strategie mit paginiertem Preload oder Zielsystem-Cache bevorzugt werden.

## Erweiterungspunkte

### Neue Source

1. Datei unter `src/source-adapters/<system>/` anlegen.
2. `SourceAdapter` implementieren.
3. Optional `readRecordStream()` implementieren.
4. Delta-Definitionen ueber `src/utils/query-source-definition.ts` nutzen, falls die Quelle inkrementell arbeiten kann.
5. Factory/Registry-Stelle in `agent-runner.ts` oder dem bestehenden Auswahlpfad ergaenzen.
6. Mindestens Build und einen realistischen Trockenlauf pruefen.

### Neues Target

1. Datei unter `src/target-adapters/<system>/` anlegen.
2. `TargetAdapter.writeRecords(records, context)` implementieren.
3. Pro Record ein `ConnectorResult` zurueckgeben.
4. Retrybarkeit und externe Keys sauber setzen.
5. Fehlertext kurz halten, aber genug Kontext fuer Logs liefern.

### Neue Admin-API

1. Fachlogik in ein Service-Modul, nicht direkt in `app.ts`.
2. Route in `app.ts` nur als duenne HTTP-Schicht.
3. Auth/CSRF/Projektzugriff pruefen.
4. Audit-History fuer Mutationen schreiben.
5. Eingaben normalisieren und Limits setzen.

### Neue UI-Funktion

1. Wenn moeglich eigenes UI-Modul unter `src/server/*-ui-module.ts`.
2. Gemeinsamen Rahmen aus `ui-template.ts` und Asset-Auslieferung aus `asset-server.ts` nutzen.
3. Keine neue grosse Inline-Logik in `admin-ui-script.ts`, ausser es gibt keinen vertretbaren kleinen Schnitt.

## Daten und Artefakte

- `artifacts/`: Laufzeitdaten, generated installer, failed records, lokale Admin-User, Health-Dateien.
- `.data/` und `data/`: lokale SQLite-Dateien.
- `logs/`: Update- und Laufzeitstatus.
- `src/public/` und `src/css/`: statische Assets fuer die Web UI.

Runtime-Daten duerfen nicht als Source-of-Truth fuer Produktcode behandelt werden. Bei neuen persistenten Artefakten immer klaeren, ob sie in Git ignoriert, gesichert oder migriert werden muessen.

## Verifikation

Vor jedem Push mindestens:

```bash
npm run build
```

Je nach Aenderung zusaetzlich:

```bash
npm audit --omit=dev
npm run docker:test:separated-hosts
npm run win:build-package
```

Bei Salesforce-relevanten Aenderungen immer die betroffenen Objekte, Felder und Permission-Sets gegen eine Sandbox pruefen.

## Bekannte Restarbeiten

- `src/server/app.ts`, `src/server/admin-data-service.ts` und `src/server/admin-ui-script.ts` sind weiterhin zu gross. Neue Arbeit sollte sie verkleinern, nicht vergroessern.
- `sqlite3` haengt aktuell an Advisory-Ketten ueber `node-gyp`/`tar`. Ein Upgrade auf `sqlite3@6` oder ein Wechsel auf `better-sqlite3` muss separat getestet werden.
- MSSQL- und Datei-Quellen liefern aktuell noch Arrays. Die Streaming-Schnittstelle ist vorbereitet; weitere Adapter sollten sukzessive umgestellt werden.
- Mapping-Lookup-Preload puffert bei Lookup-Jobs weiterhin alle Source Records. Fuer sehr grosse Lookup-Jobs braucht es eine cache- oder window-basierte Strategie.
