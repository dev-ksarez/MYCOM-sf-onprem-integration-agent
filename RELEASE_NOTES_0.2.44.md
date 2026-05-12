# Release Notes 0.2.44

## Highlights

- **Startup-Syntaxfehler behoben**: Fehler bei `Invalid regular expression` und `Unexpected identifier 'renderScheduleRecentLogs'` wurden im Admin-UI-Scripting beseitigt.
- **KI-Scheduler Testflow robuster für MSSQL**: Der KI-Assistent akzeptiert jetzt sowohl `MSSQL` als auch `MSSQL_SQL` konsistent für Vorschau/Test und Feldmetadaten.
- **Fehleranalyse-Texte verbessert**: Kein `Unknown` mehr in den Handlungsempfehlungen; stattdessen aussagekräftige Fallback-Bezeichnungen.
- **Fehlerkategorie im Modal lokalisiert**: Kategorien wie `mapping_error` werden in der UI jetzt als deutsche Labels angezeigt.
- **Datei-Scheduler präzisiert**: Textqualifier wird beim CSV-Einlesen korrekt berücksichtigt und Dateinamen-Platzhalter funktionieren jetzt in Read **und** Write.

## Technische Änderungen

### Admin UI / Script Stabilisierung
- Überflüssige Klammer in `openScheduleModal` entfernt, wodurch `await` wieder im korrekten Async-Kontext ausgeführt wird.
- Template-Escaping im AI-Scheduler-UI-Modul korrigiert:
  - RegEx-Escaping für `SELECT *`-Erkennung in gerendertem Browser-Script.
  - Newline-Escaping (`\\n`) in Script-Strings fixiert.

### KI-Scheduler: MSSQL-Kompatibilität
- `AI Scheduler Service` setzt bei MSSQL-Connectoren den Source Type nun explizit auf `MSSQL_SQL`.
- Backend-APIs für Source-Vorschau und Feldmetadaten normalisieren `MSSQL` auf `MSSQL_SQL`.
- KI-UI behandelt `MSSQL` und `MSSQL_SQL` gleichwertig als SQL-Quelle im Test-/Mapping-Flow.

### KI-Fehleranalyse / UX
- Empfehlungstexte nutzen bereinigte Systemlabels (z. B. `Quellsystem`, `Zielsystem`) statt `Unknown`.
- Fehleranalyse-Modal zeigt lokalisierte Kategorie-Badges:
  - `mapping_error` -> `Mapping-Fehler`
  - `authentication_failed` -> `Authentifizierungsfehler`
  - `connector_unavailable` -> `Connector nicht erreichbar`
  - `data_validation` -> `Datenvalidierung`
  - `network_issue` -> `Netzwerkproblem`
  - `quota_exceeded` -> `Quota überschritten`

### Datei-Transfer / Scheduler-Dateien
- CSV-Parser berücksichtigt jetzt den konfigurierten `textQualifier` beim Einlesen.
- Dateiname-/Dateipfad-Platzhalter (`${date}`, `${time}`, `${datetime}` sowie `%DATE%`, `%TIME%`, `%DATETIME%`) werden jetzt beim Lesen und Schreiben aufgelöst.

## Verifizierung

- TypeScript-Build erfolgreich (`npm run build`).
- Script-Parsing der gerenderten UI-Module erfolgreich (Admin UI und AI Scheduler Script).
- Smoke-Test für File-Placeholder erfolgreich:
  - Read: `account-export-${date}.csv` wurde korrekt aufgelöst.
  - Write: `write-check-${date}_${time}.csv` wurde korrekt erzeugt.

## Kompatibilität

- Rückwärtskompatibel zu bestehenden Scheduler-Definitionen.
- Bestehende Scheduler mit Source Type `MSSQL` werden im Vorschau-/Testkontext kompatibel behandelt.

## Hinweis

- Dieses Release fokussiert auf Stabilität, KI-Usability und Datei-Scheduler-Korrekturen ohne Breaking Changes an öffentlichen API-Endpunkten.
