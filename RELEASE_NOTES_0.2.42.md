# Release Notes 0.2.42

## Highlights

- **Scheduler-Modal-Autofill robuster**: Connector-abhängige `Source System` und `Source Type` werden jetzt zuverlässig erkannt, auch bei unterschiedlichen Schreibweisen (z. B. „Datei", „REST API", „MS-SQL").
- **File-Target-Optionen in Admin-UI**: Dateiname (mit Platzhaltern), Charset, Separator, Textqualifier und Excel Sheet-Name sind jetzt direkt im Scheduler konfigurierbar.
- **SQL-Analyzer für AI-Scheduler erweitert**: Neuer SQL-Traffic-Light (ROT/GELB/GRÜN) und Auto-Fix für `SELECT *` und fehlende `WHERE`-Klauseln.
- **Error-Analysis-Fehlerfeld-Extraktion optimiert**: Robuste Erkennung von Lookup-Fehlern; Scheduler-Link in Error-Modal zum direkten Mapping-Fix.
- **Delta-Suggestion in AI-Scheduler**: Automatische Erkennung geeigneter Delta-Felder und Übernahme in die Konfiguration.

## Technische Änderungen

### Scheduler-Autofill
- Neue Funktion `pickFirstAvailableSelectValue()`:
  - Matching über Optionswert UND Optionstext (Label).
  - Normalisierung: Whitespace, Unterstriche, Bindestriche werden ignoriert.
  - Defensive Absicherung: Leere Optionen werden übersprungen.
- Erweiterte Connector-Aliases:
  - File: `File`, `FILE`, `Datei`, `Dateisystem`
  - REST: `REST API`, `REST_API`, `REST`, `API`
  - SQL: `MS SQL`, `MSSQL`, `MS-SQL`, `SQL`, `Datenbank`
- Autofill wird ausgelöst bei:
  - Connector-Wechsel (`sch-connector` change event)
  - Eintritt in Wizard-Schritt 2 (Fallback)
  - Öffnen des Scheduler-Modal (Force-Modus)

### File-Transfer & Target-Datei-Optionen
- Neue UI-Felder in Scheduler-Modal Tab 3 (Ziel):
  - `sch-target-file-name`: Dateiname mit Platzhaltern (`${date}`, `${time}`, `${datetime}`)
  - `sch-target-file-charset`: UTF-8, Windows-1252, Latin-1, UTF-16 LE (Default: utf8)
  - `sch-target-file-delimiter`: CSV-Separator (Default: `;`)
  - `sch-target-file-text-qualifier`: Anführungszeichen für CSV (Default: `"`)
  - `sch-target-file-sheet-name`: Excel-Blattname (Default: `Sheet1`)
- Dateiname-Auto-Generierung:
  - Intelligent-Dirty-Tracking: automatische Dateinamen werden nicht überschrieben, wenn User manuell eingreift
  - Template: `export_${date}_${time}.csv|json|xlsx`
- `file-transfer.ts` erweitert:
  - `textQualifier` Unterstützung in `FileTransferDefinition`
  - `escapeCsvValue()` nutzt konfigurierbaren Qualifier
  - Platzhalter-Auflösung (`resolveDateTimePlaceholders()`) beim Datei-Write

### AI-Scheduler Service
- SQL-Extraktion aus Prompts:
  - Unterstützt SQL-Codeblöcke (` ```sql ... ``` `)
  - Unterstützt `SQL:` Präfix
  - Fallback auf inline `SELECT`/`WITH`-Pattern
- Neue Service-Property: `sqlQuery` in Analysis-Ergebnis
- MSSQL-Connector nutzt `queryText` (statt ehemaliger `query`)

### AI-Scheduler-UI Module
- Neue Abschnitte im Result-Preview:
  - **Source Preview**: Zeigt SQL-Abfrage oder Source-Definition
  - **SQL-Traffic-Light**: ROT (Fehler), GELB (Warnung), GRÜN (OK)
  - **SQL Auto-Fix Button**: Ersetzt `SELECT *` durch explizite Spalten
  - **Delta-Suggestion Button**: Übernimmt erkannte Delta-Felder
- Verfeinern-Button erweitert:
  - Testet Source-Definition gegen Connector
  - Lädt Quellfelder
  - Generiert Mapping-Regeln (für Salesforce-Ziele)
  - Erkennt Delta-Kandidaten
- Beispiel-Prompts erweitert um konkrete SQL-Beispiel

### Error-Analysis
- `extractRootCause()` robuster:
  - Bereinigung von Präfixen (`[TAG]`, `RECORD_ERROR |` etc.)
  - Längere Auszüge (bis 180 Zeichen)
- `extractAffectedFields()` erweitert:
  - Pattern für Lookup-Felder: `lookup for Contact.AccountId`
  - Pattern für Retry-Logik: `for Field (retryable`
- Spezifische Recommendations für Lookup-Fehler (External-ID, Upsert)
- `generateSuggestedFix()` kontextabhängig für Lookups

### Error-Analysis Modal
- Neuer Button „Scheduler anpassen":
  - Öffnet Scheduler mit vorgewähltem Schritt 5 (Mapping)
  - Fokussiert automatisch auf betroffenes Feld (erste `affectedField`)
  - Hilfreich zur schnellen Fehlerdiagnose

## Verifizierung

- TypeScript-Build erfolgreich ohne Fehler (`npm run build`)
- Admin-UI-Autofill-Logik ist testiert auf defensive Edge-Cases
- Dev-Server läuft stabil (Port 9010)
- Agent-Betrieb nicht beeinflusst (reines Admin-UI-Feature)

## Kompatibilität

- Vollständig rückwärtskompatibel
- Scheduler-Definitionen (JSON) erweitert um optionale File-Optionen
- Fallback-Verhalten: Fehlende Optionen nutzen sinnvolle Defaults (utf8, `;`, `"`, `Sheet1`)

## Bekannte Einschränkungen

- SQL-Traffic-Light und Auto-Fix sind nur für MSSQL_SQL / SALESFORCE_SOQL sichtbar
- Delta-Suggestion basiert auf Feldnamen-Heuristik (LastModifiedDate, Timestamp, RowVersion)
- Error-Analysis-Fokus auf Mapping-Fehler optimiert (Lookup, Required Field); Andere Fehlertypen erhalten generische Hinweise
