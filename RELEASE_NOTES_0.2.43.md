# Release Notes 0.2.43

## Highlights

- **Scheduler-UI vereinfacht**: `Source System` und `Source Type` sind jetzt automatisch aus dem Connector abgeleitet und read-only.
- **Nur bei File-Connectors editierbar**: `Source Type` bleibt für File-Connectors (CSV/JSON/Excel) bearbeitbar.
- **Visuelle Hinweise**: Labels zeigen an, ob Feld auto-bestimmt oder editierbar ist.
- **Konsistente Autofill-Logik**: Connector-Wechsel, Wizard-Navigation und Modal-Öffnung triggern sofort die richtige Policy.

## Technische Änderungen

### Source System & Type Field Policy
- Neue Funktion `applyScheduleSourceFieldPolicy(connectorId)`:
  - `Source System`: Immer **disabled** (vom Connector abgeleitet)
  - `Source Type`: 
    - Für File-Connectors: **editierbar** (Benutzer wählt CSV/JSON/Excel)
    - Für alle anderen Connectors: **disabled** (auto-bestimmt)
  - Label-Hinweise werden dynamisch ergänzt:
    - "_(vom Connector abgeleitet)_"
    - "_(nur für File-Quellen editierbar)_"

### Integration in Event-Handler
- **Connector-Wechsel** (`sch-connector` change event):
  - Ruft `applyScheduleSourceFieldPolicy()` auf → Felder direkt read-only/enabled
  
- **Wizard Schritt 2** (Quelle):
  - Fallback-Autofill + Field-Policy beim Eintritt
  
- **Modal-Öffnung** (`openScheduleModal`):
  - Field-Policy wird beim Laden des Schedule gesetzt

### UX-Verbesserung
- **Weniger Freiheitsgrade**: User kann nur noch das ändern, was tatsächlich sinnvoll ist
- **Schnelleres Setup**: Keine Fehl-Entries mehr durch fehlende oder falsche Source-Werte
- **Selbsterklärender**: Labels zeigen sofort, warum ein Feld nicht editierbar ist

## Verifizierung

- TypeScript-Build erfolgreich (`npm run build`)
- Field-Policy ist defensiv: Unbekannte Connectors werden nicht blockiert
- Vollständig rückwärtskompatibel
- Agent-Betrieb bleibt unbeeinflusst

## Migration Guide

Keine Aktion erforderlich. Bestehende Schedules funktionieren weiterhin.
- Source System & Type werden automatisch beim nächsten Modal-Öffnen korrekt gesetzt
- Bestehende Definitionen bleiben erhalten (JSON-Kompatibilität)

## Bekannte Hinweise

- Field-Policy wird nur im Scheduler-Modal angewendet (Admin-UI)
- Für Custom-Connectors ohne erkannten Typ: Source Type bleibt editierbar als Fallback
- Labels werden bei Connector-Wechsel direkt aktualisiert (real-time)

## Kompatibilität

- Vollständig rückwärtskompatibel mit allen bestehenden Scheduler-Konfigurationen
- REST-API: Keine Änderungen; Schedules können weiterhin manuell Source System/Type setzen (wird aber gelocked, wenn Modal geöffnet wird)
