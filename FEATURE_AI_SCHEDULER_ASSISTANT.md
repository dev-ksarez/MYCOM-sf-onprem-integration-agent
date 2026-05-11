# KI-Scheduler-Assistent Feature

## 📋 Überblick

Dieses Feature implementiert einen KI-basierten Assistenten zur **automatischen Generierung von Scheduler-Konfigurationen** aus natürlichsprachigen Benutzer-Anforderungen.

Stand ab **v0.2.40**:
- Der KI-Bereich wurde über den Scheduler-Assistenten hinaus erweitert.
- Zusätzlich gibt es eine KI-gestützte Dashboard-Analyse und eine Salesforce-fokussierte KI-Migrationsanalyse.
- Aus der Migrationsanalyse kann direkt ein Migrationsprofil als Entwurf erstellt werden.

**Hybride Architektur:**
- **Phase 1 (Rule-based)**: 100% kostenlos, läuft lokal
  - Keyword-Analyse des Prompts
  - Intelligente Connector-Auswahl
  - Standard-Mapping-Generierung
  - Timing-Erkennung

- **Phase 2 (Optional LLM)**: Ollama/Claude für komplexe Fälle
  - Verfeinerung bei niedriger Konfidenz
  - Erweiterte Anforderungsanalyse

## 🏗️ Architektur

### Neue Services

```
src/server/
├── ai-scheduler-service.ts      # KI-Logik & Generierung (Rule-based)
├── ai-scheduler-ui-module.ts    # UI-Rendering für Frontend
├── ai-dashboard-analyzer.ts      # KI-Analyse fuer Dashboard-Gesamtzustand
├── ai-migration-analyzer.ts      # KI-Analyse fuer Migration + Salesforce-Mapping
└── app.ts                        # API-Endpoint /api/ai/generate-scheduler
```

Zusätzliche API-Endpoints:

- `POST /api/ai/analyze-dashboard`
- `POST /api/ai/analyze-migration-source`

### API-Endpoint

**POST** `/api/ai/generate-scheduler`

```json
{
  "userPrompt": "Alle Kontakte aus MSSQL nach Salesforce, täglich 22 Uhr",
  "connectorId": "optional-connector-id",
  "targetSystem": "Salesforce",
  "existingConnectors": []
}
```

**Response:**

```json
{
  "schedule": {
    "name": "MSSQL → Salesforce (Contact)",
    "sourceSystem": "MSSQL",
    "targetSystem": "Salesforce",
    "objectName": "Contact",
    "operation": "Upsert",
    "direction": "Inbound",
    "sourceType": "MSSQL_SQL",
    "targetType": "SALESFORCE",
    "batchSize": 100,
    "connectorId": "conn-123",
    "mappingDefinition": "Email;string=Email;NONE\nFirstName;string=FirstName;NONE\n...",
    "timingDefinition": "{...}"
  },
  "confidence": 0.85,
  "reasoning": "Quelle: MSSQL • Ziel: Salesforce • Object: Contact • Operation: Upsert • Connector: MSSQL DB • Konfidenz: 85%",
  "issues": [],
  "requiresUserValidation": false
}
```

## 🎯 Features

### Benutzer-Anforderungs-Analyse

Der Assistent erkennt automatisch:

- **Quell-System**: MSSQL, REST API, Salesforce, Dateien, SAGE/ERP, etc.
- **Ziel-System**: Salesforce, REST API, MSSQL, etc.
- **Objekt-Name**: Account, Contact, Lead, Order, Opportunity, Custom Objects
- **Operation**: Insert, Update, Upsert, Delete
- **Timing**: Täglich, stündlich, wöchentlich, monatlich mit Uhrzeitangabe
- **Richtung**: Inbound/Outbound basierend auf Quelle und Ziel

### Salesforce-fokussierte Migrationsanalyse (neu)

Die KI-Migrationsanalyse unterstützt jetzt direkt mehrere Salesforce-Zielobjekte:

- Account
- Contact
- Lead
- Opportunity
- Order
- Product (technisch: Product2)
- ProductPrice (technisch: PricebookEntry)

Ergebnis der Analyse:

- Vorschlag des passenden Salesforce-Zielobjekts
- Feld-Mapping-Vorschläge auf Salesforce-Standardfelder
- Datenschutzhinweise für sensitive Felder
- Pflichtfeld-Check (Ampel: GRUEN/GELB/ROT)

Bei der Übernahme in ein Profil:

- Die UI erzeugt direkt eine Draft-Migration aus den KI-Vorschlägen.
- Fehlende Pflichtfelder werden vorab angezeigt und bestätigt.

### KI-Dashboardanalyse (neu)

Die Dashboard-KI erzeugt aus den vorhandenen Betriebsmetriken:

- Health-Score (0-100)
- Zustand (Gesund, Stabil, Beobachten, Kritisch)
- Kurz-Zusammenfassung
- Konkrete Empfehlungen zu Laufzeiten, Fehlern und Datenwuchs

### Automatische Konfiguration

✅ **Connector-Auswahl**
- Bester Match basierend auf Quellsystem
- Fallback zu verfügbaren Connectoren

✅ **Mapping-Generierung**
- Object-spezifische Standard-Mappings
- Source-Feld-Preview
- Format: `targetField;dataType=sourceField;TRANSFORMS`

✅ **Timing-Berechnung**
- Parsing von natürlichsprachigen Zeitangaben
- Automatische Wochentags-Konfiguration
- Nächsten Lauf berechnen

✅ **Source/Target-Definitionen**
- REST API: Endpoint & Auth-Einstellungen
- MSSQL: SQL Query mit Delta-Support
- FILE: Pfad- und Format-Konfiguration

### Konfidenz-System

```
Confidence = (Anzahl erkannter Features / 5) + Connector-Bewertung

< 0.5  = Warnung: Manuelle Überprüfung empfohlen
0.5-0.75 = Warnung + Issues-Liste
> 0.75 = Grünes Licht
```

## 🚀 Verwendung

### UI Integration

Der Assistent ist im neuen Tab **"KI-Assistent"** verfügbar neben Connector/Scheduler.

**Workflow:**
1. Anforderung in Textfeld eingeben
2. Optional: Connector & Ziel-System spezifizieren
3. "Konfiguration generieren" klicken
4. Preview & Issues überprüfen
5. "Speichern und verwenden" → Direct zur Scheduler-Nutzung

### Migrationsanalyse-Workflow (neu)

1. Im Migration-Tab Quelle und Zielobjekt (Salesforce) wählen.
2. Optional Datei hochladen, um Felddefinitionen automatisch zu erkennen.
3. KI-Analyse starten.
4. Pflichtfeld-Ampel und Mapping-Vorschläge prüfen.
5. "Migrationsprofil aus Analyse erstellen" klicken.
6. Entwurf wird im Migrationswizard geöffnet und kann direkt verfeinert werden.

### Beispiel-Prompts

```
"Alle Accounts aus unserer MSSQL-Datenbank nach Salesforce synchronisieren,
täglich um 08:00 Uhr. Email als eindeutige ID verwenden."

"REST-API Kundenaktualisierungen nach Salesforce Contact Object.
Stündliche Synchronisation."

"Salesforce Kontakte mit Newsletter-Opt-in nach Brevo exportieren.
Täglicher Export um 22 Uhr."
```

## 📊 Komponenten

### `AISchedulerService`

**Hauptlogik:**
- `generateScheduler()` - Entry Point
- `analyzePrompt()` - NLP-basierte Anforderungs-Analyse
- `matchBestConnector()` - Connector-Intelligenz
- `generateBaseSchedule()` - Scheduler-Template
- `validateScheduleConfiguration()` - Validierung & Issues

### `renderAISchedulerAssistantModule()`

**UI-Rendering:**
- Prompt-Input mit Beispielen
- Connector/System-Auswahl
- Live-Preview der Konfiguration
- Mapping-Vorschau
- Issues-Warnung

## 🔮 Geplante Features (Phase 2)

- [ ] Ollama-Integration für Verfeinerung
- [ ] Multi-Turn Conversation (Nutzer verfeinert Anforderung)
- [ ] Machine Learning aus bisherigen Syncs
- [ ] Custom Business-Rules Engine
- [ ] Neue Connector-Suggestion
- [ ] Komplexe Feld-Transformationen

## 🧪 Testing

```bash
# Build
npm run build

# Dev-Server starten
WEB_UI_ENABLED=1 npm run dev

# Dann im Browser:
# http://localhost:9010/
# Tab: KI-Assistent
```

## 📝 Entwickler-Notes

### Sicherheit
- ✅ Keine API-Keys/Secrets im Prompt gespeichert
- ✅ Nur lokale Rule-basierte Analyse (kein externen LLM bei Phase 1)
- ✅ Generierte Configs müssen manuell validiert werden

### Performance
- ✅ Analyse komplett synchron, <100ms
- ✅ Keine DB-Queries während Generierung
- ✅ Connector-Liste gecacht

### Fehlerbehandlung
- ✅ Fallback zu Defaults bei Parse-Fehlern
- ✅ Konfidenz-Score hilft bei Unsicherheit
- ✅ Detaillierte Issues-Liste für Debugging

## 🔗 Verwandte Dateien

- [ai-scheduler-service.ts](./ai-scheduler-service.ts) - Core-Logik
- [ai-scheduler-ui-module.ts](./ai-scheduler-ui-module.ts) - Frontend
- [ai-dashboard-analyzer.ts](./src/server/ai-dashboard-analyzer.ts) - Dashboard-KI
- [ai-migration-analyzer.ts](./src/server/ai-migration-analyzer.ts) - Migrations-KI
- [app.ts](./app.ts) - API-Routing
- [admin-data-service.ts](./admin-data-service.ts) - Daten-Persistierung

## 📚 Weitere Infos

- **Branch:** `feature/ai-scheduler-assistant`
- **Status:** MVP (Phase 1)
- **Kostenmodell:** Kostenlos (Rule-based) + Optional LLM
