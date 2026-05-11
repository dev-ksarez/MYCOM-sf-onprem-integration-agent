# Release Notes 0.2.41

## Highlights

- Neue KI-Agentenanalyse im Dashboard mit Health-Score, Status, Zusammenfassung und konkreten Empfehlungen.
- KI-Migrationsanalyse ist jetzt klar auf Salesforce-Ziele ausgerichtet (inkl. Account, Contact, Lead, Opportunity, Order, Product und ProductPrice).
- Aus der KI-Analyse kann direkt ein Migrationsprofil als Entwurf erstellt und im Wizard weiterbearbeitet werden.

## Technische Aenderungen

- Neuer Dashboard-Analyzer: KI-Auswertung ueber zentralen Endpoint fuer Laufzeiten, Fehlerbild und Datenwuchs.
- Migration-Analyzer erweitert:
  - Zielobjekt-Erkennung und Mapping-Logik fuer mehrere Salesforce-Objekte.
  - Verbesserte Feldvorschlaege fuer Salesforce-Standardfelder statt generischer Suffix-Ziele.
- Migration-UI erweitert:
  - Zielobjekt-Auswahl in der KI-Analyse.
  - Ampel-Check (GRUEN/GELB/ROT) fuer Pflichtfeld-Abdeckung pro Zielobjekt.
  - Warn-/Bestaetigungsdialog bei fehlenden Pflichtfeldern vor Profilerstellung.
- Stabilitaetsfix fuer Admin-UI-Skript:
  - Syntaxfehler durch korrektes Escaping von Newline-Sequenzen im serverseitig gerenderten Browser-JavaScript behoben.

## Verifikation

- TypeScript-Build erfolgreich (`npm run build`).
- Ausgeliefertes Browser-Skript syntaktisch validiert (`node --check` gegen `/assets/admin-ui.js`).
- Dev-Server erfolgreich gestartet (Web UI auf Port 9010 verfuegbar).
