# Release 0.2.10

- Scheduler robuster gemacht: MSSQL-Upsert kann jetzt scheduler-spezifisch konfiguriert werden und bricht bei echten Konfigurationsfehlern frueher ab
- Salesforce-Deltalauf korrigiert: SOQL-Paginierung ueber alle Seiten und stabile Sortierung fuer Delta-Checkpoints
- Monitoring verbessert: Mini-Gauge fuer Runs und Scheduler mit Unknown-Total-Anzeige waehrend laufender Jobs
- PricebookEntry sicherer gemacht: sichtbares Pricebook2Id-Feld im Scheduler, ProductCode-Guardrails und Dry-Run-Pruefung inklusive Salesforce-Existenzcheck
- Dashboard-Update-Status bereinigt: veraltete fehlgeschlagene Update-Staende bleiben nicht mehr als laufender Fortschritt haengen