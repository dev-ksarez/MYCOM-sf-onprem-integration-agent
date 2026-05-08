---
connie-publish: true
connie-title: 01 - Allgemeine Beschreibung
tags:
  - sf-onprem-integration-agent
  - allgemein
---

# Allgemeine Beschreibung

Der SF On-Prem Integration Agent ist eine Node.js-/TypeScript-Anwendung zur Salesforce-gesteuerten Integration lokaler Kundensysteme. Salesforce dient als Steuerungsebene fuer Scheduler, Connector-Konfigurationen, Laufprotokolle und Checkpoints. Der Agent laeuft im Kundennetz, liest Daten aus lokalen oder angebundenen Quellsystemen, transformiert sie ueber Mapping-Regeln und schreibt sie in Zielsysteme wie Salesforce-Objekte, Salesforce-Picklists, MSSQL oder Dateien.

## Einsatzszenarien

- Import lokaler ERP-/SAGE100-/MSSQL-Daten nach Salesforce.
- Export von Salesforce-Daten in lokale Datenbanken oder Dateien.
- Dateiimport und Dateiexport ueber CSV, XLSX oder JSON.
- REST-basierte Anbindung externer Systeme.
- Betriebsueberwachung ueber Web UI, Dashboard, Logs, Scheduler-Status und Update-Status.
- Kundeninstallation als Windows-Dienst oder Linux/systemd-Dienst.

## Zentrale Ziele

- Keine direkte eingehende Verbindung von Salesforce in das Kundennetz.
- Steuerung und Monitoring zentral in Salesforce.
- Lokale Datenzugriffe bleiben im Kundennetz.
- Reproduzierbare Installation, Update-Faehigkeit und Rollback.
- Erweiterbare Connector-Architektur fuer neue Quellen und Ziele.

## Rollen im System

| Rolle | Aufgabe |
| --- | --- |
| Salesforce Cloud | Fuehrt OAuth, Steuerungsdaten, Zielobjekte, Logs und Checkpoints. |
| Web-/DMZ-Host | Stellt Admin UI, Dashboard, Installer-UI und Remote-Bedienung bereit. |
| On-Prem Agent-Host | Fuehrt Scheduler, Runtime, Mapping und Connectoren aus. |
| On-Prem Systeme | Stellen lokale Datenquellen und Zielsysteme bereit. |
