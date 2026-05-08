---
connie-publish: true
connie-title: 06.3 - Assistent Migration
tags:
  - sf-onprem-integration-agent
  - assistent
  - migration
---

# Assistent Migration

![Assistent Migration](../assets/screenshots/10-assistent-migration.png)

Der Migrations-Assistent unterstuetzt einmalige oder vorbereitete Datenmigrationen. Er eignet sich fuer Dateiimporte, Salesforce-Zielobjekte und kontrollierte Uebernahmen mit Preflight-Pruefung und Ergebnisprotokoll.

## Ablauf

```mermaid
flowchart TD
  A[Migration anlegen] --> B[Datei oder Salesforce-Quelle waehlen]
  B --> C[Objekt und Feldmapping planen]
  C --> D[Salesforce-Login je Migration pruefen]
  D --> E[Preflight ausfuehren]
  E --> F{Preflight erfolgreich?}
  F -->|Ja| G[Migration starten]
  F -->|Nein| H[Fehler korrigieren]
  H --> C
  G --> I[Fortschritt und Ergebnisse protokollieren]
  I --> J[Fehlerhafte Datensaetze exportieren oder nachbearbeiten]
```

## Funktionsumfang

| Bereich | Beschreibung |
| --- | --- |
| Dateiupload | Import von CSV, XLSX und JSON fuer Migrationslaeufe. |
| Entwurf | Speicherung von Migrationsentwuerfen mit Objekt- und Feldplanung. |
| Salesforce-Login | Anmeldung je Migration, um Zielzugriff und Berechtigungen zu pruefen. |
| Preflight | Validierung von Datei, Pflichtfeldern, Mapping und Zielobjekt. |
| Ausfuehrung | Kontrollierter Import mit Fortschritt, Zaehlern und Ergebnisstatus. |
| Fehlerbehandlung | Dokumentation fehlerhafter Datensaetze zur Nachbearbeitung. |

## Abgrenzung zum Scheduler

Migrationen sind fuer kontrollierte Einzeluebernahmen ausgelegt. Wiederkehrende Integrationen werden ueber Scheduler und Connectoren betrieben. Nach einer erfolgreichen Migration kann aus den getesteten Parametern ein regulaeres Scheduler-Profil abgeleitet werden.
