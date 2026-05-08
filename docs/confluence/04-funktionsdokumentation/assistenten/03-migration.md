---
connie-publish: true
connie-title: 06.3 - Assistent Migration
tags:
  - sf-onprem-integration-agent
  - assistent
  - migration
---

# Assistent Migration

![Assistent Migration](../../assets/screenshots/10-assistent-migration.png)

Der Migrations-Assistent unterstuetzt einmalige oder vorbereitete Datenmigrationen. Er eignet sich fuer Dateiimporte, Salesforce-Zielobjekte und kontrollierte Uebernahmen mit Preflight-Pruefung und Ergebnisprotokoll.

## Ablauf

| Schritt | Aktion | Ergebnis |
| --- | --- | --- |
| 1 | Migration anlegen | Neuer Migrationsentwurf wird erstellt. |
| 2 | Datei oder Salesforce-Quelle waehlen | Datenquelle fuer die Uebernahme wird festgelegt. |
| 3 | Objekt und Feldmapping planen | Zielobjekt, Pflichtfelder und Zuordnungen werden definiert. |
| 4 | Salesforce-Login pruefen | Zugriff und Berechtigungen werden migrationsbezogen validiert. |
| 5 | Preflight ausfuehren | Datei, Mapping und Zielobjekt werden vor dem Import geprueft. |
| 6 | Fehler korrigieren | Fehlende Pflichtfelder oder Mappingfehler werden bereinigt. |
| 7 | Migration starten | Gueltige Datensaetze werden kontrolliert importiert. |
| 8 | Ergebnisse protokollieren | Fortschritt, Zaehler und Fehler werden nachvollziehbar dokumentiert. |
| 9 | Fehlerhafte Datensaetze nachbearbeiten | Problemfaelle koennen exportiert oder erneut verarbeitet werden. |

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
