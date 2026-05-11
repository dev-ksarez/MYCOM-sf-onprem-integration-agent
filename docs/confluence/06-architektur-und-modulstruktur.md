---
connie-publish: true
connie-title: 06 - Architektur und Modulstruktur
tags:
  - sf-onprem-integration-agent
  - architektur
  - module
---

# Architektur und Modulstruktur

## Ziel

Die Web- und Agent-Codebasis ist so aufgebaut, dass fachliche Erweiterungen als Module an gemeinsame Dienste andocken koennen. Dadurch sollen Admin-, Migration-, Reporting- oder spaetere PDF-Funktionen nicht jeweils eigene Basislogik fuer Navigation, Assets, Audit, Authentifizierung oder HTML-Rahmen duplizieren.

## Modulprinzipien

- Fachlogik und UI-Auslieferung werden getrennt.
- Wiederverwendbare HTML-, CSS- und JavaScript-Bausteine werden ueber zentrale Templates und Assets ausgeliefert.
- Neue Module registrieren Navigation, Berechtigungsbedarf und Einstiegspunkte zentral.
- Typ- und Validierungslogik wird in gemeinsamen Helpern gehalten, nicht mehrfach in UI und Runtime nachgebaut.
- Experimentelle Funktionen werden nicht dauerhaft in der Produktbasis gehalten, wenn sie nicht produktiv betrieben werden.

## Zentrale Module

| Modul | Aufgabe |
| --- | --- |
| `src/server/app.ts` | HTTP-Einstieg, Auth-Gates, Request-Orchestrierung und Delegation. |
| `src/server/app-modules.ts` | Registrierung von UI-/Fachmodulen und Aufbau von Sidebar-/Tab-Navigation. |
| `src/server/asset-server.ts` | Zentrale Auslieferung versionierter statischer Assets. |
| `src/server/ui-template.ts` | Wiederverwendbarer HTML-Dokumentrahmen fuer serverseitig gerenderte Seiten. |
| `src/server/admin-ui-script.ts` | Auslieferung des Admin-UI-JavaScripts als eigenes Asset. |
| `src/server/admin-data-service.ts` | Zentrale Aggregation und Mutation der Admin-Daten. |
| `src/server/audit-history-service.ts` | Persistenz und Abfrage der Audit-Historie. |
| `src/types/file-schedule-type.ts` | Gemeinsame Normalisierung und Pruefung von Datei-Schedule-Typen. |

## UI-Assets und Templates

Login- und Callback-Scripte liegen unter `src/public/`, Login-CSS unter `src/css/login.css`. Neue UI-Funktionen sollten eigene Assets oder serverseitige Template-Funktionen verwenden, statt JavaScript oder CSS direkt in grosse HTML-Strings einzubetten.

Der serverseitige HTML-Rahmen wird ueber `src/server/ui-template.ts` erzeugt. So koennen neue Seiten einheitlich Stylesheets, Scripte, Meta-Daten und Sicherheitsattribute verwenden.

## Assistenten und Typverhalten

Connector- und Scheduler-Assistenten zeigen Felder typabhaengig:

- SQL-Connectoren zeigen Secret-Key-Felder.
- Datei-, REST- und Salesforce-Connectoren blenden SQL-spezifische Secret-Felder aus.
- Datei-Ziele (`FILE_CSV`, `FILE_EXCEL`, `FILE_JSON`) blenden Zielobjekt, Operation, Zielsystem-Pflichtfeld und Mapping-Auswahl aus.
- Datei-Ziele nutzen serverseitige Defaults fuer `Target System`, `Objekt` und `Operation`.

Diese Regeln reduzieren Fehlkonfigurationen und verhindern, dass fachlich nicht passende Werte in Salesforce persistiert werden.

## PDF-Rueckbau

Das experimentelle PDF-Modul wurde aus Release `0.2.23` entfernt. Entfernt wurden Generator-Code, Templates, Connector-Schema, Beispieljob, Specs und API-Reste. Eine spaetere PDF-Funktion soll als neues Modul ueber die gemeinsame Modulstruktur angebunden werden.

## Erweiterung neuer Module

Neue Module sollten mindestens festlegen:

- Modul-ID, Anzeigename und Beschreibung.
- benoetigte Berechtigungen.
- Navigationseintrag oder Tab-Integration.
- benoetigte API-Endpunkte.
- eigene Assets oder Template-Bausteine.
- Audit-Ereignisse fuer mutierende Aktionen.

Fachliche Runtime-Logik bleibt ausserhalb der UI-Module und wird ueber dedizierte Services oder Adapter angebunden.
