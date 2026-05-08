---
connie-publish: true
connie-title: Assistenten
tags:
  - sf-onprem-integration-agent
  - assistenten
---

# Assistenten

Die Assistenten fuehren Administratoren durch zusammenhaengende Konfigurations- und Betriebsablaeufe. Sie reduzieren manuelle Einzelschritte und machen technische Abhaengigkeiten zwischen Connectoren, Schedulern und Migrationen sichtbar.

## Unterthemen

| Unterthema | Inhalt |
| --- | --- |
| [Connector-Assistent](./01-connector.md) | Technische Endpunkte anlegen, testen und Schedulern zuordnen |
| [Scheduler-Assistent](./02-scheduler.md) | Integrationsprofile zeitlich steuern und Laeufe kontrollieren |
| [Migrations-Assistent](./03-migration.md) | Einmalige Datenuebernahmen vorbereiten, pruefen und ausfuehren |

## Zusammenspiel

| Schritt | Komponente | Ergebnis |
| --- | --- | --- |
| 1 | Connector-Assistent | Technische Endpunkte und Zugriffsdaten werden gepflegt. |
| 2 | Scheduler-Assistent | Wiederkehrende Integrationslaeufe nutzen die gepflegten Connectoren. |
| 3 | Migrations-Assistent | Einmalige Datenuebernahmen koennen Parameter und Mappings vorbereiten. |
| 4 | Agent Runtime | Connectoren und Scheduler werden ausgefuehrt. |
| 5 | Runs, Logs, Checkpoints | Ergebnisse werden fuer Betrieb und Nachvollziehbarkeit gespeichert. |

Connectoren bilden die technischen Endpunkte. Scheduler nutzen diese Connectoren fuer wiederkehrende Integrationen. Migrationen dienen kontrollierten Einmaluebernahmen und koennen als Vorlage fuer spaetere Scheduler-Konfigurationen verwendet werden.
