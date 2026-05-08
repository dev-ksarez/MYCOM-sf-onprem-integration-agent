---
connie-publish: true
connie-title: 06 - Assistenten
tags:
  - sf-onprem-integration-agent
  - assistenten
---

# Assistenten

Die Assistenten fuehren Administratoren durch zusammenhaengende Konfigurations- und Betriebsablaeufe. Sie reduzieren manuelle Einzelschritte und machen technische Abhaengigkeiten zwischen Connectoren, Schedulern und Migrationen sichtbar.

## Unterthemen

| Unterthema | Inhalt |
| --- | --- |
| [Connector-Assistent](./assistenten/01-connector.md) | Technische Endpunkte anlegen, testen und Schedulern zuordnen |
| [Scheduler-Assistent](./assistenten/02-scheduler.md) | Integrationsprofile zeitlich steuern und Laeufe kontrollieren |
| [Migrations-Assistent](./assistenten/03-migration.md) | Einmalige Datenuebernahmen vorbereiten, pruefen und ausfuehren |

## Zusammenspiel

```mermaid
flowchart LR
  C[Connector-Assistent] --> S[Scheduler-Assistent]
  M[Migrations-Assistent] --> C
  S --> R[Runs, Logs, Checkpoints]
  C --> A[Agent Runtime]
  A --> R
```

Connectoren bilden die technischen Endpunkte. Scheduler nutzen diese Connectoren fuer wiederkehrende Integrationen. Migrationen dienen kontrollierten Einmaluebernahmen und koennen als Vorlage fuer spaetere Scheduler-Konfigurationen verwendet werden.
