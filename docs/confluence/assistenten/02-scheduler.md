---
connie-publish: true
connie-title: 06.2 - Assistent Scheduler
tags:
  - sf-onprem-integration-agent
  - assistent
  - scheduler
---

# Assistent Scheduler

![Assistent Scheduler](../assets/screenshots/09-assistent-scheduler.png)

Der Scheduler-Assistent verwaltet die zeitliche Ausfuehrung von Importen und Exporten. Er verbindet Zeitsteuerung, Richtung, Connectoren, Mapping und Laufkontrolle zu einem ausfuehrbaren Integrationsprofil.

## Ablauf

```mermaid
flowchart TD
  A[Scheduler oeffnen] --> B[Profil und Richtung festlegen]
  B --> C[Source- und Target-Connector waehlen]
  C --> D[Mapping und Filter setzen]
  D --> E[Intervall und Zeitfenster definieren]
  E --> F{Aktiv?}
  F -->|Ja| G[Due Run wird eingeplant]
  F -->|Nein| H[Profil bleibt pausiert]
  G --> I[Overlap-Schutz prueft laufende Runs]
  I --> J{Kein aktiver Run?}
  J -->|Ja| K[Job Runner startet Lauf]
  J -->|Nein| L[Run wird blockiert oder uebersprungen]
```

## Kernfunktionen

| Funktion | Beschreibung |
| --- | --- |
| Aktivstatus | Steuert, ob ein Scheduler automatisch ausgefuehrt wird. |
| Intervall | Definiert wiederkehrende Ausfuehrungen, z. B. alle 15 Minuten oder taeglich. |
| Zeitfenster | Begrenzt Ausfuehrungen auf fachlich freigegebene Zeitraeume. |
| Manuelle Ausfuehrung | Startet einen einzelnen Lauf ausserhalb des normalen Intervalls. |
| Overlap-Schutz | Verhindert parallele Laeufe desselben Profils. |
| Run-Protokoll | Schreibt Status, Zaehler, Fehler und Laufzeiten nach Salesforce. |

## Technische Wirkung

Ein aktiver Scheduler erzeugt faellige Runs, sobald Zeitfenster und Intervall passen. Die Runtime liest die zugeordneten Connectoren und Mapping-Regeln, startet den Job Runner und persistiert Ergebnisdaten als Runs, Logs und Checkpoints.
