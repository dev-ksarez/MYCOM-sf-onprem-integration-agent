---
connie-publish: true
connie-title: 06.2 - Assistent Scheduler
tags:
  - sf-onprem-integration-agent
  - assistent
  - scheduler
---

# Assistent Scheduler

![Assistent Scheduler](../../assets/screenshots/09-assistent-scheduler.png)

Der Scheduler-Assistent verwaltet die zeitliche Ausfuehrung von Importen und Exporten. Er verbindet Zeitsteuerung, Richtung, Connectoren, Mapping und Laufkontrolle zu einem ausfuehrbaren Integrationsprofil.

## Ablauf

| Schritt | Aktion | Ergebnis |
| --- | --- | --- |
| 1 | Scheduler oeffnen | Neues oder bestehendes Integrationsprofil wird bearbeitet. |
| 2 | Profil und Richtung festlegen | Import, Export oder anderer Laufkontext wird definiert. |
| 3 | Source- und Target-Connector waehlen | Technische Endpunkte werden mit dem Profil verbunden. |
| 4 | Mapping und Filter setzen | Feldzuordnung und Datenselektion werden festgelegt. |
| 5 | Intervall und Zeitfenster definieren | Automatische Ausfuehrung wird zeitlich begrenzt. |
| 6 | Aktivstatus setzen | Aktive Profile koennen faellige Runs erzeugen. |
| 7 | Overlap-Schutz pruefen | Laufende Runs verhindern parallele Ausfuehrung desselben Profils. |
| 8 | Job Runner starten | Gueltige faellige Runs werden ausgefuehrt. |
| 9 | Run protokollieren | Status, Zaehler, Fehler und Checkpoints werden gespeichert. |

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
