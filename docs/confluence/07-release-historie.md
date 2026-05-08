---
connie-publish: true
connie-title: 07 - Release Historie
tags:
  - sf-onprem-integration-agent
  - release
  - changelog
---

# Release Historie

Diese Seite fasst die publizierten Versionen des SF On-Prem Integration Agent zusammen. Detaillierte technische Release Notes liegen im Repository als `RELEASE_NOTES_<version>.md`.

## Aktuelle Version

| Version | Schwerpunkt | Betriebswirkung |
| --- | --- | --- |
| `0.2.21` | Scheduler-UI, Scheduler-Konfiguration, Intervalloptimierung | Kritische Fixes fuer Mapping-Zustand, korrigierte Contact-/Product2-Scheduler und reduzierte Org-Speicherlast |

## Versionsuebersicht

| Version | Inhalt |
| --- | --- |
| `0.2.21` | Fix fuer Scheduler Modal State Contamination, korrigierte Scheduler-Konfigurationen, optimierte Scheduler-Intervalle |
| `0.2.20` | Modularisierung von App Services und Mapping DSL |
| `0.2.19` | Windows-PowerShell-Hotfix fuer pipeline-freie Rollen-Normalisierung in `install-agent-service.ps1` |
| `0.2.18` | Windows-PowerShell-Hotfix fuer korrekt geklammerte Rollen-Normalisierung |
| `0.2.17` | Windows-PowerShell-Hotfix fuer nicht unterstuetzte `||`-Syntax |
| `0.2.16` | Trennung der Windows-Rollen `agent-host` und `web-host` |
| `0.2.15` und aelter | Vorherige Iterationen der Agent-, Installer-, Scheduler- und Update-Funktionen |

## Upgrade-Hinweise fuer `0.2.21`

1. Neue Anwendungsversion deployen.
2. Scheduler-Definitionen aktualisieren oder korrigierte Artefakte erneut importieren.
3. Agent-Dienst neu starten, damit gecachte Scheduler-Konfigurationen neu aus Salesforce geladen werden.
4. Naechste geplante Contact-, Account-, Product- und Pricebook-Laeufe pruefen.
5. Salesforce-Speicherverbrauch beobachten, insbesondere bei hohen Run- und Log-Volumina.

## Bekannte Hinweise

- Scheduler-Definitionen werden zur Laufzeit gecacht. Nach Konfigurationsaenderungen ist ein Agent-Neustart erforderlich, bis Cache-Invalidierung implementiert ist.
- Product-Scheduler mit 0 Datensaetzen sollte ueber Source Query und Delta Checkpoint diagnostiziert werden.

## Pflegeprozess

Bei jedem Release sollten folgende Artefakte aktualisiert werden:

- `package.json` Version.
- Git Tag, z. B. `v0.2.21`.
- `RELEASE_NOTES_<version>.md`.
- Diese Confluence-Seite mit Kurzfassung, Betriebswirkung und Upgrade-Hinweisen.
