---
connie-publish: true
connie-title: ReleaseNotes
tags:
  - sf-onprem-integration-agent
  - release
  - changelog
---

# ReleaseNotes

Diese Seite fasst die publizierten Versionen des SF On-Prem Integration Agent zusammen. Detaillierte technische Release Notes liegen im Repository als `RELEASE_NOTES_<version>.md`.

## Aktuelle Version

| Version | Schwerpunkt | Betriebswirkung |
| --- | --- | --- |
| `0.2.23` | PDF-Rueckbau, modulare Web UI, Assistenten-Bereinigung | Web-Server neu starten; Datei-Ziel-Scheduler pruefen; experimenteller PDF-Generator ist nicht mehr Bestandteil des Releases |

## Versionsuebersicht

| Version | Inhalt |
| --- | --- |
| `0.2.23` | Entfernt experimentelles PDF-Modul, extrahiert Web-UI-Assets und Templates, bereinigt Scheduler-/Connector-Assistenten je Typ |
| `0.2.22` | Fix fuer leere Mappinglisten auf Remote-Agenten durch Fallback auf gespeicherte Mapping-Regeln |
| `0.2.21` | Fix fuer Scheduler Modal State Contamination, korrigierte Scheduler-Konfigurationen, optimierte Scheduler-Intervalle |
| `0.2.20` | Modularisierung von App Services und Mapping DSL |
| `0.2.19` | Windows-PowerShell-Hotfix fuer pipeline-freie Rollen-Normalisierung in `install-agent-service.ps1` |
| `0.2.18` | Windows-PowerShell-Hotfix fuer korrekt geklammerte Rollen-Normalisierung |
| `0.2.17` | Windows-PowerShell-Hotfix fuer nicht unterstuetzte `||`-Syntax |
| `0.2.16` | Trennung der Windows-Rollen `agent-host` und `web-host` |
| `0.2.15` und aelter | Vorherige Iterationen der Agent-, Installer-, Scheduler- und Update-Funktionen |

## Upgrade-Hinweise fuer `0.2.23`

1. Neue Anwendungsversion deployen.
2. Web-Server neu starten, damit ausgelagerte Assets und Admin-UI-Scripte geladen werden.
3. Datei-Ziel-Scheduler pruefen, insbesondere Target Definition und Exportpfad.
4. Falls der experimentelle PDF-Generator lokal genutzt wurde, diesen vor dem Update separat sichern.
5. Agent-Dienst neu starten, damit gecachte Scheduler-Konfigurationen neu aus Salesforce geladen werden.

## Bekannte Hinweise

- Scheduler-Definitionen werden zur Laufzeit gecacht. Nach Konfigurationsaenderungen ist ein Agent-Neustart erforderlich, bis Cache-Invalidierung implementiert ist.
- Product-Scheduler mit 0 Datensaetzen sollte ueber Source Query und Delta Checkpoint diagnostiziert werden.
- Das PDF-Modul ist in `0.2.23` bewusst entfernt. Eine spaetere PDF-Funktion sollte als neues Modul auf der gemeinsamen Modulbasis implementiert werden.

## Pflegeprozess

Bei jedem Release sollten folgende Artefakte aktualisiert werden:

- `package.json` Version.
- Git Tag, z. B. `v0.2.23`.
- `RELEASE_NOTES_<version>.md`.
- Diese Confluence-Seite mit Kurzfassung, Betriebswirkung und Upgrade-Hinweisen.
