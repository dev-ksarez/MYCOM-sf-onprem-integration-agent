# Release Notes 0.2.53

**Release Date**: 18. Mai 2026  
**Release Type**: Feature / UI / Project Governance  
**Priority**: Recommended for project-layer and KI assistant installations

## Overview

Dieses Release erweitert die Projektverwaltung, die Setup-Versionierung und den KI-Assistenten fuer bestehende Scheduler-Konfigurationen. Es verbessert zudem die Lesbarkeit der Mapping- und Projektansichten.

## Highlights

- KI-Assistent kann bestehende Scheduler anhand einer Scheduler-ID erkennen und aktualisieren.
- SQL-Quelle und Mapping werden bei KI-Anpassungen gemeinsam aktualisiert.
- Diff-Anzeige zeigt Aenderungen tabellarisch und hebt neue SQL-Spalten hervor.
- Scheduler-Ansicht wird nach Speichern ueber den KI-Assistenten neu geladen.
- Mappingmanager ist kompakter; Details bleiben standardmaessig zugeklappt.
- Projektverwaltung zeigt Setup-Versionen mit Datum und Benutzer.
- Deployment kann gezielt nach Bestandteilen gestartet werden.
- Setup-Versionen speichern ein Artefakt und eine Beschreibung der Aenderungen.
- KI-Vorschlag fuer Setup-Versionsbeschreibung basiert auf dem Diff zur vorherigen Version.
- Projektpanel ist reduziert auf Metainformationen und aktuellen Health-Status.

## Technical Changes

- `AISchedulerService` unterstuetzt Update-Modus fuer bestehende Scheduler.
- Mapping-Merge behandelt DSL- und JSON-Mapping und ersetzt bestehende Zielzuordnungen sauber.
- Projekt-Setup-Versionen speichern Artefakte unter `artifacts/project-setup-versions`.
- Deployment-Runs speichern `sourceVersionId` und `deployItems`.
- Neuer Endpoint fuer Versionsbeschreibung:
  - `POST /api/admin/projects/:projectId/setup/version-note-suggestion`

## Validation

- `npm run build`
- `npm run spec:validate`

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.53"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
