# Release Notes 0.2.54

**Release Date**: 18. Mai 2026
**Release Type**: Hotfix
**Priority**: Critical for customer rollouts with project governance

## Overview

Dieses Release behebt eine Regression im Zusammenspiel aus lokaler Admin-Anmeldung, Projektverwaltung und Produktions-Schreibschutz.

## Fixes

- Die Web-UI erzeugt wieder automatisch einen lokalen Bootstrap-Admin, wenn noch keine `artifacts/admin-users.json` vorhanden ist.
- Der Bootstrap-Admin hat wieder Zugriff auf Migration, Projekte und Deployment.
- Die Default-Salesforce-Instanz wird nicht mehr implizit als Produktion behandelt.
- Scheduler und Projektkonfigurationen koennen in Default-/Test-Installationen wieder gespeichert werden, ohne vom Produktionsschutz blockiert zu werden.

## Validation

- `npm run build`
- Smoke-Test: fehlende Admin-User-Datei erzeugt lokalen Admin mit Login-Schutz.
- Smoke-Test: Schreibprüfung fuer `default` blockiert nicht mehr als Produktion.

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.54"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
