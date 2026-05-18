# Release Notes 0.2.56

**Release Date**: 18. Mai 2026
**Release Type**: UI / Operations Hotfix
**Priority**: Recommended for customer production installations

## Overview

Dieses Release trennt die zwei Einsatzvarianten der Web-UI klarer:

- Dienstleister-Rollout mit Test- und Produktionsinstanz.
- Kundeninstallation mit nur einer Produktionsinstanz fuer Monitoring und operative Pflege.

## Fixes

- Reine Produktionsinstallationen werden automatisch als Kunden-/Monitoring-Variante erkannt.
- In dieser Variante duerfen Scheduler und Connectoren trotz aktivem Produktionsschutz bearbeitet werden.
- Setup-, Deployment- und Salesforce-Metadatenaktionen bleiben weiterhin durch den Produktionsschutz blockiert.
- Dienstleister-Projekte mit Test- und Produktionsinstanz behalten den bisherigen Produktionsschutz.
- Die Kontextauswahl faellt bei Projekten mit nur einer Instanz automatisch auf diese Instanz zurueck.

## Validation

- `npm run build`
- Smoke-Test: Single-Production-Projekt erlaubt `POST /api/schedules`.
- Smoke-Test: Test/Production-Projekt blockiert `POST /api/schedules` auf Produktion.
- Smoke-Test: Single-Production-Projekt blockiert weiterhin `POST /api/setup/import`.

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.56"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
