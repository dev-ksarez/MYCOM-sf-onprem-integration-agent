# Release Notes 0.2.48

**Release Date**: 13. Mai 2026  
**Release Type**: Hotfix / Patch  
**Priority**: CRITICAL for Annaburger production

## Overview

Bereinigter Hotfix auf Basis von 0.2.45 ohne Projekt-/Produktionsschutz-Feature. Dieses Release enthält ausschließlich die notwendigen Korrekturen für MSSQL-basierte Scheduler-Runs.

## Issues Fixed

### Critical Bug: MSSQL source schedules required target parameters too early

**Symptom**:
- Klick auf `RUN` führte bei betroffenen MSSQL-Schedules zu keinem sichtbaren Lauf
- In bestimmten Fällen wurden keine Run-/Log-Einträge angelegt, weil die Ausführung schon bei der Connector-Initialisierung abbrach

**Root Cause**:
- `MssqlConnector` verlangte `table` und `upsertKey` bereits im Konstruktor
- Für source-only `MSSQL_SQL` Schedules werden diese Werte beim Lesen nicht benötigt
- Dadurch konnten reine MSSQL-Quell-Schedules vor der eigentlichen Ausführung scheitern

**Fix**:
- `MssqlConnector` validiert `table` und `upsertKey` jetzt nur noch in den Upsert-/Target-Pfaden
- Source-only MSSQL-Schedules können ohne diese Zielparameter initialisiert und ausgeführt werden

### UX Improvement: RUN errors are visible in Scheduler UI

**Fix**:
- Die Scheduler-UI zeigt Fehler beim manuellen `RUN` jetzt explizit an
- Statt stiller Fehlschläge erscheint eine klare Fehlermeldung im UI

## Important Scope Note

Dieses Release enthält **nicht** das unfertige Projekt-/Produktionsschutz-Feature. Es wurde bewusst sauber von `v0.2.45` neu geschnitten.

## Technical Details

### File Changes

- `src/connectors/mssql/mssql-connector.ts`
- `src/server/admin-ui-script.ts`
- `src/server/scheduler-ui-module.ts`
- `package.json`

## Testing & Validation

- Build passes: `npm run build`
- Specs validate: `npm run spec:validate`

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.48"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
