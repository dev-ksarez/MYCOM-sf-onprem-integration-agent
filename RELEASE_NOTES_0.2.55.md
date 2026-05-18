# Release Notes 0.2.55

**Release Date**: 18. Mai 2026
**Release Type**: Performance / Stability Hotfix
**Priority**: Critical for productive Salesforce upsert schedulers

## Overview

Dieses Release verbessert die Laufzeit und Stabilitaet grosser Salesforce-Upsert-Scheduler, insbesondere fuer produktive Account-Importe wie `SCH-0019`.

## Fixes

- Generische Salesforce-Zielschreibvorgaenge nutzen jetzt Bulk-DML mit bis zu 200 Datensaetzen pro Salesforce-Call.
- `insert`, `update` und `upsert` liefern weiterhin pro Datensatz Erfolg oder Fehler zurueck.
- Duplicate-Rule-Fallback mit `allowSave=true` bleibt fuer Bulk-Upserts erhalten, wenn Salesforce den Datensatz mit speicherbarer Duplicate-Warnung ablehnt.
- Lange Transferlaeufe schreiben Fortschrittslogs waehrend der Verarbeitung.
- Die Inaktivitaetspruefung schliesst lange, aber aktive Scheduler-Laeufe dadurch nicht mehr faelschlich nach 10 Minuten.

## Impact

- Scheduler mit `BatchSize__c = 200` verwenden die Batch Size jetzt auch fuer Salesforce-Schreibvorgaenge.
- Bestehende Scheduler-Konfigurationen muessen nicht angepasst werden.
- Fehlerhafte Datensaetze blockieren nicht den gesamten Batch, weil Bulk-DML mit `allOrNone=false` ausgefuehrt wird.

## Validation

- `npm run build`

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.55"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
