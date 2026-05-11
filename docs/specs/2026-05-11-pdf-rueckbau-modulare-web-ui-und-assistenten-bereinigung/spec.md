# PDF-Rueckbau, modulare Web UI und Assistenten-Bereinigung

- Spec-ID: 2026-05-11-pdf-rueckbau-modulare-web-ui-und-assistenten-bereinigung
- Status: done
- Owner: Codex
- Reviewers:
- Verknuepfte Tickets:

## Kontext

Das PDF-Modul war als experimenteller Generator im Repository vorhanden, ist aber nicht Teil des aktuellen produktiven Betriebs. Parallel war `src/server/app.ts` durch Serverlogik, HTML-Rendering, Inline-JavaScript und UI-Zustand zu breit geworden. Scheduler- und Connector-Assistenten zeigten ausserdem Felder, die fuer bestimmte Typen nicht sinnvoll sind, etwa Secret-Key-Eingaben bei Datei-Connectoren oder Mapping-/Objektfelder bei Datei-Zielen.

## Problem

Die bisherige Struktur erschwerte Wartung, Erweiterung und spaetere Modul-Andockpunkte fuer Migration, Admin, Reporting oder ein moegliches neues PDF-Modul. Nutzer wurden in den Assistenten durch typfremde Felder verunsichert, und die Serverdatei enthielt zu viele Verantwortlichkeiten.

## Zielbild

- PDF-Generator-Code, PDF-Specs und PDF-API-Endpunkte sind aus dem aktuellen Stand entfernt.
- Web-UI-Assets, Audit-Historie, Modulnavigation und HTML-Dokumentrahmen liegen in eigenen Servermodulen.
- Login- und OAuth-Callback-JavaScript sowie Login-CSS werden als statische Assets ausgeliefert.
- Scheduler- und Connector-Assistenten blenden typfremde Felder aus.
- Datei-Ziele verwenden serverseitig konsistente Defaults fuer System, Objekt, Operation und Mapping.
- Wiederverwendbare Moduldefinitionen bilden die Grundlage fuer kuenftige Admin-, Migration-, Reporting- oder PDF-Module.

## Nicht-Ziele

- Kein neues PDF-Modul wird implementiert.
- Kein Wechsel auf ein externes Frontend-Framework.
- Keine Aenderung am Salesforce-Datenmodell.
- Keine vollstaendige API-Router-Neustrukturierung in diesem Schritt.

## Akzeptanzkriterien

- [x] PDF-bezogene Quellcodes, Specs und API-Registrierungen sind entfernt.
- [x] `app.ts` delegiert zentrale UI- und Asset-Aufgaben an kleine Module.
- [x] Es gibt keine neuen eingebetteten CSS- oder JavaScript-Bloecke fuer Login, Callback oder Admin-UI-Asset.
- [x] Connector-Assistent zeigt Secret-Key-Felder nur fuer SQL-Connectoren.
- [x] Scheduler-Assistent behandelt `FILE_CSV`, `FILE_EXCEL` und `FILE_JSON` als Datei-Ziele ohne Objekt-/Operations-/Mapping-Pflicht.
- [x] Build, JavaScript-Syntaxcheck, Spec-Validierung und Whitespace-Pruefung laufen erfolgreich.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- `src/server/app.ts`
- `src/server/admin-ui-script.ts`
- `src/server/admin-data-service.ts`
- `src/server/app-modules.ts`
- `src/server/asset-server.ts`
- `src/server/ui-template.ts`
- `src/server/audit-history-service.ts`
- `src/public/`
- `src/css/`
- `src/types/file-schedule-type.ts`
- `src/pdf-generator/`
- `docs/`

Technische Leitplanken:

- Dateitypen werden ueber `src/types/file-schedule-type.ts` zentral normalisiert.
- UI-Module werden ueber `AppModuleDefinition` registriert und koennen spaeter erweitert werden.
- Statische Assets erhalten eine zentrale Versionskennung ueber den Asset-Server.
- Datei-Ziele speichern keine fachlich falschen Salesforce-/MSSQL-Pflichtwerte und schreiben kein unnoetiges Mapping.

## Aufgaben

- [x] PDF-Modul und zugehoerige Specs entfernen.
- [x] Web-UI-Assets und HTML-Dokumentrahmen extrahieren.
- [x] Audit-Historie aus `app.ts` auslagern.
- [x] Modulare App-Registrierung fuer Navigation und Admin-API einfuehren.
- [x] Scheduler-/Connector-Assistenten typabhaengig bereinigen.
- [x] Doku und Release Notes aktualisieren.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Spec-Validierung: `npm run spec:validate`
- Admin-UI-Syntax: `node --check` gegen das aus `renderAdminUiScript()` generierte Script
- Statische JS-Syntax: `node --check src/public/login.js && node --check src/public/migration-oauth-callback.js`
- Whitespace-/Diff-Pruefung: `git diff --check`

## Status

- Status: done
- Letzte Entscheidung: PDF wird in diesem Release entfernt; spaetere PDF-Funktion wird als neues Modul an die gemeinsame Codebasis angedockt.
- Naechster Schritt: Release `0.2.23` bauen, committen und pushen.
