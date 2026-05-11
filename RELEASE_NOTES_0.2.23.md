# Release 0.2.23

## Schwerpunkt

Dieses Release entfernt das experimentelle PDF-Modul aus dem aktuellen Produktstand und bereitet die Web-UI-Struktur fuer kuenftige andockbare Module vor. Zusaetzlich wurden Scheduler- und Connector-Assistenten typabhaengig bereinigt.

## Aenderungen

### PDF-Modul entfernt

- PDF-Generator-Quellcode, Templates, Connector-Schema und Beispieljob wurden entfernt.
- PDF-Specs und PR-Dokumente wurden aus dem aktuellen Dokumentationsstand geloescht.
- Der serverseitige PDF-API-Endpunkt und zugehoerige UI-Reste wurden entfernt.

### Web UI modularisiert

- `src/server/app.ts` delegiert statische Assets, Audit-Historie, Modulnavigation, Admin-UI-Script und HTML-Dokumentrahmen an dedizierte Module.
- Login- und Migration-OAuth-Callback-JavaScript liegen in `src/public/`.
- Login-CSS liegt in `src/css/login.css`.
- Neue Moduldefinitionen in `src/server/app-modules.ts` schaffen eine gemeinsame Grundlage fuer Admin-, Migration-, Reporting- oder spaetere PDF-Module.

### Assistenten bereinigt

- Connector-Assistent zeigt Secret-Key-Felder nur noch fuer SQL-Connectoren.
- Datei-Connectoren senden keine MSSQL-Secret-Werte mehr.
- Scheduler-Assistent blendet bei Datei-Zielen Objekt, Operation, Zielsystem-Pflichtfeld und Mapping-Auswahl aus.
- Datei-Ziele verwenden serverseitig konsistente Defaults: `File`, `FileExport`, `Write` und kein Mapping.

### Zentrale Typ- und Audit-Helfer

- Datei-Schedule-Typen werden ueber `src/types/file-schedule-type.ts` zentral normalisiert.
- Audit-Historie wurde in `src/server/audit-history-service.ts` gebuendelt.

## Migration Path

1. Neue Version deployen.
2. Web-Server neu starten, damit die ausgelagerten Assets und Admin-UI-Scripte geladen werden.
3. Bei bestehenden Datei-Ziel-Schedulern pruefen, ob Target Definition und Exportpfad gesetzt sind.
4. Falls ein lokaler PDF-Generator aus dem experimentellen Stand genutzt wurde, diesen vor dem Update separat sichern. Er ist nicht mehr Bestandteil des Releases.

## Verifikation

- `npm run build`
- `npm run spec:validate`
- Admin-UI-Syntaxcheck ueber `renderAdminUiScript()`
- `node --check src/public/login.js`
- `node --check src/public/migration-oauth-callback.js`
- `git diff --check`
