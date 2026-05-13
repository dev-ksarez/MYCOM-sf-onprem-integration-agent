# Spec-Driven Development im Projekt

Dieses Verzeichnis ist die Quelle fuer fachliche und technische Entscheidungen, bevor Code geaendert wird.

## Ziel

Jede nicht-triviale Aenderung startet mit einer Spec, die Problem, Zielbild, Akzeptanzkriterien, Umsetzungsskizze und Verifikation festhaelt.

## Standardablauf

1. Neue Spec anlegen: `npm run spec:new -- "Kurzer Titel"`
2. Spec in `docs/specs/<datum>-<slug>/spec.md` ausfuellen
3. Offene Fragen mit Fachbereich oder Betrieb klaeren
4. Umsetzung erst starten, wenn Akzeptanzkriterien und Verifikation konkret sind
5. Vor oder waehrend der Umsetzung `npm run spec:validate` ausfuehren
6. Aufgaben in der Spec abhaken und Status auf `done` setzen

## Wann eine Spec Pflicht ist

- Neue Quell- oder Zieladapter
- Aenderungen an Scheduler-Logik oder Run-Verhalten
- UI-Flows mit betrieblicher Relevanz
- Salesforce-Metadaten, Deploy- oder Migrationsaenderungen
- Alles, was neue Konfiguration, neue Risiken oder Betriebsfolgen einfuehrt

## Abgrenzung zu Kunden-Spezifikationen

- `docs/specs/` ist ausschliesslich fuer zentrale Projekt- und Feature-Specs.
- Kundenindividuelle Spezifikationen liegen unter `artifacts/customer-installations/<kunde>/`.
- Kunden-Specs werden nicht automatisch als Produkt-Backlog oder globaler Projektscope interpretiert.

## Wann eine kurze Aenderungsnotiz reicht

- Kleine refactorings ohne Verhaltensaenderung
- Isolierte Bugfixes mit klarer Ursache und klarer Verifikation
- Reine Text- oder Style-Korrekturen ohne Betriebsfolgen

## Definition of Ready

- Problem und Zielbild sind fuer Dritte verstaendlich
- Akzeptanzkriterien sind beobachtbar und testbar
- Nicht-Ziele begrenzen den Scope
- Verifikation benennt mindestens einen konkreten Check

## Definition of Done

- Code, Konfiguration und Doku entsprechen der Spec
- Offene Aufgaben sind markiert oder abgeschlossen
- Verifikation wurde ausgefuehrt und in der Spec aktualisiert
- Status steht auf `done` oder enthaelt einen begruendeten Restpunkt