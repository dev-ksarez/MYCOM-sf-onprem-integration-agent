# Dashboard, Scheduler Steuerung und Benachrichtigungen modularisieren

- Spec-ID: 2026-05-05-dashboard-scheduler-steuerung-und-benachrichtigungen-modularisieren
- Status: ready
- Owner: 
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Die aktuelle Web-Oberflaeche buendelt Dashboard, Scheduler-Verwaltung, Migrationen und Admin-API in einer grossen Datei [src/server/app.ts](src/server/app.ts#L1). Gleichzeitig zeigt die vorhandene Dashboard-Logik unterschiedliche fachliche Modelle: Die Fehlerkurve arbeitet bereits connector-basiert, waehrend die Datensatzkurve in [src/server/app.ts](src/server/app.ts#L4783) run-basiert aus den letzten Laeufen gebildet wird. In [src/server/admin-data-service.ts](src/server/admin-data-service.ts#L1639) existiert bereits eine connector-zentrierte Log-Aggregation, aber noch kein passender Pfad fuer schedulerbezogene Drilldowns oder proaktive Benachrichtigungen per Salesforce-Task.

## Problem

Das Dashboard ist fuer den Betrieb nicht interaktiv genug: Zeitbereich-Buttons verhalten sich nicht verlaesslich, die Datensatzgrafik bildet nicht denselben connectorbezogenen Blick wie die Fehlerkurve ab, und ein Klick auf einen Datenpunkt fuehrt nicht direkt zum verursachenden Scheduler. Updates koennen nicht aus dem Dashboard angestossen oder sichtbar signalisiert werden. Scheduler lassen sich weder in der grafischen Uebersicht noch in der Tabellenansicht direkt aktivieren oder deaktivieren. Zusaetzlich ist die bestehende Struktur durch die starke Konzentration von UI, API und Fachlogik in wenigen Dateien wartungsarm, redundant und schwer dokumentierbar. Fuer Connector-Fehler fehlt ausserdem eine standardisierte Benachrichtigung, die in Salesforce fuer einen definierten Benutzer einen gut lesbaren Task mit Fehlerklasse, Connector und Scheduler erzeugt.

## Zielbild

- Das Dashboard verwendet fuer Fehler- und Datensatzansichten ein konsistentes connectorbezogenes Analysemodell.
- Zeitbereich-Buttons aktualisieren alle betroffenen Dashboard-Kennzahlen und Diagramme reproduzierbar.
- Ein Klick auf Linienpunkte oeffnet den zugehoerigen Scheduler-Kontext oder eine gefilterte Scheduler-Liste.
- Das Dashboard kann verfuegbare Updates anzeigen und einen Update-Lauf sichtbar anstossen oder bestaetigen.
- Scheduler koennen in grafischer Ansicht und in der Tabellenansicht direkt aktiviert oder deaktiviert werden.
- Fehler in Connectoren koennen je Fehlerklasse automatisch einen Salesforce-Task fuer einen konfigurierten Benutzer erzeugen.
- Dashboard- und Migrationslogik werden in kleinere, klar dokumentierte Module aufgeteilt.

## Nicht-Ziele

- Kein kompletter Ersatz der aktuellen Web UI durch ein neues Frontend-Framework.
- Keine fachliche Neudefinition des Scheduler-Modells oder der Run-Ausfuehrung.
- Kein generischer Notification-Hub fuer beliebige Zielsysteme ausser Salesforce-Task-Erzeugung.
- Kein einmaliger Big-Bang-Refactor des gesamten Repositories ohne inkrementelle Lieferpakete.
- Keine verbindliche Aenderung aller bestehenden Deploy- oder Windows-Installationsprozesse in derselben Umsetzungsstufe.

## Akzeptanzkriterien

- [ ] Die Zeitbereich-Buttons im Dashboard aktualisieren reproduzierbar die KPI- und Chart-Daten fuer den gewaehlten Zeitraum.
- [ ] Die Datensatzgrafik verwendet connectorbezogene Datensaetze statt ausschliesslich runbasierter Reihen oder weist die Abweichung explizit nicht mehr auf.
- [ ] Ein Klick auf einen Linienpunkt in der Datensatz- oder Fehlergrafik zeigt den oder die Scheduler, die den Wert verursacht haben.
- [ ] Das Dashboard zeigt verfuegbare Updates mit klarer Rueckmeldung an und kann einen Update-Lauf starten oder mindestens kontrolliert anstossen.
- [ ] In grafischer Scheduler-Uebersicht und in der Scheduler-Tabelle kann der Aktiv-Status direkt geaendert werden, ohne den kompletten Scheduler-Dialog zu oeffnen.
- [ ] Fuer definierte Connector-Fehlerklassen wird in Salesforce automatisch ein Task fuer einen konfigurierten Benutzer erzeugt, der Fehlerklasse, Connector, Scheduler, Zeitbezug und eine gut lesbare Handlungsbeschreibung enthaelt.
- [ ] Die Refaktorierung trennt mindestens Dashboard- und Migrationslogik in eigene serverseitige oder UI-nahe Module und reduziert die Verantwortungskonzentration in [src/server/app.ts](src/server/app.ts#L1).
- [ ] Dokumentation und Entwicklerleitfaden beschreiben die neuen Module, Benachrichtigungsregeln und Bedienpfade.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- [src/server/app.ts](src/server/app.ts#L1) fuer aktuelle UI-, Chart- und API-Kopplung
- [src/server/admin-data-service.ts](src/server/admin-data-service.ts#L1639) fuer Aggregationen, Scheduler- und Dashboard-Daten
- [src/agent/agent-runner.ts](src/agent/agent-runner.ts#L143) fuer Fehlerklassifikation und Anschlussstelle an Benachrichtigungen
- [src/clients/salesforce/salesforce-client.ts](src/clients/salesforce/salesforce-client.ts) fuer Task-Erzeugung und Salesforce-Zugriffe
- [salesforce](salesforce) fuer moegliche Konfigurations- oder Metadatenfolgen der Benachrichtigung
- [README.md](README.md) und relevante Betriebsdoku fuer Bedien- und Architekturhinweise

Technische Leitplanken:

- Dashboard-Datenmodelle fuer Fehler, Datensaetze und Scheduler-Drilldown sollen explizit getrennt, aber kompatibel beschrieben werden.
- Scheduler-Aktivierung braucht einen schmalen, idempotenten API-Pfad statt vollstaendigem Schedule-Update ueber das gesamte Formular.
- Fehlerbenachrichtigungen muessen nach Fehlerklasse konfigurierbar sein und Deduplikation oder Spam-Schutz beruecksichtigen.
- Modularisierung erfolgt inkrementell: zuerst klare Schnittstellen fuer Dashboard und Migrationen, dann Verschiebung von Code aus Monolith-Dateien.
- Bestehende Run-, Log- und Update-Mechaniken bleiben rueckwaertskompatibel; neue UI-Funktionen duplizieren keine bestehende Fachlogik.

## Aufgaben

- [ ] Dashboard-Zeitbereichslogik und Chart-Datenquellen fachlich und technisch auseinanderziehen.
- [ ] Connectorbasierte Datensatzaggregation inklusive Scheduler-Drilldown spezifizieren.
- [ ] Update-Anzeige und Update-Aktion im Dashboard als separaten Flow beschreiben.
- [ ] Direktes Aktivieren und Deaktivieren fuer Scheduler in Chart- und Tabellenansicht ueber einen schmalen API-Pfad entwerfen.
- [ ] Salesforce-Task-Benachrichtigungen fuer definierte Fehlerklassen, Empfaenger und optische Aufbereitung spezifizieren.
- [ ] Modulgrenzen fuer Dashboard und Migrationsbereich festlegen und schrittweise Refaktorierungssequenz planen.
- [ ] Architektur-, Betriebs- und Entwicklerdoku fuer die neuen Module und Bedienpfade fortschreiben.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Manuelle Checks in Web UI oder Agent: Zeitbereich wechseln, Diagrammpunkt klicken, Scheduler direkt aktivieren/deaktivieren, Update-Hinweis und Update-Aktion pruefen
- Integrationscheck: definierte Connector-Fehlerklasse provozieren und pruefen, dass genau ein lesbarer Salesforce-Task fuer den konfigurierten Benutzer entsteht
- Architekturcheck: neue Module fuer Dashboard und Migrationen sind getrennt referenzierbar und reduzieren direkte Verantwortung in [src/server/app.ts](src/server/app.ts#L1)
- Betriebsrelevante Beobachtung nach Deploy: Support oder Betrieb kann Fehler, Scheduler-Status und Updatebedarf ohne Logsuche im Dashboard erkennen

## Status

- Status: ready
- Letzte Entscheidung: Die Anforderungen werden als eigene Produkt-Spec gefuehrt, weil sie Dashboard, Scheduler-Steuerung, Benachrichtigung und Architektur gleichzeitig betreffen und nicht in die OAuth-Spec gehoeren.
- Naechster Schritt: Umsetzung in einzelne Lieferpakete schneiden und mit dem ersten Task fuer Dashboard-Zeitbereich und connectorbasierte Datensatzgrafik starten.