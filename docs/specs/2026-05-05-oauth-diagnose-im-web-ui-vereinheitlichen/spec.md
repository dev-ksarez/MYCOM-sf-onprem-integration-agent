# OAuth Diagnose im Web UI vereinheitlichen

- Spec-ID: 2026-05-05-oauth-diagnose-im-web-ui-vereinheitlichen
- Status: ready
- Owner: 
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Der Salesforce-Login laeuft im Client-Credentials-Flow direkt ueber den Token-Endpunkt in [src/clients/salesforce/salesforce-client.ts](src/clients/salesforce/salesforce-client.ts#L376). Bei Fehlern wird aktuell im Kern nur der rohe Fehlertext `Salesforce token request failed: <status> <payload>` weitergegeben. Parallel existieren mit [OAUTH_ERROR_QUICK_FIX.md](OAUTH_ERROR_QUICK_FIX.md) und [SALESFORCE_OAUTH_TROUBLESHOOTING.md](SALESFORCE_OAUTH_TROUBLESHOOTING.md) bereits betriebliche Diagnosehinweise, die in der Web UI bisher nicht systematisch ankommen.

## Problem

Operatoren sehen in der Web UI oder im Setup-Ablauf nicht konsistent, ob ein OAuth-Fehler aus einer falsch konfigurierten Connected App, falschen Credentials, einer falschen Login-URL oder einem Transportproblem stammt. Dadurch weichen sie fuer die Erstdiagnose auf Logdateien oder externe Runbooks aus, obwohl die benoetigte Information bereits in der Token-Response und in bestehender Doku vorhanden ist.

## Zielbild

- Die Web UI klassifiziert haeufige Salesforce-OAuth-Fehler in wenige stabile Diagnoseklassen.
- Pro Diagnoseklasse zeigt die UI einen klaren naechsten Schritt fuer Betrieb oder Fachbereich an.
- Die betroffene Salesforce-Instanz oder Login-URL ist im Fehlerkontext sichtbar.
- Der rohe Response-Text bleibt fuer technische Analyse einsehbar.
- Das bestehende Verhalten des Agenten und des Debug-Skripts bleibt funktional unveraendert.

## Nicht-Ziele

- Kein kompletter Umbau des OAuth-Flows.
- Keine Aenderung an Salesforce-Credentials oder Secret-Ablage.
- Keine neue Retry-Logik auf Transportebene.
- Kein Ersatz des CLI-Diagnose-Skripts `npm run sf:debug-oauth`.
- Keine Ausweitung auf REST-Connector-OAuth2 ausserhalb des Salesforce-Clients.

## Akzeptanzkriterien

- [ ] Wenn die Token-Response `unsupported_grant_type` enthaelt, zeigt die UI als wahrscheinliche Ursache eine nicht aktivierte Client-Credentials-Flow-Konfiguration der Connected App und verweist auf die vorhandenen Setup-Schritte.
- [ ] Wenn die Token-Response `invalid_client` oder einen semantisch gleichwertigen Credential-Fehler enthaelt, zeigt die UI einen Hinweis auf Client-ID oder Client-Secret statt nur des Rohfehlers.
- [ ] Wenn Login-URL oder Netzwerkaufruf fehlschlagen, trennt die UI sichtbar zwischen Konfigurationsfehler und Transportfehler.
- [ ] Der Fehlerzustand zeigt die betroffene Instanz oder mindestens die verwendete Login-URL im Kontext an.
- [ ] Ein technischer Detailbereich mit HTTP-Status und Rohpayload kann bei Bedarf geoeffnet werden.
- [ ] Die Diagnosetexte sind mit dem bestehenden Inhalt aus [OAUTH_ERROR_QUICK_FIX.md](OAUTH_ERROR_QUICK_FIX.md) und [SALESFORCE_OAUTH_TROUBLESHOOTING.md](SALESFORCE_OAUTH_TROUBLESHOOTING.md) konsistent.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- [src/clients/salesforce/salesforce-client.ts](src/clients/salesforce/salesforce-client.ts#L376) fuer strukturiertere Fehlerklassifikation am Token-Request-Rand
- [src/server/app.ts](src/server/app.ts) fuer UI-Darstellung, Anzeige von Diagnoseklasse und technischem Detail
- [OAUTH_ERROR_QUICK_FIX.md](OAUTH_ERROR_QUICK_FIX.md) und [SALESFORCE_OAUTH_TROUBLESHOOTING.md](SALESFORCE_OAUTH_TROUBLESHOOTING.md) als fachliche Quelle fuer Diagnosehinweise

Technische Leitplanken:

- Fehlerklassifikation moeglichst nahe an der Salesforce-Client-Antwort halten, damit UI und Logs denselben Ursprung nutzen.
- Der rohe Fehlertext darf fuer Logging und Debugging erhalten bleiben; die UI bekommt zusaetzlich strukturierte Diagnosefelder.
- Mapping von Response-Fehlern auf Diagnoseklassen soll klein bleiben, z. B. `unsupported_grant_type`, `invalid_client`, `transport_error`, `unknown_oauth_error`.
- Die bestehende Rate-Limit-Sonderbehandlung im Client bleibt unveraendert.

## Aufgaben

- [ ] Vorhandene OAuth-Fehler aus Token-Response, Debug-Skript und Hilfsdokumenten in eine kleine Klassifikation ueberfuehren.
- [ ] Im Salesforce-Client strukturierte Fehlerdetails erzeugen, ohne bestehende Logtexte zu verlieren.
- [ ] In der Web UI Diagnoseklasse, naechsten Schritt und technischen Detailbereich darstellen.
- [ ] Texte gegen bestehende Troubleshooting-Dokumente abgleichen.
- [ ] Erfolgs-, Konfigurations- und Transportfehler manuell pruefen.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Manuelle Checks in Web UI oder Agent: absichtlich `invalid_client` und `unsupported_grant_type` in Sandbox provozieren oder vorhandene Diagnose-Responses simulieren
- Regression: `npm run sf:debug-oauth` liefert weiterhin den bestehenden technischen Detailgrad
- Betriebsrelevante Beobachtung nach Deploy: Support kann die Ursache ohne direkten Zugriff auf Serverlogs erstdiagnostizieren

## Status

- Status: ready
- Letzte Entscheidung: Die erste reale Spec fokussiert auf Salesforce-OAuth-Diagnose, weil dafuer bereits Doku, Debug-Skript und ein klarer Fehlerpfad im Client existieren.
- Naechster Schritt: UI-Zielzustand kurz reviewen und dann die Fehlerklassifikation im Client umsetzen.