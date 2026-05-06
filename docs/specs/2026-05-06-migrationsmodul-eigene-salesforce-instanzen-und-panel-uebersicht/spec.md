# Migrationsmodul: Eigene Salesforce-Instanzen und Panel-Uebersicht

- Spec-ID: 2026-05-06-migrationsmodul-eigene-salesforce-instanzen-und-panel-uebersicht
- Status: ready
- Owner: 
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Das Web UI verwaltet Salesforce-Instanzen heute bereits zentral ueber [src/server/admin-data-service.ts](src/server/admin-data-service.ts#L210) und stellt Connectoren im Panel-Stil im Tab [src/server/app.ts](src/server/app.ts#L1196) dar. Das Migrationsmodul selbst zeigt dagegen aktuell vor allem eine Listen- und Wizard-Sicht aus [src/server/migration-ui-module.ts](src/server/migration-ui-module.ts#L1).

Fachlich reicht die aktuelle Default- oder globale Instanzauswahl nicht aus, wenn Migrationen parallel gegen unterschiedliche Salesforce-Organisationen geplant, analysiert und ausgefuehrt werden sollen. Zusaetzlich soll die Salesforce-Anmeldung fuer Migrationen nicht mehr ueber Connected-App-Credentials im UI oder SOAP-Login mit Benutzername und Passwort erfolgen, sondern ueber die jeweilige Salesforce-Login-Seite der Migration mit anschliessender Freigabe per Allow.

## Problem

Das Migrationsmodul kann aktuell nicht sauber zwischen mehreren eigenstaendigen Salesforce-Zielinstanzen unterscheiden. Dadurch entstehen vier konkrete Probleme:

- Migrationen haengen implizit an der aktuell aktiven oder global konfigurierten Salesforce-Instanz statt ihre eigene Zielverbindung mitzutragen.
- Betreiber sehen im Migrationsbereich nicht auf einen Blick, welche Org verbunden ist, ob sie Sandbox oder Produktion ist, wann die letzte Migration lief und ob Fehler offen sind.
- Ein erneuter Login oder Token-Aufbau ist nicht als persistierter Verbindungszustand im Migrationskontext sichtbar und fuehrt zu unnötiger Unsicherheit bei laengeren Betriebsphasen.
- Die aktuelle technische Anbindung ueber Client-ID und Client-Secret bildet den gewuenschten Login ueber die Salesforce-Login-Seite mit anschliessender Benutzerfreigabe per Allow nicht ab.

## Zielbild

- Jede Migration traegt ihre eigene Salesforce-Zielverbindung als Teil der Migrationskonfiguration.
- Beim Anlegen oder Bearbeiten einer Migration wird explizit zwischen Sandbox und Produktion unterschieden; daraus ergibt sich eine passende Login-URL oder ein klarer Hinweis auf die verwendete Login-Domain.
- Die Verbindung zur Zielorganisation erfolgt ueber die Salesforce-Login-Seite der jeweiligen Umgebung; der Benutzer meldet sich dort an und bestaetigt den Zugriff per Allow.
- Der Migrationstab zeigt Migrationen mit eigenem Login als Panels aehnlich zur Connector-Darstellung.
- Jedes Panel zeigt mindestens Migrationsname, Org-Typ, Login-Status, letzte erfolgreiche oder fehlerhafte Migration, betroffene Objekte, Datensatzmengen und letzte Fehlerzusammenfassung.
- Das detaillierte Laufprotokoll verbleibt im Agenten, aber das Panel zeigt einen kompakten Auszug mit Verweis auf das im Agenten gespeicherte Log oder den letzten Laufbericht.
- Die Login- und Statusdaten bleiben zusammen mit der Migration ueber Neustarts erhalten.

## Nicht-Ziele

- Kein Umbau der bestehenden Connector-Verwaltung ausser der visuellen Panel-Analogie.
- Kein Ersatz des vorhandenen globalen Salesforce-Instanzkonzepts fuer Scheduler, Dashboard oder allgemeine Admin-Funktionen.
- Kein vollstaendiges Log-Frontend fuer alle Agenten-Logs; im Scope ist nur eine verdichtete Anzeige pro Migrationsinstanz.
- Keine automatische Datenreplikation zwischen zwei Salesforce-Instanzen ausserhalb explizit angelegter Migrationen.
- Keine Aenderung an fachlichen Mapping-, Preflight- oder Feldanlage-Regeln, sofern sie nicht fuer die Instanzzuordnung erweitert werden muessen.

## Akzeptanzkriterien

- [ ] Im Migrationstab gibt es einen sichtbaren Bereich fuer Migrationen mit eigenem Login in Panel-Darstellung analog zum Connector-Tab in [src/server/app.ts](src/server/app.ts#L1210).
- [ ] Ein Benutzer kann in einer Migration die Umgebung waehlen und den Salesforce-Login ueber die passende Login-Seite starten.
- [ ] Der gewaehlte Org-Typ wirkt sich sichtbar auf Login-Ziel oder Kennzeichnung aus, damit Fehlkonfigurationen zwischen test.salesforce.com und login.salesforce.com vermieden werden.
- [ ] Nach erfolgreichem Salesforce-Login wird der Verbindungszustand in der Migration persistiert, sodass die UI nach Neustart erkennen kann, dass fuer diese Migration bereits eine gueltige oder zuletzt erfolgreiche Verbindung existierte.
- [ ] Jedes Panel zeigt die wichtigsten Metadaten: Org-Name oder Alias, Org-Typ, Login-Status, Zeitpunkt der letzten Synchronisation oder Migration und Anzahl zuletzt migrierter Datensaetze.
- [ ] Jedes Panel zeigt die zuletzt betroffenen Objekte einer Migration in kompakter Form, z. B. als Liste oder Badge-Gruppe.
- [ ] Wenn fuer die Migration beim letzten Login oder Lauf Fehler auftraten, zeigt das Panel eine kurze Fehlerzusammenfassung und einen Einstieg zum detaillierten Agenten-Log oder Migrationsbericht.
- [ ] Bestehende Migrationen ohne eingebetteten Login bleiben rueckwaertskompatibel und koennen vorerst der bisherigen globalen Instanz folgen.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- [src/server/admin-data-service.ts](src/server/admin-data-service.ts#L210) fuer Persistenz der eingebetteten Migrations-Logins, Login-Metadaten und Aggregation der Panel-Daten
- [src/server/app.ts](src/server/app.ts#L1260) fuer Panel-Bereich im Migrationstab, Start des OAuth-Flows im Wizard und den Callback fuer die Salesforce-Freigabe
- [src/server/migration-ui-module.ts](src/server/migration-ui-module.ts#L1) fuer Client-seitiges Rendern der Panel-Zusammenfassungen und Connect-Aktionen
- [src/clients/salesforce/salesforce-client.ts](src/clients/salesforce/salesforce-client.ts#L432) fuer OAuth-Refresh-Token-Login im Migrationspfad neben bestehender client_credentials-Unterstuetzung
- [artifacts/migrations.json](artifacts/migrations.json) fuer persistente Login- und Statusdaten je Migration

Technische Leitplanken:

- Die Datenstruktur fuer Login und Laufstatus wird direkt in der Migration gespeichert, damit Migrationskonfiguration und Zielverbindung eine fachliche Einheit bilden.
- Fuer Rueckwaertskompatibilitaet sollte eine bestehende globale Salesforce-Konfiguration weiterhin als Fallback funktionieren, solange eine Migration noch keiner eigenen Migrationsinstanz zugeordnet ist.
- Persistiert werden Verbindungsmetadaten der Migration sowie der zuletzt erfolgreiche oder fehlerhafte Connect-Zustand. Fuer die Wiederverwendung des Benutzer-Logins wird ein Refresh-Token in der Migration gespeichert; kurzlebige Access-Tokens werden weiterhin nur laufzeitnah verwendet und nicht in UI-Antworten ausgegeben.
- Die Panel-Daten sollten aus bestehenden Lauf- und Migrationsinformationen abgeleitet werden, statt parallele Schattenstatistiken ohne Quelle einzufuehren.
- Fehlerdetails bleiben im Agenten-Log oder Laufbericht; die Panel-Sicht zeigt nur verdichtete, UI-taugliche Informationen.
- Die UI soll fuer Operatoren zwischen nicht verbunden, verbunden, Token abgelaufen und letzter Login fehlgeschlagen unterscheiden.

## Aufgaben

- [ ] Datenmodell fuer eingebetteten Salesforce-OAuth-Login, Org-Typ, Login-Metadaten und letzte Migrationskennzahlen festlegen.
- [ ] Rueckwaertskompatiblen Fallback zur globalen Instanz fuer alte Migrationen sauber beibehalten.
- [ ] API-Endpunkte fuer Listen-, Detail- und Connect-Aktionen auf Basis der gespeicherten Migration definieren.
- [ ] Migrationstab um Panel-Liste und Detail-/Bearbeitungsfluss erweitern.
- [ ] Migrations-Wizard um Salesforce-Login ueber Login-Seite, Allow-Freigabe und Speicherung direkt an der Migration erweitern.
- [ ] Lauf- und Fehleraggregation fuer Panel-Zusammenfassungen aus vorhandenen Migrationsdaten ableiten.
- [ ] Persistierten Login-Zustand, Reconnect-Strategie und Sicherheitsgrenzen dokumentieren.

## Verifikation

- Build oder schmaler Smoke-Test: npm run build
- Manuelle Checks in Web UI oder Agent: neue Migration fuer Sandbox und Produktion anlegen, Salesforce-Login ueber Login-Seite mit Allow ausloesen, Neustart simulieren, Status im Panel pruefen
- Regression: bestehende Migration ohne eingebetteten Login laesst sich weiterhin laden und bearbeiten
- Betriebsrelevante Beobachtung nach Deploy: Operator erkennt pro Migration mit eigenem Login innerhalb weniger Sekunden Login-Status, letzte Aktivitaet und offene Fehler ohne separaten Log-Zugriff

## Status

- Status: ready
- Letzte Entscheidung: Der Salesforce-Login fuer neue Migrationen erfolgt ueber die Login-Seite der jeweiligen Umgebung mit anschliessender Allow-Freigabe; die daraus nutzbare Verbindung bleibt direkt an der Migration gespeichert.
- Naechster Schritt: UI-Smoke-Test fuer den Salesforce-Allow-Flow im Wizard und den Panel-Status pro Migration durchfuehren.