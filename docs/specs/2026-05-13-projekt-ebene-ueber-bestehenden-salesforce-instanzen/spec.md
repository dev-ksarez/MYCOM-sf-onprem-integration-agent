# Projekt-Ebene ueber bestehenden Salesforce-Instanzen

- Spec-ID: 2026-05-13-projekt-ebene-ueber-bestehenden-salesforce-instanzen
- Status: ready
- Owner: Projektteam SF OnPrem Integration Agent
- Reviewers: Produktverantwortung, Betrieb
- Verknuepfte Tickets: 

## Kontext

Im System koennen bereits mehrere Salesforce-Instanzen verwaltet werden.
Fachlich fehlt jedoch eine uebergeordnete Gliederungsebene, um Instanzen logisch in Projekten zu organisieren.

Dadurch werden Konfiguration, Berechtigungen, Monitoring und Betriebsverantwortung unuebersichtlich, sobald mehrere Kunden, Umgebungen oder Integrationsvorhaben parallel laufen.

## Problem

Ohne Projekt-Ebene sind Instanzen nur als flache Liste abgebildet.
Das erzeugt folgende Probleme:

- Keine klare fachliche Gruppierung (z. B. pro Kunde, Bereich oder Integrationspaket)
- Schwierige Navigation im Web UI bei vielen Instanzen
- Erhoehte Fehlerrisiken bei Zuordnung von Schedules und Migrationen zur falschen Instanz
- Keine saubere Grundlage fuer projektbezogene Auswertungen und Governance

## Zielbild

1. Es gibt eine neue Entitaet `Projekt` als Ebene oberhalb bestehender Salesforce-Instanzen.
2. Jede Salesforce-Instanz ist genau einem Projekt zugeordnet.
3. Das Web UI zeigt Instanzen gruppiert nach Projekt und ermoeglicht projektspezifische Filter.
4. Bei Anlage/Bearbeitung von Instanzen ist die Projektzuordnung verpflichtend (mit Rueckwaertskompatibilitaet fuer Altbestand).
5. Schedules und Migrationen koennen projektbezogen validiert werden, damit Instanzverwechslungen reduziert werden.
6. Betriebs- und Monitoringansichten koennen mindestens nach Projekt aggregiert werden.

## Nicht-Ziele

- Keine Umstellung auf ein komplett neues Mandantenkonzept.
- Keine sofortige Aenderung aller bestehenden API-Vertraege ohne Migrationspfad.
- Keine automatische inhaltliche Neuordnung historischer Runs ausser Zuordnung zum Projektkontext.
- Keine Aenderung an Salesforce-Zielobjekten oder SQL-Logik nur wegen der neuen Ebene.

## Akzeptanzkriterien

- [ ] Verhalten ist fuer den Nutzer oder Operator eindeutig beobachtbar.
- [ ] Erfolgs- und Fehlerfall sind beschrieben.
- [ ] Konfiguration, Migration oder Deployment-Folgen sind dokumentiert.
- [ ] Es gibt persistente Projektobjekte und eine eindeutige Zuordnung Instanz -> Projekt.
- [ ] Im UI sind Projekte sichtbar und Instanzen projektweise gruppiert oder filterbar.
- [ ] Bestehende Instanzen ohne Projektzuordnung erhalten einen definierten Fallback (z. B. `Default-Projekt`) und bleiben funktionsfaehig.
- [ ] Fehlkonfigurationen (ungueltiges Projekt, geloeschte Zuordnung) liefern klare Validierungsfehler.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- `src/server/admin-data-service.ts` (Persistenzmodell fuer Projekte und Zuordnung)
- `src/server/app.ts` (API-Endpunkte, UI-Datenbereitstellung)
- `src/server/*-ui-module.ts` (Darstellung, Filter, Auswahl)
- `artifacts/sf-instances.json` (Datenmigration fuer bestehende Instanzen)
- optional: `artifacts/projects.json` oder eingebettetes Modell in bestehender Datei

Technische Leitplanken:

- Rueckwaertskompatibilitaet fuer bestehende Instanzstruktur ist Pflicht.
- Datenmigration muss idempotent sein (mehrfach ausfuehrbar ohne Seiteneffekte).
- API-Antworten sollen Projektkontext enthalten, ohne alte Consumer sofort zu brechen.
- Logging und Audit sollen Projekt-ID/-Name in relevanten Operationen mitfuehren.

Annahme fuer diese Spec:

- Mit "zusaetzliche Ebene" ist eine organisatorische Projekt-Ebene ueber den bestehenden Salesforce-Instanzen gemeint.

## Aufgaben

- [ ] Datenmodell fuer Projekt und Instanzzuordnung finalisieren.
- [ ] Migrationsstrategie fuer Bestandsinstanzen festlegen (Default-Projekt).
- [ ] API und UI fuer projektbezogene Anzeige/Filter spezifizieren.
- [ ] Validierungs- und Fehlerfaelle definieren.
- [ ] Technische Umsetzung in kleinen Schritten planen.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Manuelle Checks in Web UI oder Agent:
	- Neues Projekt anlegen
	- Instanz Projekt zuordnen
	- Instanzen pro Projekt filtern
	- Schedule-/Migrationszuordnung gegen Projekt pruefen
- Betriebsrelevante Beobachtung nach Deploy:
	- Bestehende Instanzen sind weiterhin nutzbar
	- Monitoring kann projektweise ausgewertet werden

## Status

- Status: ready
- Letzte Entscheidung: Beschlussvorschlag v1 ist bestaetigt.
	- Instanzmodell pro Projekt: Test 1..n, Produktion genau 1
	- Produktionssperre: standardmaessig aktiv
	- Deploy-Freigaberolle: Projekt-Owner und Release-Manager
	- Confluence-Integration: API-Token als MVP, OAuth 2.0 als Ausbau
	- Setup-Versionierung: Metadaten plus Artefakt-Referenz (Deploy-Paket-ID)
- Naechster Schritt: Umsetzungsplanung in konkrete Arbeitspakete (Datenmodell, Migration, API/UI, Validierung, Audit) ueberfuehren.

## Umwandlung in konkrete Spec Anforderung

Die folgende Konkretisierung leitet aus den obigen Stichpunkten umsetzbare Anforderungen ab.

### A. Projektmodell mit Test- und Produktionsinstanz

- Ein Projekt kann mindestens zwei Instanzrollen aufnehmen: `test` und `production`.
- Beide Rollen muessen im Datenmodell eindeutig markiert sein.
- Eine Instanz darf innerhalb eines Projekts nur eine Rolle haben.

Akzeptanzkriterien:

- [ ] Ein Projekt kann mit genau einer Test- und einer Produktionsinstanz angelegt werden.
- [ ] Die Rollen sind in API und UI konsistent sichtbar.

### B. Klare Umgebungskennzeichnung im UI

- In allen projektrelevanten Oberflaechen (Instanzliste, Scheduler, Migration, Deploy) ist klar sichtbar, ob der Kontext `test` oder `production` ist.
- Produktion erhaelt eine deutlichere visuelle Kennzeichnung als Test.

Akzeptanzkriterien:

- [ ] Jeder Screen mit Instanzbezug zeigt den Umgebungstyp ohne Zusatzklick.
- [ ] Verwechslungsrisiko zwischen Test und Produktion wird durch eindeutige Label/Farbkodierung reduziert.

### C. Konfigurierbare Schreibsperre fuer Produktion

- Pro Projekt gibt es eine Option `productionWriteProtection`.
- Bei aktiver Sperre sind direkte schreibende Aenderungen gegen Produktionsinstanzen blockiert.
- Ausnahmen (z. B. expliziter Deploy-Freigabeflow) sind kontrolliert und auditierbar.

Akzeptanzkriterien:

- [ ] Schreibende Aktionen auf Produktionsinstanz schlagen mit klarer Fehlermeldung fehl, wenn Sperre aktiv ist.
- [ ] Erlaubte Ausnahmen werden mit Zeitpunkt und Benutzer protokolliert.

### D. Testsystem fuer Setup-Aufbau (Objekte und Felder)

- Neue oder geaenderte Setup-Bausteine (insbesondere Custom Objects und Custom Fields) werden zuerst gegen die Testinstanz ausgefuehrt.
- Der Testlauf erzeugt ein verwertbares Ergebnis fuer die spaetere Produktionsuebernahme.

Akzeptanzkriterien:

- [ ] Setup-Aenderungen lassen sich vollstaendig in Test anwenden.
- [ ] Ergebnisstatus ist fuer das Projekt einsehbar (erfolgreich/fehlgeschlagen mit Details).

### E. Projektgesteuerter Deploy von Test nach Produktion

- Aus der Projektverwaltung kann ein gesteuerter Deploy von freigegebenem Setup in die Produktionsinstanz gestartet werden.
- Der Deploy umfasst mindestens Custom Objects und Custom Fields.
- Der Deploy ist nur moeglich, wenn Vorbedingungen erfuellt sind (z. B. erfolgreicher Testlauf, Freigabe).

Akzeptanzkriterien:

- [ ] Produktionsdeploy startet aus dem Projektkontext und nutzt die dem Projekt zugeordnete Produktionsinstanz.
- [ ] Fehlende Vorbedingungen verhindern den Deploy mit eindeutiger Rueckmeldung.

### F. Setup-Versionierung pro Projekt

- Projekte erhalten eine Versionierung des Setups (z. B. Versionsnummer, Zeitstempel, Autor, Aenderungsumfang).
- Jede Deployment-Einheit ist einer Setup-Version zuordenbar.

Akzeptanzkriterien:

- [ ] Mindestens die letzten Setup-Versionen sind pro Projekt nachvollziehbar.
- [ ] Deploys referenzieren die verwendete Setup-Version.

### G. Projektdokumentation und Export

- Das Projekt kann eine technische Dokumentation des aktuellen Setups erzeugen.
- Inhalt mindestens: Connectoren, Scheduler, Mapping, Quelle, Ziel, Outbound-Konfiguration.
- Exportoptionen:
	- Markdown-Datei
	- optional Publikation nach Confluence bei konfigurierter Confluence-URL

Akzeptanzkriterien:

- [ ] Dokument kann als Markdown erzeugt und gespeichert werden.
- [ ] Bei konfigurierter Confluence-URL kann die Doku alternativ veroeffentlicht werden.

### Priorisierungsvorschlag

1. Muss: A, B, D, E
2. Soll: C, F
3. Kann: G (Confluence-Publikation optional)

### Offene Entscheidungen fuer Stakeholder

- Soll ein Projekt mehr als eine Testinstanz oder Produktionsinstanz erlauben?
- Ist die Produktionssperre standardmaessig aktiv oder deaktiviert?
- Welche Freigaberolle darf Produktionsdeploys ausloesen?
- Welche Confluence-Auth-Methode wird verwendet (Token, OAuth, Basic via Proxy)?

### Entscheidungsmatrix

#### 1) Anzahl Instanzen pro Rolle

Optionen:

- Option A: Genau 1 Test + genau 1 Produktion pro Projekt
- Option B: 1..n Test + genau 1 Produktion pro Projekt
- Option C: 1..n Test + 1..n Produktion pro Projekt

Empfehlung:

- Option B

Auswirkung:

- Bietet Flexibilitaet fuer mehrere Teststufen (z. B. Dev/QA/UAT), haelt Produktion aber eindeutig.
- Reduziert Governance-Risiko gegenueber Option C.

#### 2) Standard fuer Produktionssperre

Optionen:

- Option A: Standard `aktiv` (secure by default)
- Option B: Standard `deaktiviert` (schneller Start)

Empfehlung:

- Option A

Auswirkung:

- Senkt Risiko unbeabsichtigter Produktivaenderungen.
- Erfordert bewusste Freigabe fuer produktive Schreibvorgaenge.

#### 3) Freigaberolle fuer Produktionsdeploy

Optionen:

- Option A: Nur Projekt-Owner
- Option B: Projekt-Owner + Release-Manager
- Option C: Jeder mit Admin-Zugriff

Empfehlung:

- Option B

Auswirkung:

- Ermoeglicht Vertretung und Betriebsfaehigkeit, ohne Freigaben zu stark zu oeffnen.
- Passt zu Audit- und Vier-Augen-Anforderungen.

#### 4) Confluence-Authentifizierung

Optionen:

- Option A: API-Token (Service Account)
- Option B: OAuth 2.0
- Option C: Basic Auth via Proxy

Empfehlung:

- Option A als MVP, Option B als Zielausbau

Auswirkung:

- Schnelle Implementierung mit stabiler Betriebsfuehrung.
- OAuth kann spaeter fuer feinere Rechte und SSO nachgezogen werden.

#### 5) Versionsierungstiefe des Setups

Optionen:

- Option A: Metadaten-only (Version, Zeit, Autor, Notiz)
- Option B: Metadaten + Artefakt-Referenz (Deploy-Paket-ID)
- Option C: Vollstaendige Snapshot-Version je Projekt

Empfehlung:

- Option B

Auswirkung:

- Gute Nachvollziehbarkeit bei moderatem Speicherbedarf.
- Ermöglicht reproduzierbare Deploy-Historie ohne Vollsnapshot-Overhead.

### Entscheidungs-Output (fuers Review)

Pro Punkt ist im Review festzuhalten:

- Gewaehlte Option
- Begruendung
- Verantwortlich
- Wirksam ab Version

### Beschlussvorschlag v1

Vorgeschlagene Startkonfiguration fuer die erste Umsetzung:

1. Instanzmodell pro Projekt:
	- Test: 1..n
	- Produktion: genau 1
2. Produktionssperre:
	- Standard aktiv
	- Freigabe fuer produktive Schreibvorgaenge nur explizit
3. Deploy-Freigaberolle:
	- Projekt-Owner und Release-Manager
4. Confluence-Integration:
	- MVP mit API-Token (Service Account)
	- OAuth 2.0 als geplanter Ausbau
5. Setup-Versionierung:
	- Metadaten plus Artefakt-Referenz (Deploy-Paket-ID)

Begruendung:

- Minimiert Produktionsrisiko bei gleichzeitig praktikabler Einfuehrung.
- Erlaubt eine iterative Umsetzung ohne Blockade durch spaetere Ausbaupunkte.
- Schafft frueh auditierbare und reproduzierbare Deployablaeufe.

Abnahmekriterien fuer den Beschluss:

- Produkt, Betrieb und Delivery stimmen den fünf Punkten zu.
- Abweichungen werden mit Alternative und Grund dokumentiert.
- Beschluss wird in dieser Spec unter Status als verbindliche Entscheidung nachgezogen.