# SaaS Implementierung

- Spec-ID: 2026-05-15-saas-implementierung
- Status: draft
- Owner: Produkt/Engineering
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Der bestehende On-Prem Integration Agent wird aktuell lokal je Kundenumgebung betrieben und ueber Salesforce-Metadaten, lokale Artefakte und eine Admin-Web-UI gesteuert. Kuenftig soll optional ein zentraler SaaS-Dienst als Konfigurations- und Betriebsplattform fungieren. Die Zielarchitektur soll AWS-faehig sein; um Entwicklungskosten zu reduzieren, wird die erste Entwicklungs- und Pilotphase jedoch auf einem virtuellen Server bei 1Blue betrieben. Salesforce kann damit perspektivisch als Steuerungsplattform fuer Agent-Konfiguration, Scheduler, Logs und Monitoring abgeloest werden.

Die SaaS-Implementierung muss klar trennen zwischen zentraler Steuerung, Persistenz, Benutzer-/Lizenzverwaltung und Dashboards im SaaS-Dienst sowie lokaler Ausfuehrung durch kundenseitige Agenten. Der lokale Agent bleibt in Kundennetzen installiert und fuehrt Scheduler zwischen lokalen Connectoren und Salesforce aus, in beide Richtungen. Bestehende Agenteninstallationen muessen zunaechst ohne SaaS-Anbindung weiterlaufen koennen.

SaaS-Kunden erhalten eine zentrale Webseite als Kundenportal. Dort sehen sie Vertrag und Lizenz, registrierte Agenten, Agent-Dashboards, Projektstatus und Downloads fuer das Installationssetup.

Die zentrale Vertragsanlage mit Vertragsparametern bleibt im MYCOM-Salesforce-Kundenbereich. Das SaaS-Kundenportal zeigt diese Vertragsdaten als synchronisierte, lesende Vertragssicht und leitet daraus Lizenz- und Funktionsfreigaben ab.

## Problem

Aktuell sind Betrieb, Projektverwaltung, Monitoring, KI-Assistenz und Update-Informationen stark an eine lokale Installation und Salesforce als technische Steuerungsplattform gekoppelt. Fuer mehrere Kunden entstehen dadurch diese Probleme:

- Kein zentraler Ueberblick ueber Kunden, Projekte, Instanzen, Agent-Versionen und Laufstatus.
- Updates, Diagnose und Konfigurationsvergleich sind pro Installation manuell zu pruefen.
- Projekt- und Scheduler-Konfigurationen sind nicht sauber zwischen zentraler Verwaltung und lokaler Ausfuehrung versioniert.
- Logs, Fehlerdaten und Betriebsmetriken liegen verteilt in lokalen Installationen bzw. Salesforce-nahen Strukturen statt zentral pro Projekt.
- Salesforce ist gleichzeitig Ziel-/Quellsystem und Steuerungsplattform; diese Kopplung erschwert Mandantenfaehigkeit, Lizenzierung und produktisierte SaaS-Dashboards.
- Bestehende Kunden duerfen durch die SaaS-Einfuehrung nicht gezwungen werden, sofort ihren Betriebsmodus zu wechseln.
- SaaS-Funktionen duerfen keine direkten Kundendatenbank-Zugangsdaten oder Rohdaten benoetigen.
- Rollen, Mandantenfaehigkeit und Audit muessen klarer als in der lokalen Admin-UI modelliert werden.
- Kunden benoetigen einen Self-Service-Zugang fuer Vertrag, Lizenzstatus, Agentenuebersicht, Dashboards und Download des passenden Agent-Setups.

## Zielbild

- Ein zentraler SaaS-Dienst kann Mandanten, Benutzer, Lizenzen, Projekte, registrierte Agenten, Salesforce-Instanzen, Connector-Konfigurationen, Scheduler, Mappings, Logs, Fehlerdaten, Health-Pulse, Update-Status und zentrale Audit-Events verwalten.
- Die erste Entwicklungs-/Pilotumgebung laeuft kostensparend auf einem 1Blue-VServer; die Architektur bleibt so geschnitten, dass spaetere AWS-Migration moeglich ist.
- SaaS ist ein optionaler Betriebsmodus. Nicht registrierte Bestandsagenten laufen im bisherigen lokalen/Salesforce-gestuetzten Modus weiter.
- Lokale Agenten melden sich nur nach expliziter Registrierung projektbezogen am SaaS-Dienst an und erhalten dann von dort ihre freigegebenen Konfigurationen.
- Lokale Agenten bleiben fuer On-Prem-Quellen, SAGE100/MSSQL/File/REST-Zugriffe und eigentliche Datenuebertragung zu oder von Salesforce verantwortlich.
- Agenten kommunizieren ausgehend mit dem SaaS-Dienst ueber signierte Heartbeats, Konfigurationsabruf, Command-Polling und Ergebnis-/Fehlerberichte.
- Scheduler- und Mapping-Konfigurationen werden versioniert; Agenten fuehren nur freigegebene aktuelle Versionen aus.
- Die SaaS-UI ergaenzt bzw. ersetzt je nach Betriebsmodus die lokale zentrale Betriebsoberflaeche und zeigt projekt- und mandantenbezogene Dashboards, Monitore, Runs, Logs, Fehlerdaten, Konfigurationsdrift und Update-Hinweise.
- Das SaaS-Kundenportal zeigt Vertrags-/Lizenzdaten, gebuchte Module, genutzte Limits, Agentenstatus, Agent-Dashboards und einen geschuetzten Downloadbereich fuer Installer und Bootstrap-Konfiguration.
- Berechtigte Kunden koennen Connectoren, Scheduler, Timing, SourceDefinition, TargetDefinition und Mapping selbst im SaaS-Portal warten; Aenderungen werden versioniert, validiert und erst nach Freigabe vom Agenten ausgefuehrt.
- Vertragsanlage und Vertragsparameter werden fuehrend im MYCOM-Salesforce-Kundenbereich gepflegt und in SaaS synchronisiert.
- Benutzerverwaltung und Lizenzmodell steuern, welche Mandanten, Projekte, Module, Agenten und Funktionen ein Nutzer verwenden darf.
- Sensitive lokale Zugangsdaten bleiben kundenseitig; SaaS speichert nur notwendige Metadaten, Statusdaten und bewusst freigegebene Konfiguration.
- Bestehende Einzelinstallationen koennen schrittweise per Opt-in angebunden werden, ohne sofortigen Funktionsbruch oder Pflichtmigration.

## Nicht-Ziele

- Vollstaendige Verlagerung der Datenuebertragung in die Cloud.
- Speicherung von SAGE100/MSSQL-Rohdaten oder lokalen DB-Credentials im SaaS.
- Sofortige Abschaffung aller lokalen Diagnosemoeglichkeiten. Lokale Minimaldiagnose bleibt fuer Support und Offline-Faelle erlaubt.
- Verpflichtende SaaS-Anbindung fuer bestehende Agenten im ersten Schritt.
- Entfernung des bisherigen lokalen/Salesforce-gestuetzten Betriebsmodus ohne explizite Migration.
- Direkte produktive AWS-Multi-Service-Implementierung in der Entwicklungsphase.
- Multi-Region- oder Enterprise-Billing-Implementierung im ersten Schritt.
- Breaking Change fuer bestehende Kundeninstallationen ohne Migrationspfad.

## Akzeptanzkriterien

- [ ] Verhalten ist fuer den Nutzer oder Operator eindeutig beobachtbar.
- [ ] Erfolgs- und Fehlerfall sind beschrieben.
- [ ] Konfiguration, Migration oder Deployment-Folgen sind dokumentiert.
- [ ] Mandantentrennung ist fuer API, UI, Persistenz und Audit beschrieben.
- [ ] Agent-Registrierung und Authentifizierung sind spezifiziert.
- [ ] Offline-/Retry-Verhalten fuer Agent-Kommunikation ist spezifiziert.
- [ ] Keine sensiblen lokalen Zugangsdaten werden unnoetig im SaaS persistiert.
- [ ] Updatepfad fuer bestehende Installationen ist beschrieben.
- [ ] SaaS-Betriebsmodus ist optional; bestehende Agenten laufen ohne Registrierung weiter.
- [ ] Hybrid-/Legacy-Modus und SaaS-Modus sind technisch unterscheidbar und dokumentiert.
- [ ] Salesforce kann als Control Plane abgeloest werden; Salesforce bleibt im SaaS-Zielbild nur Quell-/Zielsystem fuer Integrationen.
- [ ] Benutzerverwaltung und Lizenzmodell sind fachlich und technisch spezifiziert.
- [ ] Projekt-Dashboards bilden die bisher lokalen Informationen zentral ab.
- [ ] Kundenportal zeigt Vertrag, Lizenz, Agenten, Agent-Dashboards und Setup-Downloads.
- [ ] Kunden koennen Connectoren und Scheduler selbst als Entwurf pflegen, validieren, freigeben und zurueckrollen.
- [ ] Vertrags- und Lizenzparameter werden aus dem MYCOM-Salesforce-Kundenbereich synchronisiert und im SaaS-Portal nur lesend dargestellt.
- [ ] Installer-Downloads und Registration Tokens sind authentifiziert, auditiert und enthalten keine lokalen Secrets.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- src/
- scripts/
- salesforce/
- artifacts/
- infra/1blue/
- infra/aws/
- docs/

Vertiefende Phase-0-Dokumente:

- [SaaS Architektur](./architecture.md)
- [Agent API Contract](./agent-api-contract.md)
- [SaaS Datenmodell](./data-model.md)
- [SaaS Kundenportal](./customer-portal.md)
- [KI-Assistent fuer Konfigurationspruefung und -anpassung](./ai-assistant-config-editing.md)

Technische Leitplanken:

- Datenfluss und betroffene Adapter benennen.
- Schnittstellen, Konfiguration und Rueckwaertskompatibilitaet festhalten.
- Logging, Retry, Scheduler- oder UI-Auswirkungen nennen.

Vorgeschlagene Architektur-Slices:

1. SaaS Control Plane
   - Mandanten, Projekte, Benutzer/Rollen, Lizenzmodell, Agent-Registrierung, API-Keys/Secrets.
   - Zentrale REST-API fuer Agent-Registrierung, Konfigurationsabruf, Heartbeats, Command-Polling, Run-Ergebnisse, Logs und Fehlerdaten.
   - Zentrale Persistenz fuer Connector-Metadaten, Scheduler, Mapping, Target-/SourceDefinition, Logs, Run-Historie, Fehlerdaten und Audit.
   - Audit-Log fuer Konfigurationsaenderungen, Lizenzereignisse, Benutzeraktionen und operative Kommandos.
   - Wird nur fuer registrierte SaaS-/Hybrid-Projekte zum fuehrenden Konfigurationssystem.
   - Entwicklungs-/Pilotbetrieb auf 1Blue-VServer; AWS-Zielarchitektur bleibt portable Zielvorgabe.

2. Local Agent Runtime
   - Ausgehende Verbindung zum SaaS-Backend.
   - Lokale Secret-Verwaltung fuer MSSQL/SAGE100/File/REST/Salesforce.
   - Legacy-Modus: Agent nutzt bisherige lokale/Salesforce-gestuetzte Konfiguration weiter.
   - SaaS-Modus: Agent fuehrt nur aktuelle, signierte oder versionierte Scheduler-Konfigurationen aus dem SaaS-Dienst aus.
   - Hybrid-/Migrationsmodus: Agent kann lokale Konfiguration auslesen, an SaaS melden und nach Freigabe auf SaaS-Konfiguration umstellen.
   - Lokale UI bleibt fuer Bootstrap, Diagnose, lokale Secrets und Legacy-Betrieb verfuegbar.
   - Uebertraegt im SaaS-Modus Run-Status, technische Logs und Fehlerdaten zentral an den SaaS-Dienst.

3. Configuration Versioning
   - Scheduler, Connectoren, Mapping, TargetDefinition und Timing erhalten Versions-/Revision-Metadaten.
   - Agent meldet aktive und zuletzt ausgefuehrte Konfigurationsversion.
   - SaaS erkennt Drift zwischen freigegebener Zielversion und lokaler Runtime.
   - Konfigurationen werden projektbezogen freigegeben, aktiviert und optional zurueckgerollt.

4. Monitoring und Fehlerdaten
   - Aggregierte Runs, Logs, Fehlerdaten und Health-Pulse mandantenbezogen anzeigen.
   - Rohdaten nur bewusst und minimiert uebertragen.
   - Fehlerdaten muessen lesbar, exportierbar und revisionssicher referenzierbar sein.
   - Dashboards ersetzen die bisher lokalen Monitor-, Scheduler-, Connector-, Projekt- und Instanzuebersichten pro Projekt.

5. Update und Migration
   - Bestehender Windows-Updateprozess bleibt Grundlage.
   - SaaS zeigt Update-Empfehlungen und Agent-Versionen.
   - Migration bestehender Kunden erfolgt optional ueber Registrierung des lokalen Agenten.
   - Ohne Registrierung aendert sich der bestehende Betriebsmodus nicht.

6. Benutzerverwaltung und Lizenzierung
   - Benutzer gehoeren zu Mandanten und erhalten Rollen/Rechte auf Mandanten-, Projekt- oder Modul-Ebene.
   - Lizenzmodell begrenzt z.B. Anzahl Projekte, Agenten, Connectoren, Scheduler, Nutzer, Datenvolumen oder aktivierte Module.
   - Lizenzstatus beeinflusst UI und API-Funktionen nachvollziehbar, ohne laufende Datenuebertragungen unkontrolliert abzubrechen.

7. Kundenportal und Setup-Downloads
   - Kunden sehen Vertrag, Lizenzplan, gebuchte Module, genutzte Limits und Laufzeit.
   - Vertragsanlage und Vertragsparameter erfolgen zentral im MYCOM-Salesforce-Kundenbereich; SaaS nutzt eine synchronisierte Kopie.
   - Kunden sehen ihre Agenten, Agent-Dashboards, Runs, Logs, Fehlerdaten und Updatebedarf.
   - Kunden koennen Connectoren, Scheduler, Timing und Mapping selbst warten, sofern Rolle und Lizenz dies erlauben.
   - Berechtigte Kunden koennen den aktuellen Agent-Installer und optionale Bootstrap-Konfiguration herunterladen.
   - Downloads und Registration Tokens werden auditiert und enthalten keine lokalen Secrets.

8. KI-Assistent fuer bestehende Konfigurationen
   - Der KI-Assistent kann bestehende Scheduler, Connectoren und Mappings laden, pruefen und als Draft-ConfigVersion anpassen.
   - Nutzer koennen natuerliche Anweisungen geben, z.B. "Passe Scheduler SCH-0062 an und ergaenze die Rechnungsadresse und die Kundennummer".
   - Der Assistent zeigt einen Diff aus bestehender und vorgeschlagener Konfiguration.
   - Vorschlaege werden gegen Quellmetadaten, SAGE100-/MSSQL-Dokumentation und Salesforce-Metadaten validiert.
   - Aktivierung erfolgt nur nach expliziter Freigabe durch berechtigte Nutzer.

## Entwicklungs- und Zielbetrieb

Phase 1: 1Blue-VServer fuer Entwicklung und Pilot

- Ziel ist eine kostenguenstige Entwicklungsumgebung ohne laufende AWS-Kosten.
- Betrieb als klassischer virtueller Server mit Docker/Compose oder systemd Services.
- Zentrale Datenhaltung zunaechst ueber relationale DB auf dem Server, z.B. PostgreSQL.
- Datei-/Fehlerdaten koennen initial lokal auf dem Server-Dateisystem abgelegt werden, spaeter abstrahiert migrierbar zu Object Storage.
- TLS, Backups, Firewall, Deployment und Monitoring muessen fuer den VServer explizit beschrieben werden.

Phase 2: AWS-faehige Zielarchitektur

- Applikation, Persistenz, Dateispeicher, Queue/Eventing und Secrets werden so gekapselt, dass spaetere AWS-Migration moeglich bleibt.
- Keine harte Kopplung an 1Blue-spezifische Pfade oder Serverannahmen in der Fachlogik.
- Infrastrukturdefinitionen fuer AWS werden erst konkretisiert, wenn das SaaS-Modell fachlich stabil ist.

## Zentrale Datenhaltung

Der SaaS-Dienst wird im SaaS-Modus System of Record fuer:

- Mandanten, Benutzer, Rollen, Lizenzen.
- Projekte und Projektinstanzen.
- Agent-Registrierungen, Agent-Versionen, Heartbeats, Online-/Offline-Status.
- Connector-Metadaten ohne lokale Secrets.
- Scheduler, Timing, SourceDefinition, TargetDefinition, Mapping und Freigabestatus.
- Runs, Run-Schritte, Logs, Fehlerdaten, Metriken und Audit-Historie.

Dies gilt nur fuer Projekte im SaaS-Modus. Fuer Bestandsprojekte im Legacy-Modus bleiben die bisherigen Speicherorte und Steuerungsmechanismen zunaechst fuehrend.

Der lokale Agent bleibt System of Execution fuer:

- Zugriff auf lokale Connectoren und lokale Dateien.
- Zugriff auf Kundendatenbanken, SAGE100 und andere interne Systeme.
- Salesforce-API-Zugriffe fuer konkrete Integrationslaeufe.
- Lokale Secret-Verwaltung und lokale technische Diagnose.

## Betriebsmodi

1. Legacy-Modus
   - Bestehende Agenten laufen ohne SaaS-Registrierung weiter.
   - Konfiguration, Logs und Monitoring bleiben im bisherigen lokalen/Salesforce-gestuetzten Modell.
   - Updates duerfen diesen Modus nicht brechen.

2. Hybrid-Modus
   - Agent ist im SaaS registriert, meldet Status, Versionen, Runs und optional Konfigurationssnapshots.
   - Fuehrende Konfiguration kann noch lokal/Salesforce-basiert bleiben.
   - SaaS zeigt Beobachtung und Drift, ohne automatisch die Ausfuehrung umzuschalten.

3. SaaS-Modus
   - SaaS-Dienst ist fuehrende Konfigurationsplattform.
   - Agent ruft Scheduler, Connector-Metadaten, Mapping und Timing zentral ab.
   - Logs, Run-Daten und Fehlerdaten werden zentral abgelegt.

## Dashboard-Scope

Die SaaS-Dashboards muessen projektbezogen mindestens diese bisher lokalen Informationen zentral darstellen:

- Projektuebersicht: Instanzen, Agenten, Lizenzstatus, Konfigurationsstatus, letzte Aktivitaet.
- Scheduler: aktive/inaktive Scheduler, naechster Lauf, letzte Ausfuehrung, Konfigurationsversion.
- Runs: Erfolg, Fehler, Dauer, Datensatzanzahlen, Fehlerdaten.
- Logs: nur relevante Ereignisse, Fehler, Warnungen und Aktivitaeten mit Datensatzwirkung.
- Connectoren: Status, Typ, letzte Verbindungstests, lokale Secret-Verfuegbarkeit nur als Status.
- Agenten: Version, Host/Installation, letzter Heartbeat, Updatebedarf.
- Konfigurationsdrift: lokale ausgefuehrte Version vs. zentral freigegebene Version.

## Kundenportal-Scope

Das SaaS-Kundenportal muss als externe Kunden-Webseite mindestens diese Bereiche enthalten:

- Dashboard: Vertragsstatus, Lizenznutzung, aktive Projekte, Agentenstatus, letzte relevante Runs und offene Fehler.
- Vertrag: Vertragsreferenz, Plan, Laufzeit, gebuchte Module, Limits und aktuelle Nutzung.
- Vertragssynchronisation: Quelle MYCOM Salesforce, letzter Sync, Syncstatus und Supporthinweis bei veralteten Daten.
- Agenten: registrierte Installationen, Version, letzter Heartbeat, Modus, Projektzuordnung, Updatebedarf und Driftstatus.
- Agent-Dashboard: Health, Heartbeat, Runs, Logs, Fehlerdaten, Konfigurationsversion und Updatehinweise je Agent.
- Connectoren: Metadaten, Secret-Status, Verbindungstest ueber Agent, genutzte Scheduler und Konfigurationsversion.
- Scheduler: Aktivstatus, Timing, Quelle, Ziel, Operation, Mapping, Test/Validierung, Freigabe und Rollback.
- KI-Assistent: bestehende Konfiguration pruefen, gezielte Aenderungen als Diff vorschlagen, Draft validieren und Freigabe vorbereiten.
- Downloads: aktueller Agent-Installer, Pruefsumme, optionale Bootstrap-Konfiguration und Installationshinweise.
- Benutzer: Rollen und Berechtigungen fuer Kunden-Admins, Operatoren und Viewer.

Setup-Downloads muessen authentifiziert und auditiert sein. Bootstrap-Konfigurationen duerfen keine lokalen Secrets enthalten; Registration Tokens sind kurzlebig und getrennt vom Installer zu behandeln.

Connector- und Scheduler-Aenderungen muessen als Entwurf erfolgen. Nur validierte und freigegebene ConfigVersions duerfen an lokale Agenten ausgeliefert werden.

Der KI-Assistent darf bestehende Konfigurationen nicht direkt aktiv veraendern. Er arbeitet immer auf einer Draft-ConfigVersion, referenziert die aktive Ausgangsversion und muss Diff, Validierungsergebnis und offene Annahmen anzeigen.

## Umsetzungsplan

Phase 0: Architekturentscheidungen und Schnittstellenvertrag

- Betriebsmodi Legacy, Hybrid und SaaS fachlich finalisieren.
- Entscheiden, welche bestehende lokale Funktionen im Pilot unveraendert bleiben.
- API-Kontrakte fuer Agent-Registrierung, Heartbeat, Konfigurationsabruf, Run-Meldung, Logs und Fehlerdaten definieren.
- Datenklassifikation festlegen: lokale Secrets, technische Metadaten, Betriebsdaten, Fehlerdaten, moegliche personenbezogene Daten.
- Ergebnis: versionierter Architektur- und API-Entwurf ohne produktive Code-Abhaengigkeit.

Phase 1: 1Blue-Grundplattform

- 1Blue-VServer fuer Entwicklungs-/Pilotbetrieb vorbereiten.
- Deployment-Basis festlegen: Docker Compose oder systemd, Reverse Proxy, TLS, Firewall, Backup.
- Zentrale DB einrichten, initial bevorzugt PostgreSQL.
- Server-seitige Konfiguration ueber Umgebungsvariablen und Secrets trennen.
- Minimalen Health-Endpunkt, Build-/Deploy-Skript und Betriebsdokumentation erstellen.
- Ergebnis: SaaS-Service ist als leere Plattform erreichbar und betreibbar.

Phase 2: SaaS-Datenmodell und Auth

- Tabellen/Modelle fuer Tenant, User, Role, License, Project, Agent, Instance, ConfigVersion, RunSummary, LogEvent und AuditEvent anlegen.
- Benutzerverwaltung mit Login, Rollen und projektbezogenen Berechtigungen implementieren.
- Lizenzmodell zunaechst technisch einfach halten, aber Datenmodell fuer Limits und MYCOM-Salesforce-Synchronisation vorbereiten.
- Audit fuer Login, Projektzugriff, Agent-Registrierung und Konfigurationsaenderungen erfassen.
- Ergebnis: Nutzer koennen sich anmelden und sehen mandanten-/projektbezogene Grunddaten.

Phase 3: Agent-Registrierung und Hybrid-Telemetrie

- Opt-in-Registrierung fuer bestehende Agenten implementieren.
- Agent sendet Heartbeats, Version, Projektbindung, Betriebsmodus und technische Statusdaten.
- Agent sendet Run-Zusammenfassungen, relevante Logs und Fehlerdaten an SaaS, ohne lokale Secrets zu uebertragen.
- Legacy-Ausfuehrung bleibt unveraendert fuehrend; SaaS beobachtet nur.
- Ergebnis: bestehende Agenten laufen weiter, SaaS zeigt zentrale Status- und Run-Daten.

Phase 4: Projekt-Dashboards

- SaaS-Dashboard fuer Tenant/Projekt/Agenten erstellen.
- Kundenportal fuer Vertrag, Lizenz, Agentenuebersicht und Setup-Downloads erstellen.
- Projektuebersicht, Scheduler-Uebersicht, Run-Historie, Logs, Fehlerdaten und Agent-Versionen darstellen.
- Self-Service-Ansichten fuer Connectoren und Scheduler als Entwurf mit Validierung vorbereiten.
- Filter nach Projekt, Agent, Scheduler, Zeitraum, Status und Fehlerart umsetzen.
- Fehlerdaten mit lesbarer JSON-Anzeige und Exportmoeglichkeit bereitstellen.
- Ergebnis: SaaS bildet die wichtigsten lokalen Monitorinformationen zentral ab und bietet Kunden Self-Service fuer Vertrag, Agenten und Setup.

Phase 5: Zentrale Konfiguration im SaaS-Modus

- Zentrales Modell fuer Connector-Metadaten, Scheduler, Timing, SourceDefinition, TargetDefinition und Mapping implementieren.
- Kundenbearbeitung fuer Connectoren, Scheduler und Mapping ueber Rollen, Lizenz und Audit absichern.
- KI-Assistent fuer bestehende Scheduler-Aenderungen an Draft-ConfigVersions anbinden.
- Konfigurationsversionen mit Freigabe-/Aktivierungsstatus einfuehren.
- Agent kann freigegebene Konfigurationen abrufen und lokale Ausfuehrung daran binden.
- Drift-Erkennung zwischen ausgefuehrter Agent-Version und zentraler Soll-Version anzeigen.
- Ergebnis: erste Projekte koennen optional vom Hybrid-Modus in SaaS-Modus wechseln.

Phase 6: Migration bestehender Installationen

- Bestehende lokale/Salesforce-Konfigurationen auslesen und als SaaS-Konfigurationsentwurf importieren.
- Importierte Konfigurationen pruefen, versionieren und erst nach Freigabe aktivieren.
- Rollback auf Legacy-/Hybrid-Modus dokumentieren.
- Updateprozess so erweitern, dass Kunden Agenten registrieren koennen, aber nicht muessen.
- Ergebnis: Bestandskunden koennen kontrolliert und reversibel migrieren.

Phase 7: AWS-Migrationsfaehigkeit

- Abstraktionen fuer DB, Dateispeicher, Queue/Eventing, Secrets und Deployment pruefen.
- 1Blue-spezifische Annahmen aus Fachlogik entfernen oder kapseln.
- AWS-Zielmapping fuer spaetere Migration dokumentieren.
- Kostenmodell fuer AWS-Betrieb mit Pilotdaten abschaetzen.
- Ergebnis: belastbare Grundlage fuer spaetere AWS-Produktionsarchitektur.

## Aufgaben

- [ ] 1Blue-Entwicklungsarchitektur fuer App, DB, TLS, Deployment, Backup und Monitoring definieren.
- [ ] AWS-Zielarchitektur fuer API, Auth, Persistenz, Queues/Events und Deployment als spaetere Migrationszielstruktur definieren.
- [ ] Datenmodell fuer Tenant, User, Role, License, Project, Agent, Instance, Connector, Schedule, Mapping, ConfigVersion, RunSummary, LogEvent, FailedRecord und AuditEvent definieren.
- [ ] Betriebsmodi Legacy, Hybrid und SaaS inklusive Umschaltbedingungen spezifizieren.
- [ ] Agent-Registrierung inklusive Secret-Rotation, Widerruf und Opt-in-Prozess spezifizieren.
- [ ] SaaS-Agent-Protokoll fuer Heartbeat, Commands, Ack, Logs und Fehlerdaten spezifizieren.
- [ ] Konfigurationsabruf des lokalen Agenten aus dem SaaS-Dienst spezifizieren.
- [ ] Persistenzstrategie festlegen: 1Blue-Pilotpersistenz, spaetere AWS-zentrale DB/Objektspeicher/Eventing, lokale Secrets/Cache.
- [ ] Optionale Ablösung der Salesforce-Control-Plane fuer Scheduler, Logs und Konfiguration planen.
- [ ] Benutzerverwaltung und Lizenzmodell ausarbeiten.
- [ ] UI-Scope fuer SaaS-Control-Center und projektbezogene Dashboards definieren.
- [ ] Kundenportal fuer Vertrag, Lizenznutzung, Agentenuebersicht, Agent-Dashboard und Setup-Downloads spezifizieren.
- [ ] Self-Service-Pflege fuer Connectoren, Scheduler, Timing, SourceDefinition, TargetDefinition und Mapping spezifizieren.
- [ ] KI-Assistent fuer Pruefung und Anpassung bestehender Scheduler/Mappings spezifizieren.
- [ ] Installer-Artefakte, Versionen, Pruefsummen und Download-Audit modellieren.
- [ ] Backward-compatible Updatepfad fuer bestehende Kundeninstallationen beschreiben.
- [ ] Legacy-Agent-Kompatibilitaet durch Tests/Smoke-Szenarien absichern.
- [ ] Sicherheitsgrenzen und Datenschutz fuer lokale Kundendaten dokumentieren.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Manuelle Checks in Web UI oder Agent: Legacy-Agent ohne SaaS weiterlaufen lassen; SaaS-Nutzer anmelden, Vertrags-/Lizenzstatus sehen, Projekt oeffnen, Connector/Scheduler als Entwurf bearbeiten, KI-Assistent mit "Passe Scheduler SCH-0062 an und ergaenze die Rechnungsadresse und die Kundennummer" testen, validieren, ConfigVersion freigeben, Agent registrieren, Heartbeat sehen, Agent-Dashboard oeffnen, Installer herunterladen, Konfiguration abrufen, Run melden.
- Betriebsrelevante Beobachtung nach Deploy: SaaS zeigt Agent-Version, letzten Pulse, letzten Run, Fehlerstatus, Logs, Fehlerdaten und Konfigurationsversion pro Projekt.

## Status

- Status: draft
- Letzte Entscheidung: AWS-SaaS wird optionale zentrale Konfigurations- und Betriebsplattform; bestehende Agenten laufen zunaechst im Legacy-Modus weiter.
- Naechster Schritt: konkrete Implementierungsschnitte fuer Kundenportal, Downloadbereich, Migrationspfad und Phase 1 definieren.
