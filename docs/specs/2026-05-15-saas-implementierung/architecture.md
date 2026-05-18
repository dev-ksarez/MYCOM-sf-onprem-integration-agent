# SaaS Architektur

## Zweck

Dieses Dokument konkretisiert Phase 0 der SaaS-Implementierung. Es beschreibt Betriebsmodi, Komponenten, Datenverantwortung, 1Blue-Pilotbetrieb und AWS-Zielbild, ohne bestehende Agenten zu einer Migration zu zwingen.

## Architekturentscheidungen

### ADR-001: SaaS ist optional

Bestehende Agenten bleiben nach einem Update im Legacy-Modus, solange sie nicht explizit fuer ein SaaS-Projekt registriert werden.

Konsequenzen:

- Kein bestehender Scheduler darf allein durch das Update seine fuehrende Konfigurationsquelle wechseln.
- SaaS-Funktionen muessen per Opt-in aktiviert werden.
- Agenten muessen ihren Modus eindeutig melden: `legacy`, `hybrid` oder `saas`.

### ADR-002: 1Blue zuerst, AWS-faehig spaeter

Die erste Entwicklungs- und Pilotplattform laeuft auf einem 1Blue-VServer. Die Fachlogik darf trotzdem nicht an 1Blue-spezifische Pfade oder Services gekoppelt werden.

Konsequenzen:

- Runtime initial als Docker Compose oder systemd Services.
- Datenbank initial PostgreSQL auf dem VServer.
- Dateispeicher initial lokales Server-Dateisystem mit Storage-Abstraktion.
- Spaetere AWS-Zielkomponenten werden dokumentiert, aber nicht in Phase 1 erzwungen.

### ADR-003: SaaS ist Control Plane, Agent ist Execution Plane

Der SaaS-Dienst verwaltet Konfiguration, Versionen, Benutzer, Lizenzen, Dashboards und Betriebsdaten. Der lokale Agent fuehrt Scheduler aus und verbindet lokale Systeme mit Salesforce oder anderen Zielen.

Konsequenzen:

- Lokale Connector-Secrets bleiben lokal.
- SaaS speichert Connector-Metadaten, aber keine lokalen DB-Passwoerter, Salesforce-Refresh-Tokens oder SAGE-Zugangsdaten.
- Agenten kommunizieren ausgehend mit SaaS; es gibt keinen eingehenden Zugriff vom SaaS-Dienst in Kundennetze.

### ADR-004: Konfiguration ist versioniert

Scheduler, Mapping, SourceDefinition, TargetDefinition und Timing werden als freigegebene Versionen verwaltet.

Konsequenzen:

- Der Agent meldet `desiredConfigVersion`, `appliedConfigVersion` und `lastRunConfigVersion`.
- SaaS kann Drift anzeigen, ohne automatisch umzuschalten.
- Rollback ist eine neue freigegebene Version, keine stille Datenbankkorrektur.
- Kundenbearbeitung erfolgt in Entwuerfen; der Agent erhaelt nur freigegebene Versionen.

### ADR-005: Betriebsdaten werden minimiert

SaaS benoetigt Betriebsdaten fuer Monitoring und Support, aber keine unnoetigen Rohdaten aus Kundensystemen.

Konsequenzen:

- Run-Zusammenfassungen enthalten Zaehler, Status, Dauer, Scheduler-ID und Fehlerkategorien.
- Fehlerdaten enthalten nur die zur Diagnose benoetigten Felder und koennen je Tenant/Projekt begrenzt oder maskiert werden.
- Vollstaendige Payloads werden nur bewusst und konfigurierbar uebertragen.

## Komponenten

### SaaS Web/API

- Browser-UI fuer Kundenportal, Benutzer, Vertrag, Lizenzen, Downloads, Projekte, Dashboards, Scheduler, Runs, Logs, Fehlerdaten und Agenten.
- REST-API fuer UI und lokale Agenten.
- Mandantentrennung in jeder API-Anfrage.
- Audit-Events fuer Benutzeraktionen, Agent-Registrierung, Lizenzereignisse, Downloads, Registration Tokens und Konfigurationsaenderungen.

### SaaS Persistenz

Initial auf 1Blue:

- PostgreSQL fuer relationale Plattformdaten.
- Lokaler Dateispeicher fuer groessere Fehlerdaten-/Exportdateien, ueber Storage-Adapter gekapselt.
- Server-Backups fuer DB und Dateispeicher.

Spaeter auf AWS:

- Managed relational DB, z.B. RDS PostgreSQL.
- Object Storage, z.B. S3.
- Queue/Eventing, z.B. SQS/EventBridge, wenn asynchrone Verarbeitung noetig wird.
- Secrets und Konfiguration ueber AWS-native Dienste.

### Local Agent Runtime

- Fuehrt bestehende Scheduler und Connectoren lokal aus.
- Verwaltet lokale Secrets.
- Sendet Heartbeats, Run-Zusammenfassungen, relevante Logs und Fehlerdaten an SaaS, wenn registriert.
- Holt im SaaS-Modus freigegebene Konfigurationsversionen ab.
- Nutzt lokalen Cache fuer zuletzt bekannte SaaS-Konfiguration, damit kurze SaaS-Ausfaelle nicht sofort Scheduler stoppen.

### Auth und Lizenzierung

- Benutzer-Auth fuer SaaS-UI.
- Agent-Auth getrennt von Benutzer-Auth.
- Agent-Registrierung ueber kurzlebige Registration Tokens.
- Laufende Agent-Kommunikation ueber rotierbare Agent-Credentials.
- Lizenzmodell begrenzt Module und Mengen, blockiert aber laufende Datenuebertragungen nicht abrupt.
- Vertragsanlage und Vertragsparameter werden fuehrend im MYCOM-Salesforce-Kundenbereich gepflegt und in SaaS als lesende Lizenz-/Vertragssicht synchronisiert.

### Kundenportal und Downloads

- Kundenportal ist die externe Webseite fuer SaaS-Kunden.
- Kunden sehen Vertrag, Lizenznutzung, Projekte, Agenten, Agent-Dashboards und Setup-Downloads.
- Vertragsdaten sind im Portal lesend; fachliche Aenderungen erfolgen im MYCOM-Salesforce-Kundenbereich.
- Kunden koennen Connectoren und Scheduler selbst pflegen, wenn Rolle und Lizenz dies erlauben.
- Pflege erfolgt versioniert, validiert und auditiert.
- KI-Assistenten-Vorschlaege fuer bestehende Konfigurationen erzeugen nur Drafts und Diffs, keine direkte Aktivierung.
- Installer-Artefakte werden versioniert angeboten.
- Downloads sind authentifiziert und auditiert.
- Optional erzeugte Bootstrap-Konfigurationen enthalten nur SaaS-Endpunkt, Projekt-/Tenant-Bezug und Registrierungsinformationen, aber keine lokalen Secrets.

## Betriebsmodi

| Modus | Fuehrende Konfiguration | Zentrale Logs/Runs | Agent-Registrierung | Ziel |
| --- | --- | --- | --- | --- |
| Legacy | lokal/Salesforce-basiert | nein | nein | Bestehende Installationen unveraendert weiter betreiben |
| Hybrid | lokal/Salesforce-basiert | ja, soweit freigegeben | ja | Zentrale Sichtbarkeit ohne Ausfuehrungswechsel |
| SaaS | SaaS-Konfigurationsversion | ja | ja | Zentrale Plattform steuert freigegebene Scheduler/Mappings |

Umschaltregeln:

- `legacy -> hybrid`: nur durch explizite Agent-Registrierung.
- `hybrid -> saas`: nur durch Projektfreigabe im SaaS und bestaetigte Agent-Faehigkeit.
- `saas -> hybrid`: erlaubt als kontrollierter Rueckfall, wenn zentrale Konfiguration deaktiviert wird.
- `hybrid -> legacy`: erlaubt durch Widerruf der Agent-Registrierung; lokale Ausfuehrung bleibt nutzbar.

## 1Blue-Pilotarchitektur

Mindestbestandteile:

- Reverse Proxy mit TLS.
- SaaS-App/API als eigener Prozess oder Container.
- PostgreSQL.
- Persistenter Storage-Pfad fuer Exporte und Fehlerdaten.
- Backup-Skript fuer DB und Storage.
- Server-Firewall mit nur notwendigen Ports.
- Health-Endpunkt fuer Verfuegbarkeitspruefung.
- Deployment-Skript mit reproduzierbarer Version.

Empfohlene Pilotstruktur:

```text
Internet
  -> Reverse Proxy/TLS
    -> SaaS Web/API
      -> PostgreSQL
      -> Storage Adapter -> local filesystem
```

## AWS-Zielmapping

| Verantwortung | 1Blue Pilot | AWS Ziel |
| --- | --- | --- |
| Web/API Runtime | Docker/systemd auf VServer | ECS/Fargate, App Runner oder EKS |
| Relationale DB | PostgreSQL auf VServer | RDS PostgreSQL |
| Dateispeicher | lokales Dateisystem | S3 |
| Secrets | Server-Env/geschuetzte Dateien | Secrets Manager/SSM |
| TLS/Ingress | Reverse Proxy | ALB/CloudFront/API Gateway |
| Jobs/Queues | Prozessintern/cron | SQS/EventBridge/Worker |
| Monitoring | Server-Logs/Healthcheck | CloudWatch plus App-Metriken |

## Sicherheitsgrenzen

SaaS darf speichern:

- Tenant-, Benutzer-, Rollen- und Lizenzdaten.
- Vertrags- und Download-Metadaten.
- Projekt- und Agent-Metadaten.
- Connector-Metadaten ohne Secrets.
- Scheduler, Mapping, Timing, SourceDefinition und TargetDefinition.
- Run-Zusammenfassungen, technische Logs, Fehlerkategorien, bewusst freigegebene Fehlerdaten.
- Audit-Events.

SaaS darf nicht speichern:

- Lokale DB-Passwoerter.
- SAGE100-Zugangsdaten.
- Salesforce-Refresh-Tokens, wenn diese lokal fuer die Ausfuehrung genutzt werden.
- Vollstaendige Kundendatenbank-Exports.
- Rohdaten ohne klare Diagnose- oder Produktfunktion.
- Lokale Installer-Bootstrap-Dateien mit eingebetteten Kundensecrets.

## Offene Punkte

- Finales Auth-Verfahren fuer Agent-Credentials: Bearer Token plus Rotation oder HMAC-signierte Requests.
- Konkrete Limits je Lizenzmodell.
- Aufbewahrungsfristen fuer Logs und Fehlerdaten.
- Umfang der Payload-Maskierung bei Fehlerdaten.
- Ob SaaS-Konfigurationen zusaetzlich signiert werden muessen oder ob TLS plus Agent-Auth reicht.
