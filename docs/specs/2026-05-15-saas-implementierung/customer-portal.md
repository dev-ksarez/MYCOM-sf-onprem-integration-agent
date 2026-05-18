# SaaS Kundenportal

## Zweck

Das Kundenportal ist die Webseite fuer SaaS-Kunden. Ein Kunde soll dort seinen Vertrag, seine gebuchten Lizenzen, Agenten, Agent-Dashboards und Installationspakete sehen koennen. Das Portal ist nicht nur eine interne Admin-Oberflaeche, sondern der zentrale Self-Service-Zugang fuer Kunden.

Die zentrale Vertragsanlage mit Vertragsparametern erfolgt nicht im SaaS-Kundenportal, sondern im MYCOM-Salesforce-Kundenbereich. Das SaaS-Portal zeigt daraus eine synchronisierte, fuer den Kunden lesbare Vertragssicht und nutzt die Parameter fuer Lizenz- und Funktionsfreigaben.

## Zielgruppen

- Kunden-Admin: verwaltet Vertragssicht, Benutzer, Projekte, Agenten und Downloads.
- Operator: ueberwacht Agenten, Scheduler, Runs, Logs und Fehlerdaten und pflegt berechtigte Connector-/Scheduler-Konfigurationen.
- Viewer: sieht Status und Dashboards ohne Schreibrechte.
- Support: kann je nach Berechtigung Mandanten und Projekte einsehen, aber keine Kundensecrets abrufen.

## Portalbereiche

### Startseite

Die Startseite zeigt nach Login eine mandantenbezogene Uebersicht:

- Vertragsstatus.
- Lizenzplan und genutzte Limits.
- Aktive Projekte.
- Registrierte Agenten.
- Agenten mit Fehlern, Warnungen oder Updatebedarf.
- Letzte relevanten Runs.
- Offene Fehlerdaten.

### Vertrag und Lizenz

Der Kunde kann seine Vertrags- und Lizenzinformationen einsehen. Fuehrendes System fuer Vertragsanlage, Vertragsparameter und Vertragsaenderungen ist der MYCOM-Salesforce-Kundenbereich.

Anzuzeigen:

- Vertragsnummer oder Kundenreferenz.
- Quelle/Synchronisationsstatus der Vertragssicht.
- Lizenzplan.
- Laufzeit und Status.
- Gebuchte Module.
- Limits fuer Projekte, Agenten, Scheduler, Benutzer und Aufbewahrung.
- Aktuelle Nutzung je Limit.
- Hinweise bei Ueberschreitung oder bald ablaufender Lizenz.

Nicht im ersten Schritt:

- Vollautomatisches Payment.
- Vertragsanlage oder Vertragsaenderung direkt im SaaS-Portal.
- Rechnungsarchiv, sofern nicht separat beauftragt.

Regeln:

- Vertragsparameter werden aus dem MYCOM-Salesforce-Kundenbereich in das SaaS-System synchronisiert.
- Das SaaS-Portal darf Vertragsdaten nicht fuehrend bearbeiten.
- Kunden sehen Vertragsdaten als Self-Service-Ansicht.
- Operative SaaS-Funktionen richten sich nach den synchronisierten Lizenzparametern.
- Synchronisationsfehler muessen im internen Support-/Adminbereich sichtbar sein.

### Agenten

Der Kunde sieht seine registrierten Agenten und deren Zustand.

Anzuzeigen:

- Agent-Name und Installations-ID.
- Zugeordnetes Projekt.
- Betriebsmodus: `legacy`, `hybrid`, `saas`.
- Version.
- Host-/Installationshinweis, soweit freigegeben.
- Letzter Heartbeat.
- Online-/Offline-Status.
- Updatebedarf.
- Aktive Konfigurationsversion und Driftstatus.
- Link zum Agent-Dashboard.

Aktionen:

- Registration Token fuer neuen Agenten erzeugen.
- Agent umbenennen.
- Agent widerrufen.
- Credential-Rotation anstossen.
- Setup/Installer fuer passende Plattform herunterladen.

### Agent-Dashboard

Jeder Agent bekommt ein eigenes Dashboard.

Anzuzeigen:

- Health-Status.
- Letzter Heartbeat.
- Agent-Version und Mindestversion.
- Zugeordnete Projekte und Scheduler.
- Letzte Runs mit Status, Dauer und Datensatzwirkung.
- Fehlertrend.
- Letzte relevanten Logs.
- Fehlgeschlagene Datensaetze mit formatierter JSON-Ansicht.
- Konfigurationsdrift.
- Updatehinweise.

### Connectoren

Der Kunde kann seine Connectoren im SaaS-Portal selbst warten, soweit der Vertrag und die Rolle dies erlauben.

Anzuzeigen:

- Connector-Name.
- Typ, z.B. Salesforce, SAGE100, MSSQL, Datei oder REST.
- Zugeordnetes Projekt und betroffene Scheduler.
- Lokaler Secret-Status, ohne Secret-Werte anzuzeigen.
- Letzter Verbindungstest.
- Letzte erfolgreiche Nutzung.
- Konfigurationsversion.

Aktionen:

- Connector-Metadaten anlegen und bearbeiten.
- Connector aktivieren oder deaktivieren.
- Verbindungstest ueber lokalen Agenten anfordern.
- Lokale Secret-Erfassung im Agenten anstossen oder Anleitung anzeigen.
- Aenderungen als Entwurf speichern.
- Entwurf validieren und als neue ConfigVersion freigeben.

Regeln:

- Lokale Secrets werden nicht im SaaS-Portal angezeigt oder gespeichert.
- Connector-Aenderungen wirken erst nach Freigabe einer ConfigVersion.
- Verbindungstests laufen ueber den lokalen Agenten.
- Jede Aenderung wird auditiert.

### Scheduler und Mapping

Der Kunde kann Scheduler und Mappings selbst warten und anpassen, sofern die Rolle und Lizenz dies erlauben.

Anzuzeigen:

- Scheduler-Name.
- Aktiv/Inaktiv.
- Quelle und Ziel.
- Operation, z.B. Insert, Update oder Upsert.
- Timing.
- Batch-Size.
- SourceDefinition und TargetDefinition.
- Feldmapping.
- Letzte Ausfuehrung und naechster geplanter Lauf.
- Aktive, freigegebene und entworfene ConfigVersion.

Aktionen:

- Scheduler anlegen, bearbeiten, duplizieren, deaktivieren.
- Timing anpassen.
- SourceDefinition und TargetDefinition bearbeiten.
- Mapping bearbeiten.
- Testlauf oder Validierung anfordern.
- Entwurf speichern.
- ConfigVersion freigeben.
- Auf fruehere ConfigVersion zurueckrollen.
- KI-Assistenten fuer bestehende Scheduler-Aenderungen starten.

Regeln:

- Bearbeitung erfolgt immer in einem Entwurf, nie direkt auf der aktiven Runtime-Konfiguration.
- Der lokale Agent fuehrt nur freigegebene ConfigVersions aus.
- Vor Freigabe muessen Syntax, Zielmetadaten, Pflichtfelder und Mapping plausibilisiert werden.
- Fehlerhafte Entwuerfe duerfen nicht automatisch aktiv werden.
- Aktivierung, Deaktivierung, Freigabe und Rollback werden auditiert.

### KI-Assistent fuer Konfigurationsaenderungen

Der Kunde kann den KI-Assistenten nutzen, um bestehende Scheduler und Mappings zu pruefen oder gezielt anzupassen.

Beispiel:

```text
Passe Scheduler SCH-0062 an und ergaenze die Rechnungsadresse und die Kundennummer.
```

Anzuzeigen:

- Gefundener Scheduler und aktive ConfigVersion.
- Aktuelle SourceDefinition und aktuelles Mapping.
- Vorgeschlagene Aenderungen als Diff.
- Begruendung je neuem oder geaendertem Feld.
- Validierungsergebnis fuer Quelle, Ziel und Mapping.
- Offene Annahmen oder Rueckfragen.

Aktionen:

- Vorschlag als Draft uebernehmen.
- Vorschlag weiter bearbeiten.
- Validierung/Testabfrage starten.
- Draft verwerfen.
- Draft nach erfolgreicher Validierung freigeben.

Regeln:

- KI-Aenderungen erfolgen nie direkt auf der aktiven Konfiguration.
- Rechnungsadresse und Lieferadresse muessen fachlich unterschieden werden.
- Lieferfelder duerfen nicht still auf Billing-Felder gemappt werden.
- Kundennummer muss auf ein existierendes und beschreibbares Zielfeld gemappt werden.
- Prompt, Diff, Validierung und Freigabe werden auditiert.

### Setup und Downloads

Der Kunde kann sein Installationssetup herunterladen.

Anzuzeigen:

- Aktuelle stabile Agent-Version.
- Plattform: Windows initial, weitere Plattformen spaeter.
- Download des Installers.
- Optionaler Download einer projektspezifischen Bootstrap-Konfiguration.
- Pruefsumme und Signaturhinweis.
- Installationshinweise fuer Legacy-, Hybrid- und SaaS-Modus.

Regeln:

- Installer-Downloads sind authentifiziert.
- Bootstrap-Dateien enthalten keine lokalen Secrets.
- Registration Tokens sind kurzlebig und muessen getrennt vom Installer behandelt werden.
- Download-Ereignisse werden auditiert.

## Navigation

Initiale Hauptnavigation:

- Dashboard
- Projekte
- Agenten
- Scheduler
- Runs
- Fehlerdaten
- Downloads
- Vertrag
- Benutzer
- Einstellungen

## Berechtigungen

| Bereich | owner | admin | operator | viewer |
| --- | --- | --- | --- | --- |
| Vertrag sehen | ja | ja | nein | nein |
| Benutzer verwalten | ja | ja | nein | nein |
| Downloads sehen | ja | ja | ja | nein |
| Registration Token erzeugen | ja | ja | nein | nein |
| Agenten widerrufen | ja | ja | nein | nein |
| Connectoren bearbeiten | ja | ja | ja | nein |
| Scheduler bearbeiten | ja | ja | ja | nein |
| KI-Konfigurationsvorschlaege erzeugen | ja | ja | ja | nein |
| ConfigVersion freigeben | ja | ja | optional | nein |
| Dashboards sehen | ja | ja | ja | ja |
| Runs/Logs sehen | ja | ja | ja | ja |
| Fehlerdaten sehen | ja | ja | ja | optional |

## Akzeptanzkriterien

- Ein SaaS-Kunde kann sich einloggen und seine Vertrags-/Lizenzdaten sehen.
- Vertragsdaten und Lizenzparameter stammen aus dem MYCOM-Salesforce-Kundenbereich und sind im SaaS-Portal nur lesend sichtbar.
- Ein SaaS-Kunde kann registrierte Agenten und deren Status sehen.
- Ein SaaS-Kunde kann fuer einen Agenten ein Dashboard mit Heartbeat, Version, Runs, Logs und Fehlerdaten oeffnen.
- Ein berechtigter SaaS-Kunde kann Connectoren und Scheduler als Entwurf anlegen und bearbeiten.
- Ein berechtigter SaaS-Kunde kann bestehende Scheduler per KI-Assistent pruefen und gezielt als Draft anpassen.
- Ein berechtigter SaaS-Kunde kann Scheduler-Timing, SourceDefinition, TargetDefinition und Mapping validieren lassen.
- Ein berechtigter SaaS-Kunde kann eine validierte ConfigVersion freigeben oder auf eine fruehere Version zurueckrollen.
- Ein berechtigter SaaS-Kunde kann den passenden Agent-Installer herunterladen.
- Ein berechtigter SaaS-Kunde kann ein kurzlebiges Registration Token fuer eine Installation erzeugen.
- Download- und Token-Ereignisse werden im Audit protokolliert.
- Kein Download oder Bootstrap enthaelt lokale Kundensecrets.
