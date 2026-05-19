# SaaS Server-Vorbereitung

## Zweck

Dieses Dokument beschreibt die konkrete Vorbereitung des Ubuntu-Pilotservers fuer die SaaS-Control-Plane. Die Umsetzung ist fuer einen einzelnen VServer ausgelegt, bleibt aber fachlich AWS-faehig.

## Server-Baseline

Mindestkonfiguration:

- Ubuntu LTS.
- Eigener Systembenutzer `sfagent`.
- Nur Ports `22`, `80` und `443` von aussen erreichbar.
- Docker Engine plus Docker Compose Plugin.
- Fail2ban fuer SSH.
- Unattended Security Updates.
- PostgreSQL nur im internen Docker-Netz, nicht oeffentlich.
- TLS am Reverse Proxy.
- Taegliches PostgreSQL-Backup mit Retention.

Vorbereitete Artefakte:

- [infra/saas/README.md](../../../infra/saas/README.md)
- [infra/saas/docker-compose.yml](../../../infra/saas/docker-compose.yml)
- [infra/saas/postgres/schema.sql](../../../infra/saas/postgres/schema.sql)
- [infra/saas/scripts/setup-ubuntu-saas-host.sh](../../../infra/saas/scripts/setup-ubuntu-saas-host.sh)
- [infra/saas/scripts/backup-postgres.sh](../../../infra/saas/scripts/backup-postgres.sh)

## Credential-Regel

SSH-Benutzername und Passwort duerfen nicht in Chat oder Tickets uebertragen werden. Fuer die echte Serverkonfiguration soll bevorzugt ein SSH-Key hinterlegt werden. Falls ein Passwort technisch noetig ist, muss es ueber einen separaten sicheren Kanal oder interaktiv auf der Maschine eingegeben werden.

## Tenant-Bootstrap

Ein Kunde meldet sich nicht mit Salesforce-Daten am lokalen Agenten an. Der Ablauf ist:

1. SaaS Admin legt Tenant, Projekt und Lizenz an.
2. SaaS erzeugt eine eindeutige `tenant_key` und projektbezogene `project_key`.
3. Kunde sieht im Portal ein kurzlebiges Registration Token oder eine Bootstrap-Datei.
4. Lokaler Agent claimt das Token ueber `/api/agent/v1/registrations/claim`.
5. SaaS gibt ein Agent-Credential aus, das nur fuer diesen Tenant und dieses Projekt gilt.
6. Danach kommuniziert der Agent nur noch mit dem zentralen SaaS-Dienst.

## Lizenzmodell

Die erste Datenbankstruktur enthaelt harte Felder fuer diese Limits und Freigaben:

- `max_connectors`
- `max_schedulers`
- `max_records_per_month`
- `feature_ai`
- `feature_migration`
- `feature_custom_connector`
- `feature_custom_scheduler`

Weitere Limits bleiben in `limits_json` erweiterbar, damit Vertragsparameter aus MYCOM Salesforce synchronisiert werden koennen, ohne sofort eine Migration ausloesen zu muessen.

## SaaS Admin Dashboard

Die Admin-Ansicht soll projektuebergreifend zeigen:

- Tenants nach Status, Plan und Lizenzverletzung.
- Projekte nach Modus: `legacy`, `hybrid`, `saas`.
- Agenten online/offline, Version, Updatebedarf und letzter Heartbeat.
- Scheduler-Nutzung gegen Lizenzlimit.
- Monatsverbrauch Datensaetze gegen Lizenzlimit.
- Fehlerquote, Laufzeitentwicklung und Wiederholfehler pro Projekt.
- Konfigurationsdrift: gewuenschte Version gegen vom Agent gemeldete Version.
- Registration Tokens, Downloads und sicherheitsrelevante Audit-Events.

## Kunden-Dashboard

Die Kundenansicht bleibt nah an der bisherigen lokalen UI, wird aber mandanten- und projektbezogen gefiltert:

- Projektstatus und Agent Health.
- Connectoren, Scheduler, Runs und Fehlerdaten.
- Lizenzstatus und genutzte Limits.
- Setup-Downloads und Agent-Registrierung.
- Konfigurationsentwuerfe, Freigaben und Rollback, soweit Rolle und Lizenz es erlauben.
