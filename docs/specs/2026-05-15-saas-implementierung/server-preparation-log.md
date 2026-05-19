# SaaS Server-Vorbereitung Log

Stand: 2026-05-19

## Server

- Host: `178.254.18.231`
- Hostname: `v45106.1blu.de`
- OS: Ubuntu 24.04.3 LTS
- App-Pfad: `/opt/sf-agent-saas`
- Betriebsnutzer: `sfagent`

## Umgesetzt

- SSH-Key fuer Root-Erstzugang hinterlegt.
- SSH-Key fuer Betriebsnutzer `sfagent` hinterlegt.
- `sfagent` angelegt und in Docker-Gruppe aufgenommen.
- SaaS-Artefakte nach `/opt/sf-agent-saas` kopiert.
- `.env` auf dem Server aus `.env.example` erzeugt und mit zufaelligen Secrets befuellt.
- Docker Engine und Docker Compose Plugin installiert.
- UFW aktiviert: eingehend nur `22/tcp`, `80/tcp`, `443/tcp`.
- Fail2ban fuer SSH aktiviert.
- Unattended Security Updates aktiviert.
- PostgreSQL per Docker Compose gestartet.
- PostgreSQL-Schema initialisiert.
- Backup-Service und taeglicher Backup-Timer eingerichtet.
- Initiales PostgreSQL-Backup erfolgreich erzeugt.
- SaaS-API-Image auf dem Server aus Branch-Stand gebaut: `sf-agent-saas-api:local`.
- SaaS-API intern per Docker Compose gestartet.
- Interner Healthcheck erfolgreich: `GET /health` liefert `200`.
- Agent-Registration intern getestet: `POST /api/agent/v1/registrations/claim`.
- Agent-Heartbeat mit ausgegebenem Agent-Credential intern getestet: `POST /api/agent/v1/heartbeats` liefert `202`.
- Domain gesetzt: `v45106.1blu.de` -> `178.254.18.231`.
- Apache Default-Webserver deaktiviert, damit Caddy Port `80/443` uebernehmen kann.

## Bewusst noch nicht gestartet

- Caddy/TLS wurde noch nicht gestartet.

Gruende:

- Es ist noch keine produktive SaaS-Domain auf den Server geschaltet.
- Ohne Domain wuerde TLS nicht sauber ausgestellt.

## Offene operative Schritte

- Initiales Server-Passwort nach Abschluss der Arbeiten aendern.
- DNS fuer die SaaS-Domain auf `178.254.18.231` setzen.
- Produktives SaaS-App-Image in Registry veroeffentlichen und in `SAAS_APP_IMAGE` hinterlegen.
- Danach Caddy/TLS mit `docker compose --env-file .env up -d caddy` starten.
- Root-Login per Passwort nach erfolgreicher Domain-/Key-Pruefung deaktivieren.
- Backup-Ziel optional extern spiegeln.
