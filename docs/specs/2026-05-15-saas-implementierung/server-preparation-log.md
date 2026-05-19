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
- SSH gehaertet: Passwortlogin deaktiviert, Root nur noch per Key.
- Caddy/TLS aktiv unter `https://v45106.1blu.de`.
- SaaS Portal aktiv: `https://v45106.1blu.de/portal`.
- SaaS Overview API aktiv: `https://v45106.1blu.de/api/saas/v1/overview`.

## Extern geprueft

- `GET https://v45106.1blu.de/health` -> `200`.
- `GET http://v45106.1blu.de/health` -> `308` auf HTTPS.
- `GET https://v45106.1blu.de/portal` -> `200`.
- `GET https://v45106.1blu.de/api/saas/v1/overview` -> `200`.

## Offene operative Schritte

- Initiales Server-Passwort nach Abschluss der Arbeiten aendern.
- DNS fuer die SaaS-Domain auf `178.254.18.231` setzen.
- Produktives SaaS-App-Image in Registry veroeffentlichen und in `SAAS_APP_IMAGE` hinterlegen.
- Danach Caddy/TLS mit `docker compose --env-file .env up -d caddy` starten.
- Portal-Login und Rollenrechte vor echter Kundennutzung aktivieren.
- Root-Login vollstaendig deaktivieren, sobald ein zweiter Admin-Key oder Break-Glass-Zugang vorhanden ist.
- Backup-Ziel optional extern spiegeln.
