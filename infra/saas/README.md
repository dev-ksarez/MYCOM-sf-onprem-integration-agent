# SaaS Pilot Infrastructure

Dieses Verzeichnis enthaelt die technische Startbasis fuer die SaaS-Control-Plane auf einem Ubuntu-VServer.

## Ziel

- Zentrale PostgreSQL-Persistenz fuer Tenants, Projekte, Lizenzen, Agenten, Konfigurationen, Runs und Audit.
- Reverse Proxy mit TLS vor der SaaS-App.
- Agenten kommunizieren ausschliesslich ausgehend mit dem SaaS-Dienst.
- Lokale Kundensecrets bleiben im Agenten und werden nicht in PostgreSQL gespeichert.

## Dateien

- `.env.example`: Vorlage fuer Runtime-Secrets und Domain.
- `docker-compose.yml`: PostgreSQL, SaaS-App-Container und Caddy/TLS.
- `Caddyfile`: TLS-Reverse-Proxy-Konfiguration.
- `postgres/schema.sql`: initiales PostgreSQL-Schema.
- `scripts/setup-ubuntu-saas-host.sh`: Ubuntu-Haertung, Docker, Firewall, Fail2ban.
- `scripts/backup-postgres.sh`: komprimiertes PostgreSQL-Backup mit Retention.

## Erstinstallation

```bash
sudo APP_USER=sfagent APP_ROOT=/opt/sf-agent-saas bash scripts/setup-ubuntu-saas-host.sh
sudo rsync -a infra/saas/ /opt/sf-agent-saas/
sudo chown -R sfagent:sfagent /opt/sf-agent-saas
sudo -u sfagent cp /opt/sf-agent-saas/.env.example /opt/sf-agent-saas/.env
```

Danach `.env` lokal auf dem Server mit starken Zufallswerten befuellen und starten:

```bash
cd /opt/sf-agent-saas
docker compose --env-file .env up -d
```

## Security-Regeln

- SSH-Zugang bevorzugt per SSH-Key, nicht per Passwort.
- Keine Produktionspasswoerter oder Tokens in Chat, Tickets oder Git speichern.
- Registration Tokens sind kurzlebig und einmalig nutzbar.
- Agenten erhalten nach Registrierung rotierbare Credentials.
- SaaS speichert nur Connector-Metadaten und Secret-Status, nicht die lokalen Zugangsdaten.
