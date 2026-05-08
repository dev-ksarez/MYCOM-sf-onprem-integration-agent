---
connie-publish: true
connie-title: 03 - Installationsbeschreibung
tags:
  - sf-onprem-integration-agent
  - installation
---

# Installationsbeschreibung

![Installation](./assets/screenshots/06-installation.png)

## Voraussetzungen

- Node.js 22 oder hoeher.
- Zugriff auf Salesforce Login-/Instance-URL.
- Salesforce Connected App mit benoetigtem OAuth-Flow.
- Zugriff auf lokale Ziel-/Quellsysteme, z. B. MSSQL, Dateiablage oder REST-Endpunkte.
- Admin-Zugang fuer die Web UI.

## Wichtige Environment-Variablen

| Variable | Zweck |
| --- | --- |
| `SF_LOGIN_URL` | Salesforce Login-URL, z. B. `https://login.salesforce.com` oder Sandbox-URL |
| `SF_CLIENT_ID` | Connected-App Consumer Key |
| `SF_CLIENT_SECRET` | Connected-App Consumer Secret |
| `WEB_UI_PORT` | Port der lokalen Web UI |
| `AGENT_API_ENABLED` | Aktiviert die Agent API |
| `AGENT_API_PORT` | Port der Agent API, Standard `8090` |
| `AGENT_API_TOKEN` | Bearer Token fuer Remote-Zugriff |
| `ADMIN_UI_USERS_FILE` | Lokale Benutzerdatei fuer Web-Login |
| `ADMIN_AUTH_MODE` | `local` oder `salesforce_oidc` |

## Build

```bash
npm ci
npm run build
```

## Windows-Installation

Zielbild:

- App-Verzeichnis: `C:\apps\sf-onprem-integration-agent`
- Windows-Dienste fuer Agent, Web und Updater
- Optionales Kundenpaket inklusive `node_modules`
- Auto-Update ueber Release-Manifest

Kurzablauf:

1. Kundenpaket entpacken.
2. `.env` konfigurieren oder interaktive Installation starten.
3. Salesforce- und MSSQL-/SAGE100-Zugangsdaten erfassen.
4. Dienste installieren.
5. Web UI oeffnen und Scheduler/Connectoren pruefen.

Beispiel:

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run build
npm run win:install -- -AppRoot "C:\apps\sf-onprem-integration-agent"
```

Getrennte Rollen:

```powershell
powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile agent-host
powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile web-host
```

Service-Pruefung:

```powershell
Get-Service SfOnpremIntegrationAgent, SfOnpremIntegrationWeb, SfOnpremIntegrationUpdater
```

## Linux-Installation

Zielbild:

- App: `/opt/sf-integration-agent`
- Config: `/etc/sf-integration-agent/agent.env`
- Logs: `/var/log/sf-integration-agent`
- Runtime-Daten: `/var/lib/sf-integration-agent`
- systemd-Dienste und optional nginx-Reverse-Proxy mit TLS

Beispiel:

```bash
cd /opt/sf-integration-agent
npm ci
npm run build

sudo bash scripts/linux/install-linux-agent.sh \
  --app-dir /opt/sf-integration-agent \
  --service-user sfagent \
  --service-group sfagent \
  --port 9010 \
  --public-host agent.example.com
```

Nachkontrolle:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sf-integration-agent.service
sudo systemctl status sf-integration-agent.service
sudo nginx -t
sudo systemctl reload nginx
```

## Salesforce-Metadaten

Deployment:

```bash
npm run sf:deploy-metadata
```

Noetige Variablen:

- `SF_LOGIN_URL`
- `SF_CLIENT_ID`
- `SF_CLIENT_SECRET`
- `SF_USERNAME`
- `SF_PASSWORD`

## Update und Rollback

Der AutoUpdater prueft ein Release-Manifest, laedt neue Artefakte, erstellt ein Backup und fuehrt bei Fehlern einen Rollback aus. Unter Windows kann die Update-Pruefung ueber die Web UI oder per Skript erfolgen.

```powershell
cd C:\apps\sf-onprem-integration-agent
npm run win:update-now
```
