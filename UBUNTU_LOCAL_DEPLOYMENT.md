# Ubuntu Local Deployment

Dieses Runbook beschreibt Szenario 2: Betrieb des Agents auf einem Ubuntu-Server im lokalen Netz.

## Zielbild

- Der Agent laeuft als systemd-Service auf einem internen Ubuntu-Server.
- Die Web UI ist nur im LAN oder ueber VPN erreichbar.
- Ein oeffentlicher Reverse Proxy ist fuer dieses Szenario nicht erforderlich.
- Datei-Connectoren koennen lokal oder optional ueber einen internen SFTP-User genutzt werden.

## Verzeichnislayout

- App: `/opt/sf-integration-agent`
- Config: `/etc/sf-integration-agent/agent.env`
- Logs: `/var/log/sf-integration-agent`
- Laufzeitdaten: `/var/lib/sf-integration-agent`
- Dateiablage: `/opt/sf-integration-agent/artifacts/files`

Pflichtvariablen fuer gesicherte Admin-Zugaenge:

- `ADMIN_UI_USERNAME=<admin-user>`
- `ADMIN_UI_PASSWORD=<starkes-passwort>`

## Vorbereitung

1. Node.js 22 installieren.
2. openssh-server installieren, falls Datei-Connectoren per SFTP beliefert werden sollen.
3. Repo oder Release nach `/opt/sf-integration-agent` deployen.
4. Abhaengigkeiten installieren und Build ausfuehren.

Beispiel:

```bash
cd /opt/sf-integration-agent
npm ci
npm run build
```

## Installation

Der Basispfad wird durch [scripts/linux/install-linux-agent.sh](scripts/linux/install-linux-agent.sh) vorbereitet.

Beispiel:

```bash
sudo bash scripts/linux/install-linux-agent.sh \
  --app-dir /opt/sf-integration-agent \
  --service-user sfagent \
  --service-group sfagent \
  --port 9010 \
  --public-host ubuntu-agent.intern.local
```

Danach:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sf-integration-agent.service
sudo systemctl status sf-integration-agent.service
```

## Netzfreigabe im lokalen Netz

- `WEB_UI_HOST=0.0.0.0`, damit die UI im LAN erreichbar ist
- Zugriff nur aus dem internen Netz oder per VPN zulassen
- Falls ein interner Reverse Proxy vorhanden ist, kann dieser optional vorgeschaltet werden
- Keine direkte Internet-Freigabe fuer dieses Szenario

## SFTP fuer Datei-Connectoren

Optional:

```bash
sudo bash scripts/linux/setup-sftp-user.sh \
  --app-dir /opt/sf-integration-agent \
  --service-user sfagent \
  --sftp-user sfagentdrop
```

Das Skript legt einen internen SFTP-Drop fuer `inbound`, `outbound` und `archive` an und verlinkt ihn nach `artifacts/files`.

## Hardening-Checks

- dedizierter Service-User ohne Root-Dauerbetrieb
- restriktive Rechte auf `/etc/sf-integration-agent/agent.env`
- Firewall so setzen, dass nur interne Quellnetze oder VPN zugreifen duerfen
- keine Klartext-Secrets in frei lesbaren Dateien
- Admin-Login fuer die Web UI aktivieren

## Abgrenzung

Dieses Dokument gilt nur fuer Ubuntu im lokalen Netz. Fuer Internetbetrieb mit TLS und Reverse Proxy siehe [LINUX_DEPLOYMENT.md](LINUX_DEPLOYMENT.md).