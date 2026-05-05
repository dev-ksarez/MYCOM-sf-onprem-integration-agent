# Linux Deployment

Dieses Runbook beschreibt den gehaerteten Minimalpfad fuer den Betrieb des Agents auf einer oeffentlichen Linux-VM.

## Zielbild

- Der Node-Prozess laeuft nur lokal auf `127.0.0.1` hinter nginx.
- TLS wird am Reverse Proxy terminiert.
- Der Agent laeuft unter einem dedizierten Service-User ohne Root-Dauerbetrieb.
- Administrative Zugriffe werden spaeter durch Login/Token ergaenzt; bis dahin darf die Web UI nicht ungeschuetzt oeffentlich freigegeben werden.
- Datei-Connectoren koennen ueber einen separaten, eingeschraenkten SFTP-User Dateien austauschen.

## Verzeichnislayout

- App: `/opt/sf-integration-agent`
- Config: `/etc/sf-integration-agent/agent.env`
- Logs: `/var/log/sf-integration-agent`
- Laufzeitdaten: `/var/lib/sf-integration-agent`
- SFTP-Drop fuer Datei-Connectoren: `/var/lib/sf-integration-agent/sftp/<user>/drop`

## Vorbereitung

1. Node.js 22 installieren.
2. nginx und openssh-server installieren.
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
  --public-host agent.example.com
```

Der Installer:

- legt Service-User und Verzeichnisse an
- erstellt `/etc/sf-integration-agent/agent.env`, falls sie fehlt
- installiert die systemd-Unit aus [scripts/linux/sf-integration-agent.service](scripts/linux/sf-integration-agent.service)
- installiert eine nginx-Template-Konfiguration aus [scripts/linux/nginx-sf-integration-agent.conf](scripts/linux/nginx-sf-integration-agent.conf)

## SFTP fuer Datei-Connectoren

Der Agent kann vorhandene Datei-Connectoren unveraendert weiterverwenden, wenn der SFTP-Drop nach `artifacts/files` verlinkt wird.

Beispiel:

```bash
sudo bash scripts/linux/setup-sftp-user.sh \
  --app-dir /opt/sf-integration-agent \
  --service-user sfagent \
  --sftp-user sfagentdrop
```

Das Skript:

- erstellt einen dedizierten SFTP-User ohne Login-Shell
- erzwingt `internal-sftp`
- legt `inbound`, `outbound` und `archive` unter `/var/lib/sf-integration-agent/sftp/<user>/drop` an
- verlinkt das Drop-Verzeichnis nach `/opt/sf-integration-agent/artifacts/files`
- schreibt eine isolierte OpenSSH-Konfiguration nach `/etc/ssh/sshd_config.d/sf-integration-agent-sftp.conf`

Damit koennen externe Systeme Dateien per SFTP ablegen, und die bestehenden Datei-Connectoren nutzen weiterhin ihre bekannten Pfade:

- `basePath`: `artifacts/files`
- `importPath`: `inbound`
- `exportPath`: `outbound`
- `archivePath`: `archive`

## Reverse Proxy und TLS

1. nginx-Site aus [scripts/linux/nginx-sf-integration-agent.conf](scripts/linux/nginx-sf-integration-agent.conf) nach `/etc/nginx/sites-available/` kopieren.
2. Platzhalter fuer Hostname und Upstream-Port ersetzen.
3. TLS-Zertifikat einbinden, zum Beispiel ueber certbot oder vorhandene Zertifikate.
4. Nur Port 443 oeffentlich freigeben; Port 9010 nur lokal binden.

## Hardening-Checks

- Service-User ohne interaktive Shell
- `WEB_UI_HOST=127.0.0.1`
- keine Klartext-Secrets in world-readable Dateien
- `/etc/sf-integration-agent/agent.env` auf `0640`
- `/var/log/sf-integration-agent` und `/var/lib/sf-integration-agent` mit restriktiven Rechten
- Firewall so setzen, dass nur SSH und HTTPS erreichbar sind

## Nachkontrolle

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sf-integration-agent.service
sudo systemctl status sf-integration-agent.service
sudo nginx -t
sudo systemctl reload nginx
sudo systemctl restart ssh
```