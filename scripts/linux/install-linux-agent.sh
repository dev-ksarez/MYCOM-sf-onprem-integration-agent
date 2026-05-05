#!/usr/bin/env bash

set -euo pipefail

APP_DIR="/opt/sf-integration-agent"
SERVICE_USER="sfagent"
SERVICE_GROUP="sfagent"
APP_PORT="9010"
PUBLIC_HOST=""
ENV_DIR="/etc/sf-integration-agent"
ENV_FILE="$ENV_DIR/agent.env"
SYSTEMD_TARGET="/etc/systemd/system/sf-integration-agent.service"
NGINX_TARGET="/etc/nginx/sites-available/sf-integration-agent.conf"
LOG_DIR="/var/log/sf-integration-agent"
DATA_DIR="/var/lib/sf-integration-agent"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app-dir)
      APP_DIR="$2"
      shift 2
      ;;
    --service-user)
      SERVICE_USER="$2"
      shift 2
      ;;
    --service-group)
      SERVICE_GROUP="$2"
      shift 2
      ;;
    --port)
      APP_PORT="$2"
      shift 2
      ;;
    --public-host)
      PUBLIC_HOST="$2"
      shift 2
      ;;
    *)
      echo "Unbekannter Parameter: $1" >&2
      exit 1
      ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Dieses Skript muss als root laufen." >&2
  exit 1
fi

if [[ ! -d "$APP_DIR" ]]; then
  echo "App-Verzeichnis nicht gefunden: $APP_DIR" >&2
  exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null; then
  groupadd --system "$SERVICE_GROUP"
fi

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir "$APP_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$LOG_DIR"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$DATA_DIR"
install -d -m 0750 -o root -g "$SERVICE_GROUP" "$ENV_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  cat > "$ENV_FILE" <<EOF
NODE_ENV=production
WEB_UI_ENABLED=1
WEB_UI_HOST=127.0.0.1
WEB_UI_PORT=$APP_PORT
LOG_LEVEL=info
EOF
  chown root:"$SERVICE_GROUP" "$ENV_FILE"
  chmod 0640 "$ENV_FILE"
fi

sed \
  -e "s|__APP_DIR__|$APP_DIR|g" \
  -e "s|__SERVICE_USER__|$SERVICE_USER|g" \
  -e "s|__SERVICE_GROUP__|$SERVICE_GROUP|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__LOG_DIR__|$LOG_DIR|g" \
  "$APP_DIR/scripts/linux/sf-integration-agent.service" > "$SYSTEMD_TARGET"

chmod 0644 "$SYSTEMD_TARGET"

if [[ -n "$PUBLIC_HOST" ]]; then
  sed \
    -e "s|__PUBLIC_HOST__|$PUBLIC_HOST|g" \
    -e "s|__APP_PORT__|$APP_PORT|g" \
    "$APP_DIR/scripts/linux/nginx-sf-integration-agent.conf" > "$NGINX_TARGET"
  chmod 0644 "$NGINX_TARGET"
fi

chown -R "$SERVICE_USER":"$SERVICE_GROUP" "$APP_DIR"

echo "Linux-Basisinstallation vorbereitet."
echo "Naechste Schritte:"
echo "  1. Environment-Datei pruefen: $ENV_FILE"
echo "  2. systemd laden: systemctl daemon-reload"
echo "  3. Service starten: systemctl enable --now sf-integration-agent.service"
if [[ -n "$PUBLIC_HOST" ]]; then
  echo "  4. nginx-Site aktivieren und TLS konfigurieren: $NGINX_TARGET"
fi