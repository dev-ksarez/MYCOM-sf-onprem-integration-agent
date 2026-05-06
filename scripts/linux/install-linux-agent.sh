#!/usr/bin/env bash

set -euo pipefail

APP_DIR="/opt/sf-integration-agent"
SERVICE_USER="sfagent"
SERVICE_GROUP="sfagent"
APP_PORT="9010"
PUBLIC_HOST=""
INSTALL_ROLES="agent,web,updater"
ENV_DIR="/etc/sf-integration-agent"
ENV_FILE="$ENV_DIR/agent.env"
SYSTEMD_AGENT_TARGET="/etc/systemd/system/sf-integration-agent.service"
SYSTEMD_WEB_TARGET="/etc/systemd/system/sf-integration-web.service"
SYSTEMD_UPDATER_TARGET="/etc/systemd/system/sf-integration-updater.service"
NGINX_TARGET="/etc/nginx/sites-available/sf-integration-agent.conf"
LOG_DIR="/var/log/sf-integration-agent"
DATA_DIR="/var/lib/sf-integration-agent"

resolve_roles() {
  local raw_roles="$1"
  local normalized
  normalized="$(printf '%s' "$raw_roles" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]' | sed '/^$/d' | sort -u)"

  if [[ -z "$normalized" ]]; then
    normalized=$'agent\nweb\nupdater'
  fi

  if printf '%s\n' "$normalized" | grep -qx 'all'; then
    normalized=$'agent\nweb\nupdater'
  fi

  while IFS= read -r role; do
    [[ -z "$role" ]] && continue
    case "$role" in
      agent|web|updater) ;;
      *)
        echo "Ungueltige Rolle: $role (erlaubt: agent, web, updater, all)" >&2
        exit 1
        ;;
    esac
  done <<< "$normalized"

  printf '%s\n' "$normalized"
}

has_role() {
  local sought="$1"
  local role
  while IFS= read -r role; do
    [[ "$role" == "$sought" ]] && return 0
  done <<< "$RESOLVED_ROLES"
  return 1
}

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
    --roles)
      INSTALL_ROLES="$2"
      shift 2
      ;;
    *)
      echo "Unbekannter Parameter: $1" >&2
      exit 1
      ;;
  esac
done

RESOLVED_ROLES="$(resolve_roles "$INSTALL_ROLES")"

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
WEB_UI_HOST=127.0.0.1
WEB_UI_PORT=$APP_PORT
LOG_LEVEL=info
UPDATE_CHECK_INTERVAL_MS=900000
EOF
  chown root:"$SERVICE_GROUP" "$ENV_FILE"
  chmod 0640 "$ENV_FILE"
fi

for template in \
  "agent:sf-integration-agent.service:$SYSTEMD_AGENT_TARGET" \
  "web:sf-integration-web.service:$SYSTEMD_WEB_TARGET" \
  "updater:sf-integration-updater.service:$SYSTEMD_UPDATER_TARGET"
do
  role_name="${template%%:*}"
  remainder="${template#*:}"
  source_name="${remainder%%:*}"
  target_name="${remainder#*:}"
  if ! has_role "$role_name"; then
    rm -f "$target_name"
    continue
  fi
  sed \
    -e "s|__APP_DIR__|$APP_DIR|g" \
    -e "s|__SERVICE_USER__|$SERVICE_USER|g" \
    -e "s|__SERVICE_GROUP__|$SERVICE_GROUP|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
    -e "s|__LOG_DIR__|$LOG_DIR|g" \
    "$APP_DIR/scripts/linux/$source_name" > "$target_name"
  chmod 0644 "$target_name"
done

RUNTIME_DIR="$APP_DIR/artifacts/runtime"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$RUNTIME_DIR"
cat > "$RUNTIME_DIR/install-profile.json" <<EOF
{
  "updatedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "roles": [
$(printf '%s\n' "$RESOLVED_ROLES" | sed 's/^/    "/;s/$/",/' | sed '$ s/,$//')
  ],
  "settings": {
    "webUiPort": $APP_PORT
  }
}
EOF
chown "$SERVICE_USER:$SERVICE_GROUP" "$RUNTIME_DIR/install-profile.json"
chmod 0640 "$RUNTIME_DIR/install-profile.json"

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
echo "  Rollen: $(printf '%s' "$RESOLVED_ROLES" | paste -sd ', ' -)"
echo "  1. Environment-Datei pruefen: $ENV_FILE"
echo "  2. systemd laden: systemctl daemon-reload"
SERVICE_ENABLE_LIST=()
has_role agent && SERVICE_ENABLE_LIST+=("sf-integration-agent.service")
has_role web && SERVICE_ENABLE_LIST+=("sf-integration-web.service")
has_role updater && SERVICE_ENABLE_LIST+=("sf-integration-updater.service")
if [[ ${#SERVICE_ENABLE_LIST[@]} -gt 0 ]]; then
  echo "  3. Dienste starten: systemctl enable --now ${SERVICE_ENABLE_LIST[*]}"
fi
if [[ -n "$PUBLIC_HOST" ]]; then
  echo "  4. nginx-Site aktivieren und TLS konfigurieren: $NGINX_TARGET"
fi
