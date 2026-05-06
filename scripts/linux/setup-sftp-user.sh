#!/usr/bin/env bash

set -euo pipefail

APP_DIR="/opt/sf-integration-agent"
SERVICE_USER="sfagent"
SFTP_USER="sfagentdrop"
SFTP_GROUP="sftpusers"
SFTP_ROOT_BASE="/var/lib/sf-integration-agent/sftp"
SSHD_CONFIG_TARGET="/etc/ssh/sshd_config.d/sf-integration-agent-sftp.conf"
AUTHORIZED_KEYS_DIR="/etc/ssh/authorized_keys"

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
    --sftp-user)
      SFTP_USER="$2"
      shift 2
      ;;
    --sftp-group)
      SFTP_GROUP="$2"
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

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  echo "Service-User nicht gefunden: $SERVICE_USER" >&2
  exit 1
fi

if ! getent group "$SFTP_GROUP" >/dev/null; then
  groupadd --system "$SFTP_GROUP"
fi

if ! id -u "$SFTP_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SFTP_GROUP" --home-dir /nonexistent --shell /usr/sbin/nologin "$SFTP_USER"
fi

SFTP_USER_ROOT="$SFTP_ROOT_BASE/$SFTP_USER"
DROP_DIR="$SFTP_USER_ROOT/drop"

install -d -m 0755 -o root -g root "$SFTP_ROOT_BASE"
install -d -m 0755 -o root -g root "$SFTP_USER_ROOT"
install -d -m 0750 -o "$SFTP_USER" -g "$SFTP_GROUP" "$DROP_DIR"
install -d -m 0750 -o "$SFTP_USER" -g "$SFTP_GROUP" "$DROP_DIR/inbound"
install -d -m 0750 -o "$SFTP_USER" -g "$SFTP_GROUP" "$DROP_DIR/outbound"
install -d -m 0750 -o "$SFTP_USER" -g "$SFTP_GROUP" "$DROP_DIR/archive"

if id -u "$SERVICE_USER" >/dev/null 2>&1; then
  usermod -a -G "$SFTP_GROUP" "$SERVICE_USER"
fi

SERVICE_GROUP="$(id -gn "$SERVICE_USER")"

APP_ARTIFACTS_DIR="$APP_DIR/artifacts"
APP_FILES_LINK="$APP_ARTIFACTS_DIR/files"
install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$APP_ARTIFACTS_DIR"
if [[ -e "$APP_FILES_LINK" && ! -L "$APP_FILES_LINK" ]]; then
  mv "$APP_FILES_LINK" "$APP_FILES_LINK.pre-sftp-backup"
else
  rm -rf "$APP_FILES_LINK"
fi
ln -s "$DROP_DIR" "$APP_FILES_LINK"

install -d -m 0755 -o root -g root "$AUTHORIZED_KEYS_DIR"
touch "$AUTHORIZED_KEYS_DIR/$SFTP_USER"
chown root:root "$AUTHORIZED_KEYS_DIR/$SFTP_USER"
chmod 0600 "$AUTHORIZED_KEYS_DIR/$SFTP_USER"

cat > "$SSHD_CONFIG_TARGET" <<EOF
Match User $SFTP_USER
    ChrootDirectory $SFTP_USER_ROOT
    ForceCommand internal-sftp -d /drop
    AuthorizedKeysFile $AUTHORIZED_KEYS_DIR/%u
    AllowTcpForwarding no
    X11Forwarding no
    PermitTunnel no
    PasswordAuthentication yes
    PubkeyAuthentication yes
EOF

chmod 0644 "$SSHD_CONFIG_TARGET"

echo "SFTP-Drop eingerichtet fuer $SFTP_USER"
echo "Pfad fuer Datei-Connectoren: $DROP_DIR"
echo "Bitte anschliessend pruefen:"
echo "  sshd -t"
echo "  systemctl restart ssh"
echo "  passwd $SFTP_USER oder SSH-Key in $AUTHORIZED_KEYS_DIR/$SFTP_USER hinterlegen"