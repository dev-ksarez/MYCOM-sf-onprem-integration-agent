#!/usr/bin/env bash
set -euo pipefail

APP_USER="${APP_USER:-sfagent}"
APP_ROOT="${APP_ROOT:-/opt/sf-agent-saas}"
SSH_PORT="${SSH_PORT:-22}"
TIMEZONE="${TIMEZONE:-Europe/Berlin}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root or with sudo." >&2
  exit 1
fi

apt-get update
export DEBIAN_FRONTEND=noninteractive
export TZ="${TIMEZONE}"
ln -snf "/usr/share/zoneinfo/${TIMEZONE}" /etc/localtime
echo "${TIMEZONE}" >/etc/timezone

DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates \
  curl \
  fail2ban \
  ufw \
  unattended-upgrades \
  gnupg \
  lsb-release

install -m 0755 -d /etc/apt/keyrings
if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
fi

if [[ ! -f /etc/apt/sources.list.d/docker.list ]]; then
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" > /etc/apt/sources.list.d/docker.list
fi

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

if ! id "${APP_USER}" >/dev/null 2>&1; then
  useradd --create-home --shell /bin/bash "${APP_USER}"
fi

usermod -aG docker "${APP_USER}"
install -o "${APP_USER}" -g "${APP_USER}" -m 0750 -d "${APP_ROOT}" "${APP_ROOT}/backups" "${APP_ROOT}/storage"

cat >/etc/fail2ban/jail.d/sshd.local <<EOF
[sshd]
enabled = true
port = ${SSH_PORT}
maxretry = 5
findtime = 10m
bantime = 1h
EOF

systemctl enable --now fail2ban
dpkg-reconfigure -f noninteractive unattended-upgrades

ufw default deny incoming
ufw default allow outgoing
ufw allow "${SSH_PORT}/tcp"
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

cat <<EOF
SaaS host baseline prepared.

Next steps:
1. Copy infra/saas/* to ${APP_ROOT}.
2. Create ${APP_ROOT}/.env from .env.example and set strong secrets.
3. Point DNS for the SaaS domain to this server.
4. Start with: docker compose --env-file .env up -d

Do not paste SSH passwords, database passwords, or production secrets into chat or tickets.
EOF
