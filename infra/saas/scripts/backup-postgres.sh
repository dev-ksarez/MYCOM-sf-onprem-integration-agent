#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

set -a
# shellcheck source=/dev/null
source "${APP_ROOT}/.env"
set +a

BACKUP_TARGET_DIR="${BACKUP_TARGET_DIR:-${APP_ROOT}/backups}"
BACKUP_RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${BACKUP_TARGET_DIR}/postgres-${POSTGRES_DB}-${STAMP}.sql.gz"

mkdir -p "${BACKUP_TARGET_DIR}"
docker compose --env-file "${APP_ROOT}/.env" -f "${APP_ROOT}/docker-compose.yml" exec -T postgres \
  pg_dump -U "${POSTGRES_USER}" "${POSTGRES_DB}" | gzip -9 > "${OUT}"
chmod 600 "${OUT}"

find "${BACKUP_TARGET_DIR}" -type f -name "postgres-${POSTGRES_DB}-*.sql.gz" -mtime "+${BACKUP_RETENTION_DAYS}" -delete
echo "Backup written: ${OUT}"
