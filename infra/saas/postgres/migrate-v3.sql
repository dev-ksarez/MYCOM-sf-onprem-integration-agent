-- SaaS DB Migration v3 – Annaburger Projekte aus SQLite migrieren
-- Idempotent: kann mehrfach ausgeführt werden
-- Ausfuehren mit: psql $DATABASE_URL -f migrate-v3.sql

BEGIN;

-- 1. Mandant Annaburger anlegen
INSERT INTO tenants (tenant_key, name, status)
VALUES ('annaburger', 'Annaburger', 'active')
ON CONFLICT (tenant_key) DO NOTHING;

-- 2. Lizenz (Trial) anlegen
INSERT INTO licenses (tenant_id, plan, status, max_connectors, max_schedulers, max_records_per_month)
SELECT id, 'pilot', 'trial', 5, 10, 100000
FROM tenants WHERE tenant_key = 'annaburger'
ON CONFLICT DO NOTHING;

-- 3. Default-Projekt anlegen
INSERT INTO projects (tenant_id, project_key, name, mode, default_timezone)
SELECT t.id, 'default', 'Default-Projekt', 'hybrid', 'Europe/Berlin'
FROM tenants t WHERE t.tenant_key = 'annaburger'
ON CONFLICT (tenant_id, project_key) DO NOTHING;

-- 4. Annaburger SAGE100 Projekt anlegen
INSERT INTO projects (tenant_id, project_key, name, mode, default_timezone)
SELECT t.id, 'annaburger-sage100', 'Annaburger SAGE100', 'hybrid', 'Europe/Berlin'
FROM tenants t WHERE t.tenant_key = 'annaburger'
ON CONFLICT (tenant_id, project_key) DO NOTHING;

-- 5. Projekt-Konfiguration für Annaburger SAGE100 (aus SQLite)
INSERT INTO config_versions (tenant_id, project_id, version_no, status, config_json, released_at)
SELECT
  t.id,
  p.id,
  1,
  'released',
  jsonb_build_object(
    'projectKey',   'annaburger-sage100',
    'projectName',  'Annaburger SAGE100',
    'projectId',    'annaburger-sage100',
    'productionWriteProtection', false,
    'lookupCache',  jsonb_build_object('enabled', true, 'ttlMinutes', 5),
    'logBatching',  jsonb_build_object('enabled', true, 'syncIntervalMinutes', 5, 'batchSize', 200, 'bufferMaxEntries', 10000),
    'confluence',   jsonb_build_object(
      'baseUrl',        'https://mycom.atlassian.net',
      'username',       'karsten.sarez@mycom-net.com',
      'apiToken',       '',
      'spaceKey',       'Annaburger',
      'parentPageId',   '4412571652',
      'pageTitlePrefix','ANNB'
    )
  ),
  now()
FROM tenants t
JOIN projects p ON p.tenant_id = t.id AND p.project_key = 'annaburger-sage100'
WHERE t.tenant_key = 'annaburger'
ON CONFLICT (tenant_id, project_id, version_no) DO NOTHING;

COMMIT;

-- Ergebnis anzeigen
SELECT
  t.tenant_key,
  t.name as tenant_name,
  t.status as tenant_status,
  p.project_key,
  p.name as project_name,
  coalesce((SELECT version_no::text FROM config_versions cv WHERE cv.project_id = p.id ORDER BY version_no DESC LIMIT 1), '-') as config_version
FROM tenants t
JOIN projects p ON p.tenant_id = t.id
WHERE t.tenant_key = 'annaburger'
ORDER BY p.project_key;
