-- SaaS DB Migration v4 – Connector- und Scheduler-Konfiguration
-- Idempotent, sicher auf bestehender DB ausfuehrbar
-- Ausfuehren mit: psql $DATABASE_URL -f migrate-v4.sql

BEGIN;

-- Connector-Konfiguration (bereits in Schema, hier idempotent sichern)
CREATE TABLE IF NOT EXISTS connector_configs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  connector_key text NOT NULL,
  type text NOT NULL,
  display_name text NOT NULL,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  secret_policy text NOT NULL DEFAULT 'local-only',
  status text NOT NULL DEFAULT 'active',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, project_id, connector_key)
);

-- Scheduler-Konfiguration (neu)
CREATE TABLE IF NOT EXISTS scheduler_configs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  scheduler_key text NOT NULL,
  name text NOT NULL,
  direction text NOT NULL DEFAULT 'outbound' CHECK (direction IN ('inbound', 'outbound')),
  active boolean NOT NULL DEFAULT true,
  schedule_expression text,
  connector_key text,
  object_name text,
  config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'deleted')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, project_id, scheduler_key)
);

CREATE INDEX IF NOT EXISTS idx_connector_configs_tenant ON connector_configs(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_scheduler_configs_tenant ON scheduler_configs(tenant_id, status);

COMMIT;
