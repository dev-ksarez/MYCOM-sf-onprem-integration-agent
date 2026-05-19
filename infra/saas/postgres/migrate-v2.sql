-- SaaS DB Migration v2 – idempotent, sicher auf bestehender DB auszufuehren
-- Ausfuehren mit: psql $DATABASE_URL -f migrate-v2.sql

BEGIN;

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_system_admin boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS log_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  agent_id uuid REFERENCES agents(id),
  run_id uuid REFERENCES scheduler_runs(id),
  occurred_at timestamptz NOT NULL,
  level text NOT NULL CHECK (level IN ('info', 'warn', 'error')),
  code text,
  message text NOT NULL,
  context_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS failed_records (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  run_id uuid NOT NULL REFERENCES scheduler_runs(id),
  record_key text,
  status text,
  error_code text,
  message text,
  source_json jsonb,
  mapped_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_log_events_run ON log_events(run_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_log_events_agent ON log_events(agent_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_failed_records_run ON failed_records(run_id, created_at DESC);

COMMIT;
