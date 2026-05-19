CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS tenants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_key text NOT NULL UNIQUE,
  name text NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email citext UNIQUE,
  display_name text,
  password_hash text,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'invited', 'suspended', 'deleted')),
  last_login_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenant_memberships (
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  user_id uuid NOT NULL REFERENCES users(id),
  role text NOT NULL CHECK (role IN ('owner', 'admin', 'operator', 'viewer', 'support')),
  project_scope jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS licenses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  contract_reference text,
  plan text NOT NULL DEFAULT 'pilot',
  status text NOT NULL DEFAULT 'trial' CHECK (status IN ('trial', 'active', 'expired', 'suspended')),
  valid_from timestamptz,
  valid_until timestamptz,
  max_connectors integer NOT NULL DEFAULT 5,
  max_schedulers integer NOT NULL DEFAULT 10,
  max_records_per_month bigint NOT NULL DEFAULT 100000,
  feature_ai boolean NOT NULL DEFAULT false,
  feature_migration boolean NOT NULL DEFAULT false,
  feature_custom_connector boolean NOT NULL DEFAULT false,
  feature_custom_scheduler boolean NOT NULL DEFAULT false,
  limits_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  features_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_system text NOT NULL DEFAULT 'mycom_salesforce',
  source_record_id text,
  synced_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_key text NOT NULL,
  name text NOT NULL,
  mode text NOT NULL DEFAULT 'hybrid' CHECK (mode IN ('legacy', 'hybrid', 'saas')),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'suspended', 'deleted')),
  default_timezone text NOT NULL DEFAULT 'Europe/Berlin',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, project_key)
);

CREATE TABLE IF NOT EXISTS project_instances (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  type text NOT NULL CHECK (type IN ('salesforce', 'sage100', 'mssql', 'file', 'rest', 'other')),
  name text NOT NULL,
  environment text NOT NULL DEFAULT 'test' CHECK (environment IN ('dev', 'test', 'prod', 'sandbox')),
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  secret_status text NOT NULL DEFAULT 'local-only' CHECK (secret_status IN ('local-only', 'missing', 'available-local')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS registration_tokens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  token_hash text NOT NULL UNIQUE,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'claimed', 'expired', 'revoked')),
  expires_at timestamptz NOT NULL,
  claimed_at timestamptz,
  created_by uuid REFERENCES users(id),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  installation_id text NOT NULL,
  name text NOT NULL,
  mode text NOT NULL DEFAULT 'hybrid' CHECK (mode IN ('legacy', 'hybrid', 'saas')),
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'online', 'offline', 'revoked')),
  agent_version text,
  host_fingerprint_hash text,
  capabilities_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_heartbeat_at timestamptz,
  desired_config_version_id uuid,
  applied_config_version_id uuid,
  last_run_config_version_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, installation_id)
);

CREATE TABLE IF NOT EXISTS agent_credentials (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_id uuid NOT NULL REFERENCES agents(id),
  credential_hash text NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'rotating', 'revoked')),
  last_used_at timestamptz,
  expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

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

CREATE TABLE IF NOT EXISTS config_versions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  version_no integer NOT NULL,
  status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'released', 'archived', 'rejected')),
  config_json jsonb NOT NULL,
  released_at timestamptz,
  released_by uuid REFERENCES users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES users(id),
  UNIQUE (tenant_id, project_id, version_no)
);

CREATE TABLE IF NOT EXISTS scheduler_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  agent_id uuid REFERENCES agents(id),
  scheduler_key text NOT NULL,
  config_version_id uuid REFERENCES config_versions(id),
  direction text CHECK (direction IN ('inbound', 'outbound')),
  status text NOT NULL CHECK (status IN ('queued', 'running', 'success', 'partial', 'failed', 'cancelled')),
  total_records bigint,
  read_records bigint NOT NULL DEFAULT 0,
  written_records bigint NOT NULL DEFAULT 0,
  skipped_records bigint NOT NULL DEFAULT 0,
  failed_records bigint NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  error_category text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_heartbeats (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  agent_id uuid NOT NULL REFERENCES agents(id),
  status text NOT NULL,
  mode text NOT NULL,
  agent_version text,
  runtime_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  sent_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id),
  project_id uuid REFERENCES projects(id),
  actor_type text NOT NULL CHECK (actor_type IN ('user', 'agent', 'system')),
  actor_id uuid,
  event_type text NOT NULL,
  resource_type text,
  resource_id text,
  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_projects_tenant ON projects(tenant_id);
CREATE INDEX IF NOT EXISTS idx_agents_project_status ON agents(project_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_project_started ON scheduler_runs(project_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_heartbeats_agent_received ON agent_heartbeats(agent_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_created ON audit_events(tenant_id, created_at DESC);
