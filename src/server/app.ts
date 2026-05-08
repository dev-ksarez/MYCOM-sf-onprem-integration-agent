
import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import {
  AdminDataService,
  SalesforceInstanceMutationInput,
  ConnectorMutationInput,
  ScheduleMutationInput,
  ScheduleCheckpointMutationInput,
  LogChartRange,
  OverviewStatsRange,
  SetupExportDocument,
  MigrationConfig,
  ScheduleFormOptions
} from "./admin-data-service";
import { isRemoteAgentConfigured, syncRemoteAgentInstances } from "../runtime/remote-agent-client";
import {
  appendSetCookie,
  authenticateLocalAdminUser,
  buildAdminSalesforceOidcRedirectUri,
  buildSalesforceLoginAuthorizationUrl,
  buildCsrfCookie,
  buildExpiredSessionCookie,
  buildMigrationOauthRedirectUri,
  buildSessionCookie,
  clearAdminSession,
  completeSalesforceLogin,
  constantTimeEquals,
  consumeMigrationOauthState,
  createAdminSession,
  createMigrationOauthState,
  createSalesforceLoginState,
  getAdminSession,
  getAdminAuthConfig,
  getOrCreateCsrfToken,
  deleteAdminUser,
  listAdminUsers,
  saveAdminUser,
  hasPermission,
  hasModuleAccess,
  hasAllowedRequestOrigin,
  hasValidAdminSession,
  hasValidCsrfToken,
  isMutatingMethod,
  type AdminUserMutationInput
} from "./admin-auth";
import { renderConnectorUiModule } from "./connector-ui-module";
import { getDashboardUpdateStatus, triggerDashboardUpdate } from "./dashboard-update-service";
import { HealthSnapshot } from "./health-snapshot";
import { generateInstallerFiles, getInstallerSummary, INSTALLER_OUTPUT_DIR, InstallerGenerationInput } from "./installer-generator";
import { renderMigrationFailedRecordsModule, renderMigrationPlanningModule, renderMigrationPreflightModule, renderMigrationProgressModule, renderMigrationRunResultModule, renderMigrationUiModule } from "./migration-ui-module";
import { renderSchedulerUiModule } from "./scheduler-ui-module";
import { generateSalesforceMappingRules } from "../core/mapping-dsl/salesforce-mapping-generator";

const BOOTSTRAP_CSS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/css/bootstrap.min.css");
const BOOTSTRAP_JS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/js/bootstrap.bundle.min.js");
const CHART_JS_FILE = path.resolve(process.cwd(), "node_modules/chart.js/dist/chart.umd.js");
const APP_STYLE_CSS_FILE = path.resolve(process.cwd(), "src/css/style.css");
const AGENT_UI_CSS_FILE = path.resolve(process.cwd(), "src/css/agent-ui.css");
const TEMPLATE_STORE_CSS_FILE = path.resolve(process.cwd(), "src/css/template-store.css");
const UI_ASSET_VERSION = "20260506-dashboard-chart-range-1";
const SETUP_EXAMPLE_JSON_FILE = path.resolve(
  process.cwd(),
  "artifacts/file-examples/setup-file-import-export.example.json"
);

async function readJsonBody(req: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString("utf8").trim();
  return raw ? (JSON.parse(raw) as unknown) : {};
}

function escapeHtml(value: string): string {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderLoginShell(options: { errorMessage?: string; csrfToken?: string; authMode?: "local" | "salesforce_oidc" } = {}): string {
  const errorMessage = options.errorMessage || "";
  const csrfToken = options.csrfToken || "";
  const authMode = options.authMode || "local";
  const safeErrorMessage = escapeHtml(errorMessage);
  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="sf-agent-csrf-token" content="${escapeHtml(csrfToken)}" />
    <title>SF Integration Agent Login</title>
    <link href="/assets/bootstrap.min.css" rel="stylesheet" />
    <link href="/assets/style.css?v=${UI_ASSET_VERSION}" rel="stylesheet" />
    <style>
      body { min-height: 100vh; margin: 0; background: linear-gradient(135deg, #eef4f8 0%, #d9e7ef 100%); }
      .agent-login-shell { min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
      .agent-login-card { width: min(100%, 28rem); border: 0; border-radius: 1.25rem; box-shadow: 0 1.5rem 3rem rgba(31, 57, 76, 0.16); }
    </style>
  </head>
  <body>
    <main class="agent-login-shell">
      <div class="card agent-login-card">
        <div class="card-body p-4 p-lg-5">
          <div class="mb-4">
            <div class="text-uppercase text-secondary small fw-semibold">Geschützter Bereich</div>
            <h1 class="h3 mb-2">SF Integration Agent</h1>
            <p class="text-secondary mb-0">${authMode === "salesforce_oidc" ? "Bitte mit Salesforce anmelden." : "Bitte mit Admin-Benutzer und Passwort anmelden."}</p>
          </div>
          <div id="login-error" class="alert alert-danger ${safeErrorMessage ? "" : "d-none"}" role="alert">${safeErrorMessage || "Anmeldung fehlgeschlagen"}</div>
          ${authMode === "local" ? `<form id="login-form" class="d-grid gap-3">
            <div>
              <label for="login-username" class="form-label">Benutzername</label>
              <input id="login-username" name="username" class="form-control" autocomplete="username" required />
            </div>
            <div>
              <label for="login-password" class="form-label">Passwort</label>
              <input id="login-password" name="password" type="password" class="form-control" autocomplete="current-password" required />
            </div>
            <button type="submit" class="btn btn-primary">Anmelden</button>
          </form>` : `<div class="d-grid gap-3"><a class="btn btn-primary" href="/auth/salesforce/login">Mit Salesforce anmelden</a></div>`}
        </div>
      </div>
    </main>
    <script>
      var loginForm = document.getElementById('login-form');
      if (loginForm) {
      loginForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const username = String(document.getElementById('login-username').value || '').trim();
        const password = String(document.getElementById('login-password').value || '');
        const errorBox = document.getElementById('login-error');
        errorBox.classList.add('d-none');
        try {
          const csrfToken = String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || '');
          const response = await fetch('/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
            body: JSON.stringify({ username, password })
          });
          const payload = await response.json().catch(() => ({ error: 'Anmeldung fehlgeschlagen' }));
          if (!response.ok) {
            throw new Error(payload.error || 'Anmeldung fehlgeschlagen');
          }
          window.location.href = '/';
        } catch (error) {
          errorBox.textContent = error.message || 'Anmeldung fehlgeschlagen';
          errorBox.classList.remove('d-none');
        }
      });
      }
    </script>
  </body>
</html>`;
}

function formatSalesforceLoginCallbackError(req: http.IncomingMessage, error: string, description: string): string {
  const message = description || error || "Salesforce-Login fehlgeschlagen.";
  if (error !== "redirect_uri_mismatch") {
    return message;
  }

  try {
    const redirectUri = buildAdminSalesforceOidcRedirectUri(req);
    return [
      message,
      `In der Salesforce Connected App muss diese Callback URL exakt hinterlegt sein: ${redirectUri}`
    ].join(" ");
  } catch {
    return [
      message,
      "Pruefe SF_IDP_REDIRECT_URI und die Callback URL in der Salesforce Connected App."
    ].join(" ");
  }
}

const AUDIT_HISTORY_FILE = path.resolve(process.cwd(), "artifacts/audit-history.json");

interface AuditActor {
  userId?: string;
  username?: string;
}

async function appendAuditHistory(entry: {
  action: string;
  entityType: string;
  entityId?: string;
  entityName?: string;
  actor?: AuditActor | null;
  status?: "success" | "error";
  message?: string;
}): Promise<void> {
  const now = new Date().toISOString();
  let items: unknown[] = [];
  try {
    items = JSON.parse(await fs.readFile(AUDIT_HISTORY_FILE, "utf8")) as unknown[];
    if (!Array.isArray(items)) {
      items = [];
    }
  } catch {
    items = [];
  }

  const auditEntry = {
    id: `audit-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    at: now,
    actor: entry.actor || null,
    action: entry.action,
    entityType: entry.entityType,
    entityId: entry.entityId,
    entityName: entry.entityName,
    status: entry.status || "success",
    message: entry.message
  };
  const nextItems = [auditEntry, ...items].slice(0, 500);
  await fs.mkdir(path.dirname(AUDIT_HISTORY_FILE), { recursive: true });
  await fs.writeFile(AUDIT_HISTORY_FILE, JSON.stringify(nextItems, null, 2), "utf8");
}

async function listAuditHistory(limit = 100, filter?: { entityType?: string; entityId?: string }): Promise<unknown[]> {
  try {
    const items = JSON.parse(await fs.readFile(AUDIT_HISTORY_FILE, "utf8")) as unknown[];
    if (!Array.isArray(items)) {
      return [];
    }
    const entityType = String(filter?.entityType || "").trim();
    const entityId = String(filter?.entityId || "").trim();
    const filtered = items.filter((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        return false;
      }
      const candidate = item as Record<string, unknown>;
      if (entityType && String(candidate.entityType || "") !== entityType) {
        return false;
      }
      if (entityId && String(candidate.entityId || "") !== entityId) {
        return false;
      }
      return true;
    });
    return filtered.slice(0, Math.max(1, Math.min(500, limit)));
  } catch {
    return [];
  }
}

function renderMigrationOauthCallbackShell(options: { ok: boolean; migrationId: string; message: string }): string {
  const payload = JSON.stringify({
    type: "migration-salesforce-oauth",
    ok: options.ok,
    migrationId: options.migrationId,
    message: options.message
  }).replace(/</g, "\\u003c");

  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Salesforce Login</title>
    <link href="/assets/bootstrap.min.css" rel="stylesheet" />
  </head>
  <body class="bg-light d-flex align-items-center justify-content-center min-vh-100">
    <div class="card shadow-sm border-0" style="max-width:32rem; width:100%;">
      <div class="card-body p-4 text-center">
        <h1 class="h5 mb-3">${escapeHtml(options.ok ? "Salesforce-Freigabe gespeichert" : "Salesforce-Freigabe fehlgeschlagen")}</h1>
        <p class="text-secondary small mb-0">${escapeHtml(options.message)}</p>
      </div>
    </div>
    <script>
      (function() {
        var payload = ${payload};
        try {
          if (window.opener && !window.opener.closed) {
            window.opener.postMessage(payload, window.location.origin);
          } else {
            setTimeout(function() {
              window.location.href = '/';
            }, 1200);
          }
        } catch (error) {
          console.error(error);
        }
        setTimeout(function() {
          try {
            window.close();
          } catch (error) {
            console.error(error);
          }
        }, 250);
      })();
    </script>
  </body>
</html>`;
}

function renderAuthConfigurationShell(): string {
  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>SF Integration Agent Konfiguration erforderlich</title>
    <link href="/assets/bootstrap.min.css" rel="stylesheet" />
    <link href="/assets/style.css?v=${UI_ASSET_VERSION}" rel="stylesheet" />
  </head>
  <body class="bg-light">
    <main class="container py-5">
      <div class="card shadow-sm border-0 mx-auto" style="max-width: 40rem;">
        <div class="card-body p-4">
          <div class="text-uppercase text-secondary small fw-semibold mb-2">Sichere Voreinstellung</div>
          <h1 class="h4 mb-3">Admin-Zugang ist noch nicht konfiguriert</h1>
          <p class="text-secondary mb-3">Im Produktionsmodus bleibt die Web-UI gesperrt, bis ADMIN_UI_USERNAME und ADMIN_UI_PASSWORD gesetzt sind.</p>
          <div class="alert alert-warning mb-0" role="alert">Bitte die Environment-Datei ergänzen und den Dienst neu starten.</div>
        </div>
      </div>
    </main>
  </body>
</html>`;
}

function htmlShell(): string {
  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="sf-agent-csrf-token" content="__CSRF_TOKEN__" />
    <title>SF Integration Agent</title>
    <link href="/assets/bootstrap.min.css" rel="stylesheet" />
    <link href="/assets/style.css?v=${UI_ASSET_VERSION}" rel="stylesheet" />
    <link href="/assets/agent-ui.css?v=${UI_ASSET_VERSION}" rel="stylesheet" />
    <link href="/assets/template-store.css?v=${UI_ASSET_VERSION}" rel="stylesheet" />
  </head>
  <body>
    <div class="agent-shell">
      <aside class="agent-sidebar">
        <div class="agent-sidebar-head">
          <a
            class="agent-sidebar-logo"
            href="https://www.mycom-net.com/"
            target="_blank"
            rel="noopener noreferrer"
            aria-label="MYCOM Webseite"
            title="MYCOM Webseite öffnen"
          >
            <img
              class="agent-sidebar-logo-image"
              src="https://www.mycom-net.com/wp-content/uploads/MyCom_Logo.svg"
              alt="MYCOM"
              loading="lazy"
              decoding="async"
            />
          </a>
          <div>
            <div class="agent-sidebar-title">Integration Agent</div>
            <div class="agent-sidebar-subtitle">Control Center</div>
          </div>
        </div>
        <ul class="nav flex-column" id="main-tabs" role="tablist">
          <li class="nav-item" role="presentation"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-overview" type="button"><span class="agent-tab-icon">▦</span>Übersicht</button></li>
          <li class="nav-item d-none" role="presentation"><button id="tab-installer-trigger" class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-installer" type="button"><span class="agent-tab-icon">⚙</span>Installation</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-schedulers" type="button"><span class="agent-tab-icon">◷</span>Scheduler</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-connectors" type="button"><span class="agent-tab-icon">◫</span>Connectoren</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-monitor" type="button"><span class="agent-tab-icon">◉</span>Monitoring</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-migration" type="button"><span class="agent-tab-icon">⊞</span>Migration</button></li>
          <li class="nav-item" role="presentation"><button id="open-admin-modal-sidebar" class="nav-link" type="button"><span class="agent-tab-icon">☷</span>Admin</button></li>
        </ul>
      </aside>

      <div class="agent-main">
        <nav class="agent-topbar">
          <div class="agent-topbar-primary">
            <button
              class="btn btn-outline-secondary agent-topbar-menu"
              type="button"
              data-bs-toggle="offcanvas"
              data-bs-target="#agent-header-menu"
              aria-controls="agent-header-menu"
              aria-label="Header-Menü öffnen"
            >
              ☰
              <span id="agent-menu-update-bullet" class="agent-update-bullet d-none" aria-hidden="true"></span>
            </button>
            <div class="agent-topbar-brand">SF Integration Agent <span id="agent-version-label" class="agent-version-label">v-</span></div>
          </div>
          <div class="agent-navbar-actions offcanvas offcanvas-end" tabindex="-1" id="agent-header-menu" aria-labelledby="agent-header-menu-title">
            <div class="offcanvas-header agent-offcanvas-header">
              <div class="agent-offcanvas-title-wrap">
                <div id="agent-header-menu-title" class="fw-semibold">Menü</div>
                <div id="agent-header-menu-subtitle" class="agent-offcanvas-subtitle">Navigation und Arbeitsbereich</div>
              </div>
              <button type="button" class="btn-close" data-bs-dismiss="offcanvas" aria-label="Schließen"></button>
            </div>
            <div class="offcanvas-body agent-navbar-actions-body">
              <section class="agent-menu-panel agent-menu-panel-primary">
                <div class="agent-menu-section-label">Navigation</div>
                <div class="agent-menu-nav-grid" role="navigation" aria-label="Hauptnavigation">
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-overview" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">▦</span><span>Übersicht</span></button>
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-installer" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">⚙</span><span>Installation</span><span id="agent-install-update-bullet" class="agent-update-bullet agent-update-bullet-inline d-none" aria-hidden="true"></span></button>
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-schedulers" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">◷</span><span>Scheduler</span></button>
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-connectors" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">◫</span><span>Connectoren</span></button>
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-monitor" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">◉</span><span>Monitoring</span></button>
                  <button type="button" class="agent-menu-tab" data-menu-tab="#tab-migration" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">⊞</span><span>Migration</span></button>
                  <button type="button" id="open-admin-modal-menu" class="agent-menu-tab" data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">☷</span><span>Admin</span></button>
                </div>
              </section>

              <section class="agent-menu-panel">
                <div class="agent-menu-section-label">Arbeitsbereich</div>
                <div class="agent-menu-control-grid">
                  <div class="agent-menu-control-card">
                    <label class="small text-secondary" for="instance-select">Instanz</label>
                    <select id="instance-select" class="form-select form-select-sm"></select>
                  </div>
                  <div class="agent-menu-control-card">
                    <label class="small text-secondary" for="theme-select">Theme</label>
                    <select id="theme-select" class="form-select form-select-sm">
                      <option value="corporate">Corporate Light</option>
                      <option value="industrial">Industrial Blue</option>
                      <option value="midnight">Midnight Dark</option>
                    </select>
                  </div>
                </div>
              </section>

              <section class="agent-menu-panel">
                <div id="agent-menu-actions-label" class="agent-menu-section-label">Aktionen</div>
                <div id="agent-menu-auth-hint" class="agent-menu-inline-note d-none"></div>
                <div class="agent-menu-status-card">
                  <div class="agent-menu-status-head">
                    <span class="small text-secondary">Updates</span>
                    <div id="overview-update-status" class="small text-secondary">Update-Status wird geladen...</div>
                  </div>
                  <div id="overview-update-progress-wrap" class="mt-2 d-none">
                    <div class="d-flex justify-content-between gap-2 small text-secondary">
                      <span id="overview-update-progress-stage">Update wird vorbereitet...</span>
                      <span id="overview-update-progress-percent">0%</span>
                    </div>
                    <div class="progress mt-1" style="height: 6px;">
                      <div id="overview-update-progress-bar" class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: 0%"></div>
                    </div>
                    <div id="overview-update-progress-updated-at" class="small text-secondary mt-1"></div>
                  </div>
                  <div id="overview-log-retention-status" class="small text-secondary mt-2">Log-Retention wird geladen...</div>
                  <div class="agent-menu-action-grid">
                    <button id="overview-check-update" type="button" class="btn btn-sm btn-outline-secondary">Update prüfen</button>
                    <button id="overview-run-update" type="button" class="btn btn-sm btn-outline-primary">Update starten</button>
                  </div>
                </div>
                <div class="agent-menu-action-grid agent-menu-action-grid-compact">
                  <button id="export-setup" class="btn btn-outline-secondary agent-btn-subtle" aria-label="Setup exportieren"><span class="agent-btn-icon" aria-hidden="true">⭳</span><span>Export</span></button>
                  <button id="import-setup" class="btn btn-outline-secondary agent-btn-subtle" aria-label="Setup importieren"><span class="agent-btn-icon" aria-hidden="true">⭱</span><span>Import</span></button>
                </div>
                <input id="setup-import-input" type="file" accept="application/json" class="d-none" />
                <div class="agent-menu-action-grid agent-menu-action-grid-compact">
                  <button id="add-instance" class="btn btn-outline-secondary agent-btn-subtle" aria-label="Instanz hinzufügen"><span class="agent-btn-icon" aria-hidden="true">＋</span><span>Instanz</span></button>
                  <button id="refresh-all" class="btn btn-outline-secondary agent-btn-subtle" aria-label="Aktualisieren"><span class="agent-btn-icon" aria-hidden="true">↻</span><span>Refresh</span></button>
                </div>
              </section>

              <section id="agent-menu-auth-panel" class="agent-menu-panel agent-menu-panel-footer">
                <button id="logout-admin" class="btn btn-outline-danger btn-sm agent-btn-subtle w-100" aria-label="Abmelden"><span class="agent-btn-icon" aria-hidden="true">⇥</span><span>Logout</span></button>
              </section>
            </div>
          </div>
        </nav>

        <main class="container-fluid px-4 py-4 agent-content">
      <div id="global-alert" class="alert alert-danger d-none" role="alert"></div>

      <div class="tab-content">
        <section class="tab-pane fade show active" id="tab-overview" role="tabpanel">
          <div class="row g-3 mb-3">
              <div class="col-md-3"><div class="card soft-card mini-kpi mini-kpi-service h-100"><div class="card-body"><div class="text-secondary small">Service</div><h5 id="kpi-service" class="mb-0">-</h5><div class="kpi-meter"><div id="kpi-service-cpu-bar" class="kpi-meter-fill" style="width:0%"></div></div><div class="kpi-service-footer"><div id="kpi-service-cpu-text" class="kpi-inline-metric">CPU Last: -</div><div class="kpi-sparkline-wrap" aria-hidden="true"><svg id="kpi-service-cpu-sparkline" class="kpi-sparkline" viewBox="0 0 120 20" preserveAspectRatio="xMidYMid meet"><path id="kpi-service-cpu-sparkline-path" class="kpi-sparkline-path" d=""></path><circle id="kpi-service-cpu-sparkline-dot" class="kpi-sparkline-dot" r="2" cx="0" cy="0"></circle></svg></div></div><div class="kpi-service-meta"><div id="kpi-service-os" class="kpi-inline-metric">OS: -</div><div id="kpi-service-memory" class="kpi-inline-metric">RAM: -</div><div id="kpi-service-disk" class="kpi-inline-metric">Disk: -</div></div><div id="kpi-service-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Scheduler</div><h5 id="kpi-scheduler" class="mb-0">-</h5><div id="kpi-scheduler-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Aktive Scheduler</div><h5 id="kpi-schedules" class="mb-0">0</h5><div id="kpi-schedules-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Connectoren</div><h5 id="kpi-connectors" class="mb-0">0</h5><div id="kpi-connectors-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
          </div>
          <div class="row g-3 mb-3">
            <div class="col-lg-3 col-md-6">
              <div class="card soft-card stats-card h-100">
                <div class="card-header bg-white fw-semibold">Run-Qualität</div>
                <div class="card-body">
                  <div class="stats-row">
                    <span class="stats-label">Erfolgsquote</span>
                    <span class="stats-value" id="kpi-success-rate">0%</span>
                  </div>
                  <div class="progress stats-progress mb-3" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-label="Erfolgsquote">
                    <div id="kpi-success-rate-bar" class="progress-bar bg-success" style="width:0%"></div>
                  </div>
                  <div class="stats-row">
                    <span class="stats-label">Fehlerquote</span>
                    <span class="stats-value text-danger" id="kpi-error-rate">0%</span>
                  </div>
                  <div class="progress stats-progress mb-0" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-label="Fehlerquote">
                    <div id="kpi-error-rate-bar" class="progress-bar bg-danger" style="width:0%"></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="col-lg-3 col-md-6">
              <div class="card soft-card stats-card h-100">
                <div class="card-header bg-white fw-semibold">Run-Status</div>
                <div class="card-body">
                  <div class="stats-grid-two">
                    <div><div class="stats-chip stats-chip-success">Erfolg</div><div id="kpi-runs-success" class="stats-big-number">0</div></div>
                    <div><div class="stats-chip stats-chip-danger">Fehler</div><div id="kpi-runs-failed" class="stats-big-number">0</div></div>
                    <div><div class="stats-chip stats-chip-info">Laufend</div><div id="kpi-runs-running" class="stats-big-number">0</div></div>
                    <div><div class="stats-chip stats-chip-muted">Gesamt</div><div id="kpi-runs-total" class="stats-big-number">0</div></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="col-lg-3 col-md-6">
              <div class="card soft-card stats-card h-100">
                <div class="card-header bg-white fw-semibold">Scheduler-Statistik</div>
                <div class="card-body">
                  <div class="stats-row"><span class="stats-label">Inbound</span><span id="kpi-inbound-count" class="stats-value">0</span></div>
                  <div class="stats-row"><span class="stats-label">Outbound</span><span id="kpi-outbound-count" class="stats-value">0</span></div>
                  <div class="stats-row"><span class="stats-label">Durchschnitt Laufzeit</span><span id="kpi-average-run-duration" class="stats-value">-</span></div>
                  <div class="stats-row"><span class="stats-label">Auto-Deaktiviert</span><span id="kpi-auto-disabled-count" class="stats-value text-warning">0</span></div>
                  <div class="stats-row mb-0"><span class="stats-label">Letzter Run</span><span id="kpi-last-run-at" class="stats-value">-</span></div>
                </div>
              </div>
            </div>
            <div class="col-lg-3 col-md-6">
              <div class="card soft-card stats-card h-100">
                <div class="card-header bg-white fw-semibold">SQLite-Staging</div>
                <div class="card-body">
                  <div class="stats-row"><span class="stats-label">SQLite-Objekte</span><span id="kpi-sqlite-objects" class="stats-value">0</span></div>
                  <div class="stats-row"><span class="stats-label">SQLite offen</span><span id="kpi-sqlite-pending" class="stats-value">0</span></div>
                  <div class="stats-row"><span class="stats-label">SQLite OK</span><span id="kpi-sqlite-success" class="stats-value text-success">0</span></div>
                  <div class="stats-row mb-0"><span class="stats-label">SQLite Fehler</span><span id="kpi-sqlite-errors" class="stats-value text-danger">0</span></div>
                </div>
              </div>
            </div>
          </div>
          <div class="row g-3 mb-3">
            <div class="col-lg-6">
              <div class="card soft-card h-100">
                <div class="card-header bg-white d-flex justify-content-between align-items-center">
                  <span class="fw-semibold">Fehler je Connector</span>
                  <select id="log-chart-range" class="form-select form-select-sm" style="max-width: 220px;">
                    <option value="last_hour">Letzte Stunde</option>
                    <option value="last_24h" selected>Letzte 24h</option>
                    <option value="last_30d">Letzte 30 Tage</option>
                  </select>
                </div>
                <div class="card-body">
                  <div class="logs-chart-wrap logs-chart-wrap-compact">
                    <canvas id="logs-chart"></canvas>
                  </div>
                </div>
              </div>
            </div>
            <div class="col-lg-6">
              <div class="card soft-card h-100">
                <div class="card-header bg-white d-flex justify-content-between align-items-center gap-2 flex-wrap">
                  <span class="fw-semibold">Datensätze Verlauf</span>
                  <div id="overview-stats-range" class="btn-group btn-group-sm overview-stats-range" role="group" aria-label="Dashboard Zeitraum">
                    <button type="button" class="btn btn-outline-secondary" data-range="day">Heute</button>
                    <button type="button" class="btn btn-outline-secondary active" data-range="month">Monat</button>
                    <button type="button" class="btn btn-outline-secondary" data-range="year">Jahr</button>
                  </div>
                </div>
                <div class="card-body">
                  <div class="records-chart-wrap">
                    <canvas id="records-chart"></canvas>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="row g-3">
            <div class="col-lg-7">
              <div class="card soft-card">
                <div class="card-header bg-white d-flex justify-content-between align-items-center">
                  <span class="fw-semibold">Verknüpfungsübersicht</span>
                  <div class="d-flex align-items-center gap-2">
                    <span id="overview-visible-schedule-count" class="badge bg-secondary">0 Scheduler sichtbar</span>
                    <select id="overview-connector-filter" class="form-select form-select-sm" style="max-width: 280px;">
                      <option value="">Alle Connectoren</option>
                    </select>
                  </div>
                </div>
                <div class="card-body">
                  <div class="graph-wrap"><svg id="graph" width="920" height="360"></svg></div>
                  <div class="small text-secondary mt-2">Klick auf einen Knoten öffnet die passende Konfiguration im Modal. CSV/XLSX-Dateien koennen auf Datei-Connectoren gezogen werden, um automatisch einen Datei-Scheduler anzulegen.</div>
                </div>
              </div>
            </div>
            <div class="col-lg-5">
              <div class="card soft-card mb-3">
                <div class="card-header bg-white fw-semibold">Salesforce Org + Limits</div>
                <div class="card-body">
                  <div class="stats-row"><span class="stats-label">Domain</span><span id="sf-domain" class="stats-value">-</span></div>
                  <div class="stats-row mb-3"><span class="stats-label">Umgebung</span><span id="sf-environment" class="stats-value">-</span></div>
                  <div class="limits-gauge-grid">
                    <div class="limit-gauge-card">
                      <div id="sf-api-gauge" class="limit-gauge" style="--gauge-value:0; --gauge-color:#2f69a8;">
                        <div class="limit-gauge-inner"><span id="sf-api-gauge-value" class="limit-gauge-value">0%</span></div>
                      </div>
                      <div class="limit-gauge-label">API Calls</div>
                      <div id="sf-api-usage" class="limit-gauge-detail">-</div>
                    </div>
                    <div class="limit-gauge-card">
                      <div id="sf-data-gauge" class="limit-gauge" style="--gauge-value:0; --gauge-color:#1f7d57;">
                        <div class="limit-gauge-inner"><span id="sf-data-gauge-value" class="limit-gauge-value">0%</span></div>
                      </div>
                      <div class="limit-gauge-label">Datenspeicher</div>
                      <div id="sf-data-storage" class="limit-gauge-detail">-</div>
                    </div>
                    <div class="limit-gauge-card">
                      <div id="sf-file-gauge" class="limit-gauge" style="--gauge-value:0; --gauge-color:#7b5ea7;">
                        <div class="limit-gauge-inner"><span id="sf-file-gauge-value" class="limit-gauge-value">0%</span></div>
                      </div>
                      <div class="limit-gauge-label">Dateispeicher</div>
                      <div id="sf-file-storage" class="limit-gauge-detail">-</div>
                    </div>
                    <div class="limit-gauge-card">
                      <div id="sf-license-gauge" class="limit-gauge" style="--gauge-value:0; --gauge-color:#c26a2d;">
                        <div class="limit-gauge-inner"><span id="sf-license-gauge-value" class="limit-gauge-value">0%</span></div>
                      </div>
                      <div class="limit-gauge-label">Lizenzen</div>
                      <div id="sf-licenses" class="limit-gauge-detail">-</div>
                    </div>
                  </div>
                </div>
              </div>
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">Letzte Runs</div>
                <div class="card-body p-0">
                  <table id="overview-runs-table" class="table table-sm mb-0">
                    <thead><tr><th>Schedule</th><th>Status</th><th>Dauer</th><th>Start</th></tr></thead>
                    <tbody id="overview-runs-body"></tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section class="tab-pane fade" id="tab-installer" role="tabpanel">
          <div class="row g-3">
            <div class="col-lg-4">
              <div class="card soft-card h-100">
                <div class="card-header bg-white fw-semibold">Installationsstatus</div>
                <div class="card-body">
                  <div id="installer-status-summary" class="small text-secondary mb-3">Installer-Status wird geladen...</div>
                  <div class="mb-3">
                    <label class="form-label">Setup-Szenario</label>
                    <select id="installer-scenario" class="form-select"></select>
                    <div id="installer-scenario-description" class="form-text">Szenario wird geladen...</div>
                  </div>
                  <div id="installer-checks" class="d-grid gap-2"></div>
                </div>
              </div>
            </div>
            <div class="col-lg-8">
              <div class="card soft-card mb-3">
                <div class="card-header bg-white fw-semibold">Installationsdateien erzeugen</div>
                <div class="card-body">
                  <div class="row g-2 mb-3">
                    <div class="col-md-6"><label class="form-label">App Dir</label><input id="installer-app-dir" class="form-control" value="/opt/sf-integration-agent" /></div>
                    <div class="col-md-3"><label class="form-label">Service User</label><input id="installer-service-user" class="form-control" value="sfagent" /></div>
                    <div class="col-md-3"><label class="form-label">Service Group</label><input id="installer-service-group" class="form-control" value="sfagent" /></div>
                    <div class="col-md-6"><label class="form-label">Public Host</label><input id="installer-public-host" class="form-control" value="agent.example.com" /></div>
                    <div class="col-md-3"><label class="form-label">Port</label><input id="installer-port" type="number" class="form-control" value="9010" /></div>
                    <div class="col-md-3"><label class="form-label">Admin Username</label><input id="installer-admin-username" class="form-control" value="admin" /></div>
                  </div>
                  <div class="d-flex gap-2 align-items-center flex-wrap">
                    <button id="installer-generate-files" class="btn btn-primary">Dateien erzeugen</button>
                    <a id="installer-download-archive" class="btn btn-outline-secondary d-none" href="#" download>ZIP laden</a>
                    <div id="installer-generate-status" class="small text-secondary">Noch keine Dateien erzeugt.</div>
                  </div>
                  <pre id="installer-generated-files" class="bg-dark text-light p-3 rounded small mt-3 mb-0">Noch keine Dateien erzeugt.</pre>
                </div>
              </div>
              <div class="card soft-card mb-3">
                <div class="card-header bg-white fw-semibold">Linux Zielpfade</div>
                <div class="card-body">
                  <div id="installer-paths" class="row g-2"></div>
                </div>
              </div>
              <div class="card soft-card mb-3">
                <div class="card-header bg-white fw-semibold">Empfohlene Befehle</div>
                <div class="card-body">
                  <ol id="installer-commands" class="mb-0 small"></ol>
                </div>
              </div>
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">Environment Vorlage</div>
                <div class="card-body">
                  <pre id="installer-env-template" class="bg-dark text-light p-3 rounded small mb-0">Vorlage wird geladen...</pre>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section class="tab-pane fade" id="tab-schedulers" role="tabpanel">
          <div class="card soft-card">
            <div class="card-header bg-white d-flex justify-content-between align-items-center">
              <span class="fw-semibold">Scheduler-Verwaltung</span>
              <div class="d-flex gap-2">
                <button id="new-schedule-from-template" class="btn btn-sm btn-outline-primary">Neu von Vorlage</button>
                <button id="new-schedule" class="btn btn-sm btn-primary">Neuer Scheduler</button>
              </div>
            </div>
            <div class="card-body p-0">
              <div class="table-responsive">
                <div class="px-2 pt-2">
                  <div class="scheduler-filter-badges" id="schedulers-direction-tabs" role="tablist" aria-label="Richtungsfilter">
                    <button class="scheduler-filter-badge is-active" type="button" data-direction-tab="all">Alle</button>
                    <button class="scheduler-filter-badge" type="button" data-direction-tab="inbound">Inbound</button>
                    <button class="scheduler-filter-badge" type="button" data-direction-tab="outbound">Outbound</button>
                  </div>
                </div>
                <div class="d-flex flex-column flex-lg-row gap-2 p-2">
                  <input type="search" class="form-control form-control-sm" placeholder="Suche Scheduler..." id="schedulers-filter" />
                  <select id="schedulers-active-filter" class="form-select form-select-sm" style="max-width: 220px;">
                    <option value="all">Alle Stati</option>
                    <option value="active">Nur aktive</option>
                    <option value="inactive">Nur inaktive</option>
                  </select>
                  <select id="schedulers-connector-filter" class="form-select form-select-sm" style="max-width: 260px;">
                    <option value="">Alle Connectoren</option>
                  </select>
                </div>
                <div id="schedulers-auto-disabled-warning" class="alert alert-warning mx-2 mb-2 py-2 d-none" role="alert"></div>
                <table class="table table-hover align-middle mb-0" id="schedulers-table">
                  <thead><tr><th data-sortable="true">Scheduler</th><th>Aktiv</th><th>Laufstatus</th><th>Connector</th><th>Timing</th><th>Aktion</th></tr></thead>
                  <tbody id="schedules-body"></tbody>
                </table>
              </div>
            </div>
          </div>
        </section>

        <section class="tab-pane fade" id="tab-connectors" role="tabpanel">
          <div class="card soft-card">
            <div class="card-header bg-white d-flex justify-content-between align-items-center">
              <span class="fw-semibold">Connector-Verwaltung</span>
              <div class="d-flex gap-2">
                <button id="new-connector-from-template" class="btn btn-sm btn-outline-primary">Neu von Vorlage</button>
                <button id="new-connector" class="btn btn-sm btn-primary">Neuer Connector</button>
              </div>
            </div>
            <div class="card-body">
              <div class="d-flex flex-column flex-lg-row gap-2 align-items-lg-center mb-3">
                <input type="search" class="form-control form-control-sm" placeholder="Suche Connectoren..." id="connectors-filter" />
                <div id="connectors-summary" class="small text-secondary text-lg-end">Connectoren werden geladen...</div>
              </div>
              <div id="connectors-panels" class="row g-3"></div>
            </div>
          </div>
        </section>

        <section class="tab-pane fade" id="tab-monitor" role="tabpanel">
          <div class="row g-3">
            <div class="col-lg-6">
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">Runs</div>
                <div class="card-body p-0">
                  <table class="table table-sm mb-0">
                    <thead><tr><th>Schedule</th><th>Status</th><th>Datum / Zeit</th><th>Dauer</th><th>Ergebnis</th><th>Logs</th><th>Aktion</th></tr></thead>
                    <tbody id="runs-body"></tbody>
                  </table>
                </div>
              </div>
              <div class="card soft-card mt-3">
                <div class="card-header bg-white d-flex justify-content-between align-items-center gap-2 flex-wrap">
                  <span class="fw-semibold">Stale Runs</span>
                  <div class="d-flex gap-2">
                    <button id="refresh-stale-runs" class="btn btn-sm btn-outline-secondary">Aktualisieren</button>
                    <button id="release-all-stale-runs" class="btn btn-sm btn-outline-danger">Alle freigeben</button>
                  </div>
                </div>
                <div class="card-body p-0">
                  <table class="table table-sm mb-0">
                    <thead><tr><th>Schedule</th><th>Start</th><th>Alter</th><th>Aktion</th></tr></thead>
                    <tbody id="stale-runs-body"></tbody>
                  </table>
                </div>
              </div>
            </div>
            <div class="col-lg-6">
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">Run-Logs</div>
                <div class="card-body">
                  <div class="input-group mb-2">
                    <select id="log-run-select" class="form-select"></select>
                    <button id="load-logs" class="btn btn-outline-primary">Laden</button>
                  </div>
                  <pre id="logs-output" class="bg-dark text-light p-3 rounded small mb-0" style="white-space: pre-wrap; word-break: break-word; overflow-wrap: anywhere; overflow-x: hidden;">Noch keine Logs geladen.</pre>
                </div>
              </div>
            </div>
          </div>
        </section>
      </div>

        <!-- Migration Tab -->
        <section class="tab-pane fade" id="tab-migration" role="tabpanel">
          <div class="card soft-card mb-3">
            <div class="card-header bg-white d-flex justify-content-between align-items-center">
              <div>
                <div class="migration-card-title">Migrationen mit eigenem Login</div>
                <div class="migration-card-subtitle">Jede Migration traegt ihre Salesforce-Freigabe ueber die Login-Seite inklusive Status, letzter Ausfuehrung und Fehlerbild selbst.</div>
              </div>
              <div class="d-flex align-items-center gap-2 flex-wrap migration-header-actions">
                <button id="new-migration-instance" class="btn btn-sm btn-outline-primary">Neue Migration</button>
              </div>
            </div>
            <div class="card-body">
              <div id="migration-instance-summary" class="small text-secondary mb-3">Migrations-Instanzen werden geladen...</div>
              <div id="migration-instance-panels" class="row g-3"></div>
            </div>
          </div>
          <div class="card soft-card mb-3">
            <div class="card-header bg-white d-flex justify-content-between align-items-center">
              <div>
                <div class="migration-card-title">Daten-Migration</div>
                <div class="migration-card-subtitle">Dateien direkt auf die Tabelle ziehen oder oben auswählen.</div>
              </div>
              <div class="d-flex align-items-center gap-2 flex-wrap migration-header-actions">
                <button id="migration-dropzone-pick" type="button" class="btn btn-sm btn-outline-primary">Datei auswählen</button>
                <button id="new-migration" class="btn btn-sm btn-primary">+ Neue Migration</button>
                <input id="migration-dropzone-input" type="file" class="d-none" accept=".csv,.txt,.json,.xlsx,.xls" multiple />
              </div>
            </div>
            <div class="card-body p-0">
              <div id="migration-dropzone" class="migration-list-drop-target">
                <div class="migration-drop-target-hint">Dateien hier auf die Liste ziehen, um direkt einen Entwurf zu starten. Unterstützt CSV, TXT, Excel und JSON.</div>
                <div class="table-responsive">
                  <table class="table table-sm mb-0" id="migration-list-table">
                    <thead><tr><th>Name</th><th>Status</th><th>Objekte</th><th>Letzter Lauf</th><th>Aktionen</th></tr></thead>
                    <tbody id="migration-list-body"><tr><td colspan="5" class="text-secondary">Keine Migrationen vorhanden.</td></tr></tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </section>
        </main>
      </div>
    </div>

    <div class="modal fade" id="admin-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Admin</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Schliessen"></button>
          </div>
          <div class="modal-body">
            <div class="row g-3">
              <div class="col-lg-5">
                <div class="card soft-card h-100">
                  <div class="card-header bg-white fw-semibold">Benutzerverwaltung</div>
                  <div class="card-body">
                    <input id="admin-user-id" type="hidden" />
                    <div class="mb-2">
                      <label class="form-label">Benutzername</label>
                      <input id="admin-user-username" class="form-control" autocomplete="off" />
                    </div>
                    <div class="mb-2">
                      <label class="form-label">Anzeigename</label>
                      <input id="admin-user-display-name" class="form-control" autocomplete="off" />
                    </div>
                    <div class="mb-3">
                      <label class="form-label">Passwort</label>
                      <input id="admin-user-password" type="password" class="form-control" autocomplete="new-password" placeholder="Leer lassen, um beizubehalten" />
                    </div>
                    <div class="mb-3">
                      <div class="form-label">Berechtigungen</div>
                      <div class="d-flex flex-wrap gap-3 small">
                        <label><input class="form-check-input me-1" type="checkbox" data-admin-permission="read" />Lesen</label>
                        <label><input class="form-check-input me-1" type="checkbox" data-admin-permission="write" />Schreiben</label>
                        <label><input class="form-check-input me-1" type="checkbox" data-admin-permission="delete" />Loeschen</label>
                        <label><input class="form-check-input me-1" type="checkbox" data-admin-permission="admin" />Admin</label>
                      </div>
                    </div>
                    <div class="mb-3">
                      <div class="form-label">Module</div>
                      <label class="small"><input class="form-check-input me-1" type="checkbox" data-admin-module="migration" />Migrationsmodul</label>
                    </div>
                    <div class="d-flex gap-2">
                      <button id="admin-user-save" class="btn btn-primary btn-sm" type="button">Benutzer speichern</button>
                      <button id="admin-user-reset" class="btn btn-outline-secondary btn-sm" type="button">Neu</button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="col-lg-7">
                <div class="card soft-card mb-3">
                  <div class="card-header bg-white d-flex justify-content-between align-items-center">
                    <span class="fw-semibold">Benutzer</span>
                    <button id="admin-users-refresh" class="btn btn-sm btn-outline-secondary" type="button">Aktualisieren</button>
                  </div>
                  <div class="card-body p-0">
                    <div class="table-responsive">
                      <table class="table table-sm mb-0">
                        <thead><tr><th>Benutzer</th><th>Rechte</th><th>Module</th><th>Aktionen</th></tr></thead>
                        <tbody id="admin-users-body"></tbody>
                      </table>
                    </div>
                  </div>
                </div>
                <div class="card soft-card">
                  <div class="card-header bg-white d-flex justify-content-between align-items-center">
                    <span class="fw-semibold">Aenderungshistorie</span>
                    <button id="admin-audit-refresh" class="btn btn-sm btn-outline-secondary" type="button">Aktualisieren</button>
                  </div>
                  <div class="card-body p-0">
                    <div class="table-responsive">
                      <table class="table table-sm mb-0">
                        <thead><tr><th>Zeit</th><th>Benutzer</th><th>Aktion</th><th>Objekt</th><th>Status</th></tr></thead>
                        <tbody id="admin-audit-body"></tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Migration Wizard Modal -->
    <div class="modal fade" id="migration-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title" id="migration-modal-title">Migrations-Assistent</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div id="mig-modal-error" class="alert alert-danger d-none py-2" role="alert"></div>
            <!-- Wizard Steps Indicator -->
            <div class="migration-wizard-steps-line mb-4" id="mig-wizard-steps" style="--wizard-step-count: 7; --wizard-step-count-mobile: 3;">
              <div class="migration-wizard-step is-active" data-mig-step="1"><span class="migration-wizard-step-index">1</span><span class="migration-wizard-step-label">Objekte</span></div>
              <div class="migration-wizard-step" data-mig-step="2"><span class="migration-wizard-step-index">2</span><span class="migration-wizard-step-label">Dateien</span></div>
              <div class="migration-wizard-step" data-mig-step="3"><span class="migration-wizard-step-index">3</span><span class="migration-wizard-step-label">Mapping</span></div>
              <div class="migration-wizard-step" data-mig-step="4"><span class="migration-wizard-step-index">4</span><span class="migration-wizard-step-label">Abhängigkeiten</span></div>
              <div class="migration-wizard-step" data-mig-step="5"><span class="migration-wizard-step-index">5</span><span class="migration-wizard-step-label">Reihenfolge</span></div>
              <div class="migration-wizard-step" data-mig-step="6"><span class="migration-wizard-step-index">6</span><span class="migration-wizard-step-label">Felder anlegen</span></div>
              <div class="migration-wizard-step" data-mig-step="7"><span class="migration-wizard-step-index">7</span><span class="migration-wizard-step-label">Ausführen</span></div>
            </div>

            <!-- Step 1: Name + Objekte -->
            <div class="mig-wizard-panel" data-mig-step-panel="1">
              <h6 class="fw-semibold mb-3">Schritt 1: Migrationsname &amp; betroffene Salesforce-Objekte</h6>
              <div class="row g-3 mb-3">
                <div class="col-md-6">
                  <label class="form-label">Migrationsname <span class="text-danger">*</span></label>
                  <input type="text" id="mig-name" class="form-control" placeholder="z. B. Kundenmigration 2026" />
                </div>
                <div class="col-md-6">
                  <label class="form-label">Beschreibung</label>
                  <input type="text" id="mig-description" class="form-control" placeholder="Optional" />
                </div>
                <div class="col-md-4">
                  <label class="form-label">Batch Size</label>
                  <input type="number" id="mig-batch-size" class="form-control" value="200" min="1" max="200" />
                  <div class="form-text">Anzahl Datensätze pro Salesforce-Schreibvorgang.</div>
                </div>
                <div class="col-md-4">
                  <label class="form-label">Salesforce Quelle</label>
                  <select id="mig-instance-source" class="form-select">
                    <option value="embedded">Eigenen Login in Migration speichern</option>
                    <option value="existing">Bestehende Instanz verwenden</option>
                  </select>
                </div>
                <div class="col-md-8 d-none" id="mig-existing-instance-wrap">
                  <label class="form-label">Bestehende Instanz</label>
                  <select id="mig-existing-instance" class="form-select"></select>
                  <div class="form-text">Verwendet eine bereits im Agenten konfigurierte Salesforce-Instanz.</div>
                </div>
                <div class="col-md-4" id="mig-login-environment-wrap">
                  <label class="form-label">Umgebung</label>
                  <select id="mig-login-environment" class="form-select">
                    <option value="sandbox">Sandbox</option>
                    <option value="production">Produktion</option>
                  </select>
                </div>
                <div class="col-md-4" id="mig-login-auth-type-wrap">
                  <label class="form-label">Login-Modus</label>
                  <select id="mig-login-auth-type" class="form-select">
                    <option value="password">Benutzername / Passwort</option>
                    <option value="client_credentials">Client ID + Client Secret</option>
                    <option value="oauth_refresh_token">Salesforce Login mit Allow</option>
                  </select>
                </div>
                <div class="col-md-8" id="mig-login-url-wrap">
                  <label class="form-label">Login-URL</label>
                  <input type="text" id="mig-login-url" class="form-control" placeholder="https://test.salesforce.com" />
                  <div class="form-text">Standard ist die zur Umgebung passende Salesforce-Login-Seite. Bei Bedarf kannst du hier eine andere Salesforce-Domain eintragen.</div>
                </div>
                <div class="col-md-6" id="mig-login-username-wrap">
                  <label class="form-label">Salesforce Benutzername</label>
                  <input type="text" id="mig-login-username" class="form-control" placeholder="user@example.com" />
                </div>
                <div class="col-md-6" id="mig-login-password-wrap">
                  <label class="form-label">Passwort</label>
                  <input type="password" id="mig-login-password" class="form-control" />
                </div>
                <div class="col-md-6" id="mig-login-security-token-wrap">
                  <label class="form-label">Security Token (optional)</label>
                  <input type="password" id="mig-login-security-token" class="form-control" />
                </div>
                <div class="col-md-6 d-none" id="mig-login-client-id-wrap">
                  <label class="form-label">Client ID</label>
                  <input type="text" id="mig-login-client-id" class="form-control" placeholder="3MVG9..." />
                </div>
                <div class="col-md-6 d-none" id="mig-login-client-secret-wrap">
                  <label class="form-label">Client Secret</label>
                  <input type="password" id="mig-login-client-secret" class="form-control" />
                </div>
                <div class="col-md-8" id="mig-login-status-wrap">
                  <label class="form-label">Salesforce Verbindung</label>
                  <div id="mig-login-status" class="small text-secondary border rounded-3 px-3 py-2 bg-light-subtle">Noch keine Salesforce-Freigabe vorhanden.</div>
                </div>
                <div class="col-md-4 d-flex align-items-end" id="mig-login-authorize-wrap">
                  <button type="button" class="btn btn-outline-primary w-100" id="mig-login-authorize">Mit Salesforce verbinden</button>
                </div>
              </div>
              <div id="mig-pending-import-hint" class="alert alert-info py-2 small d-none"></div>
              <div id="mig-import-suggestions" class="mb-3 d-none"></div>
              <div class="d-flex justify-content-between align-items-center mb-2">
                <label class="form-label mb-0">Salesforce-Objekte</label>
                <button type="button" class="btn btn-sm btn-outline-primary" id="mig-load-sf-objects">SF-Objekte laden</button>
              </div>
              <div id="mig-sf-objects-search-wrap" class="mb-2 d-none">
                <input type="search" id="mig-sf-objects-search" class="form-control form-control-sm" placeholder="Objekt suchen (z.B. Account, Contact, Custom__c)..." />
              </div>
              <div id="mig-sf-objects-list" class="mb-3" style="max-height:220px;overflow-y:auto;border:1px solid var(--bs-border-color);border-radius:6px;padding:8px;">
                <div class="text-secondary small">Klicke „SF-Objekte laden" oder gib Objekte manuell ein.</div>
              </div>
              <div class="mb-3">
                <label class="form-label">Objekt manuell hinzufügen</label>
                <div class="input-group">
                  <input type="text" id="mig-manual-object" class="form-control" placeholder="Account" />
                  <button type="button" class="btn btn-outline-secondary" id="mig-add-manual-object">Hinzufügen</button>
                </div>
              </div>
              <div>
                <label class="form-label">Ausgewählte Objekte</label>
                <div id="mig-selected-objects" class="d-flex flex-wrap gap-2">
                  <span class="text-secondary small">Noch keine Objekte ausgewählt.</span>
                </div>
              </div>
              <details class="border rounded p-2 bg-light mt-3">
                <summary class="fw-semibold">Änderungshistorie</summary>
                <div class="d-flex justify-content-between align-items-center gap-2 my-2">
                  <div id="mig-history-meta" class="small text-secondary">Noch nicht geladen.</div>
                  <button id="mig-refresh-history" type="button" class="btn btn-outline-secondary btn-sm">Historie aktualisieren</button>
                </div>
                <div id="mig-history-list" class="small text-secondary">Migration noch nicht gespeichert.</div>
              </details>
            </div>

            <!-- Step 2: Dateien zuordnen -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="2">
              <h6 class="fw-semibold mb-3">Schritt 2: Quelldateien den Objekten zuordnen</h6>
              <div id="mig-file-import-hint" class="alert alert-light border py-2 small d-none"></div>
              <div id="mig-file-assignment-list">
                <div class="text-secondary small">Bitte zuerst Objekte in Schritt 1 auswählen.</div>
              </div>
            </div>

            <!-- Step 3: Feldmapping -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="3">
              <h6 class="fw-semibold mb-3">Schritt 3: Feldzuordnung (Datei-Spalte → Salesforce-Feld)</h6>
              <div class="mb-2">
                <label class="form-label">Objekt auswählen</label>
                <select id="mig-mapping-object-select" class="form-select form-select-sm"></select>
              </div>
              <div id="mig-mapping-assistant-shell" class="mb-3"></div>
              <div id="mig-mapping-panel">
                <div class="text-secondary small">Bitte Objekt auswählen und Datei in Schritt 2 hinterlegen.</div>
              </div>
            </div>

            <!-- Step 4: Abhängigkeiten -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="4">
              <h6 class="fw-semibold mb-3">Schritt 4: Abhängigkeiten zwischen Objekten</h6>
              <p class="text-secondary small">Definiert, welches Objekt zuerst importiert werden muss (z. B. Account vor Contact).</p>
              <div id="mig-dependencies-list" class="mb-3"></div>
              <button type="button" class="btn btn-sm btn-outline-primary" id="mig-add-dependency">+ Abhängigkeit hinzufügen</button>
              <div id="mig-dependency-form" class="d-none mt-3 p-3 border rounded">
                <div class="row g-2">
                  <div class="col-md-5">
                    <label class="form-label small">Objekt (wird zuerst importiert)</label>
                    <select id="mig-dep-from" class="form-select form-select-sm"></select>
                  </div>
                  <div class="col-md-2 d-flex align-items-end justify-content-center pb-1">→</div>
                  <div class="col-md-5">
                    <label class="form-label small">Objekt (hängt ab von)</label>
                    <select id="mig-dep-to" class="form-select form-select-sm"></select>
                  </div>
                  <div class="col-md-5">
                    <label class="form-label small">Feld in Abhängigem (z. B. AccountId)</label>
                    <input type="text" id="mig-dep-from-field" class="form-control form-control-sm" placeholder="AccountId" />
                  </div>
                  <div class="col-md-2"></div>
                  <div class="col-md-5">
                    <label class="form-label small">Feld in Quelle (z. B. Id)</label>
                    <input type="text" id="mig-dep-to-field" class="form-control form-control-sm" placeholder="Id" />
                  </div>
                </div>
                <div class="mt-2 d-flex gap-2">
                  <button type="button" class="btn btn-sm btn-primary" id="mig-save-dependency">Speichern</button>
                  <button type="button" class="btn btn-sm btn-outline-secondary" id="mig-cancel-dependency">Abbrechen</button>
                </div>
              </div>
            </div>

            <!-- Step 5: Reihenfolge -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="5">
              <h6 class="fw-semibold mb-3">Schritt 5: Ausführungsreihenfolge festlegen</h6>
              <p class="text-secondary small">Passe die Reihenfolge mit den Pfeilen an.</p>
              <ul id="mig-order-list" class="list-group"></ul>
            </div>

            <!-- Step 6: Fehlende Felder anlegen -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="6">
              <h6 class="fw-semibold mb-3">Schritt 6: Fehlende Salesforce-Felder anlegen (optional)</h6>
              <p class="text-secondary small">Felder aus der Quelldatei, die noch nicht in Salesforce vorhanden sind, können hier angelegt werden.</p>
              <div id="mig-missing-fields-list">
                <div class="text-secondary small">Wird nach dem Laden der Felder in Schritt 3 befüllt.</div>
              </div>
              <div id="mig-create-fields-result" class="mt-2"></div>
            </div>

            <!-- Step 7: Ausführen -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="7">
              <h6 class="fw-semibold mb-3">Schritt 7: Migration ausführen</h6>
              <div id="mig-review-summary" class="mb-3"></div>
              <div id="mig-run-progress" class="d-none">
                <div class="d-flex align-items-center gap-2 mb-2">
                  <div id="mig-run-status-spinner" class="spinner-border spinner-border-sm text-primary" role="status"></div>
                   <span id="mig-run-status-title">Migration läuft...</span>
                </div>
                <div id="mig-run-steps"></div>
              </div>
              <div id="mig-run-result" class="d-none"></div>
            </div>

          </div>
          <div class="modal-footer connector-wizard-footer">
            <div class="connector-wizard-footer-group connector-wizard-footer-start">
              <button type="button" class="btn btn-outline-secondary" id="mig-wizard-prev" disabled>← Zurück</button>
              <button type="button" class="btn btn-primary" id="mig-wizard-next">Weiter →</button>
            </div>
            <div class="connector-wizard-footer-group connector-wizard-footer-end">
              <button type="button" class="btn btn-outline-primary" id="mig-wizard-save">Zwischenspeichern</button>
              <button type="button" class="btn btn-light" data-bs-dismiss="modal">Abbrechen</button>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="modal fade" id="instance-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog">
        <div class="modal-content">
          <div class="modal-header"><h5 class="modal-title">Salesforce Instanz hinzufügen</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body">
            <div class="row g-2">
              <div class="col-12"><label class="form-label">Instanz-ID</label><input id="ins-id" class="form-control" placeholder="z. B. sandbox-1" /></div>
              <div class="col-12"><label class="form-label">Name</label><input id="ins-name" class="form-control" placeholder="z. B. Sandbox Team A" /></div>
              <div class="col-12"><label class="form-label">Login URL</label><input id="ins-login-url" class="form-control" placeholder="https://login.salesforce.com" /></div>
              <div class="col-12"><label class="form-label">Client ID</label><input id="ins-client-id" class="form-control" /></div>
              <div class="col-12"><label class="form-label">Client Secret</label><input id="ins-client-secret" class="form-control" type="password" /></div>
              <div class="col-12"><label class="form-label">Query Limit (optional)</label><input id="ins-query-limit" class="form-control" type="number" /></div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Abbrechen</button>
            <button id="save-instance" type="button" class="btn btn-primary">Instanz speichern</button>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="migration-instance-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog">
        <div class="modal-content">
          <div class="modal-header"><h5 class="modal-title">Migrations-Instanz verbinden</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body">
            <div class="row g-2">
              <div class="col-12"><label class="form-label">Name</label><input id="mig-ins-name" class="form-control" placeholder="z. B. Sandbox Migration Team A" /></div>
              <div class="col-md-6"><label class="form-label">Umgebung</label><select id="mig-ins-environment" class="form-select"><option value="sandbox">Sandbox</option><option value="production">Produktion</option></select></div>
              <div class="col-md-6"><label class="form-label">Query Limit (optional)</label><input id="mig-ins-query-limit" class="form-control" type="number" /></div>
              <div class="col-12"><label class="form-label">Client ID</label><input id="mig-ins-client-id" class="form-control" /></div>
              <div class="col-12"><label class="form-label">Client Secret</label><input id="mig-ins-client-secret" class="form-control" type="password" /></div>
              <div class="col-12"><div id="mig-ins-login-url-hint" class="form-text">Verwendete Login-URL: https://test.salesforce.com</div></div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Abbrechen</button>
            <button id="save-migration-instance" type="button" class="btn btn-primary">Speichern und verbinden</button>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="schedule-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Scheduler-Assistent</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div id="sch-modal-error" class="alert alert-danger d-none mx-3 mt-2 mb-0 py-2" role="alert" style="font-size:0.875rem"></div>
          <div class="modal-body">
            <input id="sch-id" type="hidden" />
            <div class="migration-wizard-steps-line connector-wizard-steps mb-3" id="sch-wizard-steps" style="--wizard-step-count: 5; --wizard-step-count-mobile: 3;">
              <button type="button" class="migration-wizard-step connector-wizard-step is-active" data-sch-step="1"><span class="migration-wizard-step-index connector-wizard-step-index">1</span><span class="migration-wizard-step-label connector-wizard-step-label">Allgemein</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-sch-step="2"><span class="migration-wizard-step-index connector-wizard-step-index">2</span><span class="migration-wizard-step-label connector-wizard-step-label">Quelle</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-sch-step="3"><span class="migration-wizard-step-index connector-wizard-step-index">3</span><span class="migration-wizard-step-label connector-wizard-step-label">Ziel</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-sch-step="4"><span class="migration-wizard-step-index connector-wizard-step-index">4</span><span class="migration-wizard-step-label connector-wizard-step-label">Timing</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-sch-step="5"><span class="migration-wizard-step-index connector-wizard-step-index">5</span><span class="migration-wizard-step-label connector-wizard-step-label">Mapping</span></button>
            </div>
            <div id="sch-wizard-hint" class="connector-wizard-hint mb-3">Assistent aktiv: Der Scheduler wird Schritt für Schritt konfiguriert und erst am Ende gespeichert.</div>

            <div class="tab-content" id="schedule-tab-content">
              
              <!-- Tab 1: Allgemein -->
              <div class="tab-pane fade show active" id="sch-tab-general" data-sch-step-panel="1" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-6"><label class="form-label">Name</label><input id="sch-name" class="form-control" /></div>
                  <div class="col-md-6"><label class="form-label">Connector</label><select id="sch-connector" class="form-select"></select></div>
                  <div class="col-md-6"><label class="form-label">Parent Scheduler</label><select id="sch-parent-schedule" class="form-select"><option value="">- Kein Parent -</option></select></div>
                  <div class="col-md-6 d-flex align-items-end"><div class="form-check"><input id="sch-inherit-parent-timing" class="form-check-input" type="checkbox" /><label class="form-check-label">Zeitsteuerung vom Parent übernehmen</label></div></div>
                  <div class="col-md-12"><label class="form-label">Batch Size</label><input id="sch-batch-size" type="number" class="form-control" value="100" /></div>
                  <div class="col-md-6"><label class="form-label">Nächster Lauf</label><input id="sch-next-run" type="datetime-local" class="form-control" /></div>
                  <div class="col-md-6"><label class="form-label">Letzter Lauf</label><input id="sch-last-run" type="datetime-local" class="form-control" readonly /></div>
                  <div class="col-md-12 d-flex align-items-end"><div class="form-check"><input id="sch-active" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Aktiv</label></div></div>
                  <div class="col-md-12 mt-2">
                    <div class="border rounded p-2 bg-light">
                      <div class="d-flex justify-content-between align-items-center gap-2 mb-2">
                        <div>
                          <div class="fw-semibold">Letzte Scheduler-Logs</div>
                          <div id="sch-recent-logs-meta" class="small text-secondary">Noch kein Run geladen.</div>
                        </div>
                        <button id="sch-refresh-recent-logs" type="button" class="btn btn-outline-secondary btn-sm">Logs aktualisieren</button>
                      </div>
                      <pre id="sch-recent-logs-output" class="bg-dark text-light p-3 rounded small mb-0" style="max-height: 220px; overflow-y: auto;">Noch keine Logs geladen.</pre>
                    </div>
                  </div>
                  <div class="col-md-12">
                    <details class="border rounded p-2 bg-light">
                      <summary class="fw-semibold">Änderungshistorie</summary>
                      <div class="d-flex justify-content-between align-items-center gap-2 my-2">
                        <div id="sch-history-meta" class="small text-secondary">Noch nicht geladen.</div>
                        <button id="sch-refresh-history" type="button" class="btn btn-outline-secondary btn-sm">Historie aktualisieren</button>
                      </div>
                      <div id="sch-history-list" class="small text-secondary">Scheduler noch nicht gespeichert.</div>
                    </details>
                  </div>
                </div>
              </div>
              
              <!-- Tab 2: Datenquelle -->
              <div class="tab-pane fade" id="sch-tab-source" data-sch-step-panel="2" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-6"><label class="form-label">Source System</label><select id="sch-source-system" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-6"><label class="form-label">Source Type</label><select id="sch-source-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE_SOQL">SALESFORCE_SOQL</option><option value="MSSQL_SQL">MSSQL_SQL</option><option value="REST_API">REST_API</option><option value="FILE_CSV">FILE_CSV</option><option value="FILE_EXCEL">FILE_EXCEL</option><option value="FILE_JSON">FILE_JSON</option></select></div>
                  <div class="col-md-12">
                    <details class="json-field-collapsible">
                      <summary class="json-field-summary">Source Definition / Abfrage</summary>
                      <textarea id="sch-source-definition" class="form-control mt-2" rows="4" placeholder='SELECT Id, Name FROM Account'></textarea>
                    </details>
                  </div>
                  <div id="sch-source-relative-directory-wrap" class="col-md-6 d-none"><label class="form-label">Source Unterverzeichnis relativ zum Connector-Importpfad</label><input id="sch-source-relative-directory" class="form-control" placeholder="z. B. kunden/import" /></div>
                  <div id="sch-source-archive-relative-directory-wrap" class="col-md-6 d-none"><label class="form-label">Archiv-Unterverzeichnis relativ zum Connector-Archivpfad</label><input id="sch-source-archive-relative-directory" class="form-control" placeholder="optional, sonst gleiches Unterverzeichnis" /></div>
                  <div id="sch-source-path-summary-wrap" class="col-md-12 d-none"><div id="sch-source-path-summary" class="small text-secondary border rounded p-2 bg-light">Keine Agent-Pfade berechnet.</div></div>
                  <div id="sch-source-delta-wrap" class="col-md-12 d-none">
                    <div class="border rounded p-2 bg-light">
                      <div class="row g-2 align-items-end">
                        <div class="col-md-4"><label class="form-label">Delta Modus</label><select id="sch-source-delta-strategy" class="form-select"><option value="">Komplettlauf</option><option value="datetime">Datum / LastModified</option><option value="timestamp">Timestamp / RowVersion</option><option value="id">ID</option></select></div>
                        <div class="col-md-8"><label class="form-label">Delta Feld</label><input id="sch-source-delta-field" class="form-control" placeholder="z. B. LastModifiedDate / rowversion / Id" /></div>
                        <div class="col-md-8"><label class="form-label">Aktueller Delta Wert</label><input id="sch-source-delta-current" class="form-control" placeholder="Wird aus dem letzten Checkpoint geladen" /></div>
                        <div class="col-md-4"><label class="form-label">Aktuelle Delta Record-ID</label><input id="sch-source-delta-record-id" class="form-control" placeholder="Optional, v. a. für datetime" /></div>
                        <div class="col-md-12"><div id="sch-source-delta-help" class="small text-secondary">Der letzte Delta-Wert wird nach jedem Lauf automatisch gespeichert.</div></div>
                      </div>
                    </div>
                  </div>
                  <div id="sch-source-after-export-wrap" class="col-md-12 d-none">
                    <div class="border rounded p-2 bg-light">
                      <div class="row g-2 align-items-end">
                        <div class="col-md-12"><label class="form-label">After Export Updates</label><input id="sch-source-after-export" class="form-control" placeholder="z. B. PostStatus__c=exported,PostDate__c=exportdate" /></div>
                        <div class="col-md-12"><div id="sch-source-after-export-help" class="small text-secondary">Nur für Salesforce-Quellen. Erfolgreich exportierte Datensätze werden danach aktualisiert. Token: exportdate, runid.</div></div>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-12 d-flex gap-2 align-items-center">
                    <button id="sch-test-source" type="button" class="btn btn-outline-primary btn-sm">Quelle testen</button>
                    <button id="sch-validate-config" type="button" class="btn btn-outline-secondary btn-sm">Konfiguration prüfen</button>
                    <div id="sch-source-test-status" class="small text-secondary">Es werden bis zu 10 Datensätze angezeigt.</div>
                  </div>
                  <div class="col-md-12">
                    <div id="sch-source-sql-highlight-wrap" class="query-highlight-wrap d-none">
                      <div class="small text-secondary mb-1">SQL Syntax-Preview</div>
                      <pre id="sch-source-sql-highlight" class="query-highlight mb-0"></pre>
                    </div>
                  </div>
                  <div class="col-md-12">
                    <div class="fw-semibold mb-2">Quellvorschau (ca. 10 Datensätze)</div>
                    <div class="border rounded p-2 bg-light" style="max-height: 260px; overflow-y: auto;">
                      <table class="table table-sm mb-0">
                        <thead id="sch-source-preview-header"></thead>
                        <tbody id="sch-source-preview-body"></tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </div>
              
              <!-- Tab 3: Datenziel -->
              <div class="tab-pane fade" id="sch-tab-target" data-sch-step-panel="3" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-4"><label class="form-label">Target System</label><select id="sch-target-system" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Objekt</label><select id="sch-object" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Operation</label><select id="sch-operation" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Target Type</label><select id="sch-target-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE">SALESFORCE</option><option value="SALESFORCE_GLOBAL_PICKLIST">SALESFORCE_GLOBAL_PICKLIST</option><option value="MSSQL">MSSQL</option><option value="FILE_CSV">FILE_CSV</option><option value="FILE_EXCEL">FILE_EXCEL</option><option value="FILE_JSON">FILE_JSON</option></select></div>
                  <div class="col-md-4"><label class="form-label">Direction</label><select id="sch-direction" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div id="sch-external-id-wrap" class="col-md-4 d-none"><label id="sch-external-id-label" class="form-label">Upsert Feld</label><select id="sch-external-id-field" class="form-select"><option value="">- Upsert Feld wählen -</option></select><div id="sch-external-id-help" class="form-text">Wählen Sie das Feld, das für Upsert verwendet werden soll.</div></div>
                  <div id="sch-pricebook2id-wrap" class="col-md-4 d-none"><label class="form-label">Pricebook</label><select id="sch-pricebook2id" class="form-select"><option value="">- Pricebook wählen -</option></select><div id="sch-pricebook2id-help" class="form-text">Optional als festes Ziel-Pricebook für PricebookEntry-Upserts.</div></div>
                  <div class="col-md-12">
                    <details class="json-field-collapsible">
                      <summary class="json-field-summary">Target Definition (JSON)</summary>
                      <textarea id="sch-target-definition" class="form-control mt-2" rows="4" placeholder='{"fields":[]}'></textarea>
                      <div id="sch-target-definition-help" class="form-text"></div>
                    </details>
                  </div>
                  <div id="sch-target-relative-directory-wrap" class="col-md-6 d-none"><label class="form-label">Target Unterverzeichnis relativ zum Connector-Exportpfad</label><input id="sch-target-relative-directory" class="form-control" placeholder="z. B. kunden/export" /></div>
                  <div id="sch-target-archive-relative-directory-wrap" class="col-md-6 d-none"><label class="form-label">Archiv-Unterverzeichnis relativ zum Connector-Archivpfad</label><input id="sch-target-archive-relative-directory" class="form-control" placeholder="optional, sonst gleiches Unterverzeichnis" /></div>
                  <div id="sch-target-path-summary-wrap" class="col-md-12 d-none"><div id="sch-target-path-summary" class="small text-secondary border rounded p-2 bg-light">Keine Agent-Pfade berechnet.</div></div>
                  <div id="sch-create-object-wrap" class="col-md-12 d-none">
                    <div class="border rounded p-2 bg-light">
                      <div class="fw-semibold mb-1">Custom-Objekt aus Quelle erzeugen</div>
                      <div class="small text-secondary mb-2">Legt ein Salesforce Custom-Objekt auf Basis der geladenen Quellfelder an.</div>
                      <div class="small text-secondary mb-2">Feldtypen können vorab manuell überschrieben werden.</div>
                      <div class="table-responsive mb-2">
                        <table class="table table-sm mb-0">
                          <thead><tr><th>Quellfeld</th><th>Typ aus Quelle</th><th>Salesforce Typ (Override)</th></tr></thead>
                          <tbody id="sch-create-object-field-overrides"><tr><td colspan="3" class="text-secondary">Quellfelder laden, um Overrides zu setzen.</td></tr></tbody>
                        </table>
                      </div>
                      <div class="row g-2 align-items-end">
                        <div class="col-md-6"><label class="form-label">Objekt API Name</label><input id="sch-new-custom-object" class="form-control" placeholder="z. B. SourceExchangeRate__c" /></div>
                        <div class="col-md-4"><label class="form-label">Label (optional)</label><input id="sch-new-custom-object-label" class="form-control" placeholder="z. B. Source Exchange Rate" /></div>
                        <div class="col-md-2 d-grid"><button id="sch-create-custom-object" type="button" class="btn btn-outline-primary btn-sm">Objekt anlegen</button></div>
                      </div>
                      <div id="sch-create-object-status" class="small text-secondary mt-2">Bereit.</div>
                    </div>
                  </div>
                </div>
              </div>
              
              <!-- Tab 4: Zeitsteuerung -->
              <div class="tab-pane fade" id="sch-tab-timing" data-sch-step-panel="4" role="tabpanel">
                <div class="card schedule-helper-card border-0">
                  <div class="card-body">
                    <div class="fw-semibold mb-1">Zeitsteuerung (Assistent)</div>
                    <div class="small text-secondary mb-3">Wochentage und Uhrzeit wählen. Der Assistent setzt automatisch den nächsten Laufzeitpunkt.</div>
                    <div class="row g-2 align-items-end">
                      <div class="col-md-4"><label class="form-label">Startdatum</label><input id="sch-timing-start" type="date" class="form-control" /></div>
                      <div class="col-md-2"><label class="form-label">Uhrzeit</label><input id="sch-timing-time" type="time" class="form-control" value="09:00" /></div>
                      <div class="col-md-2"><label class="form-label">Intervall (Min.)</label><input id="sch-timing-interval" type="number" class="form-control" value="2" min="1" max="1440" /></div>
                      <div class="col-md-4 d-flex gap-2">
                        <button id="sch-timing-apply" type="button" class="btn btn-outline-primary btn-sm">Nächsten Lauf berechnen</button>
                        <button id="sch-timing-reset" type="button" class="btn btn-outline-secondary btn-sm">Zurücksetzen</button>
                      </div>
                      <div class="col-12"><div class="fw-semibold small mb-2">Wochentage</div><div class="d-flex flex-wrap gap-2" id="sch-weekdays">
                        <label class="weekday-chip"><input type="checkbox" value="1" /> Mo</label>
                        <label class="weekday-chip"><input type="checkbox" value="2" /> Di</label>
                        <label class="weekday-chip"><input type="checkbox" value="3" /> Mi</label>
                        <label class="weekday-chip"><input type="checkbox" value="4" /> Do</label>
                        <label class="weekday-chip"><input type="checkbox" value="5" /> Fr</label>
                        <label class="weekday-chip"><input type="checkbox" value="6" /> Sa</label>
                        <label class="weekday-chip"><input type="checkbox" value="0" /> So</label>
                      </div></div>
                      <div class="col-12 mt-3"><div id="sch-timing-preview" class="small text-secondary p-2 bg-light rounded">Noch keine Zeitsteuerung berechnet.</div></div>
                    </div>
                  </div>
                </div>
              </div>
              
              <!-- Tab 5: Mapping -->
              <div class="tab-pane fade" id="sch-tab-mapping" data-sch-step-panel="5" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-12">
                    <div class="mb-3">
                      <div id="sch-mapping-manager" class="scheduler-mapping-manager mb-3"></div>
                      <div class="border rounded p-2 bg-light d-none" style="max-height: 200px; overflow-y: auto;" aria-hidden="true">
                        <table class="table table-sm mb-0">
                          <thead><tr><th>Feldname</th><th>Typ</th></tr></thead>
                          <tbody id="sch-mapping-source-fields"></tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-12 d-none" aria-hidden="true">
                    <div class="mb-3">
                      <div class="fw-semibold mb-2">Mapping-Regeln</div>
                      <div id="sch-mapping-rules-dropzone" class="border rounded p-2 bg-light" style="max-height: 250px; overflow-y: auto;">
                        <table class="table table-sm mb-0">
                          <thead><tr><th>Quelle</th><th>Ziel</th><th>Lookup</th><th>Funktion</th><th>Picklist</th><th>Aktion</th></tr></thead>
                          <tbody id="sch-mapping-rules"></tbody>
                        </table>
                      </div>
                      <div class="small text-secondary mt-2">Quellfelder per DragDrop in diese Tabelle ziehen. Klick auf eine Zeile öffnet die Bearbeitung.</div>
                      <div id="sch-mapping-required-status" class="small mt-2 text-secondary">Pflichtfelder fuer Salesforce Insert/Upsert werden geladen, sobald ein Zielobjekt gewaehlt ist.</div>
                    </div>
                  </div>
                  <div class="col-md-12 d-none" aria-hidden="true">
                    <div class="card border-0 schedule-helper-card">
                      <div class="card-body">
                        <div class="fw-semibold mb-2">Mapping-Details</div>
                        <div class="small text-secondary mb-3" id="sch-map-detail-status">Noch keine Mapping-Zeile ausgewählt.</div>
                        <div class="row g-2 mb-2">
                          <div class="col-md-12">
                            <label class="form-label">Quellfeld</label>
                            <input id="sch-map-detail-source" class="form-control" readonly />
                          </div>
                        </div>
                        <ul class="nav nav-tabs nav-fill mb-2" role="tablist" style="font-size: 0.85rem;">
                          <li class="nav-item" role="presentation"><button class="nav-link active" type="button" data-bs-toggle="tab" data-bs-target="#mapping-tab-basics">Grundlagen</button></li>
                          <li class="nav-item" role="presentation"><button class="nav-link" type="button" data-bs-toggle="tab" data-bs-target="#mapping-tab-lookup">Lookup</button></li>
                          <li class="nav-item" role="presentation"><button class="nav-link" type="button" data-bs-toggle="tab" data-bs-target="#mapping-tab-transform">Transform</button></li>
                          <li class="nav-item" role="presentation"><button class="nav-link" type="button" data-bs-toggle="tab" data-bs-target="#mapping-tab-picklist">Picklist</button></li>
                        </ul>
                        <div class="tab-content">
                          <div class="tab-pane fade show active" id="mapping-tab-basics" role="tabpanel">
                            <div class="row g-2">
                              <div class="col-md-12">
                                <label class="form-label">Zielfeld</label>
                                <select id="sch-map-detail-target" class="form-select"><option value="">- Wählen -</option></select>
                              </div>
                            </div>
                          </div>
                          <div class="tab-pane fade" id="mapping-tab-lookup" role="tabpanel">
                            <div class="row g-2">
                              <div class="col-md-12">
                                <div class="form-check">
                                  <input id="sch-map-detail-lookup-enabled" class="form-check-input" type="checkbox" />
                                  <label class="form-check-label" for="sch-map-detail-lookup-enabled">Lookup aktivieren</label>
                                </div>
                              </div>
                              <div class="col-md-6">
                                <label class="form-label">Lookup Objekt</label>
                                <input id="sch-map-detail-lookup-object" class="form-control" placeholder="z. B. Account" />
                              </div>
                              <div class="col-md-6">
                                <label class="form-label">Lookup Feld (External ID)</label>
                                <input id="sch-map-detail-lookup-field" class="form-control" placeholder="z. B. External_Id__c" />
                              </div>
                            </div>
                          </div>
                          <div class="tab-pane fade" id="mapping-tab-transform" role="tabpanel">
                            <div class="row g-2">
                              <div class="col-md-6">
                                <label class="form-label">Funktion</label>
                                <select id="sch-map-detail-transform" class="form-select"><option value="NONE">Keine Umwandlung</option></select>
                              </div>
                              <div class="col-md-6">
                                <label class="form-label">Parameter / Ausdruck</label>
                                <input id="sch-map-detail-transform-expression" class="form-control" placeholder="z. B. YYYY-MM-DD" />
                              </div>
                            </div>
                          </div>
                          <div class="tab-pane fade" id="mapping-tab-picklist" role="tabpanel">
                            <div class="row g-2">
                              <div class="col-md-12">
                                <label class="form-label">Picklist-Mapping</label>
                                <div class="border rounded p-2 bg-light" style="max-height: 200px; overflow-y: auto;">
                                  <table class="table table-sm mb-0">
                                    <thead><tr><th>Quellwert</th><th>Zielwert</th><th>Aktion</th></tr></thead>
                                    <tbody id="sch-map-detail-picklist-table"></tbody>
                                  </table>
                                </div>
                                <button id="sch-map-detail-picklist-add" type="button" class="btn btn-sm btn-outline-secondary mt-2">+ Eintrag</button>
                              </div>
                            </div>
                          </div>
                        </div>
                        <div class="row g-2 mt-3">
                          <div class="col-md-12 d-flex gap-2">
                            <button id="sch-map-detail-apply" type="button" class="btn btn-sm btn-primary">Änderungen übernehmen</button>
                            <button id="sch-map-detail-delete" type="button" class="btn btn-sm btn-outline-danger">Zeile löschen</button>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-12">
                    <details class="json-field-collapsible">
                      <summary class="json-field-summary">Mapping Definition (JSON)</summary>
                      <textarea id="sch-mapping" class="form-control mt-2" rows="4" placeholder='Mapping-Definition im JSON/DSL Format'></textarea>
                    </details>
                  </div>
                </div>
              </div>
              
            </div>
          </div>
          <div class="modal-footer connector-wizard-footer">
            <div class="connector-wizard-footer-group connector-wizard-footer-start">
              <button id="sch-wizard-back" type="button" class="btn btn-outline-secondary">← Zurück</button>
              <button id="sch-wizard-next" type="button" class="btn btn-primary">Weiter →</button>
            </div>
            <div class="connector-wizard-footer-group connector-wizard-footer-end">
              <button id="duplicate-schedule" type="button" class="btn btn-outline-secondary">Duplizieren</button>
              <button id="save-schedule-template" type="button" class="btn btn-outline-primary">Als Vorlage speichern</button>
              <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
              <button id="save-schedule" type="button" class="btn btn-primary d-none">Speichern</button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="connector-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable connector-wizard-dialog">
        <div class="modal-content connector-wizard-modal">
          <div class="modal-header connector-wizard-header"><h5 class="modal-title">Connector-Assistent</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body connector-wizard-body">
            <input id="con-id" type="hidden" />
            <div id="con-modal-error" class="alert alert-danger d-none mb-3 py-2" role="alert"></div>
            <div class="connector-wizard-stage">
            <div class="migration-wizard-steps-line connector-wizard-steps mb-3" id="con-wizard-steps" style="--wizard-step-count: 4; --wizard-step-count-mobile: 2;">
              <button type="button" class="migration-wizard-step connector-wizard-step is-active" data-step="1"><span class="migration-wizard-step-index connector-wizard-step-index">1</span><span class="migration-wizard-step-label connector-wizard-step-label">Typ</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-step="2"><span class="migration-wizard-step-index connector-wizard-step-index">2</span><span class="migration-wizard-step-label connector-wizard-step-label">Basis</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-step="3"><span class="migration-wizard-step-index connector-wizard-step-index">3</span><span class="migration-wizard-step-label connector-wizard-step-label">Parameter</span></button>
              <button type="button" class="migration-wizard-step connector-wizard-step" data-step="4"><span class="migration-wizard-step-index connector-wizard-step-index">4</span><span class="migration-wizard-step-label connector-wizard-step-label">Prüfen</span></button>
            </div>

            <div class="connector-wizard-panel" data-step-panel="1">
              <div class="row g-3">
                <div class="col-md-7"><label class="form-label">Welcher Connectortyp soll angelegt werden?</label><select id="con-wizard-type" class="form-select"><option value="MSSQL">MSSQL</option><option value="POSTGRESQL">PostgreSQL</option><option value="MYSQL">MySQL</option><option value="FILE">Datei (TXT, CSV, JSON, EXCEL)</option><option value="REST_API">REST API</option><option value="FILE_BINARY_SF_IMPORT">Datei Binärimport nach Salesforce</option><option value="CUSTOM">Benutzerdefiniert</option></select></div>
                <div class="col-md-5 d-none"><label class="form-label">Connector Type</label><input id="con-type" class="form-control" readonly /></div>
                <div class="col-12"><div id="con-wizard-hint" class="connector-wizard-hint">Assistent aktiv: Bitte zuerst den Typ wählen, danach führt dich der Assistent durch die Parameter.</div></div>
              </div>
            </div>

            <div class="connector-wizard-panel d-none" data-step-panel="2">
              <div class="row g-2">
                <div class="col-md-4"><label class="form-label">Name</label><input id="con-name" class="form-control" /></div>
                <div class="col-md-4"><label class="form-label">Target System</label><input id="con-target-system" class="form-control" /></div>
                <div class="col-md-4"><label class="form-label">Direction</label><input id="con-direction" class="form-control" /></div>
                <div class="col-md-4"><label class="form-label">Secret Key (ENV)</label><input id="con-secret" class="form-control" placeholder="z. B. MSSQL_DEV_PASSWORD" /></div>
                <div class="col-md-2"><label class="form-label">Timeout</label><input id="con-timeout" type="number" class="form-control" /></div>
                <div class="col-md-2"><label class="form-label">Retries</label><input id="con-retries" type="number" class="form-control" /></div>
                <div class="col-md-12"><label class="form-label">Beschreibung</label><textarea id="con-description" class="form-control" rows="2"></textarea></div>
                <div class="col-md-12">
                  <div class="border rounded p-2 bg-light">
                    <div class="fw-semibold mb-2">Salesforce Task-Benachrichtigung</div>
                    <div class="row g-2">
                      <div class="col-md-3 d-flex align-items-end"><div class="form-check"><input id="con-task-notify-enabled" class="form-check-input" type="checkbox" /><label class="form-check-label">Aktiv</label></div></div>
                      <div class="col-md-4"><label class="form-label">Task-Benutzer</label><select id="con-task-owner-id" class="form-select"><option value="">- Benutzer wählen -</option></select></div>
                      <div class="col-md-5"><label class="form-label">Fehlerklassen</label><select id="con-task-error-classes" class="form-select" multiple size="5"><option value="CONNECTION">Connection</option><option value="AUTH">Auth</option><option value="DATA">Data</option><option value="VALIDATION">Validation</option><option value="UNKNOWN">Unknown</option></select></div>
                      <div class="col-md-12"><div class="small text-secondary">Erzeugt bei passenden Connector-Fehlern automatisch einen Salesforce-Task mit Connector-, Scheduler- und Fehlerdetails. Mehrfachauswahl mit Cmd oder Strg.</div></div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="connector-wizard-panel d-none" data-step-panel="3">
              <div class="row g-2">
                <div class="col-md-12">
                  <details class="json-field-collapsible">
                    <summary class="json-field-summary">Parameters (JSON)</summary>
                    <textarea id="con-parameters" class="form-control mt-2" rows="4" placeholder='{"server":"...","database":"..."}'></textarea>
                  </details>
                </div>
                <div class="col-12 d-none" id="con-mssql-settings-wrap">
                <div class="border rounded p-2 bg-light">
                  <div id="con-sql-settings-title" class="fw-semibold mb-2">SQL Verbindung</div>
                  <div id="con-sql-settings-text" class="small text-secondary mb-2">Pflicht: Server, Datenbank und Benutzer. Bevorzugt Secret Key (ENV) statt Klartext-Passwort. Sichere Defaults: Encrypt aktiv, Trust Server Certificate deaktiviert.</div>
                  <div class="row g-2">
                    <div class="col-md-4"><label class="form-label">Server / Host</label><input id="con-mssql-server" class="form-control" placeholder="sql.example.local" /></div>
                    <div class="col-md-2"><label class="form-label">Port</label><input id="con-mssql-port" type="number" class="form-control" placeholder="1433" /></div>
                    <div class="col-md-3"><label class="form-label">Datenbank</label><input id="con-mssql-database" class="form-control" placeholder="ERP" /></div>
                    <div class="col-md-3"><label class="form-label">Benutzer</label><input id="con-mssql-user" class="form-control" placeholder="etl_user" /></div>
                    <div class="col-md-4"><label class="form-label">Passwort</label><input id="con-mssql-password" type="password" class="form-control" placeholder="Optional: direkt speichern" autocomplete="new-password" /></div>
                    <div class="col-md-3 d-flex align-items-end"><div class="form-check"><input id="con-mssql-encrypt" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Encrypt</label></div></div>
                    <div class="col-md-5 d-flex align-items-end"><div class="form-check"><input id="con-mssql-trust-server-certificate" class="form-check-input" type="checkbox" /><label class="form-check-label">Trust Server Certificate</label></div></div>
                  </div>
                </div>
                </div>
                <div class="col-12 d-none" id="con-file-settings-wrap">
                <div class="border rounded p-2 bg-light">
                  <div class="fw-semibold mb-2">Datei-Connector Einstellungen</div>
                  <div class="row g-2">
                    <div class="col-md-3"><label class="form-label">Dateiformat</label><select id="con-file-kind" class="form-select"><option value="TXT">TXT</option><option value="CSV">CSV</option><option value="JSON">JSON</option><option value="EXCEL">EXCEL</option></select></div>
                    <div class="col-md-3"><label class="form-label">Base Path</label><input id="con-file-base-path" class="form-control" placeholder="artifacts/files" /></div>
                    <div class="col-md-3"><label class="form-label">Import Path</label><input id="con-file-import-path" class="form-control" placeholder="inbound" /></div>
                    <div class="col-md-3"><label class="form-label">Export Path</label><input id="con-file-export-path" class="form-control" placeholder="outbound" /></div>
                    <div class="col-md-3"><label class="form-label">Archive Path</label><input id="con-file-archive-path" class="form-control" placeholder="archive" /></div>
                    <div class="col-md-3"><label class="form-label">Default Charset</label><input id="con-file-charset" class="form-control" placeholder="utf8" /></div>
                    <div class="col-md-3"><label class="form-label">Default Delimiter</label><input id="con-file-delimiter" class="form-control" placeholder=";" /></div>
                    <div class="col-md-3 d-flex align-items-end"><div class="form-check"><input id="con-file-archive-read" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Nach Lesen archivieren</label></div></div>
                    <div class="col-md-3 d-flex align-items-end"><div class="form-check"><input id="con-file-archive-write" class="form-check-input" type="checkbox" /><label class="form-check-label">Nach Schreiben archivieren</label></div></div>
                  </div>
                </div>
                </div>
                <div class="col-12 d-none" id="con-rest-settings-wrap">
                <div class="border rounded p-2 bg-light">
                  <div class="fw-semibold mb-2">REST API</div>
                  <div class="small text-secondary mb-2">Authentifizierung ist frei wählbar. OAuth2 ist nur eine zusätzliche Option neben None, Basic, Bearer Token und API Key.</div>
                  <div class="row g-2">
                    <div class="col-md-6"><label class="form-label">Base URL</label><input id="con-rest-base-url" class="form-control" placeholder="https://api.example.com" /></div>
                    <div class="col-md-6"><label class="form-label">Resource Path</label><input id="con-rest-resource-path" class="form-control" placeholder="/v1/items" /></div>
                    <div class="col-md-3"><label class="form-label">Auth Typ</label><select id="con-rest-auth-type" class="form-select"><option value="none">Keine</option><option value="basic">Basic Auth</option><option value="bearer">Bearer Token</option><option value="api_key">API Key</option><option value="oauth2">OAuth2</option></select></div>
                    <div class="col-md-3"><label class="form-label">HTTP Method</label><select id="con-rest-method" class="form-select"><option value="GET">GET</option><option value="POST">POST</option><option value="PUT">PUT</option></select></div>
                    <div class="col-md-3 d-none" id="con-rest-basic-user-wrap"><label class="form-label">Basic User</label><input id="con-rest-basic-user" class="form-control" /></div>
                    <div class="col-md-3 d-none" id="con-rest-basic-password-wrap"><label class="form-label">Basic Passwort</label><input id="con-rest-basic-password" type="password" class="form-control" autocomplete="new-password" /></div>
                    <div class="col-md-3 d-none" id="con-rest-bearer-token-wrap"><label class="form-label">Bearer Token</label><input id="con-rest-bearer-token" type="password" class="form-control" autocomplete="new-password" /></div>
                    <div class="col-md-3 d-none" id="con-rest-api-key-name-wrap"><label class="form-label">API Key Name</label><input id="con-rest-api-key-name" class="form-control" placeholder="x-api-key" /></div>
                    <div class="col-md-3 d-none" id="con-rest-api-key-value-wrap"><label class="form-label">API Key Wert</label><input id="con-rest-api-key-value" type="password" class="form-control" autocomplete="new-password" /></div>
                    <div class="col-md-3 d-none" id="con-rest-api-key-location-wrap"><label class="form-label">API Key Ort</label><select id="con-rest-api-key-location" class="form-select"><option value="header">Header</option><option value="query">Query</option></select></div>
                    <div class="col-md-6 d-none" id="con-rest-token-url-wrap"><label class="form-label">Token URL</label><input id="con-rest-token-url" class="form-control" placeholder="https://auth.example.com/oauth/token" /></div>
                    <div class="col-md-3 d-none" id="con-rest-grant-type-wrap"><label class="form-label">Grant Type</label><select id="con-rest-grant-type" class="form-select"><option value="client_credentials">client_credentials</option><option value="password">password</option><option value="authorization_code">authorization_code</option></select></div>
                    <div class="col-md-4 d-none" id="con-rest-client-id-wrap"><label class="form-label">Client ID</label><input id="con-rest-client-id" class="form-control" /></div>
                    <div class="col-md-4 d-none" id="con-rest-client-secret-wrap"><label class="form-label">Client Secret</label><input id="con-rest-client-secret" type="password" class="form-control" autocomplete="new-password" /></div>
                    <div class="col-md-4 d-none" id="con-rest-scope-wrap"><label class="form-label">Scope</label><input id="con-rest-scope" class="form-control" placeholder="api.read api.write" /></div>
                    <div class="col-md-6"><label class="form-label">Audience (optional)</label><input id="con-rest-audience" class="form-control" /></div>
                    <div class="col-md-6"><label class="form-label">Zusätzliche Header (JSON)</label><input id="con-rest-extra-headers" class="form-control" placeholder='{"X-Tenant":"abc"}' /></div>
                  </div>
                </div>
                </div>
                <div class="col-12 d-none" id="con-binary-settings-wrap">
                <div class="border rounded p-2 bg-light">
                  <div class="fw-semibold mb-2">Datei Binärimport nach Salesforce</div>
                  <div class="small text-secondary mb-2">Importiert Binärdateien aus einem Verzeichnis und schreibt sie als Salesforce-Dateiobjekte.</div>
                  <div class="row g-2">
                    <div class="col-md-4"><label class="form-label">Base Path</label><input id="con-binary-base-path" class="form-control" placeholder="artifacts/files" /></div>
                    <div class="col-md-4"><label class="form-label">Import Path</label><input id="con-binary-import-path" class="form-control" placeholder="binary-inbound" /></div>
                    <div class="col-md-4"><label class="form-label">Archive Path</label><input id="con-binary-archive-path" class="form-control" placeholder="archive" /></div>
                    <div class="col-md-4"><label class="form-label">Erlaubte Endungen</label><input id="con-binary-extensions" class="form-control" placeholder="pdf,jpg,png,zip" /></div>
                    <div class="col-md-4"><label class="form-label">Salesforce Objekt</label><input id="con-binary-sf-object" class="form-control" value="ContentVersion" /></div>
                    <div class="col-md-4"><label class="form-label">Binary Feld</label><input id="con-binary-sf-binary-field" class="form-control" value="VersionData" /></div>
                    <div class="col-md-6"><label class="form-label">Dateiname Feld</label><input id="con-binary-sf-filename-field" class="form-control" value="PathOnClient" /></div>
                    <div class="col-md-6"><label class="form-label">Titel Präfix (optional)</label><input id="con-binary-title-prefix" class="form-control" placeholder="Import" /></div>
                  </div>
                </div>
                </div>
                <div class="col-md-6 d-flex align-items-end"><div class="form-check"><input id="con-active" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Aktiv</label></div></div>
              </div>
            </div>

            <div class="connector-wizard-panel d-none" data-step-panel="4">
              <div class="connector-review-card">
                <div class="fw-semibold mb-2">Prüfung vor dem Speichern</div>
                <div id="con-review-summary" class="connector-review-summary mb-3">Noch keine Daten erfasst.</div>
                <div class="small text-secondary mb-2">Der Validierungsschritt speichert den Connector und führt danach den bestehenden Verbindungstest aus.</div>
                <label class="form-label">Parameter Vorschau</label>
                <pre id="con-review-json" class="connector-review-json mb-0">{}</pre>
              </div>
            </div>
            <details class="border rounded p-2 bg-light mt-3">
              <summary class="fw-semibold">Änderungshistorie</summary>
              <div class="d-flex justify-content-between align-items-center gap-2 my-2">
                <div id="con-history-meta" class="small text-secondary">Noch nicht geladen.</div>
                <button id="con-refresh-history" type="button" class="btn btn-outline-secondary btn-sm">Historie aktualisieren</button>
              </div>
              <div id="con-history-list" class="small text-secondary">Connector noch nicht gespeichert.</div>
            </details>
            </div>
          </div>
          <div class="modal-footer connector-wizard-footer">
            <div class="connector-wizard-footer-group connector-wizard-footer-start">
              <button id="con-wizard-back" type="button" class="btn btn-outline-secondary">← Zurück</button>
              <button id="con-wizard-next" type="button" class="btn btn-primary">Weiter →</button>
            </div>
            <div class="connector-wizard-footer-group connector-wizard-footer-end">
              <button id="save-connector-template" type="button" class="btn btn-outline-primary">Als Vorlage speichern</button>
              <button id="test-connector" type="button" class="btn btn-outline-secondary">Speichern und validieren</button>
              <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
              <button id="save-connector" type="button" class="btn btn-primary">Speichern</button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade template-store-modal" id="template-picker-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 id="template-picker-title" class="modal-title">Vorlage waehlen</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div id="template-picker-error" class="alert alert-danger d-none mb-3 py-2" role="alert"></div>
            <div class="mb-3">
              <input id="template-picker-search" type="search" class="form-control" placeholder="Vorlagen suchen..." />
            </div>
            <div id="template-picker-tags" class="d-flex flex-wrap gap-2 mb-3"></div>
            <div id="template-picker-summary" class="small text-secondary mb-3">Keine Vorlagen geladen.</div>
            <div id="template-picker-list" class="list-group"></div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
            <button id="template-picker-apply" type="button" class="btn btn-primary" disabled>Vorlage übernehmen</button>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="logs-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 id="logs-modal-title" class="modal-title">Logliste</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body p-0">
            <div class="table-responsive">
              <input type="search" class="form-control form-control-sm mb-2" placeholder="Suche Logs..." id="logs-filter" />
              <table class="table table-sm mb-0" id="logs-table" style="table-layout: fixed; width: 100%;">
                <thead>
                  <tr>
                    <th>Zeit</th>
                    <th>Level</th>
                    <th>Connector</th>
                    <th>Schedule</th>
                    <th>Step</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody id="logs-modal-body"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="records-scheduler-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 id="records-scheduler-modal-title" class="modal-title">Scheduler zum Datenpunkt</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div id="records-scheduler-modal-summary" class="small text-secondary mb-3"></div>
            <div id="records-scheduler-modal-list" class="list-group"></div>
          </div>
        </div>
      </div>
    </div>

    <script src="/assets/chart.umd.js"></script>
    <script src="/assets/bootstrap.bundle.min.js"></script>
    <script>
      const LOG_CHART_RANGE_STORAGE_KEY = 'sf-agent.logChartRange';
      const MAX_LOG_CONNECTOR_SERIES = 5;
      const MAX_RECORD_CONNECTOR_SERIES = 6;
      const UI_THEME_STORAGE_KEY = 'sf-agent.uiTheme';
      const OVERVIEW_STATS_RANGE_STORAGE_KEY = 'sf-agent.overviewStatsRange';
      const nativeFetch = window.fetch.bind(window);
      const state = {
        instanceId: '',
        schedules: [],
        connectors: [],
        connectorTestResults: {},
        migrations: [],
        cpuLoadHistory: [],
        connectorWizardStep: 1,
        scheduleWizardStep: 1,
        previousOverviewSnapshot: null,
        overviewStatsRange: 'month',
        graphData: { nodes: [], edges: [] },
        overviewConnectorFilterId: '',
        schedulerConnectorFilterId: '',
        schedulerActiveFilter: 'all',
        schedulerDirectionTab: 'all',
        runs: [],
        staleRuns: [],
        recordsSummary: null,
        updateStatus: null,
        updateStatusCheckedAt: 0,
        updateStatusPollTimer: null,
        mappingFields: [],
        sourcePreviewRows: [],
        targetFields: [],
        mappingRules: [],
        mappingFieldsLoadSeq: 0,
        targetObjectsLoadSeq: 0,
        targetFieldsLoadSeq: 0,
        schedulerLookupObjects: [],
        schedulerLookupObjectsLoaded: false,
        schedulerLookupObjectsLoadPromise: null,
        schedulerLookupExternalIdFieldsByObject: {},
        schedulerLookupExternalIdFieldPromises: {},
        hasIncompatibleScheduleMappings: false,
        scheduleMappingAssistantProfile: 'standard',
        rawMappingEditorDirty: false,
        selectedMappingRuleId: '',
        logSummary: null,
        salesforceOverview: null,
        installerSummary: null,
        installerGeneratedFiles: [],
        adminMe: null,
        adminUsers: [],
        auditHistory: [],
        customObjectFieldOverrides: {},
        scheduleOptions: {
          objectNames: [],
          operations: [],
          sourceSystems: [],
          targetSystems: [],
          directions: []
        }
      };
      const templatePickerState = {
        kind: 'connector',
        items: [],
        filteredItems: [],
        selectedTemplateId: '',
        selectedTag: '',
        resolver: null
      };

      // Migration wizard state - global to avoid hoisting issues
      let migState = {
        id: null,
        step: 1,
        totalSteps: 7,
        status: 'draft',
        activeRunVisible: false,
        name: '',
        description: '',
        batchSize: 200,
        instanceId: '',
        salesforceLogin: null,
        objects: [],
        dependencies: [],
        executionPlan: [],
        sfObjects: [],
        lastRunResult: null,
        runHistory: [],
        progressPollTimer: null,
        preflightWarnings: null,
        preflightWarningsLoading: false,
        pendingImports: [],
        pendingImportInProgress: false,
        pendingImportSuggestions: [],
        pendingImportAnalysis: null,
        mappingAssistantProfilesByObjectId: {}
      };

      function getMigLoginUrlForEnvironment(environment) {
        return String(environment || 'sandbox') === 'production'
          ? 'https://login.salesforce.com'
          : 'https://test.salesforce.com';
      }

      function populateMigExistingInstanceOptions() {
        const select = document.getElementById('mig-existing-instance');
        const globalSelect = document.getElementById('instance-select');
        if (!select) {
          return;
        }

        const options = globalSelect
          ? Array.from(globalSelect.options).filter((option) => String(option.value || '').trim())
          : [];

        if (!options.length) {
          select.innerHTML = '<option value="">Keine bestehenden Instanzen konfiguriert</option>';
          select.value = '';
          migState.instanceId = '';
          return;
        }

        select.innerHTML = options.map((option) =>
          '<option value="' + esc(option.value) + '">' + esc(option.textContent || option.value) + '</option>'
        ).join('');

        const desiredInstanceId = String(migState.instanceId || state.instanceId || '').trim();
        const hasDesiredInstance = options.some((option) => String(option.value || '').trim() === desiredInstanceId);
        select.value = hasDesiredInstance ? desiredInstanceId : String(options[0].value || '').trim();
        migState.instanceId = String(select.value || '').trim();
      }

      function renderMigSalesforceLoginStatus() {
        const statusEl = document.getElementById('mig-login-status');
        const authorizeButton = document.getElementById('mig-login-authorize');
        const instanceSourceEl = document.getElementById('mig-instance-source');
        const existingInstanceWrap = document.getElementById('mig-existing-instance-wrap');
        const existingInstanceEl = document.getElementById('mig-existing-instance');
        const environmentWrap = document.getElementById('mig-login-environment-wrap');
        const authTypeWrap = document.getElementById('mig-login-auth-type-wrap');
        const loginUrlWrap = document.getElementById('mig-login-url-wrap');
        const statusWrap = document.getElementById('mig-login-status-wrap');
        const authorizeWrap = document.getElementById('mig-login-authorize-wrap');
        const authTypeEl = document.getElementById('mig-login-auth-type');
        const usernameWrap = document.getElementById('mig-login-username-wrap');
        const passwordWrap = document.getElementById('mig-login-password-wrap');
        const securityTokenWrap = document.getElementById('mig-login-security-token-wrap');
        const clientIdWrap = document.getElementById('mig-login-client-id-wrap');
        const clientSecretWrap = document.getElementById('mig-login-client-secret-wrap');
        if (!statusEl) {
          return;
        }

        const instanceSource = String(instanceSourceEl && instanceSourceEl.value || (migState.salesforceLogin ? 'embedded' : 'existing'));
        const isExistingInstanceMode = instanceSource === 'existing';
        if (existingInstanceWrap) existingInstanceWrap.classList.toggle('d-none', !isExistingInstanceMode);
        if (environmentWrap) environmentWrap.classList.toggle('d-none', isExistingInstanceMode);
        if (authTypeWrap) authTypeWrap.classList.toggle('d-none', isExistingInstanceMode);
        if (loginUrlWrap) loginUrlWrap.classList.toggle('d-none', isExistingInstanceMode);
        if (authorizeWrap) authorizeWrap.classList.toggle('d-none', isExistingInstanceMode);
        if (isExistingInstanceMode) {
          populateMigExistingInstanceOptions();
          if (usernameWrap) usernameWrap.classList.add('d-none');
          if (passwordWrap) passwordWrap.classList.add('d-none');
          if (securityTokenWrap) securityTokenWrap.classList.add('d-none');
          if (clientIdWrap) clientIdWrap.classList.add('d-none');
          if (clientSecretWrap) clientSecretWrap.classList.add('d-none');
          if (statusWrap) statusWrap.classList.remove('d-none');
          const selectedLabel = String(existingInstanceEl && existingInstanceEl.selectedOptions && existingInstanceEl.selectedOptions[0] && existingInstanceEl.selectedOptions[0].textContent || '').trim();
          if (migState.instanceId) {
            statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
            statusEl.textContent = 'Verwendet bestehende Instanz: ' + String(selectedLabel || migState.instanceId);
          } else {
            statusEl.className = 'small text-danger border rounded-3 px-3 py-2 bg-danger-subtle';
            statusEl.textContent = 'Keine bestehende Instanz ausgewählt.';
          }
          return;
        }

        const login = migState.salesforceLogin;
        const authType = String(authTypeEl && authTypeEl.value || login && login.authType || 'password');
        const isPasswordMode = authType === 'password';
        const isClientCredentialsMode = authType === 'client_credentials';
        if (usernameWrap) usernameWrap.classList.toggle('d-none', !isPasswordMode);
        if (passwordWrap) passwordWrap.classList.toggle('d-none', !isPasswordMode);
        if (securityTokenWrap) securityTokenWrap.classList.toggle('d-none', !isPasswordMode);
        if (clientIdWrap) clientIdWrap.classList.toggle('d-none', !isClientCredentialsMode);
        if (clientSecretWrap) clientSecretWrap.classList.toggle('d-none', !isClientCredentialsMode);
        if (!login) {
          statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
          statusEl.textContent = isPasswordMode
            ? 'Noch keine Salesforce-Zugangsdaten hinterlegt.'
            : (isClientCredentialsMode
              ? 'Noch keine Client-ID und kein Client-Secret hinterlegt.'
              : 'Noch keine Salesforce-Freigabe vorhanden.');
          if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login testen' : 'Mit Salesforce verbinden';
          return;
        }

        if (String(login.lastConnectionStatus || 'never') === 'connected') {
          const orgName = login.orgOverview && (login.orgOverview.organizationName || login.orgOverview.organizationId || login.orgOverview.instanceUrl) || login.instanceUrl || login.loginUrl;
          statusEl.className = 'small text-success border rounded-3 px-3 py-2 bg-success-subtle';
          statusEl.textContent = 'Verbunden mit ' + String(orgName || 'Salesforce') + (login.lastConnectedAt ? ' am ' + formatDate(login.lastConnectedAt, 'short') : '') + '.';
          if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login erneut testen' : 'Erneut mit Salesforce verbinden';
          return;
        }

        if (String(login.lastConnectionStatus || 'never') === 'error' && login.lastConnectionError) {
          statusEl.className = 'small text-danger border rounded-3 px-3 py-2 bg-danger-subtle';
          statusEl.textContent = 'Letzter Loginfehler: ' + String(login.lastConnectionError || 'Unbekannter Fehler');
          if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login erneut testen' : 'Salesforce Login erneut starten';
          return;
        }

        statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
        statusEl.textContent = isPasswordMode
          ? 'Noch nicht verbunden. Hinterlege Benutzername und Passwort und teste dann den Login.'
          : (isClientCredentialsMode
            ? 'Noch nicht verbunden. Hinterlege Client ID und Client Secret und teste dann den Login.'
            : 'Noch nicht verbunden. Der Login erfolgt ueber die Salesforce-Login-Seite mit anschliessendem Allow.');
        if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login testen' : 'Mit Salesforce verbinden';
      }

      function syncMigSalesforceLoginFromForm() {
        const instanceSourceEl = document.getElementById('mig-instance-source');
        const existingInstanceEl = document.getElementById('mig-existing-instance');
        const environmentEl = document.getElementById('mig-login-environment');
        const authTypeEl = document.getElementById('mig-login-auth-type');
        const loginUrlEl = document.getElementById('mig-login-url');
        const usernameEl = document.getElementById('mig-login-username');
        const passwordEl = document.getElementById('mig-login-password');
        const securityTokenEl = document.getElementById('mig-login-security-token');
        const clientIdEl = document.getElementById('mig-login-client-id');
        const clientSecretEl = document.getElementById('mig-login-client-secret');
        const instanceSource = String(instanceSourceEl && instanceSourceEl.value || (migState.salesforceLogin ? 'embedded' : 'existing'));
        if (instanceSource === 'existing') {
          migState.instanceId = String(existingInstanceEl && existingInstanceEl.value || migState.instanceId || state.instanceId || '').trim();
          migState.salesforceLogin = null;
          renderMigSalesforceLoginStatus();
          return;
        }
        migState.instanceId = '';
        const environment = String(environmentEl && environmentEl.value || 'sandbox') === 'production' ? 'production' : 'sandbox';
        const rawAuthType = String(authTypeEl && authTypeEl.value || migState.salesforceLogin && migState.salesforceLogin.authType || 'password');
        const authType = rawAuthType === 'oauth_refresh_token'
          ? 'oauth_refresh_token'
          : (rawAuthType === 'client_credentials' ? 'client_credentials' : 'password');
        const defaultLoginUrl = getMigLoginUrlForEnvironment(environment);
        const previousEnvironment = String(migState.salesforceLogin && migState.salesforceLogin.environment || environment) === 'production' ? 'production' : 'sandbox';
        const previousDefaultLoginUrl = getMigLoginUrlForEnvironment(previousEnvironment);
        const currentLoginUrlValue = String(loginUrlEl && loginUrlEl.value || migState.salesforceLogin && migState.salesforceLogin.loginUrl || '').trim();
        const loginUrl = currentLoginUrlValue && currentLoginUrlValue !== previousDefaultLoginUrl
          ? currentLoginUrlValue
          : defaultLoginUrl;
        if (loginUrlEl) {
          loginUrlEl.value = loginUrl;
        }
        const username = String(usernameEl && usernameEl.value || '').trim();
        const password = String(passwordEl && passwordEl.value || '');
        const securityToken = String(securityTokenEl && securityTokenEl.value || '').trim();
        const clientId = String(clientIdEl && clientIdEl.value || '').trim();
        const clientSecret = String(clientSecretEl && clientSecretEl.value || '').trim();
        const previousAuthType = String(migState.salesforceLogin && migState.salesforceLogin.authType || '');
        const authTypeChanged = previousAuthType && previousAuthType !== authType;
        migState.salesforceLogin = {
          id: migState.id || '',
          name: (migState.name || '').trim(),
          environment,
          loginUrl,
          authType,
          username: authType === 'password' ? username : undefined,
          password: authType === 'password' ? password : undefined,
          securityToken: authType === 'password' ? (securityToken || undefined) : undefined,
          clientId: authType === 'client_credentials' ? (clientId || undefined) : undefined,
          clientSecret: authType === 'client_credentials' ? (clientSecret || undefined) : undefined,
          instanceUrl: migState.salesforceLogin && migState.salesforceLogin.instanceUrl,
          queryLimit: undefined,
          lastConnectionStatus: authTypeChanged ? 'never' : (migState.salesforceLogin && migState.salesforceLogin.lastConnectionStatus || 'never'),
          lastConnectedAt: authTypeChanged ? undefined : (migState.salesforceLogin && migState.salesforceLogin.lastConnectedAt),
          lastConnectionError: authTypeChanged ? undefined : (migState.salesforceLogin && migState.salesforceLogin.lastConnectionError),
          orgOverview: migState.salesforceLogin && migState.salesforceLogin.orgOverview,
          objectCount: migState.salesforceLogin && migState.salesforceLogin.objectCount
        };
        renderMigSalesforceLoginStatus();
      }

      async function ensureMigRuntimeInstanceId() {
        syncMigSalesforceLoginFromForm();
        if (migState.salesforceLogin) {
          if (!migState.id) {
            await migSave();
          }
          const isPasswordMode = String(migState.salesforceLogin.authType || 'password') === 'password';
          const isClientCredentialsMode = String(migState.salesforceLogin.authType || 'password') === 'client_credentials';
          if (isPasswordMode) {
            if (!String(migState.salesforceLogin.username || '').trim() || !String(migState.salesforceLogin.password || '')) {
              throw new Error('Bitte zuerst Benutzername und Passwort fuer diese Migration hinterlegen.');
            }
            return migState.id ? 'migration:' + migState.id : '';
          }
          if (isClientCredentialsMode) {
            if (!String(migState.salesforceLogin.clientId || '').trim() || !String(migState.salesforceLogin.clientSecret || '').trim()) {
              throw new Error('Bitte zuerst Client ID und Client Secret fuer diese Migration hinterlegen.');
            }
            return migState.id ? 'migration:' + migState.id : '';
          }
          if (!migState.salesforceLogin.instanceUrl && String(migState.salesforceLogin.lastConnectionStatus || 'never') !== 'connected') {
            throw new Error('Bitte zuerst den Salesforce-Login fuer diese Migration ueber die Login-Seite abschliessen.');
          }
          return migState.id ? 'migration:' + migState.id : '';
        }
        return String(migState.instanceId || state.instanceId || '').trim();
      }

      async function startMigrationSalesforceOAuth(migrationId, options) {
        const targetMigrationId = String(migrationId || '').trim();
        if (!targetMigrationId) {
          throw new Error('Migration muss erst gespeichert werden, bevor der Salesforce-Login gestartet werden kann.');
        }

        const startUrl = withInstance('/auth/migration-salesforce/start?migrationId=' + encodeURIComponent(targetMigrationId));
        window.location.assign(startUrl);
      }

      function getCsrfToken() {
        return String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || '').trim();
      }

      window.fetch = (input, options = {}) => {
        const request = input instanceof Request ? input : null;
        const method = String(options.method || (request ? request.method : 'GET')).toUpperCase();
        if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
          return nativeFetch(input, options);
        }

        const headers = new Headers(request ? request.headers : undefined);
        const optionHeaders = new Headers(options.headers || {});
        optionHeaders.forEach((value, key) => headers.set(key, value));
        const csrfToken = getCsrfToken();
        if (csrfToken && !headers.has('X-CSRF-Token')) {
          headers.set('X-CSRF-Token', csrfToken);
        }

        return nativeFetch(input, {
          ...options,
          headers
        });
      };

      function getMigImportDisplayName(fileName) {
        return String(fileName || '')
          .replace(/\.[^.]+$/, '')
          .replace(/[._-]+/g, ' ')
          .replace(/\s+/g, ' ')
          .trim();
      }

      function isMigPendingImportSelected(item) {
        return !!item && item.includeInMigration !== false;
      }

      function getMigSelectedPendingImports(imports) {
        return (Array.isArray(imports) ? imports : []).filter((item) => isMigPendingImportSelected(item));
      }

      function getMigPendingImportLabel(item) {
        const fileLabel = String(item && (item.fileName || (item.file && item.file.name) || item.sourceFileName) || 'Datei').trim() || 'Datei';
        const sheetName = String(item && (item.sheetName || (item.analysis && item.analysis.sheetName)) || '').trim();
        return sheetName ? (fileLabel + ' / ' + sheetName) : fileLabel;
      }

      function isSupportedMigrationImportFile(file) {
        const fileName = String(file && file.name ? file.name : '').toLowerCase();
        return ['.csv', '.txt', '.json', '.xlsx', '.xls'].some((extension) => fileName.endsWith(extension));
      }

      function getMigObjectDisplayName(obj) {
        const baseName = String((obj && (obj.salesforceObjectLabel || obj.salesforceObject)) || 'Objekt');
        const sameObjects = (migState.objects || []).filter((item) => item && item.salesforceObject === obj.salesforceObject);
        if (sameObjects.length <= 1) {
          return baseName;
        }

        const index = sameObjects.findIndex((item) => item.id === obj.id);
        return index >= 0 ? (baseName + ' #' + (index + 1)) : baseName;
      }

      function countMigUnassignedObjectsByApiName(objectApiName) {
        return (migState.objects || []).filter((obj) =>
          obj && obj.salesforceObject === objectApiName && !String(obj.filePath || '').trim()
        ).length;
      }

      function resolveMigTargetFieldApiName(fieldName, availableFields) {
        const rawName = String(fieldName || '').trim();
        if (!rawName) {
          return '';
        }

        const fieldNames = (Array.isArray(availableFields) ? availableFields : [])
          .map((entry) => typeof entry === 'string' ? entry : String(entry && entry.name ? entry.name : ''))
          .map((entry) => entry.trim())
          .filter(Boolean);
        const namesByLower = new Map(fieldNames.map((entry) => [entry.toLowerCase(), entry]));
        const exactMatch = namesByLower.get(rawName.toLowerCase());
        if (exactMatch) {
          return exactMatch;
        }

        if (!rawName.toLowerCase().endsWith('__c')) {
          const customFieldMatch = namesByLower.get((rawName + '__c').toLowerCase());
          if (customFieldMatch) {
            return customFieldMatch;
          }
        }

        return rawName;
      }

      function isMigMappingTargetFieldVisible(field, selectedValue) {
        const name = String(field?.name || '').trim();
        if (!name) {
          return false;
        }
        if (selectedValue && normalizeFieldKey(name) === normalizeFieldKey(selectedValue)) {
          return true;
        }

        if (field?.createable === true || field?.updateable === true || field?.isExternalId === true) {
          return true;
        }

        const lowerName = name.toLowerCase();
        return ![
          'id',
          'createddate',
          'createdbyid',
          'lastmodifieddate',
          'lastmodifiedbyid',
          'systemmodstamp',
          'lastvieweddate',
          'lastreferenceddate',
          'isdeleted'
        ].includes(lowerName);
      }

      function getMigMappingTargetOptions(availableFields, selectedValue) {
        const selected = String(selectedValue || '').trim();
        const fields = Array.isArray(availableFields) ? availableFields : [];
        const visibleFields = fields.filter((field) => isMigMappingTargetFieldVisible(field, selected));
        const hasSelected = selected && visibleFields.some((field) => String(field?.name || '').trim() === selected);

        return '<option value=""' + (!selected ? ' selected' : '') + '>Zielfeld wählen</option>' +
          visibleFields.map((field) => {
            const name = String(field?.name || '').trim();
            const label = String(field?.label || '').trim();
            const display = label && label !== name ? label + ' - ' + name : name;
            const meta = [
              field?.requiredOnCreate === true ? 'Pflicht' : '',
              field?.isExternalId === true ? 'External ID' : ''
            ].filter(Boolean).join(', ');
            return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(display + (meta ? ' (' + meta + ')' : '')) + '</option>';
          }).join('') +
          '<option value="__manual__"' + (!hasSelected && selected ? ' selected' : '') + '>Manuell eingeben…</option>';
      }

      async function autoPopulateMigFieldMappings(obj, sfFields) {
        if (!obj || !Array.isArray(obj.fileColumns) || !obj.fileColumns.length) {
          return 0;
        }

        if (!obj.fieldMappings) {
          obj.fieldMappings = [];
        }

        const generatedMappings = await generateSalesforceMappings(
          obj.fileColumns.map((column) => ({ name: String(column || '').trim(), type: 'string' })),
          Array.isArray(sfFields) ? sfFields : [],
          {
            targetObjectApiName: obj.salesforceObject,
            profile: getMigrationMappingAssistantProfile(obj.id, obj.salesforceObject)
          }
        );

        let added = 0;
        generatedMappings.forEach((generated) => {
          const sourceName = String(generated?.sourceField || '').trim();
          if (!sourceName) {
            return;
          }

          const existing = obj.fieldMappings.find((mapping) => String(mapping?.sourceColumn || '').trim() === sourceName);
          if (existing && String(existing.targetField || '').trim()) {
            return;
          }

          const nextEntry = {
            ...(existing || {}),
            sourceColumn: sourceName,
            targetField: String(generated.targetField || ''),
            targetFieldLabel: String(generated.targetFieldLabel || generated.targetField || ''),
            targetFieldType: String(generated.targetFieldType || ''),
            targetType: generated.targetType || existing?.targetType,
            transformFunction: String(existing?.transformFunction || generated.transformFunction || 'NONE'),
            transformExpression: String(existing?.transformExpression || generated.transformExpression || ''),
            lookupEnabled: existing?.lookupEnabled === true ? true : generated.lookupEnabled === true,
            lookupObject: String(existing?.lookupObject || generated.lookupObject || ''),
            lookupField: String(existing?.lookupField || generated.lookupField || ''),
            picklistMappings: Array.isArray(existing?.picklistMappings) ? existing.picklistMappings : (Array.isArray(generated.picklistMappings) ? generated.picklistMappings : [])
          };

          if (existing) {
            Object.assign(existing, nextEntry);
          } else {
            obj.fieldMappings.push(nextEntry);
          }
          added += 1;
        });

        return added;
      }

      function autoSelectMigExternalIdField(obj, sfFields) {
        if (!obj || obj.operation !== 'upsert') {
          return false;
        }

        const mappedExternalIdFields = (Array.isArray(obj.fieldMappings) ? obj.fieldMappings : [])
          .map((mapping) => String(mapping?.targetField || '').trim())
          .filter(Boolean)
          .map((targetField) => resolveMigTargetFieldApiName(targetField, sfFields))
          .filter(Boolean)
          .filter((targetField, index, entries) => entries.indexOf(targetField) === index)
          .filter((targetField) => (Array.isArray(sfFields) ? sfFields : []).some((field) =>
            String(field?.name || '').trim().toLowerCase() === targetField.toLowerCase() && field?.isExternalId === true
          ));

        if (mappedExternalIdFields.length === 1) {
          const nextExternalIdField = mappedExternalIdFields[0];
          if (String(obj.externalIdField || '').trim() !== nextExternalIdField) {
            obj.externalIdField = nextExternalIdField;
            return true;
          }
          return false;
        }

        if (mappedExternalIdFields.length === 0 && String(obj.externalIdField || '').trim()) {
          obj.externalIdField = '';
          return true;
        }

        return false;
      }

      function sanitizeMigFieldMappings(fieldMappings) {
        return (Array.isArray(fieldMappings) ? fieldMappings : []).map((mapping) => {
          const normalizedMapping = mapping ? JSON.parse(JSON.stringify(mapping)) : {};
          delete normalizedMapping._isMissing;
          return normalizedMapping;
        });
      }

      function sanitizeMigObjects(objects) {
        return (Array.isArray(objects) ? objects : []).map((obj) => {
          const normalizedObject = obj ? JSON.parse(JSON.stringify(obj)) : {};
          delete normalizedObject.failedPreviewRecords;
          delete normalizedObject.failedPreviewLoadedFor;
          normalizedObject.fieldMappings = sanitizeMigFieldMappings(normalizedObject.fieldMappings);
          return normalizedObject;
        });
      }

      async function loadMigExternalIdOptions(obj) {
        if (!obj || !obj.salesforceObject) {
          return [];
        }

        if (Array.isArray(obj._externalIdFields) && obj._externalIdFields.length) {
          return obj._externalIdFields;
        }

        try {
          const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
          if (!res.ok) {
            obj._externalIdFields = [];
            return [];
          }
          const fields = await res.json();
          obj._externalIdFields = (Array.isArray(fields) ? fields : []).filter((field) => field && field.isExternalId === true);
          return obj._externalIdFields;
        } catch {
          obj._externalIdFields = [];
          return [];
        }
      }

      function collectMigMissingFieldMappings() {
        const missing = [];
        for (const obj of migState.objects) {
          const existingFieldNames = Array.from(new Set([
            ...(obj._existingFieldNames || [])
          ].map((name) => String(name).trim()).filter(Boolean)));
          const existingFieldNamesSet = new Set(existingFieldNames.map((name) => name.toLowerCase()));
          for (const mapping of (obj.fieldMappings || [])) {
            const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, existingFieldNames);
            if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
              mapping.targetField = resolvedTargetField;
            }
            const isMissing = !!resolvedTargetField && !existingFieldNamesSet.has(resolvedTargetField.toLowerCase());
            mapping._isMissing = isMissing;
            if (isMissing) {
              missing.push({ obj, mapping });
            }
          }
        }
        return missing;
      }

      async function collectMigMissingFieldMappingsLive() {
        const missing = [];
        for (const obj of migState.objects) {
          const fields = await requestJson('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject));
          const existingFieldNames = Array.from(new Set((Array.isArray(fields) ? fields : [])
            .map((field) => String(field && field.name ? field.name : '').trim())
            .filter(Boolean)
            .map((name) => name.toLowerCase())));
          const existingFieldNamesSet = new Set(existingFieldNames);
          obj._existingFieldNames = existingFieldNames.slice();

          for (const mapping of (obj.fieldMappings || [])) {
            const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, existingFieldNames);
            if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
              mapping.targetField = resolvedTargetField;
            }
            const isMissing = !!resolvedTargetField && !existingFieldNamesSet.has(resolvedTargetField.toLowerCase());
            mapping._isMissing = isMissing;
            if (isMissing) {
              missing.push({ obj, mapping });
            }
          }
        }
        return missing;
      }

      function inferMigFieldCreationType(mapping) {
        const explicitPicklistValues = Array.isArray(mapping?.picklistValues) ? mapping.picklistValues.filter(Boolean) : [];
        if (explicitPicklistValues.length) {
          return 'Picklist';
        }

        const normalizedType = String(mapping?.targetFieldType || '').trim().toLowerCase();
        if (normalizedType === 'url') return 'Url';
        if (normalizedType === 'date') return 'Date';
        if (normalizedType === 'datetime') return 'DateTime';
        if (normalizedType === 'boolean') return 'Checkbox';
        if (normalizedType === 'email') return 'Email';
        if (normalizedType === 'phone') return 'Phone';
        if (normalizedType === 'currency') return 'Currency';
        if (normalizedType === 'percent') return 'Percent';
        if (normalizedType === 'double' || normalizedType === 'int' || normalizedType === 'integer' || normalizedType === 'number') return 'Number';

        const targetFieldName = String(mapping?.targetField || '').trim().toLowerCase();
        if (targetFieldName.includes('currency')) return 'Currency';
        if (targetFieldName.includes('percent')) return 'Percent';
        if (targetFieldName.includes('email')) return 'Email';
        if (targetFieldName.includes('phone') || targetFieldName.includes('mobile')) return 'Phone';
        if (targetFieldName.includes('date')) return 'Date';
        if (targetFieldName.includes('url') || targetFieldName.includes('website')) return 'Url';

        return 'Text';
      }

      async function createMigMissingField(obj, mapping, fieldType, picklistValues) {
        const result = await requestJson('/api/salesforce/create-field', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            objectApiName: obj.salesforceObject,
            fieldApiName: mapping.targetField,
            fieldType,
            picklistValues,
            instanceId: state.instanceId
          })
        });

        const fullFieldName = String(result && result.fullName ? result.fullName : '').split('.').pop()
          || (String(mapping.targetField).endsWith('__c') ? String(mapping.targetField) : String(mapping.targetField) + '__c');

        obj.confirmedSalesforceFields = Array.from(new Set([...(obj.confirmedSalesforceFields || []), fullFieldName]));
        obj._existingFieldNames = Array.from(new Set([...(obj._existingFieldNames || []), String(fullFieldName).toLowerCase()]));
        mapping.targetField = fullFieldName;
        mapping.targetFieldLabel = fullFieldName;
        mapping._isMissing = false;

        return { fullFieldName, result };
      }

      async function autoCreateMigMissingFields() {
        const missing = await collectMigMissingFieldMappingsLive();
        if (!missing.length) {
          return [];
        }

        const createdFields = [];
        for (const item of missing) {
          const fieldType = inferMigFieldCreationType(item.mapping);
          const picklistValues = fieldType === 'Picklist'
            ? (Array.isArray(item.mapping.picklistValues) ? item.mapping.picklistValues.map((value) => String(value || '').trim()).filter(Boolean) : [])
            : [];

          if (fieldType === 'Picklist' && !picklistValues.length) {
            throw new Error('Feld ' + item.mapping.targetField + ' kann nicht automatisch als Picklist angelegt werden, weil keine Werte hinterlegt sind.');
          }

          const created = await createMigMissingField(item.obj, item.mapping, fieldType, picklistValues);
          createdFields.push({
            objectApiName: item.obj.salesforceObject,
            fieldName: created.fullFieldName,
            action: created.result && created.result.action ? created.result.action : 'created'
          });
        }

        return createdFields;
      }

      function resetMigTransientUi() {
        stopMigRunProgressPolling();
        const progressEl = document.getElementById('mig-run-progress');
        const resultEl = document.getElementById('mig-run-result');
        const stepsEl = document.getElementById('mig-run-steps');
        const createFieldsResultEl = document.getElementById('mig-create-fields-result');
        const progressTitleEl = document.getElementById('mig-run-status-title');
        const spinnerEl = document.getElementById('mig-run-status-spinner');

        if (progressEl) {
          progressEl.classList.add('d-none');
        }
        if (spinnerEl) {
          spinnerEl.classList.remove('d-none');
        }
        if (progressTitleEl) {
          progressTitleEl.textContent = 'Migration läuft...';
        }
        if (resultEl) {
          resultEl.classList.add('d-none');
          resultEl.innerHTML = '';
        }
        if (stepsEl) {
          stepsEl.innerHTML = '';
        }
        if (createFieldsResultEl) {
          createFieldsResultEl.innerHTML = '';
        }
      }

      function stopMigRunProgressPolling() {
        if (migState.progressPollTimer) {
          clearTimeout(migState.progressPollTimer);
          migState.progressPollTimer = null;
        }
      }

      function getMigOrderedObjects() {
        const ordered = [...(migState.executionPlan || [])]
          .sort((a, b) => a.order - b.order)
          .map((step) => (migState.objects || []).find((obj) => obj.id === step.objectId))
          .filter(Boolean);

        (migState.objects || []).forEach((obj) => {
          if (!ordered.find((entry) => entry.id === obj.id)) {
            ordered.push(obj);
          }
        });

        return ordered;
      }

      async function pollMigRunProgress() {
        if (!migState.id) return;
        stopMigRunProgressPolling();
        const refresh = async () => {
          try {
            const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id));
            if (res.ok) {
              const migration = await res.json();
              migState.status = String(migration?.status || migState.status || 'draft');
              migState.lastRunResult = migration?.lastRunResult || migState.lastRunResult;
              if (migState.step === migState.totalSteps) {
                renderMigRunProgress();
                renderMigRunResult();
              }
            }
          } catch { /* ignore polling errors */ }

          if (migState.status === 'running') {
            migState.progressPollTimer = setTimeout(refresh, 1000);
          } else {
            stopMigRunProgressPolling();
          }
        };
        await refresh();
      }

      function getMigPendingRecommendationCounts(imports) {
        return getMigSelectedPendingImports(imports).reduce((acc, item) => {
          const key = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
          if (!key) {
            return acc;
          }

          acc[key] = (acc[key] || 0) + 1;
          return acc;
        }, {});
      }

      function createMigObject(name, label, options) {
        const allowDuplicate = !!(options && options.allowDuplicate);
        if (!allowDuplicate) {
          const existing = (migState.objects || []).find((obj) => obj && obj.salesforceObject === name);
          if (existing) {
            return existing;
          }
        }

        const migrationObject = {
          id: migUuidV4(),
          salesforceObject: name,
          salesforceObjectLabel: label || name,
          processingMode: 'sqlite',
          filePath: '',
          fileSheetName: '',
          availableSheetNames: [],
          fileColumns: [],
          fieldMappings: [],
          operation: 'insert'
        };
        migState.objects.push(migrationObject);
        return migrationObject;
      }

      function ensureMigObjectsForPendingImports(imports) {
        const groupedImports = (Array.isArray(imports) ? imports : []).reduce((acc, item) => {
          const objectApiName = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
          if (!objectApiName) {
            return acc;
          }

          if (!acc[objectApiName]) {
            acc[objectApiName] = {
              label: item.recommendedObjectLabel || objectApiName,
              items: []
            };
          }
          acc[objectApiName].items.push(item);
          return acc;
        }, {});

        let createdCount = 0;
        Object.keys(groupedImports).forEach((objectApiName) => {
          const group = groupedImports[objectApiName];
          const availableCount = countMigUnassignedObjectsByApiName(objectApiName);
          const missingCount = Math.max(0, group.items.length - availableCount);
          for (let index = 0; index < missingCount; index += 1) {
            createMigObject(objectApiName, group.label, { allowDuplicate: true });
            createdCount += 1;
          }
        });

        return createdCount;
      }

      function getPendingMigrationImportText() {
        const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
        const selectedImports = getMigSelectedPendingImports(pendingImports);
        if (!selectedImports.length) {
          return '';
        }

        const labels = selectedImports.map((item) => getMigPendingImportLabel(item)).join(', ');
        if (selectedImports.length === 1) {
          return 'Import-Datei vorgemerkt: ' + labels + '. Wähle genau ein Salesforce-Objekt aus, dann wird die Datei automatisch übernommen.';
        }

        return selectedImports.length + ' Importquellen vorgemerkt: ' + labels + '. Bitte nacheinander je Objekt zuordnen.';
      }

      function renderMigPendingImportHint() {
        const stepOneHint = document.getElementById('mig-pending-import-hint');
        const stepTwoHint = document.getElementById('mig-file-import-hint');
        const pendingText = getPendingMigrationImportText();

        if (stepOneHint) {
          stepOneHint.textContent = pendingText;
          stepOneHint.classList.toggle('d-none', !pendingText);
        }

        if (stepTwoHint) {
          stepTwoHint.textContent = pendingText;
          stepTwoHint.classList.toggle('d-none', !pendingText);
        }
      }

      function renderMigImportSuggestions() {
        const container = document.getElementById('mig-import-suggestions');
        if (!container) {
          return;
        }

        const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
        const selectedImports = getMigSelectedPendingImports(pendingImports);
        const recommendationCounts = getMigPendingRecommendationCounts(pendingImports);
        const recommendedImports = selectedImports.filter((item) => String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim());
        const needsAdditionalObjects = Object.keys(recommendationCounts).some((objectApiName) => recommendationCounts[objectApiName] > countMigUnassignedObjectsByApiName(objectApiName));
        if (!pendingImports.length) {
          container.classList.add('d-none');
          container.innerHTML = '';
          return;
        }
        container.classList.remove('d-none');
        container.innerHTML = '<div class="small fw-semibold mb-2">Objektvorschläge aus den Importdateien</div>' +
          (pendingImports.length !== selectedImports.length
            ? '<div class="small text-secondary mb-2">' + esc(String(selectedImports.length)) + ' von ' + esc(String(pendingImports.length)) + ' Importquellen sind aktuell ausgewählt.</div>'
            : '') +
          (recommendedImports.length
            ? '<div class="d-flex flex-wrap gap-2 mb-2">' +
                '<button type="button" class="btn btn-sm btn-primary" data-mig-create-all-suggested>Empfohlene Objekte gesammelt anlegen</button>' +
                (needsAdditionalObjects
                  ? '<span class="small text-secondary align-self-center">Mehrere Dateien zeigen teils auf dasselbe Zielobjekt. Die Sammelanlage legt dafür getrennte Import-Slots an.</span>'
                  : '<span class="small text-secondary align-self-center">Passende Zielobjekte können direkt gesammelt angelegt und zugeordnet werden.</span>') +
              '</div>'
            : '') +
          pendingImports.map((item) => {
            const analysis = item && item.analysis && typeof item.analysis === 'object' ? item.analysis : null;
            const suggestions = Array.isArray(item && item.suggestions) ? item.suggestions : [];
            const recommendedName = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
            const isSelected = isMigPendingImportSelected(item);
            const duplicateRecommendationCount = recommendedName ? (recommendationCounts[recommendedName] || 0) : 0;
            const summaryBits = [];
            if (analysis && analysis.format) summaryBits.push('Format: ' + String(analysis.format).toUpperCase());
            if (analysis && analysis.sheetName) summaryBits.push('Mappe: ' + analysis.sheetName);
            if (analysis && typeof analysis.recordCount === 'number') summaryBits.push('Datensaetze: ' + analysis.recordCount);
            if (analysis && Array.isArray(analysis.headers) && analysis.headers.length) {
              summaryBits.push('Felder: ' + analysis.headers.slice(0, 6).join(', ') + (analysis.headers.length > 6 ? ' …' : ''));
            }

            return '<div class="alert ' + (isSelected ? 'alert-light' : 'alert-secondary') + ' border py-2 mb-2">' +
              '<div class="d-flex justify-content-between align-items-start gap-3 mb-1">' +
                '<div class="small fw-semibold">' + esc(getMigPendingImportLabel(item)) + '</div>' +
                '<div class="form-check form-switch m-0">' +
                  '<input class="form-check-input" type="checkbox" role="switch" data-mig-import-toggle="' + esc(item.id || item.fileName || '') + '"' + (isSelected ? ' checked' : '') + '>' +
                '</div>' +
              '</div>' +
              (summaryBits.length ? '<div class="small text-secondary mb-2">' + esc(summaryBits.join(' | ')) + '</div>' : '') +
              (!isSelected
                ? '<div class="small text-secondary">Diese Mappe wird aktuell nicht in den Migrationsentwurf übernommen.</div>'
                : recommendedName
                ? '<div class="d-flex flex-wrap align-items-center gap-2 mb-2">' +
                    '<span class="badge text-bg-primary">Empfohlen: ' + esc((item.recommendedObjectLabel || recommendedName) + ' (' + recommendedName + ')') + '</span>' +
                    '<button type="button" class="btn btn-sm btn-outline-primary" data-mig-create-import="' + esc(item.id || item.fileName || '') + '">Objekt anlegen</button>' +
                    (duplicateRecommendationCount > 1
                      ? '<span class="small text-secondary">Diese Empfehlung tritt in ' + esc(String(duplicateRecommendationCount)) + ' Importquellen auf.</span>'
                      : '') +
                  '</div>'
                : '') +
              (isSelected && suggestions.length
                ? '<div class="d-flex flex-wrap gap-2">' +
                    suggestions.map((suggestion) =>
                      '<button type="button" class="btn btn-sm btn-outline-primary" data-mig-suggestion-import="' + esc(item.id || item.fileName || '') + '" data-mig-suggestion="' + esc(suggestion.objectApiName) + '" data-mig-suggestion-label="' + esc(suggestion.label || suggestion.objectApiName) + '">' +
                      esc((suggestion.label || suggestion.objectApiName) + ' (' + suggestion.objectApiName + ')') +
                      '</button>'
                    ).join('') +
                  '</div>' +
                  '<div class="small text-secondary mt-2">' +
                    suggestions.map((suggestion) => esc((suggestion.label || suggestion.objectApiName) + ': ' + (suggestion.reason || 'Heuristik'))).join(' | ') +
                  '</div>'
                : (isSelected ? '<div class="small text-secondary">Keine eindeutige Objektempfehlung gefunden.</div>' : '')) +
            '</div>';
          }).join('');

        container.querySelectorAll('[data-mig-import-toggle]').forEach((input) => {
          input.addEventListener('change', () => {
            const importId = input.getAttribute('data-mig-import-toggle');
            const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
            if (!pendingImport) {
              return;
            }

            pendingImport.includeInMigration = !!input.checked;
            renderMigPendingImportHint();
            renderMigImportSuggestions();
          });
        });

        container.querySelectorAll('[data-mig-create-all-suggested]').forEach((button) => {
          button.addEventListener('click', () => {
            const createdCount = ensureMigObjectsForPendingImports(recommendedImports);
            renderMigSelectedObjects();
            if (createdCount > 0) {
              showToast(createdCount + ' Zielobjekte für vorgemerkte Importdateien angelegt.');
            }
            consumePendingMigrationImportIfPossible().catch((error) => {
              alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
            });
          });
        });

        container.querySelectorAll('[data-mig-create-import]').forEach((button) => {
          button.addEventListener('click', () => {
            const importId = button.getAttribute('data-mig-create-import');
            const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
            if (!pendingImport || !pendingImport.recommendedObjectApiName) {
              return;
            }

            ensureMigObjectsForPendingImports([pendingImport]);
            renderMigSelectedObjects();
            consumePendingMigrationImportIfPossible().catch((error) => {
              alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
            });
          });
        });

        container.querySelectorAll('[data-mig-suggestion]').forEach((button) => {
          button.addEventListener('click', () => {
            const name = button.getAttribute('data-mig-suggestion');
            const label = button.getAttribute('data-mig-suggestion-label') || name;
            const importId = button.getAttribute('data-mig-suggestion-import');
            if (!name) {
              return;
            }

            const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
            if (pendingImport) {
              pendingImport.recommendedObjectApiName = name;
              pendingImport.recommendedObjectLabel = label;
              ensureMigObjectsForPendingImports([pendingImport]);
            }

            renderMigSelectedObjects();
            consumePendingMigrationImportIfPossible().catch((error) => {
              alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
            });
          });
        });
      }

      function renderMigFileSummary(obj) {
        const details = [];
        if (obj.fileFormat) details.push('Format: ' + obj.fileFormat.toUpperCase());
        if (obj.fileSheetName) details.push('Mappe: ' + obj.fileSheetName);
        if (typeof obj.fileRecordCount === 'number') details.push('Datensaetze: ' + obj.fileRecordCount);
        if (obj.fileCharset) details.push('Charset: ' + obj.fileCharset);
        if (obj.fileDelimiter) details.push('Trennzeichen: ' + (obj.fileDelimiter === '\t' ? 'TAB' : obj.fileDelimiter));
        if (obj.fileTextQualifier) details.push('Textqualifier: ' + obj.fileTextQualifier);
        if (obj.processingMode) details.push('Verarbeitung: ' + (obj.processingMode === 'sqlite' ? 'SQLite-Staging' : 'Datei direkt'));
        if (obj.stagingMode) details.push('Staging: ' + String(obj.stagingMode).toUpperCase());
        if (obj.stagingStatus) details.push('Staging-Status: ' + formatMigStageStatus(obj.stagingStatus));
        if (obj.stagingDatabasePath) details.push('SQLite: ' + obj.stagingDatabasePath);
        if (obj.fileColumns && obj.fileColumns.length) details.push('Spalten: ' + obj.fileColumns.join(', '));
        const statusSummary = renderMigStatusSummaryText(obj);
        if (statusSummary) details.push(statusSummary);
        return details.length ? details.join(' | ') : '';
      }

      function formatMigStageStatus(status) {
        const normalized = String(status || '').toLowerCase();
        if (normalized === 'ready') return 'Bereit';
        if (normalized === 'processing') return 'Verarbeitung';
        if (normalized === 'done') return 'Fertig';
        if (normalized === 'error') return 'Fehler';
        if (normalized === 'success') return 'Erfolgreich';
        if (normalized === 'mapping_error') return 'Mapping-Fehler';
        if (normalized === 'salesforce_error') return 'Salesforce-Fehler';
        if (normalized === 'pending') return 'Offen';
        return status || '-';
      }

      function renderMigStatusSummaryText(obj) {
        const summary = obj && obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : null;
        if (!summary) return '';
        const keys = Object.keys(summary).filter((key) => Number(summary[key] || 0) > 0);
        if (!keys.length) return '';
        return keys.sort().map((key) => formatMigStageStatus(key) + ': ' + summary[key]).join(', ');
      }

      function isMigServerPreview(obj) {
        return (obj?.processingMode || obj?.stagingMode || '') === 'sqlite';
      }

      function getMigLatestFailedStep(objectId) {
        const steps = Array.isArray(migState.lastRunResult && migState.lastRunResult.steps) ? migState.lastRunResult.steps : [];
        return steps.find((step) => step && step.objectId === objectId && step.failedRecordsId) || null;
      }

      async function loadMigLatestFailedPreview(obj) {
        const failedStep = getMigLatestFailedStep(obj.id);
        if (!failedStep || obj.failedPreviewLoadedFor === failedStep.failedRecordsId) return;
        const failedRes = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/failed-records/' + encodeURIComponent(failedStep.failedRecordsId));
        if (!failedRes.ok) throw new Error('Fehler beim Laden der Fehlerdetails');
        const failedData = await failedRes.json();
        obj.failedPreviewRecords = Array.isArray(failedData.records) ? failedData.records.slice(0, 5) : [];
        obj.failedPreviewLoadedFor = failedStep.failedRecordsId;
      }

      function applyMigAnalysisData(obj, data) {
        obj.filePath = data.filePath || obj.filePath || '';
        obj.fileFormat = data.format || obj.fileFormat || 'csv';
        obj.fileSheetName = typeof data.sheetName === 'string' ? data.sheetName : (obj.fileSheetName || '');
        obj.availableSheetNames = Array.isArray(data.availableSheetNames) ? data.availableSheetNames.slice() : (obj.availableSheetNames || []);
        obj.fileCharset = data.charset || obj.fileCharset || 'utf8';
        obj.fileDelimiter = data.delimiter || obj.fileDelimiter || ';';
        obj.fileTextQualifier = data.textQualifier || obj.fileTextQualifier || '"';
        obj.fileRecordCount = typeof data.recordCount === 'number' ? data.recordCount : obj.fileRecordCount;
        obj.fileColumns = data.fields || [];
        obj.previewRows = Array.isArray(data.rows) ? data.rows.slice(0, 10) : [];
        obj.processingMode = data.processingMode || obj.processingMode || 'sqlite';
        obj.stagingMode = data.stagingMode || obj.stagingMode || '';
        obj.stagingDatabasePath = data.stagingDatabasePath || obj.stagingDatabasePath || '';
        obj.stagingImportedAt = data.stagingImportedAt || obj.stagingImportedAt;
        obj.stagingStatus = data.stagingStatus || obj.stagingStatus || '';
        obj.previewOffset = typeof data.previewOffset === 'number' ? data.previewOffset : (obj.previewOffset || 0);
        obj.previewLimit = typeof data.previewLimit === 'number' ? data.previewLimit : (obj.previewLimit || 10);
        obj.filteredRecordCount = typeof data.filteredRecordCount === 'number' ? data.filteredRecordCount : (obj.filteredRecordCount ?? obj.fileRecordCount ?? 0);
        obj.previewFilter = typeof data.previewFilter === 'string' ? data.previewFilter : (obj.previewFilter || '');
        obj.previewStatusFilter = typeof data.previewStatusFilter === 'string' ? data.previewStatusFilter : (obj.previewStatusFilter || '');
        obj.statusSummary = data.statusSummary || obj.statusSummary || {};
      }

      async function uploadMigrationObjectFile(obj, file) {
        if (!obj || !file) {
          return null;
        }

        const contentBase64 = await fileToBase64(file);
        const res = await fetch('/api/migrations/upload-file', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            migrationId: migState.id,
            objectId: obj.id,
            fileName: file.name,
            contentBase64,
            sheetName: obj.fileSheetName,
            charset: obj.fileCharset,
            delimiter: obj.fileDelimiter,
            textQualifier: obj.fileTextQualifier
          })
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(data.error || 'Datei konnte nicht hochgeladen werden');
        }

        applyMigAnalysisData(obj, data);
        return data;
      }

      async function consumePendingMigrationImportIfPossible() {
        const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
        const selectedPendingImports = getMigSelectedPendingImports(pendingImports);
        if (!selectedPendingImports.length || migState.pendingImportInProgress) {
          renderMigPendingImportHint();
          return false;
        }
        migState.pendingImportInProgress = true;

        try {
          await migSave();

          let hasProgress = false;
          const remainingImports = [];

          for (const pendingImport of pendingImports) {
            if (!isMigPendingImportSelected(pendingImport)) {
              remainingImports.push(pendingImport);
              continue;
            }

            const unassignedObjects = (migState.objects || []).filter((obj) => obj && !String(obj.filePath || '').trim());
            let targetObject = null;

            if (pendingImport.recommendedObjectApiName) {
              targetObject = unassignedObjects.find((obj) => obj.salesforceObject === pendingImport.recommendedObjectApiName) || null;
            }

            if (!targetObject && selectedPendingImports.length === 1 && unassignedObjects.length === 1) {
              targetObject = unassignedObjects[0];
            }

            if (!targetObject) {
              remainingImports.push(pendingImport);
              continue;
            }

            targetObject.fileSheetName = pendingImport.sheetName || targetObject.fileSheetName || '';
            await uploadMigrationObjectFile(targetObject, pendingImport.file);
            hasProgress = true;
            showToast('Importquelle ' + getMigPendingImportLabel(pendingImport) + ' wurde dem Objekt ' + targetObject.salesforceObject + ' zugeordnet.');
          }

          if (hasProgress) {
            migState.pendingImports = remainingImports;
            await migSave();
            renderMigSelectedObjects();
            renderMigFileAssignments();
            renderMigMappingObjectSelect();
          }

          return hasProgress;
        } finally {
          migState.pendingImportInProgress = false;
          renderMigPendingImportHint();
        }
      }

      async function loadMigObjectPreview(obj, offset, limit) {
        const previewOffset = Math.max(0, Number(offset || 0) || 0);
        const previewLimit = Math.max(1, Math.min(100, Number(limit || obj.previewLimit || 10) || 10));
        const previewFilter = String(obj.previewFilter || '').trim();
        const previewStatusFilter = String(obj.previewStatusFilter || '').trim();
        const res = await fetch(
          '/api/migrations/' + encodeURIComponent(migState.id) + '/analyze-file/' + encodeURIComponent(obj.id) +
          '?offset=' + encodeURIComponent(String(previewOffset)) +
          '&limit=' + encodeURIComponent(String(previewLimit)) +
          '&filter=' + encodeURIComponent(previewFilter) +
          '&status=' + encodeURIComponent(previewStatusFilter)
        );
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        applyMigAnalysisData(obj, data);
        return data;
      }

      function renderMigPreviewTable(obj) {
        if (!obj.previewRows || !obj.previewRows.length || !obj.fileColumns || !obj.fileColumns.length) {
          return '';
        }

        const previewOffset = Math.max(0, Number(obj.previewOffset || 0) || 0);
        const previewLimit = Math.max(1, Number(obj.previewLimit || 10) || 10);
        const serverPreview = isMigServerPreview(obj);
        const totalRows = Math.max(0, Number(obj.fileRecordCount || 0) || 0);
        const summary = obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : {};
        const summaryKeys = Object.keys(summary).filter((key) => Number(summary[key] || 0) > 0);
        const filterValue = String(obj.previewFilter || '');
        const statusFilterValue = String(obj.previewStatusFilter || '');
        const filteredRows = serverPreview
          ? obj.previewRows
          : String(filterValue).toLowerCase()
          ? obj.previewRows.filter((row) => obj.fileColumns.some((column) => String(row[column] ?? '').toLowerCase().includes(filterValue)))
          : obj.previewRows;
        const filteredTotal = serverPreview
          ? Math.max(0, Number(obj.filteredRecordCount ?? totalRows) || 0)
          : (filterValue ? filteredRows.length : totalRows);
        const fromRow = filteredTotal > 0 ? previewOffset + 1 : 0;
        const toRow = filteredTotal > 0 ? Math.min(previewOffset + filteredRows.length, filteredTotal) : filteredRows.length;
        const failedStep = getMigLatestFailedStep(obj.id);
        const failedPreviewRecords = Array.isArray(obj.failedPreviewRecords) ? obj.failedPreviewRecords : [];
        const previewLabel = serverPreview && (filterValue || statusFilterValue)
          ? 'Vorschau (' + fromRow + ' - ' + toRow + ' von ' + filteredTotal + ' Treffer' + (filteredTotal !== totalRows ? ', ' + totalRows + ' gesamt' : '') + ')'
          : 'Vorschau (' + fromRow + ' - ' + toRow + ' von ' + totalRows + ')';

        return '<div class="small text-secondary mt-2">' +
          '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
            '<span>' + esc(previewLabel) + '</span>' +
            '<span class="d-flex gap-1 flex-wrap">' +
              (summaryKeys.length
                ? summaryKeys.map((key) => '<span class="badge text-bg-light border">' + esc(formatMigStageStatus(key) + ': ' + String(summary[key])) + '</span>').join('')
                : '<span class="badge text-bg-light border">' + esc(formatMigStageStatus(obj.stagingStatus || 'ready')) + '</span>') +
            '</span>' +
          '</div>' +
          '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
            '<span class="small">Seitenlaenge: ' + previewLimit + '</span>' +
            '<input type="search" class="form-control form-control-sm" style="max-width: 240px" placeholder="Vorschau filtern" value="' + esc(obj.previewFilter || '') + '" data-preview-filter="' + esc(obj.id) + '" />' +
            (serverPreview
              ? '<select class="form-select form-select-sm" style="max-width: 220px" data-preview-status-filter="' + esc(obj.id) + '">' +
                  '<option value="">Alle Status</option>' +
                  '<option value="pending"' + (statusFilterValue === 'pending' ? ' selected' : '') + '>Offen</option>' +
                  '<option value="success"' + (statusFilterValue === 'success' ? ' selected' : '') + '>Erfolg</option>' +
                  '<option value="mapping_error"' + (statusFilterValue === 'mapping_error' ? ' selected' : '') + '>Mapping-Fehler</option>' +
                  '<option value="salesforce_error"' + (statusFilterValue === 'salesforce_error' ? ' selected' : '') + '>Salesforce-Fehler</option>' +
                '</select>'
              : '') +
            '<div class="btn-group btn-group-sm">' +
              '<button type="button" class="btn btn-outline-secondary" data-preview-prev="' + esc(obj.id) + '"' + (previewOffset <= 0 ? ' disabled' : '') + '>Zurück</button>' +
              '<button type="button" class="btn btn-outline-secondary" data-preview-next="' + esc(obj.id) + '"' + (previewOffset + previewLimit >= filteredTotal ? ' disabled' : '') + '>Weiter</button>' +
            '</div>' +
          '</div>' +
          '<table class="table table-sm table-bordered"><thead><tr>' +
            obj.fileColumns.map((c) => '<th class="small">' + esc(c) + '</th>').join('') +
          '</tr></thead><tbody>' +
            (filteredRows.length
              ? filteredRows.map((row) => '<tr>' + obj.fileColumns.map((c) => '<td class="small">' + esc(String(row[c] ?? '')) + '</td>').join('') + '</tr>').join('')
              : '<tr><td colspan="' + obj.fileColumns.length + '" class="text-secondary">Keine Datensätze für den aktuellen Filter.</td></tr>') +
          '</tbody></table>' +
          (failedStep
            ? '<div class="mt-3">' +
                '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
                  '<strong>Letzte Fehlerzeilen aus dem letzten Lauf</strong>' +
                  '<span class="badge bg-danger">' + esc(String(failedStep.recordsFailed || failedPreviewRecords.length || 0)) + ' Fehler</span>' +
                '</div>' +
                (failedPreviewRecords.length
                  ? '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr><th>Zeile</th><th>Typ</th><th>Fehler</th><th>Vorschau</th></tr></thead><tbody>' +
                    failedPreviewRecords.map((record) => '<tr>' +
                      '<td>' + esc(String(record.rowIndex || '')) + '</td>' +
                      '<td><span class="badge bg-' + (record.errorType === 'salesforce' ? 'warning' : 'danger') + '">' + esc(String(record.errorType || 'mapping')) + '</span></td>' +
                      '<td class="small text-danger">' + esc(String(record.error || '')) + '</td>' +
                      '<td class="small">' + esc(Object.entries(record.sourceRecord || {}).slice(0, 3).map(([key, value]) => key + ': ' + String(value ?? '')).join(' | ')) + '</td>' +
                    '</tr>').join('') + '</tbody></table></div>'
                  : '<div class="text-secondary small">Fehlerdetails werden geladen oder sind nicht mehr verfügbar.</div>') +
              '</div>'
            : '') +
        '</div>';
      }

      let logsChart;
      let recordsChart;

      function applyUiTheme(themeName) {
        const normalized = themeName === 'industrial' || themeName === 'midnight' ? themeName : 'corporate';
        if (document.body) {
          document.body.classList.remove('theme-corporate', 'theme-industrial', 'theme-midnight');
          document.body.classList.add('theme-' + normalized);
        }

        const overviewRunsTable = document.getElementById('overview-runs-table');
        if (overviewRunsTable) {
          overviewRunsTable.classList.toggle('table-dark', normalized === 'midnight');
          if (normalized === 'midnight') {
            overviewRunsTable.style.setProperty('--bs-table-bg', '#132032');
            overviewRunsTable.style.setProperty('--bs-table-color', '#d7e4f5');
            overviewRunsTable.style.setProperty('--bs-table-border-color', '#223146');
            overviewRunsTable.style.setProperty('--bs-table-accent-bg', '#132032');
            overviewRunsTable.style.setProperty('--bs-table-striped-bg', '#1a2a3f');
            overviewRunsTable.style.setProperty('--bs-table-hover-bg', '#1a2a3f');
            overviewRunsTable.style.setProperty('--bs-table-striped-color', '#e6edf7');
            overviewRunsTable.style.setProperty('--bs-table-hover-color', '#e6edf7');
          } else {
            overviewRunsTable.style.removeProperty('--bs-table-bg');
            overviewRunsTable.style.removeProperty('--bs-table-color');
            overviewRunsTable.style.removeProperty('--bs-table-border-color');
            overviewRunsTable.style.removeProperty('--bs-table-accent-bg');
            overviewRunsTable.style.removeProperty('--bs-table-striped-bg');
            overviewRunsTable.style.removeProperty('--bs-table-hover-bg');
            overviewRunsTable.style.removeProperty('--bs-table-striped-color');
            overviewRunsTable.style.removeProperty('--bs-table-hover-color');
          }
        }

        try {
          localStorage.setItem(UI_THEME_STORAGE_KEY, normalized);
        } catch {
          // ignore storage access issues
        }

        const select = document.getElementById('theme-select');
        if (select && select.value !== normalized) {
          select.value = normalized;
        }
      }

      function initializeUiTheme() {
        let storedTheme = 'corporate';
        try {
          storedTheme = localStorage.getItem(UI_THEME_STORAGE_KEY) || 'corporate';
        } catch {
          storedTheme = 'corporate';
        }
        applyUiTheme(storedTheme);
      }

      function createModalController(modalId) {
        const element = document.getElementById(modalId);
        const bootstrapModal =
          window.bootstrap && window.bootstrap.Modal
            ? new window.bootstrap.Modal(element)
            : null;

        const showFallback = () => {
          element.style.setProperty('display', 'block', 'important');
          element.classList.add('show');
          element.removeAttribute('aria-hidden');
          document.body.classList.add('modal-open');
        };

        const hideFallback = () => {
          element.classList.remove('show');
          element.style.display = 'none';
          element.setAttribute('aria-hidden', 'true');
          document.body.classList.remove('modal-open');
        };

        if (!bootstrapModal) {
          hideFallback();
          element.querySelectorAll('[data-bs-dismiss="modal"]').forEach((button) => {
            button.addEventListener('click', hideFallback);
          });
        }

        return {
          show() {
            if (bootstrapModal) {
              try {
                bootstrapModal.show();
              } catch {
                showFallback();
                return;
              }
              if (!element.classList.contains('show')) {
                showFallback();
              }
              return;
            }
            showFallback();
          },
          hide() {
            if (bootstrapModal) {
              try {
                bootstrapModal.hide();
              } catch {
                hideFallback();
                return;
              }
              if (element.classList.contains('show')) {
                hideFallback();
              }
              return;
            }
            hideFallback();
          }
        };
      }

      const scheduleModal = createModalController('schedule-modal');
      const connectorModal = createModalController('connector-modal');
      const templatePickerModal = createModalController('template-picker-modal');
      const instanceModal = createModalController('instance-modal');
      const logsModal = createModalController('logs-modal');
      const recordsSchedulerModal = createModalController('records-scheduler-modal');
      const connectorNotificationErrorClassOptions = ['CONNECTION', 'AUTH', 'DATA', 'VALIDATION', 'UNKNOWN'];

      function esc(value) {
        return String(value ?? '-')
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;');
      }

      function getTemplateAccent(item) {
        const tags = Array.isArray(item?.tags) ? item.tags.map((tag) => String(tag || '').toLowerCase()) : [];
        if (tags.includes('ezb')) {
          return { start: '#0f4c81', end: '#6db1ff', glaze: 'rgba(255,255,255,0.22)' };
        }
        if (tags.includes('brevo')) {
          return { start: '#0f766e', end: '#2dd4bf', glaze: 'rgba(255,255,255,0.18)' };
        }
        if (tags.includes('newsletter')) {
          return { start: '#b45309', end: '#f59e0b', glaze: 'rgba(255,255,255,0.16)' };
        }
        if (item?.kind === 'bundle') {
          return { start: '#4c1d95', end: '#7c3aed', glaze: 'rgba(255,255,255,0.18)' };
        }
        if (item?.kind === 'schedule') {
          return { start: '#155e75', end: '#06b6d4', glaze: 'rgba(255,255,255,0.18)' };
        }
        return { start: '#9a3412', end: '#f97316', glaze: 'rgba(255,255,255,0.16)' };
      }

      function getTemplateSymbol(item) {
        const tags = Array.isArray(item?.tags) ? item.tags.map((tag) => String(tag || '').toUpperCase()) : [];
        if (tags.includes('EZB')) {
          return 'EZB';
        }
        if (tags.includes('BREVO')) {
          return 'BR';
        }
        if (tags.includes('NEWSLETTER')) {
          return 'NL';
        }
        if (item?.kind === 'bundle') {
          return 'SET';
        }
        const source = String(item?.name || '').trim();
        const initials = source
          .split(/\s+/)
          .map((part) => part.replace(/[^A-Za-z0-9]/g, '').slice(0, 1).toUpperCase())
          .filter(Boolean)
          .slice(0, 3)
          .join('');
        return initials || (item?.kind === 'schedule' ? 'JOB' : 'API');
      }

      function renderInstallerSummary() {
        const summary = state.installerSummary;
        const statusSummary = document.getElementById('installer-status-summary');
        const checksContainer = document.getElementById('installer-checks');
        const pathsContainer = document.getElementById('installer-paths');
        const commandsList = document.getElementById('installer-commands');
        const envTemplate = document.getElementById('installer-env-template');
        const scenarioSelect = document.getElementById('installer-scenario');
        const scenarioDescription = document.getElementById('installer-scenario-description');
        if (!statusSummary || !checksContainer || !pathsContainer || !commandsList || !envTemplate || !scenarioSelect || !scenarioDescription) {
          return;
        }

        if (!summary) {
          statusSummary.textContent = 'Installer-Daten konnten nicht geladen werden.';
          checksContainer.innerHTML = '';
          pathsContainer.innerHTML = '';
          commandsList.innerHTML = '';
          envTemplate.textContent = 'Keine Daten';
          return;
        }

        const scenarios = Array.isArray(summary.scenarios) ? summary.scenarios : [];
        if (!scenarioSelect.options.length) {
          scenarioSelect.innerHTML = scenarios.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.label) + ' - ' + esc(item.networkScope) + '</option>').join('');
          if (summary.defaultScenarioId) {
            scenarioSelect.value = summary.defaultScenarioId;
          }
        }
        const selectedScenario = scenarios.find((item) => item.id === scenarioSelect.value) || scenarios[0];
        if (!selectedScenario) {
          statusSummary.textContent = 'Keine Setup-Szenarien verfügbar.';
          return;
        }

        scenarioDescription.textContent = selectedScenario.description + ' Generierte Dateien: ' + selectedScenario.generatedFilesLabel + '.';
        statusSummary.textContent = summary.authConfigured
          ? 'Das gewählte Setup-Szenario ist vorbereitet. Admin-Login, CSRF- und Origin-Schutz sind aktiv.'
          : 'Das gewählte Setup-Szenario ist vorbereitet, aber Admin-Zugang ist noch nicht vollständig konfiguriert.';

        renderHeaderMenuState();

        checksContainer.innerHTML = (Array.isArray(selectedScenario.checks) ? selectedScenario.checks : []).map((item) => {
          const badgeClass = item.status === 'ready' ? 'text-bg-success' : item.status === 'in-progress' ? 'text-bg-warning' : 'text-bg-danger';
          return '<div class="border rounded p-2 bg-light">' +
            '<div class="d-flex justify-content-between align-items-start gap-2">' +
            '<div><div class="fw-semibold">' + esc(item.label) + '</div><div class="small text-secondary">' + esc(item.detail) + '</div></div>' +
            '<span class="badge ' + badgeClass + '">' + esc(item.status) + '</span>' +
            '</div></div>';
        }).join('');

        pathsContainer.innerHTML = Object.entries(selectedScenario.paths || {}).map(([key, value]) => (
          '<div class="col-md-6"><div class="border rounded p-2 h-100 bg-light">' +
          '<div class="small text-secondary">' + esc(key) + '</div>' +
          '<div class="fw-semibold small">' + esc(String(value || '-')) + '</div>' +
          '</div></div>'
        )).join('');

        commandsList.innerHTML = (Array.isArray(selectedScenario.commands) ? selectedScenario.commands : []).map((command) => (
          '<li class="mb-2"><code>' + esc(command) + '</code></li>'
        )).join('');

        envTemplate.textContent = String(selectedScenario.envTemplate || '');
      }

      function renderHeaderMenuState() {
        const summary = state.installerSummary;
        const authConfigured = !!summary?.authConfigured;
        const subtitle = document.getElementById('agent-header-menu-subtitle');
        const authHint = document.getElementById('agent-menu-auth-hint');
        const authPanel = document.getElementById('agent-menu-auth-panel');
        const logoutButton = document.getElementById('logout-admin');

        if (subtitle) {
          subtitle.textContent = authConfigured
            ? 'Navigation, Arbeitsbereich und Login-Session'
            : 'Navigation und Arbeitsbereich';
        }

        if (authHint) {
          if (authConfigured) {
            authHint.textContent = 'Admin-Login ist aktiv. Die aktuelle Session kann hier beendet werden.';
            authHint.classList.remove('d-none');
          } else {
            authHint.textContent = 'Für diese Instanz ist aktuell kein UI-Login aktiv.';
            authHint.classList.remove('d-none');
          }
        }

        if (authPanel) {
          authPanel.classList.toggle('d-none', !authConfigured);
        }

        if (logoutButton) {
          logoutButton.disabled = !authConfigured;
          logoutButton.setAttribute('aria-hidden', authConfigured ? 'false' : 'true');
        }
      }

      function applyInstallerScenarioDefaults() {
        const summary = state.installerSummary;
        const scenarioSelect = document.getElementById('installer-scenario');
        if (!summary || !scenarioSelect) {
          return;
        }

        const scenarios = Array.isArray(summary.scenarios) ? summary.scenarios : [];
        const selectedScenario = scenarios.find((item) => item.id === scenarioSelect.value) || scenarios[0];
        if (!selectedScenario || !selectedScenario.defaults) {
          return;
        }

        const defaults = selectedScenario.defaults;
        const appDirEl = document.getElementById('installer-app-dir');
        const serviceUserEl = document.getElementById('installer-service-user');
        const serviceGroupEl = document.getElementById('installer-service-group');
        const publicHostEl = document.getElementById('installer-public-host');
        const portEl = document.getElementById('installer-port');
        const adminUsernameEl = document.getElementById('installer-admin-username');
        if (appDirEl) appDirEl.value = defaults.appDir || '';
        if (serviceUserEl) serviceUserEl.value = defaults.serviceUser || '';
        if (serviceGroupEl) serviceGroupEl.value = defaults.serviceGroup || '';
        if (publicHostEl) publicHostEl.value = defaults.publicHost || '';
        if (portEl) portEl.value = String(defaults.port || 9010);
        if (adminUsernameEl) adminUsernameEl.value = defaults.adminUsername || 'admin';
      }

      async function generateInstallerFilesFromUi() {
        const statusEl = document.getElementById('installer-generate-status');
        const outputEl = document.getElementById('installer-generated-files');
        const button = document.getElementById('installer-generate-files');
        const downloadLink = document.getElementById('installer-download-archive');
        if (!statusEl || !outputEl || !button) {
          return;
        }

        button.disabled = true;
        if (downloadLink) {
          downloadLink.classList.add('d-none');
          downloadLink.removeAttribute('href');
        }
        statusEl.textContent = 'Installationsdateien werden erzeugt...';
        try {
          const result = await requestJson('/api/installer/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              scenarioId: document.getElementById('installer-scenario')?.value,
              appDir: document.getElementById('installer-app-dir')?.value,
              serviceUser: document.getElementById('installer-service-user')?.value,
              serviceGroup: document.getElementById('installer-service-group')?.value,
              publicHost: document.getElementById('installer-public-host')?.value,
              port: Number(document.getElementById('installer-port')?.value || '9010'),
              adminUsername: document.getElementById('installer-admin-username')?.value
            })
          });
          state.installerGeneratedFiles = Array.isArray(result.files) ? result.files : [];
          statusEl.textContent = 'Dateien erzeugt unter ' + String(result.outputDir || 'artifacts/installer/generated');
          outputEl.textContent = ['Output: ' + String(result.outputDir || ''), '', ...(state.installerGeneratedFiles || []), '', 'Archiv: ' + String(result.archiveFileName || ''), 'Install: ' + String(result.installCommand || '')].join('\\n');
          if (downloadLink && result.downloadUrl) {
            downloadLink.setAttribute('href', String(result.downloadUrl));
            downloadLink.classList.remove('d-none');
          }
        } catch (error) {
          statusEl.textContent = error.message || 'Installationsdateien konnten nicht erzeugt werden';
          outputEl.textContent = 'Fehler: ' + (error.message || 'Unbekannter Fehler');
        } finally {
          button.disabled = false;
        }
      }

      function getTemplateHeroLabel(item) {
        if (item?.kind === 'bundle') {
          return 'Komplettset';
        }
        return item?.kind === 'schedule' ? 'Scheduler' : 'Connector';
      }

      function getTemplatePreviewTitle(item) {
        if (item?.kind === 'bundle') {
          return 'Sofort einsatzbereit';
        }
        if (item?.kind === 'schedule') {
          return 'Ablauf inklusive Timing';
        }
        return 'Verbindung vorkonfiguriert';
      }

      function isoToLocalDateTimeInput(value) {
        if (!value) {
          return '';
        }

        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return '';
        }

        const localTime = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
        return localTime.toISOString().slice(0, 16);
      }

      function localDateTimeInputToIso(value) {
        if (!value) {
          return undefined;
        }

        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return undefined;
        }

        return date.toISOString();
      }

      function renderScheduleConnectorOptions(selectedId) {
        const select = document.getElementById('sch-connector');
        const options = ['<option value="">- Kein Connector -</option>'];
        state.connectors.forEach((connector) => {
          options.push('<option value="' + esc(connector.id) + '">' + esc(connector.name) + '</option>');
        });
        select.innerHTML = options.join('');
        select.value = selectedId || '';
      }

      function renderScheduleParentOptions(currentScheduleId, selectedParentId) {
        const select = document.getElementById('sch-parent-schedule');
        if (!select) {
          return;
        }

        const options = ['<option value="">- Kein Parent -</option>'];
        (state.schedules || [])
          .filter((item) => String(item.id || '') !== String(currentScheduleId || ''))
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' }))
          .forEach((item) => {
            options.push('<option value="' + esc(item.id) + '">' + esc(item.name) + '</option>');
          });

        select.innerHTML = options.join('');
        select.value = selectedParentId || '';
      }

      function updateWeekdayChips() {
        document.querySelectorAll('#sch-weekdays .weekday-chip').forEach((chip) => {
          const input = chip.querySelector('input');
          chip.classList.toggle('active', !!input && input.checked);
        });
      }

      function updateTimingInheritanceUi() {
        const inherit = !!document.getElementById('sch-inherit-parent-timing')?.checked;
        const ids = ['sch-timing-start', 'sch-timing-time', 'sch-timing-interval', 'sch-timing-apply', 'sch-timing-reset'];
        ids.forEach((id) => {
          const element = document.getElementById(id);
          if (element) {
            element.disabled = inherit;
          }
        });

        document.querySelectorAll('#sch-weekdays input').forEach((input) => {
          input.disabled = inherit;
        });

        const preview = document.getElementById('sch-timing-preview');
        if (preview && inherit) {
          preview.textContent = 'Timing wird vom Parent-Scheduler übernommen.';
        }
      }

      function calculateNextRunFromTiming() {
        const startValue = document.getElementById('sch-timing-start').value;
        const timeValue = document.getElementById('sch-timing-time').value || '09:00';
        const selectedWeekdays = Array.from(document.querySelectorAll('#sch-weekdays input:checked'))
          .map((input) => Number(input.value))
          .filter((value) => !Number.isNaN(value));

        if (!startValue || selectedWeekdays.length === 0) {
          throw new Error('Bitte Startdatum und mindestens einen Wochentag auswählen.');
        }

        const [hour, minute] = timeValue.split(':').map((item) => Number(item));
        const startDate = new Date(startValue + 'T00:00:00');
        const now = new Date();
        const maxDays = 60;

        for (let offset = 0; offset <= maxDays; offset += 1) {
          const candidate = new Date(startDate);
          candidate.setDate(startDate.getDate() + offset);
          candidate.setHours(hour || 0, minute || 0, 0, 0);

          if (candidate < now) {
            continue;
          }

          if (selectedWeekdays.includes(candidate.getDay())) {
            return {
              nextRunAtIso: candidate.toISOString(),
              weekdayList: selectedWeekdays.slice().sort((a, b) => a - b).join(', '),
              timeValue
            };
          }
        }

        throw new Error('Für die nächsten 60 Tage konnte kein Termin berechnet werden.');
      }

      function applyTimingHelper() {
        try {
          const result = calculateNextRunFromTiming();
          document.getElementById('sch-next-run').value = isoToLocalDateTimeInput(result.nextRunAtIso);
          document.getElementById('sch-timing-preview').textContent =
            'Nächster Lauf: ' + new Date(result.nextRunAtIso).toLocaleString('de-DE') +
            ' | Uhrzeit: ' + result.timeValue +
            ' | Wochentage: ' + result.weekdayList;
          clearError();
        } catch (error) {
          showError(error.message || 'Zeitsteuerung konnte nicht berechnet werden');
        }
      }

      function showError(message) {
        const alert = document.getElementById('global-alert');
        alert.textContent = message;
        alert.classList.remove('d-none');
      }

      function clearError() {
        const alert = document.getElementById('global-alert');
        alert.textContent = '';
        alert.classList.add('d-none');
      }

      function showModalError(message) {
        const el = document.getElementById('sch-modal-error');
        if (!el) { showError(message); return; }
        el.textContent = message;
        el.classList.remove('d-none');
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }

      function clearModalError() {
        const el = document.getElementById('sch-modal-error');
        if (el) { el.textContent = ''; el.classList.add('d-none'); }
      }

      function showConnectorModalError(message) {
        const el = document.getElementById('con-modal-error');
        if (!el) {
          showError(message);
          return;
        }
        el.textContent = message;
        el.classList.remove('d-none');
      }

      function clearConnectorModalError() {
        const el = document.getElementById('con-modal-error');
        if (el) {
          el.textContent = '';
          el.classList.add('d-none');
        }
      }

      function showMigrationModalError(message) {
        const el = document.getElementById('mig-modal-error');
        if (!el) {
          showError(message);
          return;
        }
        el.textContent = message;
        el.classList.remove('d-none');
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }

      function clearMigrationModalError() {
        const el = document.getElementById('mig-modal-error');
        if (el) {
          el.textContent = '';
          el.classList.add('d-none');
        }
      }

      function withInstance(path) {
        const url = new URL(path, window.location.origin);
        if (state.instanceId) {
          url.searchParams.set('instanceId', state.instanceId);
        }
        return url.pathname + url.search;
      }

      async function requestJson(path, options) {
        const requestOptions = options && typeof options === 'object' ? options : {};
        const response = await fetch(withInstance(path), requestOptions);
        if (response.status === 401) {
          window.location.href = '/';
          throw new Error('Sitzung abgelaufen');
        }
        let data;
        try {
          data = await response.json();
        } catch {
          data = { error: 'Ungueltige Antwort vom Server' };
        }

        if (!response.ok) {
          throw new Error(data.error || data.message || 'Request failed');
        }

        return data;
      }

      function getMigrationReportUrl(migrationId, asDownload) {
        return withInstance('/api/migrations/' + encodeURIComponent(migrationId) + '/report' + (asDownload ? '?download=1' : ''));
      }

      function isFileConnectorType(connectorType) {
        const normalized = String(connectorType || '').toLowerCase();
        return normalized.includes('file') || normalized.includes('csv') || normalized.includes('excel') || normalized.includes('xlsx') || normalized.includes('json');
      }

      function normalizeConnectorType(connectorType) {
        const normalized = String(connectorType || '').trim().toLowerCase();
        if (!normalized) {
          return '';
        }
        if (normalized === 'mssql' || normalized === 'ms sql' || normalized === 'ms_sql' || normalized.includes('sqlserver')) {
          return 'MSSQL';
        }
        if (normalized === 'postgresql' || normalized === 'postgres' || normalized === 'pgsql') {
          return 'POSTGRESQL';
        }
        if (normalized === 'mysql') {
          return 'MYSQL';
        }
        if (normalized.includes('binary') && normalized.includes('file')) {
          return 'FILE_BINARY_SF_IMPORT';
        }
        if (normalized.includes('rest') || normalized.includes('http') || normalized.includes('api')) {
          return 'REST_API';
        }
        if (normalized.includes('file') || normalized.includes('csv') || normalized.includes('excel') || normalized.includes('xlsx') || normalized.includes('json') || normalized.includes('txt')) {
          return 'FILE';
        }
        return String(connectorType || '').trim().toUpperCase();
      }

      function isSqlConnectorType(connectorType) {
        const normalized = normalizeConnectorType(connectorType);
        return normalized === 'MSSQL' || normalized === 'POSTGRESQL' || normalized === 'MYSQL';
      }

      function isRestConnectorType(connectorType) {
        return normalizeConnectorType(connectorType) === 'REST_API';
      }

      function isBinaryImportConnectorType(connectorType) {
        return normalizeConnectorType(connectorType) === 'FILE_BINARY_SF_IMPORT';
      }

      function isMssqlConnectorType(connectorType) {
        return normalizeConnectorType(connectorType) === 'MSSQL';
      }

      function getConnectorWizardTypeFromConnectorType(connectorType) {
        const normalized = normalizeConnectorType(connectorType);
        if (!normalized) {
          return 'MSSQL';
        }
        if (['MSSQL', 'POSTGRESQL', 'MYSQL', 'FILE', 'REST_API', 'FILE_BINARY_SF_IMPORT'].includes(normalized)) {
          return normalized;
        }
        return 'CUSTOM';
      }

      async function fileToBase64(file) {
        const buffer = await file.arrayBuffer();
        let binary = '';
        const bytes = new Uint8Array(buffer);
        const chunkSize = 0x8000;

        for (let index = 0; index < bytes.length; index += chunkSize) {
          const chunk = bytes.subarray(index, Math.min(index + chunkSize, bytes.length));
          binary += String.fromCharCode(...chunk);
        }

        return window.btoa(binary);
      }

      async function exportSetup() {
        const result = await requestJson('/api/setup/export');
        const payload = JSON.stringify(result, null, 2);
        const blob = new Blob([payload], { type: 'application/json;charset=utf-8' });
        const href = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        const instanceSuffix = state.instanceId ? '-' + state.instanceId : '';
        anchor.href = href;
        anchor.download = 'sf-agent-setup' + instanceSuffix + '.json';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(href);
      }

      async function importSetupDocument(documentBody) {
        const result = await requestJson('/api/setup/import', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(documentBody)
        });

        window.alert(
          'Import abgeschlossen. Connectoren: +' + result.connectorsCreated + ' / ~' + result.connectorsUpdated +
          ', Scheduler: +' + result.schedulesCreated + ' / ~' + result.schedulesUpdated
        );
        await refresh();
      }

      async function importSetupFromFile(file) {
        if (!file) {
          return;
        }

        const raw = await file.text();
        let documentBody;
        try {
          documentBody = JSON.parse(raw);
        } catch {
          throw new Error('Import-Datei ist kein gueltiges JSON');
        }

        await importSetupDocument(documentBody);
      }

      async function createSchedulerFromDroppedFile(connectorId, file) {
        if (!connectorId || !file) {
          return;
        }

        const base64 = await fileToBase64(file);
        const analysis = await requestJson('/api/files/analyze', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ connectorId, fileName: file.name, contentBase64: base64 })
        });

        const objectName = (state.scheduleOptions?.objectNames || [])[0] || 'Account';
        const targetDefinition = {
          objectApiName: objectName,
          operation: 'upsert',
          externalIdField: 'ExternalId__c',
          picklists: []
        };

        const payload = {
          active: false,
          sourceSystem: 'File',
          targetSystem: 'Salesforce',
          objectName,
          operation: 'Upsert',
          connectorId,
          direction: 'Inbound',
          sourceType: analysis.sourceType,
          targetType: 'SALESFORCE',
          sourceDefinition: analysis.sourceDefinition,
          targetDefinition: JSON.stringify(targetDefinition, null, 2),
          mappingDefinition: analysis.mappingDefinition,
          batchSize: 100,
          timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: '09:00' }),
          name: 'File Import ' + analysis.fileName
        };

        const created = await requestJson('/api/schedules', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        await refresh();
        await openScheduleModal(created.id);
      }

      function normalizeSystemValue(value) {
        const normalized = String(value || '').trim().toLowerCase();
        if (!normalized) {
          return '';
        }
        if (normalized === 'mssql' || normalized === 'ms sql' || normalized === 'ms_sql') {
          return 'MS SQL';
        }
        if (normalized === 'salesforce') {
          return 'Salesforce';
        }
        if (normalized === 'mock') {
          return 'Mock';
        }
        return String(value || '').trim();
      }

      function normalizeOperationValue(value) {
        const normalized = String(value || '').trim().toLowerCase();
        if (!normalized) {
          return '';
        }
        if (normalized === 'insert') return 'Insert';
        if (normalized === 'update') return 'Update';
        if (normalized === 'upsert') return 'Upsert';
        if (normalized === 'delete') return 'Delete';
        return String(value || '').trim();
      }

      function setTemplatePickerError(message) {
        const element = document.getElementById('template-picker-error');
        if (!element) {
          return;
        }
        if (!message) {
          element.textContent = '';
          element.classList.add('d-none');
          return;
        }
        element.textContent = String(message);
        element.classList.remove('d-none');
      }

      function resolveTemplatePicker(selection) {
        if (typeof templatePickerState.resolver === 'function') {
          const resolver = templatePickerState.resolver;
          templatePickerState.resolver = null;
          resolver(selection || null);
        }
      }

      function applySelectedTemplate() {
        const selected = templatePickerState.items.find((item) => item.id === templatePickerState.selectedTemplateId) || null;
        resolveTemplatePicker(selected);
        templatePickerModal.hide();
      }

      function renderTemplatePicker() {
        const list = document.getElementById('template-picker-list');
        const tagsWrap = document.getElementById('template-picker-tags');
        const summary = document.getElementById('template-picker-summary');
        const applyButton = document.getElementById('template-picker-apply');
        const searchInput = document.getElementById('template-picker-search');
        if (!list || !tagsWrap || !summary || !applyButton || !searchInput) {
          return;
        }

        const searchValue = String(searchInput.value || '').trim().toLowerCase();
        const availableTags = Array.from(new Set(templatePickerState.items.flatMap((item) => Array.isArray(item.tags) ? item.tags : []).map((tag) => String(tag || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b, 'de', { sensitivity: 'base' }));
        if (templatePickerState.selectedTag && !availableTags.includes(templatePickerState.selectedTag)) {
          templatePickerState.selectedTag = '';
        }
        tagsWrap.innerHTML = ['<button type="button" class="btn btn-sm ' + (templatePickerState.selectedTag ? 'btn-outline-secondary' : 'btn-secondary') + '" data-template-tag="">Alle</button>']
          .concat(availableTags.map((tag) => '<button type="button" class="btn btn-sm ' + (templatePickerState.selectedTag === tag ? 'btn-secondary' : 'btn-outline-secondary') + '" data-template-tag="' + esc(tag) + '">' + esc(tag) + '</button>'))
          .join('');
        tagsWrap.querySelectorAll('[data-template-tag]').forEach((button) => {
          button.addEventListener('click', () => {
            templatePickerState.selectedTag = button.getAttribute('data-template-tag') || '';
            renderTemplatePicker();
          });
        });

        const filteredItems = templatePickerState.items.filter((item) => {
          if (templatePickerState.selectedTag && !(Array.isArray(item.tags) && item.tags.includes(templatePickerState.selectedTag))) {
            return false;
          }
          if (!searchValue) {
            return true;
          }
          const haystack = [item.name, item.description, ...(Array.isArray(item.tags) ? item.tags : [])]
            .map((value) => String(value || '').toLowerCase())
            .join(' ');
          return haystack.includes(searchValue);
        });
        templatePickerState.filteredItems = filteredItems;

        if (!filteredItems.some((item) => item.id === templatePickerState.selectedTemplateId)) {
          templatePickerState.selectedTemplateId = filteredItems[0]?.id || '';
        }

        summary.textContent = filteredItems.length
          ? String(filteredItems.length) + ' Vorlagen verfügbar'
          : 'Keine passende Vorlage gefunden.';
        applyButton.disabled = !templatePickerState.selectedTemplateId;

        list.innerHTML = filteredItems.length
          ? '<div class="template-app-grid">' + filteredItems.map((item) => {
              const isSelected = item.id === templatePickerState.selectedTemplateId;
              const scopeLabel = item.scope === 'system' ? 'System' : 'Eigene Vorlage';
              const kindLabel = item.kind === 'bundle' ? 'Komplettset' : (item.kind === 'connector' ? 'Connector' : 'Scheduler');
              const accent = getTemplateAccent(item);
              const symbol = getTemplateSymbol(item);
              const heroLabel = getTemplateHeroLabel(item);
              const tags = Array.isArray(item.tags) && item.tags.length
                ? '<div class="template-app-card__tags">' + item.tags.slice(0, 5).map((tag) => '<span class="badge text-bg-light border">' + esc(tag) + '</span>').join('') + '</div>'
                : '';
              const previewTitle = getTemplatePreviewTitle(item);
              return '<button type="button" class="template-app-card' + (isSelected ? ' is-selected' : '') + '" data-template-id="' + esc(item.id) + '">' +
                '<div class="template-app-card__body">' +
                  '<div class="template-app-card__top">' +
                    '<div class="template-app-card__app">' +
                      '<div class="template-app-card__icon" style="background:linear-gradient(135deg,' + accent.start + ',' + accent.end + ');">' +
                        '<div class="template-app-card__glaze" style="background:' + accent.glaze + ';"></div>' +
                        '<span class="template-app-card__symbol">' + esc(symbol) + '</span>' +
                      '</div>' +
                      '<div class="template-app-card__meta">' +
                        '<div class="template-app-card__eyebrow">' + esc(heroLabel) + '</div>' +
                        '<div class="template-app-card__title">' + esc(item.name) + '</div>' +
                        '<div class="template-app-card__badges">' +
                          '<span class="badge ' + (isSelected ? 'text-bg-primary' : 'text-bg-secondary-subtle border text-secondary-emphasis') + '">' + esc(scopeLabel) + '</span>' +
                          '<span class="badge ' + (isSelected ? 'text-bg-info' : 'text-bg-info-subtle border text-info-emphasis') + '">' + esc(kindLabel) + '</span>' +
                        '</div>' +
                      '</div>' +
                    '</div>' +
                    '<span class="template-app-card__install">' + (isSelected ? 'Ausgewählt' : 'Öffnen') + '</span>' +
                  '</div>' +
                  '<div class="template-app-card__hero" style="background:linear-gradient(135deg,' + accent.start + ',' + accent.end + ');">' +
                    '<div class="template-app-card__hero-art"></div>' +
                    '<div class="template-app-card__hero-copy">' +
                      '<div class="template-app-card__hero-label">' + esc(previewTitle) + '</div>' +
                      '<div class="template-app-card__hero-name">' + esc(symbol) + ' · ' + esc(heroLabel) + '</div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="template-app-card__description">' + esc(item.description || 'Keine Beschreibung') + '</div>' +
                  '<div class="template-app-card__footer">' +
                    tags +
                  '</div>' +
                '</div>' +
              '</button>';
            }).join('') + '</div>'
          : '<div class="text-secondary small border rounded p-3">Keine Vorlagen gefunden.</div>';

        list.querySelectorAll('[data-template-id]').forEach((button) => {
          button.addEventListener('click', () => {
            templatePickerState.selectedTemplateId = button.getAttribute('data-template-id') || '';
            renderTemplatePicker();
          });
          button.addEventListener('dblclick', () => {
            applySelectedTemplate();
          });
        });
      }

      async function pickTemplate(kind) {
        setTemplatePickerError('');
        document.getElementById('template-picker-title').textContent = (kind === 'connector' ? 'Connector' : 'Scheduler') + '-Vorlage wählen';
        document.getElementById('template-picker-search').value = '';
        templatePickerState.kind = kind;
        templatePickerState.items = [];
        templatePickerState.filteredItems = [];
        templatePickerState.selectedTemplateId = '';
        templatePickerState.selectedTag = '';

        const result = await requestJson('/api/templates?kind=' + encodeURIComponent(kind), null);
        templatePickerState.items = Array.isArray(result.items) ? result.items : [];
        if (!templatePickerState.items.length) {
          window.alert((kind === 'connector' ? 'Connector' : 'Scheduler') + '-Vorlagen sind noch nicht vorhanden.');
          return null;
        }

        templatePickerState.selectedTemplateId = templatePickerState.items[0]?.id || '';
        renderTemplatePicker();
        templatePickerModal.show();

        return await new Promise((resolve) => {
          templatePickerState.resolver = resolve;
        });
      }

      async function createFromTemplate(kind) {
        const template = await pickTemplate(kind);
        if (!template) {
          return;
        }

        if (template.kind === 'bundle') {
          const result = await requestJson('/api/templates/' + encodeURIComponent(template.id) + '/apply', {
            method: 'POST'
          });
          await refresh();
          if (kind === 'schedule' && result.schedule?.id) {
            await openScheduleModal(result.schedule.id);
            return;
          }
          if (result.connector?.id) {
            openConnectorModal(result.connector.id);
            return;
          }
          window.alert('Komplettvorlage angelegt.');
          return;
        }

        if (kind === 'schedule') {
          await openScheduleModal('', template.schedule || {});
          return;
        }

        openConnectorModal('', template.connector || {});
      }

      function formatDate(dateString, format) {
        if (!dateString) return '-';
        try {
          const date = new Date(dateString);
          if (format === 'short') {
            return date.toLocaleString('de-DE', { dateStyle: 'short', timeStyle: 'short' });
          }
          return date.toLocaleString('de-DE');
        } catch {
          return String(dateString);
        }
      }

      function formatDurationMinSec(milliseconds) {
        const numericValue = Number(milliseconds);
        if (!Number.isFinite(numericValue) || numericValue < 0) {
          return '-';
        }

        const totalSeconds = Math.round(numericValue / 1000);
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = totalSeconds % 60;
        if (minutes >= 60) {
          const hours = Math.floor(minutes / 60);
          const restMinutes = minutes % 60;
          return hours + ':' + String(restMinutes).padStart(2, '0') + ':' + String(seconds).padStart(2, '0');
        }
        return minutes + ':' + String(seconds).padStart(2, '0');
      }

      function getRunDurationMs(run) {
        if (!run?.startedAt) {
          return null;
        }

        const startedAt = new Date(run.startedAt).getTime();
        if (Number.isNaN(startedAt)) {
          return null;
        }

        const finishedAt = run?.finishedAt ? new Date(run.finishedAt).getTime() : Date.now();
        if (Number.isNaN(finishedAt) || finishedAt < startedAt) {
          return null;
        }

        return finishedAt - startedAt;
      }

      function getConnectorNameById(connectorId) {
        if (!connectorId) return '-';
        const connector = state.connectors?.find((item) => item.id === connectorId);
        return connector ? connector.name : connectorId;
      }

      function normalizeRunStatus(status) {
        return String(status || '').trim().toLowerCase();
      }

      function getStatusBadge(status) {
        if (!status) return '<span class="badge bg-secondary">Unbekannt</span>';
        const lowerStatus = normalizeRunStatus(status);
        if (lowerStatus === 'success' || lowerStatus === 'succeeded') {
          return '<span class="badge bg-success">✓ Erfolg</span>';
        }
        if (lowerStatus === 'running' || lowerStatus === 'in-progress') {
          return '<span class="badge bg-info">◉ Läuft</span>';
        }
        if (lowerStatus === 'failed' || lowerStatus === 'error') {
          return '<span class="badge bg-danger">✕ Fehler</span>';
        }
        return '<span class="badge bg-secondary">' + esc(status) + '</span>';
      }

      function getObjectIcon(objectName) {
        if (!objectName) return '◉';
        const name = String(objectName).toLowerCase();
        if (name.includes('account')) return '🏢';
        if (name.includes('contact') || name.includes('person')) return '👤';
        if (name.includes('product')) return '📦';
        if (name.includes('price') || name.includes('pricebook')) return '💰';
        if (name.includes('order')) return '📋';
        if (name.includes('opportunity')) return '🎯';
        if (name.includes('case')) return '🎫';
        return '◉';
      }

      function getConnectorIcon(connectorType, connectorName) {
        const value = String(connectorType || connectorName || '').toLowerCase();
        if (value.includes('salesforce')) return '☁';
        if (value.includes('rest')) return '🌐';
        if (value.includes('mssql') || value.includes('sql')) return '🗄';
        if (value.includes('file') || value.includes('csv') || value.includes('excel') || value.includes('json')) return '📄';
        if (value.includes('mock') || value.includes('test')) return '🧪';
        if (value.includes('sage')) return '📚';
        return '⚙';
      }

      function getConnectorGraphClass(connectorType, connectorName) {
        const value = String(connectorType || connectorName || '').toLowerCase();
        if (value.includes('salesforce')) return 'graph-connector-salesforce';
        if (value.includes('rest')) return 'graph-connector-rest';
        if (value.includes('mssql') || value.includes('sql')) return 'graph-connector-mssql';
        if (value.includes('file') || value.includes('csv') || value.includes('excel') || value.includes('json')) return 'graph-connector-file';
        if (value.includes('mock') || value.includes('test')) return 'graph-connector-mock';
        if (value.includes('sage')) return 'graph-connector-erp';
        return 'graph-connector-generic';
      }

      function splitGraphText(value, maxChars, maxLines) {
        const text = String(value || '').trim();
        if (!text) return [];

        const words = text.split(/\\s+/).filter(Boolean);
        const lines = [];
        let currentLine = '';

        words.forEach((word) => {
          const candidate = currentLine ? currentLine + ' ' + word : word;
          if (candidate.length <= maxChars) {
            currentLine = candidate;
            return;
          }

          if (currentLine) {
            lines.push(currentLine);
          }

          if (word.length <= maxChars) {
            currentLine = word;
            return;
          }

          lines.push(word.slice(0, maxChars - 1) + '…');
          currentLine = '';
        });

        if (currentLine) {
          lines.push(currentLine);
        }

        if (lines.length > maxLines) {
          const visibleLines = lines.slice(0, maxLines);
          const lastIndex = visibleLines.length - 1;
          visibleLines[lastIndex] = visibleLines[lastIndex].slice(0, Math.max(0, maxChars - 1)).trimEnd() + '…';
          return visibleLines;
        }

        return lines;
      }

      function splitGraphTextByLine(value, lineCharLimits) {
        const text = String(value || '').trim();
        const limits = Array.isArray(lineCharLimits) ? lineCharLimits.map((item) => Math.max(1, Number(item) || 1)) : [];
        if (!text || !limits.length) return splitGraphText(text, 18, 2);

        const words = text.split(/\\s+/).filter(Boolean);
        const lines = [];
        let currentLine = '';
        let lineIndex = 0;

        words.forEach((word) => {
          const currentLimit = limits[Math.min(lineIndex, limits.length - 1)];
          const candidate = currentLine ? currentLine + ' ' + word : word;
          if (candidate.length <= currentLimit) {
            currentLine = candidate;
            return;
          }

          if (currentLine) {
            lines.push(currentLine);
            lineIndex += 1;
          }

          const nextLimit = limits[Math.min(lineIndex, limits.length - 1)];
          if (word.length <= nextLimit) {
            currentLine = word;
            return;
          }

          lines.push(word.slice(0, Math.max(1, nextLimit - 1)).trimEnd() + '…');
          currentLine = '';
          lineIndex += 1;
        });

        if (currentLine) {
          lines.push(currentLine);
        }

        if (lines.length > limits.length) {
          const visibleLines = lines.slice(0, limits.length);
          const lastIndex = visibleLines.length - 1;
          const lastLimit = limits[lastIndex] || limits[limits.length - 1] || 18;
          visibleLines[lastIndex] = visibleLines[lastIndex].slice(0, Math.max(1, lastLimit - 1)).trimEnd() + '…';
          return visibleLines;
        }

        return lines;
      }

      function renderGraphText(className, x, y, lines, lineHeight) {
        if (!lines.length) return '';
        return '<text class="' + className + '" x="' + x + '" y="' + y + '">' +
          lines.map((line, index) => '<tspan class="' + className + '-line" x="' + x + '" dy="' + (index === 0 ? 0 : lineHeight) + '">' + esc(line) + '</tspan>').join('') +
        '</text>';
      }

      function resolveEffectiveTargetSystem() {
        const explicitTargetSystem = normalizeSystemValue(document.getElementById('sch-target-system')?.value || '');
        if (explicitTargetSystem) {
          return explicitTargetSystem;
        }

        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        if (targetType === 'SALESFORCE' || targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
          return 'Salesforce';
        }
        if (targetType === 'MSSQL') {
          return 'MS SQL';
        }
        if (targetType === 'MOCK') {
          return 'Mock';
        }

        return '';
      }

      function isSalesforceTargetSelection() {
        const targetSystem = resolveEffectiveTargetSystem();
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        return targetSystem === 'Salesforce' && targetType === 'SALESFORCE';
      }

      function formatUsageBlock(value, unit) {
        if (!value || !Number.isFinite(value.max) || value.max <= 0) {
          return '-';
        }
        const max = Number(value.max);
        const used = Number(value.used || 0);
        const remaining = Number(value.remaining || 0);
        const percentage = Math.round((used / max) * 100);
        return used + '/' + max + (unit ? ' ' + unit : '') + ' (' + percentage + '%, frei ' + remaining + ')';
      }

      function resolveUsagePercentage(value) {
        if (!value || !Number.isFinite(value.max) || value.max <= 0) {
          return 0;
        }
        const max = Number(value.max);
        const used = Number(value.used || 0);
        return Math.max(0, Math.min(100, Math.round((used / max) * 100)));
      }

      function renderLimitGauge(gaugeId, valueId, usage) {
        const gauge = document.getElementById(gaugeId);
        const valueEl = document.getElementById(valueId);
        if (!gauge || !valueEl) {
          return;
        }

        if (!usage || !Number.isFinite(usage.max) || usage.max <= 0) {
          gauge.style.setProperty('--gauge-value', '0');
          gauge.classList.remove('is-warning', 'is-danger');
          valueEl.textContent = '-';
          return;
        }

        const percentage = resolveUsagePercentage(usage);
        gauge.style.setProperty('--gauge-value', String(percentage));
        gauge.classList.toggle('is-warning', percentage >= 70 && percentage < 90);
        gauge.classList.toggle('is-danger', percentage >= 90);
        valueEl.textContent = percentage + '%';
      }

      function renderSalesforceOverview(overview) {
        state.salesforceOverview = overview || null;

        const setText = (id, value) => {
          const el = document.getElementById(id);
          if (el) {
            el.textContent = String(value || '-');
          }
        };

        setText('sf-domain', overview?.domain || overview?.instanceUrl || '-');
        setText('sf-environment', overview?.environment || '-');
        setText('sf-api-usage', formatUsageBlock(overview?.apiUsage));
        setText('sf-data-storage', formatUsageBlock(overview?.dataStorageMb, 'MB'));
        setText('sf-file-storage', formatUsageBlock(overview?.fileStorageMb, 'MB'));
        setText('sf-licenses', formatUsageBlock(overview?.licenses));
        renderLimitGauge('sf-api-gauge', 'sf-api-gauge-value', overview?.apiUsage);
        renderLimitGauge('sf-data-gauge', 'sf-data-gauge-value', overview?.dataStorageMb);
        renderLimitGauge('sf-file-gauge', 'sf-file-gauge-value', overview?.fileStorageMb);
        renderLimitGauge('sf-license-gauge', 'sf-license-gauge-value', overview?.licenses);
      }

      function isSchedulerMssqlUpsertSelection() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const operation = normalizeOperationValue(document.getElementById('sch-operation')?.value || '');
        return targetType === 'MSSQL' && String(operation || '').toLowerCase() === 'upsert';
      }

      function ensureSalesforceTargetDefinition() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const targetSystem = resolveEffectiveTargetSystem();
        const isSalesforce = targetType === 'SALESFORCE' && targetSystem === 'Salesforce';
        const isMssql = isSchedulerMssqlUpsertSelection();
        if (!isSalesforce && !isMssql) {
          return;
        }

        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        if (!objectApiName && isSalesforce) {
          return;
        }

        const targetDefinitionInput = document.getElementById('sch-target-definition');
        const raw = String(targetDefinitionInput?.value || '').trim();
        const upsertField = String(document.getElementById('sch-external-id-field')?.value || '').trim();
        const pricebook2Id = String(document.getElementById('sch-pricebook2id')?.value || '').trim();
        const operation = String(normalizeOperationValue(document.getElementById('sch-operation')?.value || 'Upsert') || 'Upsert').toLowerCase();
        const nextDefinition = isSalesforce
          ? {
              objectApiName,
              operation
            }
          : {
              upsertKey: upsertField
            };

        if (isSalesforce && operation === 'upsert' && upsertField) {
          nextDefinition.externalIdField = upsertField;
        }

        if (isSalesforce && objectApiName === 'PricebookEntry' && pricebook2Id) {
          nextDefinition.pricebook2Id = pricebook2Id;
        }

        if (isMssql && upsertField) {
          nextDefinition.upsertKey = upsertField;
        }

        if (!raw) {
          targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
          return;
        }

        try {
          const parsed = JSON.parse(raw);
          const targetDefinition = getSchedulerSelectedTargetDefinitionWritableContainer(parsed) || parsed;
          if (isSalesforce) {
            targetDefinition.objectApiName = objectApiName;
            targetDefinition.operation = operation;
            if (operation === 'upsert') {
              if (upsertField) {
                targetDefinition.externalIdField = upsertField;
              }
            } else if ('externalIdField' in targetDefinition) {
              delete targetDefinition.externalIdField;
            }
            if (Array.isArray(parsed.importProfiles)) {
              if (operation === 'upsert' && String(targetDefinition.externalIdField || '').trim()) {
                parsed.externalIdField = String(targetDefinition.externalIdField || '').trim();
              } else if ('externalIdField' in parsed) {
                delete parsed.externalIdField;
              }
            }
            if (objectApiName === 'PricebookEntry' && pricebook2Id) {
              targetDefinition.pricebook2Id = pricebook2Id;
            } else if ('pricebook2Id' in targetDefinition) {
              delete targetDefinition.pricebook2Id;
            }
          }
          if (isMssql) {
            if (upsertField) {
              targetDefinition.upsertKey = upsertField;
            } else if ('upsertKey' in targetDefinition) {
              delete targetDefinition.upsertKey;
            }
          }
          targetDefinitionInput.value = JSON.stringify(parsed, null, 2);
        } catch {
          targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
        }
      }

      function isSchedulerSalesforceUpsertSelection() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const targetSystem = resolveEffectiveTargetSystem();
        const operation = normalizeOperationValue(document.getElementById('sch-operation')?.value || '');
        return targetType === 'SALESFORCE' && targetSystem === 'Salesforce' && String(operation || '').toLowerCase() === 'upsert';
      }

      function getSchedulerSelectedTargetDefinitionContainer(parsed) {
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
          return null;
        }

        const baseTargetDefinition = parsed;

        if (!Array.isArray(parsed.importProfiles) || !parsed.importProfiles.length) {
          return parsed;
        }

        const selectedName = String(parsed.selectedImportProfileName || '').trim();
        const selectedProfile = (selectedName
          ? parsed.importProfiles.find((profile) => String(profile?.name || '').trim() === selectedName)
          : parsed.importProfiles[0]) || parsed.importProfiles[0];

        if (!selectedProfile || typeof selectedProfile !== 'object' || Array.isArray(selectedProfile)) {
          return parsed;
        }

        if (selectedProfile.target && typeof selectedProfile.target === 'object' && !Array.isArray(selectedProfile.target)) {
          return {
            ...baseTargetDefinition,
            ...selectedProfile.target
          };
        }

        return {
          ...baseTargetDefinition,
          ...selectedProfile
        };
      }

      function getSchedulerSelectedTargetDefinitionWritableContainer(parsed) {
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
          return null;
        }

        if (!Array.isArray(parsed.importProfiles) || !parsed.importProfiles.length) {
          return parsed;
        }

        const selectedName = String(parsed.selectedImportProfileName || '').trim();
        const selectedProfile = (selectedName
          ? parsed.importProfiles.find((profile) => String(profile?.name || '').trim() === selectedName)
          : parsed.importProfiles[0]) || parsed.importProfiles[0];

        if (!selectedProfile || typeof selectedProfile !== 'object' || Array.isArray(selectedProfile)) {
          return parsed;
        }

        if (selectedProfile.target && typeof selectedProfile.target === 'object' && !Array.isArray(selectedProfile.target)) {
          return selectedProfile.target;
        }

        return selectedProfile;
      }

      function getSchedulerTargetDefinitionUpsertFieldValue() {
        const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
        if (!raw) {
          return '';
        }

        try {
          const parsed = JSON.parse(raw);
          const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
          if (isSchedulerSalesforceUpsertSelection()) {
            return String(targetDefinition?.externalIdField || '').trim();
          }
          if (isSchedulerMssqlUpsertSelection()) {
            return String(targetDefinition?.upsertKey || '').trim();
          }
          return '';
        } catch {
          return '';
        }
      }

      function getSchedulerGlobalPicklistTargetFields() {
        const fallbackFields = [
          { name: 'ApiName', label: 'API Name', type: 'string', requiredOnCreate: true },
          { name: 'Label', label: 'Label', type: 'string', requiredOnCreate: true }
        ];
        const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
        if (!raw) {
          return fallbackFields;
        }

        try {
          const parsed = JSON.parse(raw);
          const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
          const externalIdField = String(targetDefinition?.externalIdField || '').trim() || 'ApiName';
          const labelField = String(targetDefinition?.labelField || '').trim() || 'Label';
          const fields = [
            { name: externalIdField, label: externalIdField === 'ApiName' ? 'API Name' : externalIdField, type: 'string', requiredOnCreate: true },
            { name: labelField, label: labelField, type: 'string', requiredOnCreate: true }
          ];
          const seen = new Set();
          return fields.filter((field) => {
            const key = normalizeFieldKey(field.name);
            if (!key || seen.has(key)) {
              return false;
            }
            seen.add(key);
            return true;
          });
        } catch {
          return fallbackFields;
        }
      }

      function getSchedulerMappingRules() {
        const raw = String(document.getElementById('sch-mapping')?.value || '').trim();
        if (!raw || !raw.startsWith('[')) {
          return [];
        }

        try {
          const parsed = JSON.parse(raw);
          return Array.isArray(parsed) ? parsed : [];
        } catch {
          return [];
        }
      }

      function getSchedulerMappedSourceFieldsForTargetField(targetField) {
        const normalizedTargetField = normalizeFieldKey(targetField);
        if (!normalizedTargetField) {
          return [];
        }

        return getSchedulerMappingRules()
          .filter((rule) => normalizeFieldKey(rule?.targetField) === normalizedTargetField)
          .map((rule) => String(rule?.sourceField || '').trim())
          .filter(Boolean);
      }

      function getSchedulerSalesforceUpsertHeuristicWarning() {
        if (!isSchedulerSalesforceUpsertSelection()) {
          return '';
        }

        const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
        if (!upsertField) {
          return '';
        }

        const mappedSourceFields = getSchedulerMappedSourceFieldsForTargetField(upsertField);
        if (!mappedSourceFields.length) {
          return '';
        }

        const primarySourceField = mappedSourceFields[0];
        const normalizedUpsertField = normalizeFieldKey(upsertField);
        const normalizedPrimarySourceField = normalizeFieldKey(primarySourceField);
        const sourceDefinitionText = String(document.getElementById('sch-source-definition')?.value || '').trim().toLowerCase();

        let suggestedSourceField = '';
        if ((normalizedUpsertField === 'erpaccountnumberc' || normalizedUpsertField === 'erpcontactnumberc') && sourceDefinitionText.includes('external_key')) {
          suggestedSourceField = 'external_key';
        } else if (normalizedUpsertField === 'erpordernumberc' && sourceDefinitionText.includes('order_number')) {
          suggestedSourceField = 'order_number';
        } else if (normalizedUpsertField === 'erpquotenumberc' && sourceDefinitionText.includes('quote_number')) {
          suggestedSourceField = 'quote_number';
        } else if (normalizedUpsertField === 'erpproductcodec' && sourceDefinitionText.includes('product_code')) {
          suggestedSourceField = 'product_code';
        }

        if (!suggestedSourceField || normalizeFieldKey(suggestedSourceField) === normalizedPrimarySourceField) {
          return '';
        }

        return 'Hinweis: Das Upsert-Feld ' + upsertField + ' ist aktuell mit ' + primarySourceField + ' verknuepft. Die Quelle enthaelt auch ' + suggestedSourceField + ', was fuer dieses Feld typischer wirkt.';
      }

      function hasSchedulerPricebook2IdMapping() {
        return getSchedulerMappingRules().some((rule) => String(rule?.targetField || '').trim() === 'Pricebook2Id');
      }

      function getSchedulerMappedStaticPricebook2IdValue() {
        const match = getSchedulerMappingRules().find((rule) => (
          String(rule?.targetField || '').trim() === 'Pricebook2Id'
          && String(rule?.transformFunction || '').trim().toUpperCase() === 'STATIC'
          && String(rule?.transformExpression || '').trim()
        ));

        return String(match?.transformExpression || '').trim();
      }

      function getSchedulerTargetDefinitionPricebook2IdValue() {
        const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
        if (!raw) {
          return '';
        }

        try {
          const parsed = JSON.parse(raw);
          const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
          return String(targetDefinition?.pricebook2Id || '').trim();
        } catch {
          return '';
        }
      }

      function hasSchedulerPricebook2IdConfigured() {
        return Boolean(
          String(document.getElementById('sch-pricebook2id')?.value || '').trim()
          || getSchedulerTargetDefinitionPricebook2IdValue()
          || getSchedulerMappedStaticPricebook2IdValue()
          || hasSchedulerPricebook2IdMapping()
        );
      }

      function isSchedulerPricebookEntryProductCodeSelection() {
        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
        return isSchedulerSalesforceUpsertSelection() && objectApiName === 'PricebookEntry' && upsertField === 'ProductCode';
      }

      function syncSchedulerTargetDefinitionEditorState() {
        const targetDefinitionInput = document.getElementById('sch-target-definition');
        const targetDefinitionHelp = document.getElementById('sch-target-definition-help');
        const shouldMirrorReadonly = isSchedulerSalesforceUpsertSelection()
          && String(document.getElementById('sch-object')?.value || '').trim() === 'PricebookEntry';

        if (targetDefinitionInput) {
          targetDefinitionInput.readOnly = shouldMirrorReadonly;
          targetDefinitionInput.classList.toggle('bg-light', shouldMirrorReadonly);
        }

        if (targetDefinitionHelp) {
          targetDefinitionHelp.textContent = shouldMirrorReadonly
            ? 'Wird aus Objekt, Operation, Upsert-Feld und Pricebook2Id gespiegelt. Fuer PricebookEntry bitte die sichtbaren Felder oberhalb verwenden.'
            : '';
        }
      }

      function getSchedulerSalesforceUpsertConstraintMessage() {
        if (!isSchedulerSalesforceUpsertSelection()) {
          return '';
        }

        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();

        if (!upsertField) {
          return 'Bitte waehle ein Upsert-Feld fuer die Salesforce-Zielkonfiguration.';
        }

        const rawTargetDefinition = String(document.getElementById('sch-target-definition')?.value || '').trim();
        if (rawTargetDefinition) {
          try {
            const parsed = JSON.parse(rawTargetDefinition);
            if (Array.isArray(parsed?.importProfiles)) {
              const baseExternalIdField = String(parsed?.externalIdField || '').trim();
              if (baseExternalIdField && baseExternalIdField !== upsertField) {
                parsed.externalIdField = upsertField;
                document.getElementById('sch-target-definition').value = JSON.stringify(parsed, null, 2);
              }
            }
          } catch {
            // JSON validity is handled elsewhere.
          }
        }

        const providedTargetFieldKeys = getProvidedSchedulerTargetFieldKeys();
        if (!providedTargetFieldKeys.has(normalizeFieldKey(upsertField))) {
          return 'Das Upsert-Feld ' + upsertField + ' wird im Mapping oder als statischer Zielwert nicht bereitgestellt.';
        }

        if (objectApiName === 'PricebookEntry' && upsertField === 'ProductCode' && !hasSchedulerPricebook2IdConfigured()) {
          return 'ProductCode ist für PricebookEntry nur zulässig, wenn Pricebook2Id als Ziel-Feld oder Mapping gesetzt ist.';
        }

        return '';
      }

      function updateSchedulerExternalIdValidationState() {
        const select = document.getElementById('sch-external-id-field');
        const help = document.getElementById('sch-external-id-help');
        const pricebookInput = document.getElementById('sch-pricebook2id');
        const pricebookHelp = document.getElementById('sch-pricebook2id-help');
        const baseHelpText = String(help?.dataset.baseText || 'Wählen Sie das Feld, das für Upsert verwendet werden soll.');
        const message = getSchedulerSalesforceUpsertConstraintMessage();
        const warning = message ? '' : getSchedulerSalesforceUpsertHeuristicWarning();
        const requiresPricebook2Id = isSchedulerPricebookEntryProductCodeSelection();

        if (select) {
          select.classList.toggle('is-invalid', Boolean(message));
        }

        if (pricebookInput) {
          pricebookInput.classList.toggle('is-invalid', Boolean(message) && requiresPricebook2Id && !String(pricebookInput.value || '').trim() && !hasSchedulerPricebook2IdMapping());
        }

        if (help) {
          help.textContent = message || warning || baseHelpText;
          help.classList.toggle('text-danger', Boolean(message));
          help.classList.toggle('text-warning', !message && Boolean(warning));
        }

        if (pricebookHelp) {
          const basePricebookHelp = String(pricebookHelp.dataset.baseText || 'Optional als festes Ziel-Pricebook für PricebookEntry-Upserts.');
          pricebookHelp.textContent = message && requiresPricebook2Id ? message : basePricebookHelp;
          pricebookHelp.classList.toggle('text-danger', Boolean(message) && requiresPricebook2Id);
        }

        return message;
      }

      async function loadSchedulerPricebookOptions(preferredValue) {
        const pricebookSelect = document.getElementById('sch-pricebook2id');
        if (!pricebookSelect) {
          return [];
        }

        if (!isSchedulerSalesforceUpsertSelection() || String(document.getElementById('sch-object')?.value || '').trim() !== 'PricebookEntry') {
          pricebookSelect.innerHTML = '<option value="">- Pricebook wählen -</option>';
          pricebookSelect.value = '';
          return [];
        }

        const currentValue = String(preferredValue || getSchedulerTargetDefinitionPricebook2IdValue() || getSchedulerMappedStaticPricebook2IdValue() || '').trim();

        try {
          const res = await fetch('/api/salesforce/pricebooks?instanceId=' + encodeURIComponent(state.instanceId || ''));
          if (!res.ok) {
            pricebookSelect.innerHTML = '<option value="">Pricebooks konnten nicht geladen werden</option>';
            return [];
          }

          const payload = await res.json();
          const normalizedPricebooks = Array.isArray(payload) ? payload : [];
          pricebookSelect.innerHTML = '<option value="">- Pricebook wählen -</option>' + normalizedPricebooks.map((pricebook) => {
            const id = String(pricebook?.id || '').trim();
            const name = String(pricebook?.name || id).trim();
            const suffix = pricebook?.isStandard === true
              ? 'Standard'
              : pricebook?.isActive === true
                ? 'Aktiv'
                : 'Inaktiv';
            const label = name && name !== id ? name + ' (' + suffix + ')' : id;
            return '<option value="' + esc(id) + '"' + (currentValue === id ? ' selected' : '') + '>' + esc(label) + '</option>';
          }).join('');
          if (currentValue && !normalizedPricebooks.some((pricebook) => String(pricebook?.id || '').trim() === currentValue)) {
            pricebookSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
          }
          if (currentValue) {
            pricebookSelect.value = currentValue;
          }
          return normalizedPricebooks;
        } catch {
          pricebookSelect.innerHTML = '<option value="">Pricebooks konnten nicht geladen werden</option>';
          return [];
        }
      }

      async function loadConnectorTaskOwnerOptions(preferredValue) {
        const ownerSelect = document.getElementById('con-task-owner-id');
        if (!ownerSelect) {
          return [];
        }

        const currentValue = String(preferredValue || ownerSelect.value || '').trim();
        ownerSelect.innerHTML = '<option value="">- Benutzer wählen -</option>';

        try {
          const res = await fetch('/api/salesforce/users?instanceId=' + encodeURIComponent(state.instanceId || ''));
          if (!res.ok) {
            ownerSelect.innerHTML = '<option value="">Benutzer konnten nicht geladen werden</option>';
            if (currentValue) {
              ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (gespeichert)') + '</option>';
            }
            return [];
          }

          const payload = await res.json();
          const normalizedUsers = Array.isArray(payload) ? payload : [];
          ownerSelect.innerHTML = '<option value="">- Benutzer wählen -</option>' + normalizedUsers.map((user) => {
            const id = String(user?.id || '').trim();
            const name = String(user?.name || '').trim();
            const username = String(user?.username || '').trim();
            const labelBase = name && username && name !== username
              ? name + ' (' + username + ')'
              : (name || username || id);
            const label = user?.isActive === true ? labelBase : labelBase + ' (inaktiv)';
            return '<option value="' + esc(id) + '" data-username="' + esc(username) + '"' + (currentValue === id ? ' selected' : '') + '>' + esc(label) + '</option>';
          }).join('');
          if (currentValue && !normalizedUsers.some((user) => String(user?.id || '').trim() === currentValue)) {
            ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
          }
          if (currentValue) {
            ownerSelect.value = currentValue;
          }
          return normalizedUsers;
        } catch {
          ownerSelect.innerHTML = '<option value="">Benutzer konnten nicht geladen werden</option>';
          if (currentValue) {
            ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (gespeichert)') + '</option>';
            ownerSelect.value = currentValue;
          }
          return [];
        }
      }

      async function loadSchedulerExternalIdOptions(selectedValue) {
        const select = document.getElementById('sch-external-id-field');
        if (!select) {
          return [];
        }

        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        if (!objectApiName || !isSchedulerSalesforceUpsertSelection()) {
          select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
          select.value = '';
          return [];
        }

        try {
          const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(objectApiName) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
          if (!res.ok) {
            select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
            return [];
          }

          const fields = await res.json();
          const externalIdFields = (Array.isArray(fields) ? fields : []).filter((field) => field && field.isExternalId === true);
          const currentValue = String(selectedValue || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
          const allowPricebookProductCode = objectApiName === 'PricebookEntry' && hasSchedulerPricebook2IdConfigured();
          select.innerHTML = '<option value="">- Upsert Feld wählen -</option>' + externalIdFields.map((field) => {
            const name = String(field?.name || '').trim();
            const label = String(field?.label || '').trim();
            const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
            return '<option value="' + esc(name) + '"' + (currentValue === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
          }).join('');
          if (objectApiName === 'PricebookEntry') {
            if (allowPricebookProductCode) {
              select.innerHTML += '<option value="ProductCode"' + (currentValue === 'ProductCode' ? ' selected' : '') + '>ProductCode (Composite Key mit Pricebook2Id)</option>';
            } else if (currentValue === 'ProductCode') {
              select.innerHTML += '<option value="ProductCode" selected>ProductCode (Pricebook2Id-Mapping fehlt)</option>';
            }
          }
          if (currentValue && !externalIdFields.some((field) => String(field?.name || '').trim() === currentValue)) {
            if (!(objectApiName === 'PricebookEntry' && currentValue === 'ProductCode')) {
              select.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
            }
          }
          if (currentValue) {
            select.value = currentValue;
          }
          return externalIdFields;
        } catch {
          select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
          return [];
        }
      }

      async function syncSchedulerExternalIdUi(selectedValue) {
        const wrap = document.getElementById('sch-external-id-wrap');
        const help = document.getElementById('sch-external-id-help');
        const label = document.getElementById('sch-external-id-label');
        const select = document.getElementById('sch-external-id-field');
        const pricebookWrap = document.getElementById('sch-pricebook2id-wrap');
        const pricebookInput = document.getElementById('sch-pricebook2id');
        const pricebookHelp = document.getElementById('sch-pricebook2id-help');
        const isSalesforce = isSchedulerSalesforceUpsertSelection();
        const isMssql = isSchedulerMssqlUpsertSelection();
        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        const show = isSalesforce || isMssql;
        if (wrap) {
          wrap.classList.toggle('d-none', !show);
        }

        if (!show) {
          syncSchedulerTargetDefinitionEditorState();
          if (select) {
            select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
            select.value = '';
          }
          if (label) {
            label.textContent = 'Upsert Feld';
          }
          if (help) {
            help.textContent = 'Wählen Sie das Feld, das für Upsert verwendet werden soll.';
          }
          if (pricebookWrap) {
            pricebookWrap.classList.add('d-none');
          }
          if (pricebookInput) {
            pricebookInput.value = '';
          }
          return;
        }

        const showPricebook2Id = isSalesforce && objectApiName === 'PricebookEntry';
        syncSchedulerTargetDefinitionEditorState();
        if (pricebookWrap) {
          pricebookWrap.classList.toggle('d-none', !showPricebook2Id);
        }
        if (pricebookInput && showPricebook2Id) {
          await loadSchedulerPricebookOptions(
            String(
              pricebookInput.value
              || getSchedulerTargetDefinitionPricebook2IdValue()
              || getSchedulerMappedStaticPricebook2IdValue()
              || ''
            ).trim()
          );
        } else if (pricebookInput) {
          pricebookInput.value = '';
        }
        if (pricebookHelp) {
          pricebookHelp.dataset.baseText = showPricebook2Id
            ? 'Festes Ziel-Pricebook für PricebookEntry-Upserts auswählen. Leer lassen, wenn Pricebook2Id aus dem Mapping kommt.'
            : 'Optional als festes Ziel-Pricebook für PricebookEntry-Upserts.';
          pricebookHelp.textContent = pricebookHelp.dataset.baseText;
        }

        if (isMssql) {
          const currentValue = String(selectedValue || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
          const fields = Array.isArray(state.targetFields) ? state.targetFields : [];
          if (select) {
            select.innerHTML = '<option value="">- Upsert Feld wählen -</option>' + fields.map((field) => {
              const name = String(field?.name || '').trim();
              const optionLabel = String(field?.label || '').trim() || name;
              return '<option value="' + esc(name) + '"' + (currentValue === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
            }).join('');
            if (currentValue && !fields.some((field) => String(field?.name || '').trim() === currentValue)) {
              select.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
            }
            if (currentValue) {
              select.value = currentValue;
            }
          }
          if (help) {
            help.dataset.baseText = fields.length
              ? 'Dieses Zieltabellen-Feld wird für Upsert als Match-Kriterium verwendet. Ohne Auswahl gilt der Connector-Default.'
              : 'Zuerst Zielobjekt und Connector wählen, damit die MSSQL-Felder geladen werden können.';
            help.textContent = help.dataset.baseText;
          }
          updateSchedulerExternalIdValidationState();
          return;
        }

        const options = await loadSchedulerExternalIdOptions(selectedValue);
        if (help) {
          help.dataset.baseText = options.length
            ? 'Nur echte Salesforce External-ID-Felder werden angeboten.'
            : 'Für dieses Objekt wurden keine External-ID-Felder gefunden.';
          help.textContent = help.dataset.baseText;
        }
        updateSchedulerExternalIdValidationState();
      }

      function toggleCreateObjectFromSourceUi() {
        const wrap = document.getElementById('sch-create-object-wrap');
        if (!wrap) {
          return;
        }
        wrap.classList.toggle('d-none', !isSalesforceTargetSelection());
      }

      function setCreateObjectStatus(message, level) {
        const el = document.getElementById('sch-create-object-status');
        if (!el) {
          return;
        }

        el.textContent = message || '';
        el.classList.remove('text-secondary', 'text-success', 'text-danger', 'text-warning');
        if (level === 'success') {
          el.classList.add('text-success');
          return;
        }
        if (level === 'error') {
          el.classList.add('text-danger');
          return;
        }
        if (level === 'warning') {
          el.classList.add('text-warning');
          return;
        }
        el.classList.add('text-secondary');
      }

      function mapSourceTypeToDefaultOverride(sourceType) {
        const normalized = String(sourceType || '').trim().toLowerCase();
        if (normalized === 'boolean' || normalized === 'bool') return 'Checkbox';
        if (normalized === 'date') return 'Date';
        if (normalized === 'datetime' || normalized === 'timestamp') return 'DateTime';
        if (normalized.includes('int') || normalized === 'number' || normalized === 'double' || normalized === 'float' || normalized === 'decimal') return 'Number';
        return 'Text';
      }

      function renderCreateObjectFieldOverrides() {
        const body = document.getElementById('sch-create-object-field-overrides');
        if (!body) {
          return;
        }

        const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        if (!sourceFields.length) {
          body.innerHTML = '<tr><td colspan="3" class="text-secondary">Quellfelder laden, um Overrides zu setzen.</td></tr>';
          return;
        }

        const typeOptions = ['Text', 'Number', 'Date', 'DateTime', 'Checkbox'];
        body.innerHTML = sourceFields.map((field) => {
          const sourceName = String(field?.name || '').trim();
          const sourceType = String(field?.type || 'string').trim();
          const selected = String(state.customObjectFieldOverrides[sourceName] || mapSourceTypeToDefaultOverride(sourceType));
          return '<tr data-source-field="' + esc(sourceName) + '">' +
            '<td>' + esc(sourceName) + '</td>' +
            '<td>' + esc(sourceType) + '</td>' +
            '<td><select class="form-select form-select-sm sch-field-override-type">' +
              typeOptions.map((option) => '<option value="' + esc(option) + '" ' + (option === selected ? 'selected' : '') + '>' + esc(option) + '</option>').join('') +
            '</select></td>' +
          '</tr>';
        }).join('');

        body.querySelectorAll('.sch-field-override-type').forEach((selectEl) => {
          selectEl.addEventListener('change', (event) => {
            const row = event.target?.closest ? event.target.closest('tr[data-source-field]') : null;
            const sourceName = String(row?.getAttribute('data-source-field') || '').trim();
            if (!sourceName) {
              return;
            }
            state.customObjectFieldOverrides[sourceName] = String(event.target?.value || '').trim();
          });
        });
      }

      function normalizeFieldKey(value) {
        return String(value || '')
          .trim()
          .toLowerCase()
          .replace(/[^a-z0-9]/g, '');
      }

      async function generateSalesforceMappings(sourceFields, targetFields, options) {
        const targetObjectApiName = String(options?.targetObjectApiName || '').trim();
        const profile = String(options?.profile || (
          targetObjectApiName === 'PricebookEntry'
            ? 'salesforce-pricebook'
            : (targetObjectApiName === 'Product2' ? 'salesforce-product' : 'standard')
        )).trim();
        const response = await requestJson('/api/salesforce/generate-mapping', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sourceFields: Array.isArray(sourceFields) ? sourceFields : [],
            targetFields: Array.isArray(targetFields) ? targetFields : [],
            targetObjectApiName,
            profile
          })
        });
        return Array.isArray(response?.items) ? response.items : [];
      }

      function getSalesforceMappingAssistantProfiles(targetObjectApiName) {
        const objectName = String(targetObjectApiName || '').trim();
        const profiles = [
          { value: 'standard', label: 'Standard', hint: 'Allgemeines Salesforce-Feldmapping per Name, Label und Basis-Aliase.' },
          { value: 'salesforce-product', label: 'Produkt', hint: 'Erweitert das Mapping für Produktfelder wie Produktcode, Familie und Beschreibung.' }
        ];

        if (objectName === 'PricebookEntry' || objectName === 'Product2') {
          profiles.push({
            value: 'salesforce-pricebook',
            label: 'Preisbuch',
            hint: 'Berücksichtigt Preis-, Preisbuch- und Produkt-Lookup-Felder für Salesforce Pricebook/Preise.'
          });
        }

        return profiles;
      }

      function getDefaultSalesforceMappingAssistantProfile(targetObjectApiName) {
        const objectName = String(targetObjectApiName || '').trim();
        if (objectName === 'PricebookEntry') return 'salesforce-pricebook';
        if (objectName === 'Product2') return 'salesforce-product';
        return 'standard';
      }

      function renderSchedulerMappingAssistant() {
        const profileSelect = document.getElementById('sch-mapping-assistant-profile');
        const hint = document.getElementById('sch-mapping-assistant-hint');
        if (!profileSelect || !hint) {
          return;
        }

        const objectName = String(document.getElementById('sch-object')?.value || '').trim();
        const profiles = getSalesforceMappingAssistantProfiles(objectName);
        const currentProfile = String(state.scheduleMappingAssistantProfile || getDefaultSalesforceMappingAssistantProfile(objectName)).trim();
        const allowedProfile = profiles.some((profile) => profile.value === currentProfile)
          ? currentProfile
          : getDefaultSalesforceMappingAssistantProfile(objectName);

        profileSelect.innerHTML = profiles.map((profile) =>
          '<option value="' + esc(profile.value) + '">' + esc(profile.label) + '</option>'
        ).join('');
        profileSelect.value = allowedProfile;
        state.scheduleMappingAssistantProfile = allowedProfile;

        const selectedProfile = profiles.find((profile) => profile.value === allowedProfile) || profiles[0];
        hint.textContent = selectedProfile?.hint || 'Erzeugt Salesforce-Mapping-Vorschläge für das aktuelle Zielobjekt.';
      }

      function getMigrationMappingAssistantProfile(objectId, targetObjectApiName) {
        const storedProfile = String(migState.mappingAssistantProfilesByObjectId?.[objectId] || '').trim();
        if (storedProfile) {
          return storedProfile;
        }
        return getDefaultSalesforceMappingAssistantProfile(targetObjectApiName);
      }

      function renderMigrationMappingAssistant(obj) {
        const shell = document.getElementById('mig-mapping-assistant-shell');
        if (!shell || !obj) {
          return;
        }

        const profiles = getSalesforceMappingAssistantProfiles(obj.salesforceObject);
        const currentProfile = getMigrationMappingAssistantProfile(obj.id, obj.salesforceObject);
        const allowedProfile = profiles.some((profile) => profile.value === currentProfile)
          ? currentProfile
          : getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject);
        migState.mappingAssistantProfilesByObjectId[obj.id] = allowedProfile;

        const selectedProfile = profiles.find((profile) => profile.value === allowedProfile) || profiles[0];
        shell.innerHTML =
          '<div class="scheduler-mapping-assistant-bar">' +
            '<div class="fw-semibold small">Mapping-Assistent</div>' +
            '<div class="small text-secondary" id="mig-mapping-assistant-hint">' + esc(selectedProfile?.hint || '') + '</div>' +
            '<div class="d-flex gap-2 align-items-end ms-auto">' +
              '<select id="mig-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;">' +
                profiles.map((profile) => '<option value="' + esc(profile.value) + '"' + (profile.value === allowedProfile ? ' selected' : '') + '>' + esc(profile.label) + '</option>').join('') +
              '</select>' +
              '<button type="button" class="btn btn-primary btn-sm sch-btn-iconized" id="mig-mapping-assistant-apply"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
            '</div>' +
          '</div>';

        const profileSelect = document.getElementById('mig-mapping-assistant-profile');
        const hint = document.getElementById('mig-mapping-assistant-hint');
        profileSelect?.addEventListener('change', () => {
          const nextProfile = String(profileSelect.value || getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject)).trim();
          migState.mappingAssistantProfilesByObjectId[obj.id] = nextProfile;
          const nextSelected = profiles.find((profile) => profile.value === nextProfile) || profiles[0];
          if (hint) {
            hint.textContent = nextSelected?.hint || '';
          }
        });
      }

      function resolveSourceFieldName(value) {
        const requested = String(value || '').trim();
        if (!requested) {
          return '';
        }

        const fields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        if (!fields.length) {
          return requested;
        }

        const exact = fields.find((field) => String(field?.name || '').trim() === requested);
        if (exact?.name) {
          return String(exact.name).trim();
        }

        const requestedKey = normalizeFieldKey(requested);
        const normalizedMatch = fields.find((field) => normalizeFieldKey(field?.name) === requestedKey);
        if (normalizedMatch?.name) {
          return String(normalizedMatch.name).trim();
        }

        return requested;
      }

      function reconcileMappingRuleSourceFields() {
        if (!Array.isArray(state.mappingRules) || !state.mappingRules.length) {
          return;
        }

        state.mappingRules = state.mappingRules.map((rule) => ({
          ...rule,
          sourceField: resolveSourceFieldName(rule?.sourceField)
        }));
      }

      function parseLegacyLookupValue(value) {
        const raw = String(value || '').trim();
        if (!raw) {
          return null;
        }
        const normalized = raw.startsWith('LOOKUP[') && raw.endsWith(']')
          ? raw.slice(7, -1)
          : raw;
        const separator = normalized.includes('|') ? '|' : (normalized.includes('.') ? '.' : '');
        if (!separator) {
          return null;
        }
        const parts = normalized.split(separator);
        const lookupObject = String(parts[0] || '').trim();
        const lookupField = String(parts.slice(1).join(separator) || '').trim();
        if (!lookupObject || !lookupField) {
          return null;
        }
        return { lookupObject, lookupField };
      }

      function matchesKnownTargetField(targetField, targetFields) {
        const requested = String(targetField || '').trim();
        if (!requested) {
          return false;
        }

        const requestedKey = normalizeFieldKey(requested);
        return targetFields.some((field) => {
          const apiName = String(field?.name || '').trim();
          const label = String(field?.label || '').trim();
          return apiName === requested
            || normalizeFieldKey(apiName) === requestedKey
            || (label && normalizeFieldKey(label) === requestedKey);
        });
      }

      function refreshSchedulerMappingCompatibilityState() {
        const mappingRules = Array.isArray(state.mappingRules) ? state.mappingRules : [];
        const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        const targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
        if (!mappingRules.length || !sourceFields.length || !targetFields.length) {
          return;
        }

        const compatibleRules = mappingRules.filter((rule) => {
          const sourceField = resolveSourceFieldName(rule?.sourceField);
          const hasSourceMatch = sourceFields.some((field) => String(field?.name || '').trim() === sourceField);
          const hasTargetMatch = matchesKnownTargetField(rule?.targetField, targetFields);
          return hasSourceMatch && hasTargetMatch;
        });

        state.hasIncompatibleScheduleMappings = compatibleRules.length !== mappingRules.length;
      }

      function getOperationOptionsForTarget() {
        const targetSystem = normalizeSystemValue(document.getElementById('sch-target-system')?.value || '');
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const baseOperations = Array.isArray(state.scheduleOptions?.operations) && state.scheduleOptions.operations.length
          ? state.scheduleOptions.operations.map((item) => normalizeOperationValue(item)).filter(Boolean)
          : ['Insert', 'Update', 'Upsert', 'Delete'];

        // Global picklist sync should be idempotent and is best handled as upsert.
        if (targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
          return ['Upsert'];
        }

        if (targetSystem === 'Salesforce' || targetSystem === 'MS SQL') {
          const allowed = new Set(['Insert', 'Update', 'Upsert', 'Delete']);
          const filtered = baseOperations.filter((item) => allowed.has(item));
          return filtered.length ? filtered : ['Insert', 'Update', 'Upsert', 'Delete'];
        }

        return baseOperations;
      }

      function applyOperationOptions(selectedValue) {
        const currentValue = normalizeOperationValue(selectedValue || document.getElementById('sch-operation')?.value || '');
        renderSelectOptions('sch-operation', getOperationOptionsForTarget(), currentValue);
      }

      function renderSelectOptions(selectId, values, selectedValue) {
        const select = document.getElementById(selectId);
        if (!select) {
          return;
        }

        const normalizedValues = Array.from(new Set((values || []).filter(Boolean)));
        const finalValues = normalizedValues.slice();
        if (selectedValue && !finalValues.includes(selectedValue)) {
          finalValues.unshift(selectedValue);
        }

        select.innerHTML = '<option value="">- Wählen -</option>' + finalValues.map((value) =>
          '<option value="' + esc(value) + '">' + esc(value) + '</option>'
        ).join('');

        if (selectedValue) {
          select.value = selectedValue;
        }
      }

      function generateMappingRuleId() {
        return 'map-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
      }

      function createMappingRuleFromSource(sourceField) {
        const sourceName = resolveSourceFieldName(sourceField?.name || sourceField || '');
        const sourceKey = normalizeFieldKey(sourceName);
        const isExternalIdentifier = sourceKey.includes('externalid') || sourceKey.includes('externalkey');
        const externalIdTarget = (() => {
          try {
            const targetDefinitionRaw = String(document.getElementById('sch-target-definition')?.value || '').trim();
            if (!targetDefinitionRaw) {
              return '';
            }
            const parsed = JSON.parse(targetDefinitionRaw);
            const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
            return String(targetDefinition?.externalIdField || '').trim();
          } catch {
            return '';
          }
        })();

        return {
          id: generateMappingRuleId(),
          sourceField: sourceName,
          sourceType: String(sourceField?.type || 'string'),
          targetField: externalIdTarget || sourceName,
          lookupEnabled: isExternalIdentifier,
          lookupObject: isExternalIdentifier ? String(document.getElementById('sch-object')?.value || '').trim() : '',
          lookupField: isExternalIdentifier ? (externalIdTarget || sourceName) : '',
          transformFunction: 'NONE',
          transformExpression: '',
          picklistMappings: []
        };
      }

      function toStoredMappingRule(rule) {
        return {
          sourceField: rule.sourceField,
          sourceType: rule.sourceType,
          targetType: rule.targetType || getSchedulerMappingRuleTargetType(rule),
          targetField: rule.targetField,
          lookupEnabled: !!rule.lookupEnabled,
          lookupObject: rule.lookupObject || '',
          lookupField: rule.lookupField || '',
          transformFunction: rule.transformFunction || 'NONE',
          transformExpression: rule.transformExpression || '',
          emailValidationEnabled: rule.emailValidationEnabled === true,
          emailInvalidAction: String(rule.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY',
          picklistMappings: Array.isArray(rule.picklistMappings) ? rule.picklistMappings : []
        };
      }

      function getSchedulerMappingRuleTargetMeta(rule) {
        const targetField = String(rule?.targetField || '').trim();
        if (!targetField) {
          return null;
        }
        return (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
          normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
        ) || null;
      }

      function getSchedulerMappingRuleTargetType(rule) {
        const type = String(getSchedulerMappingRuleTargetMeta(rule)?.type || rule?.targetType || rule?.sourceType || 'string').trim().toLowerCase();
        if (['int', 'integer'].includes(type)) return 'integer';
        if (['double', 'currency', 'percent', 'number'].includes(type)) return 'number';
        if (['boolean', 'checkbox'].includes(type)) return 'boolean';
        if (['date', 'datetime'].includes(type)) return 'datetime';
        return 'string';
      }

      function extractLookupTransformDetails(value) {
        const rawValue = String(value || '').trim();
        const lookupMatch = rawValue.match(/^LOOKUP\\[([^|\\]]+)\\|([^\\]]+)\\]$/);
        if (!lookupMatch) {
          return null;
        }
        return {
          lookupObject: String(lookupMatch[1] || '').trim(),
          lookupField: String(lookupMatch[2] || '').trim()
        };
      }

      function parsePicklistMappingsText(value) {
        return String(value || '')
          .split(/\\r?\\n/)
          .map((line) => line.trim())
          .filter(Boolean)
          .map((line) => {
            const splitToken = line.includes('=>') ? '=>' : '=';
            const parts = line.split(splitToken);
            return {
              source: String(parts[0] || '').trim(),
              target: String(parts.slice(1).join(splitToken) || '').trim()
            };
          })
          .filter((item) => item.source || item.target);
      }

      function formatPicklistMappingsText(mappings) {
        if (!Array.isArray(mappings) || !mappings.length) {
          return '';
        }
        return mappings
          .map((item) => String(item?.source || '').trim() + ' => ' + String(item?.target || '').trim())
          .join('\\n');
      }

      function syncMappingDefinitionFromRules() {
        const mappingInput = document.getElementById('sch-mapping');
        if (!mappingInput) {
          return;
        }

        if (state.rawMappingEditorDirty) {
          return;
        }

        mappingInput.value = JSON.stringify(state.mappingRules.map(toStoredMappingRule), null, 2);
        state.rawMappingEditorDirty = false;
      }

      function isSchedulerSalesforceInsertOrUpsertSelection() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const targetSystem = resolveEffectiveTargetSystem();
        const operation = String(normalizeOperationValue(document.getElementById('sch-operation')?.value || '') || '').trim().toLowerCase();
        return targetType === 'SALESFORCE' && targetSystem === 'Salesforce' && (operation === 'insert' || operation === 'upsert');
      }

      function getRequiredSalesforceTargetFields() {
        if (!isSchedulerSalesforceInsertOrUpsertSelection()) {
          return [];
        }

        return (Array.isArray(state.targetFields) ? state.targetFields : []).filter((field) => field && field.requiredOnCreate === true);
      }

      function getProvidedSchedulerTargetFieldKeys() {
        const provided = new Set(
          (Array.isArray(state.mappingRules) ? state.mappingRules : [])
            .map((rule) => normalizeFieldKey(rule?.targetField))
            .filter(Boolean)
        );

        const rawTargetDefinition = String(document.getElementById('sch-target-definition')?.value || '').trim();
        if (!rawTargetDefinition) {
          return provided;
        }

        try {
          const parsed = JSON.parse(rawTargetDefinition);
          const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
          const knownFieldKeys = new Set(
            (Array.isArray(state.targetFields) ? state.targetFields : [])
              .map((field) => normalizeFieldKey(field?.name))
              .filter(Boolean)
          );

          Object.entries(targetDefinition || {}).forEach(([key, value]) => {
            const normalizedKey = normalizeFieldKey(key);
            if (!normalizedKey || !knownFieldKeys.has(normalizedKey)) {
              return;
            }
            if (typeof value === 'string') {
              if (value.trim()) {
                provided.add(normalizedKey);
              }
              return;
            }
            if (typeof value === 'number' || typeof value === 'boolean') {
              provided.add(normalizedKey);
            }
          });
        } catch {
          return provided;
        }

        return provided;
      }

      function getMissingRequiredSchedulerTargetFields() {
        const requiredFields = getRequiredSalesforceTargetFields();
        if (!requiredFields.length) {
          return [];
        }

        const providedTargetFieldKeys = getProvidedSchedulerTargetFieldKeys();
        return requiredFields.filter((field) => !providedTargetFieldKeys.has(normalizeFieldKey(field?.name)));
      }

      function getRequiredSalesforceFieldSaveMessage() {
        if (!document.getElementById('sch-active')?.checked) {
          return '';
        }

        const missingRequiredFields = getMissingRequiredSchedulerTargetFields();
        if (!missingRequiredFields.length) {
          return '';
        }

        const objectName = String(document.getElementById('sch-object')?.value || '').trim() || 'das Zielobjekt';
        return 'Aktivierung nicht moeglich: Fuer ' + objectName + ' fehlen erforderliche Zielfelder im Mapping oder in der Zielkonfiguration: '
          + missingRequiredFields.map((field) => String(field?.name || '').trim()).filter(Boolean).join(', ') + '.';
      }

      function renderRequiredSchedulerFieldStatus() {
        const status = document.getElementById('sch-mapping-required-status');
        if (!status) {
          return;
        }

        if (!isSchedulerSalesforceInsertOrUpsertSelection()) {
          status.className = 'small mt-2 text-secondary';
          status.textContent = 'Pflichtfelder werden fuer Salesforce Insert/Upsert je Zielobjekt gekennzeichnet.';
          return;
        }

        const requiredFields = getRequiredSalesforceTargetFields();
        if (!requiredFields.length) {
          status.className = 'small mt-2 text-secondary';
          status.textContent = 'Keine Pflichtfelder aus den aktuellen Salesforce-Metadaten erkannt.';
          return;
        }

        const missingRequiredFields = getMissingRequiredSchedulerTargetFields();
        const requiredLabels = requiredFields.map((field) => {
          const name = String(field?.name || '').trim();
          return name ? (name + ' *') : '';
        }).filter(Boolean).join(', ');

        if (missingRequiredFields.length) {
          const missingLabels = missingRequiredFields.map((field) => String(field?.name || '').trim()).filter(Boolean).join(', ');
          status.className = 'small mt-2 text-danger';
          status.textContent = 'Pflichtfelder: ' + requiredLabels + '. Noch offen: ' + missingLabels + '. Aktive Speicherung ist erst danach moeglich.';
          return;
        }

        status.className = 'small mt-2 text-success';
        status.textContent = 'Pflichtfelder gesetzt: ' + requiredLabels + '.';
      }

      function updateMappingDetailEditorState() {
        const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
        const status = document.getElementById('sch-map-detail-status');
        const source = document.getElementById('sch-map-detail-source');
        const target = document.getElementById('sch-map-detail-target');
        const transform = document.getElementById('sch-map-detail-transform');
        const expression = document.getElementById('sch-map-detail-transform-expression');
        const lookupEnabled = document.getElementById('sch-map-detail-lookup-enabled');
        const lookupObject = document.getElementById('sch-map-detail-lookup-object');
        const lookupField = document.getElementById('sch-map-detail-lookup-field');
        const applyButton = document.getElementById('sch-map-detail-apply');
        const deleteButton = document.getElementById('sch-map-detail-delete');

        if (!selectedRule) {
          status.textContent = 'Noch keine Mapping-Zeile ausgewählt.';
          source.value = '';
          target.value = '';
          transform.value = 'NONE';
          expression.value = '';
          lookupEnabled.checked = false;
          lookupObject.value = '';
          lookupField.value = '';
          applyButton.disabled = true;
          deleteButton.disabled = true;
          renderPicklistMappingTable([]);
          return;
        }

        status.textContent = 'Bearbeitung für Quellfeld: ' + selectedRule.sourceField;
        source.value = selectedRule.sourceField || '';
        if (selectedRule.targetField && !Array.from(target.options || []).some((option) => option.value === selectedRule.targetField)) {
          const fallbackOption = document.createElement('option');
          fallbackOption.value = selectedRule.targetField;
          fallbackOption.textContent = selectedRule.targetField;
          target.appendChild(fallbackOption);
        }
        target.value = selectedRule.targetField || '';
        transform.value = selectedRule.transformFunction || 'NONE';
        expression.value = selectedRule.transformExpression || '';
        lookupEnabled.checked = !!selectedRule.lookupEnabled;
        lookupObject.value = selectedRule.lookupObject || '';
        lookupField.value = selectedRule.lookupField || '';
        applyButton.disabled = false;
        deleteButton.disabled = false;
        renderPicklistMappingTable(selectedRule.picklistMappings || []);
      }

      function renderPicklistMappingTable(mappings) {
        const tableBody = document.getElementById('sch-map-detail-picklist-table');
        if (!tableBody) {
          return;
        }

        if (!Array.isArray(mappings) || !mappings.length) {
          tableBody.innerHTML = '<tr><td colspan="3" class="text-secondary">Keine Picklist-Mappings.</td></tr>';
          return;
        }

        tableBody.innerHTML = mappings.map((mapping, idx) => {
          return (
            '<tr data-picklist-idx="' + idx + '">' +
              '<td><input type="text" class="form-control form-control-sm picklist-source" value="' + esc(mapping.source || '') + '" /></td>' +
              '<td><input type="text" class="form-control form-control-sm picklist-target" value="' + esc(mapping.target || '') + '" /></td>' +
              '<td><button type="button" class="btn btn-sm btn-outline-danger btn-delete-picklist-entry" data-idx="' + idx + '">Löschen</button></td>' +
            '</tr>'
          );
        }).join('');

        tableBody.querySelectorAll('button.btn-delete-picklist-entry').forEach((btn) => {
          btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const idx = Number(btn.getAttribute('data-idx'));
            const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
            if (selectedRule && Array.isArray(selectedRule.picklistMappings)) {
              selectedRule.picklistMappings.splice(idx, 1);
              renderPicklistMappingTable(selectedRule.picklistMappings);
            }
          });
        });
      }

      function addPicklistMappingEntry() {
        const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
        if (!selectedRule) {
          return;
        }

        if (!Array.isArray(selectedRule.picklistMappings)) {
          selectedRule.picklistMappings = [];
        }

        selectedRule.picklistMappings.push({ source: '', target: '' });
        renderPicklistMappingTable(selectedRule.picklistMappings);
      }

      async function loadTransformFunctions() {
        try {
          const result = await requestJson('/api/mapping/transforms');
          const select = document.getElementById('sch-map-detail-transform');
          if (!select) {
            return;
          }

          const functions = Array.isArray(result.functions) ? result.functions : [];
          select.innerHTML = functions.map((fn) =>
            '<option value="' + esc(fn.id) + '" title="' + esc(fn.description || '') + '">' + esc(fn.label || fn.id) + '</option>'
          ).join('');
        } catch {
          const select = document.getElementById('sch-map-detail-transform');
          if (select) {
            select.innerHTML = '<option value="NONE">Fehler beim Laden</option>';
          }
        }
      }

      function renderTargetObjectOptions(objects, selectedValue) {
        const select = document.getElementById('sch-object');
        if (!select) {
          return;
        }

        const items = Array.isArray(objects) ? objects : [];
        select.innerHTML = '<option value="">- Wählen -</option>' + items.map((item) => {
          const value = String(item?.name || '').trim();
          const label = String(item?.label || value).trim();
          return '<option value="' + esc(value) + '">' + esc(label) + '</option>';
        }).join('');

        if (selectedValue && items.some((item) => String(item?.name || '') === selectedValue)) {
          select.value = selectedValue;
        } else if (selectedValue) {
          select.innerHTML = '<option value="">- Wählen -</option><option value="' + esc(selectedValue) + '">' + esc(selectedValue) + '</option>' + items.map((item) => {
            const value = String(item?.name || '').trim();
            const label = String(item?.label || value).trim();
            return '<option value="' + esc(value) + '">' + esc(label) + '</option>';
          }).join('');
          select.value = selectedValue;
        }
      }

      function getPreferredMssqlTargetObjectName(connectorId, selectedObjectName, objects) {
        const items = Array.isArray(objects) ? objects : [];
        const requested = String(selectedObjectName || '').trim();
        const connector = Array.isArray(state.connectors)
          ? state.connectors.find((item) => String(item?.id || '').trim() === String(connectorId || '').trim())
          : null;
        const configuredTable = String(connector?.parameters?.table || '').trim();

        if (configuredTable && items.some((item) => String(item?.name || '').trim() === configuredTable)) {
          return configuredTable;
        }

        return requested;
      }

      async function ensureMssqlTargetObjectSelection() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        if (targetType !== 'MSSQL') {
          return;
        }

        const select = document.getElementById('sch-object');
        if (!select) {
          return;
        }

        let optionValues = Array.from(select.options || [])
          .map((option) => String(option.value || '').trim())
          .filter(Boolean);
        if (!optionValues.length) {
          await loadTargetObjects(String(select.value || '').trim());
          optionValues = Array.from(select.options || [])
            .map((option) => String(option.value || '').trim())
            .filter(Boolean);
        }
        const currentValue = String(select.value || '').trim();
        if (currentValue && optionValues.includes(currentValue)) {
          return;
        }

        const connectorId = String(document.getElementById('sch-connector')?.value || '').trim();
        const preferredValue = getPreferredMssqlTargetObjectName(
          connectorId,
          currentValue,
          optionValues.map((name) => ({ name }))
        );
        const fallbackValue = String(preferredValue || optionValues[0] || '').trim();
        if (fallbackValue) {
          select.value = fallbackValue;
        }
      }

      async function loadTargetObjects(selectedObjectName) {
        const loadSeq = Number(state.targetObjectsLoadSeq || 0) + 1;
        state.targetObjectsLoadSeq = loadSeq;
        const targetSystem = resolveEffectiveTargetSystem();
        const connectorId = document.getElementById('sch-connector').value;

        if (!targetSystem) {
          renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
          renderSchedulerMappingAssistant();
          return;
        }

        try {
          const result = await requestJson('/api/targets/objects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              targetSystem,
              connectorId
            })
          });

          if (loadSeq !== state.targetObjectsLoadSeq) {
            return;
          }

          const objects = Array.isArray(result.objects) ? result.objects : [];
          if (!objects.length) {
            renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
            renderSchedulerMappingAssistant();
            return;
          }

          const preferredObjectName = normalizeSystemValue(targetSystem) === 'MS SQL'
            ? getPreferredMssqlTargetObjectName(connectorId, selectedObjectName || '', objects)
            : (selectedObjectName || '');
          renderTargetObjectOptions(objects, preferredObjectName);
          renderSchedulerMappingAssistant();
        } catch {
          if (loadSeq !== state.targetObjectsLoadSeq) {
            return;
          }
          renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
          renderSchedulerMappingAssistant();
        }
      }

      async function loadTargetFields() {
        const loadSeq = Number(state.targetFieldsLoadSeq || 0) + 1;
        state.targetFieldsLoadSeq = loadSeq;
        const targetSystem = resolveEffectiveTargetSystem();
        const objectName = document.getElementById('sch-object').value;
        const connectorId = document.getElementById('sch-connector').value;
        const select = document.getElementById('sch-map-detail-target');
        const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
        const preferredField = String(selectedRule?.targetField || '').trim();

        if (!select || !targetSystem) {
          state.targetFields = [];
          state.schedulerLookupObjects = [];
          state.schedulerLookupObjectsLoaded = false;
          state.schedulerLookupObjectsLoadPromise = null;
          state.schedulerLookupExternalIdFieldsByObject = {};
          state.schedulerLookupExternalIdFieldPromises = {};
          select.innerHTML = '<option value="">- Wählen -</option>';
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
          return;
        }

        // Clear select while loading
        select.innerHTML = '<option value="">Wird geladen...</option>';

        // Always use selected target object/table as base
        const targetObject = objectName;
        if (!targetObject) {
          state.targetFields = [];
          state.schedulerLookupObjects = [];
          state.schedulerLookupObjectsLoaded = false;
          state.schedulerLookupObjectsLoadPromise = null;
          state.schedulerLookupExternalIdFieldsByObject = {};
          state.schedulerLookupExternalIdFieldPromises = {};
          select.innerHTML = '<option value="">Zielobjekt wählen</option>';
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
          return;
        }

        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        if (targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
          const fields = getSchedulerGlobalPicklistTargetFields();
          state.targetFields = fields;
          state.schedulerLookupObjects = [];
          state.schedulerLookupObjectsLoaded = false;
          state.schedulerLookupObjectsLoadPromise = null;
          state.schedulerLookupExternalIdFieldsByObject = {};
          state.schedulerLookupExternalIdFieldPromises = {};
          const currentValue = preferredField || select.value;
          select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
            '<option value="' + esc(field.name) + '">' + esc((field.label ? field.label : field.name) + (field.requiredOnCreate === true ? ' *' : '')) + '</option>'
          ).join('');
          if (currentValue && !fields.some((field) => field.name === currentValue)) {
            select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
          }
          if (currentValue) {
            select.value = currentValue;
          }
          refreshSchedulerMappingCompatibilityState();
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
          return;
        }

        try {
          const result = await requestJson('/api/mapping/target-fields', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              targetSystem: targetSystem,
              targetObject: targetObject,
              connectorId: connectorId
            })
          });

          if (loadSeq !== state.targetFieldsLoadSeq) {
            return;
          }

          const fields = Array.isArray(result.fields) ? result.fields : [];
          state.targetFields = fields;
          const currentValue = preferredField || select.value;
          select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
            '<option value="' + esc(field.name) + '">' + esc((field.label ? field.label : field.name) + (field.requiredOnCreate === true ? ' *' : '')) + '</option>'
          ).join('');
          if (currentValue && !fields.some((f) => f.name === currentValue)) {
            select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
          }
          if (currentValue && fields.some((f) => f.name === currentValue)) {
            select.value = currentValue;
          } else if (currentValue) {
            select.value = currentValue;
          }

          if (canUseSchedulerLookupSelection()) {
            await loadSchedulerLookupObjects();
          } else {
            state.schedulerLookupObjects = [];
            state.schedulerLookupObjectsLoaded = false;
            state.schedulerLookupObjectsLoadPromise = null;
            state.schedulerLookupExternalIdFieldsByObject = {};
            state.schedulerLookupExternalIdFieldPromises = {};
          }

          refreshSchedulerMappingCompatibilityState();
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
        } catch (error) {
          if (loadSeq !== state.targetFieldsLoadSeq) {
            return;
          }
          state.targetFields = [];
          state.schedulerLookupObjects = [];
          state.schedulerLookupObjectsLoaded = false;
          state.schedulerLookupObjectsLoadPromise = null;
          state.schedulerLookupExternalIdFieldsByObject = {};
          state.schedulerLookupExternalIdFieldPromises = {};
          select.innerHTML = '<option value="">Fehler beim Laden</option>';
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
          console.error('Error loading target fields:', error);
        }
      }

      function renderMappingRulesTable() {
        const rulesBody = document.getElementById('sch-mapping-rules');
        if (!rulesBody) {
          return;
        }

        if (!state.mappingRules.length) {
          rulesBody.innerHTML = '<tr><td colspan="6" class="text-secondary">Noch keine Mapping-Regeln. Ziehen Sie Quellfelder in diese Tabelle.</td></tr>';
          updateMappingDetailEditorState();
          syncMappingDefinitionFromRules();
          renderRequiredSchedulerFieldStatus();
          renderSchedulerMappingManager();
          return;
        }

        rulesBody.innerHTML = state.mappingRules.map((rule) => {
          const isSelected = rule.id === state.selectedMappingRuleId;
          const source = esc(rule.sourceField || '-');
          const target = esc(rule.targetField || '-');
          const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
            normalizeFieldKey(field?.name) === normalizeFieldKey(rule?.targetField)
          );
          const targetDisplay = targetMeta?.requiredOnCreate === true && target !== '-'
            ? target + ' <span class="badge bg-warning-subtle text-dark border">Pflicht</span>'
            : target;
          const lookup = rule.lookupEnabled
            ? esc((rule.lookupObject || '-') + '.' + (rule.lookupField || '-'))
            : '-';
          const transform = esc(rule.transformFunction || 'NONE');
          const picklistCount = Array.isArray(rule.picklistMappings) ? rule.picklistMappings.length : 0;
          const picklist = picklistCount > 0 ? String(picklistCount) + ' Mapping(s)' : '-';

          return (
            '<tr class="' + (isSelected ? 'mapping-rule-selected' : '') + '" data-rule-id="' + esc(rule.id) + '">' +
              '<td>' + source + '</td>' +
              '<td>' + targetDisplay + '</td>' +
              '<td>' + lookup + '</td>' +
              '<td>' + transform + '</td>' +
              '<td>' + esc(picklist) + '</td>' +
              '<td><button type="button" class="btn btn-sm btn-outline-danger" data-delete-rule="' + esc(rule.id) + '">Löschen</button></td>' +
            '</tr>'
          );
        }).join('');

        rulesBody.querySelectorAll('tr[data-rule-id]').forEach((row) => {
          row.addEventListener('click', () => {
            const ruleId = row.getAttribute('data-rule-id');
            state.selectedMappingRuleId = ruleId || '';
            renderMappingRulesTable();
          });
        });

        rulesBody.querySelectorAll('button[data-delete-rule]').forEach((button) => {
          button.addEventListener('click', (event) => {
            event.stopPropagation();
            const ruleId = button.getAttribute('data-delete-rule');
            state.mappingRules = state.mappingRules.filter((rule) => rule.id !== ruleId);
            if (state.selectedMappingRuleId === ruleId) {
              state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
            }
            renderMappingRulesTable();
          });
        });

        updateMappingDetailEditorState();
        syncMappingDefinitionFromRules();
        renderRequiredSchedulerFieldStatus();
        renderSchedulerMappingManager();
      }

      function setupMappingDropZone() {
        const dropzone = document.getElementById('sch-mapping-rules-dropzone');
        if (!dropzone || dropzone.dataset.dndBound === '1') {
          return;
        }

        dropzone.dataset.dndBound = '1';

        dropzone.addEventListener('dragover', (event) => {
          event.preventDefault();
          dropzone.classList.add('mapping-dropzone-active');
        });

        dropzone.addEventListener('dragleave', () => {
          dropzone.classList.remove('mapping-dropzone-active');
        });

        dropzone.addEventListener('drop', (event) => {
          event.preventDefault();
          dropzone.classList.remove('mapping-dropzone-active');
          const data = event.dataTransfer.getData('application/json') || event.dataTransfer.getData('text/plain');
          if (!data) {
            return;
          }

          let sourceField;
          try {
            sourceField = JSON.parse(data);
          } catch {
            sourceField = { name: String(data || '').trim(), type: 'string' };
          }

          if (!sourceField || !sourceField.name) {
            return;
          }

          const newRule = createMappingRuleFromSource(sourceField);
          state.mappingRules.push(newRule);
          state.selectedMappingRuleId = newRule.id;
          renderMappingRulesTable();
        });
      }

      function getSchedulerMappingManagerSources() {
        const byKey = new Map();
        const loadedFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        loadedFields.forEach((field) => {
          const name = resolveSourceFieldName(field?.name || field || '');
          const key = normalizeFieldKey(name);
          if (name && key && !byKey.has(key)) {
            byKey.set(key, {
              name,
              label: String(field?.label || '').trim(),
              type: String(field?.type || 'string').trim() || 'string'
            });
          }
        });
        // Fallback: wenn keine Felder vom Connector geladen wurden (z.B. Remote-Agent),
        // Quellfelder aus den gespeicherten Mapping-Regeln (Salesforce) synthetisieren.
        if (byKey.size === 0) {
          (Array.isArray(state.mappingRules) ? state.mappingRules : []).forEach((rule) => {
            const name = resolveSourceFieldName(String(rule?.sourceField || '').trim());
            const key = normalizeFieldKey(name);
            if (name && key && !byKey.has(key)) {
              byKey.set(key, {
                name,
                label: '',
                type: String(rule?.sourceType || 'string').trim() || 'string'
              });
            }
          });
        }
        return Array.from(byKey.values()).sort((a, b) => {
          const left = String(a?.label || a?.name || '').trim();
          const right = String(b?.label || b?.name || '').trim();
          return left.localeCompare(right, 'de', { sensitivity: 'base', numeric: true });
        });
      }

      function parseCompactPicklistMappings(value) {
        return String(value || '')
          .split(/[;\\n]/)
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => {
            const splitToken = part.includes('=>') ? '=>' : '=';
            const pieces = part.split(splitToken);
            return {
              source: String(pieces[0] || '').trim(),
              target: String(pieces.slice(1).join(splitToken) || '').trim()
            };
          })
          .filter((entry) => entry.source || entry.target);
      }

      function formatCompactPicklistMappings(mappings) {
        return (Array.isArray(mappings) ? mappings : [])
          .map((entry) => String(entry?.source || '').trim() + '=' + String(entry?.target || '').trim())
          .filter((part) => part !== '=')
          .join('; ');
      }

      function findSchedulerMappingRuleBySource(sourceName) {
        const sourceKey = normalizeFieldKey(sourceName);
        if (!sourceKey) {
          return null;
        }
        return (Array.isArray(state.mappingRules) ? state.mappingRules : []).find((rule) =>
          normalizeFieldKey(rule?.sourceField) === sourceKey
        ) || null;
      }

      function getSchedulerMappingTargetOptions(selectedValue) {
        const selected = String(selectedValue || '').trim();
        const fields = Array.isArray(state.targetFields) ? state.targetFields : [];
        const visibleFields = fields
          .filter((field) => isSchedulerMappingTargetFieldVisible(field, selected))
          .slice()
          .sort((a, b) => {
            const left = String(a?.label || a?.name || '').trim();
            const right = String(b?.label || b?.name || '').trim();
            return left.localeCompare(right, 'de', { sensitivity: 'base', numeric: true });
          });
        const hasSelected = selected && visibleFields.some((field) => String(field?.name || '') === selected);
        return '<option value=""' + (!selected ? ' selected' : '') + '>Zielfeld wählen</option>' + visibleFields.map((field) => {
          const name = String(field?.name || '').trim();
          const label = String(field?.label || '').trim();
          const display = label && label !== name ? label + ' - ' + name : name;
          const meta = [
            field?.requiredOnCreate === true ? 'Pflicht' : '',
            field?.isExternalId === true ? 'External ID' : ''
          ].filter(Boolean).join(', ');
          return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(display + (meta ? ' (' + meta + ')' : '')) + '</option>';
        }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>Manuell: ' + esc(selected) + '</option>' : '');
      }

      function isSchedulerMappingTargetFieldVisible(field, selectedValue) {
        const name = String(field?.name || '').trim();
        if (!name) {
          return false;
        }
        if (selectedValue && normalizeFieldKey(name) === normalizeFieldKey(selectedValue)) {
          return true;
        }

        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        if (targetType !== 'SALESFORCE') {
          return true;
        }

        const createable = field?.createable === true;
        const updateable = field?.updateable === true;
        if (createable || updateable || field?.isExternalId === true) {
          return true;
        }

        const lowerName = name.toLowerCase();
        return ![
          'id',
          'createddate',
          'createdbyid',
          'lastmodifieddate',
          'lastmodifiedbyid',
          'systemmodstamp',
          'lastvieweddate',
          'lastreferenceddate',
          'isdeleted'
        ].includes(lowerName);
      }

      function canUseSchedulerLookupSelection() {
        return isSalesforceTargetSelection()
          && String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase() === 'SALESFORCE';
      }

      async function loadSchedulerLookupObjects() {
        if (!canUseSchedulerLookupSelection()) {
          state.schedulerLookupObjects = [];
          state.schedulerLookupObjectsLoaded = false;
          state.schedulerLookupObjectsLoadPromise = null;
          return [];
        }
        if (state.schedulerLookupObjectsLoaded && Array.isArray(state.schedulerLookupObjects) && state.schedulerLookupObjects.length) {
          return state.schedulerLookupObjects;
        }
        if (state.schedulerLookupObjectsLoadPromise) {
          return state.schedulerLookupObjectsLoadPromise;
        }

        state.schedulerLookupObjectsLoadPromise = (async () => {
          try {
            const result = await requestJson('/api/salesforce/objects?instanceId=' + encodeURIComponent(state.instanceId || ''));
            const objects = Array.isArray(result) ? result : [];
            state.schedulerLookupObjects = objects
              .map((item) => ({
                name: String(item?.name || '').trim(),
                label: String(item?.label || item?.name || '').trim()
              }))
              .filter((item) => item.name);
            state.schedulerLookupObjectsLoaded = true;
            return state.schedulerLookupObjects;
          } catch {
            state.schedulerLookupObjects = [];
            state.schedulerLookupObjectsLoaded = false;
            return [];
          } finally {
            state.schedulerLookupObjectsLoadPromise = null;
          }
        })();

        return state.schedulerLookupObjectsLoadPromise;
      }

      async function loadSchedulerLookupExternalIdFields(objectName) {
        const normalizedObject = String(objectName || '').trim();
        if (!normalizedObject || !canUseSchedulerLookupSelection()) {
          return [];
        }
        if (Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[normalizedObject])) {
          return state.schedulerLookupExternalIdFieldsByObject[normalizedObject];
        }
        if (state.schedulerLookupExternalIdFieldPromises?.[normalizedObject]) {
          return state.schedulerLookupExternalIdFieldPromises[normalizedObject];
        }

        state.schedulerLookupExternalIdFieldPromises[normalizedObject] = (async () => {
          try {
            const result = await requestJson('/api/salesforce/object-fields?object=' + encodeURIComponent(normalizedObject) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
            const fields = (Array.isArray(result) ? result : [])
              .filter((field) => field && field.isExternalId === true)
              .map((field) => ({
                name: String(field?.name || '').trim(),
                label: String(field?.label || field?.name || '').trim()
              }))
              .filter((field) => field.name);
            state.schedulerLookupExternalIdFieldsByObject[normalizedObject] = fields;
            return fields;
          } catch {
            state.schedulerLookupExternalIdFieldsByObject[normalizedObject] = [];
            return [];
          } finally {
            delete state.schedulerLookupExternalIdFieldPromises[normalizedObject];
          }
        })();

        return state.schedulerLookupExternalIdFieldPromises[normalizedObject];
      }

      function getSchedulerLookupObjectOptions(selectedValue) {
        const selected = String(selectedValue || '').trim();
        const objects = Array.isArray(state.schedulerLookupObjects) ? state.schedulerLookupObjects : [];
        const hasSelected = selected && objects.some((item) => String(item?.name || '') === selected);
        return '<option value="">- SF Objekt wählen -</option>' + objects.map((item) => {
          const name = String(item?.name || '').trim();
          const label = String(item?.label || name).trim();
          return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(label) + '</option>';
        }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>' + esc(selected + ' (gespeichert)') + '</option>' : '');
      }

      function getSchedulerLookupFieldOptions(lookupObject, selectedValue) {
        const selected = String(selectedValue || '').trim();
        const normalizedObject = String(lookupObject || '').trim();
        const fields = normalizedObject && Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[normalizedObject])
          ? state.schedulerLookupExternalIdFieldsByObject[normalizedObject]
          : [];
        const hasSelected = selected && fields.some((item) => String(item?.name || '') === selected);
        const fallbackLabel = selected ? selected + ' (keine External ID)' : '';
        return '<option value="">- Ext-ID Feld wählen -</option>' + fields.map((item) => {
          const name = String(item?.name || '').trim();
          const label = String(item?.label || name).trim();
          const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
          return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
        }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>' + esc(fallbackLabel) + '</option>' : '');
      }

      function renderSchedulerMappingManager() {
        const shell = document.getElementById('sch-mapping-manager');
        if (!shell) {
          return;
        }

        const sources = getSchedulerMappingManagerSources();
        const useLookupSelection = canUseSchedulerLookupSelection();
        if (useLookupSelection && !state.schedulerLookupObjectsLoaded && !state.schedulerLookupObjectsLoadPromise) {
          loadSchedulerLookupObjects().then(() => {
            renderSchedulerMappingManager();
          }).catch(() => {
            // keep current UI state when lookup metadata load fails
          });
        }

        const lookupObjectsToPreload = new Set(
          (Array.isArray(state.mappingRules) ? state.mappingRules : [])
            .map((rule) => String(rule?.lookupObject || '').trim())
            .filter(Boolean)
        );
        if (useLookupSelection && lookupObjectsToPreload.size) {
          lookupObjectsToPreload.forEach((objectName) => {
            if (Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[objectName]) || state.schedulerLookupExternalIdFieldPromises?.[objectName]) {
              return;
            }
            loadSchedulerLookupExternalIdFields(objectName).then(() => {
              renderSchedulerMappingManager();
            }).catch(() => {
              // ignore lookup preload errors in manager rendering
            });
          });
        }

        const mappedCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) =>
          String(rule?.sourceField || '').trim() && String(rule?.targetField || '').trim()
        ).length;
        const lookupCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) => rule?.lookupEnabled === true).length;
        const picklistCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) =>
          Array.isArray(rule?.picklistMappings) && rule.picklistMappings.length > 0
        ).length;
        const missingRequiredCount = getMissingRequiredSchedulerTargetFields().length;

        if (!sources.length) {
          shell.innerHTML =
            '<div class="migration-mapping-toolbar scheduler-mapping-toolbar">' +
              '<div>' +
                '<div class="fw-semibold">Mappingmanager</div>' +
                '<div class="small text-secondary">Quellfelder laden, um die zeilenweise Zuordnung zu bearbeiten.</div>' +
              '</div>' +
              '<div class="d-flex gap-2 align-items-center">' +
                '<button id="sch-automapping" type="button" class="btn btn-outline-success btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M7.1 1.2a.75.75 0 0 1 .8 0l4.5 2.6a.75.75 0 0 1 0 1.3L7.9 7.7a.75.75 0 0 1-.8 0L2.6 5.1a.75.75 0 0 1 0-1.3zm-3 5 3 1.7v3.4l-4.5-2.6a.75.75 0 0 1-.4-.65V6.2a.75.75 0 0 0 1.9 0zm7.8 0a.75.75 0 0 0 1.9 0v1.85a.75.75 0 0 1-.4.65l-4.5 2.6V7.9z"/></svg><span>Auto-Mapping</span></button>' +
                '<button id="sch-manager-load-fields" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1a7 7 0 1 0 6.3 10h-2.1A5 5 0 1 1 8 3v2.2l3.3-2.8L8 0z"/></svg><span>Felder laden</span></button>' +
              '</div>' +
            '</div>' +
            '<div class="scheduler-mapping-assistant-bar">' +
              '<div class="fw-semibold small">Mapping-Assistent</div>' +
              '<div class="small text-secondary" id="sch-mapping-assistant-hint">Wählt ein Salesforce-Profil und erzeugt Vorschläge für Ziel- und Lookup-Felder.</div>' +
              '<div class="d-flex gap-2 align-items-end ms-auto">' +
                '<select id="sch-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;"></select>' +
                '<button id="sch-mapping-assistant-apply" type="button" class="btn btn-primary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
              '</div>' +
            '</div>';
          document.getElementById('sch-manager-load-fields')?.addEventListener('click', loadMappingFields);
          renderSchedulerMappingAssistant();
          return;
        }

        shell.innerHTML =
          '<div class="migration-mapping-overview scheduler-mapping-overview">' +
            '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(mappedCount)) + '</span><span class="migration-mapping-stat-label">gemappt</span></div>' +
            '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(Math.max(0, sources.length - mappedCount))) + '</span><span class="migration-mapping-stat-label">offen</span></div>' +
            '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(lookupCount)) + '</span><span class="migration-mapping-stat-label">Lookups</span></div>' +
            '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(missingRequiredCount)) + '</span><span class="migration-mapping-stat-label">Pflicht offen</span></div>' +
          '</div>' +
          '<div class="sch-mapping-manager-shell">' +
          '<div class="migration-mapping-toolbar scheduler-mapping-toolbar">' +
            '<div>' +
              '<div class="fw-semibold">Mappingmanager</div>' +
              '<div class="small text-secondary">Quellfelder links, Zielfelder rechts. Details nur öffnen, wenn Lookup, Transform oder Picklist gebraucht werden.</div>' +
            '</div>' +
            '<div class="d-flex gap-2 align-items-center">' +
              '<input class="form-control form-control-sm migration-mapping-search" type="search" placeholder="Quelle oder Ziel suchen" data-sch-map-filter>' +
              '<button id="sch-mapping-preview-btn" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 3C3.7 3 1.4 7.2 1 8c.4.8 2.7 5 7 5s6.6-4.2 7-5c-.4-.8-2.7-5-7-5m0 8a3 3 0 1 1 0-6 3 3 0 0 1 0 6"/></svg><span>Vorschau</span></button>' +
              '<button id="sch-automapping" type="button" class="btn btn-outline-success btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M7.1 1.2a.75.75 0 0 1 .8 0l4.5 2.6a.75.75 0 0 1 0 1.3L7.9 7.7a.75.75 0 0 1-.8 0L2.6 5.1a.75.75 0 0 1 0-1.3zm-3 5 3 1.7v3.4l-4.5-2.6a.75.75 0 0 1-.4-.65V6.2a.75.75 0 0 0 1.9 0zm7.8 0a.75.75 0 0 0 1.9 0v1.85a.75.75 0 0 1-.4.65l-4.5 2.6V7.9z"/></svg><span>Auto-Mapping</span></button>' +
              '<button id="sch-manager-load-fields" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1a7 7 0 1 0 6.3 10h-2.1A5 5 0 1 1 8 3v2.2l3.3-2.8L8 0z"/></svg><span>Aktualisieren</span></button>' +
            '</div>' +
          '</div>' +
          '<div class="scheduler-mapping-assistant-bar">' +
            '<div class="fw-semibold small">Mapping-Assistent</div>' +
            '<div class="small text-secondary" id="sch-mapping-assistant-hint">Wählt ein Salesforce-Profil und erzeugt Vorschläge für Ziel- und Lookup-Felder.</div>' +
            '<div class="d-flex gap-2 align-items-end ms-auto">' +
              '<select id="sch-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;"></select>' +
              '<button id="sch-mapping-assistant-apply" type="button" class="btn btn-primary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
            '</div>' +
          '</div>' +
          '<div id="sch-mapping-preview-section" class="sch-mapping-preview-section" aria-hidden="true">' +
            '<div class="d-flex align-items-center justify-content-between mb-2">' +
              '<span class="fw-semibold small">Mapping-Vorschau</span>' +
              '<button type="button" class="btn-close btn-sm" id="sch-mapping-preview-close" aria-label="Schließen"></button>' +
            '</div>' +
            '<div id="sch-mapping-preview-status" class="small text-secondary mb-2">Vorschau wird geladen...</div>' +
            '<div style="max-height: 220px; overflow: auto;">' +
              '<table class="table table-sm table-bordered mb-0" id="sch-mapping-preview-table">' +
                '<thead id="sch-mapping-preview-head"></thead>' +
                '<tbody id="sch-mapping-preview-body"></tbody>' +
              '</table>' +
            '</div>' +
          '</div>' +
          '<div class="migration-mapping-list scheduler-mapping-list">' +
            sources.map((source) => {
              const rule = findSchedulerMappingRuleBySource(source.name);
              const targetField = String(rule?.targetField || '').trim();
              const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
                normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
              );
              const transformFunction = String(rule?.transformFunction || 'NONE').trim() || 'NONE';
              const transformExpression = String(rule?.transformExpression || '').trim();
              const lookupEnabled = rule?.lookupEnabled === true;
              const lookupObject = String(rule?.lookupObject || '').trim();
              const lookupField = String(rule?.lookupField || '').trim();
              const picklistText = formatCompactPicklistMappings(rule?.picklistMappings);
              const isEmailTarget = String(targetMeta?.type || '').trim().toLowerCase() === 'email'
                || normalizeFieldKey(targetField).includes('email');
              const emailValidationEnabled = isEmailTarget && rule?.emailValidationEnabled === true;
              const emailInvalidAction = String(rule?.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
              const rowStatus = targetField ? 'mapped' : 'open';
              const rowStatusClass = targetField ? 'text-bg-success' : 'text-bg-light';
              const rowStatusLabel = targetField ? 'Gemappt' : 'Offen';
              const targetType = targetMeta?.type || (targetField ? 'manuell' : '');
              const detailsOpen = lookupEnabled || transformFunction !== 'NONE' || Boolean(picklistText) || isEmailTarget;
              const rowSearchText = [source.name, source.label, source.type, targetField, targetType, transformFunction, lookupObject, lookupField].join(' ').toLowerCase();
              const lookupFieldMeta = lookupObject ? state.schedulerLookupExternalIdFieldsByObject?.[lookupObject] : null;
              const lookupFieldMissing = useLookupSelection
                && lookupEnabled
                && lookupObject
                && lookupField
                && Array.isArray(lookupFieldMeta)
                && !lookupFieldMeta.some((field) => String(field?.name || '').trim() === lookupField);
              // Example value from first source preview row
              const previewRows = Array.isArray(state.sourcePreviewRows) ? state.sourcePreviewRows : [];
              const exampleValue = previewRows.length > 0
                ? String(previewRows.find((r) => r[source.name] !== undefined && r[source.name] !== null && r[source.name] !== '')?.[source.name] ?? previewRows[0]?.[source.name] ?? '').trim()
                : '';
              const exampleHtml = exampleValue
                ? '<span class="sch-manager-example-value" title="Beispielwert aus Quelle">' + esc(exampleValue.length > 28 ? exampleValue.slice(0, 28) + '…' : exampleValue) + '</span>'
                : '';
              return '<section class="migration-mapping-row scheduler-mapping-row" data-sch-map-row data-sch-map-source="' + esc(source.name) + '" data-sch-map-search="' + esc(rowSearchText) + '">' +
                '<div class="migration-mapping-row-main">' +
                  '<div class="migration-mapping-source">' +
                    '<span class="badge ' + rowStatusClass + '">' + esc(rowStatusLabel) + '</span>' +
                    '<code>' + esc(source.name) + '</code>' +
                    '<span class="small text-secondary">' + esc(source.label || source.type || 'string') + '</span>' +
                    exampleHtml +
                  '</div>' +
                  '<div class="migration-mapping-arrow" aria-hidden="true">&rarr;</div>' +
                  '<div class="migration-mapping-target">' +
                    '<select class="form-select form-select-sm" data-sch-manager-target>' + getSchedulerMappingTargetOptions(targetField) + '</select>' +
                    '<span class="badge bg-secondary migration-mapping-type">' + esc(targetType) + (targetMeta?.requiredOnCreate === true ? ' *' : '') + '</span>' +
                  '</div>' +
                  '<div class="migration-mapping-transform">' +
                    '<label class="form-label form-label-sm mb-1">Umwandlung</label>' +
                    '<select class="form-select form-select-sm" data-sch-manager-transform>' +
                      ['NONE','TRIM','UPPERCASE','LOWERCASE','TO_INTEGER','TO_DECIMAL','TO_BOOLEAN','DATETIME_ISO','STATIC'].map((fn) =>
                        '<option value="' + esc(fn) + '"' + (transformFunction === fn ? ' selected' : '') + '>' + esc(fn) + '</option>'
                      ).join('') +
                    '</select>' +
                  '</div>' +
                '</div>' +
                '<details class="migration-mapping-details"' + (detailsOpen ? ' open' : '') + '>' +
                  '<summary>Details</summary>' +
                  '<div class="migration-mapping-detail-grid">' +
                    '<div>' +
                      '<label class="form-label form-label-sm mb-1">Parameter / statischer Wert</label>' +
                      '<input class="form-control form-control-sm" value="' + esc(transformExpression) + '" placeholder="Nur bei STATIC oder Transform mit Parameter" data-sch-manager-transform-expression>' +
                    '</div>' +
                    '<div class="migration-mapping-lookup-box">' +
                      '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-sch-manager-lookup-enabled' + (lookupEnabled ? ' checked' : '') + '><label class="form-check-label small">Lookup aktivieren</label></div>' +
                      '<div class="migration-mapping-detail-grid migration-mapping-detail-grid-compact">' +
                        (useLookupSelection
                          ? '<select class="form-select form-select-sm" data-sch-manager-lookup-object>' + getSchedulerLookupObjectOptions(lookupObject) + '</select>'
                          : '<input class="form-control form-control-sm" placeholder="Lookup Objekt" value="' + esc(lookupObject) + '" data-sch-manager-lookup-object>') +
                        (useLookupSelection
                          ? '<select class="form-select form-select-sm" data-sch-manager-lookup-field>' + getSchedulerLookupFieldOptions(lookupObject, lookupField) + '</select>'
                          : '<input class="form-control form-control-sm" placeholder="Lookup Feld / External ID" value="' + esc(lookupField) + '" data-sch-manager-lookup-field>') +
                      '</div>' +
                      (lookupFieldMissing ? '<div class="small text-warning mt-1">Gespeichertes Lookup-Feld ist keine External ID mehr.</div>' : '') +
                    '</div>' +
                    '<div>' +
                      '<label class="form-label form-label-sm mb-1">Picklist-Mapping</label>' +
                      '<div style="max-height: 200px; overflow-y: auto; border: 1px solid #dee2e6; border-radius: 0.25rem; margin-bottom: 0.5rem;">' +
                        '<table class="table table-sm mb-0" data-sch-manager-picklist-table>' +
                          '<thead style="position: sticky; top: 0; background: #f8f9fa;">' +
                            '<tr><th style="width: 45%;">Quelle</th><th style="width: 45%;">Ziel</th><th style="width: 10%; text-align: center;">Aktion</th></tr>' +
                          '</thead>' +
                          '<tbody>' +
                            (Array.isArray(rule?.picklistMappings) && rule.picklistMappings.length > 0
                              ? rule.picklistMappings.map((entry, idx) =>
                                  '<tr data-picklist-idx="' + esc(String(idx)) + '">' +
                                    '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-source" type="text" value="' + esc(String(entry?.source || '')) + '" placeholder="Quellwert" style="font-size: 0.8rem;"></td>' +
                                    '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-target" type="text" value="' + esc(String(entry?.target || '')) + '" placeholder="Zielwert" style="font-size: 0.8rem;"></td>' +
                                    '<td style="padding: 0.25rem 0.5rem; text-align: center;"><button type="button" class="btn btn-sm btn-outline-danger picklist-remove" data-picklist-idx="' + esc(String(idx)) + '" style="padding: 0.1rem 0.3rem; font-size: 0.7rem;">×</button></td>' +
                                  '</tr>'
                                ).join('')
                              : '<tr><td colspan="3" class="text-secondary text-center" style="padding: 0.5rem; font-size: 0.9rem;">Keine Picklist-Einträge</td></tr>') +
                          '</tbody>' +
                        '</table>' +
                      '</div>' +
                      '<button type="button" class="btn btn-sm btn-outline-secondary picklist-add-row" style="font-size: 0.85rem;">+ Eintrag hinzufügen</button>' +
                    '</div>' +
                    '<div class="scheduler-email-options' + (isEmailTarget ? '' : ' d-none') + '" data-sch-manager-email-options style="border-top: 1px solid #dee2e6; padding-top: 0.75rem; margin-top: 0.75rem;">' +
                      '<label class="form-label form-label-sm mb-2" style="font-weight: 600;">E-Mail-Validierung</label>' +
                      '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-sch-manager-email-validation' + (emailValidationEnabled ? ' checked' : '') + ' id="email-val-' + esc(source.name) + '"><label class="form-check-label small" for="email-val-' + esc(source.name) + '">E-Mail-Adresse validieren</label></div>' +
                      '<div class="ps-3">' +
                        '<label class="form-label form-label-sm mb-2">Bei ungültiger E-Mail:</label>' +
                        '<select class="form-select form-select-sm" data-sch-manager-email-invalid-action>' +
                          '<option value="EMPTY"' + (emailInvalidAction === 'EMPTY' ? ' selected' : '') + '>→ Leer übermitteln</option>' +
                          '<option value="ERROR"' + (emailInvalidAction === 'ERROR' ? ' selected' : '') + '>→ Datensatz als fehlerhaft kennzeichnen</option>' +
                        '</select>' +
                      '</div>' +
                    '</div>' +
                  '</div>' +
                '</details>' +
              '</section>';
            }).join('') +
          '</div>' +
          '</div>';

        const updateRuleFromRow = (row) => {
          const sourceName = String(row.getAttribute('data-sch-map-source') || '').trim();
          if (!sourceName) {
            return;
          }

          const source = sources.find((item) => normalizeFieldKey(item.name) === normalizeFieldKey(sourceName)) || { name: sourceName, type: 'string' };
          const targetField = String(row.querySelector('[data-sch-manager-target]')?.value || '').trim();
          const existing = findSchedulerMappingRuleBySource(sourceName);

          if (!targetField) {
            if (existing) {
              state.mappingRules = state.mappingRules.filter((rule) => rule.id !== existing.id);
              if (state.selectedMappingRuleId === existing.id) {
                state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
              }
            }
            renderMappingRulesTable();
            return;
          }

          const rule = existing || createMappingRuleFromSource(source);
          rule.sourceField = source.name;
          rule.sourceType = source.type || rule.sourceType || 'string';
          rule.targetField = targetField;
          const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
            normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
          );
          const lookupAllowedForTarget = ['reference', 'id'].includes(String(targetMeta?.type || '').trim().toLowerCase());
          rule.targetType = getSchedulerMappingRuleTargetType({ ...rule, targetField, targetType: targetMeta?.type || rule.targetType });
          rule.transformFunction = String(row.querySelector('[data-sch-manager-transform]')?.value || 'NONE').trim() || 'NONE';
          rule.transformExpression = String(row.querySelector('[data-sch-manager-transform-expression]')?.value || '').trim();
          rule.lookupEnabled = lookupAllowedForTarget && Boolean(row.querySelector('[data-sch-manager-lookup-enabled]')?.checked);
          rule.lookupObject = String(row.querySelector('[data-sch-manager-lookup-object]')?.value || '').trim();
          rule.lookupField = String(row.querySelector('[data-sch-manager-lookup-field]')?.value || '').trim();
          if (!rule.lookupEnabled) {
            rule.lookupObject = '';
            rule.lookupField = '';
          }
          if (rule.lookupEnabled && canUseSchedulerLookupSelection()) {
            const availableLookupFields = Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[rule.lookupObject])
              ? state.schedulerLookupExternalIdFieldsByObject[rule.lookupObject]
              : null;
            if (availableLookupFields && rule.lookupField && !availableLookupFields.some((field) => String(field?.name || '').trim() === rule.lookupField)) {
              rule.lookupField = '';
            }
          }
          rule.emailValidationEnabled = Boolean(row.querySelector('[data-sch-manager-email-validation]')?.checked);
          rule.emailInvalidAction = String(row.querySelector('[data-sch-manager-email-invalid-action]')?.value || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
          
          // Read picklist mappings from table
          const picklistEntries = [];
          const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
          if (picklistTable) {
            picklistTable.querySelectorAll('tr[data-picklist-idx]').forEach((tableRow) => {
              const sourceInput = tableRow.querySelector('input.picklist-source');
              const targetInput = tableRow.querySelector('input.picklist-target');
              const source = sourceInput ? String(sourceInput.value || '').trim() : '';
              const target = targetInput ? String(targetInput.value || '').trim() : '';
              if (source || target) {
                picklistEntries.push({ source, target });
              }
            });
          }
          rule.picklistMappings = picklistEntries;
          
          if (!existing) {
            state.mappingRules.push(rule);
            state.selectedMappingRuleId = rule.id;
          }
          renderMappingRulesTable();
        };

        shell.querySelector('[data-sch-map-filter]')?.addEventListener('input', (event) => {
          const term = String(event.target?.value || '').trim().toLowerCase();
          shell.querySelectorAll('[data-sch-map-row]').forEach((row) => {
            const searchText = String(row.getAttribute('data-sch-map-search') || '').toLowerCase();
            row.classList.toggle('d-none', Boolean(term) && !searchText.includes(term));
          });
        });
        document.getElementById('sch-manager-load-fields')?.addEventListener('click', loadMappingFields);
        shell.querySelectorAll('[data-sch-map-row]').forEach((row) => {
          row.querySelectorAll('[data-sch-manager-target], [data-sch-manager-transform], [data-sch-manager-transform-expression], [data-sch-manager-lookup-enabled], [data-sch-manager-lookup-field], [data-sch-manager-email-validation], [data-sch-manager-email-invalid-action]').forEach((field) => {
            field.addEventListener('change', () => updateRuleFromRow(row));
          });

          // Picklist table inputs
          const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
          if (picklistTable) {
            picklistTable.querySelectorAll('input.picklist-source, input.picklist-target').forEach((input) => {
              input.addEventListener('change', () => updateRuleFromRow(row));
              input.addEventListener('blur', () => updateRuleFromRow(row));
            });
          }

          // Picklist remove buttons
          row.querySelectorAll('button.picklist-remove').forEach((btn) => {
            btn.addEventListener('click', (event) => {
              event.preventDefault();
              const idx = String(btn.getAttribute('data-picklist-idx') || '').trim();
              const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
              if (picklistTable) {
                const tableRow = picklistTable.querySelector('tr[data-picklist-idx="' + idx + '"]');
                if (tableRow) {
                  tableRow.remove();
                }
              }
              updateRuleFromRow(row);
            });
          });

          // Picklist add row button
          const addBtn = row.querySelector('button.picklist-add-row');
          if (addBtn) {
            addBtn.addEventListener('click', (event) => {
              event.preventDefault();
              const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
              if (picklistTable) {
                const tbody = picklistTable.querySelector('tbody');
                if (tbody) {
                  // Remove "no entries" row if exists
                  const emptyRow = tbody.querySelector('tr td.text-secondary');
                  if (emptyRow) {
                    emptyRow.closest('tr').remove();
                  }

                  const newIdx = String(Date.now());
                  const newRow = document.createElement('tr');
                  newRow.setAttribute('data-picklist-idx', newIdx);
                  newRow.innerHTML = '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-source" type="text" placeholder="Quellwert" style="font-size: 0.8rem;"></td>' +
                    '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-target" type="text" placeholder="Zielwert" style="font-size: 0.8rem;"></td>' +
                    '<td style="padding: 0.25rem 0.5rem; text-align: center;"><button type="button" class="btn btn-sm btn-outline-danger picklist-remove" data-picklist-idx="' + newIdx + '" style="padding: 0.1rem 0.3rem; font-size: 0.7rem;">×</button></td>';
                  
                  tbody.appendChild(newRow);

                  // Add event listeners to new inputs
                  newRow.querySelectorAll('input.picklist-source, input.picklist-target').forEach((input) => {
                    input.addEventListener('change', () => updateRuleFromRow(row));
                    input.addEventListener('blur', () => updateRuleFromRow(row));
                  });

                  // Add event listener to remove button
                  newRow.querySelector('button.picklist-remove').addEventListener('click', (event) => {
                    event.preventDefault();
                    newRow.remove();
                    updateRuleFromRow(row);
                  });

                  // Focus on first input
                  newRow.querySelector('input.picklist-source').focus();
                }
              }
            });
          }

          const lookupObjectSelect = row.querySelector('[data-sch-manager-lookup-object]');
          if (lookupObjectSelect) {
            lookupObjectSelect.addEventListener('change', async () => {
              const selectedLookupObject = String(lookupObjectSelect.value || '').trim();
              if (selectedLookupObject && canUseSchedulerLookupSelection()) {
                await loadSchedulerLookupExternalIdFields(selectedLookupObject);
                const lookupFieldSelect = row.querySelector('[data-sch-manager-lookup-field]');
                if (lookupFieldSelect) {
                  const currentLookupField = String(lookupFieldSelect.value || '').trim();
                  lookupFieldSelect.innerHTML = getSchedulerLookupFieldOptions(selectedLookupObject, currentLookupField);
                }
              }
              updateRuleFromRow(row);
            });
          }
        });
      }

      async function autoMapByName() {
        clearModalError();

        const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        if (!sourceFields.length) {
          showModalError('Bitte zuerst Quellfelder laden bevor Auto-Mapping ausgeführt wird.');
          return;
        }

        const selectedTargetObject = String(document.getElementById('sch-object')?.value || '').trim();
        let targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
        if (selectedTargetObject && !targetFields.length) {
          await loadTargetFields();
          targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
        }

        if (!targetFields.length) {
          showModalError(selectedTargetObject
            ? 'Zielfelder fuer ' + selectedTargetObject + ' konnten nicht geladen werden. Bitte Salesforce-Verbindung, Zielsystem und Objektberechtigungen pruefen.'
            : 'Bitte zuerst ein Zielobjekt wählen, damit Zielfelder geladen werden können.');
          return;
        }

        const generatedMappings = await generateSalesforceMappings(
          sourceFields,
          targetFields,
          {
            targetObjectApiName: selectedTargetObject,
            profile: String(state.scheduleMappingAssistantProfile || '').trim() || undefined
          }
        );

        const rulesBySourceKey = new Map();
        state.mappingRules.forEach((rule) => {
          const sourceKey = normalizeFieldKey(rule?.sourceField);
          if (!sourceKey) {
            return;
          }
          const bucket = rulesBySourceKey.get(sourceKey) || [];
          bucket.push(rule);
          rulesBySourceKey.set(sourceKey, bucket);
        });

        let updated = 0;
        let added = 0;

        for (const sourceField of sourceFields) {
          const sourceName = String(sourceField?.name || '').trim();
          const sourceKey = normalizeFieldKey(sourceName);
          if (!sourceName || !sourceKey) {
            continue;
          }

          const generated = generatedMappings.find((item) => normalizeFieldKey(item?.sourceField) === sourceKey);
          const matchedTarget = String(generated?.targetField || '').trim();
          if (!matchedTarget) {
            continue;
          }

          const existingRules = rulesBySourceKey.get(sourceKey) || [];
          const alreadyMappedToTarget = existingRules.some((rule) =>
            normalizeFieldKey(rule?.targetField) === normalizeFieldKey(matchedTarget)
          );
          if (alreadyMappedToTarget) {
            continue;
          }

          const placeholderRule = existingRules.find((rule) => {
            const target = String(rule?.targetField || '').trim();
            return !target || normalizeFieldKey(target) === sourceKey;
          });

          if (placeholderRule) {
            placeholderRule.targetField = matchedTarget;
            placeholderRule.sourceType = String(sourceField?.type || placeholderRule.sourceType || 'string');
            placeholderRule.lookupEnabled = generated?.lookupEnabled === true;
            placeholderRule.lookupObject = String(generated?.lookupObject || '');
            placeholderRule.lookupField = String(generated?.lookupField || '');
            updated += 1;
            continue;
          }

          const newRule = createMappingRuleFromSource(sourceField);
          newRule.targetField = matchedTarget;
          newRule.lookupEnabled = generated?.lookupEnabled === true;
          newRule.lookupObject = String(generated?.lookupObject || '');
          newRule.lookupField = String(generated?.lookupField || '');
          state.mappingRules.push(newRule);
          const bucket = rulesBySourceKey.get(sourceKey) || [];
          bucket.push(newRule);
          rulesBySourceKey.set(sourceKey, bucket);
          added += 1;
        }

        if (updated === 0 && added === 0) {
          showModalError('Keine gleichnamigen Felder zwischen Quelle und Ziel gefunden.');
          return;
        }

        if (!state.selectedMappingRuleId && state.mappingRules.length) {
          state.selectedMappingRuleId = state.mappingRules[0].id;
        }

        syncMappingDefinitionFromRules();
        renderMappingRulesTable();
      }

      function hydrateMappingRulesFromDefinition() {
        const mappingRaw = document.getElementById('sch-mapping').value || '';
        const raw = mappingRaw.trim();
        state.rawMappingEditorDirty = false;
        state.mappingRules = [];
        state.selectedMappingRuleId = '';

        if (!raw) {
          renderMappingRulesTable();
          return;
        }

        try {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed)) {
            state.mappingRules = parsed
              .filter((item) => item && (item.sourceField || item.targetField))
              .map((item) => {
                const storedTransformFunction = String(item.transformFunction || 'NONE').trim() || 'NONE';
                const lookupDetails = extractLookupTransformDetails(storedTransformFunction);
                const legacyLookup = parseLegacyLookupValue(item.lookup || item.lookupPath || item.lookupValue);
                return {
                  id: generateMappingRuleId(),
                  sourceField: String(item.sourceField || '').trim(),
                  sourceType: String(item.sourceType || 'string'),
                  targetField: String(item.targetField || '').trim(),
                  lookupEnabled: !!item.lookupEnabled || !!lookupDetails,
                  lookupObject: lookupDetails ? lookupDetails.lookupObject : String(item.lookupObject || legacyLookup?.lookupObject || ''),
                  lookupField: lookupDetails ? lookupDetails.lookupField : String(item.lookupField || legacyLookup?.lookupField || ''),
                  transformFunction: lookupDetails ? 'NONE' : storedTransformFunction,
                  transformExpression: String(item.transformExpression || ''),
                  targetType: String(item.targetType || item.sourceType || 'string'),
                  emailValidationEnabled: item.emailValidationEnabled === true || item?.emailValidation?.enabled === true,
                  emailInvalidAction: String(item.emailInvalidAction || item?.emailValidation?.invalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY',
                  picklistMappings: Array.isArray(item.picklistMappings) ? item.picklistMappings.map((entry) => ({
                    source: String(entry?.source || ''),
                    target: String(entry?.target || '')
                  })) : []
                };
              });
          }
        } catch {
          const dslRules = raw.split(/\\r?\\n/).map((line) => line.trim()).filter(Boolean);
          state.mappingRules = dslRules.map((line) => {
            const [leftPart, rightPart] = line.split('=');
            const [targetField] = String(leftPart || '').split(';').map((item) => item.trim());
            const rightParts = String(rightPart || '').split(';').map((item) => item.trim());
            const sourceField = rightParts[0] || '';
            const transformFunction = rightParts[1] || 'NONE';
            const lookupDetails = extractLookupTransformDetails(transformFunction);
            return {
              id: generateMappingRuleId(),
              sourceField,
              sourceType: 'string',
              targetField: targetField || sourceField,
              lookupEnabled: !!lookupDetails,
              lookupObject: lookupDetails ? lookupDetails.lookupObject : '',
              lookupField: lookupDetails ? lookupDetails.lookupField : '',
              transformFunction: lookupDetails ? 'NONE' : transformFunction,
              transformExpression: '',
              targetType: 'string',
              emailValidationEnabled: false,
              emailInvalidAction: 'EMPTY',
              picklistMappings: []
            };
          });
        }

        if (state.mappingRules.length) {
          state.selectedMappingRuleId = state.mappingRules[0].id;
        }
        reconcileMappingRuleSourceFields();
        renderMappingRulesTable();
      }

      function applySelectedMappingDetailChanges() {
        const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
        if (!selectedRule) {
          return;
        }

        selectedRule.targetField = String(document.getElementById('sch-map-detail-target').value || '').trim();
        selectedRule.transformFunction = String(document.getElementById('sch-map-detail-transform').value || 'NONE').trim() || 'NONE';
        selectedRule.transformExpression = String(document.getElementById('sch-map-detail-transform-expression').value || '').trim();
        selectedRule.lookupEnabled = !!document.getElementById('sch-map-detail-lookup-enabled').checked;
        selectedRule.lookupObject = String(document.getElementById('sch-map-detail-lookup-object').value || '').trim();
        selectedRule.lookupField = String(document.getElementById('sch-map-detail-lookup-field').value || '').trim();

        // Read picklist mappings from table
        const picklistEntries = [];
        const picklistTable = document.getElementById('sch-map-detail-picklist-table');
        if (picklistTable) {
          picklistTable.querySelectorAll('tr[data-picklist-idx]').forEach((row) => {
            const sourceInput = row.querySelector('input.picklist-source');
            const targetInput = row.querySelector('input.picklist-target');
            const source = sourceInput ? String(sourceInput.value || '').trim() : '';
            const target = targetInput ? String(targetInput.value || '').trim() : '';
            if (source || target) {
              picklistEntries.push({ source, target });
            }
          });
        }
        selectedRule.picklistMappings = picklistEntries;

        renderMappingRulesTable();
      }

      function deleteSelectedMappingRule() {
        const selectedRuleId = state.selectedMappingRuleId;
        if (!selectedRuleId) {
          return;
        }
        state.mappingRules = state.mappingRules.filter((item) => item.id !== selectedRuleId);
        state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
        renderMappingRulesTable();
      }

      async function loadScheduleOptions() {
        const response = await safeRequest('/api/schedules/options', null);
        if (!response) {
          return;
        }

        state.scheduleOptions = response;
      }

      function formatPreviewCell(value) {
        if (value === null || value === undefined || value === '') {
          return '-';
        }
        if (typeof value === 'object') {
          return JSON.stringify(value);
        }
        return String(value);
      }

      function renderGenericPreviewTable(headerId, bodyId, previewData) {
        const header = document.getElementById(headerId);
        const body = document.getElementById(bodyId);

        if (!header || !body) {
          return;
        }

        if (!Array.isArray(previewData) || previewData.length === 0) {
          header.innerHTML = '<tr><th>Keine Daten</th></tr>';
          body.innerHTML = '<tr><td class="text-secondary">Keine Vorschaudaten verfügbar</td></tr>';
          return;
        }

        const firstRecord = previewData[0] || {};
        const columns = Object.keys(firstRecord).slice(0, 10);

        header.innerHTML = '<tr>' + columns.map((col) => '<th>' + esc(col) + '</th>').join('') + '</tr>';
        body.innerHTML = previewData.slice(0, 10).map((record) =>
          '<tr>' + columns.map((col) => '<td>' + esc(formatPreviewCell(record[col])) + '</td>').join('') + '</tr>'
        ).join('');
      }

      function highlightSqlQuery(query) {
        return esc(query || '')
          .replace(/('[^']*')/g, '<span class="sql-string">$1</span>')
          .replace(/\b(SELECT|FROM|WHERE|AND|OR|ORDER|BY|GROUP|LIMIT|TOP|INNER|LEFT|RIGHT|JOIN|ON|AS|DISTINCT|INSERT|UPDATE|DELETE|INTO|VALUES|SET|LIKE|IS|NULL|NOT|ASC|DESC)\b/gi, '<span class="sql-keyword">$1</span>')
          .replace(/\b([0-9]+)\b/g, '<span class="sql-number">$1</span>');
      }

      function updateSourceQueryAssist() {
        const sourceType = document.getElementById('sch-source-type').value;
        const sourceDefinition = document.getElementById('sch-source-definition').value;
        const deltaWrap = document.getElementById('sch-source-delta-wrap');
        const deltaHelp = document.getElementById('sch-source-delta-help');
        const deltaStrategy = document.getElementById('sch-source-delta-strategy').value;
        const deltaField = document.getElementById('sch-source-delta-field').value;
        const deltaCurrentInput = document.getElementById('sch-source-delta-current');
        const deltaRecordIdInput = document.getElementById('sch-source-delta-record-id');
        const afterExportValue = String(document.getElementById('sch-source-after-export').value || '').trim();
        const afterExportWrap = document.getElementById('sch-source-after-export-wrap');
        const highlightWrap = document.getElementById('sch-source-sql-highlight-wrap');
        const highlight = document.getElementById('sch-source-sql-highlight');
        const status = document.getElementById('sch-source-test-status');
        const isSql = sourceType === 'MSSQL_SQL';
        const isFile = sourceType === 'FILE_CSV' || sourceType === 'FILE_EXCEL' || sourceType === 'FILE_JSON';
        const isRest = sourceType === 'REST_API';
        const supportsDelta = isSql || sourceType === 'SALESFORCE_SOQL';
        const supportsAfterExport = sourceType === 'SALESFORCE_SOQL';

        deltaWrap.classList.toggle('d-none', !supportsDelta);
        afterExportWrap.classList.toggle('d-none', !supportsAfterExport);
        document.getElementById('sch-source-relative-directory-wrap').classList.toggle('d-none', !isFile);
        document.getElementById('sch-source-archive-relative-directory-wrap').classList.toggle('d-none', !isFile);
        document.getElementById('sch-source-path-summary-wrap').classList.toggle('d-none', !isFile);
        if (deltaCurrentInput) {
          deltaCurrentInput.disabled = !supportsDelta;
        }
        if (deltaRecordIdInput) {
          deltaRecordIdInput.disabled = !supportsDelta || deltaStrategy !== 'datetime';
        }
        if (supportsDelta) {
          const normalizedDeltaField = String(deltaField || '').trim().toLowerCase();
          const usesMutableSalesforceTimestamp = sourceType === 'SALESFORCE_SOQL'
            && deltaStrategy === 'datetime'
            && (normalizedDeltaField === 'lastmodifieddate' || normalizedDeltaField === 'systemmodstamp')
            && !!afterExportValue;

          deltaHelp.textContent = usesMutableSalesforceTimestamp
            ? 'Warnung: After Export plus LastModifiedDate/SystemModstamp fuehrt auf demselben Salesforce-Objekt leicht zu Wiederholungsschleifen. Fuer produktive Exporte besser ID oder ein separates fachliches Delta-Feld verwenden.'
            : deltaStrategy && deltaField.trim()
              ? 'Delta aktiv: ' + deltaStrategy + ' auf Feld ' + deltaField.trim() + '. Der letzte Wert wird nach jedem Lauf gespeichert.'
              : 'Optional: Delta-Lauf ueber ein Feld aktivieren. Unterstuetzt Datum, Timestamp und ID.';
        }

        highlightWrap.classList.toggle('d-none', !isSql);
        if (isSql) {
          highlight.innerHTML = highlightSqlQuery(sourceDefinition || '-- keine SQL-Abfrage --');
          status.textContent = 'SQL-Abfrage kann direkt getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
        } else if (sourceType === 'SALESFORCE_SOQL') {
          highlight.textContent = '';
          status.textContent = 'SOQL-Abfrage kann direkt gegen Salesforce getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
        } else if (isRest) {
          highlight.textContent = '';
          status.textContent = 'REST-Quelle: Source Definition z. B. {"endpoint":"/api/customers","method":"GET","resultPath":"items"}. Es werden bis zu 10 Datensätze angezeigt.';
        } else if (isFile) {
          highlight.textContent = '';
          status.textContent = 'Datei-Quelle: Source Definition z. B. {"fileName":"datei.json","format":"json"} oder CSV/Excel. Connector muss ein Datei-Connector sein.';
        } else {
          highlight.textContent = '';
          status.textContent = 'Es werden bis zu 10 Datensätze angezeigt.';
        }

        updateScheduleFilePathSummaries();
      }

      function isFileScheduleSourceType(sourceType) {
        return sourceType === 'FILE_CSV' || sourceType === 'FILE_EXCEL' || sourceType === 'FILE_JSON';
      }

      function isFileScheduleTargetType(targetType) {
        return targetType === 'FILE_CSV' || targetType === 'FILE_EXCEL' || targetType === 'FILE_JSON';
      }

      function detectFileFormatFromName(value) {
        const normalized = String(value || '').trim().toLowerCase();
        if (normalized.endsWith('.json')) {
          return 'json';
        }
        if (normalized.endsWith('.xlsx') || normalized.endsWith('.xls')) {
          return 'excel';
        }
        if (normalized.endsWith('.csv') || normalized.endsWith('.txt')) {
          return 'csv';
        }
        return '';
      }

      function normalizeRelativeDirectoryInput(value) {
        return String(value || '')
          .trim()
          .replace(/\\\\+/g, '/')
          .split('/')
          .map((segment) => String(segment || '').trim())
          .filter((segment) => segment && segment !== '.')
          .join('/');
      }

      function tryParseJsonObject(rawValue) {
        const trimmed = String(rawValue || '').trim();
        if (!trimmed) {
          return null;
        }

        try {
          const parsed = JSON.parse(trimmed);
          return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
        } catch {
          return null;
        }
      }

      function parseScheduleFileDefinition(rawDefinition) {
        const trimmed = String(rawDefinition || '').trim();
        if (!trimmed) {
          return {
            editorText: '',
            relativeDirectory: '',
            archiveRelativeDirectory: '',
            parsed: null
          };
        }

        const parsed = tryParseJsonObject(trimmed);
        if (!parsed) {
          return {
            editorText: trimmed,
            relativeDirectory: '',
            archiveRelativeDirectory: '',
            parsed: null
          };
        }

        const editable = { ...parsed };
        const relativeDirectory = normalizeRelativeDirectoryInput(editable.relativeDirectory || '');
        const archiveRelativeDirectory = normalizeRelativeDirectoryInput(editable.archiveRelativeDirectory || '');
        delete editable.relativeDirectory;
        delete editable.archiveRelativeDirectory;

        return {
          editorText: JSON.stringify(editable, null, 2),
          relativeDirectory,
          archiveRelativeDirectory,
          parsed
        };
      }

      function buildScheduleFileDefinitionValue(textareaId, relativeDirectoryId, archiveRelativeDirectoryId) {
        const rawValue = String(document.getElementById(textareaId)?.value || '').trim();
        const relativeDirectory = normalizeRelativeDirectoryInput(document.getElementById(relativeDirectoryId)?.value || '');
        const archiveRelativeDirectory = normalizeRelativeDirectoryInput(document.getElementById(archiveRelativeDirectoryId)?.value || '');
        if (!rawValue) {
          return undefined;
        }

        const parsed = tryParseJsonObject(rawValue);
        let definition = parsed ? { ...parsed } : null;

        if (!definition && (relativeDirectory || archiveRelativeDirectory)) {
          const looksLikePath = rawValue.includes('/') || rawValue.includes('\\\\');
          definition = looksLikePath ? { filePath: rawValue } : { fileName: rawValue };
          const detectedFormat = detectFileFormatFromName(rawValue);
          if (detectedFormat) {
            definition.format = detectedFormat;
          }
        }

        if (!definition) {
          return rawValue;
        }

        if (relativeDirectory) {
          definition.relativeDirectory = relativeDirectory;
        } else {
          delete definition.relativeDirectory;
        }
        if (archiveRelativeDirectory) {
          definition.archiveRelativeDirectory = archiveRelativeDirectory;
        } else {
          delete definition.archiveRelativeDirectory;
        }

        return JSON.stringify(definition, null, 2);
      }

      function joinAgentPath(basePath, relativePath) {
        const base = String(basePath || '').trim();
        const relative = normalizeRelativeDirectoryInput(relativePath || '');
        if (!base) {
          return relative;
        }
        if (!relative) {
          return base;
        }

        const separator = base.includes('\\\\') ? '\\\\' : '/';
        const normalizedBase = base.replace(/[\\\\/]+$/, '');
        const normalizedRelative = relative.replace(/\\\//g, separator);
        return normalizedBase + separator + normalizedRelative;
      }

      function resolveScheduleFilePathDetails(mode, connector, rawDefinition) {
        const filePaths = connector && connector.filePaths ? connector.filePaths : null;
        if (!filePaths) {
          return null;
        }

        const parsed = tryParseJsonObject(rawDefinition || '');
        const relativeDirectory = normalizeRelativeDirectoryInput(parsed?.relativeDirectory || '');
        const archiveRelativeDirectory = normalizeRelativeDirectoryInput(parsed?.archiveRelativeDirectory || relativeDirectory || '');
        const isRead = mode === 'read';
        const rootPath = isRead ? filePaths.importPath : filePaths.exportPath;
        const effectiveDirectory = joinAgentPath(rootPath, relativeDirectory);
        const archiveDirectory = joinAgentPath(filePaths.archivePath, archiveRelativeDirectory);
        const explicitPath = String(parsed?.filePath || '').trim();
        const fileName = String(parsed?.fileName || '').trim();
        let effectiveFilePath = '';

        if (explicitPath) {
          if (/^[a-zA-Z]:[\\\\/]/.test(explicitPath) || explicitPath.startsWith('\\\\\\\\') || explicitPath.startsWith('/')) {
            effectiveFilePath = explicitPath;
          } else {
            effectiveFilePath = joinAgentPath(filePaths.basePath, explicitPath);
          }
        } else if (fileName) {
          effectiveFilePath = joinAgentPath(effectiveDirectory, fileName);
        }

        return {
          rootPath,
          effectiveDirectory,
          archiveDirectory,
          effectiveFilePath
        };
      }

      function buildScheduleFilePathLines(schedule) {
        const connectorId = String(schedule?.connectorId || '').trim();
        const connector = (state.connectors || []).find((item) => String(item.id || '').trim() === connectorId);
        if (!connector || !connector.filePaths) {
          return [];
        }

        const lines = [];
        if (isFileScheduleSourceType(String(schedule?.sourceType || '').trim().toUpperCase())) {
          const sourceDetails = resolveScheduleFilePathDetails('read', connector, schedule?.sourceDefinition || '');
          if (sourceDetails) {
            lines.push('Quelle: ' + sourceDetails.effectiveDirectory);
            if (sourceDetails.effectiveFilePath) {
              lines.push('Quelldatei: ' + sourceDetails.effectiveFilePath);
            }
            lines.push('Archiv: ' + sourceDetails.archiveDirectory);
          }
        }
        if (isFileScheduleTargetType(String(schedule?.targetType || '').trim().toUpperCase())) {
          const targetDetails = resolveScheduleFilePathDetails('write', connector, schedule?.targetDefinition || '');
          if (targetDetails) {
            lines.push('Ziel: ' + targetDetails.effectiveDirectory);
            if (targetDetails.effectiveFilePath) {
              lines.push('Zieldatei: ' + targetDetails.effectiveFilePath);
            }
            lines.push('Archiv: ' + targetDetails.archiveDirectory);
          }
        }
        return lines;
      }

      function renderScheduleFilePathLines(lines) {
        const entries = Array.isArray(lines) ? lines.filter(Boolean) : [];
        if (!entries.length) {
          return '<span class="text-secondary">Keine Datei-Pfade aktiv.</span>';
        }
        return entries.map((line) => '<div>' + esc(line) + '</div>').join('');
      }

      function updateScheduleFilePathSummaries() {
        const connectorId = String(document.getElementById('sch-connector')?.value || '').trim();
        const connector = (state.connectors || []).find((item) => String(item.id || '').trim() === connectorId);
        const sourceSummary = document.getElementById('sch-source-path-summary');
        const targetSummary = document.getElementById('sch-target-path-summary');
        const sourceType = String(document.getElementById('sch-source-type')?.value || '').trim().toUpperCase();
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();

        document.getElementById('sch-target-relative-directory-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));
        document.getElementById('sch-target-archive-relative-directory-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));
        document.getElementById('sch-target-path-summary-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));

        if (sourceSummary) {
          if (!isFileScheduleSourceType(sourceType)) {
            sourceSummary.textContent = 'Keine Datei-Quelle aktiv.';
          } else if (!connector || !connector.filePaths) {
            sourceSummary.textContent = 'Für Datei-Pfade bitte einen File-Connector wählen.';
          } else {
            const details = resolveScheduleFilePathDetails('read', connector, buildScheduleSourceDefinitionValue() || '');
            const lines = details
              ? [
                'Importpfad: ' + details.effectiveDirectory,
                details.effectiveFilePath ? 'Quelldatei: ' + details.effectiveFilePath : '',
                'Archivpfad: ' + details.archiveDirectory
              ].filter(Boolean)
              : ['Datei-Definition ist noch nicht vollständig.'];
            sourceSummary.innerHTML = renderScheduleFilePathLines(lines);
          }
        }

        if (targetSummary) {
          if (!isFileScheduleTargetType(targetType)) {
            targetSummary.textContent = 'Kein Datei-Ziel aktiv.';
          } else if (!connector || !connector.filePaths) {
            targetSummary.textContent = 'Für Datei-Pfade bitte einen File-Connector wählen.';
          } else {
            const details = resolveScheduleFilePathDetails('write', connector, buildScheduleTargetDefinitionValue() || '');
            const lines = details
              ? [
                'Exportpfad: ' + details.effectiveDirectory,
                details.effectiveFilePath ? 'Zieldatei: ' + details.effectiveFilePath : '',
                'Archivpfad: ' + details.archiveDirectory
              ].filter(Boolean)
              : ['Datei-Definition ist noch nicht vollständig.'];
            targetSummary.innerHTML = renderScheduleFilePathLines(lines);
          }
        }
      }

      function parseScheduleSourceDefinition(sourceType, rawDefinition) {
        const trimmed = String(rawDefinition || '').trim();
        if (isFileScheduleSourceType(String(sourceType || '').trim().toUpperCase())) {
          const fileDefinition = parseScheduleFileDefinition(trimmed);
          return {
            queryText: fileDefinition.editorText,
            deltaStrategy: '',
            deltaField: '',
            afterExportText: '',
            relativeDirectory: fileDefinition.relativeDirectory,
            archiveRelativeDirectory: fileDefinition.archiveRelativeDirectory
          };
        }
        if ((sourceType !== 'MSSQL_SQL' && sourceType !== 'SALESFORCE_SOQL') || !trimmed) {
          return { queryText: trimmed, deltaStrategy: '', deltaField: '', afterExportText: '', relativeDirectory: '', archiveRelativeDirectory: '' };
        }

        try {
          const parsed = JSON.parse(trimmed);
          const queryText = parsed && typeof parsed === 'object' && !Array.isArray(parsed)
            ? String(parsed.queryText || parsed.soql || parsed.query || '').trim()
            : '';
          if (queryText) {
            const afterExportEntries = parsed.afterExport && typeof parsed.afterExport === 'object' && !Array.isArray(parsed.afterExport)
              ? Object.entries(parsed.afterExport).map(([key, value]) => String(key || '').trim() && String(value || '').trim() ? String(key).trim() + '=' + String(value).trim() : '').filter(Boolean)
              : [];
            return {
              queryText,
              deltaStrategy: String(parsed.delta && parsed.delta.strategy || '').trim(),
              deltaField: String(parsed.delta && parsed.delta.field || '').trim(),
              afterExportText: afterExportEntries.join(','),
              relativeDirectory: '',
              archiveRelativeDirectory: ''
            };
          }
        } catch {
          // Backward compatible: plain query text.
        }

        return { queryText: trimmed, deltaStrategy: '', deltaField: '', afterExportText: '', relativeDirectory: '', archiveRelativeDirectory: '' };
      }

      function parseAfterExportAssignments(rawValue) {
        return String(rawValue || '').split(',').map((entry) => entry.trim()).filter(Boolean).reduce((acc, entry) => {
          const separatorIndex = entry.indexOf('=');
          if (separatorIndex <= 0) {
            return acc;
          }
          const fieldName = entry.slice(0, separatorIndex).trim();
          const fieldValue = entry.slice(separatorIndex + 1).trim();
          if (fieldName && fieldValue) {
            acc[fieldName] = fieldValue;
          }
          return acc;
        }, {});
      }

      function buildScheduleSourceDefinitionValue() {
        const sourceType = document.getElementById('sch-source-type').value;
        if (isFileScheduleSourceType(String(sourceType || '').trim().toUpperCase())) {
          return buildScheduleFileDefinitionValue(
            'sch-source-definition',
            'sch-source-relative-directory',
            'sch-source-archive-relative-directory'
          );
        }

        const queryText = String(document.getElementById('sch-source-definition').value || '').trim();
        if (sourceType !== 'MSSQL_SQL' && sourceType !== 'SALESFORCE_SOQL') {
          return queryText || undefined;
        }

        const deltaStrategy = String(document.getElementById('sch-source-delta-strategy').value || '').trim().toLowerCase();
        const deltaField = String(document.getElementById('sch-source-delta-field').value || '').trim();
        const afterExportUpdates = sourceType === 'SALESFORCE_SOQL'
          ? parseAfterExportAssignments(document.getElementById('sch-source-after-export').value)
          : {};
        if ((!deltaStrategy || !deltaField) && !Object.keys(afterExportUpdates).length) {
          return queryText || undefined;
        }

        const definition = {
          queryText
        };
        if (deltaStrategy && deltaField) {
          definition.delta = {
            strategy: deltaStrategy,
            field: deltaField
          };
        }
        if (Object.keys(afterExportUpdates).length) {
          definition.afterExport = afterExportUpdates;
        }

        return JSON.stringify(definition, null, 2);
      }

      function parseScheduleTargetDefinition(targetType, rawDefinition) {
        const trimmed = String(rawDefinition || '').trim();
        if (!isFileScheduleTargetType(String(targetType || '').trim().toUpperCase())) {
          return { editorText: trimmed, relativeDirectory: '', archiveRelativeDirectory: '' };
        }

        const fileDefinition = parseScheduleFileDefinition(trimmed);
        return {
          editorText: fileDefinition.editorText,
          relativeDirectory: fileDefinition.relativeDirectory,
          archiveRelativeDirectory: fileDefinition.archiveRelativeDirectory
        };
      }

      function buildScheduleTargetDefinitionValue() {
        const targetType = document.getElementById('sch-target-type').value;
        if (isFileScheduleTargetType(String(targetType || '').trim().toUpperCase())) {
          return buildScheduleFileDefinitionValue(
            'sch-target-definition',
            'sch-target-relative-directory',
            'sch-target-archive-relative-directory'
          );
        }

        return String(document.getElementById('sch-target-definition').value || '').trim() || undefined;
      }

      async function loadScheduleCheckpoint(scheduleId) {
        if (!scheduleId) {
          document.getElementById('sch-source-delta-current').value = '';
          document.getElementById('sch-source-delta-record-id').value = '';
          return;
        }

        try {
          const checkpoint = await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/checkpoint');
          document.getElementById('sch-source-delta-current').value = String(checkpoint?.lastCheckpoint || '');
          document.getElementById('sch-source-delta-record-id').value = String(checkpoint?.lastRecordId || '');
        } catch {
          document.getElementById('sch-source-delta-current').value = '';
          document.getElementById('sch-source-delta-record-id').value = '';
        }
      }

      async function saveScheduleCheckpoint(scheduleId) {
        const deltaStrategy = String(document.getElementById('sch-source-delta-strategy').value || '').trim().toLowerCase();
        const deltaField = String(document.getElementById('sch-source-delta-field').value || '').trim();
        const lastCheckpoint = String(document.getElementById('sch-source-delta-current').value || '').trim();
        const lastRecordId = String(document.getElementById('sch-source-delta-record-id').value || '').trim();
        if (!scheduleId || !deltaStrategy || !deltaField || (!lastCheckpoint && !lastRecordId)) {
          return;
        }

        await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/checkpoint', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            lastCheckpoint: lastCheckpoint || undefined,
            lastRecordId: lastRecordId || undefined
          })
        });
      }

      async function safeRequest(path, fallback) {
        try {
          return await requestJson(path);
        } catch (error) {
          showError(error.message || 'API-Fehler');
          return fallback;
        }
      }

      function currentUserHasPermission(permission) {
        const permissions = (state.adminMe && state.adminMe.user && state.adminMe.user.permissions) || [];
        return permissions.includes('admin') || permissions.includes(permission);
      }

      function currentUserHasModule(moduleName) {
        const user = state.adminMe && state.adminMe.user;
        const permissions = (user && user.permissions) || [];
        const modules = (user && user.modules) || [];
        return permissions.includes('admin') || modules.includes(moduleName);
      }

      function applyAdminAccessUi() {
        const canUseMigration = currentUserHasModule('migration');
        document.querySelectorAll('[data-bs-target="#tab-migration"], [data-menu-tab="#tab-migration"]').forEach((el) => {
          el.classList.toggle('d-none', !canUseMigration);
        });
        const canAdmin = currentUserHasPermission('admin');
        document.querySelectorAll('#open-admin-modal-sidebar, #open-admin-modal-menu').forEach((el) => {
          el.classList.toggle('d-none', !canAdmin);
        });
      }

      function resetAdminUserForm() {
        document.getElementById('admin-user-id').value = '';
        document.getElementById('admin-user-username').value = '';
        document.getElementById('admin-user-display-name').value = '';
        document.getElementById('admin-user-password').value = '';
        document.querySelectorAll('[data-admin-permission]').forEach((el) => { el.checked = el.getAttribute('data-admin-permission') === 'read'; });
        document.querySelectorAll('[data-admin-module]').forEach((el) => { el.checked = false; });
      }

      function editAdminUser(user) {
        document.getElementById('admin-user-id').value = user.id || '';
        document.getElementById('admin-user-username').value = user.username || '';
        document.getElementById('admin-user-display-name').value = user.displayName || '';
        document.getElementById('admin-user-password').value = '';
        const permissions = Array.isArray(user.permissions) ? user.permissions : [];
        const modules = Array.isArray(user.modules) ? user.modules : [];
        document.querySelectorAll('[data-admin-permission]').forEach((el) => {
          el.checked = permissions.includes(el.getAttribute('data-admin-permission'));
        });
        document.querySelectorAll('[data-admin-module]').forEach((el) => {
          el.checked = modules.includes(el.getAttribute('data-admin-module'));
        });
      }

      function renderAdminUsers() {
        const body = document.getElementById('admin-users-body');
        if (!body) return;
        const users = state.adminUsers || [];
        if (!users.length) {
          body.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine Benutzer gefunden.</td></tr>';
          return;
        }
        body.innerHTML = users.map((user) => {
          return '<tr>' +
            '<td><div class="fw-semibold">' + esc(user.displayName || user.username) + '</div><div class="small text-secondary">' + esc(user.username) + '</div></td>' +
            '<td>' + esc((user.permissions || []).join(', ') || '-') + '</td>' +
            '<td>' + esc((user.modules || []).join(', ') || '-') + '</td>' +
            '<td class="text-nowrap"><button class="btn btn-sm btn-outline-primary me-1" data-admin-user-edit="' + esc(user.id) + '">Bearbeiten</button><button class="btn btn-sm btn-outline-danger" data-admin-user-delete="' + esc(user.id) + '">Löschen</button></td>' +
            '</tr>';
        }).join('');
        body.querySelectorAll('[data-admin-user-edit]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const user = users.find((item) => item.id === btn.getAttribute('data-admin-user-edit'));
            if (user) editAdminUser(user);
          });
        });
        body.querySelectorAll('[data-admin-user-delete]').forEach((btn) => {
          btn.addEventListener('click', async () => {
            if (!confirm('Benutzer wirklich löschen?')) return;
            await requestJson('/api/admin/users/' + encodeURIComponent(btn.getAttribute('data-admin-user-delete')), { method: 'DELETE' });
            await loadAdminData();
          });
        });
      }

      function renderAuditHistory() {
        const body = document.getElementById('admin-audit-body');
        if (!body) return;
        const items = state.auditHistory || [];
        if (!items.length) {
          body.innerHTML = '<tr><td colspan="5" class="text-secondary">Keine Historie vorhanden.</td></tr>';
          return;
        }
        body.innerHTML = items.slice(0, 100).map((item) => {
          const actor = item.actor && item.actor.username ? item.actor.username : '-';
          const objectName = [item.entityType, item.entityName || item.entityId].filter(Boolean).join(': ');
          return '<tr>' +
            '<td>' + esc(formatDate(item.at, 'short')) + '</td>' +
            '<td>' + esc(actor) + '</td>' +
            '<td>' + esc(item.action || '-') + '</td>' +
            '<td>' + esc(objectName || '-') + '</td>' +
            '<td>' + esc(item.status || '-') + '</td>' +
            '</tr>';
        }).join('');
      }

      async function loadAdminData() {
        state.adminMe = await safeRequest('/api/admin/me', { user: null });
        applyAdminAccessUi();
        if (currentUserHasPermission('admin')) {
          const users = await safeRequest('/api/admin/users', { items: [] });
          const audit = await safeRequest('/api/admin/audit-history?limit=100', { items: [] });
          state.adminUsers = users.items || [];
          state.auditHistory = audit.items || [];
          renderAdminUsers();
          renderAuditHistory();
        }
      }

      async function saveAdminUserFromForm() {
        const permissions = Array.from(document.querySelectorAll('[data-admin-permission]')).filter((el) => el.checked).map((el) => el.getAttribute('data-admin-permission'));
        const modules = Array.from(document.querySelectorAll('[data-admin-module]')).filter((el) => el.checked).map((el) => el.getAttribute('data-admin-module'));
        const payload = {
          id: document.getElementById('admin-user-id').value || undefined,
          username: document.getElementById('admin-user-username').value,
          displayName: document.getElementById('admin-user-display-name').value,
          password: document.getElementById('admin-user-password').value || undefined,
          roles: permissions.includes('admin') ? ['admin'] : (permissions.includes('write') ? ['editor'] : ['viewer']),
          permissions,
          modules
        };
        await requestJson('/api/admin/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        resetAdminUserForm();
        await loadAdminData();
      }

      function renderLogChart(summary) {
        const canvas = document.getElementById('logs-chart');
        if (!canvas || typeof window.Chart !== 'function') {
          return;
        }

        if (logsChart) {
          logsChart.destroy();
        }

        const labels = (summary?.buckets || []).map((item) => item.label);
        const connectorTotals = (Array.isArray(summary?.connectors) ? summary.connectors : [])
          .map((connectorName) => ({
            connectorName,
            total: (summary?.buckets || []).reduce((sum, item) => sum + Number(item?.connectorErrors?.[connectorName] || 0), 0)
          }))
          .filter((item) => item.total > 0)
          .sort((left, right) => right.total - left.total);
        const primaryConnectors = connectorTotals.slice(0, MAX_LOG_CONNECTOR_SERIES).map((item) => item.connectorName);
        const remainingConnectors = connectorTotals.slice(MAX_LOG_CONNECTOR_SERIES).map((item) => item.connectorName);
        const palette = [
          'rgba(208, 73, 73, 1)',
          'rgba(43, 122, 184, 1)',
          'rgba(31, 125, 87, 1)',
          'rgba(194, 106, 45, 1)',
          'rgba(123, 94, 167, 1)',
          'rgba(39, 145, 132, 1)',
          'rgba(153, 72, 122, 1)',
          'rgba(93, 110, 126, 1)'
        ];
        const datasets = primaryConnectors.map((connectorName, index) => ({
          label: connectorName,
          connectorName,
          data: (summary?.buckets || []).map((item) => Number(item?.connectorErrors?.[connectorName] || 0)),
          backgroundColor: palette[index % palette.length].replace(', 1)', ', 0.14)'),
          borderColor: palette[index % palette.length],
          borderWidth: 2,
          tension: 0.35,
          fill: false,
          pointRadius: 2,
          pointHoverRadius: 4
        })).filter((dataset) => dataset.data.some((value) => value > 0));

        if (remainingConnectors.length) {
          datasets.push({
            label: 'Sonstige',
            connectorName: '',
            data: (summary?.buckets || []).map((item) => remainingConnectors.reduce((sum, connectorName) => sum + Number(item?.connectorErrors?.[connectorName] || 0), 0)),
            backgroundColor: 'rgba(93, 110, 126, 0.12)',
            borderColor: 'rgba(93, 110, 126, 0.92)',
            borderWidth: 2,
            tension: 0.35,
            fill: false,
            pointRadius: 2,
            pointHoverRadius: 4
          });
        }

        if (!datasets.length) {
          datasets.push({
            label: 'Keine Fehler',
            connectorName: '',
            data: labels.map(() => 0),
            backgroundColor: 'rgba(93, 110, 126, 0.12)',
            borderColor: 'rgba(93, 110, 126, 0.85)',
            borderWidth: 2,
            tension: 0.35,
            fill: false,
            pointRadius: 2,
            pointHoverRadius: 4
          });
        }

        logsChart = new window.Chart(canvas, {
          type: 'line',
          data: {
            labels,
            datasets
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: {
                position: 'top'
              }
            },
            scales: {
              x: {
                stacked: false
              },
              y: {
                beginAtZero: true,
                stacked: false,
                ticks: {
                  precision: 0
                }
              }
            },
            onClick: async (event, elements) => {
              if (!elements || !elements.length) {
                return;
              }

              const point = elements[0];
              const bucket = summary?.buckets?.[point.index];
              if (!bucket) {
                return;
              }

              const connectorName = logsChart?.data?.datasets?.[point.datasetIndex]?.connectorName || '';
              await openLogsByBucket(bucket, 'error', connectorName);
            }
          }
        });
      }

      function getRecordsChartRange() {
        const range = String(state.overviewStatsRange || 'month').trim();
        if (range === 'day' || range === 'month' || range === 'year') {
          return range;
        }
        return 'month';
      }

      function renderRecordsTrendChart(summary) {
        const canvas = document.getElementById('records-chart');
        if (!canvas || typeof window.Chart !== 'function') {
          return;
        }

        if (recordsChart) {
          recordsChart.destroy();
        }

        const labels = (summary?.buckets || []).map((item) => item.label);
        const connectorTotals = (Array.isArray(summary?.connectors) ? summary.connectors : [])
          .map((connectorName) => ({
            connectorName,
            total: (summary?.buckets || []).reduce((sum, item) => sum + Number(item?.connectorTotals?.[connectorName] || 0), 0)
          }))
          .filter((item) => item.total > 0)
          .sort((left, right) => right.total - left.total);
        const primaryConnectors = connectorTotals.slice(0, MAX_RECORD_CONNECTOR_SERIES).map((item) => item.connectorName);
        const remainingConnectors = connectorTotals.slice(MAX_RECORD_CONNECTOR_SERIES).map((item) => item.connectorName);
        const palette = [
          'rgba(43, 122, 184, 1)',
          'rgba(31, 125, 87, 1)',
          'rgba(194, 106, 45, 1)',
          'rgba(123, 94, 167, 1)',
          'rgba(208, 73, 73, 1)',
          'rgba(39, 145, 132, 1)',
          'rgba(153, 72, 122, 1)'
        ];
        const datasets = primaryConnectors.map((connectorName, index) => ({
          label: connectorName,
          connectorName,
          data: (summary?.buckets || []).map((item) => Number(item?.connectorTotals?.[connectorName] || 0)),
          borderColor: palette[index % palette.length],
          backgroundColor: palette[index % palette.length].replace(', 1)', ', 0.12)'),
          borderWidth: 2,
          tension: 0.35,
          fill: false,
          pointRadius: 2,
          pointHoverRadius: 4
        })).filter((dataset) => dataset.data.some((value) => value > 0));

        // Add failed records as dashed line
        const failedData = (summary?.buckets || []).map((item) => Number(item?.failed || 0));
        if (failedData.some((value) => value > 0)) {
          datasets.push({
            label: 'Fehlgeschlagene Datensätze',
            connectorName: '__failed__',
            data: failedData,
            borderColor: 'rgba(208, 73, 73, 1)',
            backgroundColor: 'rgba(208, 73, 73, 0.12)',
            borderWidth: 2,
            borderDash: [5, 5],
            tension: 0.35,
            fill: false,
            pointRadius: 2,
            pointHoverRadius: 4
          });
        }

        if (remainingConnectors.length) {
          datasets.push({
            label: 'Sonstige',
            connectorName: '__other__',
            data: (summary?.buckets || []).map((item) => remainingConnectors.reduce((sum, connectorName) => sum + Number(item?.connectorTotals?.[connectorName] || 0), 0)),
            borderColor: 'rgba(93, 110, 126, 0.92)',
            backgroundColor: 'rgba(93, 110, 126, 0.12)',
            borderWidth: 2,
            tension: 0.35,
            fill: false,
            pointRadius: 2,
            pointHoverRadius: 4
          });
        }

        if (!datasets.length) {
          datasets.push({
            label: 'Keine Datensätze',
            connectorName: '',
            data: labels.map(() => 0),
            borderColor: 'rgba(93, 110, 126, 0.85)',
            backgroundColor: 'rgba(93, 110, 126, 0.12)',
            borderWidth: 2,
            tension: 0.35,
            fill: false,
            pointRadius: 2,
            pointHoverRadius: 4
          });
        }

        recordsChart = new window.Chart(canvas, {
          type: 'line',
          data: {
            labels,
            datasets
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: {
                position: 'top'
              }
            },
            scales: {
              y: {
                beginAtZero: true,
                ticks: {
                  precision: 0
                }
              }
            },
            onClick: async (event, elements) => {
              if (!elements || !elements.length) {
                return;
              }

              const point = elements[0];
              const bucket = summary?.buckets?.[point.index];
              if (!bucket) {
                return;
              }

              const connectorName = recordsChart?.data?.datasets?.[point.datasetIndex]?.connectorName || '';
              await openRecordSchedulersByBucket(bucket, connectorName);
            }
          }
        });
      }

      async function loadRecordsSummary() {
        const range = getRecordsChartRange();
        const fallback = { range, buckets: [], connectors: [] };
        const summary = await safeRequest('/api/dashboard/records-summary?range=' + encodeURIComponent(range), fallback);
        state.recordsSummary = summary;
        renderRecordsTrendChart(summary);
      }

      function renderOverviewUpdateStatus() {
        const statusEl = document.getElementById('overview-update-status');
        const progressWrap = document.getElementById('overview-update-progress-wrap');
        const progressStageEl = document.getElementById('overview-update-progress-stage');
        const progressPercentEl = document.getElementById('overview-update-progress-percent');
        const progressBarEl = document.getElementById('overview-update-progress-bar');
        const progressUpdatedAtEl = document.getElementById('overview-update-progress-updated-at');
        const checkButton = document.getElementById('overview-check-update');
        const runButton = document.getElementById('overview-run-update');
        const versionLabelEl = document.getElementById('agent-version-label');
        const menuUpdateBulletEl = document.getElementById('agent-menu-update-bullet');
        const installUpdateBulletEl = document.getElementById('agent-install-update-bullet');
        if (!statusEl || !progressWrap || !progressStageEl || !progressPercentEl || !progressBarEl || !progressUpdatedAtEl || !checkButton || !runButton) {
          return;
        }

        const status = state.updateStatus;
        if (!status) {
          statusEl.textContent = 'Update-Status unbekannt';
          progressWrap.classList.add('d-none');
          checkButton.disabled = false;
          runButton.disabled = true;
          return;
        }

        const getUpdateStageLabel = (stage) => {
          const normalized = String(stage || '').trim().toLowerCase();
          if (normalized === 'init' || normalized === 'start') return 'Vorbereitung';
          if (normalized === 'manifest') return 'Manifest laden';
          if (normalized === 'download') return 'Paket herunterladen';
          if (normalized === 'verify') return 'Paket prüfen';
          if (normalized === 'extract') return 'Paket entpacken';
          if (normalized === 'stop-service') return 'Dienst stoppen';
          if (normalized === 'apply') return 'Dateien einspielen';
          if (normalized === 'start-service') return 'Dienst starten';
          if (normalized === 'rollback') return 'Rollback';
          if (normalized === 'completed') return 'Abgeschlossen';
          if (normalized === 'failed') return 'Fehlgeschlagen';
          if (normalized === 'idle') return 'Bereit';
          return normalized || 'Unbekannt';
        };

        const isInProgress = !!status.inProgress;
        const progressPercent = Number(status.progressPercent);
        const normalizedProgressPercent = Number.isFinite(progressPercent)
          ? Math.max(0, Math.min(100, Math.round(progressPercent)))
          : 0;
        const hasFailureMessage = /fehlgeschlagen/i.test(String(status.message || ''));
        const currentVersion = String(status.currentVersion || '').trim() || '-';
        const hasUpdateAvailable = !!status.updateAvailable;
        statusEl.textContent = status.message || 'Update-Status unbekannt';
        if (versionLabelEl) {
          versionLabelEl.textContent = 'v' + currentVersion;
        }
        if (menuUpdateBulletEl) {
          menuUpdateBulletEl.classList.toggle('d-none', !hasUpdateAvailable);
        }
        if (installUpdateBulletEl) {
          installUpdateBulletEl.classList.toggle('d-none', !hasUpdateAvailable);
        }
        progressWrap.classList.toggle('d-none', !isInProgress && !status.stage && !Number.isFinite(progressPercent));
        progressStageEl.textContent = status.stage
          ? 'Schritt: ' + getUpdateStageLabel(status.stage)
          : (isInProgress ? 'Update wird ausgefuehrt.' : 'Kein laufendes Update');
        progressPercentEl.textContent = normalizedProgressPercent + '%';
        progressBarEl.style.width = normalizedProgressPercent + '%';
        progressBarEl.setAttribute('aria-valuenow', String(normalizedProgressPercent));
        progressBarEl.classList.toggle('bg-danger', hasFailureMessage);
        progressBarEl.classList.toggle('bg-success', !isInProgress && !hasFailureMessage && normalizedProgressPercent >= 100);
        progressUpdatedAtEl.textContent = status.updatedAt
          ? 'Stand: ' + formatDate(status.updatedAt, 'short')
          : '';

        checkButton.disabled = isInProgress;
        runButton.disabled = isInProgress || !status.updateAvailable;
        runButton.title = status.supported === false
          ? 'Der Direktstart richtet sich nach dem Agent-Host, nicht nach dem Browser-Client.'
          : '';

        if (isInProgress) {
          startOverviewUpdatePolling();
        } else {
          stopOverviewUpdatePolling();
        }
      }

      function stopOverviewUpdatePolling() {
        if (!state.updateStatusPollTimer) {
          return;
        }

        window.clearTimeout(state.updateStatusPollTimer);
        state.updateStatusPollTimer = null;
      }

      function startOverviewUpdatePolling() {
        if (state.updateStatusPollTimer) {
          return;
        }

        state.updateStatusPollTimer = window.setTimeout(async () => {
          state.updateStatusPollTimer = null;
          await loadOverviewUpdateStatus(true, false, true);
        }, 3000);
      }

      function renderOverviewLogRetentionStatus() {
        const retentionEl = document.getElementById('overview-log-retention-status');
        if (!retentionEl) {
          return;
        }

        const retentionDays = Number(state.health?.logRetentionDays || 0);
        retentionEl.textContent = retentionDays > 0
          ? 'Log-Retention: ' + retentionDays + ' Tage'
          : 'Log-Retention: deaktiviert';
      }

      async function loadOverviewUpdateStatus(force, notifyUser, silent) {
        if (!force && state.updateStatus && (Date.now() - Number(state.updateStatusCheckedAt || 0) < 60000)) {
          renderOverviewUpdateStatus();
          if (notifyUser) {
            window.alert(state.updateStatus?.message || 'Update-Status unbekannt.');
          }
          return;
        }

        let status = null;
        try {
          status = await requestJson('/api/system/update-status');
        } catch (error) {
          if (!silent) {
            showError(error.message || 'Update-Status konnte nicht geladen werden.');
          }

          if (state.updateStatus?.inProgress) {
            status = {
              ...state.updateStatus,
              message: 'Update wird ausgefuehrt. Verbindung zum Agenten wird neu aufgebaut.'
            };
          } else {
            status = {
              currentVersion: '-',
              updateAvailable: false,
              supported: false,
              manifestUrl: '',
              message: 'Update-Status konnte nicht geladen werden.'
            };
          }
        }

        state.updateStatus = status;
        state.updateStatusCheckedAt = Date.now();
        renderOverviewUpdateStatus();

        if (notifyUser) {
          window.alert(status?.message || (status?.updateAvailable ? 'Ein Update ist verfügbar.' : 'Kein Update verfügbar.'));
        }
      }

      async function triggerOverviewUpdate() {
        if (state.updateStatus?.supported === false) {
          window.alert(state.updateStatus?.message || 'Der Direktstart richtet sich nach dem Agent-Host, nicht nach dem Browser-Client.');
          return;
        }

        const result = await requestJson('/api/system/update-now', {
          method: 'POST'
        });
        window.alert(result.message || (result.ok ? 'Update gestartet.' : 'Update fehlgeschlagen.'));
        if (result.output) {
          console.log('Update output:', result.output);
        }
        await loadOverviewUpdateStatus(true, false, true);
      }

      async function setScheduleActive(scheduleId, active) {
        await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/active', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ active: !!active })
        });
        await refresh({ refreshChart: false });
      }

      async function openRecordSchedulersByBucket(bucket, connectorName) {
        if (!bucket) {
          return;
        }

        const connectorSchedules = bucket?.connectorSchedules || {};
        const primaryConnectorNames = (state.recordsSummary?.connectors || []).slice(0, MAX_RECORD_CONNECTOR_SERIES);
        let effectiveConnectorName = String(connectorName || '').trim();
        let scheduleEntries = [];

        if (effectiveConnectorName === '__other__') {
          scheduleEntries = Object.entries(connectorSchedules)
            .filter(([name]) => !primaryConnectorNames.includes(name))
            .flatMap(([, entries]) => Array.isArray(entries) ? entries : []);
          effectiveConnectorName = 'Sonstige';
        } else {
          scheduleEntries = Array.isArray(connectorSchedules[effectiveConnectorName]) ? connectorSchedules[effectiveConnectorName] : [];
        }

        const normalizedEntries = scheduleEntries
          .filter((entry) => entry && (entry.scheduleId || entry.scheduleName))
          .sort((left, right) => Number(right.total || 0) - Number(left.total || 0));

        if (!normalizedEntries.length) {
          return;
        }

        if (normalizedEntries.length === 1 && normalizedEntries[0].scheduleId) {
          await openScheduleModal(normalizedEntries[0].scheduleId);
          return;
        }

        const title = document.getElementById('records-scheduler-modal-title');
        const summaryEl = document.getElementById('records-scheduler-modal-summary');
        const listEl = document.getElementById('records-scheduler-modal-list');
        if (!title || !summaryEl || !listEl) {
          return;
        }

        title.textContent = 'Scheduler für ' + (effectiveConnectorName || 'Datensätze');
        summaryEl.textContent = 'Zeitslot: ' + String(bucket.label || '-') + ' · Scheduler: ' + normalizedEntries.length;
        listEl.innerHTML = normalizedEntries.map((entry) => {
          const label = String(entry.scheduleName || entry.scheduleId || 'Unbekannter Scheduler');
          const count = Number(entry.total || 0);
          const failed = Number(entry.failed || 0);
          return '<button type="button" class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" data-open-record-scheduler="' + esc(entry.scheduleId || '') + '">' +
            '<span><strong>' + esc(label) + '</strong><span class="d-block small text-secondary">Connector: ' + esc(entry.connectorName || effectiveConnectorName || '-') + '</span></span>' +
            '<span class="text-end small"><span class="d-block">Datensätze: ' + count + '</span><span class="d-block text-danger">Fehler: ' + failed + '</span></span>' +
          '</button>';
        }).join('');

        listEl.querySelectorAll('[data-open-record-scheduler]').forEach((button) => {
          button.addEventListener('click', async () => {
            const scheduleId = String(button.getAttribute('data-open-record-scheduler') || '').trim();
            if (!scheduleId) {
              return;
            }
            recordsSchedulerModal.hide();
            await openScheduleModal(scheduleId);
          });
        });

        recordsSchedulerModal.show();
      }

      function isEmptyLogSummary(summary) {
        const buckets = Array.isArray(summary?.buckets) ? summary.buckets : [];
        const connectors = Array.isArray(summary?.connectors) ? summary.connectors : [];
        if (connectors.length > 0) {
          return false;
        }
        return !buckets.some((bucket) => Number(bucket?.errors || 0) > 0);
      }

      async function loadLogSummary() {
        const range = document.getElementById('log-chart-range').value || 'last_24h';
        try {
          window.localStorage.setItem(LOG_CHART_RANGE_STORAGE_KEY, range);
        } catch {
          // Ignore storage errors in restricted browser contexts.
        }
        let summary = await safeRequest('/api/logs/summary?range=' + encodeURIComponent(range), { range, buckets: [], connectors: [] });
        if (isEmptyLogSummary(summary)) {
          const retryDelaysMs = [750, 1500, 2500];
          for (const delayMs of retryDelaysMs) {
            await new Promise((resolve) => setTimeout(resolve, delayMs));
            const retried = await safeRequest('/api/logs/summary?range=' + encodeURIComponent(range), summary);
            if (!isEmptyLogSummary(retried)) {
              summary = retried;
              break;
            }
          }
        }
        state.logSummary = summary;
        renderLogChart(summary);
      }

      function restoreLogChartRange() {
        const select = document.getElementById('log-chart-range');
        if (!select) {
          return;
        }

        try {
          const stored = window.localStorage.getItem(LOG_CHART_RANGE_STORAGE_KEY);
          if (stored && Array.from(select.options).some((option) => option.value === stored)) {
            select.value = stored;
          }
        } catch {
          // Ignore storage errors in restricted browser contexts.
        }
      }

      function restoreOverviewStatsRange() {
        try {
          const stored = window.localStorage.getItem(OVERVIEW_STATS_RANGE_STORAGE_KEY);
          if (stored && ['day', 'month', 'year'].includes(stored)) {
            state.overviewStatsRange = stored;
          }
        } catch {
          // Ignore storage errors in restricted browser contexts.
        }
      }

      function renderOverviewStatsRangeButtons() {
        const group = document.getElementById('overview-stats-range');
        if (!group) {
          return;
        }

        group.querySelectorAll('[data-range]').forEach((button) => {
          const range = String(button.getAttribute('data-range') || '').trim();
          button.classList.toggle('active', range === state.overviewStatsRange);
        });
      }

      function getOverviewRangeStartDate(now, range) {
        const start = new Date(now);
        if (range === 'day') {
          start.setHours(0, 0, 0, 0);
          return start;
        }

        if (range === 'year') {
          start.setMonth(0, 1);
          start.setHours(0, 0, 0, 0);
          return start;
        }

        start.setDate(1);
        start.setHours(0, 0, 0, 0);
        return start;
      }

      function getConnectorWizardTotalSteps() {
        return 4;
      }

      function renderConnectorWizardStep() {
        const currentStep = Math.max(1, Math.min(getConnectorWizardTotalSteps(), Number(state.connectorWizardStep) || 1));
        state.connectorWizardStep = currentStep;

        document.querySelectorAll('[data-step-panel]').forEach((panel) => {
          const step = Number(panel.getAttribute('data-step-panel') || '0');
          panel.classList.toggle('d-none', step !== currentStep);
        });

        document.querySelectorAll('#con-wizard-steps .connector-wizard-step').forEach((button) => {
          const step = Number(button.getAttribute('data-step') || '0');
          button.classList.toggle('is-active', step === currentStep);
          button.classList.toggle('is-complete', step < currentStep);
        });

        const backButton = document.getElementById('con-wizard-back');
        const nextButton = document.getElementById('con-wizard-next');
        const saveButton = document.getElementById('save-connector');
        const validateButton = document.getElementById('test-connector');
        if (backButton) {
          backButton.disabled = currentStep === 1;
        }
        if (nextButton) {
          nextButton.classList.toggle('d-none', currentStep >= getConnectorWizardTotalSteps());
        }
        if (saveButton) {
          saveButton.classList.toggle('d-none', currentStep !== getConnectorWizardTotalSteps());
        }
        if (validateButton) {
          validateButton.classList.toggle('d-none', currentStep !== getConnectorWizardTotalSteps());
        }

        if (currentStep === getConnectorWizardTotalSteps()) {
          updateConnectorReviewStep();
        }
      }

      function collectConnectorParametersPreview() {
        let parsedParameters = {};
        const rawParameters = String(document.getElementById('con-parameters')?.value || '').trim();
        if (rawParameters) {
          parsedParameters = JSON.parse(rawParameters);
        }

        applyConnectorWizardSelection(true);
        const normalizedConnectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');

        if (normalizedConnectorType === 'FILE') {
          parsedParameters = mergeFileConnectorSettingsIntoParameters(parsedParameters);
        }
        if (isSqlConnectorType(normalizedConnectorType)) {
          parsedParameters = mergeMssqlConnectorSettingsIntoParameters(parsedParameters);
        }
        if (isRestConnectorType(normalizedConnectorType)) {
          parsedParameters = mergeRestConnectorSettingsIntoParameters(parsedParameters);
        }
        if (isBinaryImportConnectorType(normalizedConnectorType)) {
          parsedParameters = mergeBinaryImportConnectorSettingsIntoParameters(parsedParameters);
        }
        parsedParameters = mergeConnectorNotificationSettingsIntoParameters(parsedParameters);

        return {
          connectorType: normalizedConnectorType,
          parameters: parsedParameters
        };
      }

      function fillConnectorNotificationSettingsFromParameters(parameters) {
        const params = parameters || {};
        document.getElementById('con-task-notify-enabled').checked = params.notificationTaskEnabled === true;
        document.getElementById('con-task-owner-id').value = String(params.notificationTaskOwnerId || '');
        const errorClassSelect = document.getElementById('con-task-error-classes');
        const selectedClasses = Array.isArray(params.notificationTaskErrorClasses)
          ? params.notificationTaskErrorClasses
          : String(params.notificationTaskErrorClasses || '')
              .split(',')
              .map((value) => value.trim().toUpperCase())
              .filter(Boolean);
        Array.from(errorClassSelect?.options || []).forEach((option) => {
          option.selected = selectedClasses.includes(String(option.value || '').trim().toUpperCase());
        });
      }

      function mergeConnectorNotificationSettingsIntoParameters(parameters) {
        const merged = { ...(parameters || {}) };
        const enabled = !!document.getElementById('con-task-notify-enabled').checked;
        const ownerSelect = document.getElementById('con-task-owner-id');
        const ownerId = String(ownerSelect?.value || '').trim();
        const ownerUsername = String(ownerSelect?.selectedOptions?.[0]?.getAttribute('data-username') || '').trim();
        const errorClassSelect = document.getElementById('con-task-error-classes');
        const errorClasses = Array.from(errorClassSelect?.selectedOptions || [])
          .map((option) => String(option.value || '').trim().toUpperCase())
          .filter(Boolean);

        if (enabled && ownerId) {
          merged.notificationTaskEnabled = true;
          merged.notificationTaskOwnerId = ownerId;
          merged.notificationTaskOwnerUsername = ownerUsername;
          merged.notificationTaskErrorClasses = errorClasses.length ? errorClasses : connectorNotificationErrorClassOptions.slice();
        } else {
          delete merged.notificationTaskEnabled;
          delete merged.notificationTaskOwnerId;
          delete merged.notificationTaskOwnerUsername;
          delete merged.notificationTaskErrorClasses;
        }

        return merged;
      }

      function updateConnectorReviewStep() {
        const summaryEl = document.getElementById('con-review-summary');
        const jsonEl = document.getElementById('con-review-json');
        if (!summaryEl || !jsonEl) {
          return;
        }

        const preview = collectConnectorParametersPreview();
        const summaryItems = [
          ['Typ', preview.connectorType || '-'],
          ['Name', String(document.getElementById('con-name')?.value || '-').trim() || '-'],
          ['Target System', String(document.getElementById('con-target-system')?.value || '-').trim() || '-'],
          ['Direction', String(document.getElementById('con-direction')?.value || '-').trim() || '-'],
          ['Timeout', String(document.getElementById('con-timeout')?.value || '-').trim() || '-'],
          ['Retries', String(document.getElementById('con-retries')?.value || '-').trim() || '-']
        ];

        summaryEl.innerHTML = summaryItems.map((item) =>
          '<div class="connector-review-row"><span class="connector-review-label">' + esc(item[0]) + '</span><span class="connector-review-value">' + esc(item[1]) + '</span></div>'
        ).join('');
        jsonEl.textContent = JSON.stringify(preview.parameters, null, 2);
      }

      function validateConnectorWizardStep(step) {
        clearConnectorModalError();

        if (step === 1) {
          const wizardType = String(document.getElementById('con-wizard-type')?.value || '').trim();
          if (!wizardType) {
            throw new Error('Bitte zuerst einen Connectortyp auswählen.');
          }
          return;
        }

        if (step === 2) {
          const name = String(document.getElementById('con-name')?.value || '').trim();
          if (!name) {
            throw new Error('Bitte einen Connector-Namen eingeben.');
          }
          return;
        }

        if (step === 3) {
          const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
          if (isSqlConnectorType(connectorType)) {
            if (!String(document.getElementById('con-mssql-server')?.value || '').trim() ||
                !String(document.getElementById('con-mssql-database')?.value || '').trim() ||
                !String(document.getElementById('con-mssql-user')?.value || '').trim()) {
              throw new Error('Bitte Host, Datenbank und Benutzer für den SQL-Connector angeben.');
            }
          }
          if (connectorType === 'FILE') {
            if (!String(document.getElementById('con-file-base-path')?.value || '').trim()) {
              throw new Error('Bitte mindestens den Base Path für den Datei-Connector angeben.');
            }
          }
          if (connectorType === 'REST_API') {
            if (!String(document.getElementById('con-rest-base-url')?.value || '').trim()) {
              throw new Error('Bitte eine Base URL für den REST-Connector angeben.');
            }
          }
          if (connectorType === 'FILE_BINARY_SF_IMPORT') {
            if (!String(document.getElementById('con-binary-base-path')?.value || '').trim()) {
              throw new Error('Bitte den Base Path für den Binärimport angeben.');
            }
          }
        }
      }

      function goToConnectorWizardStep(nextStep) {
        state.connectorWizardStep = Math.max(1, Math.min(getConnectorWizardTotalSteps(), nextStep));
        renderConnectorWizardStep();
      }

      function getScheduleWizardTotalSteps() {
        return 5;
      }

      function renderScheduleWizardStep() {
        const currentStep = Math.max(1, Math.min(getScheduleWizardTotalSteps(), Number(state.scheduleWizardStep) || 1));
        state.scheduleWizardStep = currentStep;

        document.querySelectorAll('[data-sch-step-panel]').forEach((panel) => {
          const step = Number(panel.getAttribute('data-sch-step-panel') || '0');
          const isActive = step === currentStep;
          panel.classList.toggle('show', isActive);
          panel.classList.toggle('active', isActive);
          panel.classList.toggle('d-none', !isActive);
        });

        document.querySelectorAll('#sch-wizard-steps [data-sch-step]').forEach((button) => {
          const step = Number(button.getAttribute('data-sch-step') || '0');
          button.classList.toggle('is-active', step === currentStep);
          button.classList.toggle('is-complete', step < currentStep);
        });

        const backButton = document.getElementById('sch-wizard-back');
        const nextButton = document.getElementById('sch-wizard-next');
        const saveButton = document.getElementById('save-schedule');
        const hint = document.getElementById('sch-wizard-hint');
        if (backButton) {
          backButton.disabled = currentStep === 1;
        }
        if (nextButton) {
          nextButton.classList.toggle('d-none', currentStep >= getScheduleWizardTotalSteps());
        }
        if (saveButton) {
          saveButton.classList.toggle('d-none', currentStep !== getScheduleWizardTotalSteps());
        }
        if (hint) {
          const labels = {
            1: 'Basisdaten und Einordnung des Schedulers.',
            2: 'Quelle auswählen, Delta konfigurieren und Vorschau testen.',
            3: 'Zielsystem, Objekt und technische Zieldefinition festlegen.',
            4: 'Zeitsteuerung festlegen oder vom Parent übernehmen.',
            5: 'Mapping prüfen und den Scheduler speichern.'
          };
          hint.textContent = 'Assistent aktiv: ' + (labels[currentStep] || 'Scheduler Schritt für Schritt konfigurieren.');
        }
      }

      function validateScheduleWizardStep(step) {
        clearModalError();

        if (step === 1) {
          if (!String(document.getElementById('sch-name')?.value || '').trim()) {
            throw new Error('Bitte einen Namen für den Scheduler eingeben.');
          }
          if (!String(document.getElementById('sch-connector')?.value || '').trim()) {
            throw new Error('Bitte einen Connector auswählen.');
          }
          return;
        }

        if (step === 2) {
          if (!String(document.getElementById('sch-source-type')?.value || '').trim()) {
            throw new Error('Bitte einen Source Type wählen.');
          }
          if (!String(document.getElementById('sch-source-definition')?.value || '').trim()) {
            throw new Error('Bitte eine Source Definition oder Abfrage angeben.');
          }
          return;
        }

        if (step === 3) {
          if (!String(document.getElementById('sch-target-type')?.value || '').trim()) {
            throw new Error('Bitte einen Target Type wählen.');
          }
          if (!String(document.getElementById('sch-object')?.value || '').trim()) {
            throw new Error('Bitte ein Zielobjekt wählen.');
          }
          if (!String(document.getElementById('sch-operation')?.value || '').trim()) {
            throw new Error('Bitte eine Operation wählen.');
          }
          return;
        }

        if (step === 4 && !document.getElementById('sch-inherit-parent-timing')?.checked) {
          const hasWeekday = Array.from(document.querySelectorAll('#sch-weekdays input')).some((input) => input.checked);
          if (!hasWeekday) {
            throw new Error('Bitte mindestens einen Wochentag für die Zeitsteuerung auswählen.');
          }
          if (!String(document.getElementById('sch-timing-time')?.value || '').trim()) {
            throw new Error('Bitte eine Uhrzeit für die Zeitsteuerung wählen.');
          }
        }
      }

      function goToScheduleWizardStep(nextStep) {
        state.scheduleWizardStep = Math.max(1, Math.min(getScheduleWizardTotalSteps(), nextStep));
        renderScheduleWizardStep();
      }

      function advanceScheduleWizardStep() {
        try {
          validateScheduleWizardStep(state.scheduleWizardStep);
          goToScheduleWizardStep(state.scheduleWizardStep + 1);
        } catch (error) {
          showModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
        }
      }

      function advanceConnectorWizardStep() {
        try {
          validateConnectorWizardStep(state.connectorWizardStep);
          goToConnectorWizardStep(state.connectorWizardStep + 1);
        } catch (error) {
          showConnectorModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
        }
      }

      async function openLogsByBucket(bucket, type, connectorName) {
        const path = '/api/logs?start=' + encodeURIComponent(bucket.start) +
          '&end=' + encodeURIComponent(bucket.end) +
          '&type=' + encodeURIComponent(type) +
          '&connector=' + encodeURIComponent(connectorName || '') +
          '&limit=300';

        const result = await safeRequest(path, { items: [] });
        const rows = result.items || [];
        document.getElementById('logs-modal-title').textContent =
          'Logliste ' + (type === 'error' ? '(Fehler)' : '(Alle)') +
          (connectorName ? ' | ' + connectorName : '') +
          ' | ' + new Date(bucket.start).toLocaleString('de-DE') +
          ' - ' + new Date(bucket.end).toLocaleString('de-DE');

        const body = document.getElementById('logs-modal-body');
        const logsFilter = document.getElementById('logs-filter');
        if (logsFilter) {
          logsFilter.value = '';
          try {
            localStorage.removeItem(TABLE_STORAGE_KEY + '.logs');
          } catch (e) {
            // Ignore storage errors
          }
        }
        if (!rows.length) {
          body.innerHTML = '<tr><td colspan="6" class="text-secondary p-3">Keine Logs in diesem Zeitraum.</td></tr>';
          logsModal.show();
          return;
        }

        body.innerHTML = rows.map((entry) =>
          '<tr>' +
            '<td>' + esc(entry.createdAt ? new Date(entry.createdAt).toLocaleString('de-DE') : '-') + '</td>' +
            '<td>' + esc(entry.level || '-') + '</td>' +
            '<td>' + esc(entry.connectorName || '-') + '</td>' +
            '<td>' + esc(entry.scheduleName || '-') + '</td>' +
            '<td>' + esc(entry.step || '-') + '</td>' +
            '<td style="white-space: normal; word-break: break-word; overflow-wrap: anywhere;">' + esc(entry.message || '-') + '</td>' +
          '</tr>'
        ).join('');

        logsModal.show();
      }

      function updateServiceCpuSparkline(cpuPercent) {
        const sparklinePath = document.getElementById('kpi-service-cpu-sparkline-path');
        const sparklineDot = document.getElementById('kpi-service-cpu-sparkline-dot');
        if (!sparklinePath || !sparklineDot) {
          return;
        }

        const hasCpuValue = Number.isFinite(cpuPercent);
        if (hasCpuValue) {
          state.cpuLoadHistory.push(Math.max(0, Math.min(100, Math.round(cpuPercent))));
        }

        const maxPoints = 18;
        if (state.cpuLoadHistory.length > maxPoints) {
          state.cpuLoadHistory = state.cpuLoadHistory.slice(-maxPoints);
        }

        if (!state.cpuLoadHistory.length) {
          sparklinePath.setAttribute('d', '');
          sparklineDot.setAttribute('cx', '0');
          sparklineDot.setAttribute('cy', '0');
          return;
        }

        const width = 120;
        const height = 20;
        const chartPadding = 1;
        const points = state.cpuLoadHistory.map((value, index, list) => {
          const x = list.length <= 1 ? chartPadding : chartPadding + (index * (width - chartPadding * 2)) / (list.length - 1);
          const y = height - chartPadding - (Math.max(0, Math.min(100, value)) / 100) * (height - chartPadding * 2);
          return { x, y };
        });

        const pathData = points
          .map((point, index) => (index === 0 ? 'M' : 'L') + point.x.toFixed(2) + ' ' + point.y.toFixed(2))
          .join(' ');
        sparklinePath.setAttribute('d', pathData);

        const lastPoint = points[points.length - 1];
        sparklineDot.setAttribute('cx', lastPoint.x.toFixed(2));
        sparklineDot.setAttribute('cy', lastPoint.y.toFixed(2));

        const lastValue = state.cpuLoadHistory[state.cpuLoadHistory.length - 1];
        sparklinePath.className.baseVal = 'kpi-sparkline-path';
        sparklineDot.className.baseVal = 'kpi-sparkline-dot';
        if (lastValue >= 80) {
          sparklinePath.classList.add('kpi-sparkline-danger');
          sparklineDot.classList.add('kpi-sparkline-danger');
        } else if (lastValue >= 55) {
          sparklinePath.classList.add('kpi-sparkline-warn');
          sparklineDot.classList.add('kpi-sparkline-warn');
        } else {
          sparklinePath.classList.add('kpi-sparkline-ok');
          sparklineDot.classList.add('kpi-sparkline-ok');
        }
      }

      function renderOverview(healthData) {
        const previousSnapshot = state.previousOverviewSnapshot;
        const formatByteSize = (bytes) => {
          const numericBytes = Number(bytes);
          if (!Number.isFinite(numericBytes) || numericBytes < 0) {
            return null;
          }

          const units = ['B', 'KB', 'MB', 'GB', 'TB'];
          let value = numericBytes;
          let unitIndex = 0;
          while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex += 1;
          }

          const digits = value >= 10 || unitIndex === 0 ? 0 : 1;
          return value.toFixed(digits) + ' ' + units[unitIndex];
        };

        const formatUsageMetric = (usedBytes, totalBytes) => {
          const used = Number(usedBytes);
          const total = Number(totalBytes);
          if (!Number.isFinite(used) || !Number.isFinite(total) || total <= 0) {
            return 'nicht verfuegbar';
          }

          const percentage = Math.max(0, Math.min(100, Math.round((used / total) * 100)));
          return formatByteSize(used) + ' / ' + formatByteSize(total) + ' (' + percentage + '%)';
        };

        renderOverviewStatsRangeButtons();
        state.health = healthData || {};
        renderOverviewLogRetentionStatus();
        document.getElementById('kpi-service').textContent = healthData.service || '-';
        document.getElementById('kpi-scheduler').textContent = healthData.scheduler || '-';
        document.getElementById('kpi-schedules').textContent = String(state.schedules.length);
        document.getElementById('kpi-connectors').textContent = String(state.connectors.length);

        const cpuPercent = Number(healthData.cpuLoadPercent);
        const hasCpuPercent = Number.isFinite(cpuPercent);
        const serviceCpuBar = document.getElementById('kpi-service-cpu-bar');
        const serviceCpuText = document.getElementById('kpi-service-cpu-text');
        const normalizedCpuPercent = hasCpuPercent ? Math.max(0, Math.min(100, Math.round(cpuPercent))) : null;
        if (serviceCpuBar) {
          const cpuValue = normalizedCpuPercent === null ? 0 : normalizedCpuPercent;
          serviceCpuBar.style.width = cpuValue + '%';
          serviceCpuBar.className = 'kpi-meter-fill';
          if (normalizedCpuPercent !== null) {
            if (normalizedCpuPercent >= 80) {
              serviceCpuBar.classList.add('kpi-meter-fill-danger');
            } else if (normalizedCpuPercent >= 55) {
              serviceCpuBar.classList.add('kpi-meter-fill-warn');
            } else {
              serviceCpuBar.classList.add('kpi-meter-fill-ok');
            }
          }
        }
        if (serviceCpuText) {
          serviceCpuText.textContent = normalizedCpuPercent === null
            ? 'CPU Last: nicht verfuegbar'
            : 'CPU Last: ' + normalizedCpuPercent + '%';
        }
        const serviceOsText = document.getElementById('kpi-service-os');
        if (serviceOsText) {
          serviceOsText.textContent = 'OS: ' + String(healthData.operatingSystem || 'nicht verfuegbar');
        }
        const serviceMemoryText = document.getElementById('kpi-service-memory');
        if (serviceMemoryText) {
          serviceMemoryText.textContent = 'RAM: ' + formatUsageMetric(healthData.memoryUsedBytes, healthData.memoryTotalBytes);
        }
        const serviceDiskText = document.getElementById('kpi-service-disk');
        if (serviceDiskText) {
          serviceDiskText.textContent = 'Disk: ' + formatUsageMetric(healthData.diskUsedBytes, healthData.diskTotalBytes);
        }
        updateServiceCpuSparkline(normalizedCpuPercent);

        const runs = Array.isArray(state.runs) ? state.runs : [];
        const now = new Date();
        const rangeStart = getOverviewRangeStartDate(now, state.overviewStatsRange);
        const scopedRuns = runs.filter((run) => {
          if (!run || !run.startedAt) {
            return false;
          }
          const startedAt = new Date(run.startedAt);
          return !Number.isNaN(startedAt.getTime()) && startedAt >= rangeStart;
        });
        const schedules = Array.isArray(state.schedules) ? state.schedules : [];
        const migrations = Array.isArray(state.migrations) ? state.migrations : [];
        const normalizeStatus = (value) => String(value || '').trim().toLowerCase();

        const successCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'success').length;
        const failedCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'failed' || normalizeStatus(run.status) === 'error').length;
        const runningCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'running').length;
        const totalCount = scopedRuns.length;
        const successRate = totalCount > 0 ? Math.round((successCount / totalCount) * 100) : 0;
        const errorRate = totalCount > 0 ? Math.round((failedCount / totalCount) * 100) : 0;

        const inboundCount = schedules.filter((schedule) => String(schedule.direction || '').toLowerCase() === 'inbound').length;
        const outboundCount = schedules.filter((schedule) => String(schedule.direction || '').toLowerCase() === 'outbound').length;
        const autoDisabledCount = schedules.filter((schedule) => schedule.autoDisabledDueToErrors && schedule.active === false).length;
        const completedRunDurations = scopedRuns
          .map((run) => getRunDurationMs(run))
          .filter((duration) => duration !== null);
        const averageRunDurationMs = completedRunDurations.length
          ? completedRunDurations.reduce((sum, duration) => sum + duration, 0) / completedRunDurations.length
          : null;

        const latestRun = scopedRuns
          .filter((run) => run && run.startedAt)
          .sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime())[0];

        const sqliteObjects = migrations
          .flatMap((migration) => Array.isArray(migration?.objects) ? migration.objects : [])
          .filter((obj) => String(obj?.processingMode || obj?.stagingMode || '').trim().toLowerCase() === 'sqlite');
        const sqliteStatusTotals = sqliteObjects.reduce((totals, obj) => {
          const summary = obj && obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : null;
          if (summary) {
            Object.entries(summary).forEach(([key, value]) => {
              const normalizedKey = String(key || '').trim().toLowerCase();
              totals[normalizedKey] = Number(totals[normalizedKey] || 0) + (Number(value || 0) || 0);
            });
            return totals;
          }

          const fallbackStatus = String(obj?.stagingStatus || '').trim().toLowerCase();
          if (fallbackStatus) {
            totals[fallbackStatus] = Number(totals[fallbackStatus] || 0) + 1;
          }
          return totals;
        }, {});
        const sqlitePendingCount = Number(sqliteStatusTotals.pending || 0) + Number(sqliteStatusTotals.ready || 0);
        const sqliteSuccessCount = Number(sqliteStatusTotals.success || 0) + Number(sqliteStatusTotals.done || 0);
        const sqliteErrorCount = Number(sqliteStatusTotals.mapping_error || 0) + Number(sqliteStatusTotals.salesforce_error || 0) + Number(sqliteStatusTotals.error || 0);

        const successRateLabel = document.getElementById('kpi-success-rate');
        const errorRateLabel = document.getElementById('kpi-error-rate');
        const successRateBar = document.getElementById('kpi-success-rate-bar');
        const errorRateBar = document.getElementById('kpi-error-rate-bar');
        const runsSuccess = document.getElementById('kpi-runs-success');
        const runsFailed = document.getElementById('kpi-runs-failed');
        const runsRunning = document.getElementById('kpi-runs-running');
        const runsTotal = document.getElementById('kpi-runs-total');
        const inboundCounter = document.getElementById('kpi-inbound-count');
        const outboundCounter = document.getElementById('kpi-outbound-count');
        const averageRunDuration = document.getElementById('kpi-average-run-duration');
        const autoDisabledCounter = document.getElementById('kpi-auto-disabled-count');
        const lastRunAt = document.getElementById('kpi-last-run-at');
        const sqliteObjectsCounter = document.getElementById('kpi-sqlite-objects');
        const sqlitePendingCounter = document.getElementById('kpi-sqlite-pending');
        const sqliteSuccessCounter = document.getElementById('kpi-sqlite-success');
        const sqliteErrorsCounter = document.getElementById('kpi-sqlite-errors');

        const updateKpiTrend = (elementId, delta, positiveWhenUp, neutralText) => {
          const element = document.getElementById(elementId);
          if (!element) {
            return;
          }

          if (delta === null || Number.isNaN(delta)) {
            element.className = 'kpi-trend kpi-trend-neutral';
            element.textContent = '• ' + neutralText;
            return;
          }

          if (delta === 0) {
            element.className = 'kpi-trend kpi-trend-neutral';
            element.textContent = '→ unverändert';
            return;
          }

          const isUp = delta > 0;
          const isPositive = positiveWhenUp ? isUp : !isUp;
          element.className = 'kpi-trend ' + (isPositive ? 'kpi-trend-positive' : 'kpi-trend-negative');
          element.textContent = (isUp ? '↑ ' : '↓ ') + Math.abs(delta) + ' seit letztem Update';
        };

        if (successRateLabel) {
          successRateLabel.textContent = successRate + '%';
        }
        if (errorRateLabel) {
          errorRateLabel.textContent = errorRate + '%';
        }
        if (successRateBar) {
          successRateBar.style.width = Math.max(0, Math.min(100, successRate)) + '%';
        }
        if (errorRateBar) {
          errorRateBar.style.width = Math.max(0, Math.min(100, errorRate)) + '%';
        }
        if (runsSuccess) {
          runsSuccess.textContent = String(successCount);
        }
        if (runsFailed) {
          runsFailed.textContent = String(failedCount);
        }
        if (runsRunning) {
          runsRunning.textContent = String(runningCount);
        }
        if (runsTotal) {
          runsTotal.textContent = String(totalCount);
        }
        if (inboundCounter) {
          inboundCounter.textContent = String(inboundCount);
        }
        if (outboundCounter) {
          outboundCounter.textContent = String(outboundCount);
        }
        if (averageRunDuration) {
          averageRunDuration.classList.remove('text-success', 'text-warning', 'text-danger');
          averageRunDuration.textContent = averageRunDurationMs === null ? '-' : formatDurationMinSec(averageRunDurationMs);
          if (averageRunDurationMs !== null) {
            if (averageRunDurationMs < 60_000) {
              averageRunDuration.classList.add('text-success');
            } else if (averageRunDurationMs < 5 * 60_000) {
              averageRunDuration.classList.add('text-warning');
            } else {
              averageRunDuration.classList.add('text-danger');
            }
          }
        }
        if (autoDisabledCounter) {
          autoDisabledCounter.textContent = String(autoDisabledCount);
        }
        if (lastRunAt) {
          lastRunAt.textContent = latestRun ? formatDate(latestRun.startedAt, 'short') : '-';
        }
        if (sqliteObjectsCounter) {
          sqliteObjectsCounter.textContent = String(sqliteObjects.length);
        }
        if (sqlitePendingCounter) {
          sqlitePendingCounter.textContent = String(sqlitePendingCount);
        }
        if (sqliteSuccessCounter) {
          sqliteSuccessCounter.textContent = String(sqliteSuccessCount);
        }
        if (sqliteErrorsCounter) {
          sqliteErrorsCounter.textContent = String(sqliteErrorCount);
        }

        const serviceTrend = document.getElementById('kpi-service-trend');
        if (serviceTrend) {
          const isOk = String(healthData.service || '').toLowerCase() === 'ok';
          if (isOk && normalizedCpuPercent !== null && normalizedCpuPercent < 55) {
            serviceTrend.className = 'kpi-trend kpi-trend-positive';
            serviceTrend.textContent = '↑ laeuft rund';
          } else if (isOk && normalizedCpuPercent !== null && normalizedCpuPercent < 80) {
            serviceTrend.className = 'kpi-trend kpi-trend-neutral';
            serviceTrend.textContent = '→ laeuft, aber leicht unter Last';
          } else if (isOk) {
            serviceTrend.className = 'kpi-trend kpi-trend-negative';
            serviceTrend.textContent = '↓ hoher CPU-Druck';
          } else {
            serviceTrend.className = 'kpi-trend kpi-trend-negative';
            serviceTrend.textContent = '↓ Service ist degraded';
          }
        }

        const schedulerTrend = document.getElementById('kpi-scheduler-trend');
        if (schedulerTrend) {
          const schedulerState = String(healthData.scheduler || '').toLowerCase();
          if (schedulerState === 'running') {
            schedulerTrend.className = 'kpi-trend kpi-trend-positive';
            schedulerTrend.textContent = '↑ aktiv';
          } else if (schedulerState === 'error') {
            schedulerTrend.className = 'kpi-trend kpi-trend-negative';
            schedulerTrend.textContent = '↓ Fehlerzustand';
          } else {
            schedulerTrend.className = 'kpi-trend kpi-trend-neutral';
            schedulerTrend.textContent = '→ idle';
          }
        }

        updateKpiTrend(
          'kpi-schedules-trend',
          previousSnapshot ? (state.schedules.length - previousSnapshot.schedulesCount) : null,
          true,
          'warten auf Vergleich'
        );
        updateKpiTrend(
          'kpi-connectors-trend',
          previousSnapshot ? (state.connectors.length - previousSnapshot.connectorsCount) : null,
          true,
          'warten auf Vergleich'
        );

        state.previousOverviewSnapshot = {
          schedulesCount: state.schedules.length,
          connectorsCount: state.connectors.length
        };

        const body = document.getElementById('overview-runs-body');
        if (!scopedRuns.length) {
          body.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine Runs im gewählten Zeitraum gefunden.</td></tr>';
          return;
        }

        body.innerHTML = scopedRuns.slice(0, 8).map((run) =>
          '<tr><td class="text-truncate" title="' + esc(run.scheduleName || run.scheduleId || '-') + '">' + esc(run.scheduleName || run.scheduleId || '-') + '</td><td>' + getStatusBadge(run.status) + '</td><td>' + formatDurationMinSec(getRunDurationMs(run)) + '</td><td>' + formatDate(run.startedAt, 'short') + '</td></tr>'
        ).join('');
      }

      function renderSchedulerConnectorFilterOptions() {
        const select = document.getElementById('schedulers-connector-filter');
        if (!select) {
          return;
        }

        const desiredConnectorFilterId = String(select.value || state.schedulerConnectorFilterId || '').trim();
        if (desiredConnectorFilterId) {
          state.schedulerConnectorFilterId = desiredConnectorFilterId;
        }

        const connectorIds = new Set(
          (state.schedules || [])
            .map((schedule) => String(schedule.connectorId || '').trim())
            .filter(Boolean)
        );

        const options = (state.connectors || [])
          .filter((connector) => connectorIds.has(String(connector.id || '').trim()))
          .map((connector) => {
            const connectorId = String(connector.id || '').trim();
            return {
              connectorId,
              name: String(connector.name || connectorId || '-'),
              count: (state.schedules || []).filter((schedule) => String(schedule.connectorId || '').trim() === connectorId).length
            };
          })
          .sort((a, b) => a.name.localeCompare(b.name, 'de', { sensitivity: 'base' }));

        if (!options.length && connectorIds.size) {
          Array.from(connectorIds)
            .map((connectorId) => ({
              connectorId,
              name: getConnectorNameById(connectorId),
              count: (state.schedules || []).filter((schedule) => String(schedule.connectorId || '').trim() === connectorId).length
            }))
            .sort((a, b) => a.name.localeCompare(b.name, 'de', { sensitivity: 'base' }))
            .forEach((option) => options.push(option));
        }

        select.innerHTML = '<option value="">Alle Connectoren</option>' + options.map((option) =>
          '<option value="' + esc(option.connectorId) + '">' + esc(option.name) + ' (' + option.count + ')</option>'
        ).join('');

        if (desiredConnectorFilterId && options.some((option) => option.connectorId === desiredConnectorFilterId)) {
          state.schedulerConnectorFilterId = desiredConnectorFilterId;
          select.value = desiredConnectorFilterId;
        } else {
          state.schedulerConnectorFilterId = '';
          select.value = '';
        }
      }

      function getRunSortTime(run) {
        return new Date(run?.startedAt || run?.finishedAt || 0).getTime();
      }

      function getLatestRunForSchedule(scheduleId) {
        return (state.runs || [])
          .filter((run) => String(run.scheduleId || '').trim() === String(scheduleId || '').trim())
          .sort((left, right) => getRunSortTime(right) - getRunSortTime(left))[0] || null;
      }

      function getLatestFailedRunForSchedule(scheduleId) {
        return (state.runs || [])
          .filter((run) => String(run.scheduleId || '').trim() === String(scheduleId || '').trim() && normalizeRunStatus(run.status) === 'failed')
          .sort((left, right) => getRunSortTime(right) - getRunSortTime(left))[0] || null;
      }

      function buildRunProgressMetrics(run) {
        if (!run) {
          return null;
        }

        const recordsRead = Math.max(0, Number(run.recordsRead ?? 0) || 0);
        const recordsProcessed = Math.max(0, Number(run.recordsProcessed ?? 0) || 0);
        const recordsSucceeded = Math.max(0, Number(run.recordsSucceeded ?? 0) || 0);
        const recordsFailed = Math.max(0, Number(run.recordsFailed ?? 0) || 0);
        const total = Math.max(recordsRead, recordsProcessed, recordsSucceeded + recordsFailed);
        const completed = Math.max(recordsProcessed, recordsSucceeded + recordsFailed);
        const pending = Math.max(0, total - recordsSucceeded - recordsFailed);
        const completedPercent = total > 0 ? Math.max(0, Math.min(100, (completed / total) * 100)) : 0;
        const successPercent = total > 0 ? Math.max(0, Math.min(100, (recordsSucceeded / total) * 100)) : 0;
        const failedPercent = total > 0 ? Math.max(0, Math.min(100, (recordsFailed / total) * 100)) : 0;
        const pendingPercent = total > 0 ? Math.max(0, Math.min(100, 100 - successPercent - failedPercent)) : 0;

        return {
          recordsRead,
          recordsProcessed,
          recordsSucceeded,
          recordsFailed,
          total,
          completed,
          pending,
          completedPercent,
          successPercent,
          failedPercent,
          pendingPercent,
          normalizedStatus: normalizeRunStatus(run.status)
        };
      }

      function renderRunProgressMarkup(run, options) {
        const viewOptions = options || {};
        const compact = Boolean(viewOptions.compact);
        const metrics = buildRunProgressMetrics(run);

        if (!metrics) {
          return '<div class="run-mini-gauge-empty">Noch kein Lauf</div>';
        }

        const wrapperClassName = compact ? 'run-mini-gauge-wrap is-compact' : 'run-mini-gauge-wrap';
        const metaPrefix = compact ? 'Letzter Lauf: ' : '';

        if (metrics.total > 0) {
          const title = metrics.recordsSucceeded + ' erfolgreich, ' + metrics.recordsFailed + ' fehlerhaft, ' + metrics.pending + ' offen von ' + metrics.total;
          const summary = metrics.normalizedStatus === 'running'
            ? Math.round(metrics.completedPercent) + '% • ' + metrics.completed + ' / ' + metrics.total + ' verarbeitet'
            : metrics.recordsSucceeded + ' ok / ' + metrics.recordsFailed + ' fail / ' + metrics.total + ' gesamt';

          return '<div class="' + wrapperClassName + '">' +
            '<div class="run-mini-gauge" title="' + esc(title) + '">' +
              '<span class="run-mini-gauge-segment is-success" style="width:' + metrics.successPercent.toFixed(2) + '%"></span>' +
              '<span class="run-mini-gauge-segment is-failed" style="width:' + metrics.failedPercent.toFixed(2) + '%"></span>' +
              '<span class="run-mini-gauge-segment is-pending" style="width:' + metrics.pendingPercent.toFixed(2) + '%"></span>' +
            '</div>' +
            '<div class="run-mini-gauge-meta">' + esc(metaPrefix + summary) + '</div>' +
          '</div>';
        }

        if (metrics.normalizedStatus === 'running') {
          return '<div class="' + wrapperClassName + '">' +
            '<div class="run-mini-gauge run-mini-gauge-activity" title="Gesamtmenge aktuell noch unbekannt">' +
              '<span class="run-mini-gauge-activity-indicator"></span>' +
            '</div>' +
            '<div class="run-mini-gauge-meta">' + esc(metaPrefix + 'läuft, Gesamtzahl noch unbekannt') + '</div>' +
          '</div>';
        }

        return '<div class="' + wrapperClassName + '"><div class="run-mini-gauge-empty">' + esc(metaPrefix + metrics.recordsSucceeded + ' ok / ' + metrics.recordsFailed + ' fail') + '</div></div>';
      }

      function renderSchedules() {
        const body = document.getElementById('schedules-body');
        const autoDisabledWarning = document.getElementById('schedulers-auto-disabled-warning');
        renderSchedulerConnectorFilterOptions();

        const autoDisabledCount = (state.schedules || []).filter((item) => item.autoDisabledDueToErrors && item.active === false).length;
        if (autoDisabledWarning) {
          if (autoDisabledCount > 0) {
            autoDisabledWarning.textContent = autoDisabledCount + ' Scheduler wurden wegen fortlaufender Fehler automatisch deaktiviert.';
            autoDisabledWarning.classList.remove('d-none');
          } else {
            autoDisabledWarning.textContent = '';
            autoDisabledWarning.classList.add('d-none');
          }
        }

        const filteredSchedules = (state.schedules || []).filter((item) => {
          const direction = String(item.direction || '').toLowerCase();
          if (state.schedulerDirectionTab === 'inbound' && direction !== 'inbound') {
            return false;
          }
          if (state.schedulerDirectionTab === 'outbound' && direction !== 'outbound') {
            return false;
          }
          if (state.schedulerActiveFilter === 'active' && !item.active) {
            return false;
          }
          if (state.schedulerActiveFilter === 'inactive' && item.active) {
            return false;
          }
          if (!state.schedulerConnectorFilterId) {
            return true;
          }
          return String(item.connectorId || '').trim() === state.schedulerConnectorFilterId;
        });

        if (!filteredSchedules.length) {
          body.innerHTML = '<tr><td colspan="6" class="text-secondary">Keine Scheduler gefunden.</td></tr>';
          return;
        }

        const scheduleById = new Map(filteredSchedules.map((item) => [item.id, item]));
        const childrenByParent = new Map();
        const roots = [];

        filteredSchedules.forEach((item) => {
          const parentId = String(item.parentScheduleId || '').trim();
          if (parentId && parentId !== item.id && scheduleById.has(parentId)) {
            if (!childrenByParent.has(parentId)) {
              childrenByParent.set(parentId, []);
            }
            childrenByParent.get(parentId).push(item);
            return;
          }
          roots.push(item);
        });

        const ordered = [];
        const depthById = new Map();
        const visited = new Set();

        function visit(node, depth, trail) {
          if (!node || visited.has(node.id) || trail.has(node.id)) {
            return;
          }
          trail.add(node.id);
          visited.add(node.id);
          depthById.set(node.id, depth);
          ordered.push(node);

          const children = (childrenByParent.get(node.id) || []).slice().sort((a, b) =>
            String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' })
          );
          children.forEach((child) => visit(child, depth + 1, trail));
          trail.delete(node.id);
        }

        roots
          .slice()
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' }))
          .forEach((root) => visit(root, 0, new Set()));

        filteredSchedules.forEach((item) => {
          if (!visited.has(item.id)) {
            visit(item, 0, new Set());
          }
        });

        body.innerHTML = ordered.map((item) => {
            const depth = Number(depthById.get(item.id) || 0);
            const indent = depth * 18;
            const parentName = item.parentScheduleId && scheduleById.get(item.parentScheduleId)
              ? String(scheduleById.get(item.parentScheduleId).name || item.parentScheduleId)
              : '-';
            const objectIcon = getObjectIcon(item.objectName);
            const connectorName = getConnectorNameById(item.connectorId);
            const intervalLabel = getScheduleIntervalLabel(item, scheduleById, false, new Set());
            const hierarchyBadge = depth > 0
              ? '<span class="badge bg-light text-dark border ms-1">Level ' + (depth + 1) + '</span>'
              : '<span class="badge bg-secondary-subtle text-secondary border ms-1">Root</span>';
            const activeHint = item.autoDisabledDueToErrors
              ? '<span class="badge bg-warning-subtle text-warning border mt-1" title="Automatisch wegen Fehlern deaktiviert">auto deaktiviert</span>'
              : '<span class="small text-secondary">' + (item.active ? 'aktiv' : 'inaktiv') + '</span>';
            const latestRun = getLatestRunForSchedule(item.id);
            const lastFailedRun = getLatestFailedRunForSchedule(item.id);
            const progressMarkup = renderRunProgressMarkup(latestRun, { compact: true });
            const errorMarkup = lastFailedRun
              ? '<button class="btn btn-sm btn-outline-danger mt-2" title="Letzter Fehler: ' + esc(lastFailedRun.errorMessage || 'Unbekannter Fehler') + '" data-show-run-logs="' + esc(lastFailedRun.id) + '">Fehlerdetails</button>'
              : '<span class="small text-secondary d-block mt-2">keine offenen Fehler</span>';
            
            return '<tr data-schedule-active="' + (item.active ? 'active' : 'inactive') + '">' +
              '<td><div style="padding-left:' + indent + 'px"><strong class="text-truncate d-block" title="' + esc(item.name) + '">' + esc(item.name) + hierarchyBadge + '</strong><div class="small text-secondary text-truncate" title="' + esc(item.objectName) + ' / ' + esc(item.operation) + '">' + objectIcon + ' ' + esc(item.objectName) + ' / ' + esc(item.operation) + '</div><div class="small text-secondary text-truncate mt-1" title="' + esc(parentName) + '">Parent: ' + esc(parentName) + (item.inheritTimingFromParent ? ' <span class="badge bg-primary-subtle text-primary border">inherits</span>' : '') + '</div></div></td>' +
              '<td><div class="form-check form-switch mb-1"><input class="form-check-input" type="checkbox" role="switch" data-toggle-schedule-active="' + esc(item.id) + '"' + (item.active ? ' checked' : '') + '></div>' + activeHint + '</td>' +
              '<td>' + getStatusBadge(item.status) + progressMarkup + errorMarkup + '</td>' +
              '<td><div class="fw-semibold text-truncate" title="' + esc(connectorName) + '">' + esc(connectorName) + '</div><div class="small text-secondary">' + esc(item.direction || '-') + '</div></td>' +
              '<td><div class="fw-semibold">' + esc(intervalLabel) + '</div><div class="small text-secondary">Nächster Lauf: ' + formatDate(item.nextRunAt, 'short') + '</div></td>' +
              '<td><div class="d-flex flex-wrap gap-1">' +
                '<button class="btn btn-sm btn-outline-primary" data-edit-schedule="' + esc(item.id) + '">Öffnen</button>' +
                '<button class="btn btn-sm btn-outline-secondary" data-dup-schedule="' + esc(item.id) + '">Dupl.</button>' +
                '<button class="btn btn-sm btn-outline-success" data-run-now="' + esc(item.id) + '">Run</button>' +
                '<button class="btn btn-sm btn-outline-info" data-dry-run="' + esc(item.id) + '">Dry-Run</button>' +
                '<button class="btn btn-sm btn-outline-danger" data-delete-schedule="' + esc(item.id) + '">Löschen</button>' +
              '</div>' +
              '</td>' +
            '</tr>';
        }).join('');

        body.querySelectorAll('button[data-edit-schedule]').forEach((button) => {
          button.addEventListener('click', () => openScheduleModal(button.getAttribute('data-edit-schedule')));
        });

        body.querySelectorAll('input[data-toggle-schedule-active]').forEach((input) => {
          input.addEventListener('change', async () => {
            const scheduleId = String(input.getAttribute('data-toggle-schedule-active') || '').trim();
            const nextActive = Boolean(input.checked);
            if (!scheduleId) {
              return;
            }
            try {
              await setScheduleActive(scheduleId, nextActive);
            } catch (error) {
              input.checked = !nextActive;
              showError(error.message || 'Scheduler-Status konnte nicht geändert werden');
            }
          });
        });

        body.querySelectorAll('button[data-dup-schedule]').forEach((button) => {
          button.addEventListener('click', async () => {
            await requestJson('/api/schedules/' + encodeURIComponent(button.getAttribute('data-dup-schedule')) + '/duplicate', { method: 'POST' });
            await refresh();
          });
        });

        body.querySelectorAll('button[data-run-now]').forEach((button) => {
          button.addEventListener('click', async () => {
            await requestJson('/api/schedules/' + encodeURIComponent(button.getAttribute('data-run-now')) + '/run', { method: 'POST' });
            await refresh();
          });
        });

        body.querySelectorAll('button[data-dry-run]').forEach((button) => {
          button.addEventListener('click', async () => {
            const scheduleId = button.getAttribute('data-dry-run');
            if (!scheduleId) {
              return;
            }

            const result = await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/dry-run', { method: 'POST' });
            const summary = result.ok
              ? 'OK: ' + (result.message || 'Quelle erreichbar')
              : 'Fehler: ' + (result.message || 'Dry-Run fehlgeschlagen');
            window.alert(summary);
          });
        });

        body.querySelectorAll('button[data-delete-schedule]').forEach((button) => {
          button.addEventListener('click', async () => {
            const scheduleId = button.getAttribute('data-delete-schedule');
            if (!scheduleId) {
              return;
            }

            const schedule = (state.schedules || []).find((item) => item.id === scheduleId);
            const confirmed = window.confirm('Scheduler "' + (schedule?.name || scheduleId) + '" inkl. Child-Scheduler löschen?');
            if (!confirmed) {
              return;
            }

            await requestJson('/api/schedules/' + encodeURIComponent(scheduleId), { method: 'DELETE' });
            await refresh();
          });
        });

        body.querySelectorAll('button[data-show-run-logs]').forEach((button) => {
          button.addEventListener('click', async () => {
            const runId = button.getAttribute('data-show-run-logs');
            if (!runId) {
              return;
            }
            const logs = await requestJson('/api/runs/' + encodeURIComponent(runId) + '/logs', {});
            const logList = (logs.items || []).map((log) => {
              return '[' + (log.level || 'INFO') + '] ' + (log.step || '') + ': ' + (log.message || '');
            }).join('\\n');
            window.alert('Fehlerlog für Run ' + runId + ':\\n\\n' + (logList || 'Keine Logs vorhanden'));
          });
        });

        applySchedulerTableClientFilters();

        setTimeout(() => initializeTableFilters(), 100);
      }

      function renderRuns() {
        const body = document.getElementById('runs-body');
        const select = document.getElementById('log-run-select');
        if (!state.runs.length) {
          body.innerHTML = '<tr><td colspan="7" class="text-secondary">Keine Runs gefunden.</td></tr>';
          select.innerHTML = '<option value="">Keine Runs</option>';
          return;
        }

        body.innerHTML = state.runs.map((item) =>
          (function() {
            const canCancel = String(item.status || '') === 'Running';
            const actionMarkup = canCancel
              ? '<button class="btn btn-sm btn-outline-danger" data-cancel-run="' + esc(item.id) + '">Abbrechen</button>'
              : '<span class="text-secondary small">-</span>';
            const durationMs = getRunDurationMs(item);
            return '<tr>' +
              '<td class="text-truncate" title="' + esc(item.scheduleName || item.scheduleId || '-') + '">' + esc(item.scheduleName || item.scheduleId || '-') + '</td>' +
              '<td>' + getStatusBadge(item.status) + '</td>' +
              '<td>' + esc(formatDate(item.startedAt || item.finishedAt, 'short')) + '</td>' +
              '<td>' + esc(formatDurationMinSec(durationMs)) + '</td>' +
              '<td>' + renderRunProgressMarkup(item) + '</td>' +
              '<td><button class="btn btn-sm btn-outline-primary" data-log-run="' + esc(item.id) + '">Logs</button></td>' +
              '<td>' + actionMarkup + '</td>' +
              '</tr>';
          })()
        ).join('');

        select.innerHTML = state.runs.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.scheduleName || item.id) + '</option>').join('');

        body.querySelectorAll('button[data-log-run]').forEach((button) => {
          button.addEventListener('click', async () => {
            select.value = button.getAttribute('data-log-run');
            await loadLogs();
          });
        });

        body.querySelectorAll('button[data-cancel-run]').forEach((button) => {
          button.addEventListener('click', async () => {
            const runId = button.getAttribute('data-cancel-run');
            if (!runId) {
              return;
            }
            const run = (state.runs || []).find((item) => item.id === runId);
            const label = run?.scheduleName || runId;
            if (!confirm('Laufenden Run für ' + label + ' wirklich abbrechen?')) {
              return;
            }
            try {
              await requestJson('/api/runs/' + encodeURIComponent(runId) + '/cancel', {
                method: 'POST'
              });
              await refresh({ refreshChart: false });
            } catch (error) {
              showError(error.message || 'Run konnte nicht abgebrochen werden');
            }
          });
        });
      }

      function formatRunAgeMinutes(ageMinutes) {
        const totalMinutes = Math.max(0, Number(ageMinutes) || 0);
        if (totalMinutes >= 60 * 24) {
          const days = Math.floor(totalMinutes / (60 * 24));
          const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
          return days + 'd ' + hours + 'h';
        }
        if (totalMinutes >= 60) {
          const hours = Math.floor(totalMinutes / 60);
          const minutes = totalMinutes % 60;
          return hours + 'h ' + minutes + 'm';
        }
        return totalMinutes + ' min';
      }

      function renderStaleRuns() {
        const body = document.getElementById('stale-runs-body');
        if (!body) {
          return;
        }

        const staleRuns = Array.isArray(state.staleRuns) ? state.staleRuns : [];
        if (!staleRuns.length) {
          body.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine stale Runs gefunden.</td></tr>';
          return;
        }

        body.innerHTML = staleRuns.map((item) =>
          '<tr>' +
          '<td class="text-truncate" title="' + esc(item.scheduleName || item.scheduleId || item.id) + '">' + esc(item.scheduleName || item.scheduleId || item.id) + '</td>' +
          '<td>' + esc(formatDate(item.startedAt, 'short')) + '</td>' +
          '<td><span class="badge text-bg-warning">' + esc(formatRunAgeMinutes(item.ageMinutes)) + '</span></td>' +
          '<td><button class="btn btn-sm btn-outline-danger" data-release-stale-run="' + esc(item.id) + '">Freigeben</button></td>' +
          '</tr>'
        ).join('');

        body.querySelectorAll('button[data-release-stale-run]').forEach((button) => {
          button.addEventListener('click', async () => {
            try {
              await requestJson('/api/runs/release-stale', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ runIds: [button.getAttribute('data-release-stale-run')] })
              });
              await refresh({ refreshChart: false });
            } catch (error) {
              showError(error.message || 'Stale Run konnte nicht freigegeben werden');
            }
          });
        });
      }

      function getConnectorIdsWithSchedulers() {
        return new Set(
          (state.schedules || [])
            .map((schedule) => String(schedule.connectorId || '').trim())
            .filter(Boolean)
        );
      }

      function parseTimingIntervalMinutes(timingDefinition) {
        if (!timingDefinition) {
          return undefined;
        }

        try {
          const parsed = JSON.parse(String(timingDefinition));
          if (Number.isFinite(parsed?.intervalMinutes) && Number(parsed.intervalMinutes) > 0) {
            return Number(parsed.intervalMinutes);
          }

          if (Array.isArray(parsed?.rules)) {
            const ruleWithInterval = parsed.rules.find((rule) => Number.isFinite(rule?.intervalMinutes) && Number(rule.intervalMinutes) > 0);
            if (ruleWithInterval) {
              return Number(ruleWithInterval.intervalMinutes);
            }
          }
        } catch {
          return undefined;
        }

        return undefined;
      }

      function getScheduleIntervalLabel(schedule, scheduleById, fromParent, trail) {
        const visited = trail || new Set();
        if (!schedule || visited.has(schedule.id)) {
          return fromParent ? 'Parent' : '-';
        }

        visited.add(schedule.id);

        const ownInterval = parseTimingIntervalMinutes(schedule.timingDefinition);
        if (Number.isFinite(ownInterval) && ownInterval > 0) {
          return String(ownInterval) + ' min' + (fromParent ? ' (Parent)' : '');
        }

        const parentId = String(schedule.parentScheduleId || '').trim();
        if (schedule.inheritTimingFromParent && parentId) {
          const parent = scheduleById.get(parentId);
          if (!parent) {
            return 'Parent';
          }
          return getScheduleIntervalLabel(parent, scheduleById, true, visited);
        }

        return fromParent ? 'Parent' : '-';
      }

      function renderOverviewConnectorFilter() {
        const select = document.getElementById('overview-connector-filter');
        if (!select) {
          return;
        }

        const connectorIdsWithSchedulers = getConnectorIdsWithSchedulers();
        const selectableConnectors = (state.connectors || [])
          .filter((connector) => connectorIdsWithSchedulers.has(String(connector.id || '').trim()))
          .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' }));

        const schedulerCountByConnectorId = new Map();
        (state.schedules || []).forEach((schedule) => {
          const connectorId = String(schedule.connectorId || '').trim();
          if (!connectorId) {
            return;
          }
          schedulerCountByConnectorId.set(connectorId, (schedulerCountByConnectorId.get(connectorId) || 0) + 1);
        });

        if (!selectableConnectors.some((connector) => String(connector.id) === state.overviewConnectorFilterId)) {
          state.overviewConnectorFilterId = '';
        }

        const options = ['<option value="">Alle mit Schedulern</option>'];
        selectableConnectors.forEach((connector) => {
          const connectorId = String(connector.id || '');
          const scheduleCount = Number(schedulerCountByConnectorId.get(connectorId) || 0);
          const label = String(connector.name || connectorId) + ' (' + scheduleCount + ')';
          options.push('<option value="' + esc(connectorId) + '">' + esc(label) + '</option>');
        });

        select.innerHTML = options.join('');
        select.value = state.overviewConnectorFilterId || '';
        select.disabled = selectableConnectors.length === 0;
      }

      function buildFilteredOverviewGraph(graph) {
        const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
        const edges = Array.isArray(graph?.edges) ? graph.edges : [];
        const selectedConnectorId = String(state.overviewConnectorFilterId || '').trim();

        const activeSchedules = (state.schedules || []).filter((schedule) => Boolean(schedule?.active));
        const activeScheduleIds = new Set(
          activeSchedules
            .map((schedule) => String(schedule.id || '').trim())
            .filter(Boolean)
        );
        const activeConnectorIds = new Set(
          activeSchedules
            .map((schedule) => String(schedule.connectorId || '').trim())
            .filter(Boolean)
        );
        const connectorActiveById = new Map(
          (state.connectors || []).map((connector) => [String(connector.id || '').trim(), Boolean(connector.active)])
        );

        const baseNodes = nodes.filter((node) => {
          if (node.kind === 'scheduler') {
            return activeScheduleIds.has(String(node.refId || '').trim());
          }
          if (node.kind === 'connector') {
            const connectorId = String(node.refId || '').trim();
            return activeConnectorIds.has(connectorId) && connectorActiveById.get(connectorId) !== false;
          }
          return true;
        });
        const baseNodeIds = new Set(baseNodes.map((node) => String(node.id || '')));
        const baseEdges = edges.filter(
          (edge) =>
            baseNodeIds.has(String(edge.from || '')) &&
            baseNodeIds.has(String(edge.to || ''))
        );

        if (!selectedConnectorId) {
          return { nodes: baseNodes, edges: baseEdges };
        }

        const selectedConnectorNode = baseNodes.find(
          (node) => node.kind === 'connector' && String(node.refId || '').trim() === selectedConnectorId
        );

        if (!selectedConnectorNode) {
          return { nodes: baseNodes, edges: baseEdges };
        }

        const outgoingEdgesByNode = new Map();
        baseEdges.forEach((edge) => {
          const fromId = String(edge.from || '');
          if (!outgoingEdgesByNode.has(fromId)) {
            outgoingEdgesByNode.set(fromId, []);
          }
          outgoingEdgesByNode.get(fromId).push(edge);
        });

        const keepNodeIds = new Set([selectedConnectorNode.id]);
        const queue = [selectedConnectorNode.id];

        while (queue.length) {
          const currentNodeId = queue.shift();
          const outgoing = outgoingEdgesByNode.get(String(currentNodeId || '')) || [];
          outgoing.forEach((edge) => {
            const toId = String(edge.to || '');
            if (!toId || keepNodeIds.has(toId)) {
              return;
            }
            keepNodeIds.add(toId);
            queue.push(toId);
          });
        }

        return {
          nodes: baseNodes.filter((node) => keepNodeIds.has(String(node.id || ''))),
          edges: baseEdges.filter(
            (edge) =>
              keepNodeIds.has(String(edge.from || '')) &&
              keepNodeIds.has(String(edge.to || ''))
          )
        };
      }

      function relayoutOverviewGraph(graph) {
        const nodes = Array.isArray(graph?.nodes) ? graph.nodes.map((node) => ({ ...node })) : [];
        const edges = Array.isArray(graph?.edges) ? graph.edges.slice() : [];
        if (!nodes.length) {
          return { nodes, edges };
        }

        const baseX = 30;
        const columnGap = 320;
        const baseY = 26;
        const rowGap = 18;
        const nodeHeight = 82;
        const rootGap = 36;
        const connectors = nodes.filter((node) => node.kind === 'connector');
        const schedulers = nodes.filter((node) => node.kind === 'scheduler');
        const schedulerById = new Map(schedulers.map((node) => [String(node.id || ''), node]));
        const childrenByParent = new Map();
        const parentByChild = new Map();
        const rootScheduleIds = [];
        const schedulerIds = new Set(schedulers.map((node) => String(node.id || '')));
        const connectorTargetsByConnector = new Map();

        schedulers.forEach((node) => {
          childrenByParent.set(String(node.id || ''), []);
        });

        edges.forEach((edge) => {
          const from = String(edge.from || '');
          const to = String(edge.to || '');
          if (schedulerIds.has(from) && schedulerIds.has(to)) {
            if (!childrenByParent.has(from)) {
              childrenByParent.set(from, []);
            }
            childrenByParent.get(from).push(to);
            parentByChild.set(to, from);
            return;
          }
          const fromNode = nodes.find((node) => String(node.id || '') === from);
          if (fromNode?.kind === 'connector' && schedulerIds.has(to)) {
            if (!connectorTargetsByConnector.has(from)) {
              connectorTargetsByConnector.set(from, []);
            }
            connectorTargetsByConnector.get(from).push(to);
          }
        });

        schedulers.forEach((node) => {
          const id = String(node.id || '');
          if (!parentByChild.has(id)) {
            rootScheduleIds.push(id);
          }
        });

        rootScheduleIds.sort((leftId, rightId) =>
          String(schedulerById.get(leftId)?.label || '').localeCompare(
            String(schedulerById.get(rightId)?.label || ''),
            'de',
            { sensitivity: 'base' }
          )
        );

        childrenByParent.forEach((childIds) => {
          childIds.sort((leftId, rightId) =>
            String(schedulerById.get(leftId)?.label || '').localeCompare(
              String(schedulerById.get(rightId)?.label || ''),
              'de',
              { sensitivity: 'base' }
            )
          );
        });

        const subtreeHeightCache = new Map();
        const computeSubtreeHeight = (scheduleId) => {
          if (subtreeHeightCache.has(scheduleId)) {
            return subtreeHeightCache.get(scheduleId);
          }

          const childIds = childrenByParent.get(scheduleId) || [];
          if (!childIds.length) {
            subtreeHeightCache.set(scheduleId, nodeHeight);
            return nodeHeight;
          }

          const childrenHeight = childIds.reduce((sum, childId, index) => {
            const nextSum = sum + computeSubtreeHeight(childId);
            return index < childIds.length - 1 ? nextSum + rowGap : nextSum;
          }, 0);
          const height = Math.max(nodeHeight, childrenHeight);
          subtreeHeightCache.set(scheduleId, height);
          return height;
        };

        const placeSchedule = (scheduleId, depth, topY) => {
          const node = schedulerById.get(scheduleId);
          if (!node) {
            return nodeHeight;
          }

          const childIds = childrenByParent.get(scheduleId) || [];
          const childrenHeight = childIds.length
            ? childIds.reduce((sum, childId, index) => {
                const nextSum = sum + computeSubtreeHeight(childId);
                return index < childIds.length - 1 ? nextSum + rowGap : nextSum;
              }, 0)
            : 0;
          const subtreeHeight = Math.max(nodeHeight, childrenHeight || 0);
          node.x = baseX + columnGap + depth * columnGap;
          node.y = topY + Math.max(0, (subtreeHeight - nodeHeight) / 2);

          if (childIds.length) {
            let cursorY = topY;
            childIds.forEach((childId) => {
              const childHeight = computeSubtreeHeight(childId);
              placeSchedule(childId, depth + 1, cursorY);
              cursorY += childHeight + rowGap;
            });
          }

          return subtreeHeight;
        };

        let cursorY = baseY;
        rootScheduleIds.forEach((scheduleId, index) => {
          const height = computeSubtreeHeight(scheduleId);
          placeSchedule(scheduleId, 0, cursorY);
          cursorY += height + (index < rootScheduleIds.length - 1 ? rootGap : 0);
        });

        const fallbackConnectorY = new Map();
        connectors
          .slice()
          .sort((left, right) => String(left.label || '').localeCompare(String(right.label || ''), 'de', { sensitivity: 'base' }))
          .forEach((node, index) => {
            fallbackConnectorY.set(String(node.id || ''), baseY + index * (nodeHeight + rootGap));
          });

        connectors.forEach((node) => {
          const connectorId = String(node.id || '');
          const targetIds = (connectorTargetsByConnector.get(connectorId) || []).filter((targetId) => schedulerById.has(targetId));
          node.x = baseX;
          if (!targetIds.length) {
            node.y = fallbackConnectorY.get(connectorId) || baseY;
            return;
          }

          const averageY = targetIds.reduce((sum, targetId) => {
            const targetNode = schedulerById.get(targetId);
            return sum + Number(targetNode?.y || baseY);
          }, 0) / targetIds.length;
          node.y = Math.max(baseY, averageY);
        });

        return { nodes, edges };
      }

      function redrawOverviewGraph() {
        const filteredGraph = buildFilteredOverviewGraph(state.graphData);
        drawGraph(relayoutOverviewGraph(filteredGraph));
      }

      function drawGraph(graph) {
        const svg = document.getElementById('graph');
        const nodes = graph.nodes || [];
        const edges = graph.edges || [];
        const visibleScheduleCountElement = document.getElementById('overview-visible-schedule-count');
        if (visibleScheduleCountElement) {
          const visibleScheduleCount = nodes.filter((node) => node.kind === 'scheduler').length;
          visibleScheduleCountElement.textContent = String(visibleScheduleCount) + ' Scheduler sichtbar';
        }
        const latestRunByScheduleId = new Map();
        (state.runs || []).forEach((run) => {
          const scheduleId = String(run.scheduleId || '').trim();
          if (!scheduleId) {
            return;
          }

          const previous = latestRunByScheduleId.get(scheduleId);
          const previousTime = previous
            ? new Date(previous.startedAt || previous.finishedAt || 0).getTime()
            : -Infinity;
          const currentTime = new Date(run.startedAt || run.finishedAt || 0).getTime();
          if (!previous || currentTime >= previousTime) {
            latestRunByScheduleId.set(scheduleId, run);
          }
        });
        const scheduleById = new Map((state.schedules || []).map((schedule) => [String(schedule.id || ''), schedule]));
        const getLatestFailedRun = (scheduleId) => {
          return (state.runs || [])
            .filter((run) => String(run.scheduleId || '') === String(scheduleId || '') && normalizeRunStatus(run.status) === 'failed')
            .sort((a, b) => {
              const timeA = new Date(a.finishedAt || a.startedAt || 0).getTime();
              const timeB = new Date(b.finishedAt || b.startedAt || 0).getTime();
              return timeB - timeA;
            })[0] || null;
        };
        const getLatestRunningRun = (scheduleId) => {
          return (state.runs || [])
            .filter((run) => String(run.scheduleId || '') === String(scheduleId || '') && normalizeRunStatus(run.status) === 'running')
            .sort((a, b) => {
              const timeA = new Date(a.startedAt || a.finishedAt || 0).getTime();
              const timeB = new Date(b.startedAt || b.finishedAt || 0).getTime();
              return timeB - timeA;
            })[0] || null;
        };
        const getScheduleGraphStatus = (scheduleId) => {
          const schedule = scheduleById.get(String(scheduleId || ''));
          const latestRun = latestRunByScheduleId.get(String(scheduleId || ''));
          const latestRunStatus = normalizeRunStatus(latestRun?.status);
          if (latestRunStatus === 'running' || latestRunStatus === 'in-progress') {
            return { key: 'running', label: 'Läuft' };
          }
          if (latestRunStatus === 'failed' || latestRunStatus === 'error') {
            return { key: 'failed', label: 'Fehler' };
          }
          const scheduleStatus = String(schedule?.status || '').trim().toLowerCase();
          if (!schedule?.active || scheduleStatus === 'inactive') {
            return { key: 'inactive', label: 'Inaktiv' };
          }
          if (scheduleStatus === 'due') {
            return { key: 'due', label: 'Fällig' };
          }
          if (latestRunStatus === 'success' || latestRunStatus === 'succeeded') {
            return { key: 'success', label: 'OK' };
          }
          return { key: 'scheduled', label: 'Geplant' };
        };
        const nodeMap = new Map(nodes.map((node) => [node.id, node]));
        const nodeWidth = 260;
        const nodeHeight = 82;
        const maxY = Math.max(360, ...nodes.map((node) => Number(node.y) + nodeHeight + 20));
        const maxX = Math.max(920, ...nodes.map((node) => Number(node.x) + nodeWidth + 24));
        svg.setAttribute('height', String(maxY));
        svg.setAttribute('width', String(maxX));

        const defs = '<defs>' +
          '<marker id="arrowInbound" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
            '<polygon points="0,1 0,9 12,5" style="fill:#2276d2" />' +
          '</marker>' +
          '<marker id="arrowOutbound" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
            '<polygon points="0,1 0,9 12,5" style="fill:#2e9b4d" />' +
          '</marker>' +
          '<marker id="arrowGeneric" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
            '<polygon points="0,1 0,9 12,5" style="fill:#7f8b95" />' +
          '</marker>' +
        '</defs>';

        const outgoingEdgeOrder = new Map();
        const incomingEdgeOrder = new Map();

        nodes.forEach((node) => {
          const outgoing = edges
            .filter((edge) => edge.from === node.id)
            .slice()
            .sort((a, b) => {
              const nodeA = nodeMap.get(a.to);
              const nodeB = nodeMap.get(b.to);
              return Number(nodeA?.y || 0) - Number(nodeB?.y || 0);
            });
          outgoing.forEach((edge, index) => {
            outgoingEdgeOrder.set(String(edge.from) + '::' + String(edge.to), {
              index,
              total: outgoing.length
            });
          });

          const incoming = edges
            .filter((edge) => edge.to === node.id)
            .slice()
            .sort((a, b) => {
              const nodeA = nodeMap.get(a.from);
              const nodeB = nodeMap.get(b.from);
              return Number(nodeA?.y || 0) - Number(nodeB?.y || 0);
            });
          incoming.forEach((edge, index) => {
            incomingEdgeOrder.set(String(edge.from) + '::' + String(edge.to), {
              index,
              total: incoming.length
            });
          });
        });

        const edgeMarkup = edges.map((edge) => {
          const from = nodeMap.get(edge.from);
          const to = nodeMap.get(edge.to);
          if (!from || !to) return '';

          const normalizedDirection = String(edge.direction || '').toLowerCase();
          const isInbound = normalizedDirection === 'inbound';
          const isOutbound = normalizedDirection === 'outbound';
          const edgeColor = isInbound ? '#2276d2' : isOutbound ? '#2e9b4d' : '#7f8b95';
          const markerId = isInbound ? 'arrowInbound' : isOutbound ? 'arrowOutbound' : 'arrowGeneric';
          const edgeKey = String(edge.from) + '::' + String(edge.to);
          const outgoingOrder = outgoingEdgeOrder.get(edgeKey);
          const incomingOrder = incomingEdgeOrder.get(edgeKey);
          const outgoingOffset = outgoingOrder
            ? ((outgoingOrder.index + 1) / (outgoingOrder.total + 1) - 0.5) * Math.min(44, nodeHeight - 26)
            : 0;
          const incomingOffset = incomingOrder
            ? ((incomingOrder.index + 1) / (incomingOrder.total + 1) - 0.5) * Math.min(44, nodeHeight - 26)
            : 0;
          const startX = Number(from.x) + nodeWidth;
          const startY = Number(from.y) + nodeHeight / 2 + outgoingOffset;
          const endX = Number(to.x) - 14;
          const endY = Number(to.y) + nodeHeight / 2 + incomingOffset;
          const horizontalGap = Math.max(18, Math.min(34, (endX - startX) / 4));
          const curveStartX = startX + horizontalGap;
          const curveEndX = endX - horizontalGap;
          const controlOffset = Math.max(30, (curveEndX - curveStartX) / 2);
          const pathData = 'M ' + startX + ' ' + startY +
            ' L ' + curveStartX + ' ' + startY +
            ' C ' + (curveStartX + controlOffset) + ' ' + startY + ', ' + (curveEndX - controlOffset) + ' ' + endY + ', ' + curveEndX + ' ' + endY +
            ' L ' + endX + ' ' + endY;
          const fromStatus = from.kind === 'scheduler' ? getScheduleGraphStatus(from.refId).key : '';
          const toStatus = to.kind === 'scheduler' ? getScheduleGraphStatus(to.refId).key : '';
          const edgeClasses = ['graph-edge'];
          if (fromStatus === 'running' || toStatus === 'running') {
            edgeClasses.push('graph-edge-running');
          }
          if (fromStatus === 'failed' || toStatus === 'failed') {
            edgeClasses.push('graph-edge-failed');
          }

          return '<path class="' + edgeClasses.join(' ') + '" style="stroke:' + edgeColor + ';stroke-width:2.5;fill:none;opacity:0.9;stroke-linecap:round;stroke-linejoin:round" marker-end="url(#' + markerId + ')" d="' + pathData + '" />';
        }).join('');

        const nodeMarkup = nodes.map((node) => {
          const isInbound = String(node.direction || '').toLowerCase() === 'inbound';
          const schedulerUsesFile = String(node.sourceType || '').toUpperCase().startsWith('FILE_') || String(node.targetType || '').toUpperCase().startsWith('FILE_');
          const schedulerStatus = node.kind === 'scheduler'
            ? getScheduleGraphStatus(node.refId)
            : null;
          const directionClass = node.kind === 'scheduler'
            ? ((isInbound ? 'graph-inbound' : 'graph-outbound') + ' ' + (schedulerUsesFile ? 'graph-scheduler-file' : 'graph-scheduler-db') + ' graph-node-status-' + schedulerStatus.key)
            : 'graph-connector ' + getConnectorGraphClass(node.connectorType, node.label);
          const titleLines = node.kind === 'scheduler'
            ? splitGraphTextByLine(node.label, [16, 18])
            : splitGraphText(node.label, 24, 2);
          const subtitleLines = splitGraphText(node.subtitle, 28, 2);
          const icon = node.kind === 'scheduler'
            ? getObjectIcon(node.objectName)
            : getConnectorIcon(node.connectorType, node.label);
          const metaLabel = node.kind === 'scheduler'
            ? (isInbound ? 'Inbound' : 'Outbound')
            : String(node.connectorType || 'Connector').toUpperCase();
          const metaLines = splitGraphText(metaLabel, 22, 1);
          const failedRun = node.kind === 'scheduler' && schedulerStatus?.key === 'failed'
            ? getLatestFailedRun(node.refId)
            : null;
          const runningRun = node.kind === 'scheduler' && schedulerStatus?.key === 'running'
            ? getLatestRunningRun(node.refId)
            : null;
          const failedRunErrorMessage = failedRun?.errorMessage
            ? String(failedRun.errorMessage).trim()
            : '';
          const titleY = 28;
          const subtitleY = titleLines.length > 1 ? 60 : 48;
          const metaY = titleLines.length > 1 ? 74 : 68;
          const titleMarkup = renderGraphText('graph-title', 58, titleY, titleLines, 14);
          const subtitleMarkup = renderGraphText('graph-subtitle', 58, subtitleY, subtitleLines, 13);
          const metaMarkup = renderGraphText('graph-meta', 58, metaY, metaLines, 12);
          const pillRun = schedulerStatus?.key === 'running' ? runningRun : failedRun;
          const pillAction = schedulerStatus?.key === 'running' ? 'cancel' : (failedRun?.id ? 'logs' : '');
          const statusPillMarkup = node.kind === 'scheduler'
            ? '<g class="graph-status-pill graph-status-pill-' + esc(schedulerStatus.key) + (pillRun?.id ? ' graph-status-pill-clickable' : '') + '" transform="translate(156,8)"' +
                (pillRun?.id ? ' data-run-id="' + esc(pillRun.id) + '"' : '') +
                (pillAction ? ' data-pill-action="' + esc(pillAction) + '"' : '') +
                (failedRunErrorMessage ? ' data-error-message="' + esc(failedRunErrorMessage) + '"' : '') +
                (pillRun?.id ? ' data-schedule-name="' + esc(node.label || node.refId || '') + '"' : '') +
                '>' +
                '<rect class="graph-status-pill-bg" width="92" height="22" rx="11" />' +
                '<circle class="graph-status-pill-dot" cx="11" cy="10" r="4" />' +
                '<text class="graph-status-pill-label" x="20" y="13">' + esc(schedulerStatus.label) + '</text>' +
              '</g>'
            : '';
          const togglePillMarkup = node.kind === 'scheduler'
            ? '<g class="graph-status-pill graph-status-pill-clickable" transform="translate(152,38)" data-toggle-schedule-id="' + esc(node.refId || '') + '" data-next-active="' + esc(String(!node.active)) + '">' +
                '<text class="graph-status-pill-label" x="0" y="12" style="fill:#5f6b76;font-size:10px;font-weight:700">Aktiv</text>' +
                '<rect x="40" y="0" width="46" height="20" rx="10" style="fill:' + (node.active ? '#2e9b4d' : '#a0aab5') + ';opacity:0.95" />' +
                '<circle cx="' + (node.active ? '74' : '52') + '" cy="10" r="7" style="fill:#ffffff;stroke:rgba(47,64,80,0.15);stroke-width:1" />' +
              '</g>'
            : '';

          // Inline styles to guarantee fill even when external CSS is not applied to inline SVG
          let cardBgStyle, accentStyle, badgeStyle, iconStyle, metaStyle;
          if (node.kind === 'scheduler') {
            if (isInbound) {
              cardBgStyle = 'fill:#edf5ff;stroke:#2276d2;stroke-width:2';
              accentStyle = 'fill:#2276d2;stroke:none';
              badgeStyle = 'fill:#dcebff;stroke:rgba(34,118,210,0.15);stroke-width:1';
              iconStyle = 'fill:#2276d2';
              metaStyle = 'fill:#2276d2';
            } else {
              cardBgStyle = 'fill:#eefaf0;stroke:#2e9b4d;stroke-width:2';
              accentStyle = 'fill:#2e9b4d;stroke:none';
              badgeStyle = 'fill:#dcf3e0;stroke:rgba(46,155,77,0.15);stroke-width:1';
              iconStyle = 'fill:#2e9b4d';
              metaStyle = 'fill:#2e9b4d';
            }
          } else {
            const connectorClass = getConnectorGraphClass(node.connectorType, node.label);
            const colorMap = {
              'graph-connector-salesforce': { card: 'fill:#eef8ff;stroke:#3a8dde', accent: '#2d7dd2', badge: '#dceefe' },
              'graph-connector-mssql':      { card: 'fill:#f3f8f3;stroke:#2f8f5b', accent: '#2f8f5b', badge: '#e0f1e6' },
              'graph-connector-file':       { card: 'fill:#fff8ec;stroke:#d78c1d', accent: '#d78c1d', badge: '#ffefcf' },
              'graph-connector-mock':       { card: 'fill:#f6eefc;stroke:#8a56c2', accent: '#8a56c2', badge: '#eadcf8' },
              'graph-connector-erp':        { card: 'fill:#fff2ea;stroke:#c7683b', accent: '#c7683b', badge: '#fde2d6' },
              'graph-connector-generic':    { card: 'fill:#f7f8f9;stroke:#7f8b95', accent: '#7f8b95', badge: '#e9edf0' },
            };
            const colors = colorMap[connectorClass] || colorMap['graph-connector-generic'];
            cardBgStyle = colors.card + ';stroke-width:2';
            accentStyle = 'fill:' + colors.accent + ';stroke:none';
            badgeStyle = 'fill:' + colors.badge + ';stroke:rgba(47,64,80,0.1);stroke-width:1';
            iconStyle = 'fill:' + colors.accent;
            metaStyle = 'fill:#5f6b76';
          }

          return (
            '<g class="graph-node ' + directionClass + '" data-kind="' + esc(node.kind) + '" data-ref-id="' + esc(node.refId) + '" data-connector-type="' + esc(node.connectorType || '') + '" transform="translate(' + Number(node.x) + ',' + Number(node.y) + ')" title="' + esc(node.label) + '">' +
              '<rect class="graph-card-bg" style="' + cardBgStyle + '" width="' + nodeWidth + '" height="' + nodeHeight + '" rx="16" />' +
              '<rect class="graph-accent" style="' + accentStyle + '" width="10" height="' + nodeHeight + '" rx="8" />' +
              '<circle class="graph-icon-badge" style="' + badgeStyle + '" cx="30" cy="41" r="18" />' +
              '<text class="graph-icon" style="' + iconStyle + '" x="30" y="47">' + esc(icon) + '</text>' +
              statusPillMarkup +
              togglePillMarkup +
              titleMarkup.replace('<text ', '<text style="fill:#2f4050;font-weight:700;font-size:12px" ') +
              subtitleMarkup.replace('<text ', '<text style="fill:#66717d;font-size:11px" ') +
              metaMarkup.replace('<text ', '<text style="' + metaStyle + ';font-size:10px;font-weight:700;letter-spacing:0.6px" ') +
            '</g>'
          );
        }).join('');

        svg.innerHTML = defs + edgeMarkup + nodeMarkup;
        svg.querySelectorAll('g.graph-status-pill[data-run-id]').forEach((pillEl) => {
          pillEl.addEventListener('click', async (event) => {
            event.stopPropagation();
            const runId = String(pillEl.getAttribute('data-run-id') || '').trim();
            const action = String(pillEl.getAttribute('data-pill-action') || '').trim();
            const errorMessage = String(pillEl.getAttribute('data-error-message') || '').trim();
            const scheduleName = String(pillEl.getAttribute('data-schedule-name') || '').trim();
            if (!runId) {
              return;
            }
            if (action === 'cancel') {
              if (!window.confirm('Laufenden Run für ' + (scheduleName || 'diesen Scheduler') + ' wirklich abbrechen?')) {
                return;
              }
              try {
                await requestJson('/api/runs/' + encodeURIComponent(runId) + '/cancel', {
                  method: 'POST'
                });
                await refresh({ refreshChart: false });
              } catch (error) {
                showError(error.message || 'Run konnte nicht abgebrochen werden');
              }
              return;
            }
            if (errorMessage) {
              window.alert('Letzter Fehler für ' + (scheduleName || 'diesen Scheduler') + ':\\n\\n' + errorMessage);
              return;
            }

            const logs = await requestJson('/api/runs/' + encodeURIComponent(runId) + '/logs', {});
            const logList = (logs.items || []).map((log) => {
              return '[' + (log.level || 'INFO') + '] ' + (log.step || '') + ': ' + (log.message || '');
            }).join('\\n');
            window.alert('Fehlerdetails für ' + (scheduleName || 'diesen Scheduler') + ':\\n\\n' + (logList || 'Keine Fehlerdetails vorhanden.'));
          });
        });
        svg.querySelectorAll('g.graph-status-pill[data-toggle-schedule-id]').forEach((pillEl) => {
          pillEl.addEventListener('click', async (event) => {
            event.stopPropagation();
            const scheduleId = String(pillEl.getAttribute('data-toggle-schedule-id') || '').trim();
            const nextActive = String(pillEl.getAttribute('data-next-active') || '').trim() === 'true';
            if (!scheduleId) {
              return;
            }
            try {
              await setScheduleActive(scheduleId, nextActive);
            } catch (error) {
              showError(error.message || 'Scheduler-Status konnte nicht geändert werden');
            }
          });
        });
        svg.querySelectorAll('g.graph-node').forEach((nodeEl) => {
          nodeEl.addEventListener('click', () => {
            const kind = nodeEl.getAttribute('data-kind');
            const refId = nodeEl.getAttribute('data-ref-id');
            if (kind === 'connector') {
              openConnectorModal(refId);
            }
            if (kind === 'scheduler') {
              openScheduleModal(refId);
            }
          });

          const kind = nodeEl.getAttribute('data-kind');
          const refId = nodeEl.getAttribute('data-ref-id');
          const connectorType = nodeEl.getAttribute('data-connector-type');
          if (kind === 'connector' && isFileConnectorType(connectorType)) {
            nodeEl.addEventListener('dragover', (event) => {
              event.preventDefault();
              nodeEl.classList.add('graph-drop-target');
            });

            nodeEl.addEventListener('dragleave', () => {
              nodeEl.classList.remove('graph-drop-target');
            });

            nodeEl.addEventListener('drop', async (event) => {
              event.preventDefault();
              nodeEl.classList.remove('graph-drop-target');
              const file = event.dataTransfer?.files?.[0];
              if (!file) {
                return;
              }

              try {
                await createSchedulerFromDroppedFile(refId, file);
              } catch (error) {
                showError(error.message || 'Datei konnte nicht als Scheduler importiert werden');
              }
            });
          }
        });
      }

      function collectScheduleFormPayload() {
        const selectedWeekdays = Array.from(document.querySelectorAll('#sch-weekdays input:checked'))
          .map((input) => Number(input.value))
          .filter((value) => !Number.isNaN(value));
        const timingDefinition = {
          days: selectedWeekdays,
          intervalMinutes: Number(document.getElementById('sch-timing-interval').value || 2),
          startTime: document.getElementById('sch-timing-time').value || '09:00'
        };

        return {
          id: document.getElementById('sch-id').value || undefined,
          name: document.getElementById('sch-name').value || undefined,
          active: document.getElementById('sch-active').checked,
          sourceSystem: normalizeSystemValue(document.getElementById('sch-source-system').value),
          targetSystem: normalizeSystemValue(document.getElementById('sch-target-system').value),
          objectName: document.getElementById('sch-object').value,
          operation: normalizeOperationValue(document.getElementById('sch-operation').value),
          connectorId: document.getElementById('sch-connector').value || undefined,
          parentScheduleId: document.getElementById('sch-parent-schedule').value || undefined,
          inheritTimingFromParent: document.getElementById('sch-inherit-parent-timing').checked,
          sourceType: document.getElementById('sch-source-type').value || undefined,
          targetType: document.getElementById('sch-target-type').value || undefined,
          direction: document.getElementById('sch-direction').value || undefined,
          batchSize: Number(document.getElementById('sch-batch-size').value || 100),
          nextRunAt: localDateTimeInputToIso(document.getElementById('sch-next-run').value),
          lastRunAt: localDateTimeInputToIso(document.getElementById('sch-last-run').value),
          sourceDefinition: buildScheduleSourceDefinitionValue(),
          targetDefinition: buildScheduleTargetDefinitionValue(),
          mappingDefinition: document.getElementById('sch-mapping').value || undefined,
          timingDefinition: JSON.stringify(timingDefinition)
        };
      }

      async function saveCurrentAsTemplate(kind) {
        const payload = kind === 'schedule'
          ? { kind, schedule: collectScheduleFormPayload() }
          : { kind, connector: collectConnectorFormPayload() };
        const defaultName = kind === 'schedule'
          ? String(payload.schedule?.name || document.getElementById('sch-object').value || 'Neue Scheduler Vorlage').trim()
          : String(payload.connector?.name || document.getElementById('con-name').value || 'Neue Connector Vorlage').trim();
        const name = window.prompt('Vorlagenname', defaultName || 'Neue Vorlage');
        if (name === null) {
          return;
        }
        const description = window.prompt('Kurzbeschreibung (optional)', '');
        const result = await requestJson('/api/templates', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...payload,
            name,
            description: description === null ? undefined : description
          })
        });
        window.alert('Vorlage gespeichert: ' + (result.name || name));
      }

      async function openScheduleModal(scheduleId, templateDraft) {
        const entry = scheduleId
          ? state.schedules.find((item) => item.id === scheduleId)
          : (templateDraft || null);
        if (!state.scheduleOptions || !Array.isArray(state.scheduleOptions.objectNames) || !state.scheduleOptions.objectNames.length) {
          await loadScheduleOptions();
        }

        document.getElementById('sch-id').value = entry?.id || '';
        document.getElementById('sch-name').value = entry?.name || '';
        renderScheduleConnectorOptions(entry?.connectorId || '');
        renderScheduleParentOptions(entry?.id || '', entry?.parentScheduleId || '');
        document.getElementById('sch-inherit-parent-timing').checked = !!entry?.inheritTimingFromParent;
        renderSelectOptions('sch-source-system', state.scheduleOptions.sourceSystems, entry?.sourceSystem || '');
        renderSelectOptions('sch-target-system', state.scheduleOptions.targetSystems, entry?.targetSystem || '');
        renderSelectOptions('sch-direction', state.scheduleOptions.directions, entry?.direction || '');
        document.getElementById('sch-source-type').value = entry?.sourceType || '';
        document.getElementById('sch-target-type').value = entry?.targetType || '';
        applyOperationOptions(entry?.operation || '');
        document.getElementById('sch-batch-size').value = entry?.batchSize || 100;
        document.getElementById('sch-next-run').value = isoToLocalDateTimeInput(entry?.nextRunAt);
        document.getElementById('sch-last-run').value = isoToLocalDateTimeInput(entry?.lastRunAt);
        document.getElementById('sch-active').checked = entry ? !!entry.active : true;
        const parsedSourceDefinition = parseScheduleSourceDefinition(entry?.sourceType || '', entry?.sourceDefinition || '');
        document.getElementById('sch-source-definition').value = parsedSourceDefinition.queryText || '';
        document.getElementById('sch-source-relative-directory').value = parsedSourceDefinition.relativeDirectory || '';
        document.getElementById('sch-source-archive-relative-directory').value = parsedSourceDefinition.archiveRelativeDirectory || '';
        document.getElementById('sch-source-delta-strategy').value = parsedSourceDefinition.deltaStrategy || '';
        document.getElementById('sch-source-delta-field').value = parsedSourceDefinition.deltaField || '';
        document.getElementById('sch-source-after-export').value = parsedSourceDefinition.afterExportText || '';
        document.getElementById('sch-source-delta-current').value = String(entry?.currentDeltaCheckpoint || '');
        document.getElementById('sch-source-delta-record-id').value = String(entry?.currentDeltaRecordId || '');
        const parsedTargetDefinition = parseScheduleTargetDefinition(entry?.targetType || '', entry?.targetDefinition || '');
        document.getElementById('sch-target-definition').value = parsedTargetDefinition.editorText || '';
        document.getElementById('sch-pricebook2id').value = '';
        document.getElementById('sch-target-relative-directory').value = parsedTargetDefinition.relativeDirectory || '';
        document.getElementById('sch-target-archive-relative-directory').value = parsedTargetDefinition.archiveRelativeDirectory || '';
        state.rawMappingEditorDirty = false;
        state.mappingFieldsLoadSeq = Number(state.mappingFieldsLoadSeq || 0) + 1;
        state.targetObjectsLoadSeq = Number(state.targetObjectsLoadSeq || 0) + 1;
        state.targetFieldsLoadSeq = Number(state.targetFieldsLoadSeq || 0) + 1;
        state.mappingFields = [];
        state.sourcePreviewRows = [];
        state.targetFields = [];
        state.hasIncompatibleScheduleMappings = false;
        state.schedulerLookupObjects = [];
        state.schedulerLookupObjectsLoaded = false;
        state.schedulerLookupObjectsLoadPromise = null;
        state.schedulerLookupExternalIdFieldsByObject = {};
        state.schedulerLookupExternalIdFieldPromises = {};
        const sourceFieldsBody = document.getElementById('sch-mapping-source-fields');
        if (sourceFieldsBody) {
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Quellfelder werden geladen.</td></tr>';
        }
        document.getElementById('sch-mapping').value = entry?.mappingDefinition || '';
        hydrateMappingRulesFromDefinition();
        await syncSchedulerExternalIdUi();
        state.customObjectFieldOverrides = {};
        setCreateObjectStatus('Bereit.', 'neutral');
        document.getElementById('sch-timing-start').value = new Date().toISOString().slice(0, 10);
        document.getElementById('sch-timing-time').value = '09:00';
        document.getElementById('sch-timing-interval').value = '2';
        
        // Load timing definition if available
        let timingData = { days: [], intervalMinutes: 2, startTime: '09:00' };
        if (entry?.timingDefinition) {
          try {
            timingData = JSON.parse(entry.timingDefinition);
          } catch (e) {
            console.warn('Failed to parse timing definition:', e);
          }
        }
        
        // Restore weekday checkboxes
        document.querySelectorAll('#sch-weekdays input').forEach((input) => {
          const dayValue = Number(input.value);
          input.checked = timingData.days && timingData.days.includes(dayValue);
        });
        
        // Restore timing values
        if (timingData.startTime) {
          document.getElementById('sch-timing-time').value = timingData.startTime;
        }
        if (timingData.intervalMinutes) {
          document.getElementById('sch-timing-interval').value = String(timingData.intervalMinutes);
        }
        updateWeekdayChips();
        document.getElementById('sch-timing-preview').textContent = entry?.nextRunAt
          ? 'Aktueller nächster Lauf: ' + new Date(entry.nextRunAt).toLocaleString('de-DE')
          : 'Noch keine Zeitsteuerung berechnet.';
        updateTimingInheritanceUi();
        document.getElementById('sch-source-test-status').textContent = 'Es werden bis zu 10 Datensätze angezeigt.';
        renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', []);
        clearModalError();
        updateSourceQueryAssist();
        updateScheduleFilePathSummaries();
        await renderScheduleRecentLogs(entry?.id || '');
        await loadEntityHistory('schedule', entry?.id || '', 'sch-history-list', 'sch-history-meta', 'Scheduler noch nicht gespeichert.');
        await loadScheduleCheckpoint(entry?.id || '');
        setupMappingDropZone();
        loadTransformFunctions();
        await loadTargetObjects(entry?.objectName || '');
        toggleCreateObjectFromSourceUi();
        await loadTargetFields();
        renderSchedulerMappingAssistant();
        await syncSchedulerExternalIdUi();
        state.scheduleWizardStep = 1;
        renderScheduleWizardStep();
        // Load mapping fields from backend metadata API
        // Use setTimeout to ensure all DOM values (source-type, connector) are applied before fetching
        setTimeout(() => loadMappingFields(), 0);
        scheduleModal.show();
      }

      async function createSalesforceCustomObjectFromSource() {
        clearModalError();
        setCreateObjectStatus('Objekt wird erstellt ...', 'warning');

        try {

        if (!isSalesforceTargetSelection()) {
          showModalError('Bitte Target System = Salesforce und Target Type = SALESFORCE wählen.');
          setCreateObjectStatus('Abbruch: Salesforce Ziel nicht aktiv.', 'error');
          return;
        }

        if (!Array.isArray(state.mappingFields) || state.mappingFields.length === 0) {
          await loadMappingFields();
        }

        if (!Array.isArray(state.mappingFields) || state.mappingFields.length === 0) {
          showModalError('Es konnten keine Quellfelder geladen werden.');
          setCreateObjectStatus('Abbruch: keine Quellfelder verfügbar.', 'error');
          return;
        }

        const objectApiNameInput = document.getElementById('sch-new-custom-object');
        const objectLabelInput = document.getElementById('sch-new-custom-object-label');
        const objectApiName = String(objectApiNameInput?.value || '').trim();
        const label = String(objectLabelInput?.value || '').trim();

        if (!objectApiName) {
          showModalError('Bitte einen Objekt API Namen angeben, z. B. SourceExchangeRate__c.');
          setCreateObjectStatus('Abbruch: Objekt API Name fehlt.', 'error');
          return;
        }

        const fieldOverrides = Object.entries(state.customObjectFieldOverrides || {}).map(([sourceName, type]) => ({
          sourceName,
          type: String(type || '').trim()
        })).filter((item) => item.sourceName && item.type);

        const result = await requestJson('/api/setup/create-custom-object-from-source', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            objectApiName,
            label: label || undefined,
            sourceFields: state.mappingFields,
            fieldOverrides
          })
        });

        await loadTargetObjects(result.objectApiName || objectApiName);
        const objectSelect = document.getElementById('sch-object');
        if (objectSelect) {
          objectSelect.value = result.objectApiName || objectApiName;
        }
        await loadTargetFields();
        await syncSchedulerExternalIdUi(result.objectApiName || objectApiName);
        ensureSalesforceTargetDefinition();
        setCreateObjectStatus(
          'Fertig: ' + (result.objectApiName || objectApiName) + ' (' + (result.fieldsCreated || 0) + ' Felder) und Tab bereit.',
          'success'
        );
        } catch (error) {
          const message = error?.message || 'Objekt konnte nicht erstellt werden';
          showModalError(message);
          setCreateObjectStatus('Fehler: ' + message, 'error');
        }
      }

      function updateConnectorConfigUi() {
        const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
        const fileWrap = document.getElementById('con-file-settings-wrap');
        const mssqlWrap = document.getElementById('con-mssql-settings-wrap');
        const restWrap = document.getElementById('con-rest-settings-wrap');
        const binaryWrap = document.getElementById('con-binary-settings-wrap');
        const hint = document.getElementById('con-wizard-hint');
        const sqlTitle = document.getElementById('con-sql-settings-title');
        const sqlText = document.getElementById('con-sql-settings-text');
        if (!fileWrap || !mssqlWrap || !restWrap || !binaryWrap) {
          return;
        }

        if (connectorType === 'FILE') {
          fileWrap.classList.remove('d-none');
        } else {
          fileWrap.classList.add('d-none');
        }

        if (isSqlConnectorType(connectorType)) {
          mssqlWrap.classList.remove('d-none');
        } else {
          mssqlWrap.classList.add('d-none');
        }

        if (isRestConnectorType(connectorType)) {
          restWrap.classList.remove('d-none');
        } else {
          restWrap.classList.add('d-none');
        }

        if (isBinaryImportConnectorType(connectorType)) {
          binaryWrap.classList.remove('d-none');
        } else {
          binaryWrap.classList.add('d-none');
        }

        if (sqlTitle) {
          sqlTitle.textContent = connectorType === 'POSTGRESQL'
            ? 'PostgreSQL Verbindung'
            : connectorType === 'MYSQL'
              ? 'MySQL Verbindung'
              : 'MSSQL Verbindung';
        }
        if (sqlText) {
          sqlText.textContent = connectorType === 'POSTGRESQL'
            ? 'Pflicht: Host, Datenbank und Benutzer. Standard-Port ist 5432.'
            : connectorType === 'MYSQL'
              ? 'Pflicht: Host, Datenbank und Benutzer. Standard-Port ist 3306.'
              : 'Pflicht: Server, Datenbank und Benutzer. Passwort kann direkt eingegeben werden. Alternativ kann das Passwort über Secret Key (ENV) aus einer Umgebungsvariable gelesen werden.';
        }

        if (hint) {
          const labels = {
            MSSQL: 'SQL-Parameter für MSSQL ausfüllen.',
            POSTGRESQL: 'SQL-Parameter für PostgreSQL ausfüllen.',
            MYSQL: 'SQL-Parameter für MySQL ausfüllen.',
            FILE: 'Datei-Einstellungen inkl. Format auswählen.',
            REST_API: 'REST Endpunkt + gewünschte Authentifizierung erfassen.',
            FILE_BINARY_SF_IMPORT: 'Binary Import-Pfade + Salesforce Zielfelder setzen.',
            CUSTOM: 'Benutzerdefiniert: Parameter im JSON Bereich pflegen.'
          };
          hint.textContent = labels[connectorType] || labels.CUSTOM;
        }

        updateRestAuthUi();
      }

      function updateRestAuthUi() {
        const authType = String(document.getElementById('con-rest-auth-type')?.value || 'none').trim().toLowerCase();
        const toggle = (id, visible) => {
          const element = document.getElementById(id);
          if (element) {
            element.classList.toggle('d-none', !visible);
          }
        };

        toggle('con-rest-basic-user-wrap', authType === 'basic');
        toggle('con-rest-basic-password-wrap', authType === 'basic');
        toggle('con-rest-bearer-token-wrap', authType === 'bearer');
        toggle('con-rest-api-key-name-wrap', authType === 'api_key');
        toggle('con-rest-api-key-value-wrap', authType === 'api_key');
        toggle('con-rest-api-key-location-wrap', authType === 'api_key');
        toggle('con-rest-token-url-wrap', authType === 'oauth2');
        toggle('con-rest-grant-type-wrap', authType === 'oauth2');
        toggle('con-rest-client-id-wrap', authType === 'oauth2');
        toggle('con-rest-client-secret-wrap', authType === 'oauth2');
        toggle('con-rest-scope-wrap', authType === 'oauth2');
      }

      function applyConnectorWizardSelection(preserveValues) {
        const wizardType = String(document.getElementById('con-wizard-type')?.value || 'MSSQL').trim().toUpperCase();
        const connectorTypeInput = document.getElementById('con-type');
        if (!connectorTypeInput) {
          return;
        }

        const finalType = wizardType === 'CUSTOM'
          ? String(connectorTypeInput.value || '').trim() || 'CUSTOM'
          : wizardType;
        connectorTypeInput.value = finalType;

        if (!preserveValues) {
          if (wizardType === 'FILE_BINARY_SF_IMPORT') {
            if (!document.getElementById('con-target-system').value) {
              document.getElementById('con-target-system').value = 'Salesforce';
            }
            if (!document.getElementById('con-direction').value) {
              document.getElementById('con-direction').value = 'Inbound';
            }
          }
          if (wizardType === 'REST_API' && !document.getElementById('con-direction').value) {
            document.getElementById('con-direction').value = 'Outbound';
          }
        }

        updateConnectorConfigUi();
      }

      function fillMssqlConnectorSettingsFromParameters(parameters) {
        const params = parameters || {};
        document.getElementById('con-mssql-server').value = String(params.server || '');
        document.getElementById('con-mssql-port').value = params.port === undefined || params.port === null || params.port === '' ? '' : String(params.port);
        document.getElementById('con-mssql-database').value = String(params.database || '');
        document.getElementById('con-mssql-user').value = String(params.user || '');
        document.getElementById('con-mssql-password').value = '';
        document.getElementById('con-mssql-encrypt').checked = params.encrypt === undefined ? (params.ssl === undefined ? true : !!params.ssl) : !!params.encrypt;
        document.getElementById('con-mssql-trust-server-certificate').checked = params.trustServerCertificate === undefined ? false : !!params.trustServerCertificate;
      }

      function mergeMssqlConnectorSettingsIntoParameters(parameters) {
        const merged = { ...(parameters || {}) };
        const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
        const server = String(document.getElementById('con-mssql-server').value || '').trim();
        const database = String(document.getElementById('con-mssql-database').value || '').trim();
        const user = String(document.getElementById('con-mssql-user').value || '').trim();
        const password = String(document.getElementById('con-mssql-password').value || '').trim();
        const portRaw = String(document.getElementById('con-mssql-port').value || '').trim();

        if (server) {
          merged.server = server;
        }
        if (database) {
          merged.database = database;
        }
        if (user) {
          merged.user = user;
        }
        if (password) {
          merged.password = password;
        }
        if (portRaw) {
          const parsedPort = Number(portRaw);
          if (!Number.isNaN(parsedPort)) {
            merged.port = parsedPort;
          }
        } else if (connectorType === 'POSTGRESQL') {
          merged.port = 5432;
        } else if (connectorType === 'MYSQL') {
          merged.port = 3306;
        }

        if (connectorType === 'MSSQL') {
          merged.encrypt = !!document.getElementById('con-mssql-encrypt').checked;
          merged.trustServerCertificate = !!document.getElementById('con-mssql-trust-server-certificate').checked;
        } else {
          merged.ssl = !!document.getElementById('con-mssql-encrypt').checked;
        }

        return merged;
      }

      function fillFileConnectorSettingsFromParameters(parameters) {
        const params = parameters || {};
        document.getElementById('con-file-kind').value = String(params.fileKind || params.format || 'CSV').toUpperCase();
        document.getElementById('con-file-base-path').value = String(params.basePath || params.fileBasePath || 'artifacts/files');
        document.getElementById('con-file-import-path').value = String(params.importPath || 'inbound');
        document.getElementById('con-file-export-path').value = String(params.exportPath || 'outbound');
        document.getElementById('con-file-archive-path').value = String(params.archivePath || 'archive');
        document.getElementById('con-file-charset').value = String(params.defaultCharset || 'utf8');
        document.getElementById('con-file-delimiter').value = String(params.defaultDelimiter || ';');
        document.getElementById('con-file-archive-read').checked = params.archiveOnRead === undefined ? true : !!params.archiveOnRead;
        document.getElementById('con-file-archive-write').checked = !!params.archiveOnWrite;
      }

      function mergeFileConnectorSettingsIntoParameters(parameters) {
        const merged = { ...(parameters || {}) };
        merged.fileKind = String(document.getElementById('con-file-kind').value || 'CSV').toUpperCase();
        merged.basePath = document.getElementById('con-file-base-path').value || 'artifacts/files';
        merged.importPath = document.getElementById('con-file-import-path').value || 'inbound';
        merged.exportPath = document.getElementById('con-file-export-path').value || 'outbound';
        merged.archivePath = document.getElementById('con-file-archive-path').value || 'archive';
        merged.defaultCharset = document.getElementById('con-file-charset').value || 'utf8';
        merged.defaultDelimiter = document.getElementById('con-file-delimiter').value || ';';
        merged.archiveOnRead = document.getElementById('con-file-archive-read').checked;
        merged.archiveOnWrite = document.getElementById('con-file-archive-write').checked;
        return merged;
      }

      function fillRestConnectorSettingsFromParameters(parameters) {
        const params = parameters || {};
        document.getElementById('con-rest-base-url').value = String(params.baseUrl || '');
        document.getElementById('con-rest-resource-path').value = String(params.resourcePath || params.path || '');
        document.getElementById('con-rest-auth-type').value = String(params.authType || 'none').toLowerCase();
        document.getElementById('con-rest-token-url').value = String(params.tokenUrl || '');
        document.getElementById('con-rest-grant-type').value = String(params.grantType || 'client_credentials');
        document.getElementById('con-rest-method').value = String(params.method || 'GET').toUpperCase();
        document.getElementById('con-rest-basic-user').value = String(params.username || '');
        document.getElementById('con-rest-basic-password').value = '';
        document.getElementById('con-rest-bearer-token').value = '';
        document.getElementById('con-rest-api-key-name').value = String(params.apiKeyName || '');
        document.getElementById('con-rest-api-key-value').value = '';
        document.getElementById('con-rest-api-key-location').value = String(params.apiKeyLocation || 'header').toLowerCase();
        document.getElementById('con-rest-client-id').value = String(params.clientId || '');
        document.getElementById('con-rest-client-secret').value = '';
        document.getElementById('con-rest-scope').value = String(params.scope || '');
        document.getElementById('con-rest-audience').value = String(params.audience || '');
        document.getElementById('con-rest-extra-headers').value = params.extraHeaders ? JSON.stringify(params.extraHeaders) : '';
        updateRestAuthUi();
      }

      function mergeRestConnectorSettingsIntoParameters(parameters) {
        const merged = { ...(parameters || {}) };
        merged.baseUrl = String(document.getElementById('con-rest-base-url').value || '').trim();
        merged.resourcePath = String(document.getElementById('con-rest-resource-path').value || '').trim();
        merged.authType = String(document.getElementById('con-rest-auth-type').value || 'none').trim().toLowerCase();
        merged.method = String(document.getElementById('con-rest-method').value || 'GET').trim().toUpperCase();
        if (merged.authType === 'oauth2') {
          merged.tokenUrl = String(document.getElementById('con-rest-token-url').value || '').trim();
          merged.grantType = String(document.getElementById('con-rest-grant-type').value || 'client_credentials').trim();
          merged.clientId = String(document.getElementById('con-rest-client-id').value || '').trim();
          const clientSecret = String(document.getElementById('con-rest-client-secret').value || '').trim();
          if (clientSecret) {
            merged.clientSecret = clientSecret;
          }
          merged.scope = String(document.getElementById('con-rest-scope').value || '').trim();
        } else {
          delete merged.tokenUrl;
          delete merged.grantType;
          delete merged.clientId;
          delete merged.clientSecret;
          delete merged.scope;
        }
        if (merged.authType === 'basic') {
          merged.username = String(document.getElementById('con-rest-basic-user').value || '').trim();
          const password = String(document.getElementById('con-rest-basic-password').value || '').trim();
          if (password) {
            merged.password = password;
          }
        } else {
          delete merged.username;
          delete merged.password;
        }
        if (merged.authType === 'bearer') {
          const bearerToken = String(document.getElementById('con-rest-bearer-token').value || '').trim();
          if (bearerToken) {
            merged.bearerToken = bearerToken;
          }
        } else {
          delete merged.bearerToken;
        }
        if (merged.authType === 'api_key') {
          merged.apiKeyName = String(document.getElementById('con-rest-api-key-name').value || '').trim();
          merged.apiKeyLocation = String(document.getElementById('con-rest-api-key-location').value || 'header').trim().toLowerCase();
          const apiKeyValue = String(document.getElementById('con-rest-api-key-value').value || '').trim();
          if (apiKeyValue) {
            merged.apiKeyValue = apiKeyValue;
          }
        } else {
          delete merged.apiKeyName;
          delete merged.apiKeyLocation;
          delete merged.apiKeyValue;
        }
        merged.audience = String(document.getElementById('con-rest-audience').value || '').trim();
        const rawHeaders = String(document.getElementById('con-rest-extra-headers').value || '').trim();
        if (rawHeaders) {
          try {
            merged.extraHeaders = JSON.parse(rawHeaders);
          } catch {
            throw new Error('Zusätzliche Header müssen gültiges JSON sein');
          }
        } else {
          delete merged.extraHeaders;
        }
        return merged;
      }

      function fillBinaryImportConnectorSettingsFromParameters(parameters) {
        const params = parameters || {};
        document.getElementById('con-binary-base-path').value = String(params.basePath || 'artifacts/files');
        document.getElementById('con-binary-import-path').value = String(params.importPath || 'binary-inbound');
        document.getElementById('con-binary-archive-path').value = String(params.archivePath || 'archive');
        document.getElementById('con-binary-extensions').value = String(params.allowedExtensions || 'pdf,jpg,png,zip');
        document.getElementById('con-binary-sf-object').value = String(params.salesforceObject || 'ContentVersion');
        document.getElementById('con-binary-sf-binary-field').value = String(params.binaryField || 'VersionData');
        document.getElementById('con-binary-sf-filename-field').value = String(params.fileNameField || 'PathOnClient');
        document.getElementById('con-binary-title-prefix').value = String(params.titlePrefix || '');
      }

      function mergeBinaryImportConnectorSettingsIntoParameters(parameters) {
        const merged = { ...(parameters || {}) };
        merged.basePath = String(document.getElementById('con-binary-base-path').value || 'artifacts/files').trim();
        merged.importPath = String(document.getElementById('con-binary-import-path').value || 'binary-inbound').trim();
        merged.archivePath = String(document.getElementById('con-binary-archive-path').value || 'archive').trim();
        merged.allowedExtensions = String(document.getElementById('con-binary-extensions').value || 'pdf,jpg,png,zip').trim();
        merged.salesforceObject = String(document.getElementById('con-binary-sf-object').value || 'ContentVersion').trim();
        merged.binaryField = String(document.getElementById('con-binary-sf-binary-field').value || 'VersionData').trim();
        merged.fileNameField = String(document.getElementById('con-binary-sf-filename-field').value || 'PathOnClient').trim();
        merged.titlePrefix = String(document.getElementById('con-binary-title-prefix').value || '').trim();
        return merged;
      }

      function openConnectorModal(connectorId, templateDraft) {
        const entry = connectorId
          ? state.connectors.find((item) => item.id === connectorId)
          : (templateDraft || null);
        clearConnectorModalError();
        document.getElementById('con-id').value = entry?.id || '';
        document.getElementById('con-name').value = entry?.name || '';
        document.getElementById('con-type').value = entry?.connectorType || 'MSSQL';
        document.getElementById('con-wizard-type').value = getConnectorWizardTypeFromConnectorType(entry?.connectorType || 'MSSQL');
        document.getElementById('con-target-system').value = entry?.targetSystem || '';
        document.getElementById('con-direction').value = entry?.direction || '';
        document.getElementById('con-secret').value = entry?.secretKey || '';
        document.getElementById('con-timeout').value = entry?.timeoutMs || '';
        document.getElementById('con-retries').value = entry?.maxRetries || '';
        document.getElementById('con-description').value = entry?.description || '';
        const parameters = entry?.parameters || {};
        document.getElementById('con-parameters').value = JSON.stringify(parameters, null, 2);
        fillConnectorNotificationSettingsFromParameters(parameters);
        void loadConnectorTaskOwnerOptions(parameters.notificationTaskOwnerId);
        fillMssqlConnectorSettingsFromParameters(parameters);
        fillFileConnectorSettingsFromParameters(parameters);
        fillRestConnectorSettingsFromParameters(parameters);
        fillBinaryImportConnectorSettingsFromParameters(parameters);
        applyConnectorWizardSelection(!!entry);
        updateConnectorConfigUi();
        document.getElementById('con-active').checked = entry ? !!entry.active : true;
        state.connectorWizardStep = 1;
        renderConnectorWizardStep();
        void loadEntityHistory('connector', entry?.id || '', 'con-history-list', 'con-history-meta', 'Connector noch nicht gespeichert.');
        connectorModal.show();
      }

      async function saveSchedule() {
        clearError();
        clearModalError();
        const saveButton = document.getElementById('save-schedule');
        saveButton.disabled = true;

        try {
          await ensureMssqlTargetObjectSelection();
          for (let step = 1; step < getScheduleWizardTotalSteps(); step += 1) {
            validateScheduleWizardStep(step);
          }
          ensureSalesforceTargetDefinition();
          const targetConstraintMessage = updateSchedulerExternalIdValidationState();
          if (targetConstraintMessage) {
            throw new Error(targetConstraintMessage);
          }
          const requiredFieldMessage = getRequiredSalesforceFieldSaveMessage();
          if (requiredFieldMessage) {
            throw new Error(requiredFieldMessage);
          }

          const payload = collectScheduleFormPayload();
          const scheduleId = payload.id;

          // Only include name for new schedules (Name is an auto-number field and cannot be updated)
          if (!scheduleId) {
            payload.name = document.getElementById('sch-name').value;
          } else {
            delete payload.name;
          }

          const validationResult = await validateCurrentScheduleConfiguration();
          const validationError = Array.isArray(validationResult?.issues)
            ? validationResult.issues.find((issue) => issue.severity === 'error')
            : null;
          if (validationError) {
            throw new Error(validationError.message || 'Scheduler-Konfiguration ist ungueltig.');
          }

          const result = await requestJson('/api/schedules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });

          scheduleModal.hide();
          await saveScheduleCheckpoint(result?.id || scheduleId);
          await refresh();
        } catch (error) {
          showModalError(error.message || 'Scheduler konnte nicht gespeichert werden');
        } finally {
          saveButton.disabled = false;
        }
      }

      async function testScheduleSource() {
        clearError();
        clearModalError();
        const testButton = document.getElementById('sch-test-source');
        const sourceType = document.getElementById('sch-source-type').value;
        const sourceDefinition = buildScheduleSourceDefinitionValue() || '';
        const connectorId = document.getElementById('sch-connector').value || undefined;
        const status = document.getElementById('sch-source-test-status');

        testButton.disabled = true;
        status.textContent = 'Quelle wird getestet...';

        try {
          const result = await requestJson('/api/sources/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              sourceType,
              sourceDefinition,
              connectorId,
              limit: 10
            })
          });

          renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', result.rows || []);
          status.textContent = (result.rowCount || 0) + ' Datensätze geladen.';
        } catch (error) {
          renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', []);
          status.textContent = 'Quelltest fehlgeschlagen.';
          showModalError(error.message || 'Quelle konnte nicht getestet werden');
        } finally {
          testButton.disabled = false;
        }
      }

      function renderEntityHistory(containerId, metaId, items, emptyText) {
        const container = document.getElementById(containerId);
        const meta = document.getElementById(metaId);
        if (!container) return;
        const historyItems = Array.isArray(items) ? items : [];
        if (meta) {
          meta.textContent = historyItems.length
            ? historyItems.length + ' Historieneinträge geladen.'
            : 'Keine Historieneinträge gefunden.';
        }
        if (!historyItems.length) {
          container.innerHTML = '<div class="text-secondary">' + esc(emptyText || 'Keine Änderungshistorie vorhanden.') + '</div>';
          return;
        }
        container.innerHTML =
          '<div class="table-responsive"><table class="table table-sm mb-0">' +
            '<thead><tr><th>Zeit</th><th>Benutzer</th><th>Aktion</th><th>Status</th><th>Hinweis</th></tr></thead><tbody>' +
              historyItems.map((item) => {
                const actor = item.actor && item.actor.username ? item.actor.username : '-';
                return '<tr>' +
                  '<td>' + esc(formatDate(item.at, 'short')) + '</td>' +
                  '<td>' + esc(actor) + '</td>' +
                  '<td>' + esc(item.action || '-') + '</td>' +
                  '<td>' + esc(item.status || '-') + '</td>' +
                  '<td>' + esc(item.message || item.entityName || '-') + '</td>' +
                '</tr>';
              }).join('') +
            '</tbody></table></div>';
      }

      async function loadEntityHistory(entityType, entityId, containerId, metaId, emptyText) {
        const normalizedId = String(entityId || '').trim();
        if (!normalizedId) {
          renderEntityHistory(containerId, metaId, [], emptyText || 'Noch nicht gespeichert.');
          return;
        }
        const result = await safeRequest(
          '/api/admin/audit-history?limit=50&entityType=' + encodeURIComponent(entityType) + '&entityId=' + encodeURIComponent(normalizedId),
          { items: [] }
        );
        renderEntityHistory(containerId, metaId, result.items || [], emptyText);
      }

      function renderScheduleValidationResult(result) {
        const issues = Array.isArray(result?.issues) ? result.issues : [];
        if (!issues.length) {
          clearModalError();
          const status = document.getElementById('sch-source-test-status');
          if (status) status.textContent = 'Konfiguration geprüft: keine strukturellen Probleme gefunden.';
          return;
        }
        const errors = issues.filter((issue) => issue.severity === 'error');
        const warnings = issues.filter((issue) => issue.severity !== 'error');
        const message = issues.map((issue) =>
          '[' + String(issue.severity || 'warning').toUpperCase() + '] ' + String(issue.area || 'general') + ': ' + String(issue.message || '')
        ).join('\\n');
        if (errors.length) {
          showModalError('Konfigurationsprüfung fehlgeschlagen:\\n' + message);
        } else {
          showModalError('Konfigurationsprüfung mit Warnungen:\\n' + message);
        }
        const status = document.getElementById('sch-source-test-status');
        if (status) {
          status.textContent = errors.length
            ? errors.length + ' Fehler, ' + warnings.length + ' Warnungen gefunden.'
            : warnings.length + ' Warnungen gefunden.';
        }
      }

      async function validateCurrentScheduleConfiguration() {
        clearModalError();
        const payload = collectScheduleFormPayload();
        if (!payload.name && !payload.id) {
          payload.name = String(document.getElementById('sch-name')?.value || '').trim() || 'Neuer Scheduler';
        }
        const result = await requestJson('/api/schedules/validate-config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        renderScheduleValidationResult(result);
        return result;
      }

      function collectConnectorFormPayload() {
        const preview = collectConnectorParametersPreview();
        return {
          id: document.getElementById('con-id').value || undefined,
          name: document.getElementById('con-name').value,
          active: document.getElementById('con-active').checked,
          connectorType: preview.connectorType || document.getElementById('con-type').value,
          targetSystem: document.getElementById('con-target-system').value || undefined,
          direction: document.getElementById('con-direction').value || undefined,
          secretKey: document.getElementById('con-secret').value || undefined,
          timeoutMs: Number(document.getElementById('con-timeout').value || 0) || undefined,
          maxRetries: Number(document.getElementById('con-retries').value || 0) || undefined,
          description: document.getElementById('con-description').value || undefined,
          parameters: preview.parameters
        };
      }

      async function persistConnector(options = {}) {
        const validateAfterSave = options.validateAfterSave === true;
        const payload = collectConnectorFormPayload();

        const saved = await requestJson('/api/connectors', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        document.getElementById('con-id').value = saved.id || payload.id || '';

        if (validateAfterSave && saved.id) {
          const result = await requestJson('/api/connectors/' + encodeURIComponent(saved.id) + '/test', { method: 'POST' });
          alert(result.message || (result.ok ? 'OK' : 'Fehler'));
        }

        await refresh();
        return saved;
      }

      async function saveConnector() {
        try {
          validateConnectorWizardStep(1);
          validateConnectorWizardStep(2);
          validateConnectorWizardStep(3);
          await persistConnector({ validateAfterSave: false });
          connectorModal.hide();
        } catch (error) {
          showConnectorModalError(error?.message || 'Connector konnte nicht gespeichert werden.');
        }
      }

      async function saveAndValidateConnector() {
        try {
          validateConnectorWizardStep(1);
          validateConnectorWizardStep(2);
          validateConnectorWizardStep(3);
          await persistConnector({ validateAfterSave: true });
          connectorModal.hide();
        } catch (error) {
          showConnectorModalError(error?.message || 'Connector konnte nicht validiert werden.');
        }
      }

      async function loadLogs() {
        const runId = document.getElementById('log-run-select').value;
        if (!runId) {
          return;
        }

        const logs = await safeRequest('/api/runs/' + encodeURIComponent(runId) + '/logs', { items: [] });
        const lines = (logs.items || []).map((entry) => '[' + formatDate(entry.createdAt, 'short') + '] [' + (entry.level || '-') + '] ' + (entry.step || '-') + ' | ' + (entry.message || ''));
        document.getElementById('logs-output').textContent = lines.join('\\n') || 'Keine Logs gefunden.';
      }

      async function renderScheduleRecentLogs(scheduleId) {
        const outputEl = document.getElementById('sch-recent-logs-output');
        const metaEl = document.getElementById('sch-recent-logs-meta');
        if (!outputEl || !metaEl) {
          return;
        }

        const normalizedScheduleId = String(scheduleId || '').trim();
        if (!normalizedScheduleId) {
          metaEl.textContent = 'Scheduler noch nicht gespeichert.';
          outputEl.textContent = 'Logs stehen nach dem ersten Lauf zur Verfügung.';
          return;
        }

        const latestRun = (state.runs || [])
          .filter((run) => String(run.scheduleId || '').trim() === normalizedScheduleId)
          .sort((left, right) => {
            const leftTime = new Date(left.startedAt || left.finishedAt || 0).getTime();
            const rightTime = new Date(right.startedAt || right.finishedAt || 0).getTime();
            return rightTime - leftTime;
          })[0];

        if (!latestRun) {
          metaEl.textContent = 'Noch kein Run für diesen Scheduler gefunden.';
          outputEl.textContent = 'Keine Logs vorhanden.';
          return;
        }

        metaEl.textContent = [
          'Letzter Run: ' + formatDate(latestRun.startedAt || latestRun.finishedAt, 'short'),
          'Status: ' + String(latestRun.status || '-'),
          'Dauer: ' + formatDurationMinSec(getRunDurationMs(latestRun))
        ].join(' • ');

        outputEl.textContent = 'Logs werden geladen...';
        const logs = await safeRequest('/api/runs/' + encodeURIComponent(latestRun.id) + '/logs', { items: [] });
        const lines = (logs.items || []).slice(0, 30).map((entry) => {
          return '[' + formatDate(entry.createdAt, 'short') + '] [' + (entry.level || '-') + '] ' + (entry.step || '-') + ' | ' + (entry.message || '');
        });
        outputEl.textContent = lines.join('\\n') || 'Keine Logs für den letzten Run gefunden.';
      }

      async function previewSql() {
        const connectorId = document.getElementById('sql-connector-select').value;
        const query = document.getElementById('sql-query').value;
        const result = await requestJson('/api/queries/preview', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ connectorId, query, limit: 10 })
        });

        if (result.rows) {
          document.getElementById('mapping-source').value = JSON.stringify(result.rows.slice(0, 5), null, 2);
        }
        document.getElementById('mapping-output').textContent = JSON.stringify(result, null, 2);
      }

      async function previewMapping() {
        const mappingDefinition = document.getElementById('mapping-definition').value;
        const sourceData = JSON.parse(document.getElementById('mapping-source').value || '[]');
        const result = await requestJson('/api/mappings/preview', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mappingDefinition, sourceData })
        });
        document.getElementById('mapping-output').textContent = JSON.stringify(result, null, 2);
      }

      async function loadInstances() {
        const select = document.getElementById('instance-select');
        const response = await safeRequest('/api/instances', { items: [] });
        const items = response.items || [];
        const defaultInstance = items.find((item) => item.isDefault);

        if (!items.length) {
          select.innerHTML = '<option value="">Keine Instanzen konfiguriert</option>';
          state.instanceId = '';
          return;
        }

        select.innerHTML = items.map((item) => {
          const label = item.isDefault ? (String(item.name || item.id) + ' (Default aus .env)') : String(item.name || item.id);
          return '<option value="' + esc(item.id) + '">' + esc(label) + '</option>';
        }).join('');

        const hasCurrent = items.some((item) => item.id === state.instanceId);
        if (defaultInstance) {
          state.instanceId = String(defaultInstance.id || '').trim();
        } else if (!state.instanceId || !hasCurrent) {
          state.instanceId = items[0].id;
        }
        select.value = state.instanceId;
      }

      async function saveInstance() {
        clearError();
        try {
          const payload = {
            id: document.getElementById('ins-id').value,
            name: document.getElementById('ins-name').value || undefined,
            loginUrl: document.getElementById('ins-login-url').value,
            clientId: document.getElementById('ins-client-id').value,
            clientSecret: document.getElementById('ins-client-secret').value,
            queryLimit: Number(document.getElementById('ins-query-limit').value || 0) || undefined
          };

          const result = await requestJson('/api/instances', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });

          instanceModal.hide();
          state.instanceId = result.id;
          await loadInstances();
          await refresh();
        } catch (error) {
          showError(error.message || 'Instanz konnte nicht gespeichert werden');
        }
      }

      async function refresh(options = {}) {
        const shouldRefreshChart = options.refreshChart !== false;
        clearError();

        await loadAdminData();
        const healthData = await safeRequest('/api/system/health', {});
        const installerSummary = await safeRequest('/api/installer/summary', null);
        state.installerSummary = installerSummary;
        renderInstallerSummary();
        applyInstallerScenarioDefaults();
        const schedules = await safeRequest('/api/schedules', { items: [] });
        const connectors = await safeRequest('/api/connectors', { items: [] });
        const runs = await safeRequest('/api/runs', { items: [] });
        const staleRuns = await safeRequest('/api/runs/stale', { items: [] });
        const migrations = currentUserHasModule('migration') ? await safeRequest('/api/migrations', { items: [] }) : { items: [] };
        const graph = await safeRequest('/api/graph', { nodes: [], edges: [] });
        const salesforceOverview = await safeRequest('/api/salesforce/overview', {});
        await loadScheduleOptions();

        state.schedules = schedules.items || [];
        state.connectors = connectors.items || [];
        state.runs = runs.items || [];
        state.staleRuns = staleRuns.items || [];
        state.migrations = migrations.items || [];
        state.graphData = graph;
        renderSalesforceOverview(salesforceOverview);

        renderOverview(healthData);
        renderInstallerSummary();
        renderSchedules();
        renderConnectors();
        renderRuns();
        renderStaleRuns();
        renderOverviewConnectorFilter();
        redrawOverviewGraph();
        await loadRecordsSummary();
        await loadOverviewUpdateStatus();
        if (shouldRefreshChart) {
          await loadLogSummary();
        }
      }

      function activateMainTab(tabTarget) {
        const trigger = document.querySelector('#main-tabs [data-bs-target="' + tabTarget + '"]');
        if (!trigger) {
          return;
        }

        try {
          if (window.bootstrap?.Tab) {
            window.bootstrap.Tab.getOrCreateInstance(trigger).show();
            return;
          }
        } catch {
          // fall back to a native click if Bootstrap Tab is not available
        }

        trigger.click();
        syncHeaderMenuTabState(tabTarget);
      }

      function syncHeaderMenuTabState(activeTabTarget) {
        const currentTabTarget = activeTabTarget || document.querySelector('#main-tabs .nav-link.active')?.getAttribute('data-bs-target') || '';
        document.querySelectorAll('[data-menu-tab]').forEach((button) => {
          button.classList.toggle('is-active', button.getAttribute('data-menu-tab') === currentTabTarget);
        });
      }

      function bindEventListenerOnce(elementId, eventName, handler) {
        const element = document.getElementById(elementId);
        if (!element) {
          return;
        }

        const marker = 'bound_' + eventName;
        if (element.dataset[marker] === '1') {
          return;
        }

        element.addEventListener(eventName, handler);
        element.dataset[marker] = '1';
      }

      function openAdminModal() {
        if (!currentUserHasPermission('admin')) {
          showError('Admin-Berechtigung fehlt');
          return;
        }
        const modalEl = document.getElementById('admin-modal');
        if (!modalEl || !window.bootstrap?.Modal) {
          return;
        }
        window.bootstrap.Modal.getOrCreateInstance(modalEl).show();
      }

      // Boot data loading before the large listener block so the UI still initializes
      // even if a later non-critical listener registration fails.
      (async () => {
        try {
          try {
            initializeUiTheme();
          } catch {
            // never block initial data load because of theme handling
          }
          restoreLogChartRange();
          restoreOverviewStatsRange();
          await loadInstances();
          await refresh();
          updateWeekdayChips();
          initializeTableFilters();
          setInterval(() => {
            void refresh({ refreshChart: false });
          }, 7000);
        } catch (error) {
          console.error('UI bootstrap failed', error);
          showError(error?.message || 'UI bootstrap failed');
        }
      })();

      bindEventListenerOnce('new-schedule', 'click', () => openScheduleModal(''));
      bindEventListenerOnce('open-admin-modal-sidebar', 'click', openAdminModal);
      bindEventListenerOnce('open-admin-modal-menu', 'click', openAdminModal);
      bindEventListenerOnce('admin-user-save', 'click', async () => {
        try {
          await saveAdminUserFromForm();
        } catch (error) {
          showError(error.message || 'Benutzer konnte nicht gespeichert werden');
        }
      });
      bindEventListenerOnce('admin-user-reset', 'click', resetAdminUserForm);
      bindEventListenerOnce('admin-users-refresh', 'click', loadAdminData);
      bindEventListenerOnce('admin-audit-refresh', 'click', loadAdminData);
      document.querySelectorAll('[data-menu-tab]').forEach((button) => {
        if (button.dataset.boundMenuTab === '1') {
          return;
        }

        button.dataset.boundMenuTab = '1';
        button.addEventListener('click', () => {
          const tabTarget = String(button.getAttribute('data-menu-tab') || '').trim();
          if (tabTarget) {
            activateMainTab(tabTarget);
          }
        });
      });
      document.querySelectorAll('#main-tabs .nav-link').forEach((button) => {
        if (button.dataset.boundMenuSync === '1') {
          return;
        }

        button.dataset.boundMenuSync = '1';
        button.addEventListener('click', () => {
          const tabTarget = String(button.getAttribute('data-bs-target') || '').trim();
          window.setTimeout(() => syncHeaderMenuTabState(tabTarget), 0);
        });
      });
      const headerMenu = document.getElementById('agent-header-menu');
      if (headerMenu && headerMenu.dataset.boundOffcanvasShown !== '1') {
        headerMenu.dataset.boundOffcanvasShown = '1';
        headerMenu.addEventListener('shown.bs.offcanvas', () => syncHeaderMenuTabState());
      }
      bindEventListenerOnce('new-schedule-from-template', 'click', async () => {
        try {
          await createFromTemplate('schedule');
        } catch (error) {
          showError(error.message || 'Scheduler-Vorlage konnte nicht geladen werden');
        }
      });
      bindEventListenerOnce('sch-target-system', 'change', async () => {
        applyOperationOptions('');
        await loadTargetObjects('');
        await loadTargetFields();
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-target-type', 'change', async () => {
        applyOperationOptions('');
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
        updateScheduleFilePathSummaries();
      });
      bindEventListenerOnce('sch-object', 'change', async () => {
        renderSchedulerMappingAssistant();
        await loadTargetFields();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-operation', 'change', async () => {
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-external-id-field', 'change', () => {
        ensureSalesforceTargetDefinition();
        updateSchedulerExternalIdValidationState();
      });
      bindEventListenerOnce('sch-pricebook2id', 'change', async () => {
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-target-definition', 'change', async () => {
        await syncSchedulerExternalIdUi();
        updateScheduleFilePathSummaries();
      });
      bindEventListenerOnce('sch-mapping', 'input', () => {
        state.rawMappingEditorDirty = true;
      });
      bindEventListenerOnce('sch-mapping', 'change', async () => {
        hydrateMappingRulesFromDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-connector', 'change', async () => {
        await loadTargetObjects(document.getElementById('sch-object').value || '');
        await loadTargetFields();
        await syncSchedulerExternalIdUi();
        updateScheduleFilePathSummaries();
        const srcType = document.getElementById('sch-source-type').value;
        if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
          loadMappingFields();
        }
      });

      document.getElementById('instance-select').addEventListener('change', async (event) => {
        state.instanceId = event.target.value;
        await refresh();
      });
      const themeSelect = document.getElementById('theme-select');
      if (themeSelect) {
        themeSelect.addEventListener('change', (event) => {
          const target = event && event.target ? event.target : null;
          applyUiTheme(String(target && 'value' in target ? target.value : 'corporate'));
        });
      }
      document.getElementById('add-instance').addEventListener('click', () => {
        document.getElementById('ins-id').value = '';
        document.getElementById('ins-name').value = '';
        document.getElementById('ins-login-url').value = 'https://login.salesforce.com';
        document.getElementById('ins-client-id').value = '';
        document.getElementById('ins-client-secret').value = '';
        document.getElementById('ins-query-limit').value = '';
        clearError();
        instanceModal.show();
      });
      document.getElementById('save-instance').addEventListener('click', saveInstance);
      document.getElementById('export-setup').addEventListener('click', async () => {
        try {
          clearError();
          await exportSetup();
        } catch (error) {
          showError(error.message || 'Setup konnte nicht exportiert werden');
        }
      });
      document.getElementById('import-setup').addEventListener('click', () => {
        const input = document.getElementById('setup-import-input');
        if (input) {
          input.value = '';
          input.click();
        }
      });
      document.getElementById('setup-import-input').addEventListener('change', async (event) => {
        const file = event.target?.files?.[0];
        if (!file) {
          return;
        }

        try {
          clearError();
          await importSetupFromFile(file);
        } catch (error) {
          showError(error.message || 'Setup konnte nicht importiert werden');
        }
      });
      document.getElementById('refresh-all').addEventListener('click', refresh);
      document.getElementById('installer-generate-files')?.addEventListener('click', generateInstallerFilesFromUi);
      document.getElementById('installer-scenario')?.addEventListener('change', () => {
        applyInstallerScenarioDefaults();
        renderInstallerSummary();
      });
      document.getElementById('logout-admin')?.addEventListener('click', async () => {
        try {
          await fetch('/auth/logout', { method: 'POST' });
        } finally {
          window.location.href = '/';
        }
      });
      document.getElementById('overview-check-update').addEventListener('click', async () => {
        await loadOverviewUpdateStatus(true, true);
      });
      document.getElementById('overview-run-update').addEventListener('click', async () => {
        try {
          await triggerOverviewUpdate();
        } catch (error) {
          showError(error.message || 'Update konnte nicht gestartet werden');
        }
      });
      document.getElementById('overview-connector-filter').addEventListener('change', (event) => {
        state.overviewConnectorFilterId = String(event.target?.value || '');
        redrawOverviewGraph();
      });
      const overviewRangeGroup = document.getElementById('overview-stats-range');
      if (overviewRangeGroup) {
        overviewRangeGroup.addEventListener('click', async (event) => {
          const trigger = event.target && event.target.closest ? event.target.closest('[data-range]') : null;
          if (!trigger) {
            return;
          }

          const range = String(trigger.getAttribute('data-range') || '').trim();
          if (!['day', 'month', 'year'].includes(range) || range === state.overviewStatsRange) {
            return;
          }

          state.overviewStatsRange = range;
          try {
            window.localStorage.setItem(OVERVIEW_STATS_RANGE_STORAGE_KEY, range);
          } catch {
            // Ignore storage errors in restricted browser contexts.
          }
          await refresh({ refreshChart: false });
        });
      }
      document.getElementById('log-chart-range').addEventListener('change', loadLogSummary);
      document.getElementById('template-picker-search').addEventListener('input', renderTemplatePicker);
      document.getElementById('template-picker-apply').addEventListener('click', applySelectedTemplate);
      document.getElementById('template-picker-modal')?.addEventListener('hidden.bs.modal', () => {
        resolveTemplatePicker(null);
      });
      document.getElementById('sch-load-source-fields')?.addEventListener('click', loadMappingFields);
      document.getElementById('sch-automapping')?.addEventListener('click', autoMapByName);
      bindEventListenerOnce('sch-mapping-assistant-profile', 'change', () => {
        state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || 'standard').trim() || 'standard';
        renderSchedulerMappingAssistant();
      });
      bindEventListenerOnce('sch-mapping-assistant-apply', 'click', async () => {
        state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || state.scheduleMappingAssistantProfile || 'standard').trim() || 'standard';
        await autoMapByName();
      });
      // Event delegation for dynamically rendered manager buttons
      document.getElementById('sch-mapping-manager')?.addEventListener('click', async (event) => {
        const target = event.target;
        if (!target) return;
        if (target.id === 'sch-manager-load-fields') {
          await loadMappingFields();
        } else if (target.id === 'sch-automapping') {
          await autoMapByName();
        } else if (target.id === 'sch-mapping-assistant-apply') {
          state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || state.scheduleMappingAssistantProfile || 'standard').trim() || 'standard';
          await autoMapByName();
        } else if (target.id === 'sch-mapping-preview-btn') {
          await showMappingPreview();
        } else if (target.id === 'sch-mapping-preview-close') {
          closeSchedulerMappingPreview();
        }
      });
      document.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape') {
          return;
        }
        const section = document.getElementById('sch-mapping-preview-section');
        if (!section || !section.classList.contains('is-visible')) {
          return;
        }
        closeSchedulerMappingPreview();
        event.preventDefault();
      });
      document.getElementById('sch-mapping-manager')?.addEventListener('change', (event) => {
        const target = event.target;
        if (!target) return;
        if (target.id === 'sch-mapping-assistant-profile') {
          state.scheduleMappingAssistantProfile = String(target.value || 'standard').trim() || 'standard';
          renderSchedulerMappingAssistant();
        }
      });
      bindEventListenerOnce('new-schedule', 'click', () => openScheduleModal(''));
      bindEventListenerOnce('new-schedule-from-template', 'click', async () => {
        try {
          await createFromTemplate('schedule');
        } catch (error) {
          showError(error.message || 'Scheduler-Vorlage konnte nicht geladen werden');
        }
      });
      document.getElementById('new-connector').addEventListener('click', () => openConnectorModal(''));
      document.getElementById('new-connector-from-template').addEventListener('click', async () => {
        try {
          await createFromTemplate('connector');
        } catch (error) {
          showError(error.message || 'Connector-Vorlage konnte nicht geladen werden');
        }
      });
      document.getElementById('sch-wizard-back').addEventListener('click', () => {
        goToScheduleWizardStep(state.scheduleWizardStep - 1);
      });
      document.getElementById('sch-wizard-next').addEventListener('click', advanceScheduleWizardStep);
      document.querySelectorAll('#sch-wizard-steps [data-sch-step]').forEach((button) => {
        button.addEventListener('click', () => {
          const nextStep = Number(button.getAttribute('data-sch-step') || '1');
          if (nextStep > state.scheduleWizardStep) {
            try {
              validateScheduleWizardStep(state.scheduleWizardStep);
            } catch (error) {
              showModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
              return;
            }
          }
          goToScheduleWizardStep(nextStep);
        });
      });
      bindEventListenerOnce('save-schedule', 'click', saveSchedule);
      document.getElementById('save-schedule-template').addEventListener('click', async () => {
        try {
          await saveCurrentAsTemplate('schedule');
        } catch (error) {
          showError(error.message || 'Scheduler-Vorlage konnte nicht gespeichert werden');
        }
      });
      document.getElementById('sch-test-source').addEventListener('click', testScheduleSource);
      bindEventListenerOnce('sch-active', 'change', renderRequiredSchedulerFieldStatus);
      document.getElementById('sch-source-type').addEventListener('change', () => {
        updateSourceQueryAssist();
        const srcType = document.getElementById('sch-source-type').value;
        if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
          loadMappingFields();
        }
      });
      document.getElementById('sch-source-definition').addEventListener('change', () => {
        updateSourceQueryAssist();
        const srcType = document.getElementById('sch-source-type').value;
        if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
          loadMappingFields();
        }
      });
      document.getElementById('sch-source-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
      document.getElementById('sch-source-archive-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
      document.getElementById('sch-target-type').addEventListener('change', updateScheduleFilePathSummaries);
      document.getElementById('sch-target-definition').addEventListener('input', updateScheduleFilePathSummaries);
      document.getElementById('sch-target-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
      document.getElementById('sch-target-archive-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
      document.getElementById('sch-source-definition').addEventListener('input', updateSourceQueryAssist);
      document.getElementById('sch-source-delta-strategy').addEventListener('change', updateSourceQueryAssist);
      document.getElementById('sch-source-delta-field').addEventListener('input', updateSourceQueryAssist);
      document.getElementById('sch-source-delta-current').addEventListener('input', updateSourceQueryAssist);
      document.getElementById('sch-source-delta-record-id').addEventListener('input', updateSourceQueryAssist);
      document.getElementById('sch-source-after-export').addEventListener('input', updateSourceQueryAssist);
      document.getElementById('sch-timing-apply').addEventListener('click', applyTimingHelper);
      document.getElementById('sch-timing-reset').addEventListener('click', () => {
        document.querySelectorAll('#sch-weekdays input').forEach((input) => {
          input.checked = false;
        });
        updateWeekdayChips();
        document.getElementById('sch-next-run').value = '';
        document.getElementById('sch-timing-preview').textContent = 'Noch keine Zeitsteuerung berechnet.';
      });
      document.querySelectorAll('#sch-weekdays input').forEach((input) => {
        input.addEventListener('change', updateWeekdayChips);
      });
      document.getElementById('sch-inherit-parent-timing').addEventListener('change', updateTimingInheritanceUi);
      document.getElementById('con-wizard-back').addEventListener('click', () => goToConnectorWizardStep(state.connectorWizardStep - 1));
      document.getElementById('con-wizard-next').addEventListener('click', advanceConnectorWizardStep);
      document.getElementById('save-connector').addEventListener('click', saveConnector);
      document.getElementById('save-connector-template').addEventListener('click', async () => {
        try {
          await saveCurrentAsTemplate('connector');
        } catch (error) {
          showError(error.message || 'Connector-Vorlage konnte nicht gespeichert werden');
        }
      });
      document.getElementById('con-type').addEventListener('input', updateConnectorConfigUi);
      document.getElementById('con-wizard-type').addEventListener('change', () => applyConnectorWizardSelection(false));
      document.getElementById('con-rest-auth-type').addEventListener('change', updateRestAuthUi);
      document.getElementById('load-logs').addEventListener('click', loadLogs);
      document.getElementById('sch-refresh-recent-logs')?.addEventListener('click', async () => {
        await renderScheduleRecentLogs(document.getElementById('sch-id')?.value || '');
      });
      document.getElementById('refresh-stale-runs')?.addEventListener('click', async () => {
        await refresh({ refreshChart: false });
      });
      document.getElementById('release-all-stale-runs')?.addEventListener('click', async () => {
        try {
          const result = await requestJson('/api/runs/release-stale', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
          });
          if (Number(result.releasedCount || 0) <= 0) {
            showError('Keine stale Runs zum Freigeben gefunden');
            return;
          }
          await refresh({ refreshChart: false });
        } catch (error) {
          showError(error.message || 'Stale Runs konnten nicht freigegeben werden');
        }
      });
      document.getElementById('sch-map-detail-apply').addEventListener('click', applySelectedMappingDetailChanges);
      document.getElementById('sch-map-detail-delete').addEventListener('click', deleteSelectedMappingRule);
      document.getElementById('sch-map-detail-picklist-add').addEventListener('click', addPicklistMappingEntry);
      document.getElementById('sch-validate-config')?.addEventListener('click', async () => {
        try {
          await validateCurrentScheduleConfiguration();
        } catch (error) {
          showModalError(error.message || 'Konfiguration konnte nicht geprüft werden');
        }
      });
      document.getElementById('sch-refresh-history')?.addEventListener('click', () => {
        loadEntityHistory('schedule', document.getElementById('sch-id')?.value || '', 'sch-history-list', 'sch-history-meta', 'Scheduler noch nicht gespeichert.');
      });
      document.getElementById('con-refresh-history')?.addEventListener('click', () => {
        loadEntityHistory('connector', document.getElementById('con-id')?.value || '', 'con-history-list', 'con-history-meta', 'Connector noch nicht gespeichert.');
      });
      document.getElementById('mig-refresh-history')?.addEventListener('click', () => {
        loadEntityHistory('migration', migState.id || '', 'mig-history-list', 'mig-history-meta', 'Migration noch nicht gespeichert.');
      });
      bindEventListenerOnce('sch-target-system', 'change', async () => {
        applyOperationOptions('');
        await loadTargetObjects('');
        await loadTargetFields();
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-target-type', 'change', async () => {
        applyOperationOptions('');
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-object', 'change', async () => {
        await loadTargetFields();
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-operation', 'change', async () => {
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-external-id-field', 'change', () => {
        ensureSalesforceTargetDefinition();
        updateSchedulerExternalIdValidationState();
      });
      bindEventListenerOnce('sch-pricebook2id', 'change', async () => {
        ensureSalesforceTargetDefinition();
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-target-definition', 'change', async () => {
        await syncSchedulerExternalIdUi();
      });
      bindEventListenerOnce('sch-mapping', 'input', () => {
        state.rawMappingEditorDirty = true;
      });
      bindEventListenerOnce('sch-mapping', 'change', async () => {
        hydrateMappingRulesFromDefinition();
        await syncSchedulerExternalIdUi();
      });
      document.getElementById('sch-create-custom-object').addEventListener('click', createSalesforceCustomObjectFromSource);
      bindEventListenerOnce('sch-connector', 'change', async () => {
        await loadTargetObjects(document.getElementById('sch-object').value || '');
        await loadTargetFields();
        await syncSchedulerExternalIdUi();
        const srcType = document.getElementById('sch-source-type').value;
        if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
          loadMappingFields();
        }
      });
      document.getElementById('duplicate-schedule').addEventListener('click', async () => {
        const scheduleId = document.getElementById('sch-id').value;
        if (!scheduleId) {
          return;
        }
        await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/duplicate', { method: 'POST' });
        scheduleModal.hide();
        await refresh();
      });
      document.getElementById('test-connector').addEventListener('click', saveAndValidateConnector);

      // ===== MAPPING FIELD LOADING & PREVIEW =====
      async function loadMappingFields() {
        const loadSeq = Number(state.mappingFieldsLoadSeq || 0) + 1;
        state.mappingFieldsLoadSeq = loadSeq;
        const sourceType = document.getElementById('sch-source-type').value;
        const sourceDefinition = buildScheduleSourceDefinitionValue() || '';
        const objectName = sourceType === 'SALESFORCE_SOQL'
          ? ''
          : document.getElementById('sch-object').value;
        const connectorId = document.getElementById('sch-connector').value || undefined;
        const sourceFieldsBody = document.getElementById('sch-mapping-source-fields');
        if (!sourceFieldsBody) {
          return;
        }

        state.mappingFields = [];
        renderSchedulerMappingManager();

        if (!sourceType || !sourceDefinition.trim()) {
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Keine Quellmetadaten verfügbar.</td></tr>';
          renderSchedulerMappingManager();
          return;
        }

        try {
          const result = await requestJson('/api/sources/fields', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sourceType, sourceDefinition, objectName, connectorId })
          });

          const fields = Array.isArray(result.fields) ? result.fields : [];
          if (loadSeq !== state.mappingFieldsLoadSeq) {
            return;
          }
          state.mappingFields = fields;
          reconcileMappingRuleSourceFields();
          refreshSchedulerMappingCompatibilityState();

          // Also fetch sample rows for inline examples in manager
          requestJson('/api/sources/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sourceType, sourceDefinition, connectorId, limit: 5 })
          }).then((previewResult) => {
            if (loadSeq !== state.mappingFieldsLoadSeq) return;
            state.sourcePreviewRows = Array.isArray(previewResult.rows) ? previewResult.rows : [];
            renderSchedulerMappingManager();
          }).catch(() => {
            state.sourcePreviewRows = [];
          });

          sourceFieldsBody.innerHTML = fields.length
            ? fields.map((field, idx) =>
              '<tr data-field-index="' + idx + '" draggable="true">' +
                '<td>' + esc(field.label ? field.name + ' (' + field.label + ')' : field.name || '-') + '</td>' +
                '<td>' + esc(field.type || 'string') + '</td>' +
              '</tr>'
            ).join('')
            : '<tr><td colspan="2" class="text-secondary">Keine Felder gefunden.</td></tr>';

          sourceFieldsBody.querySelectorAll('tr').forEach(row => {
            row.addEventListener('dragstart', (e) => {
              e.dataTransfer.effectAllowed = 'copy';
              const fieldIndex = Number(row.getAttribute('data-field-index'));
              const field = fields[fieldIndex];
              const payload = JSON.stringify({
                name: String(field?.name || '').trim(),
                type: String(field?.type || 'string').trim(),
                label: String(field?.label || '').trim()
              });
              e.dataTransfer.setData('application/json', payload);
              e.dataTransfer.setData('text/plain', String(field?.name || '').trim());
            });
          });

          renderMappingRulesTable();

          renderCreateObjectFieldOverrides();
        } catch (error) {
          if (loadSeq !== state.mappingFieldsLoadSeq) {
            return;
          }
          state.mappingFields = [];
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Feldmetadaten konnten nicht geladen werden.</td></tr>';
          showModalError(error.message || 'Feldmetadaten konnten nicht geladen werden');
          renderSchedulerMappingManager();
          renderCreateObjectFieldOverrides();
        }
      }

      function loadMappingPreview(previewData) {
        // Render ~10 rows of preview data from source
        if (!previewData || !Array.isArray(previewData)) {
          previewData = [];
        }

        const header = document.getElementById('sch-mapping-preview-header');
        const body = document.getElementById('sch-mapping-preview-body');
        
        if (!header || !body) {
          return;
        }

        // Build header from first record keys
        const firstRecord = previewData[0] || {};
        const columns = Object.keys(firstRecord).slice(0, 10);
        
        if (columns.length === 0) {
          header.innerHTML = '<tr><th>Keine Daten</th></tr>';
          body.innerHTML = '<tr><td colspan="1" class="text-secondary">Keine Vorschaudaten verfügbar</td></tr>';
          return;
        }

        header.innerHTML = '<tr>' + columns.map(col => '<th>' + esc(col) + '</th>').join('') + '</tr>';
        
        // Build rows (max 10)
        body.innerHTML = previewData.slice(0, 10).map(record =>
          '<tr>' + columns.map(col => '<td>' + esc(record[col] || '-') + '</td>').join('') + '</tr>'
        ).join('');
      }

      function closeSchedulerMappingPreview() {
        const section = document.getElementById('sch-mapping-preview-section');
        if (!section) {
          return;
        }
        section.classList.remove('is-visible');
        section.setAttribute('aria-hidden', 'true');
      }

      async function showMappingPreview() {
        const section = document.getElementById('sch-mapping-preview-section');
        const statusEl = document.getElementById('sch-mapping-preview-status');
        const head = document.getElementById('sch-mapping-preview-head');
        const body = document.getElementById('sch-mapping-preview-body');
        if (!section || !statusEl || !head || !body) return;

        const managerShell = document.querySelector('#sch-mapping-manager .sch-mapping-manager-shell');
        const toolbar = managerShell?.querySelector ? managerShell.querySelector('.scheduler-mapping-toolbar') : null;
        const assistantBar = managerShell?.querySelector ? managerShell.querySelector('.scheduler-mapping-assistant-bar') : null;
        let previewTop = 8;
        if (toolbar && Number.isFinite(toolbar.offsetHeight)) {
          previewTop += toolbar.offsetHeight;
        }
        if (assistantBar && Number.isFinite(assistantBar.offsetHeight)) {
          previewTop += assistantBar.offsetHeight;
        }
        section.style.top = String(previewTop) + 'px';
        section.classList.add('is-visible');
        section.setAttribute('aria-hidden', 'false');

        const previewRows = Array.isArray(state.sourcePreviewRows) ? state.sourcePreviewRows : [];
        if (!previewRows.length) {
          statusEl.textContent = 'Keine Quelldaten verfügbar. Bitte zuerst Felder laden.';
          head.innerHTML = '';
          body.innerHTML = '<tr><td class="text-secondary">Keine Daten</td></tr>';
          return;
        }

        syncMappingDefinitionFromRules();
        const mappingDefinition = String(document.getElementById('sch-mapping')?.value || '').trim();
        if (!mappingDefinition) {
          statusEl.textContent = 'Kein Mapping definiert.';
          head.innerHTML = '';
          body.innerHTML = '<tr><td class="text-secondary">Kein Mapping</td></tr>';
          return;
        }

        statusEl.textContent = 'Vorschau wird berechnet...';
        head.innerHTML = '';
        body.innerHTML = '<tr><td class="text-secondary">Wird geladen...</td></tr>';

        try {
          const result = await requestJson('/api/mappings/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mappingDefinition, sourceData: previewRows })
          });
          const fields = Array.isArray(result.fields) ? result.fields : [];
          const rows = Array.isArray(result.rows) ? result.rows : [];
          if (!fields.length) {
            statusEl.textContent = 'Keine gemappten Felder gefunden.';
            head.innerHTML = '';
            body.innerHTML = '<tr><td class="text-secondary">Keine Daten</td></tr>';
            return;
          }
          statusEl.textContent = rows.length + ' Datensätze (Vorschau, max. 5)';
          head.innerHTML = '<tr>' + fields.map((f) => '<th class="small">' + esc(String(f)) + '</th>').join('') + '</tr>';
          body.innerHTML = rows.slice(0, 5).map((row) =>
            '<tr>' + fields.map((f) => '<td class="small">' + esc(String(row[f] ?? '')) + '</td>').join('') + '</tr>'
          ).join('');
        } catch (error) {
          statusEl.textContent = 'Vorschau konnte nicht berechnet werden: ' + (error.message || '');
          head.innerHTML = '';
          body.innerHTML = '<tr><td class="text-secondary">Fehler</td></tr>';
        }
      }

      function applySchedulerTableClientFilters() {
        const schedulersFilter = document.getElementById('schedulers-filter');
        const query = String(schedulersFilter?.value || '').trim().toLowerCase();
        const activeFilter = String(state.schedulerActiveFilter || 'all').trim().toLowerCase();
        const rows = document.querySelectorAll('#schedules-body tr');

        rows.forEach((row) => {
          const text = String(row.textContent || '').toLowerCase();
          const toggle = row.querySelector('input[data-toggle-schedule-active]');
          const rowActiveState = toggle && toggle.checked ? 'active' : 'inactive';
          const matchesQuery = !query || text.includes(query);
          const matchesActive = activeFilter === 'all' || rowActiveState === activeFilter;
          row.style.display = matchesQuery && matchesActive ? '' : 'none';
        });
      }

      // ===== NATIVE TABLE FILTERING & SEARCH =====
      const TABLE_STORAGE_KEY = 'sf-agent.table-filters';

      function initializeTableFilters() {
        // Scheduler table filter
        const schedulersFilter = document.getElementById('schedulers-filter');
        if (schedulersFilter && schedulersFilter.dataset.bound !== '1') {
          schedulersFilter.dataset.bound = '1';
          schedulersFilter.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            applySchedulerTableClientFilters();

            // Store filter value
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.schedulers', query);
            } catch (e) {
              // Ignore storage errors
            }
          });
          // Restore filter value
          try {
            const stored = localStorage.getItem(TABLE_STORAGE_KEY + '.schedulers');
            if (stored) {
              schedulersFilter.value = stored;
            }
          } catch (e) {
            // Ignore storage errors
          }
        }

        const directionTabs = document.querySelectorAll('#schedulers-direction-tabs [data-direction-tab]');
        directionTabs.forEach((tabButton) => {
          if (tabButton.dataset.bound === '1') {
            return;
          }

          tabButton.dataset.bound = '1';
          tabButton.addEventListener('click', () => {
            const direction = String(tabButton.getAttribute('data-direction-tab') || 'all').trim().toLowerCase();
            state.schedulerDirectionTab = ['all', 'inbound', 'outbound'].includes(direction) ? direction : 'all';
            directionTabs.forEach((button) => {
              button.classList.remove('is-active');
              button.setAttribute('aria-pressed', 'false');
            });
            tabButton.classList.add('is-active');
            tabButton.setAttribute('aria-pressed', 'true');
            renderSchedules();
          });
        });

        const schedulersActiveFilter = document.getElementById('schedulers-active-filter');
        if (schedulersActiveFilter && schedulersActiveFilter.dataset.bound !== '1') {
          schedulersActiveFilter.dataset.bound = '1';
          schedulersActiveFilter.addEventListener('change', (e) => {
            const value = String(e.target.value || 'all').trim().toLowerCase();
            state.schedulerActiveFilter = ['all', 'active', 'inactive'].includes(value) ? value : 'all';
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.schedulers.active', state.schedulerActiveFilter);
            } catch (error) {
              // Ignore storage errors
            }
            renderSchedules();
            applySchedulerTableClientFilters();
          });

          try {
            const storedActiveFilter = localStorage.getItem(TABLE_STORAGE_KEY + '.schedulers.active');
            if (storedActiveFilter) {
              state.schedulerActiveFilter = ['all', 'active', 'inactive'].includes(storedActiveFilter) ? storedActiveFilter : 'all';
            }
          } catch (error) {
            // Ignore storage errors
          }

          schedulersActiveFilter.value = state.schedulerActiveFilter || 'all';
          applySchedulerTableClientFilters();
        }

        const schedulersConnectorFilter = document.getElementById('schedulers-connector-filter');
        if (schedulersConnectorFilter && schedulersConnectorFilter.dataset.bound !== '1') {
          schedulersConnectorFilter.dataset.bound = '1';
          schedulersConnectorFilter.addEventListener('change', (e) => {
            state.schedulerConnectorFilterId = String(e.target.value || '').trim();
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.schedulers.connector', state.schedulerConnectorFilterId);
            } catch (error) {
              // Ignore storage errors
            }
            renderSchedules();
          });

          try {
            const storedConnectorFilter = localStorage.getItem(TABLE_STORAGE_KEY + '.schedulers.connector');
            if (storedConnectorFilter) {
              state.schedulerConnectorFilterId = storedConnectorFilter;
            }
          } catch (error) {
            // Ignore storage errors
          }

          renderSchedulerConnectorFilterOptions();
        }

        function applyConnectorsPanelFilter(query) {
          const normalizedQuery = String(query || '').toLowerCase();
          const panels = document.querySelectorAll('#connectors-panels [data-connector-panel]');
          panels.forEach((panel) => {
            const text = String(panel.textContent || '').toLowerCase();
            const isMatch = text.includes(normalizedQuery);
            panel.style.display = isMatch ? '' : 'none';
          });
        }

        // Connectors table filter
        const connectorsFilter = document.getElementById('connectors-filter');
        if (connectorsFilter && connectorsFilter.dataset.bound !== '1') {
          connectorsFilter.dataset.bound = '1';
          connectorsFilter.addEventListener('input', (e) => {
            const query = String(e.target.value || '');
            applyConnectorsPanelFilter(query);
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.connectors', query.toLowerCase());
            } catch (e) {
              // Ignore storage errors
            }
          });
          try {
            const stored = localStorage.getItem(TABLE_STORAGE_KEY + '.connectors');
            if (stored) {
              connectorsFilter.value = stored;
            }
          } catch (e) {
            // Ignore storage errors
          }
        }
        if (connectorsFilter) {
          applyConnectorsPanelFilter(connectorsFilter.value || '');
        }

        // Logs table filter
        const logsFilter = document.getElementById('logs-filter');
        if (logsFilter && logsFilter.dataset.bound !== '1') {
          logsFilter.dataset.bound = '1';
          logsFilter.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            const rows = document.querySelectorAll('#logs-modal-body tr');
            rows.forEach(row => {
              const text = row.textContent.toLowerCase();
              const isMatch = text.includes(query);
              row.style.display = isMatch ? '' : 'none';
            });
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.logs', query);
            } catch (e) {
              // Ignore storage errors
            }
          });
          try {
            const stored = localStorage.getItem(TABLE_STORAGE_KEY + '.logs');
            if (stored) {
              logsFilter.value = stored;
              logsFilter.dispatchEvent(new Event('input'));
            }
          } catch (e) {
            // Ignore storage errors
          }
        }
      }

      // Re-initialize table filters when data changes
      const originalOpenScheduleModal = window.openScheduleModal;
      window.openScheduleModal = function(scheduleId) {
        originalOpenScheduleModal(scheduleId);
        setTimeout(() => {
          initializeTableFilters();
          const sourceDefEl = document.getElementById('sch-source-definition');
          if (sourceDefEl && sourceDefEl.value) {
            loadMappingFields();
          }
        }, 100);
      };

      // ──────────────────────────────────────────────────────────────────────
      //  MIGRATION WIZARD
      // ──────────────────────────────────────────────────────────────────────

      function migUuidV4() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
          const r = (Math.random() * 16) | 0;
          return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
        });
      }

      function renderMigWizardSteps() {
        document.querySelectorAll('#mig-wizard-steps [data-mig-step]').forEach((btn) => {
          const step = Number(btn.getAttribute('data-mig-step'));
          btn.classList.toggle('is-active', step === migState.step);
          btn.classList.toggle('is-complete', step < migState.step);
        });
        document.querySelectorAll('.mig-wizard-panel').forEach((panel) => {
          const step = Number(panel.getAttribute('data-mig-step-panel'));
          panel.classList.toggle('d-none', step !== migState.step);
        });
        const prev = document.getElementById('mig-wizard-prev');
        const next = document.getElementById('mig-wizard-next');
        if (prev) prev.disabled = migState.step <= 1;
        if (next) {
          if (migState.step === migState.totalSteps) {
            next.textContent = 'Migration starten ▶';
            next.className = 'btn btn-success';
          } else {
            next.textContent = 'Weiter →';
            next.className = 'btn btn-primary';
          }
        }
      }

      function renderMigSelectedObjects() {
        const container = document.getElementById('mig-selected-objects');
        if (!container) return;
        renderMigImportSuggestions();
        if (!migState.objects.length) {
          container.innerHTML = '<span class="text-secondary small">Noch keine Objekte ausgewählt.</span>';
          return;
        }
        container.innerHTML = migState.objects.map((obj) =>
          '<span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:0.85em">' +
          esc(getMigObjectDisplayName(obj)) +
          '<button type="button" class="btn-close btn-close-white" style="font-size:0.6em" data-remove-obj="' + esc(obj.id) + '" aria-label="Entfernen"></button></span>'
        ).join('');
        container.querySelectorAll('[data-remove-obj]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const id = btn.getAttribute('data-remove-obj');
            migState.objects = migState.objects.filter((o) => o.id !== id);
            migState.dependencies = migState.dependencies.filter((d) => d.fromObjectId !== id && d.toObjectId !== id);
            migState.executionPlan = migState.executionPlan.filter((s) => s.objectId !== id);
            renderMigSelectedObjects();
          });
        });
      }

      async function renderMigFileAssignments() {
        const container = document.getElementById('mig-file-assignment-list');
        if (!container) return;
        renderMigPendingImportHint();
        if (!migState.objects.length) {
          container.innerHTML = '<div class="text-secondary small">Bitte zuerst Objekte in Schritt 1 auswählen.</div>';
          return;
        }

        await Promise.all(migState.objects.map((obj) => loadMigExternalIdOptions(obj)));

        container.innerHTML = migState.objects.map((obj) => {
          const safeId = esc(obj.id);
          const availableSheetNames = Array.isArray(obj.availableSheetNames) ? obj.availableSheetNames : [];
          const externalIdOptions = (Array.isArray(obj._externalIdFields) ? obj._externalIdFields : [])
            .map((field) => {
              const name = String(field?.name || '').trim();
              const label = String(field?.label || '').trim();
              const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
              return '<option value="' + esc(name) + '"' + (String(obj.externalIdField || '').trim() === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
            })
            .join('');
          const sheetOptions = availableSheetNames.length
            ? ('<option value="">Primäre Mappe</option>' +
                availableSheetNames.map((sheetName) =>
                  '<option value="' + esc(sheetName) + '"' + (String(obj.fileSheetName || '') === sheetName ? ' selected' : '') + '>' + esc(sheetName) + '</option>'
                ).join(''))
            : '<option value="">Nicht erforderlich</option>';
          return '<div class="card soft-card mb-2"><div class="card-body"><div class="d-flex justify-content-between align-items-center mb-2">' +
            '<strong>' + esc(getMigObjectDisplayName(obj)) + '</strong>' +
            '<select class="form-select form-select-sm w-auto" style="min-width:120px" data-op-select="' + safeId + '">' +
            '<option value="insert"' + (obj.operation === 'insert' ? ' selected' : '') + '>Insert</option>' +
            '<option value="upsert"' + (obj.operation === 'upsert' ? ' selected' : '') + '>Upsert</option>' +
            '<option value="update"' + (obj.operation === 'update' ? ' selected' : '') + '>Update</option>' +
            '</select></div>' +
            '<div class="mb-2"><label class="form-label small mb-1">Upsert-Feld (External ID)</label>' +
            '<select class="form-select form-select-sm" data-external-id-select="' + safeId + '"' + (obj.operation === 'upsert' ? '' : ' disabled') + '>' +
            '<option value="">- Bitte wählen -</option>' + externalIdOptions +
            '</select>' +
            '<div class="small text-secondary mt-1">Wird nur für Upsert verwendet.</div></div>' +
            '<div class="input-group mb-1">' +
            '<input type="text" class="form-control form-control-sm" placeholder="Noch keine Datei ausgewählt" value="' + esc(obj.filePath || '') + '" data-file-path="' + safeId + '" readonly />' +
            '<input type="file" class="d-none" data-file-dialog="' + safeId + '" accept=".csv,.txt,.json,.xlsx,.xls" />' +
            '<button class="btn btn-sm btn-outline-primary" data-pick-file="' + safeId + '">Datei wählen</button>' +
            '<button class="btn btn-sm btn-outline-secondary" data-analyze-file="' + safeId + '">Analysieren</button>' +
            '</div>' +
            '<div class="row g-2 mb-2">' +
            '<div class="col-md-4"><label class="form-label small mb-1">Charset</label><select class="form-select form-select-sm" data-file-charset="' + safeId + '">' +
            '<option value="utf8"' + ((obj.fileCharset || 'utf8') === 'utf8' ? ' selected' : '') + '>UTF-8</option>' +
            '<option value="windows-1252"' + ((obj.fileCharset || '') === 'windows-1252' ? ' selected' : '') + '>Windows-1252 (ANSI)</option>' +
            '<option value="latin1"' + (obj.fileCharset === 'latin1' ? ' selected' : '') + '>Latin-1</option>' +
            '<option value="utf-16le"' + (obj.fileCharset === 'utf-16le' ? ' selected' : '') + '>UTF-16 LE</option>' +
            '<option value="ascii"' + (obj.fileCharset === 'ascii' ? ' selected' : '') + '>ASCII</option>' +
            '</select></div>' +
            '<div class="col-md-4"><label class="form-label small mb-1">Trennzeichen</label><select class="form-select form-select-sm" data-file-delimiter="' + safeId + '">' +
            '<option value=""' + ((!obj.fileDelimiter || obj.fileDelimiter === 'auto') ? ' selected' : '') + '>Automatisch erkennen</option>' +
            '<option value=";"' + ((obj.fileDelimiter || ';') === ';' ? ' selected' : '') + '>Semikolon (;)</option>' +
            '<option value=","' + (obj.fileDelimiter === ',' ? ' selected' : '') + '>Komma (,)</option>' +
            '<option value="|"' + (obj.fileDelimiter === '|' ? ' selected' : '') + '>Pipe (|)</option>' +
            '<option value="\t"' + (obj.fileDelimiter === '\t' ? ' selected' : '') + '>Tabulator</option>' +
            '</select></div>' +
            '<div class="col-md-4"><label class="form-label small mb-1">Textqualifier</label><select class="form-select form-select-sm" data-file-text-qualifier="' + safeId + '">' +
            '<option value="\""' + ((obj.fileTextQualifier || '"') === '"' ? ' selected' : '') + '>Doppelte Anführungszeichen (")</option>' +
            '<option value="&#39;"' + (obj.fileTextQualifier === "'" ? ' selected' : '') + '>Einfache Anführungszeichen (&#39;)</option>' +
            '<option value=""' + (obj.fileTextQualifier === '' ? ' selected' : '') + '>Keiner</option>' +
            '</select></div>' +
            '<div class="col-md-4"><label class="form-label small mb-1">Excel-Mappe</label><select class="form-select form-select-sm" data-file-sheet="' + safeId + '"' + (availableSheetNames.length ? '' : ' disabled') + '>' + sheetOptions + '</select></div>' +
            '<div class="col-md-4"><label class="form-label small mb-1">Verarbeitungsmodus</label><select class="form-select form-select-sm" data-processing-mode="' + safeId + '">' +
            '<option value="sqlite"' + (((obj.processingMode || obj.stagingMode || 'sqlite') === 'sqlite') ? ' selected' : '') + '>SQLite-Staging</option>' +
            '<option value="file"' + (obj.processingMode === 'file' ? ' selected' : '') + '>Datei direkt</option>' +
            '</select></div>' +
            '</div>' +
            '<div id="mig-file-cols-' + safeId + '" class="small text-secondary">' +
            esc(renderMigFileSummary(obj)) +
            '</div></div></div>';
        }).join('');

        migState.objects.forEach((obj) => {
          const fileInput = container.querySelector('[data-file-path="' + obj.id + '"]');
          const fileDialog = container.querySelector('[data-file-dialog="' + obj.id + '"]');
          const pickBtn = container.querySelector('[data-pick-file="' + obj.id + '"]');
          const opSelect = container.querySelector('[data-op-select="' + obj.id + '"]');
          const externalIdSelect = container.querySelector('[data-external-id-select="' + obj.id + '"]');
          if (opSelect) {
            opSelect.addEventListener('change', () => {
              obj.operation = opSelect.value;
              if (externalIdSelect) {
                externalIdSelect.disabled = obj.operation !== 'upsert';
              }
              if (obj.operation !== 'upsert') {
                obj.externalIdField = '';
                if (externalIdSelect) {
                  externalIdSelect.value = '';
                }
              }
            });
          }
          if (externalIdSelect) {
            externalIdSelect.addEventListener('change', () => {
              obj.externalIdField = externalIdSelect.value || '';
            });
          }
          const analyzeBtn = container.querySelector('[data-analyze-file="' + obj.id + '"]');
          const charsetInput = container.querySelector('[data-file-charset="' + obj.id + '"]');
          const delimiterInput = container.querySelector('[data-file-delimiter="' + obj.id + '"]');
          const textQualifierInput = container.querySelector('[data-file-text-qualifier="' + obj.id + '"]');
          const sheetInput = container.querySelector('[data-file-sheet="' + obj.id + '"]');
          const processingModeInput = container.querySelector('[data-processing-mode="' + obj.id + '"]');

          const syncCsvOptions = () => {
            obj.fileCharset = charsetInput ? charsetInput.value.trim() || 'utf8' : (obj.fileCharset || 'utf8');
            obj.fileDelimiter = delimiterInput ? delimiterInput.value || ';' : (obj.fileDelimiter || ';');
            obj.fileTextQualifier = textQualifierInput ? textQualifierInput.value || '"' : (obj.fileTextQualifier || '"');
            obj.fileSheetName = sheetInput ? sheetInput.value || '' : (obj.fileSheetName || '');
            obj.processingMode = processingModeInput ? processingModeInput.value || 'sqlite' : (obj.processingMode || 'sqlite');
          };

          [charsetInput, delimiterInput, textQualifierInput, sheetInput, processingModeInput].forEach((input) => {
            if (!input) return;
            input.addEventListener('change', syncCsvOptions);
          });

          if (pickBtn && fileDialog) {
            pickBtn.addEventListener('click', () => {
              fileDialog.click();
            });

            fileDialog.addEventListener('change', async () => {
              const file = fileDialog.files && fileDialog.files[0] ? fileDialog.files[0] : null;
              if (!file) return;

              pickBtn.disabled = true;
              if (analyzeBtn) analyzeBtn.disabled = true;
              pickBtn.textContent = 'Upload…';

              try {
                syncCsvOptions();
                await migSave();
                await uploadMigrationObjectFile(obj, file);

                if (fileInput) fileInput.value = obj.filePath || '';
                const colDiv = document.getElementById('mig-file-cols-' + obj.id);
                if (colDiv) {
                  colDiv.textContent = renderMigFileSummary(obj);
                }

                await migSave();
                renderMigMappingObjectSelect();
              } catch (err) {
                alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
              } finally {
                pickBtn.disabled = false;
                if (analyzeBtn) analyzeBtn.disabled = false;
                pickBtn.textContent = 'Datei wählen';
                fileDialog.value = '';
              }
            });
          }

          if (analyzeBtn) {
            analyzeBtn.addEventListener('click', async () => {
              const pathEl = container.querySelector('[data-file-path="' + obj.id + '"]');
              obj.filePath = pathEl ? pathEl.value.trim() : obj.filePath;
              syncCsvOptions();
              if (!obj.filePath) { alert('Bitte zuerst eine Datei auswählen.'); return; }
              analyzeBtn.disabled = true;
              analyzeBtn.textContent = '…';
              try {
                await migSave();
                await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
                const colDiv = document.getElementById('mig-file-cols-' + obj.id);
                if (colDiv) colDiv.textContent = renderMigFileSummary(obj);
                await migSave();
                renderMigMappingObjectSelect();
              } catch (err) {
                alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
              } finally {
                analyzeBtn.disabled = false;
                analyzeBtn.textContent = 'Analysieren';
              }
            });
          }
        });

        if (getMigSelectedPendingImports(migState.pendingImports).length && !migState.pendingImportInProgress) {
          queueMicrotask(() => {
            consumePendingMigrationImportIfPossible().catch((error) => {
              alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
            });
          });
        }
      }

      function renderMigMappingObjectSelect() {
        const sel = document.getElementById('mig-mapping-object-select');
        if (!sel) return;
        sel.innerHTML = migState.objects.map((obj) =>
          '<option value="' + esc(obj.id) + '">' + esc(getMigObjectDisplayName(obj)) + '</option>'
        ).join('');
        renderMigMappingPanel();
      }

      async function renderMigMappingPanel() {
        const sel = document.getElementById('mig-mapping-object-select');
        const panel = document.getElementById('mig-mapping-panel');
        const assistantShell = document.getElementById('mig-mapping-assistant-shell');
        if (!sel || !panel) return;
        const objectId = sel.value;
        const obj = migState.objects.find((o) => o.id === objectId);
        if (!obj) {
          if (assistantShell) assistantShell.innerHTML = '';
          panel.innerHTML = '<div class="text-secondary small">Kein Objekt ausgewählt.</div>';
          return;
        }
        if (!obj.fileColumns || !obj.fileColumns.length) {
          if (assistantShell) assistantShell.innerHTML = '';
          panel.innerHTML = '<div class="text-secondary small">Bitte zuerst die Datei in Schritt 2 analysieren.</div>'; return;
        }

        if (obj.stagingMode === 'sqlite' && (!obj.previewRows || !obj.previewRows.length) && Number(obj.fileRecordCount || 0) >= 0) {
          try {
            await loadMigObjectPreview(obj, obj.previewOffset || 0, obj.previewLimit || 10);
          } catch {
            // preview bootstrap falls back to current state
          }
        }

        if (getMigLatestFailedStep(obj.id) && !obj.failedPreviewLoadedFor) {
          try {
            await loadMigLatestFailedPreview(obj);
          } catch {
            obj.failedPreviewRecords = [];
          }
        }

        panel.innerHTML = '<div class="spinner-border spinner-border-sm me-2"></div>Salesforce-Felder laden…';
        let sfFields = [];
        let sfObjects = [];
        try {
          const [fieldsRes, objectsRes] = await Promise.all([
            fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject) + '&instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || '')),
            fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''))
          ]);
          if (fieldsRes.ok) sfFields = await fieldsRes.json();
          if (objectsRes.ok) sfObjects = await objectsRes.json();
        } catch { /* ignore */ }

        const existingFieldNames = new Set([
          ...(sfFields || []).map((f) => String(f.name || '').toLowerCase())
        ]);
        obj._existingFieldNames = Array.from(existingFieldNames);
        renderMigrationMappingAssistant(obj);

        const autoMappedCount = await autoPopulateMigFieldMappings(obj, sfFields);
        const externalIdChanged = autoSelectMigExternalIdField(obj, sfFields);
        if (autoMappedCount > 0 || externalIdChanged) {
          await migSave();
        }

        // Mark mapping entries that point to fields not yet existing in Salesforce.
        (obj.fieldMappings || []).forEach((mapping) => {
          const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, sfFields);
          const sfField = (sfFields || []).find((field) => String(field.name || '').toLowerCase() === resolvedTargetField.toLowerCase());
          if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
            mapping.targetField = resolvedTargetField;
            mapping.targetFieldLabel = sfField?.label || resolvedTargetField;
            mapping.targetFieldType = sfField?.type || mapping.targetFieldType;
          }
          mapping._isMissing = !!resolvedTargetField && !existingFieldNames.has(resolvedTargetField.toLowerCase());
        });

        const parsePicklistText = (value) => String(value || '').split(';')
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => {
            const idx = part.indexOf('=');
            if (idx < 0) return null;
            return { source: part.slice(0, idx).trim(), target: part.slice(idx + 1).trim() };
          })
          .filter((entry) => entry && (entry.source || entry.target));
        const renderMigPicklistMappingRows = (entries, sourceColumn) => {
          const normalizedEntries = Array.isArray(entries) ? entries : [];
          if (!normalizedEntries.length) {
            return '<tr><td colspan="3" class="migration-picklist-empty">Keine Picklist-Mappings.</td></tr>';
          }

          return normalizedEntries.map((entry, index) => (
            '<tr data-map-picklist-row="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '">' +
              '<td><input class="form-control form-control-sm" value="' + esc(String(entry?.source || '')) + '" data-map-picklist-source="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '" placeholder="Quellwert" /></td>' +
              '<td><input class="form-control form-control-sm" value="' + esc(String(entry?.target || '')) + '" data-map-picklist-target="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '" placeholder="Zielfeldwert" /></td>' +
              '<td class="text-end"><button type="button" class="btn btn-sm btn-outline-danger" data-map-picklist-delete="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '">Löschen</button></td>' +
            '</tr>'
          )).join('');
        };

        // Build lookup object <select> options once (reused per row)
        const sfObjectOptHtml = '<option value="">- SF Objekt wählen -</option>' +
          (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '">' + esc(o.label || o.name) + '</option>').join('');

        const mappedColumns = obj.fileColumns.filter((col) => {
          const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
          return existing && String(existing.targetField || '').trim();
        });
        const sortedFileColumns = obj.fileColumns
          .slice()
          .sort((a, b) => String(a || '').localeCompare(String(b || ''), 'de', { sensitivity: 'base', numeric: true }));
        const missingTargetCount = (obj.fieldMappings || []).filter((mapping) => mapping && mapping._isMissing === true).length;
        const lookupCount = (obj.fieldMappings || []).filter((mapping) => mapping && mapping.lookupEnabled === true).length;

        panel.innerHTML =
          '<div class="migration-mapping-overview">' +
            '<div class="migration-mapping-stat">' +
              '<span class="migration-mapping-stat-value">' + esc(String(mappedColumns.length)) + '</span>' +
              '<span class="migration-mapping-stat-label">gemappt</span>' +
            '</div>' +
            '<div class="migration-mapping-stat">' +
              '<span class="migration-mapping-stat-value">' + esc(String(Math.max(0, obj.fileColumns.length - mappedColumns.length))) + '</span>' +
              '<span class="migration-mapping-stat-label">offen</span>' +
            '</div>' +
            '<div class="migration-mapping-stat">' +
              '<span class="migration-mapping-stat-value">' + esc(String(missingTargetCount)) + '</span>' +
              '<span class="migration-mapping-stat-label">neu anzulegen</span>' +
            '</div>' +
            '<div class="migration-mapping-stat">' +
              '<span class="migration-mapping-stat-value">' + esc(String(lookupCount)) + '</span>' +
              '<span class="migration-mapping-stat-label">Lookups</span>' +
            '</div>' +
          '</div>' +
          '<div class="migration-mapping-toolbar">' +
            '<div>' +
              '<div class="fw-semibold">Mappingmanager</div>' +
              '<div class="small text-secondary">Quellfelder links, Zielfelder rechts. Details nur öffnen, wenn Lookup, Transform oder Picklist gebraucht werden.</div>' +
            '</div>' +
            '<input class="form-control form-control-sm migration-mapping-search" type="search" placeholder="Quelle oder Ziel suchen" data-mig-map-filter>' +
          '</div>' +
          '<div class="migration-mapping-list">' +
          sortedFileColumns.map((col) => {
            const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
            const currentTarget = existing ? resolveMigTargetFieldApiName(existing.targetField, sfFields) : '';
            const targetMeta = (sfFields || []).find((f) => String(f.name || '').toLowerCase() === currentTarget.toLowerCase());
            const targetType = targetMeta?.type || (existing?._isMissing ? 'neu' : '');
            const transformFunction = String(existing?.transformFunction || 'NONE');
            const transformExpression = String(existing?.transformExpression || '');
            const isStatic = transformFunction === 'STATIC';
            const lookupEnabled = existing?.lookupEnabled === true;
            const lookupObject = String(existing?.lookupObject || '');
            const lookupField = String(existing?.lookupField || '');
            const picklistMappings = Array.isArray(existing?.picklistMappings) ? existing.picklistMappings : [];
            const isEmailTarget = String(targetMeta?.type || '').trim().toLowerCase() === 'email'
              || normalizeFieldKey(currentTarget).includes('email');
            const emailValidationEnabled = isEmailTarget && (existing?.emailValidation?.enabled === true || existing?.emailValidationEnabled === true);
            const emailInvalidAction = String(existing?.emailValidation?.invalidAction || existing?.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
            const rowSearchText = [col, currentTarget, targetType, transformFunction, lookupObject, lookupField, (emailValidationEnabled ? 'email' : '')].join(' ').toLowerCase();
            const rowStatus = currentTarget ? (existing?._isMissing ? 'new' : 'mapped') : 'open';
            const rowStatusLabel = currentTarget ? (existing?._isMissing ? 'Neues Feld' : 'Gemappt') : 'Offen';
            const rowStatusClass = currentTarget ? (existing?._isMissing ? 'text-bg-warning' : 'text-bg-success') : 'text-bg-light';
            const detailsOpen = lookupEnabled || picklistMappings.length > 0 || isStatic || isEmailTarget;
            const usesManualTarget = Boolean(currentTarget) && !targetMeta;

            // Lookup object options with pre-selected value
            const lookupObjOptions = '<option value="">- SF Objekt wählen -</option>' +
              (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '"' + (lookupObject === o.name ? ' selected' : '') + '>' + esc(o.label || o.name) + '</option>').join('');

            return '<section class="migration-mapping-row" data-mig-map-row data-mig-map-status="' + esc(rowStatus) + '" data-mig-map-search="' + esc(rowSearchText) + '">' +
              '<div class="migration-mapping-row-main">' +
                '<div class="migration-mapping-source">' +
                  '<span class="badge ' + rowStatusClass + '">' + esc(rowStatusLabel) + '</span>' +
                  '<code>' + esc(col) + '</code>' +
                '</div>' +
                '<div class="migration-mapping-arrow" aria-hidden="true">&rarr;</div>' +
                '<div class="migration-mapping-target">' +
                  '<div class="migration-mapping-target-inputs">' +
                    '<select class="form-select form-select-sm" data-map-target-select="' + esc(col) + '">' + getMigMappingTargetOptions(sfFields, currentTarget) + '</select>' +
                    '<input class="form-control form-control-sm' + (usesManualTarget ? '' : ' d-none') + '" placeholder="Neues Salesforce-Feld eingeben" value="' + esc(usesManualTarget ? currentTarget : '') + '" data-map-col="' + esc(col) + '" data-map-obj="' + esc(objectId) + '" />' +
                  '</div>' +
                  '<span class="badge bg-secondary migration-mapping-type" data-map-type="' + esc(col) + '">' + esc(targetType) + (targetMeta?.requiredOnCreate === true ? ' *' : '') + '</span>' +
                '</div>' +
                '<div class="migration-mapping-transform">' +
                  '<label class="form-label form-label-sm mb-1">Umwandlung</label>' +
                  '<select class="form-select form-select-sm" data-map-transform="' + esc(col) + '">' +
                    ['NONE','TRIM','UPPERCASE','LOWERCASE','TO_INTEGER','TO_BOOLEAN','DATETIME_ISO','STATIC'].map((fn) =>
                      '<option value="' + fn + '"' + (transformFunction === fn ? ' selected' : '') + '>' + fn + '</option>'
                    ).join('') +
                  '</select>' +
                '</div>' +
              '</div>' +
              '<details class="migration-mapping-details"' + (detailsOpen ? ' open' : '') + '>' +
                '<summary>Details</summary>' +
                '<div class="migration-mapping-detail-grid">' +
                  '<div>' +
                    '<label class="form-label form-label-sm mb-1">Statischer Wert</label>' +
                    '<input class="form-control form-control-sm" placeholder="Nur bei STATIC" value="' + esc(transformExpression) + '" data-map-transform-expression="' + esc(col) + '"' + (isStatic ? '' : ' style="display:none"') + ' />' +
                  '</div>' +
                  '<div class="migration-mapping-lookup-box">' +
                    '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-map-lookup-enabled="' + esc(col) + '"' + (lookupEnabled ? ' checked' : '') + '><label class="form-check-label small">Lookup aktivieren</label></div>' +
                    '<div class="small text-secondary mb-2">Nur External-ID-Felder sind auswählbar.</div>' +
                    '<div class="migration-mapping-detail-grid migration-mapping-detail-grid-compact">' +
                      '<select class="form-select form-select-sm" data-map-lookup-object="' + esc(col) + '">' + lookupObjOptions + '</select>' +
                      '<select class="form-select form-select-sm" data-map-lookup-field="' + esc(col) + '">' +
                        '<option value="">- Feld wählen -</option>' +
                        (lookupField ? '<option value="' + esc(lookupField) + '" selected>' + esc(lookupField) + '</option>' : '') +
                      '</select>' +
                    '</div>' +
                    '<div class="small text-warning mt-1 d-none" data-map-lookup-status="' + esc(col) + '"></div>' +
                  '</div>' +
                  '<div>' +
                    '<div class="d-flex justify-content-between align-items-center mb-1">' +
                      '<label class="form-label form-label-sm mb-0">Picklist-Mapping</label>' +
                      '<button type="button" class="btn btn-sm btn-outline-secondary" data-map-picklist-add="' + esc(col) + '">Eintrag hinzufügen</button>' +
                    '</div>' +
                    '<div class="table-responsive"><table class="table table-sm migration-picklist-table mb-0"><thead><tr><th>Quelle</th><th>Ziel</th><th></th></tr></thead><tbody data-map-picklist-table="' + esc(col) + '">' + renderMigPicklistMappingRows(picklistMappings, col) + '</tbody></table></div>' +
                  '</div>' +
                  '<div class="scheduler-email-options' + (isEmailTarget ? '' : ' d-none') + '" data-map-email-options="' + esc(col) + '" style="border-top: 1px solid #dee2e6; padding-top: 0.75rem; margin-top: 0.75rem;">' +
                    '<label class="form-label form-label-sm mb-2" style="font-weight: 600;">E-Mail-Validierung</label>' +
                    '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-map-email-enabled="' + esc(col) + '"' + (emailValidationEnabled ? ' checked' : '') + '><label class="form-check-label small">E-Mail-Adresse validieren</label></div>' +
                    '<div class="ps-3">' +
                      '<label class="form-label form-label-sm mb-2">Bei ungültiger E-Mail:</label>' +
                      '<select class="form-select form-select-sm" data-map-email-action="' + esc(col) + '">' +
                        '<option value="EMPTY"' + (emailInvalidAction === 'EMPTY' ? ' selected' : '') + '>→ Leer übermitteln</option>' +
                        '<option value="ERROR"' + (emailInvalidAction === 'ERROR' ? ' selected' : '') + '>→ Datensatz als fehlerhaft kennzeichnen</option>' +
                      '</select>' +
                    '</div>' +
                  '</div>' +
                '</div>' +
              '</details>' +
            '</section>';
          }).join('') +
          '</div>' +
          renderMigPreviewTable(obj);

        const updateMappingEntry = (col) => {
          const objId = objectId;
          const target = migState.objects.find((o) => o.id === objId);
          if (!target) return;
          if (!target.fieldMappings) target.fieldMappings = [];
          const idx = target.fieldMappings.findIndex((m) => m.sourceColumn === col);
          const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
          const fieldInput = panel.querySelector('[data-map-col="' + col + '"]');
          const transformSel = panel.querySelector('[data-map-transform="' + col + '"]');
          const transformExprEl = panel.querySelector('[data-map-transform-expression="' + col + '"]');
          const lookupEnabledEl = panel.querySelector('[data-map-lookup-enabled="' + col + '"]');
          const lookupObjectEl = panel.querySelector('[data-map-lookup-object="' + col + '"]');
          const lookupFieldEl = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          const picklistRows = Array.from(panel.querySelectorAll('[data-map-picklist-row="' + col + '"]'));
          const emailEnabledEl = panel.querySelector('[data-map-email-enabled="' + col + '"]');
          const emailActionEl = panel.querySelector('[data-map-email-action="' + col + '"]');

          const usesManualTarget = String(targetSelectEl?.value || '') === '__manual__';
          const rawSelectedFieldName = usesManualTarget
            ? String(fieldInput?.value || '').trim()
            : String(targetSelectEl?.value || '').trim();
          if (!rawSelectedFieldName) {
            if (idx >= 0) target.fieldMappings.splice(idx, 1);
            const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
            if (typeBadge) typeBadge.textContent = '';
            const row = fieldInput?.closest ? fieldInput.closest('[data-mig-map-row]') : null;
            const statusBadge = row?.querySelector ? row.querySelector('.migration-mapping-source .badge') : null;
            if (row) {
              row.setAttribute('data-mig-map-status', 'open');
              row.setAttribute('data-mig-map-search', String(col || '').toLowerCase());
            }
            if (statusBadge) {
              statusBadge.className = 'badge text-bg-light';
              statusBadge.textContent = 'Offen';
            }
            renderMigMissingFields();
            return;
          }

          const selectedFieldName = resolveMigTargetFieldApiName(rawSelectedFieldName, sfFields);
          const sfField = (sfFields || []).find((f) => String(f.name || '').toLowerCase() === selectedFieldName.toLowerCase());
          if (fieldInput && fieldInput.value !== selectedFieldName) {
            fieldInput.value = selectedFieldName;
          }
          const lookupAllowedForTarget = String(sfField?.type || '').toLowerCase() === 'reference' || String(sfField?.type || '').toLowerCase() === 'id';
          if (!lookupAllowedForTarget && lookupEnabledEl?.checked) {
            lookupEnabledEl.checked = false;
          }
          const picklistMappings = picklistRows.map((row) => ({
            source: String(row.querySelector('[data-map-picklist-source="' + col + '"]')?.value || '').trim(),
            target: String(row.querySelector('[data-map-picklist-target="' + col + '"]')?.value || '').trim()
          })).filter((entry) => entry.source || entry.target);
          const current = idx >= 0 ? target.fieldMappings[idx] : { sourceColumn: col };
          const nextEntry = {
            ...current,
            sourceColumn: col,
            targetField: selectedFieldName,
            targetFieldLabel: sfField?.label || selectedFieldName,
            targetFieldType: sfField?.type,
            transformFunction: String(transformSel?.value || 'NONE'),
            transformExpression: String(transformExprEl?.value || '').trim(),
            lookupEnabled: lookupAllowedForTarget && Boolean(lookupEnabledEl?.checked),
            lookupObject: String(lookupObjectEl?.value || '').trim(),
            lookupField: String(lookupFieldEl?.value || '').trim(),
            picklistMappings,
            _isMissing: !sfField,
            emailValidation: ((String(sfField?.type || '').trim().toLowerCase() === 'email' || normalizeFieldKey(selectedFieldName).includes('email')) && emailEnabledEl && emailEnabledEl.checked)
              ? { enabled: true, invalidAction: String(emailActionEl?.value || 'EMPTY') }
              : undefined
          };

          if (idx >= 0) target.fieldMappings[idx] = nextEntry;
          else target.fieldMappings.push(nextEntry);

          const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
          if (typeBadge) {
            typeBadge.textContent = sfField ? String(sfField.type || '') : (selectedFieldName ? 'neu' : '');
          }
          const row = fieldInput?.closest ? fieldInput.closest('[data-mig-map-row]') : null;
          const statusBadge = row?.querySelector ? row.querySelector('.migration-mapping-source .badge') : null;
          const emailOptions = row?.querySelector ? row.querySelector('[data-map-email-options="' + col + '"]') : null;
          if (row) {
            row.setAttribute('data-mig-map-status', sfField ? 'mapped' : 'new');
            row.setAttribute('data-mig-map-search', [col, selectedFieldName, sfField?.type || 'neu', transformSel?.value || '', (emailEnabledEl && emailEnabledEl.checked ? 'email' : '')].join(' ').toLowerCase());
          }
          if (statusBadge) {
            statusBadge.className = 'badge ' + (sfField ? 'text-bg-success' : 'text-bg-warning');
            statusBadge.textContent = sfField ? 'Gemappt' : 'Neues Feld';
          }
          if (emailOptions) {
            const isEmailTarget = String(sfField?.type || '').trim().toLowerCase() === 'email' || normalizeFieldKey(selectedFieldName).includes('email');
            emailOptions.classList.toggle('d-none', !isEmailTarget);
            if (!isEmailTarget && emailEnabledEl) {
              emailEnabledEl.checked = false;
            }
          }

          renderMigMissingFields();
        };

        const addMigPicklistEntry = (col) => {
          const target = migState.objects.find((o) => o.id === objectId);
          if (!target) return;
          if (!target.fieldMappings) target.fieldMappings = [];
          let mapping = target.fieldMappings.find((entry) => entry.sourceColumn === col);
          if (!mapping) {
            const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
            const fieldInput = panel.querySelector('[data-map-col="' + col + '"]');
            const nextTargetField = String(targetSelectEl?.value || '') === '__manual__'
              ? String(fieldInput?.value || '').trim()
              : String(targetSelectEl?.value || '').trim();
            if (!nextTargetField) {
              return;
            }
            mapping = {
              sourceColumn: col,
              targetField: nextTargetField,
              picklistMappings: []
            };
            target.fieldMappings.push(mapping);
          }
          if (!Array.isArray(mapping.picklistMappings)) {
            mapping.picklistMappings = [];
          }
          mapping.picklistMappings.push({ source: '', target: '' });
          renderMigMappingPanel();
        };

        const deleteMigPicklistEntry = (col, index) => {
          const target = migState.objects.find((o) => o.id === objectId);
          const mapping = target?.fieldMappings?.find((entry) => entry.sourceColumn === col);
          if (!mapping || !Array.isArray(mapping.picklistMappings)) {
            return;
          }
          mapping.picklistMappings.splice(index, 1);
          renderMigMappingPanel();
        };

        const setLookupValidationState = (col, message) => {
          const fieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          const statusEl = panel.querySelector('[data-map-lookup-status="' + col + '"]');
          if (fieldSel) {
            fieldSel.classList.toggle('is-invalid', Boolean(message));
          }
          if (statusEl) {
            statusEl.textContent = message || '';
            statusEl.classList.toggle('d-none', !message);
          }
        };

        // Helper: load lookup fields for a column's lookup-field <select>
        const loadLookupFields = async (col, selectedObject) => {
          const fieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          if (!fieldSel) return;
          if (!selectedObject) {
            const cur = fieldSel.value;
            fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' + (cur ? '<option value="' + esc(cur) + '" selected>' + esc(cur) + '</option>' : '');
            setLookupValidationState(col, '');
            return;
          }
          try {
            const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(selectedObject) + '&instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''));
            if (!res.ok) return;
            const fields = await res.json();
            const externalIdFields = (fields || []).filter((f) => f && f.isExternalId === true);
            const curVal = fieldSel.value;
            fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' +
              externalIdFields.map((f) => '<option value="' + esc(f.name) + '"' + (f.name === curVal ? ' selected' : '') + '>' + esc(f.label && f.label !== f.name ? f.label + ' (' + f.name + ')' : f.name) + '</option>').join('');
            if (curVal && !externalIdFields.some((f) => f.name === curVal)) {
              fieldSel.value = '';
              setLookupValidationState(col, 'Gespeichertes Lookup-Feld ist keine External ID mehr und wurde geleert.');
              return;
            }
            setLookupValidationState(col, '');
          } catch { /* ignore */ }
        };

        const filterInput = panel.querySelector('[data-mig-map-filter]');
        if (filterInput) {
          filterInput.addEventListener('input', () => {
            const term = String(filterInput.value || '').trim().toLowerCase();
            panel.querySelectorAll('[data-mig-map-row]').forEach((row) => {
              const searchText = String(row.getAttribute('data-mig-map-search') || '').toLowerCase();
              row.classList.toggle('d-none', Boolean(term) && !searchText.includes(term));
            });
          });
        }

        obj.fileColumns.forEach((col) => {
          // Salesforce field select / manual input
          const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
          const sfFieldInput = panel.querySelector('[data-map-col="' + col + '"]');
          if (targetSelectEl && sfFieldInput) {
            targetSelectEl.addEventListener('change', () => {
              const showManualInput = targetSelectEl.value === '__manual__';
              sfFieldInput.classList.toggle('d-none', !showManualInput);
              if (!showManualInput) {
                sfFieldInput.value = '';
              }
              updateMappingEntry(col);
            });
          }
          if (sfFieldInput) {
            sfFieldInput.addEventListener('input', () => updateMappingEntry(col));
            sfFieldInput.addEventListener('change', () => updateMappingEntry(col));
          }

          // Transform select → show/hide expression input + update
          const transformEl = panel.querySelector('[data-map-transform="' + col + '"]');
          const exprEl = panel.querySelector('[data-map-transform-expression="' + col + '"]');
          if (transformEl) {
            transformEl.addEventListener('change', () => {
              if (exprEl) exprEl.style.display = transformEl.value === 'STATIC' ? '' : 'none';
              updateMappingEntry(col);
            });
          }
          // Transform expression input
          if (exprEl) {
            exprEl.addEventListener('input', () => updateMappingEntry(col));
            exprEl.addEventListener('change', () => updateMappingEntry(col));
          }

          // Lookup enabled checkbox
          const lookupEnabledEl = panel.querySelector('[data-map-lookup-enabled="' + col + '"]');
          if (lookupEnabledEl) {
            lookupEnabledEl.addEventListener('change', () => updateMappingEntry(col));
          }

          // Lookup object select → reload lookup fields, then update
          const lookupObjSel = panel.querySelector('[data-map-lookup-object="' + col + '"]');
          if (lookupObjSel) {
            lookupObjSel.addEventListener('change', async () => {
              await loadLookupFields(col, lookupObjSel.value);
              updateMappingEntry(col);
            });
          }

          // Email validation controls
          if (emailEnabledEl && emailActionEl) {
            emailEnabledEl.addEventListener('change', () => {
              emailActionEl.style.display = emailEnabledEl.checked ? '' : 'none';
              updateMappingEntry(col);
            });
            emailActionEl.addEventListener('change', () => updateMappingEntry(col));
          }

          // Lookup field select
          const lookupFieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          if (lookupFieldSel) {
            lookupFieldSel.addEventListener('change', () => {
              setLookupValidationState(col, '');
              updateMappingEntry(col);
            });
          }

          // Picklist table editor
          panel.querySelector('[data-map-picklist-add="' + col + '"]')?.addEventListener('click', () => addMigPicklistEntry(col));
          panel.querySelectorAll('[data-map-picklist-delete="' + col + '"]').forEach((button) => {
            button.addEventListener('click', () => {
              const index = Number(button.getAttribute('data-map-picklist-index') || '-1');
              if (index >= 0) {
                deleteMigPicklistEntry(col, index);
              }
            });
          });
          panel.querySelectorAll('[data-map-picklist-source="' + col + '"], [data-map-picklist-target="' + col + '"]').forEach((input) => {
            input.addEventListener('input', () => updateMappingEntry(col));
            input.addEventListener('change', () => updateMappingEntry(col));
          });

          // Pre-load lookup fields for rows that already have a lookup object set
          const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
          if (existing?.lookupObject) {
            loadLookupFields(col, existing.lookupObject);
          }
        });

        const assistantApplyButton = document.getElementById('mig-mapping-assistant-apply');
        assistantApplyButton?.addEventListener('click', async () => {
          const selectedProfile = String(document.getElementById('mig-mapping-assistant-profile')?.value || getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject)).trim();
          migState.mappingAssistantProfilesByObjectId[obj.id] = selectedProfile;
          const autoMapped = await autoPopulateMigFieldMappings(obj, sfFields);
          const externalIdUpdated = autoSelectMigExternalIdField(obj, sfFields);
          if (autoMapped > 0 || externalIdUpdated) {
            await migSave();
          }
          await renderMigMappingPanel();
        });

        panel.querySelectorAll('[data-preview-prev], [data-preview-next]').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const isNext = btn.hasAttribute('data-preview-next');
            const pageSize = Math.max(1, Number(obj.previewLimit || 10) || 10);
            const currentOffset = Math.max(0, Number(obj.previewOffset || 0) || 0);
            const nextOffset = isNext ? currentOffset + pageSize : Math.max(0, currentOffset - pageSize);
            btn.disabled = true;
            try {
              await loadMigObjectPreview(obj, nextOffset, pageSize);
              renderMigMappingPanel();
            } catch (err) {
              alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
              btn.disabled = false;
            }
          });
        });

        panel.querySelectorAll('[data-preview-filter]').forEach((input) => {
          input.addEventListener('input', () => {
            obj.previewFilter = input.value || '';
            if (!isMigServerPreview(obj)) {
              renderMigMappingPanel();
              return;
            }

            window.clearTimeout(obj._previewFilterTimer || 0);
            obj._previewFilterTimer = window.setTimeout(async () => {
              try {
                await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
                renderMigMappingPanel();
              } catch (err) {
                alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
              }
            }, 250);
          });
        });

        panel.querySelectorAll('[data-preview-status-filter]').forEach((input) => {
          input.addEventListener('change', async () => {
            obj.previewStatusFilter = input.value || '';
            try {
              await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
              renderMigMappingPanel();
            } catch (err) {
              alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
            }
          });
        });

        renderMigMissingFields();
      }

      function renderMigMissingFields() {
        const container = document.getElementById('mig-missing-fields-list');
        if (!container) return;
        const missing = collectMigMissingFieldMappings();
        if (!missing.length) {
          container.innerHTML = '<div class="alert alert-success">Alle gemappten Felder existieren in Salesforce – keine Aktion erforderlich.</div>';
          return;
        }
        const buildPicklistText = (values) => (Array.isArray(values) ? values : []).map((value) => String(value || '').trim()).filter(Boolean).join('\\n');
        const countPicklistValues = (value) => String(value || '').split(/\\r?\\n/).map((entry) => entry.trim()).filter(Boolean).length;
        container.innerHTML = '<table class="table table-sm"><thead><tr><th>Objekt</th><th>SF-Feld</th><th>Typ</th><th>Aktion</th></tr></thead><tbody>' +
          missing.map((item) =>
            '<tr><td>' + esc(item.obj.salesforceObject) + '</td>' +
            '<td><code>' + esc(item.mapping.targetField) + '</code></td>' +
            '<td>' +
              '<select class="form-select form-select-sm" data-field-type="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' +
              ['Text', 'Number', 'Date', 'DateTime', 'Checkbox', 'Currency', 'Percent', 'Email', 'Phone', 'Url', 'Picklist'].map((t) => '<option>' + t + '</option>').join('') +
              '</select>' +
              '<div class="mt-2 d-none" data-picklist-config="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' +
                '<div class="d-flex gap-2 align-items-center mb-2">' +
                  '<button class="btn btn-sm btn-outline-secondary" type="button" data-picklist-autofill="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">AutoFill</button>' +
                  '<span class="small text-secondary">Liest alle Varianten aus der Quellspalte.</span>' +
                  '<span class="badge text-bg-light" data-picklist-count="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' + countPicklistValues(buildPicklistText(item.mapping.picklistValues)) + ' Werte</span>' +
                '</div>' +
                '<textarea class="form-control form-control-sm" rows="5" placeholder="Ein Wert pro Zeile" data-picklist-values="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' + esc(buildPicklistText(item.mapping.picklistValues)) + '</textarea>' +
                '<div class="small text-secondary mt-1">Jede Zeile wird als Picklist-Wert angelegt.</div>' +
              '</div>' +
            '</td>' +
            '<td><button class="btn btn-sm btn-outline-primary" data-create-field-obj="' + esc(item.obj.id) + '" data-create-field-name="' + esc(item.mapping.targetField) + '" data-create-source-column="' + esc(item.mapping.sourceColumn || '') + '">Anlegen</button></td></tr>'
          ).join('') + '</tbody></table>';

        const updatePicklistValueState = (typeKey) => {
          const textarea = container.querySelector('[data-picklist-values="' + typeKey + '"]');
          const countEl = container.querySelector('[data-picklist-count="' + typeKey + '"]');
          const values = String(textarea?.value || '').split(/\\r?\\n/).map((value) => value.trim()).filter(Boolean);
          const createBtn = container.querySelector('[data-create-field-obj][data-field-type-key="' + typeKey + '"]');
          const fieldName = createBtn ? createBtn.getAttribute('data-create-field-name') : '';
          const objectId = createBtn ? createBtn.getAttribute('data-create-field-obj') : '';
          const obj = (migState.objects || []).find((entry) => entry.id === objectId);
          const mapping = obj ? (obj.fieldMappings || []).find((entry) => entry.targetField === fieldName) : null;
          if (mapping) {
            mapping.picklistValues = values;
          }
          if (countEl) {
            countEl.textContent = values.length + ' Werte';
          }
        };

        const togglePicklistConfig = (typeKey) => {
          const typeSelect = container.querySelector('[data-field-type="' + typeKey + '"]');
          const configPanel = container.querySelector('[data-picklist-config="' + typeKey + '"]');
          if (!typeSelect || !configPanel) return;
          configPanel.classList.toggle('d-none', typeSelect.value !== 'Picklist');
        };

        container.querySelectorAll('[data-field-type]').forEach((select) => {
          const typeKey = select.getAttribute('data-field-type');
          togglePicklistConfig(typeKey);
          select.addEventListener('change', () => togglePicklistConfig(typeKey));
        });

        container.querySelectorAll('[data-picklist-autofill]').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const typeKey = btn.getAttribute('data-picklist-autofill');
            const createBtn = container.querySelector('[data-create-field-obj][data-create-field-name][data-field-type-key="' + typeKey + '"]');
            const objectId = createBtn ? createBtn.getAttribute('data-create-field-obj') : null;
            const sourceColumn = createBtn ? createBtn.getAttribute('data-create-source-column') : null;
            const textarea = container.querySelector('[data-picklist-values="' + typeKey + '"]');
            if (!objectId || !sourceColumn || !textarea) return;
            btn.disabled = true;
            const originalText = btn.textContent;
            btn.textContent = '…';
            try {
              const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/objects/' + encodeURIComponent(objectId) + '/distinct-values?column=' + encodeURIComponent(sourceColumn));
              const result = await res.json();
              if (!res.ok) throw new Error(result.error || 'Fehler');
              const values = Array.isArray(result.values) ? result.values : [];
              textarea.value = values.join('\\n');
              updatePicklistValueState(typeKey);
            } catch (err) {
              alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
            } finally {
              btn.disabled = false;
              btn.textContent = originalText || 'AutoFill';
            }
          });
        });

        container.querySelectorAll('[data-picklist-values]').forEach((textarea) => {
          const typeKey = textarea.getAttribute('data-picklist-values');
          updatePicklistValueState(typeKey);
          textarea.addEventListener('input', () => updatePicklistValueState(typeKey));
        });

        container.querySelectorAll('[data-create-field-obj]').forEach((btn) => {
          const typeKey = btn.getAttribute('data-create-field-name') + '-' + btn.getAttribute('data-create-field-obj');
          btn.setAttribute('data-field-type-key', typeKey);
          btn.addEventListener('click', async () => {
            const objId = btn.getAttribute('data-create-field-obj');
            const fieldName = btn.getAttribute('data-create-field-name');
            const typeKey = fieldName + '-' + objId;
            const typeSelect = container.querySelector('[data-field-type="' + typeKey + '"]');
            const fieldType = typeSelect ? typeSelect.value : inferMigFieldCreationType({ targetField: fieldName });
            const picklistValuesEl = container.querySelector('[data-picklist-values="' + typeKey + '"]');
            const obj = migState.objects.find((o) => o.id === objId);
            if (!obj) return;
            const mapping = (obj.fieldMappings || []).find((m) => m.targetField === fieldName);
            if (!mapping) return;
            const picklistValues = fieldType === 'Picklist'
              ? String(picklistValuesEl?.value || '').split(/\\r?\\n/).map((value) => value.trim()).filter(Boolean)
              : [];
            if (fieldType === 'Picklist' && !picklistValues.length) {
              alert('Bitte zuerst Picklist-Werte eintragen oder per AutoFill laden.');
              return;
            }
            btn.disabled = true; btn.textContent = '…';
            try {
              mapping.picklistValues = picklistValues;
              const created = await createMigMissingField(obj, mapping, fieldType, picklistValues);

              btn.className = 'btn btn-sm btn-success'; btn.textContent = '✓ Angelegt';
              const resultDiv = document.getElementById('mig-create-fields-result');
              if (resultDiv) resultDiv.innerHTML += '<div class="alert alert-success py-1 small mt-1">' + esc(obj.salesforceObject + '.' + created.fullFieldName) + (created.result && created.result.action === 'exists' ? ' existiert bereits.' : ' erfolgreich angelegt.') + '</div>';
              renderMigMissingFields();
            } catch (err) {
              btn.className = 'btn btn-sm btn-danger'; btn.textContent = 'Fehler';
              alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
            }
          });
        });
      }

      async function migSave() {
        const nameEl = document.getElementById('mig-name');
        const descEl = document.getElementById('mig-description');
        const batchSizeEl = document.getElementById('mig-batch-size');
        if (nameEl) migState.name = nameEl.value.trim() || migState.name;
        if (descEl) migState.description = descEl.value.trim();
        if (batchSizeEl) {
          const parsedBatchSize = Number(batchSizeEl.value || migState.batchSize || 200);
          migState.batchSize = Number.isFinite(parsedBatchSize)
            ? Math.max(1, Math.min(200, Math.trunc(parsedBatchSize)))
            : 200;
          batchSizeEl.value = String(migState.batchSize);
        }
        syncMigSalesforceLoginFromForm();
        if (migState.salesforceLogin) {
          migState.salesforceLogin.name = migState.name || migState.salesforceLogin.name;
          migState.salesforceLogin.id = migState.id || migState.salesforceLogin.id || '';
        }

        migState.objects = sanitizeMigObjects(migState.objects);

        const payload = {
          id: migState.id,
          name: migState.name,
          description: migState.description,
          batchSize: migState.batchSize || 200,
          instanceId: migState.salesforceLogin ? undefined : (migState.instanceId || state.instanceId || undefined),
          salesforceLogin: migState.salesforceLogin || null,
          status: 'draft',
          objects: sanitizeMigObjects(migState.objects),
          dependencies: migState.dependencies,
          executionPlan: migState.executionPlan
        };
        const method = migState.id ? 'PUT' : 'POST';
        const url = migState.id ? '/api/migrations/' + encodeURIComponent(migState.id) : '/api/migrations';
        const res = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const saved = await res.json();
        if (!migState.id) migState.id = saved.id;
        migState.status = String(saved.status || migState.status || 'draft');
        return saved;
      }

      function openMigWizard(migration, options) {
        clearMigrationModalError();
        const requestedStep = Number(options && options.startStep ? options.startStep : 1);
        const hasLastRunResult = !!(migration && migration.lastRunResult && (
          (Array.isArray(migration.lastRunResult.steps) && migration.lastRunResult.steps.length) ||
          String(migration.lastRunResult.reportPath || '').trim()
        ));
        migState.id = migration ? migration.id : null;
        migState.step = requestedStep >= 1 && requestedStep <= migState.totalSteps ? requestedStep : 1;
        migState.status = migration ? String(migration.status || 'draft') : 'draft';
        migState.activeRunVisible = migState.status === 'running' || hasLastRunResult || !!(options && options.showRunSummary);
        migState.name = migration ? migration.name : (options && options.name ? options.name : '');
        migState.description = migration ? (migration.description || '') : (options && options.description ? options.description : '');
        migState.batchSize = migration ? Number(migration.batchSize || 200) : 200;
        if (!Number.isFinite(migState.batchSize) || migState.batchSize <= 0) {
          migState.batchSize = 200;
        }
        migState.batchSize = Math.max(1, Math.min(200, Math.trunc(migState.batchSize)));
        migState.instanceId = migration ? String(migration.instanceId || '') : String(state.instanceId || '');
        migState.salesforceLogin = migration ? JSON.parse(JSON.stringify(migration.salesforceLogin || null)) : null;
        migState.objects = migration ? sanitizeMigObjects(migration.objects || []) : [];
        migState.dependencies = migration ? JSON.parse(JSON.stringify(migration.dependencies || [])) : [];
        migState.executionPlan = migration ? JSON.parse(JSON.stringify(migration.executionPlan || [])) : [];
        migState.sfObjects = [];
        migState.lastRunResult = migration ? JSON.parse(JSON.stringify(migration.lastRunResult || null)) : null;
        migState.runHistory = migration ? JSON.parse(JSON.stringify(migration.runHistory || [])) : [];
        migState.preflightWarnings = null;
        migState.preflightWarningsLoading = false;
        migState.pendingImports = options && Array.isArray(options.pendingImports)
          ? options.pendingImports.slice()
          : [];
        migState.pendingImportInProgress = false;
        migState.pendingImportSuggestions = options && Array.isArray(options.pendingImportSuggestions)
          ? options.pendingImportSuggestions.slice()
          : [];
        migState.pendingImportAnalysis = options && options.pendingImportAnalysis
          ? options.pendingImportAnalysis
          : null;

        const nameEl = document.getElementById('mig-name');
        const descEl = document.getElementById('mig-description');
        const batchSizeEl = document.getElementById('mig-batch-size');
        const instanceSourceEl = document.getElementById('mig-instance-source');
        const existingInstanceEl = document.getElementById('mig-existing-instance');
        const loginEnvironmentEl = document.getElementById('mig-login-environment');
        const loginAuthTypeEl = document.getElementById('mig-login-auth-type');
        const loginUrlEl = document.getElementById('mig-login-url');
        const loginUsernameEl = document.getElementById('mig-login-username');
        const loginPasswordEl = document.getElementById('mig-login-password');
        const loginSecurityTokenEl = document.getElementById('mig-login-security-token');
        const loginClientIdEl = document.getElementById('mig-login-client-id');
        const loginClientSecretEl = document.getElementById('mig-login-client-secret');
        if (nameEl) nameEl.value = migState.name;
        if (descEl) descEl.value = migState.description;
        if (batchSizeEl) batchSizeEl.value = String(migState.batchSize || 200);
        if (instanceSourceEl) instanceSourceEl.value = migration ? (migration.salesforceLogin ? 'embedded' : 'existing') : 'embedded';
        populateMigExistingInstanceOptions();
        if (existingInstanceEl) existingInstanceEl.value = String(migState.instanceId || state.instanceId || existingInstanceEl.value || '');
        if (loginEnvironmentEl) loginEnvironmentEl.value = String(migState.salesforceLogin && migState.salesforceLogin.environment || 'sandbox');
        if (loginAuthTypeEl) loginAuthTypeEl.value = String(migState.salesforceLogin && migState.salesforceLogin.authType || 'password');
        if (loginUrlEl) loginUrlEl.value = String(migState.salesforceLogin && migState.salesforceLogin.loginUrl || getMigLoginUrlForEnvironment(migState.salesforceLogin && migState.salesforceLogin.environment));
        if (loginUsernameEl) loginUsernameEl.value = String(migState.salesforceLogin && migState.salesforceLogin.username || '');
        if (loginPasswordEl) loginPasswordEl.value = String(migState.salesforceLogin && migState.salesforceLogin.password || '');
        if (loginSecurityTokenEl) loginSecurityTokenEl.value = String(migState.salesforceLogin && migState.salesforceLogin.securityToken || '');
        if (loginClientIdEl) loginClientIdEl.value = String(migState.salesforceLogin && migState.salesforceLogin.clientId || '');
        if (loginClientSecretEl) loginClientSecretEl.value = String(migState.salesforceLogin && migState.salesforceLogin.clientSecret || '');
        renderMigSalesforceLoginStatus();

        resetMigTransientUi();

        renderMigWizardSteps();
        renderMigSelectedObjects();
        renderMigPendingImportHint();
        renderMigImportSuggestions();
        if (migState.step === 2) renderMigFileAssignments();
        if (migState.step === 3) renderMigMappingObjectSelect();
        if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
        if (migState.step === 5) renderMigOrderList();
        if (migState.step === 6) renderMigMissingFields();
        if (migState.step === 7) renderMigReview();
        void loadEntityHistory('migration', migState.id || '', 'mig-history-list', 'mig-history-meta', 'Migration noch nicht gespeichert.');

        const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('migration-modal'));
        document.getElementById('migration-modal-title').textContent = migration ? 'Migration bearbeiten: ' + migration.name : 'Neue Migration';
        modal.show();
      }

${renderMigrationUiModule()}
${renderMigrationPlanningModule()}
${renderConnectorUiModule()}
${renderSchedulerUiModule()}
      ${renderMigrationPreflightModule()}
${renderMigrationProgressModule()}
${renderMigrationFailedRecordsModule()}
${renderMigrationRunResultModule()}

      document.getElementById('mig-wizard-prev')?.addEventListener('click', () => {
        if (migState.step <= 1) return;
        migState.step--;
        renderMigWizardSteps();
        if (migState.step === 2) renderMigFileAssignments();
        if (migState.step === 3) renderMigMappingObjectSelect();
        if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
        if (migState.step === 5) renderMigOrderList();
        if (migState.step === 6) renderMigMissingFields();
        if (migState.step === 7) renderMigReview();
      });

      document.getElementById('mig-wizard-next')?.addEventListener('click', async () => {
        if (migState.step === migState.totalSteps) {
          // Execute migration
          const progressEl = document.getElementById('mig-run-progress');
          const resultEl = document.getElementById('mig-run-result');
          const nextBtn = document.getElementById('mig-wizard-next');
          const prevBtn = document.getElementById('mig-wizard-prev');
          const createFieldsResultEl = document.getElementById('mig-create-fields-result');
          nextBtn.disabled = true; prevBtn.disabled = true;
          progressEl.classList.remove('d-none');
          resultEl.classList.add('d-none');
          try {
            const autoCreatedFields = await autoCreateMigMissingFields();
            if (createFieldsResultEl) {
              createFieldsResultEl.innerHTML = autoCreatedFields.length
                ? autoCreatedFields.map((item) => '<div class="alert alert-success py-1 small mt-1">' + esc(item.objectApiName + '.' + item.fieldName) + (item.action === 'exists' ? ' existiert bereits.' : ' automatisch angelegt.') + '</div>').join('')
                : '';
            }
            await migSave();
            await loadMigPreflightWarnings(true);
            migState.status = 'running';
            migState.activeRunVisible = true;
            const orderedObjects = getMigOrderedObjects();
            migState.lastRunResult = {
              startedAt: new Date().toISOString(),
              steps: orderedObjects.map((obj, index) => ({
                objectId: obj.id,
                salesforceObject: obj.salesforceObject,
                status: index === 0 ? 'running' : 'pending',
                recordsProcessed: index === 0 ? Math.max(0, Number(obj.fileRecordCount || 0) || 0) : 0,
                recordsSucceeded: 0,
                recordsFailed: 0
              }))
            };
            renderMigRunProgress();
            pollMigRunProgress();
            const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/run', { method: 'POST' });
            const result = await res.json();
            if (!res.ok) {
              throw new Error(result.error || 'Migration konnte nicht gestartet werden');
            }
            if (result && result.accepted) {
              migState.status = 'running';
              migState.activeRunVisible = true;
              if (result.lastRunResult) {
                migState.lastRunResult = result.lastRunResult;
              }
              renderMigRunProgress();
              pollMigRunProgress();
              resultEl.classList.remove('d-none');
              resultEl.innerHTML = '<div class="alert alert-info">Migration wurde gestartet. Fortschritt wird automatisch aktualisiert.</div>';
              return;
            }
            migState.lastRunResult = result;
            migState.status = result.steps.every((s) => s.status !== 'error') ? 'done' : 'error';
            migState.activeRunVisible = true;
            stopMigRunProgressPolling();
            renderMigRunProgress();
            resultEl.classList.remove('d-none');
            const allOk = result.steps.every((s) => s.status !== 'error');
            resultEl.innerHTML = '<div class="alert ' + (allOk ? 'alert-success' : 'alert-warning') + '">' +
              (allOk ? '✓ Migration erfolgreich abgeschlossen.' : '⚠ Migration mit Fehlern abgeschlossen.') +
              '</div>' +
              (result.reportPath ? '<div class="alert alert-info py-2 small">Protokoll erzeugt: <a href="' + esc(getMigrationReportUrl(migState.id, true)) + '">Datei öffnen</a><div class="text-secondary mt-1"><code>' + esc(result.reportPath) + '</code></div></div>' : '') +
              '<table class="table table-sm"><thead><tr><th>Objekt</th><th>Verarbeitet</th><th>OK</th><th>Fehler</th><th>Status</th></tr></thead><tbody>' +
              (result.steps || []).map((s) =>
                '<tr><td>' + esc(s.salesforceObject) + '</td><td>' + (s.recordsProcessed || 0) +
                '</td><td>' + (s.recordsSucceeded || 0) + '</td><td>' + (s.recordsFailed || 0) +
                '</td><td><span class="badge bg-' + (s.status === 'done' ? 'success' : 'danger') + '">' + esc(s.status) + '</span>' +
                (s.errorMessage ? '<div class="text-danger small">' + esc(s.errorMessage) + '</div>' : '') +
                '</td></tr>'
              ).join('') + '</tbody></table>';
            const failedSteps = (result.steps || []).filter((s) => s.failedRecordsId);
            if (failedSteps.length) {
              resultEl.innerHTML += failedSteps.map((s) => {
                const detailsId = 'mig-errors-' + s.failedRecordsId;
                return '<div class="card mt-3">' +
                  '<div class="card-header d-flex gap-2 align-items-center">' +
                  '<strong class="me-auto">Fehlerhafte Datensätze: ' + esc(s.salesforceObject) + '</strong>' +
                  '<button class="btn btn-sm btn-outline-danger" data-load-failed-records="' + esc(migState.id) + '" data-object-id="' + esc(s.objectId) + '" data-failed-records-id="' + esc(s.failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Details laden</button>' +
                  '</div>' +
                  '<div id="' + esc(detailsId) + '" class="card-body" style="display:none;"></div>' +
                '</div>';
              }).join('');

              const bindLoadFailedDetails = (btn) => {
                btn.addEventListener('click', async () => {
                  const migId = btn.getAttribute('data-load-failed-records');
                  const objectId = btn.getAttribute('data-object-id');
                  const failedRecordsId = btn.getAttribute('data-failed-records-id');
                  const detailsId = btn.getAttribute('data-details-id');
                  const detailsDiv = document.getElementById(detailsId);
                  if (!detailsDiv) return;

                  btn.disabled = true;
                  btn.textContent = 'Lade…';
                  detailsDiv.style.display = '';

                  try {
                    const failedRes = await fetch('/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(failedRecordsId));
                    if (!failedRes.ok) throw new Error('Fehler beim Laden der Fehlerdetails');
                    const failedData = await failedRes.json();
                    const records = Array.isArray(failedData.records) ? failedData.records : [];
                    const migrationObject = (migState.objects || []).find((item) => item && item.id === objectId);
                    const allowStageSave = !!migrationObject && (migrationObject.stagingMode === 'sqlite' || migrationObject.processingMode === 'sqlite');

                    if (!records.length) {
                      detailsDiv.innerHTML = '<div class="alert alert-info">Keine fehlgeschlagenen Datensätze gefunden.</div>';
                      btn.textContent = 'Details laden';
                      return;
                    }

                    detailsDiv.innerHTML =
                      '<div class="d-flex align-items-center gap-2 mb-2 flex-wrap">' +
                        '<button class="btn btn-sm btn-primary" data-retry-failed-records data-mode="all" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Korrigierte Datensätze neu importieren</button>' +
                        '<button class="btn btn-sm btn-outline-primary" data-retry-failed-records data-mode="partial" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Nur erfolgreiche Korrekturen übernehmen</button>' +
                        (allowStageSave
                          ? '<button class="btn btn-sm btn-outline-secondary" data-save-failed-corrections data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '">Korrekturen ins Staging übernehmen</button>'
                          : '') +
                        '<button class="btn btn-sm btn-outline-secondary" data-export-failed-csv>Restfehler als CSV exportieren</button>' +
                        '<span class="small text-secondary" data-retry-status></span>' +
                      '</div>' +
                      '<p class="small text-secondary mb-2">Feldwerte direkt korrigieren und anschließend neu importieren.</p>' +
                      '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr><th>Zeile</th><th>Fehlertyp</th><th>Fehler</th><th>Korrigierbare Feldwerte</th></tr></thead><tbody>' +
                      records.map((rec, idx) => {
                        const sourceObj = rec.sourceRecord || {};
                        const sourceEntries = Object.entries(sourceObj);
                        const previewPairs = sourceEntries.slice(0, 3)
                          .map(([key, value]) => '<span class="badge text-bg-light border me-1 mb-1">' + esc(String(key)) + ': ' + esc(String(value ?? '')) + '</span>')
                          .join('');
                        return '<tr data-failed-row="' + idx + '" data-row-index="' + esc(String(rec.rowIndex || 0)) + '" data-error="' + esc(String(rec.error || '')) + '" data-error-type="' + esc(String(rec.errorType || 'mapping')) + '">' +
                          '<td><strong>' + esc(String(rec.rowIndex)) + '</strong></td>' +
                          '<td><span class="badge bg-' + (rec.errorType === 'salesforce' ? 'warning' : 'danger') + '">' + esc(String(rec.errorType || 'mapping')) + '</span></td>' +
                          '<td class="text-danger small">' + esc(String(rec.error || '')) + '</td>' +
                          '<td>' +
                            '<div class="small text-secondary mb-1">' + sourceEntries.length + ' Felder</div>' +
                            '<div class="mb-1">' + previewPairs + (sourceEntries.length > 3 ? '<span class="small text-secondary">…</span>' : '') + '</div>' +
                            '<details class="border rounded p-2 bg-body-tertiary">' +
                              '<summary class="small" style="cursor:pointer">Felder bearbeiten</summary>' +
                              '<div class="vstack gap-1 mt-2" style="max-height: 260px; overflow:auto;">' +
                                sourceEntries.map(([key, value]) =>
                                  '<div class="input-group input-group-sm">' +
                                    '<span class="input-group-text" style="min-width: 180px">' + esc(String(key)) + '</span>' +
                                    '<input class="form-control" data-retry-field data-field-name="' + esc(String(key)) + '" value="' + esc(String(value ?? '')) + '" />' +
                                  '</div>'
                                ).join('') +
                              '</div>' +
                            '</details>' +
                          '</td>' +
                        '</tr>';
                      }).join('') +
                      '</tbody></table></div>';

                    const retryButtons = Array.from(detailsDiv.querySelectorAll('[data-retry-failed-records]'));
                    const saveCorrectionsBtn = detailsDiv.querySelector('[data-save-failed-corrections]');
                    const exportCsvBtn = detailsDiv.querySelector('[data-export-failed-csv]');
                    const retryStatus = detailsDiv.querySelector('[data-retry-status]');

                    const collectEditedRows = () => {
                      const rows = Array.from(detailsDiv.querySelectorAll('[data-failed-row]'));
                      return rows.map((row) => {
                        const rowIndex = Number(row.getAttribute('data-row-index') || '0');
                        const sourceRecord = {};
                        row.querySelectorAll('[data-retry-field]').forEach((input) => {
                          const key = input.getAttribute('data-field-name') || '';
                          sourceRecord[key] = input.value;
                        });
                        return {
                          rowIndex,
                          error: row.getAttribute('data-error') || '',
                          errorType: row.getAttribute('data-error-type') || 'mapping',
                          sourceRecord
                        };
                      });
                    };

                    const csvEscape = (value) => {
                      const delimiter = ';';
                      const str = String(value ?? '');
                      if (str.includes('"') || str.includes('\\n') || str.includes('\\r') || str.includes(delimiter)) {
                        return '"' + str.replace(/"/g, '""') + '"';
                      }
                      return str;
                    };

                    if (exportCsvBtn) {
                      exportCsvBtn.addEventListener('click', () => {
                        const editedRows = collectEditedRows();
                        if (!editedRows.length) {
                          alert('Keine Restfehler zum Exportieren vorhanden.');
                          return;
                        }
                        const sourceKeys = Array.from(new Set(editedRows.flatMap((row) => Object.keys(row.sourceRecord || {}))));
                        const header = ['rowIndex', 'errorType', 'error', ...sourceKeys];
                        const delimiter = ';';
                        const lines = [header.map(csvEscape).join(delimiter)];
                        editedRows.forEach((row) => {
                          const values = [row.rowIndex, row.errorType, row.error, ...sourceKeys.map((key) => row.sourceRecord[key] ?? '')];
                          lines.push(values.map(csvEscape).join(delimiter));
                        });
                        const bom = '\\uFEFF';
                        const blob = new Blob([bom + lines.join('\\r\\n')], { type: 'text/csv;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = 'restfehler-' + objectId + '-' + failedRecordsId + '.csv';
                        document.body.appendChild(a);
                        a.click();
                        document.body.removeChild(a);
                        URL.revokeObjectURL(url);
                      });
                    }

                    const runRetry = async (mode) => {
                      const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
                      retryButtons.forEach((button) => { button.disabled = true; });
                      if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = true;
                      if (retryStatus) {
                        retryStatus.textContent = mode === 'partial'
                          ? 'Neuimport läuft (nur erfolgreiche Korrekturen werden übernommen)...'
                          : 'Neuimport läuft...';
                      }
                      try {
                        const retryRes = await fetch(
                          '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
                          {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ records: payloadRecords, mode })
                          }
                        );
                        const retryResult = await retryRes.json();
                        if (!retryRes.ok) throw new Error(retryResult.error || 'Retry fehlgeschlagen');

                        if (retryStatus) {
                          retryStatus.textContent = 'Neuimport abgeschlossen: ' + retryResult.recordsSucceeded + ' OK, ' + retryResult.recordsFailed + ' Fehler.';
                        }

                        if (retryResult.failedRecordsId) {
                          btn.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
                          retryButtons.forEach((button) => {
                            button.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
                          });
                          btn.click();
                        }
                      } catch (err) {
                        if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
                      } finally {
                        retryButtons.forEach((button) => { button.disabled = false; });
                        if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = false;
                      }
                    };

                    if (saveCorrectionsBtn) {
                      saveCorrectionsBtn.addEventListener('click', async () => {
                        const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
                        retryButtons.forEach((button) => { button.disabled = true; });
                        saveCorrectionsBtn.disabled = true;
                        if (retryStatus) {
                          retryStatus.textContent = 'Korrekturen werden ins SQLite-Staging übernommen...';
                        }

                        try {
                          const saveRes = await fetch(
                            '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
                            {
                              method: 'POST',
                              headers: { 'Content-Type': 'application/json' },
                              body: JSON.stringify({ records: payloadRecords, mode: 'stage' })
                            }
                          );
                          const saveResult = await saveRes.json();
                          if (!saveRes.ok) throw new Error(saveResult.error || 'Speichern ins Staging fehlgeschlagen');

                          if (retryStatus) {
                            retryStatus.textContent = (saveResult.updatedRows || 0) + ' Zeilen im Staging aktualisiert.';
                          }

                          if (migrationObject) {
                            migrationObject.statusSummary = saveResult.statusSummary || migrationObject.statusSummary || {};
                            await loadMigObjectPreview(migrationObject, migrationObject.previewOffset || 0, migrationObject.previewLimit || 10);
                            renderMigMappingPanel();
                          }
                        } catch (err) {
                          if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
                        } finally {
                          retryButtons.forEach((button) => { button.disabled = false; });
                          saveCorrectionsBtn.disabled = false;
                        }
                      });
                    }

                    retryButtons.forEach((button) => {
                      button.addEventListener('click', () => {
                        const mode = button.getAttribute('data-mode') || 'all';
                        runRetry(mode);
                      });
                    });

                    btn.textContent = 'Details aktualisieren';
                  } catch (err) {
                    detailsDiv.innerHTML = '<div class="alert alert-danger">Fehler: ' + esc(err instanceof Error ? err.message : String(err)) + '</div>';
                    btn.textContent = 'Details laden';
                  } finally {
                    btn.disabled = false;
                  }
                });
              };

              resultEl.querySelectorAll('[data-load-failed-records]').forEach((btn) => bindLoadFailedDetails(btn));
            }
            renderMigrationList();
            return;
          } catch (err) {
            migState.status = 'error';
            stopMigRunProgressPolling();
            resultEl.classList.remove('d-none');
            resultEl.innerHTML = '<div class="alert alert-danger">Fehler: ' + esc(err instanceof Error ? err.message : String(err)) + '</div>';
          } finally {
            nextBtn.disabled = false; prevBtn.disabled = false;
          }
          return;
        }

        // Validate step 1
        if (migState.step === 1) {
          const nameEl = document.getElementById('mig-name');
          if (!nameEl || !nameEl.value.trim()) { alert('Bitte Migrationsname eingeben.'); return; }
          if (!migState.objects.length) { alert('Bitte mindestens ein Salesforce-Objekt auswählen.'); return; }
        }

        await migSave();
        migState.step++;
        renderMigWizardSteps();
        if (migState.step === 2) renderMigFileAssignments();
        if (migState.step === 3) renderMigMappingObjectSelect();
        if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
        if (migState.step === 5) renderMigOrderList();
        if (migState.step === 6) renderMigMissingFields();
        if (migState.step === 7) renderMigReview();
      });

      document.getElementById('mig-wizard-save')?.addEventListener('click', async () => {
        await migSave();
        showToast('Migration gespeichert.');
      });

      document.getElementById('mig-login-environment')?.addEventListener('change', () => {
        syncMigSalesforceLoginFromForm();
        migState.sfObjects = [];
        const listEl = document.getElementById('mig-sf-objects-list');
        const searchWrap = document.getElementById('mig-sf-objects-search-wrap');
        if (listEl) {
          listEl.innerHTML = '<div class="text-secondary small">Login-Umgebung gewechselt. Bitte Salesforce-Objekte fuer diese Migration neu laden.</div>';
        }
        if (searchWrap) {
          searchWrap.classList.add('d-none');
        }
      });

      document.getElementById('mig-login-auth-type')?.addEventListener('change', () => {
        syncMigSalesforceLoginFromForm();
      });

      document.getElementById('mig-instance-source')?.addEventListener('change', () => {
        syncMigSalesforceLoginFromForm();
      });

      document.getElementById('mig-existing-instance')?.addEventListener('change', () => {
        syncMigSalesforceLoginFromForm();
      });

      document.getElementById('mig-login-url')?.addEventListener('input', () => {
        syncMigSalesforceLoginFromForm();
      });

      document.getElementById('mig-login-authorize')?.addEventListener('click', async () => {
        const nameEl = document.getElementById('mig-name');
        if (nameEl) {
          migState.name = String(nameEl.value || '').trim();
        }
        if (!migState.name.trim()) {
          showMigrationModalError('Bitte zuerst einen Migrationsnamen vergeben.');
          return;
        }
        try {
          clearMigrationModalError();
          syncMigSalesforceLoginFromForm();
          await migSave();
          if (String(migState.salesforceLogin && migState.salesforceLogin.authType || 'password') !== 'oauth_refresh_token') {
            await requestJson('/api/migration-instances/' + encodeURIComponent(String(migState.id || '')) + '/connect', { method: 'POST' });
            const refreshedMigration = await requestJson('/api/migrations/' + encodeURIComponent(String(migState.id || '')));
            migState.salesforceLogin = refreshedMigration && refreshedMigration.salesforceLogin ? JSON.parse(JSON.stringify(refreshedMigration.salesforceLogin)) : migState.salesforceLogin;
            renderMigSalesforceLoginStatus();
            await renderMigrationInstances();
            showToast('Salesforce-Login erfolgreich getestet.');
            return;
          }
          await startMigrationSalesforceOAuth(String(migState.id || ''));
        } catch (error) {
          showMigrationModalError(error.message || 'Salesforce-Login konnte nicht gestartet werden.');
        }
      });

      // SF Objects loading
      document.getElementById('mig-load-sf-objects')?.addEventListener('click', async () => {
        const btn = document.getElementById('mig-load-sf-objects');
        btn.disabled = true; btn.textContent = '…';
        const listEl = document.getElementById('mig-sf-objects-list');
        const searchWrap = document.getElementById('mig-sf-objects-search-wrap');
        try {
          const res = await fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''));
          if (!res.ok) throw new Error(await res.text());
          migState.sfObjects = await res.json();
          renderMigSfObjectsList(migState.sfObjects);
          searchWrap.classList.remove('d-none');
          document.getElementById('mig-sf-objects-search').addEventListener('input', (e) => {
            const q = e.target.value.toLowerCase();
            renderMigSfObjectsList(migState.sfObjects.filter((o) =>
              o.name.toLowerCase().includes(q) || (o.label || '').toLowerCase().includes(q)
            ));
          });
        } catch (err) {
          listEl.innerHTML = '<div class="text-danger small">Fehler: ' + esc(String(err)) + '</div>';
        } finally {
          btn.disabled = false; btn.textContent = 'SF-Objekte laden';
        }
      });

      function renderMigSfObjectsList(objects) {
        const listEl = document.getElementById('mig-sf-objects-list');
        if (!listEl) return;
        if (!objects.length) { listEl.innerHTML = '<div class="text-secondary small">Keine Objekte gefunden.</div>'; return; }
        listEl.innerHTML = objects.map((obj) => {
          const alreadySelected = migState.objects.some((o) => o.salesforceObject === obj.name);
          return '<button type="button" class="btn btn-sm ' + (alreadySelected ? 'btn-success disabled' : 'btn-outline-secondary') + ' me-1 mb-1" data-sf-obj="' + esc(obj.name) + '" data-sf-label="' + esc(obj.label || obj.name) + '">' +
            esc(obj.label || obj.name) + ' <span class="text-secondary small">(' + esc(obj.name) + ')</span></button>';
        }).join('');
        listEl.querySelectorAll('[data-sf-obj]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const name = btn.getAttribute('data-sf-obj');
            const label = btn.getAttribute('data-sf-label');
            if (migState.objects.some((o) => o.salesforceObject === name)) return;
            migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: label, processingMode: 'sqlite', filePath: '', fileSheetName: '', availableSheetNames: [], fileColumns: [], fieldMappings: [], operation: 'insert' });
            renderMigSelectedObjects();
            consumePendingMigrationImportIfPossible().catch((error) => {
              alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
            });
            btn.className = 'btn btn-sm btn-success disabled me-1 mb-1';
          });
        });
      }

      document.getElementById('mig-add-manual-object')?.addEventListener('click', () => {
        const input = document.getElementById('mig-manual-object');
        const name = input ? input.value.trim() : '';
        if (!name) return;
        if (migState.objects.some((o) => o.salesforceObject === name)) { alert('Objekt bereits hinzugefügt.'); return; }
        migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: name, processingMode: 'sqlite', filePath: '', fileSheetName: '', availableSheetNames: [], fileColumns: [], fieldMappings: [], operation: 'insert' });
        renderMigSelectedObjects();
        consumePendingMigrationImportIfPossible().catch((error) => {
          alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
        });
        if (input) input.value = '';
      });

      document.getElementById('mig-mapping-object-select')?.addEventListener('change', () => {
        renderMigMappingPanel();
      });

      document.getElementById('mig-add-dependency')?.addEventListener('click', () => {
        const form = document.getElementById('mig-dependency-form');
        if (form) form.classList.toggle('d-none');
        renderMigDepSelects();
      });

      document.getElementById('mig-save-dependency')?.addEventListener('click', () => {
        const from = document.getElementById('mig-dep-from')?.value;
        const to = document.getElementById('mig-dep-to')?.value;
        const fromField = document.getElementById('mig-dep-from-field')?.value.trim();
        const toField = document.getElementById('mig-dep-to-field')?.value.trim();
        if (!from || !to || !fromField || !toField) { alert('Bitte alle Felder ausfüllen.'); return; }
        migState.dependencies.push({ fromObjectId: from, toObjectId: to, fromField, toField });
        renderMigDependencies();
        document.getElementById('mig-dependency-form').classList.add('d-none');
      });

      document.getElementById('mig-cancel-dependency')?.addEventListener('click', () => {
        document.getElementById('mig-dependency-form').classList.add('d-none');
      });

      function showToast(message) {
        const existing = document.getElementById('mig-toast');
        if (existing) existing.remove();
        const toast = document.createElement('div');
        toast.id = 'mig-toast';
        toast.className = 'position-fixed bottom-0 end-0 m-3 alert alert-success shadow';
        toast.style.zIndex = '9999';
        toast.textContent = message;
        document.body.appendChild(toast);
        setTimeout(() => toast.remove(), 2500);
      }

    </script>
  </body>
</html>`;
}

export function createAppServer(
  getHealthSnapshot: () => Promise<HealthSnapshot> | HealthSnapshot,
  adminDataService = new AdminDataService()
): http.Server {
  return http.createServer((req, res) => {
    void (async () => {
      const adminAuth = getAdminAuthConfig();
      const adminAuthRequired = adminAuth.enabled;
      const adminAuthMisconfigured = process.env.NODE_ENV === "production" && !adminAuthRequired;
      const csrfToken = getOrCreateCsrfToken(req);
      const requestUrl = new URL(req.url || "/", "http://localhost");
      const sendJson = (statusCode: number, payload: unknown): void => {
        res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify(payload));
      };
      const sendHtml = (statusCode: number, html: string, headers: http.OutgoingHttpHeaders = {}): void => {
        const mergedHeaders = {
          ...headers,
          "Set-Cookie": appendSetCookie(headers["Set-Cookie"] as string | string[] | undefined, buildCsrfCookie(req, csrfToken))
        };
        res.writeHead(statusCode, {
          "Content-Type": "text/html; charset=utf-8",
          ...mergedHeaders
        });
        res.end(html.replace("__CSRF_TOKEN__", escapeHtml(csrfToken)));
      };
      const sendRedirect = (location: string, statusCode = 302): void => {
        res.writeHead(statusCode, {
          Location: location,
          "Set-Cookie": appendSetCookie(undefined, buildCsrfCookie(req, csrfToken))
        });
        res.end();
      };
      const sendFile = async (filePath: string, contentType: string): Promise<void> => {
        const file = await fs.readFile(filePath);
        res.writeHead(200, {
          "Content-Type": contentType,
          "Cache-Control": "no-cache, no-store, must-revalidate",
          Pragma: "no-cache",
          Expires: "0"
        });
        res.end(file);
      };
      const sendDownloadFile = async (filePath: string, contentType: string, downloadFileName: string): Promise<void> => {
        const file = await fs.readFile(filePath);
        res.writeHead(200, {
          "Content-Type": contentType,
          "Content-Disposition": `attachment; filename="${downloadFileName.replace(/"/g, "")}"`,
          "Cache-Control": "no-cache, no-store, must-revalidate",
          Pragma: "no-cache",
          Expires: "0"
        });
        res.end(file);
      };
      const session = getAdminSession(req);
      const auditActor = session ? { userId: session.userId, username: session.username } : null;
      const isAuthenticated = !adminAuthRequired || Boolean(session);
      const isAssetRequest = requestUrl.pathname.startsWith("/assets/");
      const isPublicRequest =
        requestUrl.pathname === "/api/system/health" ||
        requestUrl.pathname === "/auth/login" ||
        requestUrl.pathname === "/auth/logout" ||
        requestUrl.pathname === "/auth/migration-salesforce/callback" ||
        requestUrl.pathname === "/auth/salesforce/login" ||
        requestUrl.pathname === "/auth/salesforce/callback";
      const requiresMutationProtection = isMutatingMethod(req.method) && (requestUrl.pathname.startsWith("/api/") || requestUrl.pathname.startsWith("/auth/"));
      const requiredPermission = (() => {
        if (requestUrl.pathname === "/auth/logout") {
          return null;
        }
        if (requestUrl.pathname === "/auth/login" || requestUrl.pathname === "/auth/salesforce/login" || requestUrl.pathname === "/auth/salesforce/callback") {
          return null;
        }
        if (requestUrl.pathname === "/api/system/health" || isAssetRequest) {
          return null;
        }
        if (req.method === "DELETE") {
          return "delete" as const;
        }
        if (req.method === "POST" || req.method === "PUT" || req.method === "PATCH") {
          return "write" as const;
        }
        return "read" as const;
      })();

      if (requiresMutationProtection && !hasAllowedRequestOrigin(req)) {
        sendJson(403, { error: "Origin nicht erlaubt" });
        return;
      }

      if (requiresMutationProtection && !hasValidCsrfToken(req)) {
        sendJson(403, { error: "CSRF-Token fehlt oder ist ungültig" });
        return;
      }

      if (adminAuthMisconfigured && !isAssetRequest && !isPublicRequest) {
        if (requestUrl.pathname === "/") {
          sendHtml(503, renderAuthConfigurationShell());
          return;
        }

        if (requestUrl.pathname.startsWith("/api/")) {
          sendJson(503, { error: "Admin-Zugang ist nicht konfiguriert" });
          return;
        }
      }

      if (req.method === "POST" && requestUrl.pathname === "/auth/login") {
        if (!adminAuthRequired) {
          sendJson(adminAuthMisconfigured ? 503 : 200, { ok: !adminAuthMisconfigured, authEnabled: false, error: adminAuthMisconfigured ? "Admin-Zugang ist nicht konfiguriert" : undefined });
          return;
        }
        if (adminAuth.mode !== "local") {
          sendJson(400, { error: "Lokaler Login ist nicht aktiviert." });
          return;
        }

        const body = (await readJsonBody(req)) as { username?: string; password?: string };
        const username = String(body.username || "").trim();
        const password = String(body.password || "");
        const user = authenticateLocalAdminUser(username, password, adminAuth);
        if (!user) {
          res.setHeader("Set-Cookie", buildExpiredSessionCookie(req));
          sendJson(401, { error: "Ungültige Zugangsdaten" });
          return;
        }

        const sessionToken = createAdminSession({
          userId: user.id,
          username: user.username,
          displayName: user.displayName,
          roles: user.roles,
          permissions: user.permissions,
          modules: user.modules,
          authProvider: "local"
        });
        res.setHeader("Set-Cookie", buildSessionCookie(req, sessionToken));
        sendJson(200, { ok: true, permissions: user.permissions, roles: user.roles });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/auth/salesforce/login") {
        if (!adminAuthRequired || adminAuth.mode !== "salesforce_oidc") {
          sendHtml(400, renderLoginShell({ errorMessage: "Salesforce-Login ist nicht aktiviert.", csrfToken, authMode: adminAuth.mode }));
          return;
        }

        try {
          const redirectUri = buildAdminSalesforceOidcRedirectUri(req);
          const state = createSalesforceLoginState(redirectUri);
          sendRedirect(buildSalesforceLoginAuthorizationUrl(state, redirectUri, adminAuth));
        } catch (error) {
          sendHtml(500, renderLoginShell({ errorMessage: error instanceof Error ? error.message : String(error), csrfToken, authMode: adminAuth.mode }));
        }
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/auth/salesforce/callback") {
        const state = String(requestUrl.searchParams.get("state") || "").trim();
        const code = String(requestUrl.searchParams.get("code") || "").trim();
        const oauthError = String(requestUrl.searchParams.get("error") || "").trim();
        const oauthErrorDescription = String(requestUrl.searchParams.get("error_description") || "").trim();
        if (oauthError) {
          sendHtml(401, renderLoginShell({
            errorMessage: formatSalesforceLoginCallbackError(req, oauthError, oauthErrorDescription),
            csrfToken,
            authMode: "salesforce_oidc"
          }));
          return;
        }

        if (!code || !state) {
          sendHtml(400, renderLoginShell({ errorMessage: "Salesforce hat keinen gueltigen Login-Callback geliefert.", csrfToken, authMode: "salesforce_oidc" }));
          return;
        }

        try {
          const user = await completeSalesforceLogin(code, state, adminAuth);
          const sessionToken = createAdminSession({
            userId: user.id,
            username: user.username,
            displayName: user.displayName,
            roles: user.roles,
            permissions: user.permissions,
            modules: user.modules,
            authProvider: "salesforce_oidc"
          });
          res.setHeader("Set-Cookie", buildSessionCookie(req, sessionToken));
          sendRedirect("/");
        } catch (error) {
          sendHtml(401, renderLoginShell({ errorMessage: error instanceof Error ? error.message : String(error), csrfToken, authMode: "salesforce_oidc" }));
        }
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/auth/logout") {
        clearAdminSession(req);
        res.setHeader("Set-Cookie", buildExpiredSessionCookie(req));
        sendJson(200, { ok: true });
        return;
      }

      if (adminAuthRequired && !isAuthenticated && !isAssetRequest && !isPublicRequest) {
        if (requestUrl.pathname === "/") {
          sendHtml(401, renderLoginShell({ csrfToken, authMode: adminAuth.mode }));
          return;
        }

        if (requestUrl.pathname.startsWith("/api/")) {
          sendJson(401, { error: "Authentifizierung erforderlich" });
          return;
        }
      }

      if (adminAuthRequired && requiredPermission && !hasPermission(session, requiredPermission)) {
        if (requestUrl.pathname.startsWith("/api/")) {
          sendJson(403, { error: `Berechtigung '${requiredPermission}' fehlt` });
          return;
        }

        sendHtml(403, renderLoginShell({ errorMessage: `Berechtigung '${requiredPermission}' fehlt`, csrfToken, authMode: adminAuth.mode }));
        return;
      }

      const isMigrationRequest =
        requestUrl.pathname === "/auth/migration-salesforce/start" ||
        requestUrl.pathname.startsWith("/api/migrations") ||
        requestUrl.pathname.startsWith("/api/migration-instances");
      if (adminAuthRequired && isMigrationRequest && !hasModuleAccess(session, "migration")) {
        if (requestUrl.pathname.startsWith("/api/")) {
          sendJson(403, { error: "Modulberechtigung 'migration' fehlt" });
          return;
        }
        sendHtml(403, renderLoginShell({ errorMessage: "Modulberechtigung 'migration' fehlt", csrfToken, authMode: adminAuth.mode }));
        return;
      }

      const instanceId = requestUrl.searchParams.get("instanceId") || undefined;
      const connectorTestMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/connectors\/([^/]+)\/test$/) : null;
      const connectorDeleteMatch = req.method === "DELETE" ? requestUrl.pathname.match(/^\/api\/connectors\/([^/]+)$/) : null;
      const scheduleRunMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/run$/) : null;
      const scheduleDryRunMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/dry-run$/) : null;
      const scheduleDuplicateMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/duplicate$/) : null;
      const scheduleDeleteMatch = req.method === "DELETE" ? requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)$/) : null;
      const runLogsMatch = req.method === "GET" ? requestUrl.pathname.match(/^\/api\/runs\/([^/]+)\/logs$/) : null;
      const logRangeParam = requestUrl.searchParams.get("range") || "last_24h";
      const logRange: LogChartRange =
        logRangeParam === "last_hour" || logRangeParam === "last_30d" || logRangeParam === "last_24h"
          ? logRangeParam
          : "last_24h";

      if (req.method === "GET" && requestUrl.pathname === "/auth/migration-salesforce/callback") {
        const state = String(requestUrl.searchParams.get("state") || "").trim();
        const code = String(requestUrl.searchParams.get("code") || "").trim();
        const oauthError = String(requestUrl.searchParams.get("error") || "").trim();
        const oauthErrorDescription = String(requestUrl.searchParams.get("error_description") || "").trim();
        const pendingState = consumeMigrationOauthState(state);

        if (!pendingState) {
          sendHtml(400, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: "",
            message: "Der Salesforce-Login-Status ist abgelaufen oder ungueltig. Bitte den Login erneut aus der Migration starten."
          }));
          return;
        }

        if (oauthError) {
          sendHtml(400, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: pendingState.migrationId,
            message: oauthErrorDescription || oauthError
          }));
          return;
        }

        if (!code) {
          sendHtml(400, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: pendingState.migrationId,
            message: "Salesforce hat keinen Authorization Code geliefert."
          }));
          return;
        }

        try {
          await adminDataService.completeMigrationOAuth(pendingState.migrationId, code, pendingState.redirectUri);
          sendHtml(200, renderMigrationOauthCallbackShell({
            ok: true,
            migrationId: pendingState.migrationId,
            message: "Salesforce-Freigabe gespeichert. Das Fenster schliesst sich gleich."
          }));
        } catch (error) {
          sendHtml(500, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: pendingState.migrationId,
            message: error instanceof Error ? error.message : String(error)
          }));
        }
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/auth/migration-salesforce/start") {
        const migrationInstanceId = String(requestUrl.searchParams.get("migrationId") || "").trim();
        if (!migrationInstanceId) {
          sendHtml(400, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: "",
            message: "Migration-ID fuer Salesforce-Login fehlt."
          }));
          return;
        }

        try {
          const redirectUri = buildMigrationOauthRedirectUri(req);
          const state = createMigrationOauthState(migrationInstanceId, redirectUri);
          const authorizationUrl = adminDataService.getMigrationOAuthAuthorizationUrl(migrationInstanceId, state, redirectUri);
          sendRedirect(authorizationUrl);
        } catch (error) {
          sendHtml(500, renderMigrationOauthCallbackShell({
            ok: false,
            migrationId: migrationInstanceId,
            message: error instanceof Error ? error.message : String(error)
          }));
        }
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/system/health") {
        sendJson(200, await getHealthSnapshot());
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/admin/me") {
        sendJson(200, {
          authenticated: Boolean(session),
          user: session ? {
            id: session.userId,
            username: session.username,
            displayName: session.displayName,
            roles: session.roles,
            permissions: session.permissions,
            modules: session.modules,
            authProvider: session.authProvider
          } : null
        });
        return;
      }

      if (requestUrl.pathname.startsWith("/api/admin/") && !hasPermission(session, "admin")) {
        sendJson(403, { error: "Admin-Berechtigung fehlt" });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/admin/users") {
        sendJson(200, { items: listAdminUsers() });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/admin/users") {
        const body = (await readJsonBody(req)) as AdminUserMutationInput;
        const item = saveAdminUser(body);
        await appendAuditHistory({
          actor: auditActor,
          action: body.id ? "update" : "create",
          entityType: "admin-user",
          entityId: item.id,
          entityName: item.username
        });
        sendJson(200, item);
        return;
      }

      const adminUserDeleteMatch = req.method === "DELETE" ? requestUrl.pathname.match(/^\/api\/admin\/users\/([^/]+)$/) : null;
      if (adminUserDeleteMatch) {
        const id = decodeURIComponent(adminUserDeleteMatch[1]);
        const deleted = deleteAdminUser(id);
        await appendAuditHistory({
          actor: auditActor,
          action: "delete",
          entityType: "admin-user",
          entityId: id,
          status: deleted ? "success" : "error"
        });
        sendJson(deleted ? 200 : 404, { ok: deleted });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/admin/audit-history") {
        const limit = Number(requestUrl.searchParams.get("limit") || 100) || 100;
        sendJson(200, {
          items: await listAuditHistory(limit, {
            entityType: requestUrl.searchParams.get("entityType") || undefined,
            entityId: requestUrl.searchParams.get("entityId") || undefined
          })
        });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/installer/summary") {
        sendJson(200, getInstallerSummary());
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/installer/generate") {
        const body = (await readJsonBody(req)) as InstallerGenerationInput;
        sendJson(200, await generateInstallerFiles(body));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/installer/archive") {
        const directoryName = String(requestUrl.searchParams.get("dir") || "").trim();
        const archiveFileName = String(requestUrl.searchParams.get("file") || "").trim();
        if (!directoryName || !archiveFileName) {
          sendJson(400, { error: "Archivparameter fehlen" });
          return;
        }

        const candidatePath = path.resolve(INSTALLER_OUTPUT_DIR, directoryName, archiveFileName);
        const expectedRoot = path.resolve(INSTALLER_OUTPUT_DIR) + path.sep;
        if (!candidatePath.startsWith(expectedRoot)) {
          sendJson(400, { error: "Ungültiger Archivpfad" });
          return;
        }

        try {
          await fs.access(candidatePath);
        } catch {
          sendJson(404, { error: "Archiv nicht gefunden" });
          return;
        }

        await sendDownloadFile(candidatePath, "application/zip", path.basename(candidatePath));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/system/update-status") {
        sendJson(200, await getDashboardUpdateStatus());
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/system/update-now") {
        const result = await triggerDashboardUpdate();
        sendJson(result.ok ? 200 : 500, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/bootstrap.min.css") {
        await sendFile(BOOTSTRAP_CSS_FILE, "text/css; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/bootstrap.bundle.min.js") {
        await sendFile(BOOTSTRAP_JS_FILE, "application/javascript; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/chart.umd.js") {
        await sendFile(CHART_JS_FILE, "application/javascript; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/style.css") {
        await sendFile(APP_STYLE_CSS_FILE, "text/css; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/agent-ui.css") {
        await sendFile(AGENT_UI_CSS_FILE, "text/css; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/template-store.css") {
        await sendFile(TEMPLATE_STORE_CSS_FILE, "text/css; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/assets/examples/setup-file-import-export.example.json") {
        await sendFile(SETUP_EXAMPLE_JSON_FILE, "application/json; charset=utf-8");
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/instances") {
        const items = adminDataService.listInstances();
        sendJson(200, { items, total: items.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/instances") {
        const body = (await readJsonBody(req)) as SalesforceInstanceMutationInput;
        const item = adminDataService.saveInstance(body);
        await appendAuditHistory({ actor: auditActor, action: body.id ? "update" : "create", entityType: "salesforce-instance", entityId: item.id, entityName: item.name || item.id });
        if (isRemoteAgentConfigured()) {
          await syncRemoteAgentInstances(adminDataService.listConfiguredInstanceConfigs());
        }
        sendJson(200, item);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/setup/export") {
        const exported = await adminDataService.exportSetup(instanceId);
        sendJson(200, exported satisfies SetupExportDocument);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/setup/import") {
        const body = (await readJsonBody(req)) as SetupExportDocument;
        const result = await adminDataService.importSetup(body, instanceId);
        await appendAuditHistory({ actor: auditActor, action: "import", entityType: "setup", entityId: instanceId });
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/setup/deploy-ezb") {
        try {
          const result = await adminDataService.deployEzbMetadata(instanceId);
          sendJson(200, {
            ok: true,
            message: "EZB__c metadata deployed successfully",
            result
          });
        } catch (error) {
          const msg = error instanceof Error ? error.message : String(error);
          sendJson(500, {
            ok: false,
            error: msg,
            details: error
          });
        }
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/setup/create-custom-object-from-source") {
        const body = (await readJsonBody(req)) as {
          objectApiName?: string;
          sourceFields?: Array<{ name?: string; label?: string; type?: string }>;
          fieldOverrides?: Array<{ sourceName?: string; type?: string }>;
          label?: string;
        };

        const result = await adminDataService.createCustomObjectFromSource(
          {
            objectApiName: String(body.objectApiName || "").trim(),
            sourceFields: Array.isArray(body.sourceFields)
              ? body.sourceFields.map((field) => ({
                  name: String(field?.name || "").trim(),
                  label: String(field?.label || "").trim() || undefined,
                  type: String(field?.type || "string").trim()
                }))
              : [],
            fieldOverrides: Array.isArray(body.fieldOverrides)
              ? body.fieldOverrides.map((item) => ({
                  sourceName: String(item?.sourceName || "").trim(),
                  type: String(item?.type || "").trim()
                }))
              : [],
            label: body.label
          },
          instanceId
        );

        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/files/analyze") {
        const body = (await readJsonBody(req)) as {
          connectorId?: string;
          fileName?: string;
          contentBase64?: string;
        };
        const result = await adminDataService.analyzeUploadedSourceFile(
          body.connectorId || "",
          body.fileName || "",
          body.contentBase64 || "",
          instanceId
        );
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/migrations/analyze-import") {
        const body = (await readJsonBody(req)) as {
          fileName?: string;
          contentBase64?: string;
        };
        const result = await adminDataService.analyzeMigrationImportFile(
          String(body.fileName || "").trim(),
          String(body.contentBase64 || "").trim(),
          instanceId
        );
        sendJson(200, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/templates") {
        const kindParam = requestUrl.searchParams.get("kind");
        const kind = kindParam === "connector" || kindParam === "schedule" ? kindParam : undefined;
        const templates = await adminDataService.listTemplates(kind);
        sendJson(200, { items: templates, total: templates.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/templates") {
        const body = await readJsonBody(req);
        const result = await adminDataService.saveTemplate(body as any);
        await appendAuditHistory({ actor: auditActor, action: "save", entityType: "template", entityId: result.id, entityName: result.name });
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname.match(/^\/api\/templates\/([^/]+)\/apply$/)) {
        const templateId = decodeURIComponent(requestUrl.pathname.replace(/^\/api\/templates\/([^/]+)\/apply$/, "$1"));
        const result = await adminDataService.applyTemplate(templateId, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/schedules") {
        const schedules = await adminDataService.listSchedules(instanceId);
        sendJson(200, { items: schedules, total: schedules.length });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/schedules/options") {
        const options = await adminDataService.getScheduleFormOptions(instanceId);
        sendJson(200, options satisfies ScheduleFormOptions);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/schedules") {
        const body = (await readJsonBody(req)) as ScheduleMutationInput;
        const result = await adminDataService.saveSchedule(body, instanceId);
        await appendAuditHistory({ actor: auditActor, action: body.id ? "update" : "create", entityType: "schedule", entityId: result.id, entityName: body.name });
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/schedules/validate-config") {
        const body = (await readJsonBody(req)) as ScheduleMutationInput;
        const result = await adminDataService.validateScheduleConfiguration(body, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/active$/)) {
        const scheduleId = decodeURIComponent(requestUrl.pathname.replace(/^\/api\/schedules\/([^/]+)\/active$/, "$1"));
        const body = (await readJsonBody(req)) as { active?: boolean };
        const result = await adminDataService.setScheduleActive(scheduleId, body.active === true, instanceId);
        await appendAuditHistory({ actor: auditActor, action: body.active === true ? "activate" : "deactivate", entityType: "schedule", entityId: scheduleId });
        sendJson(200, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/checkpoint$/)) {
        const scheduleId = decodeURIComponent(requestUrl.pathname.replace(/^\/api\/schedules\/([^/]+)\/checkpoint$/, "$1"));
        const result = await adminDataService.getScheduleCheckpoint(scheduleId, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/checkpoint$/)) {
        const scheduleId = decodeURIComponent(requestUrl.pathname.replace(/^\/api\/schedules\/([^/]+)\/checkpoint$/, "$1"));
        const body = (await readJsonBody(req)) as ScheduleCheckpointMutationInput;
        const result = await adminDataService.updateScheduleCheckpoint(scheduleId, body, instanceId);
        await appendAuditHistory({ actor: auditActor, action: "update-checkpoint", entityType: "schedule", entityId: scheduleId });
        sendJson(200, result);
        return;
      }

      if (scheduleDeleteMatch) {
        const scheduleId = decodeURIComponent(scheduleDeleteMatch[1]);
        const result = await adminDataService.deleteSchedule(scheduleId, instanceId);
        await appendAuditHistory({ actor: auditActor, action: "delete", entityType: "schedule", entityId: scheduleId, entityName: result.deletedNames.join(", ") });
        sendJson(200, result);
        return;
      }

      if (scheduleDryRunMatch) {
        const scheduleId = decodeURIComponent(scheduleDryRunMatch[1]);
        const result = await adminDataService.dryRunScheduleSource(scheduleId, instanceId);
        sendJson(200, result);
        return;
      }

      if (scheduleDuplicateMatch) {
        const scheduleId = decodeURIComponent(scheduleDuplicateMatch[1]);
        const body = (await readJsonBody(req)) as { name?: string };
        const result = await adminDataService.duplicateSchedule(scheduleId, body.name, instanceId);
        await appendAuditHistory({ actor: auditActor, action: "duplicate", entityType: "schedule", entityId: scheduleId, entityName: body.name });
        sendJson(200, result);
        return;
      }

      if (scheduleRunMatch) {
        const scheduleId = decodeURIComponent(scheduleRunMatch[1]);
        const result = await adminDataService.triggerScheduleNow(
          process.env.AGENT_ID || "local-agent-01",
          scheduleId,
          instanceId
        );
        sendJson(result.triggered ? 200 : 409, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/connectors") {
        const connectors = await adminDataService.listConnectors(instanceId);
        sendJson(200, { items: connectors, total: connectors.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/connectors") {
        const body = (await readJsonBody(req)) as ConnectorMutationInput;
        const result = await adminDataService.saveConnector(body, instanceId);
        await appendAuditHistory({ actor: auditActor, action: body.id ? "update" : "create", entityType: "connector", entityId: result.id, entityName: body.name });
        sendJson(200, result);
        return;
      }

      if (connectorDeleteMatch) {
        const connectorId = decodeURIComponent(connectorDeleteMatch[1]);
        const result = await adminDataService.deleteConnector(connectorId, instanceId);
        await appendAuditHistory({ actor: auditActor, action: "delete", entityType: "connector", entityId: connectorId, entityName: result.connectorName });
        sendJson(200, result);
        return;
      }

      if (connectorTestMatch) {
        const connectorId = decodeURIComponent(connectorTestMatch[1]);
        const result = await adminDataService.testConnector(connectorId, instanceId);
        sendJson(result.ok ? 200 : 500, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/runs") {
        const runs = await adminDataService.listRuns(50, instanceId);
        sendJson(200, { items: runs, total: runs.length });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/dashboard/records-summary") {
        const rangeParam = requestUrl.searchParams.get("range") || "month";
        const range: OverviewStatsRange =
          rangeParam === "day" || rangeParam === "month" || rangeParam === "year"
            ? rangeParam
            : "month";
        const summary = await adminDataService.summarizeRecordsByRange(range, instanceId);
        sendJson(200, summary);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/runs/stale") {
        const runs = await adminDataService.listStaleRuns(50, instanceId);
        sendJson(200, { items: runs, total: runs.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/runs/release-stale") {
        const body = (await readJsonBody(req)) as { runIds?: string[] };
        const result = await adminDataService.releaseStaleRuns(body.runIds, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname.match(/^\/api\/runs\/([^/]+)\/cancel$/)) {
        const runId = decodeURIComponent(requestUrl.pathname.replace(/^\/api\/runs\/([^/]+)\/cancel$/, "$1"));
        const result = await adminDataService.cancelRun(runId, instanceId);
        sendJson(200, result);
        return;
      }

      if (runLogsMatch) {
        const runId = decodeURIComponent(runLogsMatch[1]);
        const logs = await adminDataService.listLogs(runId, 200, instanceId);
        sendJson(200, { items: logs, total: logs.length });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/logs/summary") {
        const summary = await adminDataService.summarizeLogsByRange(logRange, instanceId);
        sendJson(200, summary);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/logs") {
        const start = requestUrl.searchParams.get("start");
        const end = requestUrl.searchParams.get("end");
        const typeParam = requestUrl.searchParams.get("type") || "all";
        const connector = requestUrl.searchParams.get("connector") || undefined;
        const type = typeParam === "error" ? "error" : "all";
        const limit = Number(requestUrl.searchParams.get("limit") || 300);

        if (!start || !end) {
          sendJson(400, { error: "start und end sind erforderlich" });
          return;
        }

        const logs = await adminDataService.listLogsByRange(start, end, type, limit, connector, instanceId);
        sendJson(200, { items: logs, total: logs.length });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/graph") {
        const graph = await adminDataService.getConnectionGraph(instanceId);
        sendJson(200, graph);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/salesforce/overview") {
        const overview = await adminDataService.getSalesforceOverview(instanceId);
        sendJson(200, overview);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/salesforce/objects") {
        const objects = await adminDataService.listSalesforceObjects(instanceId);
        sendJson(200, objects);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/salesforce/pricebooks") {
        const pricebooks = await adminDataService.listSalesforcePricebooks(instanceId);
        sendJson(200, pricebooks);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/salesforce/users") {
        const users = await adminDataService.listSalesforceUsers(instanceId);
        sendJson(200, users);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/salesforce/object-fields") {
        const objectName = requestUrl.searchParams.get("object") || "";
        if (!objectName) {
          sendJson(400, { error: "object parameter required" });
          return;
        }
        const fields = await adminDataService.describeSalesforceObjectFields(objectName, instanceId);
        sendJson(200, fields);
        return;
      }

      const migrationDistinctValuesMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/objects\/([^/]+)\/distinct-values$/);
      if (migrationDistinctValuesMatch && req.method === "GET") {
        const migrationId = decodeURIComponent(migrationDistinctValuesMatch[1]);
        const objectId = decodeURIComponent(migrationDistinctValuesMatch[2]);
        const columnName = String(requestUrl.searchParams.get("column") || "").trim();
        if (!columnName) {
          sendJson(400, { error: "column parameter required" });
          return;
        }
        const values = await adminDataService.getMigrationSourceDistinctValues(migrationId, objectId, columnName);
        sendJson(200, { values });
        return;
      }

      const migrationPreflightMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/preflight$/);
      if (migrationPreflightMatch && req.method === "GET") {
        const migrationId = decodeURIComponent(migrationPreflightMatch[1]);
        const result = await adminDataService.getMigrationPreflightWarnings(migrationId, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/salesforce/create-field") {
        const body = (await readJsonBody(req)) as {
          objectApiName?: string;
          fieldApiName?: string;
          fieldType?: string;
          picklistValues?: string[];
          externalId?: boolean;
          unique?: boolean;
        };
        if (!body.objectApiName || !body.fieldApiName) {
          sendJson(400, { error: "objectApiName and fieldApiName required" });
          return;
        }
        if (body.fieldType === 'Picklist' && (!Array.isArray(body.picklistValues) || !body.picklistValues.length)) {
          sendJson(400, { error: "picklistValues required for Picklist fields" });
          return;
        }
        const result = await adminDataService.createSalesforceCustomField(
          body.objectApiName,
          body.fieldApiName,
          body.fieldType || "Text",
          { picklistValues: body.picklistValues, externalId: body.externalId, unique: body.unique },
          instanceId
        );
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/salesforce/generate-mapping") {
        const body = (await readJsonBody(req)) as {
          sourceFields?: Array<{ name?: string; type?: string }>;
          targetFields?: Array<{ name?: string; label?: string; type?: string; isExternalId?: boolean }>;
          targetObjectApiName?: string;
          profile?: "standard" | "salesforce-product" | "salesforce-pricebook";
        };

        const items = generateSalesforceMappingRules({
          sourceFields: Array.isArray(body.sourceFields)
            ? body.sourceFields.map((field) => ({
                name: String(field?.name || "").trim(),
                type: String(field?.type || "").trim() || undefined
              })).filter((field) => field.name)
            : [],
          targetFields: Array.isArray(body.targetFields)
            ? body.targetFields.map((field) => ({
                name: String(field?.name || "").trim(),
                label: String(field?.label || "").trim() || undefined,
                type: String(field?.type || "").trim() || undefined,
                isExternalId: field?.isExternalId === true
              })).filter((field) => field.name)
            : [],
          targetObjectApiName: String(body.targetObjectApiName || "").trim() || undefined,
          profile: body.profile
        });

        sendJson(200, { items });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/queries/preview") {
        const body = (await readJsonBody(req)) as { connectorId?: string; query?: string; limit?: number };
        const result = await adminDataService.previewSql(body.connectorId || "", body.query || "", body.limit || 10, instanceId);
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/sources/preview") {
        const body = (await readJsonBody(req)) as {
          sourceType?: string;
          sourceDefinition?: string;
          connectorId?: string;
          limit?: number;
        };
        const result = await adminDataService.previewSource(
          body.sourceType || "",
          body.sourceDefinition || "",
          body.connectorId,
          body.limit || 10,
          instanceId
        );
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/sources/fields") {
        const body = (await readJsonBody(req)) as {
          sourceType?: string;
          sourceDefinition?: string;
          objectName?: string;
          connectorId?: string;
        };
        const fields = await adminDataService.getSourceFields(
          body.sourceType || "",
          body.sourceDefinition || "",
          body.objectName,
          body.connectorId,
          instanceId
        );
        sendJson(200, { fields, total: fields.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/mappings/preview") {
        const body = (await readJsonBody(req)) as { mappingDefinition?: string; sourceData?: Record<string, unknown>[] };
        const result = await adminDataService.previewMapping(body.mappingDefinition || "", Array.isArray(body.sourceData) ? body.sourceData : []);
        sendJson(200, result);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/mapping/transforms") {
        const result = await adminDataService.getTransformFunctions();
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/targets/objects") {
        const body = (await readJsonBody(req)) as { targetSystem?: string; connectorId?: string };
        const result = await adminDataService.getTargetObjects(
          body.targetSystem,
          body.connectorId,
          instanceId
        );
        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/mapping/target-fields") {
        const body = (await readJsonBody(req)) as { targetSystem?: string; targetObject?: string; connectorId?: string };
        const result = await adminDataService.getTargetFields(
          body.targetSystem,
          body.targetObject,
          body.connectorId,
          instanceId
        );
        sendJson(200, result);
        return;
      }

      // ─── Migration API ──────────────────────────────────────────────────────

      if (req.method === "GET" && requestUrl.pathname === "/api/migrations") {
        sendJson(200, { items: adminDataService.listMigrationsForUi() });
        return;
      }

      const migrationInstanceIdMatch = requestUrl.pathname.match(/^\/api\/migration-instances\/([^/]+)$/);
      const migrationInstanceConnectMatch = requestUrl.pathname.match(/^\/api\/migration-instances\/([^/]+)\/connect$/);

      if (req.method === "GET" && requestUrl.pathname === "/api/migration-instances") {
        sendJson(200, { items: adminDataService.listMigrationInstances() });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/migration-instances") {
        const body = (await readJsonBody(req)) as {
          id?: string;
          name?: string;
          environment?: "sandbox" | "production";
          loginUrl?: string;
          queryLimit?: number;
        };
        const item = adminDataService.saveMigrationInstance({
          id: body.id,
          name: String(body.name || ""),
          environment: body.environment === "production" ? "production" : "sandbox",
          loginUrl: String(body.loginUrl || ""),
          queryLimit: body.queryLimit
        });
        await appendAuditHistory({ actor: auditActor, action: body.id ? "update" : "create", entityType: "migration-instance", entityId: item.id, entityName: item.name });
        sendJson(201, item);
        return;
      }

      if (migrationInstanceConnectMatch && req.method === "POST") {
        const migrationInstanceId = decodeURIComponent(migrationInstanceConnectMatch[1]);
        const item = await adminDataService.connectMigrationInstance(migrationInstanceId);
        await appendAuditHistory({ actor: auditActor, action: "connect", entityType: "migration-instance", entityId: item.id, entityName: item.name, status: item.connectionStatus === "connected" ? "success" : "error", message: item.lastConnectionError });
        sendJson(200, item);
        return;
      }

      if (migrationInstanceIdMatch && req.method === "GET") {
        const migrationInstanceId = decodeURIComponent(migrationInstanceIdMatch[1]);
        const item = adminDataService.listMigrationInstances().find((entry) => entry.id === migrationInstanceId);
        if (!item) {
          sendJson(404, { error: "Migration instance not found" });
          return;
        }
        sendJson(200, item);
        return;
      }

      const migrationIdMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)$/);
      const migrationRunMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/run$/);
      const migrationReportMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/report$/);
      const migrationAnalyzeMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/analyze-file\/([^/]+)$/);
        const failedRecordsMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/failed-records\/([^/]+)$/);
        const retryFailedRecordsMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/failed-records\/([^/]+)\/([^/]+)\/retry$/);

      if (req.method === "POST" && requestUrl.pathname === "/api/migrations/upload-file") {
        const body = (await readJsonBody(req)) as {
          migrationId?: string;
          objectId?: string;
          fileName?: string;
          contentBase64?: string;
          sheetName?: string;
          charset?: string;
          delimiter?: string;
          textQualifier?: string;
        };

        const migrationId = String(body.migrationId || "").trim();
        const objectId = String(body.objectId || "").trim();
        const fileName = path.basename(String(body.fileName || "").trim());
        const contentBase64 = String(body.contentBase64 || "").trim();

        if (!migrationId || !objectId || !fileName || !contentBase64) {
          sendJson(400, { error: "migrationId, objectId, fileName und contentBase64 sind erforderlich" });
          return;
        }

        const fileBuffer = Buffer.from(contentBase64, "base64");
        const analysis = await adminDataService.stageMigrationSourceFile(migrationId, objectId, fileName, fileBuffer, {
          sheetName: String(body.sheetName || '').trim() || undefined,
          charset: String(body.charset || '').trim() || undefined,
          delimiter: body.delimiter,
          textQualifier: body.textQualifier
        });
        await appendAuditHistory({ actor: auditActor, action: "upload-file", entityType: "migration", entityId: migrationId, entityName: fileName });

        sendJson(200, {
          filePath: analysis.filePath,
          format: analysis.format,
          charset: analysis.charset,
          delimiter: analysis.delimiter,
          textQualifier: analysis.textQualifier,
          sheetName: analysis.sheetName,
          availableSheetNames: analysis.availableSheetNames,
          recordCount: analysis.recordCount,
          fields: analysis.fields,
          rows: analysis.rows,
          stagingMode: analysis.stagingMode,
          stagingDatabasePath: analysis.stagingDatabasePath,
          stagingImportedAt: analysis.stagingImportedAt,
          stagingStatus: analysis.stagingStatus
        });
        return;
      }

      if (migrationRunMatch && req.method === "POST") {
        const migId = decodeURIComponent(migrationRunMatch[1]);
        const migration = adminDataService.getMigration(migId);
        if (!migration) {
          sendJson(404, { error: "Migration not found" });
          return;
        }

        if (String(migration.status || "") === "running") {
          sendJson(202, {
            accepted: true,
            migrationId: migId,
            status: "running",
            lastRunResult: migration.lastRunResult || null
          });
          return;
        }

        void adminDataService.runMigration(migId, instanceId || undefined).catch((err) => {
          console.error("Migration run failed", {
            migrationId: migId,
            error: err instanceof Error ? err.message : String(err)
          });
        });
        await appendAuditHistory({ actor: auditActor, action: "run", entityType: "migration", entityId: migId, entityName: migration.name });

        sendJson(202, {
          accepted: true,
          migrationId: migId,
          status: "running",
          lastRunResult: migration.lastRunResult || null
        });
        return;
      }

      if (migrationReportMatch && req.method === "GET") {
        const migId = decodeURIComponent(migrationReportMatch[1]);
        const migration = adminDataService.getMigration(migId);
        if (!migration) {
          sendJson(404, { error: "Migration not found" });
          return;
        }

        const explicitReportPath = String(migration.lastRunResult?.reportPath || "").trim();
        let reportFilePath = explicitReportPath
          ? path.resolve(process.cwd(), explicitReportPath)
          : "";

        const fileExists = async (candidatePath: string) => {
          if (!candidatePath) {
            return false;
          }

          try {
            await fs.access(candidatePath);
            return true;
          } catch {
            return false;
          }
        };

        if (!(await fileExists(reportFilePath))) {
          const reportDir = path.join(process.cwd(), "artifacts", "migrations", migId, "reports");
          if (!(await fileExists(reportDir))) {
            sendJson(404, { error: "Report not found" });
            return;
          }

          const candidates = (await fs.readdir(reportDir))
            .filter((fileName: string) => fileName.endsWith('.md'))
            .sort((a: string, b: string) => b.localeCompare(a, 'de'));

          if (!candidates.length) {
            sendJson(404, { error: "Report not found" });
            return;
          }

          reportFilePath = path.join(reportDir, candidates[0]);
        }

        const reportContent = await fs.readFile(reportFilePath, 'utf8');
        const reportFileName = path.basename(reportFilePath);
        const asDownload = requestUrl.searchParams.get('download') === '1';
        res.writeHead(200, {
          'Content-Type': 'text/markdown; charset=utf-8',
          'Content-Disposition': (asDownload ? 'attachment' : 'inline') + '; filename="' + reportFileName.replace(/"/g, '') + '"'
        });
        res.end(reportContent);
        return;
      }

      if (migrationAnalyzeMatch && req.method === "GET") {
        const migId = decodeURIComponent(migrationAnalyzeMatch[1]);
        const objectId = decodeURIComponent(migrationAnalyzeMatch[2]);
        const migration = adminDataService.getMigration(migId);
        if (!migration) {
          sendJson(404, { error: "Migration not found" });
          return;
        }
        const obj = migration.objects.find((o) => o.id === objectId);
        if (!obj) {
          sendJson(404, { error: "Object not found" });
          return;
        }
        const previewOffset = Math.max(0, Number(requestUrl.searchParams.get("offset") || 0) || 0);
        const previewLimit = Math.max(1, Math.min(100, Number(requestUrl.searchParams.get("limit") || 10) || 10));
        const previewFilter = String(requestUrl.searchParams.get("filter") || "").trim();
        const previewStatusFilter = String(requestUrl.searchParams.get("status") || "").trim();
        const analysis = await adminDataService.analyzeMigrationObjectSource(migId, objectId, {
          offset: previewOffset,
          limit: previewLimit,
          filter: previewFilter,
          status: previewStatusFilter
        });
        sendJson(200, analysis);
        return;
      }

      if (failedRecordsMatch && req.method === "GET") {
        const migId = decodeURIComponent(failedRecordsMatch[1]);
        const failedRecordsId = decodeURIComponent(failedRecordsMatch[2]);
        const failedDir = path.join(process.cwd(), "artifacts", "migrations", migId, "failed-records");
        const failedFile = path.join(failedDir, `${failedRecordsId}.json`);
        try {
          const content = await fs.readFile(failedFile, "utf-8");
          const failedRecords = JSON.parse(content);
          sendJson(200, { records: failedRecords });
        } catch {
          sendJson(404, { error: "Failed records not found" });
        }
        return;
      }

      if (retryFailedRecordsMatch && req.method === "POST") {
        const migId = decodeURIComponent(retryFailedRecordsMatch[1]);
        const objectId = decodeURIComponent(retryFailedRecordsMatch[2]);
        const failedRecordsId = decodeURIComponent(retryFailedRecordsMatch[3]);
        const body = (await readJsonBody(req)) as {
          records?: Array<{ rowIndex: number; sourceRecord: Record<string, unknown> }>;
          mode?: string;
        };
        const records = Array.isArray(body.records) ? body.records : [];
        const result = String(body.mode || '') === 'stage'
          ? await adminDataService.saveFailedMigrationRecordCorrections(
              migId,
              objectId,
              records
            )
          : await adminDataService.retryFailedMigrationRecords(
              migId,
              objectId,
              failedRecordsId,
              records,
              instanceId || undefined
            );
        sendJson(200, result);
        return;
      }

      if (migrationIdMatch) {
        const migId = decodeURIComponent(migrationIdMatch[1]);
        if (req.method === "GET") {
          const m = adminDataService.getMigrationForUi(migId);
          if (!m) {
            sendJson(404, { error: "Migration not found" });
          } else {
            sendJson(200, m);
          }
          return;
        }
        if (req.method === "PUT" || req.method === "PATCH") {
          const body = (await readJsonBody(req)) as Partial<MigrationConfig>;
          const existing = adminDataService.getMigration(migId);
          if (!existing) {
            sendJson(404, { error: "Migration not found" });
            return;
          }
          const updated = adminDataService.saveMigration({ ...existing, ...body, id: migId });
          await appendAuditHistory({ actor: auditActor, action: "update", entityType: "migration", entityId: updated.id, entityName: updated.name });
          sendJson(200, adminDataService.getMigrationForUi(updated.id));
          return;
        }
        if (req.method === "DELETE") {
          const deleted = adminDataService.deleteMigration(migId);
          await appendAuditHistory({ actor: auditActor, action: "delete", entityType: "migration", entityId: migId, status: deleted ? "success" : "error" });
          sendJson(deleted ? 200 : 404, { ok: deleted });
          return;
        }
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/migrations") {
        const body = (await readJsonBody(req)) as Partial<MigrationConfig>;
        const id = body.id || `mig-${Date.now()}`;
        const saved = adminDataService.saveMigration({
          id,
          name: String(body.name || "Neue Migration"),
          description: body.description,
          batchSize: Number.isFinite(Number(body.batchSize)) ? Number(body.batchSize) : 200,
          salesforceLogin: body.salesforceLogin,
          instanceId: body.instanceId || instanceId || undefined,
          status: body.status || "draft",
          objects: body.objects || [],
          dependencies: body.dependencies || [],
          executionPlan: body.executionPlan || []
        });
        await appendAuditHistory({ actor: auditActor, action: "create", entityType: "migration", entityId: saved.id, entityName: saved.name });
        sendJson(201, adminDataService.getMigrationForUi(saved.id));
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/") {
        sendHtml(200, htmlShell());
        return;
      }

      sendJson(404, { error: "Not Found" });
    })().catch((error) => {
      res.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown server error" }));
    });
  });
}
