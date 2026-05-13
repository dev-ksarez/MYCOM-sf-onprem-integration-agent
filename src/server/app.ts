
import http from "node:http";
import { readFileSync } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import {
  AdminDataService,
  SalesforceInstanceMutationInput,
  SalesforceProjectMutationInput,
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
import { getDashboardUpdateStatus, triggerDashboardUpdate } from "./dashboard-update-service";
import { HealthSnapshot } from "./health-snapshot";
import { generateInstallerFiles, getInstallerSummary, INSTALLER_OUTPUT_DIR, InstallerGenerationInput } from "./installer-generator";
import { AISchedulerService } from "./ai-scheduler-service";
import { AIErrorAnalyzer, type RunErrorData } from "./ai-error-analyzer";
import { AIMigrationAnalyzer, type MigrationSourceData } from "./ai-migration-analyzer";
import { AIDashboardAnalyzer, type AIDashboardAnalysisInput } from "./ai-dashboard-analyzer";
import { generateSalesforceMappingRules } from "../core/mapping-dsl/salesforce-mapping-generator";
import { serveStaticAsset, UI_ASSET_VERSION } from "./asset-server";
import { appendAuditHistory, listAuditHistory } from "./audit-history-service";
import { renderAdminUiScript } from "./admin-ui-script";
import { listAppModules, renderMenuModuleNavigation, renderSidebarModuleNavigation } from "./app-modules";
import { renderHtmlDocument } from "./ui-template";
import { renderAISchedulerAssistantModule } from "./ai-scheduler-ui-module";

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

const APP_VERSION = (() => {
  try {
    const packageJsonPath = path.resolve(process.cwd(), "package.json");
    const parsed = JSON.parse(readFileSync(packageJsonPath, "utf8")) as { version?: unknown };
    const version = String(parsed.version || "").trim();
    return version || "-";
  } catch {
    return "-";
  }
})();

function renderLoginShell(options: { errorMessage?: string; csrfToken?: string; authMode?: "local" | "salesforce_oidc" } = {}): string {
  const errorMessage = options.errorMessage || "";
  const csrfToken = options.csrfToken || "";
  const authMode = options.authMode || "local";
  const safeErrorMessage = escapeHtml(errorMessage);
  return renderHtmlDocument({
    title: "SF Integration Agent Login",
    csrfToken: escapeHtml(csrfToken),
    stylesheets: ["/assets/bootstrap.min.css", "/assets/style.css", "/assets/login.css"],
    scripts: ["/assets/login.js"],
    body: `    <main class="agent-login-shell">
      <div class="card agent-login-card">
        <div class="card-body p-4 p-lg-5">
          <div class="mb-4">
            <div class="text-uppercase text-secondary small fw-semibold">Geschützter Bereich</div>
            <h1 class="h3 mb-2">SF Integration Agent</h1>
            <p class="text-secondary mb-0">${authMode === "salesforce_oidc" ? "Bitte mit Salesforce anmelden." : "Bitte mit Admin-Benutzer und Passwort anmelden."}</p>
          </div>
          <div class="agent-login-content">
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
          <div class="agent-login-meta">
            <span class="agent-login-version">v${escapeHtml(APP_VERSION)}</span>
          </div>
        </div>
      </div>
    </main>`
  });
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

function renderMigrationOauthCallbackShell(options: { ok: boolean; migrationId: string; message: string }): string {
  return renderHtmlDocument({
    title: "Salesforce Login",
    stylesheets: ["/assets/bootstrap.min.css"],
    scripts: ["/assets/migration-oauth-callback.js"],
    bodyClass: "bg-light d-flex align-items-center justify-content-center min-vh-100",
    body: `    <div
      id="migration-oauth-payload"
      class="card shadow-sm border-0"
      style="max-width:32rem; width:100%;"
      data-ok="${options.ok ? "true" : "false"}"
      data-migration-id="${escapeHtml(options.migrationId)}"
      data-message="${escapeHtml(options.message)}"
    >
      <div class="card-body p-4 text-center">
        <h1 class="h5 mb-3">${escapeHtml(options.ok ? "Salesforce-Freigabe gespeichert" : "Salesforce-Freigabe fehlgeschlagen")}</h1>
        <p class="text-secondary small mb-0">${escapeHtml(options.message)}</p>
      </div>
    </div>`
  });
}

function renderAuthConfigurationShell(): string {
  return renderHtmlDocument({
    title: "SF Integration Agent Konfiguration erforderlich",
    stylesheets: ["/assets/bootstrap.min.css", "/assets/style.css"],
    bodyClass: "bg-light",
    body: `    <main class="container py-5">
      <div class="card shadow-sm border-0 mx-auto" style="max-width: 40rem;">
        <div class="card-body p-4">
          <div class="text-uppercase text-secondary small fw-semibold mb-2">Sichere Voreinstellung</div>
          <h1 class="h4 mb-3">Admin-Zugang ist noch nicht konfiguriert</h1>
          <p class="text-secondary mb-3">Im Produktionsmodus bleibt die Web-UI gesperrt, bis ADMIN_UI_USERNAME und ADMIN_UI_PASSWORD gesetzt sind.</p>
          <div class="alert alert-warning mb-0" role="alert">Bitte die Environment-Datei ergänzen und den Dienst neu starten.</div>
        </div>
      </div>
    </main>`
  });
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
${renderSidebarModuleNavigation()}
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
            <div class="agent-topbar-brand">SF Integration Agent <span id="agent-version-label" class="agent-version-label">v${escapeHtml(APP_VERSION)}</span></div>
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
${renderMenuModuleNavigation()}
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
                  <button id="manage-projects" class="btn btn-outline-secondary agent-btn-subtle" aria-label="Projekte verwalten"><span class="agent-btn-icon" aria-hidden="true">▦</span><span>Projekte</span></button>
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
            <div class="col-12">
              <div class="card soft-card">
                <div class="card-header bg-white d-flex justify-content-between align-items-center flex-wrap gap-2">
                  <span class="fw-semibold">Agentenanalyse (KI)</span>
                  <span id="agent-analysis-status" class="badge bg-secondary">-</span>
                </div>
                <div class="card-body">
                  <div class="row g-3">
                    <div class="col-lg-2 col-md-4 col-6">
                      <div class="small text-secondary">Health-Score</div>
                      <div id="agent-analysis-score" class="h4 mb-0">-</div>
                    </div>
                    <div class="col-lg-3 col-md-4 col-6">
                      <div class="small text-secondary">Laufzeiten</div>
                      <div id="agent-analysis-runtime" class="small">-</div>
                    </div>
                    <div class="col-lg-3 col-md-4 col-12">
                      <div class="small text-secondary">Fehlerbild</div>
                      <div id="agent-analysis-errors" class="small">-</div>
                    </div>
                    <div class="col-lg-3 col-md-6 col-12">
                      <div class="small text-secondary">Datenwuchs</div>
                      <div id="agent-analysis-growth" class="small">-</div>
                    </div>
                    <div class="col-lg-1 col-md-6 col-12 text-md-end">
                      <div class="small text-secondary">Stand</div>
                      <div id="agent-analysis-updated" class="small">-</div>
                    </div>
                    <div class="col-12">
                      <div class="small text-secondary">KI-Zusammenfassung</div>
                      <div id="agent-analysis-summary" class="small">-</div>
                    </div>
                    <div class="col-12">
                      <div class="small text-secondary">KI-Empfehlungen</div>
                      <ul id="agent-analysis-recommendations" class="small mb-0 ps-3">
                        <li>-</li>
                      </ul>
                    </div>
                  </div>
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
                <div class="card-header bg-white d-flex justify-content-between align-items-center">
                  <span class="fw-semibold">Salesforce Org + Limits</span>
                  <span id="sf-api-throttle-badge" class="badge rounded-pill bg-secondary">Adaptive Cache: -</span>
                </div>
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

        <section class="tab-pane fade" id="tab-ai-assistant" role="tabpanel">
${renderAISchedulerAssistantModule()}
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
                    <button id="analyze-run-error" class="btn btn-outline-info" title="Analysiere Fehler mit KI-Assistent">⚡ Fehleranalyse</button>
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
                <div class="migration-card-title">🤖 KI-Datenquellen-Analyse</div>
                <div class="migration-card-subtitle">Datenquellen mit Fokus auf Datenschutz & automatisches Mapping analysieren</div>
              </div>
              <button id="migration-ai-analyze" type="button" class="btn btn-sm btn-info">⚡ Quelle analysieren</button>
            </div>
            <div class="card-body">
              <div class="row g-3">
                <div class="col-md-4">
                  <label class="form-label small">Quellname</label>
                  <input type="text" id="migration-source-name" class="form-control form-control-sm" placeholder="z.B. SAP_Customers" />
                </div>
                <div class="col-md-4">
                  <label class="form-label small">Quelltyp</label>
                  <select id="migration-source-type" class="form-select form-select-sm">
                    <option value="MSSQL_SQL">MSSQL/SQL Server</option>
                    <option value="REST_API">REST API</option>
                    <option value="FILE_CSV">CSV Datei</option>
                    <option value="FILE_XLSX">Excel Datei</option>
                    <option value="SALESFORCE">Salesforce</option>
                    <option value="OTHER">Sonstiges</option>
                  </select>
                </div>
                <div class="col-md-4">
                  <label class="form-label small">Salesforce Zielobjekt</label>
                  <select id="migration-target-object" class="form-select form-select-sm">
                    <option value="Contact" selected>Contact</option>
                    <option value="Account">Account</option>
                    <option value="Lead">Lead</option>
                    <option value="Opportunity">Opportunity</option>
                    <option value="Order">Order</option>
                    <option value="Product2">Product</option>
                    <option value="PricebookEntry">ProductPrice</option>
                  </select>
                </div>
                <div class="col-12">
                  <label class="form-label small">Datei für Auto-Analyse (optional)</label>
                  <div id="migration-analysis-dropzone" class="migration-list-drop-target p-3">
                    <div class="d-flex align-items-center gap-2 flex-wrap mb-2">
                      <button id="migration-analysis-file-pick" type="button" class="btn btn-sm btn-outline-primary">Datei auswählen</button>
                      <input id="migration-analysis-file" type="file" class="d-none" accept=".csv,.txt,.json,.xlsx,.xls" />
                      <small id="migration-analysis-file-meta" class="text-secondary">Keine Datei gewählt.</small>
                    </div>
                    <div class="migration-drop-target-hint">Datei hierher ziehen (CSV, TXT, JSON, Excel), um Felddefinitionen automatisch zu erzeugen.</div>
                  </div>
                  <div class="form-text">Die Datei wird lokal verarbeitet, Header/Feldnamen werden automatisch übernommen.</div>
                </div>
                <div class="col-12">
                  <label class="form-label small">Feld-Definitionen (JSON)</label>
                  <textarea id="migration-field-defs" class="form-control form-control-sm" rows="4" placeholder='[{"name":"FirstName","type":"varchar(100)"},{"name":"Email","type":"varchar(255)"}]'></textarea>
                </div>
                <div class="col-md-6">
                  <label class="form-label small">Geschätzte Records</label>
                  <input type="number" id="migration-est-records" class="form-control form-control-sm" placeholder="z.B. 10000" />
                </div>
                <div class="col-md-6">
                  <label class="form-label small">Beschreibung (optional)</label>
                  <input type="text" id="migration-description" class="form-control form-control-sm" placeholder="z.B. Kundenaddaten aus SAP" />
                </div>
              </div>
              <div id="migration-analysis-result" class="mt-3"></div>
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
          <div class="modal-header connector-wizard-header">
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
            <div id="mig-wizard-meta" class="connector-wizard-meta">Erstellt: - · Letzte Änderung: -</div>
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
              <div class="col-md-8"><label class="form-label">Projekt</label><select id="ins-project-id" class="form-select"></select></div>
              <div class="col-md-4"><label class="form-label">Rolle</label><select id="ins-role" class="form-select"><option value="test">Test</option><option value="production">Produktion</option></select></div>
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

    <div class="modal fade" id="project-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-lg">
        <div class="modal-content">
          <div class="modal-header"><h5 class="modal-title">Projekte verwalten</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body">
            <div class="table-responsive mb-3">
              <table class="table table-sm align-middle mb-0">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Schreibschutz Produktion</th>
                    <th>Status</th>
                    <th>Aktion</th>
                  </tr>
                </thead>
                <tbody id="project-table-body"></tbody>
              </table>
            </div>
            <hr class="my-3" />
            <div class="row g-2">
              <div class="col-12"><label class="form-label">Projekt-ID (optional)</label><input id="prj-id" class="form-control" placeholder="leer = automatisch aus Name" /></div>
              <div class="col-12"><label class="form-label">Name</label><input id="prj-name" class="form-control" placeholder="z. B. Annaburger Rollout" /></div>
              <div class="col-12"><label class="form-label">Beschreibung (optional)</label><input id="prj-description" class="form-control" /></div>
              <div class="col-12">
                <div class="form-check mt-1">
                  <input id="prj-production-write-protection" class="form-check-input" type="checkbox" checked />
                  <label class="form-check-label" for="prj-production-write-protection">Produktions-Schreibschutz aktiv</label>
                </div>
              </div>
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
            <button id="save-project" type="button" class="btn btn-primary">Projekt speichern</button>
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
          <div class="modal-header connector-wizard-header">
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
                  <div id="sch-target-file-options-wrap" class="col-md-12 d-none">
                    <div class="border rounded p-2 bg-light">
                      <div class="fw-semibold mb-2">Datei-Optionen</div>
                      <div class="row g-2">
                        <div class="col-md-4"><label class="form-label">Dateiname</label><input id="sch-target-file-name" class="form-control" placeholder="z. B. export_\${date}_\${time}.csv" /><div class="form-text">Platzhalter: \${date}, \${time}, \${datetime} oder %DATE%, %TIME%, %DATETIME%</div></div>
                        <div class="col-md-3"><label class="form-label">Charset</label><select id="sch-target-file-charset" class="form-select"><option value="utf8">UTF-8</option><option value="windows-1252">Windows-1252</option><option value="latin1">Latin-1</option><option value="utf16le">UTF-16 LE</option></select></div>
                        <div class="col-md-2"><label class="form-label">Separator</label><input id="sch-target-file-delimiter" class="form-control" placeholder=";" maxlength="1" /></div>
                        <div class="col-md-3"><label class="form-label">Textqualifier</label><input id="sch-target-file-text-qualifier" class="form-control" placeholder='"' maxlength="1" /></div>
                        <div class="col-md-4"><label class="form-label">Excel Sheet</label><input id="sch-target-file-sheet-name" class="form-control" placeholder="Sheet1" /></div>
                        <div class="col-md-8"><div class="small text-secondary mt-4">Die Datei-Header werden über die Mapping-Zielfelder definiert (Tab Mapping).</div></div>
                      </div>
                    </div>
                  </div>
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
            <div id="sch-wizard-meta" class="connector-wizard-meta">Erstellt: - · Letzte Änderung: -</div>
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
            <div id="con-wizard-meta" class="connector-wizard-meta">Erstellt: - · Letzte Änderung: -</div>
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

    <div class="modal fade" id="failed-records-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 id="failed-records-modal-title" class="modal-title">Fehlgeschlagene Datensätze</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body p-0">
            <div class="table-responsive">
              <table class="table table-sm mb-0">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Key</th>
                    <th>Status</th>
                    <th>Fehler</th>
                    <th>Quelle</th>
                    <th>Mapped</th>
                  </tr>
                </thead>
                <tbody id="failed-records-modal-body"></tbody>
              </table>
            </div>
          </div>
          <div class="modal-footer">
            <button id="failed-records-export-csv" type="button" class="btn btn-outline-primary btn-sm" disabled>CSV exportieren</button>
            <button id="failed-records-export-json" type="button" class="btn btn-outline-secondary btn-sm" disabled>JSON exportieren</button>
            <button type="button" class="btn btn-light btn-sm" data-bs-dismiss="modal">Schließen</button>
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
    <script src="/assets/admin-ui.js?v=${UI_ASSET_VERSION}"></script>
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
        requestUrl.pathname === "/favicon.ico" ||
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
        if (requestUrl.pathname === "/api/system/health" || requestUrl.pathname === "/favicon.ico" || isAssetRequest) {
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
      const projectArchiveMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/projects\/([^/]+)\/archive$/) : null;
      const projectDeleteMatch = req.method === "DELETE" ? requestUrl.pathname.match(/^\/api\/projects\/([^/]+)$/) : null;
      const runLogsMatch = req.method === "GET" ? requestUrl.pathname.match(/^\/api\/runs\/([^/]+)\/logs$/) : null;
      const runFailedRecordsMatch = req.method === "GET" ? requestUrl.pathname.match(/^\/api\/runs\/([^/]+)\/failed-records$/) : null;
      const requiresInstanceWriteCheck = (() => {
        if (!instanceId) {
          return false;
        }

        if (req.method === "POST" && (
          requestUrl.pathname === "/api/setup/import"
          || requestUrl.pathname === "/api/setup/deploy-ezb"
          || requestUrl.pathname === "/api/setup/create-custom-object-from-source"
          || requestUrl.pathname === "/api/schedules"
          || requestUrl.pathname === "/api/schedules/validate-config"
          || requestUrl.pathname === "/api/connectors"
          || requestUrl.pathname === "/api/runs/release-stale"
          || requestUrl.pathname === "/api/salesforce/create-field"
        )) {
          return true;
        }

        if (req.method === "POST" && (
          requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/active$/)
          || requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/checkpoint$/)
          || requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/duplicate$/)
          || requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)\/run$/)
          || requestUrl.pathname.match(/^\/api\/runs\/([^/]+)\/cancel$/)
        )) {
          return true;
        }

        if (req.method === "DELETE" && (
          requestUrl.pathname.match(/^\/api\/schedules\/([^/]+)$/)
          || requestUrl.pathname.match(/^\/api\/connectors\/([^/]+)$/)
        )) {
          return true;
        }

        return false;
      })();

      if (requiresInstanceWriteCheck) {
        adminDataService.assertInstanceWriteAllowed(instanceId, `${req.method} ${requestUrl.pathname}`);
      }

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

      if (req.method === "GET" && requestUrl.pathname === "/api/modules") {
        sendJson(200, { items: listAppModules() });
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

      if (req.method === "GET" && requestUrl.pathname === "/assets/admin-ui.js") {
        res.writeHead(200, {
          "Content-Type": "application/javascript; charset=utf-8",
          "Cache-Control": "no-cache, no-store, must-revalidate",
          Pragma: "no-cache",
          Expires: "0"
        });
        res.end(renderAdminUiScript());
        return;
      }

      if (req.method === "GET" && requestUrl.pathname.endsWith(".map")) {
        res.writeHead(204);
        res.end();
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/favicon.ico") {
        res.writeHead(204);
        res.end();
        return;
      }

      if (req.method === "GET" && await serveStaticAsset(requestUrl.pathname, res)) {
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/instances") {
        const items = adminDataService.listInstances();
        sendJson(200, { items, total: items.length });
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/api/projects") {
        const items = adminDataService.listProjects();
        sendJson(200, { items, total: items.length });
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/projects") {
        const body = (await readJsonBody(req)) as SalesforceProjectMutationInput;
        const item = adminDataService.saveProject(body);
        await appendAuditHistory({ actor: auditActor, action: body.id ? "update" : "create", entityType: "project", entityId: item.id, entityName: item.name });
        sendJson(200, item);
        return;
      }

      if (projectArchiveMatch) {
        const projectId = decodeURIComponent(projectArchiveMatch[1]);
        const body = (await readJsonBody(req)) as { archived?: boolean };
        const archived = body.archived === true;
        const item = adminDataService.setProjectArchived(projectId, archived);
        await appendAuditHistory({ actor: auditActor, action: archived ? "archive" : "unarchive", entityType: "project", entityId: item.id, entityName: item.name });
        sendJson(200, item);
        return;
      }

      if (projectDeleteMatch) {
        const projectId = decodeURIComponent(projectDeleteMatch[1]);
        const result = adminDataService.deleteProject(projectId);
        await appendAuditHistory({ actor: auditActor, action: result.deleted ? "delete" : "delete-noop", entityType: "project", entityId: projectId });
        sendJson(result.deleted ? 200 : 404, result);
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

      if (req.method === "POST" && requestUrl.pathname === "/api/ai/generate-scheduler") {
        const body = (await readJsonBody(req)) as {
          userPrompt?: string;
          connectorId?: string;
          targetSystem?: string;
          objectName?: string;
          existingConnectors?: any[];
        };

        const userPrompt = String(body.userPrompt || "").trim();
        if (!userPrompt) {
          sendJson(400, { error: "userPrompt ist erforderlich" });
          return;
        }

        const connectors = await adminDataService.listConnectors(instanceId);
        const aiService = new AISchedulerService();
        const result = await aiService.generateScheduler({
          userPrompt,
          connectorId: body.connectorId,
          targetSystem: body.targetSystem,
          objectName: body.objectName,
          existingConnectors: connectors
        });

        sendJson(200, result);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/ai/analyze-error") {
        const body = (await readJsonBody(req)) as {
          runId?: string;
          scheduleName?: string;
          sourceSystem?: string;
          targetSystem?: string;
          errorLog?: string;
          errorCode?: string;
          recordsProcessed?: number;
          failedRecords?: number;
        };

        const errorLog = String(body.errorLog || "").trim();
        if (!errorLog) {
          sendJson(400, { error: "errorLog ist erforderlich" });
          return;
        }

        const errorAnalyzer = new AIErrorAnalyzer();
        const analysis = await errorAnalyzer.analyzeRunError({
          runId: body.runId || "unknown",
          scheduleName: body.scheduleName || "Unknown Schedule",
          sourceSystem: body.sourceSystem || "Unknown",
          targetSystem: body.targetSystem || "Unknown",
          errorLog,
          errorCode: body.errorCode,
          recordsProcessed: body.recordsProcessed,
          failedRecords: body.failedRecords,
          timestamp: new Date()
        });

        // Audit-Log für Fehleranalyse
        await appendAuditHistory({
          action: "AI_ERROR_ANALYSIS",
          entityType: "run",
          entityId: body.runId,
          entityName: body.scheduleName,
          status: analysis.severity === "critical" ? "error" : "success",
          message: `${analysis.errorCategory}: ${analysis.rootCause.substring(0, 100)}`
        });

        sendJson(200, analysis);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/ai/analyze-migration-source") {
        const body = (await readJsonBody(req)) as {
          sourceName?: string;
          sourceType?: string;
          targetObject?: "Account" | "Contact" | "Lead" | "Opportunity" | "Order" | "Product2" | "PricebookEntry";
          sampleData?: Record<string, unknown>[];
          fieldDefinitions?: Array<{ name: string; type: string; nullable?: boolean; sampleValues?: unknown[] }>;
          estimatedRecords?: number;
          description?: string;
        };

        if (!body.sourceName) {
          sendJson(400, { error: "sourceName ist erforderlich" });
          return;
        }

        const migrationAnalyzer = new AIMigrationAnalyzer();
        const allowedTargetObjects = new Set(["Account", "Contact", "Lead", "Opportunity", "Order", "Product2", "PricebookEntry"]);
        const requestedTargetObject = String(body.targetObject || "").trim();
        const analysis = await migrationAnalyzer.analyzeMigrationSource({
          sourceName: body.sourceName,
          sourceType: (body.sourceType as MigrationSourceData["sourceType"]) || "OTHER",
          targetObject: (allowedTargetObjects.has(requestedTargetObject) ? requestedTargetObject : "Contact") as MigrationSourceData["targetObject"],
          sampleData: body.sampleData,
          fieldDefinitions: body.fieldDefinitions,
          estimatedRecords: body.estimatedRecords,
          description: body.description
        });

        // Audit-Log für Migrations-Analyse (sensitive!)
        await appendAuditHistory({
          action: "AI_MIGRATION_ANALYSIS",
          entityType: "migration",
          entityId: body.sourceName,
          entityName: `Analysis: ${body.sourceName}`,
          status: analysis.sensitiveFields.length > 0 ? "success" : "success",
          message: `${analysis.totalFields} Felder, ${analysis.sensitiveFields.length} sensitiv, Qualität: ${Math.round(analysis.dataQualityScore * 100)}%`
        });

        sendJson(200, analysis);
        return;
      }

      if (req.method === "POST" && requestUrl.pathname === "/api/ai/analyze-dashboard") {
        const body = (await readJsonBody(req)) as AIDashboardAnalysisInput;
        const dashboardAnalyzer = new AIDashboardAnalyzer();
        const analysis = dashboardAnalyzer.analyze(body || {});

        await appendAuditHistory({
          action: "AI_DASHBOARD_ANALYSIS",
          entityType: "dashboard",
          entityId: "overview",
          entityName: "Agentenanalyse",
          status: analysis.status === "Kritisch" ? "error" : "success",
          message: analysis.summary.substring(0, 160)
        });

        sendJson(200, analysis);
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

      if (runFailedRecordsMatch) {
        const runId = decodeURIComponent(runFailedRecordsMatch[1]);
        const failedRecords = adminDataService.getRunFailedRecords(runId);
        sendJson(200, failedRecords);
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
