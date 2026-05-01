import http from "node:http";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  AdminDataService,
  SalesforceInstanceMutationInput,
  ConnectorMutationInput,
  ScheduleMutationInput,
  LogChartRange,
  SetupExportDocument,
  MigrationConfig,
  ScheduleFormOptions
} from "./admin-data-service";

const BOOTSTRAP_CSS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/css/bootstrap.min.css");
const BOOTSTRAP_JS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/js/bootstrap.bundle.min.js");
const CHART_JS_FILE = path.resolve(process.cwd(), "node_modules/chart.js/dist/chart.umd.js");
const APP_STYLE_CSS_FILE = path.resolve(process.cwd(), "src/css/style.css");
const AGENT_UI_CSS_FILE = path.resolve(process.cwd(), "src/css/agent-ui.css");
const SETUP_EXAMPLE_JSON_FILE = path.resolve(
  process.cwd(),
  "artifacts/file-examples/setup-file-import-export.example.json"
);

export interface HealthSnapshot {
  service: "ok" | "degraded";
  scheduler: "running" | "idle" | "error";
  startedAt: string;
  uptimeSeconds: number;
  cpuLoadPercent?: number;
  lastRunStartedAt?: string;
  lastRunFinishedAt?: string;
  lastRunStatus?: "success" | "error";
  lastRunError?: string;
  schedulesFound?: number;
  dueSchedules?: number;
  processedSchedules?: number;
}

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

function getCpuLoadPercent(): number | undefined {
  const [load1m] = os.loadavg();
  const coreCount = os.cpus().length;
  if (!Number.isFinite(load1m) || coreCount <= 0) {
    return undefined;
  }

  return Math.max(0, Math.min(100, Math.round((load1m / coreCount) * 100)));
}

function htmlShell(): string {
  return `<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>SF Integration Agent</title>
    <link href="/assets/bootstrap.min.css" rel="stylesheet" />
    <link href="/assets/style.css" rel="stylesheet" />
    <link href="/assets/agent-ui.css" rel="stylesheet" />
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
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-schedulers" type="button"><span class="agent-tab-icon">◷</span>Scheduler</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-connectors" type="button"><span class="agent-tab-icon">◫</span>Connectoren</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-monitor" type="button"><span class="agent-tab-icon">◉</span>Monitoring</button></li>
          <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-migration" type="button"><span class="agent-tab-icon">⊞</span>Migration</button></li>
        </ul>
      </aside>

      <div class="agent-main">
        <nav class="agent-topbar">
          <div class="agent-topbar-brand">SF Integration Agent</div>
          <div class="agent-navbar-actions ms-auto">
            <div class="agent-instance-picker d-flex gap-2 align-items-center">
              <label class="small text-secondary">Instanz</label>
              <select id="instance-select" class="form-select form-select-sm"></select>
            </div>
            <div class="agent-theme-picker d-flex gap-2 align-items-center">
              <label class="small text-secondary">Theme</label>
              <select id="theme-select" class="form-select form-select-sm">
                <option value="corporate">Corporate Light</option>
                <option value="industrial">Industrial Blue</option>
                <option value="midnight">Midnight Dark</option>
              </select>
            </div>
            <div class="btn-group btn-group-sm" role="group" aria-label="Setup Aktionen">
              <button id="export-setup" class="btn btn-outline-secondary agent-btn-subtle" title="Setup exportieren"><span class="agent-btn-icon" aria-hidden="true">⭳</span><span>Export</span></button>
              <button id="import-setup" class="btn btn-outline-secondary agent-btn-subtle" title="Setup importieren"><span class="agent-btn-icon" aria-hidden="true">⭱</span><span>Import</span></button>
            </div>
            <input id="setup-import-input" type="file" accept="application/json" class="d-none" />
            <div class="btn-group btn-group-sm" role="group" aria-label="Instanz Aktionen">
              <button id="add-instance" class="btn btn-outline-secondary agent-btn-subtle" title="Instanz hinzufügen"><span class="agent-btn-icon" aria-hidden="true">＋</span><span>Instanz</span></button>
              <button id="refresh-all" class="btn btn-outline-secondary agent-btn-subtle" title="Aktualisieren"><span class="agent-btn-icon" aria-hidden="true">↻</span><span>Refresh</span></button>
            </div>
          </div>
        </nav>

        <main class="container-fluid px-4 py-4 agent-content">
      <div id="global-alert" class="alert alert-danger d-none" role="alert"></div>

      <div class="tab-content">
        <section class="tab-pane fade show active" id="tab-overview" role="tabpanel">
          <div class="row g-3 mb-3">
              <div class="col-md-3"><div class="card soft-card mini-kpi mini-kpi-service h-100"><div class="card-body"><div class="text-secondary small">Service</div><h5 id="kpi-service" class="mb-0">-</h5><div class="kpi-meter"><div id="kpi-service-cpu-bar" class="kpi-meter-fill" style="width:0%"></div></div><div class="kpi-service-footer"><div id="kpi-service-cpu-text" class="kpi-inline-metric">CPU Last: -</div><div class="kpi-sparkline-wrap" aria-hidden="true"><svg id="kpi-service-cpu-sparkline" class="kpi-sparkline" viewBox="0 0 120 20" preserveAspectRatio="xMidYMid meet"><path id="kpi-service-cpu-sparkline-path" class="kpi-sparkline-path" d=""></path><circle id="kpi-service-cpu-sparkline-dot" class="kpi-sparkline-dot" r="2" cx="0" cy="0"></circle></svg></div></div><div id="kpi-service-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Scheduler</div><h5 id="kpi-scheduler" class="mb-0">-</h5><div id="kpi-scheduler-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Aktive Scheduler</div><h5 id="kpi-schedules" class="mb-0">0</h5><div id="kpi-schedules-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi h-100"><div class="card-body"><div class="text-secondary small">Connectoren</div><h5 id="kpi-connectors" class="mb-0">0</h5><div id="kpi-connectors-trend" class="kpi-trend kpi-trend-neutral">• warten auf Daten</div></div></div></div>
          </div>
          <div class="d-flex justify-content-between align-items-center mb-2">
            <div class="small text-secondary">Dashboard Zeitraum</div>
            <div id="overview-stats-range" class="btn-group btn-group-sm overview-stats-range" role="group" aria-label="Dashboard Zeitraum">
              <button type="button" class="btn btn-outline-secondary" data-range="day">Heute</button>
              <button type="button" class="btn btn-outline-secondary active" data-range="month">Monat</button>
              <button type="button" class="btn btn-outline-secondary" data-range="year">Jahr</button>
            </div>
          </div>
          <div class="row g-3 mb-3">
            <div class="col-lg-4">
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
            <div class="col-lg-4">
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
            <div class="col-lg-4">
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
          </div>
          <div class="row g-3 mb-3">
            <div class="col-lg-6">
              <div class="card soft-card h-100">
                <div class="card-header bg-white d-flex justify-content-between align-items-center">
                  <span class="fw-semibold">Log + Fehler Verlauf</span>
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
                <div class="card-header bg-white fw-semibold">Datensätze Verlauf</div>
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
                  <div class="stats-row"><span class="stats-label">Umgebung</span><span id="sf-environment" class="stats-value">-</span></div>
                  <div class="stats-row"><span class="stats-label">API Calls</span><span id="sf-api-usage" class="stats-value">-</span></div>
                  <div class="stats-row"><span class="stats-label">Datenspeicher</span><span id="sf-data-storage" class="stats-value">-</span></div>
                  <div class="stats-row"><span class="stats-label">Dateispeicher</span><span id="sf-file-storage" class="stats-value">-</span></div>
                  <div class="stats-row mb-0"><span class="stats-label">Lizenzen</span><span id="sf-licenses" class="stats-value">-</span></div>
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

        <section class="tab-pane fade" id="tab-schedulers" role="tabpanel">
          <div class="card soft-card">
            <div class="card-header bg-white d-flex justify-content-between align-items-center">
              <span class="fw-semibold">Scheduler-Verwaltung</span>
              <button id="new-schedule" class="btn btn-sm btn-primary">Neuer Scheduler</button>
            </div>
            <div class="card-body p-0">
              <div class="table-responsive">
                <div class="px-2 pt-2">
                  <ul class="nav nav-pills nav-fill" id="schedulers-direction-tabs">
                    <li class="nav-item"><button class="nav-link active" type="button" data-direction-tab="all">Alle</button></li>
                    <li class="nav-item"><button class="nav-link" type="button" data-direction-tab="inbound">Inbound</button></li>
                    <li class="nav-item"><button class="nav-link" type="button" data-direction-tab="outbound">Outbound</button></li>
                  </ul>
                </div>
                <div class="d-flex flex-column flex-lg-row gap-2 p-2">
                  <input type="search" class="form-control form-control-sm" placeholder="Suche Scheduler..." id="schedulers-filter" />
                  <select id="schedulers-connector-filter" class="form-select form-select-sm" style="max-width: 260px;">
                    <option value="">Alle Connectoren</option>
                  </select>
                </div>
                <div id="schedulers-auto-disabled-warning" class="alert alert-warning mx-2 mb-2 py-2 d-none" role="alert"></div>
                <table class="table table-hover mb-0" id="schedulers-table">
                  <thead><tr><th data-sortable="true">Name</th><th>Parent</th><th>Aktiv</th><th>Status</th><th>Connector</th><th>Intervall</th><th>Nächster Lauf</th><th>Fehler</th><th>Aktion</th></tr></thead>
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
              <button id="new-connector" class="btn btn-sm btn-primary">Neuer Connector</button>
            </div>
            <div class="card-body p-0">
              <div class="table-responsive">
                <input type="search" class="form-control form-control-sm mb-2" placeholder="Suche Connectoren..." id="connectors-filter" />
                <table class="table table-hover mb-0" id="connectors-table">
                  <thead><tr><th data-sortable="true">Name</th><th>Typ</th><th>Status</th><th>Parameter</th><th>Aktion</th></tr></thead>
                  <tbody id="connectors-body"></tbody>
                </table>
              </div>
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
                    <thead><tr><th>Schedule</th><th>Status</th><th>Ergebnis</th><th>Logs</th></tr></thead>
                    <tbody id="runs-body"></tbody>
                  </table>
                </div>
              </div>
            </div>
            <div class="col-lg-6">
              <div class="card soft-card mb-3">
                <div class="card-header bg-white fw-semibold">Run-Logs</div>
                <div class="card-body">
                  <div class="input-group mb-2">
                    <select id="log-run-select" class="form-select"></select>
                    <button id="load-logs" class="btn btn-outline-primary">Laden</button>
                  </div>
                  <pre id="logs-output" class="bg-dark text-light p-3 rounded small mb-0">Noch keine Logs geladen.</pre>
                </div>
              </div>
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">SQL / Mapping Vorschau</div>
                <div class="card-body">
                  <div class="input-group mb-2">
                    <select id="sql-connector-select" class="form-select"></select>
                    <button id="preview-sql" class="btn btn-outline-primary">SQL testen</button>
                  </div>
                  <textarea id="sql-query" class="form-control mb-2" rows="3" placeholder="SELECT ..."></textarea>
                  <textarea id="mapping-definition" class="form-control mb-2" rows="2" placeholder="target;string=source;NONE"></textarea>
                  <textarea id="mapping-source" class="form-control mb-2" rows="3" placeholder='[{"source":"value"}]'></textarea>
                  <button id="preview-mapping" class="btn btn-outline-secondary btn-sm mb-2">Mapping prüfen</button>
                  <pre id="mapping-output" class="bg-dark text-light p-3 rounded small mb-0">Noch keine Vorschau.</pre>
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
              <span class="fw-semibold">Daten-Migration</span>
              <button id="new-migration" class="btn btn-sm btn-primary">+ Neue Migration</button>
            </div>
            <div class="card-body p-0">
              <table class="table table-sm mb-0" id="migration-list-table">
                <thead><tr><th>Name</th><th>Status</th><th>Objekte</th><th>Letzter Lauf</th><th>Aktionen</th></tr></thead>
                <tbody id="migration-list-body"><tr><td colspan="5" class="text-secondary">Keine Migrationen vorhanden.</td></tr></tbody>
              </table>
            </div>
          </div>
        </section>
      </div>
        </main>
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
            <!-- Wizard Steps Indicator -->
            <div class="connector-wizard-steps mb-4" id="mig-wizard-steps">
              <button type="button" class="connector-wizard-step is-active" data-mig-step="1"><span class="connector-wizard-step-index">1</span><span>Objekte</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="2"><span class="connector-wizard-step-index">2</span><span>Dateien</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="3"><span class="connector-wizard-step-index">3</span><span>Mapping</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="4"><span class="connector-wizard-step-index">4</span><span>Abhängigkeiten</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="5"><span class="connector-wizard-step-index">5</span><span>Reihenfolge</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="6"><span class="connector-wizard-step-index">6</span><span>Felder anlegen</span></button>
              <button type="button" class="connector-wizard-step" data-mig-step="7"><span class="connector-wizard-step-index">7</span><span>Ausführen</span></button>
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
              </div>
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
            </div>

            <!-- Step 2: Dateien zuordnen -->
            <div class="mig-wizard-panel d-none" data-mig-step-panel="2">
              <h6 class="fw-semibold mb-3">Schritt 2: Quelldateien den Objekten zuordnen</h6>
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
                  <div class="spinner-border spinner-border-sm text-primary" role="status"></div>
                  <span>Migration läuft...</span>
                </div>
                <div id="mig-run-steps"></div>
              </div>
              <div id="mig-run-result" class="d-none"></div>
            </div>

          </div>
          <div class="modal-footer d-flex justify-content-between">
            <button type="button" class="btn btn-outline-secondary" id="mig-wizard-prev" disabled>← Zurück</button>
            <div class="d-flex gap-2">
              <button type="button" class="btn btn-outline-primary" id="mig-wizard-save">Zwischenspeichern</button>
              <button type="button" class="btn btn-primary" id="mig-wizard-next">Weiter →</button>
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

    <div class="modal fade" id="schedule-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-xl modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Scheduler konfigurieren</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div id="sch-modal-error" class="alert alert-danger d-none mx-3 mt-2 mb-0 py-2" role="alert" style="font-size:0.875rem"></div>
          <div class="modal-body">
            <input id="sch-id" type="hidden" />
            
            <!-- Tab Navigation -->
            <ul class="nav nav-tabs mb-3" id="schedule-tabs" role="tablist">
              <li class="nav-item" role="presentation">
                <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#sch-tab-general" type="button" role="tab">Allgemein</button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#sch-tab-source" type="button" role="tab">Datenquelle</button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#sch-tab-target" type="button" role="tab">Datenziel</button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#sch-tab-timing" type="button" role="tab">Zeitsteuerung</button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link" data-bs-toggle="tab" data-bs-target="#sch-tab-mapping" type="button" role="tab">Mapping</button>
              </li>
            </ul>

            <!-- Tab Content -->
            <div class="tab-content" id="schedule-tab-content">
              
              <!-- Tab 1: Allgemein -->
              <div class="tab-pane fade show active" id="sch-tab-general" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-6"><label class="form-label">Name</label><input id="sch-name" class="form-control" /></div>
                  <div class="col-md-6"><label class="form-label">Connector</label><select id="sch-connector" class="form-select"></select></div>
                  <div class="col-md-6"><label class="form-label">Parent Scheduler</label><select id="sch-parent-schedule" class="form-select"><option value="">- Kein Parent -</option></select></div>
                  <div class="col-md-6 d-flex align-items-end"><div class="form-check"><input id="sch-inherit-parent-timing" class="form-check-input" type="checkbox" /><label class="form-check-label">Zeitsteuerung vom Parent übernehmen</label></div></div>
                  <div class="col-md-12"><label class="form-label">Batch Size</label><input id="sch-batch-size" type="number" class="form-control" value="100" /></div>
                  <div class="col-md-6"><label class="form-label">Nächster Lauf</label><input id="sch-next-run" type="datetime-local" class="form-control" /></div>
                  <div class="col-md-6"><label class="form-label">Letzter Lauf</label><input id="sch-last-run" type="datetime-local" class="form-control" readonly /></div>
                  <div class="col-md-12 d-flex align-items-end"><div class="form-check"><input id="sch-active" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Aktiv</label></div></div>
                </div>
              </div>
              
              <!-- Tab 2: Datenquelle -->
              <div class="tab-pane fade" id="sch-tab-source" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-6"><label class="form-label">Source System</label><select id="sch-source-system" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-6"><label class="form-label">Source Type</label><select id="sch-source-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE_SOQL">SALESFORCE_SOQL</option><option value="MSSQL_SQL">MSSQL_SQL</option><option value="REST_API">REST_API</option><option value="FILE_CSV">FILE_CSV</option><option value="FILE_EXCEL">FILE_EXCEL</option><option value="FILE_JSON">FILE_JSON</option></select></div>
                  <div class="col-md-12"><label class="form-label">Source Definition (JSON)</label><textarea id="sch-source-definition" class="form-control" rows="4" placeholder='{"fields":[...]}'></textarea></div>
                  <div class="col-md-12 d-flex gap-2 align-items-center">
                    <button id="sch-test-source" type="button" class="btn btn-outline-primary btn-sm">Quelle testen</button>
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
              <div class="tab-pane fade" id="sch-tab-target" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-4"><label class="form-label">Target System</label><select id="sch-target-system" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Objekt</label><select id="sch-object" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Operation</label><select id="sch-operation" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-4"><label class="form-label">Target Type</label><select id="sch-target-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE">SALESFORCE</option><option value="SALESFORCE_GLOBAL_PICKLIST">SALESFORCE_GLOBAL_PICKLIST</option><option value="MSSQL">MSSQL</option><option value="FILE_CSV">FILE_CSV</option><option value="FILE_EXCEL">FILE_EXCEL</option><option value="FILE_JSON">FILE_JSON</option></select></div>
                  <div class="col-md-4"><label class="form-label">Direction</label><select id="sch-direction" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-12"><label class="form-label">Target Definition (JSON)</label><textarea id="sch-target-definition" class="form-control" rows="4" placeholder='{"fields":[...]}'></textarea></div>
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
              <div class="tab-pane fade" id="sch-tab-timing" role="tabpanel">
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
              <div class="tab-pane fade" id="sch-tab-mapping" role="tabpanel">
                <div class="row g-2">
                  <div class="col-md-12">
                    <div class="mb-3">
                      <div class="d-flex justify-content-between align-items-center mb-2">
                        <div>
                          <div class="fw-semibold">Verfügbare Quellfelder</div>
                          <div class="small text-secondary">Felder aus dem Quellobjekt (DragDrop aktivieren)</div>
                        </div>
                          <div class="d-flex gap-2">
                            <button id="sch-automapping" type="button" class="btn btn-outline-success btn-sm">Auto-Mapping</button>
                            <button id="sch-load-source-fields" type="button" class="btn btn-outline-secondary btn-sm">Felder laden</button>
                          </div>
                      </div>
                      <div class="border rounded p-2 bg-light" style="max-height: 200px; overflow-y: auto;">
                        <table class="table table-sm mb-0">
                          <thead><tr><th>Feldname</th><th>Typ</th></tr></thead>
                          <tbody id="sch-mapping-source-fields"></tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-12">
                    <div class="mb-3">
                      <div class="fw-semibold mb-2">Mapping-Regeln</div>
                      <div id="sch-mapping-rules-dropzone" class="border rounded p-2 bg-light" style="max-height: 250px; overflow-y: auto;">
                        <table class="table table-sm mb-0">
                          <thead><tr><th>Quelle</th><th>Ziel</th><th>Lookup</th><th>Funktion</th><th>Picklist</th><th>Aktion</th></tr></thead>
                          <tbody id="sch-mapping-rules"></tbody>
                        </table>
                      </div>
                      <div class="small text-secondary mt-2">Quellfelder per DragDrop in diese Tabelle ziehen. Klick auf eine Zeile öffnet die Bearbeitung.</div>
                    </div>
                  </div>
                  <div class="col-md-12">
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
                    <div class="mb-3">
                      <div class="fw-semibold mb-2">Datenvorschau (ca. 10 Sätze)</div>
                      <div class="border rounded p-2 bg-light" style="max-height: 300px; overflow-y: auto;">
                        <table class="table table-sm mb-0">
                          <thead id="sch-mapping-preview-header"></thead>
                          <tbody id="sch-mapping-preview-body"></tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                  <div class="col-md-12">
                    <label class="form-label">Mapping Definition (JSON)</label>
                    <textarea id="sch-mapping" class="form-control" rows="4" placeholder='Mapping-Definition im JSON/DSL Format'></textarea>
                  </div>
                </div>
              </div>
              
            </div>
          </div>
          <div class="modal-footer">
            <button id="duplicate-schedule" type="button" class="btn btn-outline-secondary">Duplizieren</button>
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
            <button id="save-schedule" type="button" class="btn btn-primary">Speichern</button>
          </div>
        </div>
      </div>
    </div>

    <div class="modal fade" id="connector-modal" tabindex="-1" aria-hidden="true">
      <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header"><h5 class="modal-title">Connector-Assistent</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body">
            <input id="con-id" type="hidden" />
            <div id="con-modal-error" class="alert alert-danger d-none mb-3 py-2" role="alert"></div>
            <div class="connector-wizard-steps mb-3" id="con-wizard-steps">
              <button type="button" class="connector-wizard-step is-active" data-step="1"><span class="connector-wizard-step-index">1</span><span>Typ</span></button>
              <button type="button" class="connector-wizard-step" data-step="2"><span class="connector-wizard-step-index">2</span><span>Basis</span></button>
              <button type="button" class="connector-wizard-step" data-step="3"><span class="connector-wizard-step-index">3</span><span>Parameter</span></button>
              <button type="button" class="connector-wizard-step" data-step="4"><span class="connector-wizard-step-index">4</span><span>Prüfen</span></button>
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
              </div>
            </div>

            <div class="connector-wizard-panel d-none" data-step-panel="3">
              <div class="row g-2">
                <div class="col-md-12"><label class="form-label">Parameters (JSON)</label><textarea id="con-parameters" class="form-control" rows="4" placeholder='{"server":"...","database":"..."}'></textarea></div>
                <div class="col-12 d-none" id="con-mssql-settings-wrap">
                <div class="border rounded p-2 bg-light">
                  <div id="con-sql-settings-title" class="fw-semibold mb-2">SQL Verbindung</div>
                  <div id="con-sql-settings-text" class="small text-secondary mb-2">Pflicht: Server, Datenbank und Benutzer. Passwort kann direkt eingegeben werden. Alternativ kann das Passwort über Secret Key (ENV) aus einer Umgebungsvariable gelesen werden.</div>
                  <div class="row g-2">
                    <div class="col-md-4"><label class="form-label">Server / Host</label><input id="con-mssql-server" class="form-control" placeholder="sql.example.local" /></div>
                    <div class="col-md-2"><label class="form-label">Port</label><input id="con-mssql-port" type="number" class="form-control" placeholder="1433" /></div>
                    <div class="col-md-3"><label class="form-label">Datenbank</label><input id="con-mssql-database" class="form-control" placeholder="ERP" /></div>
                    <div class="col-md-3"><label class="form-label">Benutzer</label><input id="con-mssql-user" class="form-control" placeholder="etl_user" /></div>
                    <div class="col-md-4"><label class="form-label">Passwort</label><input id="con-mssql-password" type="password" class="form-control" placeholder="Optional: direkt speichern" autocomplete="new-password" /></div>
                    <div class="col-md-3 d-flex align-items-end"><div class="form-check"><input id="con-mssql-encrypt" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Encrypt</label></div></div>
                    <div class="col-md-5 d-flex align-items-end"><div class="form-check"><input id="con-mssql-trust-server-certificate" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Trust Server Certificate</label></div></div>
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
          </div>
          <div class="modal-footer">
            <button id="con-wizard-back" type="button" class="btn btn-outline-secondary">Zurück</button>
            <button id="con-wizard-next" type="button" class="btn btn-outline-primary">Weiter</button>
            <button id="test-connector" type="button" class="btn btn-outline-secondary">Speichern und validieren</button>
            <button type="button" class="btn btn-light" data-bs-dismiss="modal">Schließen</button>
            <button id="save-connector" type="button" class="btn btn-primary">Speichern</button>
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
              <table class="table table-sm mb-0" id="logs-table">
                <thead>
                  <tr>
                    <th>Zeit</th>
                    <th>Level</th>
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

    <script src="/assets/chart.umd.js"></script>
    <script src="/assets/bootstrap.bundle.min.js"></script>
    <script>
      const LOG_CHART_RANGE_STORAGE_KEY = 'sf-agent.logChartRange';
      const UI_THEME_STORAGE_KEY = 'sf-agent.uiTheme';
      const OVERVIEW_STATS_RANGE_STORAGE_KEY = 'sf-agent.overviewStatsRange';
      const state = {
        instanceId: '',
        schedules: [],
        connectors: [],
        cpuLoadHistory: [],
        connectorWizardStep: 1,
        previousOverviewSnapshot: null,
        overviewStatsRange: 'month',
        graphData: { nodes: [], edges: [] },
        overviewConnectorFilterId: '',
        schedulerConnectorFilterId: '',
        schedulerDirectionTab: 'all',
        runs: [],
        mappingFields: [],
        targetFields: [],
        mappingRules: [],
        selectedMappingRuleId: '',
        logSummary: null,
        salesforceOverview: null,
        customObjectFieldOverrides: {},
        scheduleOptions: {
          objectNames: [],
          operations: [],
          sourceSystems: [],
          targetSystems: [],
          directions: []
        }
      };

      // Migration wizard state - global to avoid hoisting issues
      let migState = {
        id: null,
        step: 1,
        totalSteps: 7,
        name: '',
        description: '',
        objects: [],
        dependencies: [],
        executionPlan: [],
        sfObjects: []
      };

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
      const instanceModal = createModalController('instance-modal');
      const logsModal = createModalController('logs-modal');

      function esc(value) {
        return String(value ?? '-')
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;');
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

      function withInstance(path) {
        const url = new URL(path, window.location.origin);
        if (state.instanceId) {
          url.searchParams.set('instanceId', state.instanceId);
        }
        return url.pathname + url.search;
      }

      async function requestJson(path, options) {
        const response = await fetch(withInstance(path), options);
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

      function getConnectorNameById(connectorId) {
        if (!connectorId) return '-';
        const connector = state.connectors?.find((item) => item.id === connectorId);
        return connector ? connector.name : connectorId;
      }

      function getStatusBadge(status) {
        if (!status) return '<span class="badge bg-secondary">Unbekannt</span>';
        const lowerStatus = String(status).toLowerCase();
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
        if (value.includes('mssql') || value.includes('sql')) return '🗄';
        if (value.includes('csv') || value.includes('excel')) return '📄';
        if (value.includes('mock') || value.includes('test')) return '🧪';
        if (value.includes('sage')) return '📚';
        return '⚙';
      }

      function getConnectorGraphClass(connectorType, connectorName) {
        const value = String(connectorType || connectorName || '').toLowerCase();
        if (value.includes('salesforce')) return 'graph-connector-salesforce';
        if (value.includes('mssql') || value.includes('sql')) return 'graph-connector-mssql';
        if (value.includes('csv') || value.includes('excel')) return 'graph-connector-file';
        if (value.includes('mock') || value.includes('test')) return 'graph-connector-mock';
        if (value.includes('sage')) return 'graph-connector-erp';
        return 'graph-connector-generic';
      }

      function splitGraphText(value, maxChars, maxLines) {
        const text = String(value || '').trim();
        if (!text) return [];

        const words = text.split(/\s+/).filter(Boolean);
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
      }

      function ensureSalesforceTargetDefinition() {
        const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
        const targetSystem = resolveEffectiveTargetSystem();
        if (targetType !== 'SALESFORCE' || targetSystem !== 'Salesforce') {
          return;
        }

        const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
        if (!objectApiName) {
          return;
        }

        const targetDefinitionInput = document.getElementById('sch-target-definition');
        const raw = String(targetDefinitionInput?.value || '').trim();
        const nextDefinition = {
          objectApiName,
          operation: normalizeOperationValue(document.getElementById('sch-operation')?.value || 'Upsert') || 'Upsert'
        };

        if (!raw) {
          targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
          return;
        }

        try {
          const parsed = JSON.parse(raw);
          parsed.objectApiName = objectApiName;
          if (!parsed.operation) {
            parsed.operation = nextDefinition.operation;
          }
          targetDefinitionInput.value = JSON.stringify(parsed, null, 2);
        } catch {
          targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
        }
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
            return String(parsed?.externalIdField || '').trim();
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
          targetField: rule.targetField,
          lookupEnabled: !!rule.lookupEnabled,
          lookupObject: rule.lookupObject || '',
          lookupField: rule.lookupField || '',
          transformFunction: rule.transformFunction || 'NONE',
          transformExpression: rule.transformExpression || '',
          picklistMappings: Array.isArray(rule.picklistMappings) ? rule.picklistMappings : []
        };
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
        mappingInput.value = JSON.stringify(state.mappingRules.map(toStoredMappingRule), null, 2);
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

      async function loadTargetObjects(selectedObjectName) {
        const targetSystem = resolveEffectiveTargetSystem();
        const connectorId = document.getElementById('sch-connector').value;

        if (!targetSystem) {
          renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
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

          const objects = Array.isArray(result.objects) ? result.objects : [];
          if (!objects.length) {
            renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
            return;
          }

          renderTargetObjectOptions(objects, selectedObjectName || '');
        } catch {
          renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
        }
      }

      async function loadTargetFields() {
        const targetSystem = resolveEffectiveTargetSystem();
        const objectName = document.getElementById('sch-object').value;
        const connectorId = document.getElementById('sch-connector').value;
        const select = document.getElementById('sch-map-detail-target');
        const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
        const preferredField = String(selectedRule?.targetField || '').trim();

        if (!select || !targetSystem) {
          state.targetFields = [];
          select.innerHTML = '<option value="">- Wählen -</option>';
          return;
        }

        // Clear select while loading
        select.innerHTML = '<option value="">Wird geladen...</option>';

        // Always use selected target object/table as base
        const targetObject = objectName;
        if (!targetObject) {
          state.targetFields = [];
          select.innerHTML = '<option value="">Zielobjekt wählen</option>';
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

          const fields = Array.isArray(result.fields) ? result.fields : [];
          state.targetFields = fields;
          const currentValue = preferredField || select.value;
          select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
            '<option value="' + esc(field.name) + '">' + esc(field.label ? field.label : field.name) + '</option>'
          ).join('');
          if (currentValue && !fields.some((f) => f.name === currentValue)) {
            select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
          }
          if (currentValue && fields.some((f) => f.name === currentValue)) {
            select.value = currentValue;
          } else if (currentValue) {
            select.value = currentValue;
          }
        } catch (error) {
          state.targetFields = [];
          select.innerHTML = '<option value="">Fehler beim Laden</option>';
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
          return;
        }

        rulesBody.innerHTML = state.mappingRules.map((rule) => {
          const isSelected = rule.id === state.selectedMappingRuleId;
          const source = esc(rule.sourceField || '-');
          const target = esc(rule.targetField || '-');
          const lookup = rule.lookupEnabled
            ? esc((rule.lookupObject || '-') + '.' + (rule.lookupField || '-'))
            : '-';
          const transform = esc(rule.transformFunction || 'NONE');
          const picklistCount = Array.isArray(rule.picklistMappings) ? rule.picklistMappings.length : 0;
          const picklist = picklistCount > 0 ? String(picklistCount) + ' Mapping(s)' : '-';

          return (
            '<tr class="' + (isSelected ? 'mapping-rule-selected' : '') + '" data-rule-id="' + esc(rule.id) + '">' +
              '<td>' + source + '</td>' +
              '<td>' + target + '</td>' +
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

      function autoMapByName() {
        clearModalError();

        const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
        if (!sourceFields.length) {
          showModalError('Bitte zuerst Quellfelder laden bevor Auto-Mapping ausgeführt wird.');
          return;
        }

        const targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
        if (!targetFields.length) {
          showModalError('Bitte zuerst ein Zielobjekt wählen, damit Zielfelder geladen werden können.');
          return;
        }

        const targetByKey = new Map();
        targetFields.forEach((field) => {
          const apiName = String(field?.name || '').trim();
          const label = String(field?.label || '').trim();
          if (apiName) {
            targetByKey.set(normalizeFieldKey(apiName), apiName);
          }
          if (label) {
            targetByKey.set(normalizeFieldKey(label), apiName || label);
          }
        });

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

          const matchedTarget = String(targetByKey.get(sourceKey) || '').trim();
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
            updated += 1;
            continue;
          }

          const newRule = createMappingRuleFromSource(sourceField);
          newRule.targetField = matchedTarget;
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
                return {
                  id: generateMappingRuleId(),
                  sourceField: String(item.sourceField || '').trim(),
                  sourceType: String(item.sourceType || 'string'),
                  targetField: String(item.targetField || '').trim(),
                  lookupEnabled: !!item.lookupEnabled || !!lookupDetails,
                  lookupObject: lookupDetails ? lookupDetails.lookupObject : String(item.lookupObject || ''),
                  lookupField: lookupDetails ? lookupDetails.lookupField : String(item.lookupField || ''),
                  transformFunction: lookupDetails ? 'NONE' : storedTransformFunction,
                  transformExpression: String(item.transformExpression || ''),
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
        const highlightWrap = document.getElementById('sch-source-sql-highlight-wrap');
        const highlight = document.getElementById('sch-source-sql-highlight');
        const status = document.getElementById('sch-source-test-status');
        const isSql = sourceType === 'MSSQL_SQL';
        const isFile = sourceType === 'FILE_CSV' || sourceType === 'FILE_EXCEL' || sourceType === 'FILE_JSON';
        const isRest = sourceType === 'REST_API';

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
      }

      async function safeRequest(path, fallback) {
        try {
          return await requestJson(path);
        } catch (error) {
          showError(error.message || 'API-Fehler');
          return fallback;
        }
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
        const totals = (summary?.buckets || []).map((item) => item.total || 0);
        const errors = (summary?.buckets || []).map((item) => item.errors || 0);

        logsChart = new window.Chart(canvas, {
          type: 'line',
          data: {
            labels,
            datasets: [
              {
                label: 'Logs',
                data: totals,
                backgroundColor: 'rgba(62, 137, 189, 0.16)',
                borderColor: 'rgba(62, 137, 189, 1)',
                borderWidth: 2,
                tension: 0.35,
                fill: false,
                pointRadius: 2,
                pointHoverRadius: 4
              },
              {
                label: 'Fehler',
                data: errors,
                backgroundColor: 'rgba(208, 73, 73, 0.16)',
                borderColor: 'rgba(208, 73, 73, 1)',
                borderWidth: 2,
                tension: 0.35,
                fill: false,
                pointRadius: 2,
                pointHoverRadius: 4
              }
            ]
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

              const logType = point.datasetIndex === 1 ? 'error' : 'all';
              await openLogsByBucket(bucket, logType);
            }
          }
        });
      }

      function renderRecordsTrendChart(runs) {
        const canvas = document.getElementById('records-chart');
        if (!canvas || typeof window.Chart !== 'function') {
          return;
        }

        if (recordsChart) {
          recordsChart.destroy();
        }

        const sortedRuns = (Array.isArray(runs) ? runs.slice() : [])
          .filter((item) => item && item.startedAt)
          .sort((a, b) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime())
          .slice(-20);

        const labels = sortedRuns.map((item) => formatDate(item.startedAt, 'short'));
        const transferred = sortedRuns.map((item) => {
          const ok = Number(item.recordsSucceeded ?? 0);
          const failed = Number(item.recordsFailed ?? 0);
          return Math.max(0, ok + failed);
        });
        const failed = sortedRuns.map((item) => Math.max(0, Number(item.recordsFailed ?? 0)));

        recordsChart = new window.Chart(canvas, {
          type: 'line',
          data: {
            labels,
            datasets: [
              {
                label: 'Datensätze gesamt',
                data: transferred,
                borderColor: 'rgba(43, 122, 184, 1)',
                backgroundColor: 'rgba(43, 122, 184, 0.14)',
                borderWidth: 2,
                tension: 0.35,
                fill: false,
                pointRadius: 2,
                pointHoverRadius: 4
              },
              {
                label: 'Datensätze fehlgeschlagen',
                data: failed,
                borderColor: 'rgba(184, 68, 80, 1)',
                backgroundColor: 'rgba(184, 68, 80, 0.12)',
                borderWidth: 2,
                tension: 0.35,
                fill: false,
                pointRadius: 2,
                pointHoverRadius: 4
              }
            ]
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
            }
          }
        });
      }

      async function loadLogSummary() {
        const range = document.getElementById('log-chart-range').value || 'last_24h';
        try {
          window.localStorage.setItem(LOG_CHART_RANGE_STORAGE_KEY, range);
        } catch {
          // Ignore storage errors in restricted browser contexts.
        }
        const summary = await safeRequest('/api/logs/summary?range=' + encodeURIComponent(range), { range, buckets: [] });
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

        return {
          connectorType: normalizedConnectorType,
          parameters: parsedParameters
        };
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

      function advanceConnectorWizardStep() {
        try {
          validateConnectorWizardStep(state.connectorWizardStep);
          goToConnectorWizardStep(state.connectorWizardStep + 1);
        } catch (error) {
          showConnectorModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
        }
      }

      async function openLogsByBucket(bucket, type) {
        const path = '/api/logs?start=' + encodeURIComponent(bucket.start) +
          '&end=' + encodeURIComponent(bucket.end) +
          '&type=' + encodeURIComponent(type) +
          '&limit=300';

        const result = await safeRequest(path, { items: [] });
        const rows = result.items || [];
        document.getElementById('logs-modal-title').textContent =
          'Logliste ' + (type === 'error' ? '(Fehler)' : '(Alle)') +
          ' | ' + new Date(bucket.start).toLocaleString('de-DE') +
          ' - ' + new Date(bucket.end).toLocaleString('de-DE');

        const body = document.getElementById('logs-modal-body');
        if (!rows.length) {
          body.innerHTML = '<tr><td colspan="5" class="text-secondary p-3">Keine Logs in diesem Zeitraum.</td></tr>';
          logsModal.show();
          return;
        }

        body.innerHTML = rows.map((entry) =>
          '<tr>' +
            '<td>' + esc(entry.createdAt ? new Date(entry.createdAt).toLocaleString('de-DE') : '-') + '</td>' +
            '<td>' + esc(entry.level || '-') + '</td>' +
            '<td>' + esc(entry.scheduleName || '-') + '</td>' +
            '<td>' + esc(entry.step || '-') + '</td>' +
            '<td>' + esc(entry.message || '-') + '</td>' +
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
        const formatDurationMinSec = (milliseconds) => {
          if (!Number.isFinite(milliseconds) || milliseconds < 0) {
            return '-';
          }

          const totalSeconds = Math.max(0, Math.round(milliseconds / 1000));
          const minutes = Math.floor(totalSeconds / 60);
          const seconds = totalSeconds % 60;
          return String(minutes) + ':' + String(seconds).padStart(2, '0');
        };

        const getRunDurationMs = (run) => {
          if (!run?.startedAt || !run?.finishedAt) {
            return null;
          }

          const startedAt = new Date(run.startedAt).getTime();
          const finishedAt = new Date(run.finishedAt).getTime();
          if (Number.isNaN(startedAt) || Number.isNaN(finishedAt) || finishedAt < startedAt) {
            return null;
          }

          return finishedAt - startedAt;
        };

        renderOverviewStatsRangeButtons();
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

        renderRecordsTrendChart(scopedRuns);

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

        const connectorIds = Array.from(new Set(
          (state.schedules || [])
            .map((schedule) => String(schedule.connectorId || '').trim())
            .filter(Boolean)
        ));

        const options = connectorIds
          .map((connectorId) => ({
            connectorId,
            name: getConnectorNameById(connectorId),
            count: (state.schedules || []).filter((schedule) => String(schedule.connectorId || '').trim() === connectorId).length
          }))
          .sort((a, b) => a.name.localeCompare(b.name, 'de', { sensitivity: 'base' }));

        select.innerHTML = '<option value="">Alle Connectoren</option>' + options.map((option) =>
          '<option value="' + esc(option.connectorId) + '">' + esc(option.name) + ' (' + option.count + ')</option>'
        ).join('');

        if (state.schedulerConnectorFilterId && options.some((option) => option.connectorId === state.schedulerConnectorFilterId)) {
          select.value = state.schedulerConnectorFilterId;
        } else {
          state.schedulerConnectorFilterId = '';
          select.value = '';
        }
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
          if (!state.schedulerConnectorFilterId) {
            return true;
          }
          return String(item.connectorId || '').trim() === state.schedulerConnectorFilterId;
        });

        if (!filteredSchedules.length) {
          body.innerHTML = '<tr><td colspan="9" class="text-secondary">Keine Scheduler gefunden.</td></tr>';
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
            const activeBadge = item.active
              ? '<span class="badge bg-success-subtle text-success border">aktiv</span>'
              : item.autoDisabledDueToErrors
                ? '<span class="badge bg-warning-subtle text-warning border" title="Automatisch wegen Fehlern deaktiviert">inaktiv (auto)</span>'
                : '<span class="badge bg-secondary-subtle text-secondary border">inaktiv</span>';
            const lastFailedRun = (state.runs || [])
              .filter((run) => run.scheduleId === item.id && run.status === 'Failed')
              .sort((a, b) => {
                const timeA = new Date(a.finishedAt || 0).getTime();
                const timeB = new Date(b.finishedAt || 0).getTime();
                return timeB - timeA;
              })
              [0];
            const errorCell = lastFailedRun
              ? '<td><button class="btn btn-sm btn-outline-danger" title="Letzter Fehler: ' + esc(lastFailedRun.errorMessage || 'Unbekannter Fehler') + '" data-show-run-logs="' + esc(lastFailedRun.id) + '">🔴</button></td>'
              : '<td>-</td>';
            
            return '<tr>' +
              '<td><div style="padding-left:' + indent + 'px"><strong class="text-truncate d-block" title="' + esc(item.name) + '">' + esc(item.name) + hierarchyBadge + '</strong><div class="small text-secondary text-truncate" title="' + esc(item.objectName) + ' / ' + esc(item.operation) + '">' + objectIcon + ' ' + esc(item.objectName) + ' / ' + esc(item.operation) + '</div></div></td>' +
              '<td class="text-truncate" title="' + esc(parentName) + '">' + esc(parentName) + (item.inheritTimingFromParent ? ' <span class="badge bg-primary-subtle text-primary border">inherits</span>' : '') + '</td>' +
              '<td>' + activeBadge + '</td>' +
              '<td>' + getStatusBadge(item.status) + '</td>' +
              '<td class="text-truncate" title="' + esc(connectorName) + '">' + esc(connectorName) + '</td>' +
              '<td>' + esc(intervalLabel) + '</td>' +
              '<td>' + formatDate(item.nextRunAt, 'short') + '</td>' +
              errorCell +
              '<td>' +
                '<button class="btn btn-sm btn-outline-primary me-1" data-edit-schedule="' + esc(item.id) + '">Edit</button>' +
                '<button class="btn btn-sm btn-outline-secondary me-1" data-dup-schedule="' + esc(item.id) + '">Dupl</button>' +
                '<button class="btn btn-sm btn-outline-success me-1" data-run-now="' + esc(item.id) + '">Run</button>' +
                '<button class="btn btn-sm btn-outline-info me-1" data-dry-run="' + esc(item.id) + '">DryRun</button>' +
                '<button class="btn btn-sm btn-outline-danger" data-delete-schedule="' + esc(item.id) + '">Del</button>' +
              '</td>' +
            '</tr>';
        }).join('');

        body.querySelectorAll('button[data-edit-schedule]').forEach((button) => {
          button.addEventListener('click', () => openScheduleModal(button.getAttribute('data-edit-schedule')));
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

        const schedulersFilter = document.getElementById('schedulers-filter');
        if (schedulersFilter && String(schedulersFilter.value || '').trim()) {
          schedulersFilter.dispatchEvent(new Event('input'));
        }

        setTimeout(() => initializeTableFilters(), 100);
      }

      function renderConnectors() {
        const body = document.getElementById('connectors-body');
        const sqlSelect = document.getElementById('sql-connector-select');

        const mssqlItems = state.connectors.filter((item) => String(item.connectorType).toLowerCase() === 'mssql');
        sqlSelect.innerHTML = mssqlItems.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.name) + '</option>').join('');
        if (!sqlSelect.innerHTML) {
          sqlSelect.innerHTML = '<option value="">Keine MSSQL-Connectoren</option>';
        }

        if (!state.connectors.length) {
          body.innerHTML = '<tr><td colspan="5" class="text-secondary">Keine Connectoren gefunden.</td></tr>';
          return;
        }

        body.innerHTML = state.connectors.map((item) =>
          '<tr>' +
          '<td><strong>' + esc(item.name) + '</strong></td>' +
          '<td>' + esc(item.connectorType) + '</td>' +
          '<td>' + (item.active ? 'aktiv' : 'inaktiv') + '</td>' +
          '<td>' + esc((item.parameterKeys || []).join(', ') || '-') + '</td>' +
          '<td>' +
            '<button class="btn btn-sm btn-outline-primary me-1" data-edit-connector="' + esc(item.id) + '">Edit</button>' +
            '<button class="btn btn-sm btn-outline-secondary" data-test-connector="' + esc(item.id) + '">Test</button>' +
          '</td>' +
          '</tr>'
        ).join('');

        body.querySelectorAll('button[data-edit-connector]').forEach((button) => {
          button.addEventListener('click', () => openConnectorModal(button.getAttribute('data-edit-connector')));
        });

        body.querySelectorAll('button[data-test-connector]').forEach((button) => {
          button.addEventListener('click', async () => {
            const result = await requestJson('/api/connectors/' + encodeURIComponent(button.getAttribute('data-test-connector')) + '/test', { method: 'POST' });
            alert(result.message || (result.ok ? 'OK' : 'Fehler'));
          });
        });

        // Re-initialize table filters
        setTimeout(() => initializeTableFilters(), 100);
      }

      function renderRuns() {
        const body = document.getElementById('runs-body');
        const select = document.getElementById('log-run-select');
        if (!state.runs.length) {
          body.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine Runs gefunden.</td></tr>';
          select.innerHTML = '<option value="">Keine Runs</option>';
          return;
        }

        body.innerHTML = state.runs.map((item) =>
          '<tr>' +
          '<td class="text-truncate" title="' + esc(item.scheduleName || item.scheduleId || '-') + '">' + esc(item.scheduleName || item.scheduleId || '-') + '</td>' +
          '<td>' + getStatusBadge(item.status) + '</td>' +
          '<td>' + esc((item.recordsSucceeded ?? 0) + ' ok / ' + (item.recordsFailed ?? 0) + ' fail') + '</td>' +
          '<td><button class="btn btn-sm btn-outline-primary" data-log-run="' + esc(item.id) + '">Logs</button></td>' +
          '</tr>'
        ).join('');

        select.innerHTML = state.runs.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.scheduleName || item.id) + '</option>').join('');

        body.querySelectorAll('button[data-log-run]').forEach((button) => {
          button.addEventListener('click', async () => {
            select.value = button.getAttribute('data-log-run');
            await loadLogs();
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

        if (!selectedConnectorId) {
          return { nodes, edges };
        }

        const selectedConnectorNode = nodes.find(
          (node) => node.kind === 'connector' && String(node.refId || '').trim() === selectedConnectorId
        );

        if (!selectedConnectorNode) {
          return { nodes, edges };
        }

        const outgoingEdgesByNode = new Map();
        edges.forEach((edge) => {
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
          nodes: nodes.filter((node) => keepNodeIds.has(String(node.id || ''))),
          edges: edges.filter(
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

        const nodeIds = new Set(nodes.map((node) => String(node.id || '')));
        const adjacency = new Map();
        const indegree = new Map();

        nodes.forEach((node) => {
          const id = String(node.id || '');
          adjacency.set(id, []);
          indegree.set(id, 0);
        });

        edges.forEach((edge) => {
          const from = String(edge.from || '');
          const to = String(edge.to || '');
          if (!nodeIds.has(from) || !nodeIds.has(to)) {
            return;
          }
          adjacency.get(from).push(to);
          indegree.set(to, Number(indegree.get(to) || 0) + 1);
        });

        const levelByNode = new Map();
        const queue = [];
        const selectedConnectorId = String(state.overviewConnectorFilterId || '').trim();
        const selectedConnectorNode = selectedConnectorId
          ? nodes.find((node) => node.kind === 'connector' && String(node.refId || '').trim() === selectedConnectorId)
          : null;

        if (selectedConnectorNode) {
          const selectedId = String(selectedConnectorNode.id || '');
          levelByNode.set(selectedId, 0);
          queue.push(selectedId);
        } else {
          nodes.forEach((node) => {
            const id = String(node.id || '');
            if ((indegree.get(id) || 0) === 0) {
              levelByNode.set(id, 0);
              queue.push(id);
            }
          });
        }

        while (queue.length) {
          const current = String(queue.shift() || '');
          const currentLevel = Number(levelByNode.get(current) || 0);
          (adjacency.get(current) || []).forEach((nextId) => {
            const candidateLevel = currentLevel + 1;
            if (!levelByNode.has(nextId) || candidateLevel > Number(levelByNode.get(nextId) || 0)) {
              levelByNode.set(nextId, candidateLevel);
            }
            queue.push(nextId);
          });
        }

        nodes.forEach((node) => {
          const id = String(node.id || '');
          if (!levelByNode.has(id)) {
            levelByNode.set(id, 0);
          }
        });

        const levels = new Map();
        nodes.forEach((node) => {
          const id = String(node.id || '');
          const level = Number(levelByNode.get(id) || 0);
          if (!levels.has(level)) {
            levels.set(level, []);
          }
          levels.get(level).push(node);
        });

        const sortedLevels = Array.from(levels.keys()).sort((a, b) => a - b);
        const baseX = 30;
        const columnGap = 320;
        const baseY = 26;
        const rowGap = 24;
        const nodeHeight = 82;

        sortedLevels.forEach((level) => {
          const levelNodes = levels.get(level) || [];
          levelNodes.sort((a, b) => {
            const kindA = String(a.kind || '');
            const kindB = String(b.kind || '');
            if (kindA !== kindB) {
              return kindA.localeCompare(kindB);
            }
            return String(a.label || '').localeCompare(String(b.label || ''), 'de', { sensitivity: 'base' });
          });

          levelNodes.forEach((node, index) => {
            node.x = baseX + level * columnGap;
            node.y = baseY + index * (nodeHeight + rowGap);
          });
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

          return '<path class="graph-edge" style="stroke:' + edgeColor + ';stroke-width:2.5;fill:none;opacity:0.9;stroke-linecap:round;stroke-linejoin:round" marker-end="url(#' + markerId + ')" d="' + pathData + '" />';
        }).join('');

        const nodeMarkup = nodes.map((node) => {
          const isInbound = String(node.direction || '').toLowerCase() === 'inbound';
          const schedulerUsesFile = String(node.sourceType || '').toUpperCase().startsWith('FILE_') || String(node.targetType || '').toUpperCase().startsWith('FILE_');
          const directionClass = node.kind === 'scheduler'
            ? ((isInbound ? 'graph-inbound' : 'graph-outbound') + ' ' + (schedulerUsesFile ? 'graph-scheduler-file' : 'graph-scheduler-db'))
            : 'graph-connector ' + getConnectorGraphClass(node.connectorType, node.label);
          const titleLines = splitGraphText(node.label, 24, 2);
          const subtitleLines = splitGraphText(node.subtitle, 28, 2);
          const icon = node.kind === 'scheduler'
            ? getObjectIcon(node.objectName)
            : getConnectorIcon(node.connectorType, node.label);
          const metaLabel = node.kind === 'scheduler'
            ? (isInbound ? 'Inbound' : 'Outbound')
            : String(node.connectorType || 'Connector').toUpperCase();
          const metaLines = splitGraphText(metaLabel, 22, 1);
          const titleMarkup = renderGraphText('graph-title', 58, 28, titleLines, 14);
          const subtitleMarkup = renderGraphText('graph-subtitle', 58, 48, subtitleLines, 13);
          const metaMarkup = renderGraphText('graph-meta', 58, 68, metaLines, 12);

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
              titleMarkup.replace('<text ', '<text style="fill:#2f4050;font-weight:700;font-size:12px" ') +
              subtitleMarkup.replace('<text ', '<text style="fill:#66717d;font-size:11px" ') +
              metaMarkup.replace('<text ', '<text style="' + metaStyle + ';font-size:10px;font-weight:700;letter-spacing:0.6px" ') +
            '</g>'
          );
        }).join('');

        svg.innerHTML = defs + edgeMarkup + nodeMarkup;
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

      async function openScheduleModal(scheduleId) {
        const entry = state.schedules.find((item) => item.id === scheduleId);
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
        document.getElementById('sch-source-definition').value = entry?.sourceDefinition || '';
        document.getElementById('sch-target-definition').value = entry?.targetDefinition || '';
        document.getElementById('sch-mapping').value = entry?.mappingDefinition || '';
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
        setupMappingDropZone();
        hydrateMappingRulesFromDefinition();
        loadTransformFunctions();
        await loadTargetObjects(entry?.objectName || '');
        toggleCreateObjectFromSourceUi();
        loadTargetFields();
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
        document.getElementById('con-mssql-trust-server-certificate').checked = params.trustServerCertificate === undefined ? true : !!params.trustServerCertificate;
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

      function openConnectorModal(connectorId) {
        const entry = state.connectors.find((item) => item.id === connectorId);
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
        fillMssqlConnectorSettingsFromParameters(parameters);
        fillFileConnectorSettingsFromParameters(parameters);
        fillRestConnectorSettingsFromParameters(parameters);
        fillBinaryImportConnectorSettingsFromParameters(parameters);
        applyConnectorWizardSelection(!!entry);
        updateConnectorConfigUi();
        document.getElementById('con-active').checked = entry ? !!entry.active : true;
        state.connectorWizardStep = 1;
        renderConnectorWizardStep();
        connectorModal.show();
      }

      async function saveSchedule() {
        clearError();
        const saveButton = document.getElementById('save-schedule');
        saveButton.disabled = true;

        try {
          ensureSalesforceTargetDefinition();

          const selectedWeekdays = Array.from(document.querySelectorAll('#sch-weekdays input:checked'))
            .map((input) => Number(input.value))
            .filter((value) => !Number.isNaN(value));
          
          const timingDefinition = {
            days: selectedWeekdays,
            intervalMinutes: Number(document.getElementById('sch-timing-interval').value || 2),
            startTime: document.getElementById('sch-timing-time').value || '09:00'
          };

          const scheduleId = document.getElementById('sch-id').value || undefined;
          
          const payload = {
            id: scheduleId,
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
            sourceDefinition: document.getElementById('sch-source-definition').value || undefined,
            targetDefinition: document.getElementById('sch-target-definition').value || undefined,
            mappingDefinition: document.getElementById('sch-mapping').value || undefined,
            timingDefinition: JSON.stringify(timingDefinition)
          };
          
          // Only include name for new schedules (Name is an auto-number field and cannot be updated)
          if (!scheduleId) {
            payload.name = document.getElementById('sch-name').value;
          }

          await requestJson('/api/schedules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });

          scheduleModal.hide();
          await refresh();
        } catch (error) {
          showError(error.message || 'Scheduler konnte nicht gespeichert werden');
        } finally {
          saveButton.disabled = false;
        }
      }

      async function testScheduleSource() {
        clearError();
        clearModalError();
        const testButton = document.getElementById('sch-test-source');
        const sourceType = document.getElementById('sch-source-type').value;
        const sourceDefinition = document.getElementById('sch-source-definition').value;
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

      async function persistConnector(options = {}) {
        const validateAfterSave = options.validateAfterSave === true;
        const preview = collectConnectorParametersPreview();
        const payload = {
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
        const lines = (logs.items || []).map((entry) => '[' + (entry.level || '-') + '] ' + (entry.step || '-') + ' | ' + (entry.message || ''));
        document.getElementById('logs-output').textContent = lines.join('\\n') || 'Keine Logs gefunden.';
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

        if (!items.length) {
          select.innerHTML = '<option value="">Keine Instanzen konfiguriert</option>';
          state.instanceId = '';
          return;
        }

        select.innerHTML = items.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.name) + '</option>').join('');
        const hasCurrent = items.some((item) => item.id === state.instanceId);
        if (!state.instanceId || !hasCurrent) {
          state.instanceId = items.find((item) => item.isDefault)?.id || items[0].id;
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

        const healthData = await safeRequest('/api/system/health', {});
        const schedules = await safeRequest('/api/schedules', { items: [] });
        const connectors = await safeRequest('/api/connectors', { items: [] });
        const runs = await safeRequest('/api/runs', { items: [] });
        const graph = await safeRequest('/api/graph', { nodes: [], edges: [] });
        const salesforceOverview = await safeRequest('/api/salesforce/overview', {});
        await loadScheduleOptions();

        state.schedules = schedules.items || [];
        state.connectors = connectors.items || [];
        state.runs = runs.items || [];
        state.graphData = graph;
        renderSalesforceOverview(salesforceOverview);

        renderOverview(healthData);
        renderSchedules();
        renderConnectors();
        renderRuns();
        renderOverviewConnectorFilter();
        redrawOverviewGraph();
        if (shouldRefreshChart) {
          await loadLogSummary();
        }
      }

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
      document.getElementById('sch-load-source-fields').addEventListener('click', loadMappingFields);
        document.getElementById('sch-automapping').addEventListener('click', autoMapByName);
      document.getElementById('new-schedule').addEventListener('click', () => openScheduleModal(''));
      document.getElementById('new-connector').addEventListener('click', () => openConnectorModal(''));
      document.getElementById('save-schedule').addEventListener('click', saveSchedule);
      document.getElementById('sch-test-source').addEventListener('click', testScheduleSource);
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
      document.getElementById('sch-source-definition').addEventListener('input', updateSourceQueryAssist);
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
      document.getElementById('con-type').addEventListener('input', updateConnectorConfigUi);
      document.getElementById('con-wizard-type').addEventListener('change', () => applyConnectorWizardSelection(false));
      document.getElementById('con-rest-auth-type').addEventListener('change', updateRestAuthUi);
      document.getElementById('load-logs').addEventListener('click', loadLogs);
      document.getElementById('preview-sql').addEventListener('click', previewSql);
      document.getElementById('preview-mapping').addEventListener('click', previewMapping);
      document.getElementById('sch-map-detail-apply').addEventListener('click', applySelectedMappingDetailChanges);
      document.getElementById('sch-map-detail-delete').addEventListener('click', deleteSelectedMappingRule);
      document.getElementById('sch-map-detail-picklist-add').addEventListener('click', addPicklistMappingEntry);
      document.getElementById('sch-target-system').addEventListener('change', async () => {
        applyOperationOptions('');
        await loadTargetObjects('');
        await loadTargetFields();
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
      });
      document.getElementById('sch-target-type').addEventListener('change', () => {
        applyOperationOptions('');
        toggleCreateObjectFromSourceUi();
        ensureSalesforceTargetDefinition();
      });
      document.getElementById('sch-object').addEventListener('change', async () => {
        await loadTargetFields();
        ensureSalesforceTargetDefinition();
      });
      document.getElementById('sch-create-custom-object').addEventListener('click', createSalesforceCustomObjectFromSource);
      document.getElementById('sch-connector').addEventListener('change', async () => {
        await loadTargetObjects(document.getElementById('sch-object').value || '');
        await loadTargetFields();
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
        const sourceType = document.getElementById('sch-source-type').value;
        const sourceDefinition = document.getElementById('sch-source-definition').value;
        const objectName = document.getElementById('sch-object').value;
        const connectorId = document.getElementById('sch-connector').value || undefined;
        const sourceFieldsBody = document.getElementById('sch-mapping-source-fields');
        if (!sourceFieldsBody) {
          return;
        }

        if (!sourceType || !sourceDefinition.trim()) {
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Keine Quellmetadaten verfügbar.</td></tr>';
          return;
        }

        try {
          const result = await requestJson('/api/sources/fields', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sourceType, sourceDefinition, objectName, connectorId })
          });

          const fields = Array.isArray(result.fields) ? result.fields : [];
          state.mappingFields = fields;
          reconcileMappingRuleSourceFields();
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

          if (state.mappingRules.length) {
            renderMappingRulesTable();
          }

          renderCreateObjectFieldOverrides();
        } catch (error) {
          state.mappingFields = [];
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Feldmetadaten konnten nicht geladen werden.</td></tr>';
          showModalError(error.message || 'Feldmetadaten konnten nicht geladen werden');
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

      // ===== NATIVE TABLE FILTERING & SEARCH =====
      const TABLE_STORAGE_KEY = 'sf-agent.table-filters';

      function initializeTableFilters() {
        // Scheduler table filter
        const schedulersFilter = document.getElementById('schedulers-filter');
        if (schedulersFilter && schedulersFilter.dataset.bound !== '1') {
          schedulersFilter.dataset.bound = '1';
          schedulersFilter.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            const rows = document.querySelectorAll('#schedules-body tr');
            rows.forEach(row => {
              const text = row.textContent.toLowerCase();
              const isMatch = text.includes(query);
              row.style.display = isMatch ? '' : 'none';
            });

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
              schedulersFilter.dispatchEvent(new Event('input'));
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
            directionTabs.forEach((button) => button.classList.remove('active'));
            tabButton.classList.add('active');
            renderSchedules();
          });
        });

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
        }

        // Connectors table filter
        const connectorsFilter = document.getElementById('connectors-filter');
        if (connectorsFilter && connectorsFilter.dataset.bound !== '1') {
          connectorsFilter.dataset.bound = '1';
          connectorsFilter.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            const rows = document.querySelectorAll('#connectors-body tr');
            rows.forEach(row => {
              const text = row.textContent.toLowerCase();
              const isMatch = text.includes(query);
              row.style.display = isMatch ? '' : 'none';
            });
            try {
              localStorage.setItem(TABLE_STORAGE_KEY + '.connectors', query);
            } catch (e) {
              // Ignore storage errors
            }
          });
          try {
            const stored = localStorage.getItem(TABLE_STORAGE_KEY + '.connectors');
            if (stored) {
              connectorsFilter.value = stored;
              connectorsFilter.dispatchEvent(new Event('input'));
            }
          } catch (e) {
            // Ignore storage errors
          }
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
        if (!migState.objects.length) {
          container.innerHTML = '<span class="text-secondary small">Noch keine Objekte ausgewählt.</span>';
          return;
        }
        container.innerHTML = migState.objects.map((obj) =>
          '<span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:0.85em">' +
          esc(obj.salesforceObject) +
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

      function renderMigFileAssignments() {
        const container = document.getElementById('mig-file-assignment-list');
        if (!container) return;
        if (!migState.objects.length) {
          container.innerHTML = '<div class="text-secondary small">Bitte zuerst Objekte in Schritt 1 auswählen.</div>';
          return;
        }
        container.innerHTML = migState.objects.map((obj) => {
          const safeId = esc(obj.id);
          return '<div class="card soft-card mb-2"><div class="card-body"><div class="d-flex justify-content-between align-items-center mb-2">' +
            '<strong>' + esc(obj.salesforceObject) + '</strong>' +
            '<select class="form-select form-select-sm w-auto" style="min-width:120px" data-op-select="' + safeId + '">' +
            '<option value="insert"' + (obj.operation === 'insert' ? ' selected' : '') + '>Insert</option>' +
            '<option value="upsert"' + (obj.operation === 'upsert' ? ' selected' : '') + '>Upsert</option>' +
            '<option value="update"' + (obj.operation === 'update' ? ' selected' : '') + '>Update</option>' +
            '</select></div>' +
            '<div class="input-group mb-1">' +
            '<input type="text" class="form-control form-control-sm" placeholder="Noch keine Datei ausgewählt" value="' + esc(obj.filePath || '') + '" data-file-path="' + safeId + '" readonly />' +
            '<input type="file" class="d-none" data-file-dialog="' + safeId + '" accept=".csv,.txt,.json,.xlsx,.xls" />' +
            '<button class="btn btn-sm btn-outline-primary" data-pick-file="' + safeId + '">Datei wählen</button>' +
            '<button class="btn btn-sm btn-outline-secondary" data-analyze-file="' + safeId + '">Analysieren</button>' +
            '</div>' +
            '<div id="mig-file-cols-' + safeId + '" class="small text-secondary">' +
            (obj.fileColumns && obj.fileColumns.length ? 'Spalten: ' + obj.fileColumns.map(esc).join(', ') : '') +
            '</div></div></div>';
        }).join('');

        const fileToBase64 = async (file) => {
          const buffer = await file.arrayBuffer();
          const bytes = new Uint8Array(buffer);
          const chunkSize = 0x8000;
          let binary = '';
          for (let i = 0; i < bytes.length; i += chunkSize) {
            binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
          }
          return btoa(binary);
        };

        migState.objects.forEach((obj) => {
          const fileInput = container.querySelector('[data-file-path="' + obj.id + '"]');
          const fileDialog = container.querySelector('[data-file-dialog="' + obj.id + '"]');
          const pickBtn = container.querySelector('[data-pick-file="' + obj.id + '"]');
          const opSelect = container.querySelector('[data-op-select="' + obj.id + '"]');
          if (opSelect) {
            opSelect.addEventListener('change', () => {
              obj.operation = opSelect.value;
            });
          }
          const analyzeBtn = container.querySelector('[data-analyze-file="' + obj.id + '"]');

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
                await migSave();
                const contentBase64 = await fileToBase64(file);
                const res = await fetch('/api/migrations/upload-file', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    migrationId: migState.id,
                    objectId: obj.id,
                    fileName: file.name,
                    contentBase64
                  })
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.error || 'Datei konnte nicht hochgeladen werden');

                obj.filePath = data.filePath || '';
                obj.fileColumns = data.fields || [];
                obj.previewRows = Array.isArray(data.rows) ? data.rows.slice(0, 3) : [];

                if (fileInput) fileInput.value = obj.filePath || '';
                const colDiv = document.getElementById('mig-file-cols-' + obj.id);
                if (colDiv) {
                  colDiv.textContent = obj.fileColumns && obj.fileColumns.length
                    ? 'Spalten: ' + obj.fileColumns.join(', ')
                    : '';
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
              if (!obj.filePath) { alert('Bitte zuerst eine Datei auswählen.'); return; }
              analyzeBtn.disabled = true;
              analyzeBtn.textContent = '…';
              try {
                await migSave();
                const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/analyze-file/' + encodeURIComponent(obj.id));
                if (!res.ok) throw new Error(await res.text());
                const data = await res.json();
                obj.fileColumns = data.fields || [];
                if (data.rows && data.rows.length) obj.previewRows = data.rows.slice(0, 3);
                const colDiv = document.getElementById('mig-file-cols-' + obj.id);
                if (colDiv) colDiv.textContent = 'Spalten: ' + (obj.fileColumns || []).join(', ');
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
      }

      function renderMigMappingObjectSelect() {
        const sel = document.getElementById('mig-mapping-object-select');
        if (!sel) return;
        sel.innerHTML = migState.objects.map((obj) =>
          '<option value="' + esc(obj.id) + '">' + esc(obj.salesforceObject) + '</option>'
        ).join('');
        renderMigMappingPanel();
      }

      async function renderMigMappingPanel() {
        const sel = document.getElementById('mig-mapping-object-select');
        const panel = document.getElementById('mig-mapping-panel');
        if (!sel || !panel) return;
        const objectId = sel.value;
        const obj = migState.objects.find((o) => o.id === objectId);
        if (!obj) { panel.innerHTML = '<div class="text-secondary small">Kein Objekt ausgewählt.</div>'; return; }
        if (!obj.fileColumns || !obj.fileColumns.length) {
          panel.innerHTML = '<div class="text-secondary small">Bitte zuerst die Datei in Schritt 2 analysieren.</div>'; return;
        }

        panel.innerHTML = '<div class="spinner-border spinner-border-sm me-2"></div>Salesforce-Felder laden…';
        let sfFields = [];
        let sfObjects = [];
        try {
          const [fieldsRes, objectsRes] = await Promise.all([
            fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject) + '&instanceId=' + encodeURIComponent(state.instanceId || '')),
            fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(state.instanceId || ''))
          ]);
          if (fieldsRes.ok) sfFields = await fieldsRes.json();
          if (objectsRes.ok) sfObjects = await objectsRes.json();
        } catch { /* ignore */ }

        const existingFieldNames = new Set((sfFields || []).map((f) => String(f.name || '').toLowerCase()));
        obj._existingFieldNames = Array.from(existingFieldNames);

        // Mark mapping entries that point to fields not yet existing in Salesforce.
        (obj.fieldMappings || []).forEach((mapping) => {
          mapping._isMissing = !!mapping.targetField && !existingFieldNames.has(String(mapping.targetField).toLowerCase());
        });

        const objectFieldListId = 'mig-sf-fields-' + objectId;
        const parsePicklistText = (value) => String(value || '').split(';')
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => {
            const idx = part.indexOf('=');
            if (idx < 0) return null;
            return { source: part.slice(0, idx).trim(), target: part.slice(idx + 1).trim() };
          })
          .filter((entry) => entry && (entry.source || entry.target));
        const toPicklistText = (entries) => (Array.isArray(entries) ? entries : [])
          .map((entry) => String(entry?.source || '').trim() + '=' + String(entry?.target || '').trim())
          .filter((part) => part !== '=')
          .join('; ');

        // Build lookup object <select> options once (reused per row)
        const sfObjectOptHtml = '<option value="">- SF Objekt wählen -</option>' +
          (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '">' + esc(o.label || o.name) + '</option>').join('');

        panel.innerHTML = '<div class="table-responsive"><table class="table table-sm">' +
          '<thead><tr><th>Datei-Spalte</th><th>→ Salesforce-Feld</th><th>Typ</th><th>Umwandlung</th><th>Lookup</th><th>Picklist-Mapping</th></tr></thead><tbody>' +
          obj.fileColumns.map((col) => {
            const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
            const currentTarget = existing ? existing.targetField : '';
            const targetType = (sfFields || []).find((f) => f.name === currentTarget)?.type || (existing?._isMissing ? 'neu' : '');
            const transformFunction = String(existing?.transformFunction || 'NONE');
            const transformExpression = String(existing?.transformExpression || '');
            const isStatic = transformFunction === 'STATIC';
            const lookupEnabled = existing?.lookupEnabled === true;
            const lookupObject = String(existing?.lookupObject || '');
            const lookupField = String(existing?.lookupField || '');
            const picklistText = toPicklistText(existing?.picklistMappings);

            // Lookup object options with pre-selected value
            const lookupObjOptions = '<option value="">- SF Objekt wählen -</option>' +
              (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '"' + (lookupObject === o.name ? ' selected' : '') + '>' + esc(o.label || o.name) + '</option>').join('');

            return '<tr>' +
              '<td><code>' + esc(col) + '</code></td>' +
              '<td><input class="form-control form-control-sm" list="' + esc(objectFieldListId) + '" placeholder="z. B. Name oder My_New_Field__c" value="' + esc(currentTarget || '') + '" data-map-col="' + esc(col) + '" data-map-obj="' + esc(objectId) + '" /></td>' +
              '<td><span class="badge bg-secondary" data-map-type="' + esc(col) + '">' + esc(targetType) + '</span></td>' +
              '<td>' +
                '<select class="form-select form-select-sm" data-map-transform="' + esc(col) + '">' +
                  ['NONE','TRIM','UPPERCASE','LOWERCASE','TO_INTEGER','TO_BOOLEAN','DATETIME_ISO','STATIC'].map((fn) =>
                    '<option value="' + fn + '"' + (transformFunction === fn ? ' selected' : '') + '>' + fn + '</option>'
                  ).join('') +
                '</select>' +
                '<input class="form-control form-control-sm mt-1" placeholder="Statischer Wert" value="' + esc(transformExpression) + '" data-map-transform-expression="' + esc(col) + '"' + (isStatic ? '' : ' style="display:none"') + ' />' +
              '</td>' +
              '<td>' +
                '<div class="form-check mb-1"><input class="form-check-input" type="checkbox" data-map-lookup-enabled="' + esc(col) + '"' + (lookupEnabled ? ' checked' : '') + '><label class="form-check-label small">aktiv</label></div>' +
                '<select class="form-select form-select-sm mb-1" data-map-lookup-object="' + esc(col) + '">' + lookupObjOptions + '</select>' +
                '<select class="form-select form-select-sm" data-map-lookup-field="' + esc(col) + '">' +
                  '<option value="">- Feld wählen -</option>' +
                  (lookupField ? '<option value="' + esc(lookupField) + '" selected>' + esc(lookupField) + '</option>' : '') +
                '</select>' +
              '</td>' +
              '<td><input class="form-control form-control-sm" placeholder="A=B; C=D" value="' + esc(picklistText) + '" data-map-picklist="' + esc(col) + '" /></td>' +
              '</tr>';
          }).join('') +
          '</tbody></table></div>' +
          '<datalist id="' + esc(objectFieldListId) + '">' +
          (sfFields || []).map((f) => {
            const label = f.label && f.label !== f.name ? f.label + ' (' + f.name + ')' : f.name;
            return '<option value="' + esc(f.name) + '">' + esc(label) + '</option>';
          }).join('') +
          '</datalist>' +
          (obj.previewRows && obj.previewRows.length
            ? '<div class="small text-secondary mt-1">Vorschau (3 Zeilen): ' +
              '<table class="table table-sm table-bordered"><thead><tr>' +
              obj.fileColumns.map((c) => '<th class="small">' + esc(c) + '</th>').join('') +
              '</tr></thead><tbody>' +
              obj.previewRows.map((row) => '<tr>' + obj.fileColumns.map((c) => '<td class="small">' + esc(String(row[c] ?? '')) + '</td>').join('') + '</tr>').join('') +
              '</tbody></table></div>'
            : '');

        const updateMappingEntry = (col) => {
          const objId = objectId;
          const target = migState.objects.find((o) => o.id === objId);
          if (!target) return;
          if (!target.fieldMappings) target.fieldMappings = [];
          const idx = target.fieldMappings.findIndex((m) => m.sourceColumn === col);
          const fieldInput = panel.querySelector('[data-map-col="' + col + '"]');
          const transformSel = panel.querySelector('[data-map-transform="' + col + '"]');
          const transformExprEl = panel.querySelector('[data-map-transform-expression="' + col + '"]');
          const lookupEnabledEl = panel.querySelector('[data-map-lookup-enabled="' + col + '"]');
          const lookupObjectEl = panel.querySelector('[data-map-lookup-object="' + col + '"]');
          const lookupFieldEl = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          const picklistEl = panel.querySelector('[data-map-picklist="' + col + '"]');

          const selectedFieldName = String(fieldInput?.value || '').trim();
          if (!selectedFieldName) {
            if (idx >= 0) target.fieldMappings.splice(idx, 1);
            const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
            if (typeBadge) typeBadge.textContent = '';
            renderMigMissingFields();
            return;
          }

          const sfField = (sfFields || []).find((f) => f.name === selectedFieldName);
          const current = idx >= 0 ? target.fieldMappings[idx] : { sourceColumn: col };
          const nextEntry = {
            ...current,
            sourceColumn: col,
            targetField: selectedFieldName,
            targetFieldLabel: sfField?.label || selectedFieldName,
            targetFieldType: sfField?.type,
            transformFunction: String(transformSel?.value || 'NONE'),
            transformExpression: String(transformExprEl?.value || '').trim(),
            lookupEnabled: Boolean(lookupEnabledEl?.checked),
            lookupObject: String(lookupObjectEl?.value || '').trim(),
            lookupField: String(lookupFieldEl?.value || '').trim(),
            picklistMappings: parsePicklistText(picklistEl?.value),
            _isMissing: !sfField
          };

          if (idx >= 0) target.fieldMappings[idx] = nextEntry;
          else target.fieldMappings.push(nextEntry);

          const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
          if (typeBadge) {
            typeBadge.textContent = sfField ? String(sfField.type || '') : (selectedFieldName ? 'neu' : '');
          }

          renderMigMissingFields();
        };

        // Helper: load lookup fields for a column's lookup-field <select>
        const loadLookupFields = async (col, selectedObject) => {
          const fieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          if (!fieldSel) return;
          if (!selectedObject) {
            const cur = fieldSel.value;
            fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' + (cur ? '<option value="' + esc(cur) + '" selected>' + esc(cur) + '</option>' : '');
            return;
          }
          try {
            const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(selectedObject) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
            if (!res.ok) return;
            const fields = await res.json();
            const curVal = fieldSel.value;
            fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' +
              (fields || []).map((f) => '<option value="' + esc(f.name) + '"' + (f.name === curVal ? ' selected' : '') + '>' + esc(f.label && f.label !== f.name ? f.label + ' (' + f.name + ')' : f.name) + '</option>').join('');
          } catch { /* ignore */ }
        };

        obj.fileColumns.forEach((col) => {
          // SF field text input
          const sfFieldInput = panel.querySelector('[data-map-col="' + col + '"]');
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

          // Lookup field select
          const lookupFieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
          if (lookupFieldSel) {
            lookupFieldSel.addEventListener('change', () => updateMappingEntry(col));
          }

          // Picklist text input
          const picklistEl = panel.querySelector('[data-map-picklist="' + col + '"]');
          if (picklistEl) {
            picklistEl.addEventListener('input', () => updateMappingEntry(col));
            picklistEl.addEventListener('change', () => updateMappingEntry(col));
          }

          // Pre-load lookup fields for rows that already have a lookup object set
          const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
          if (existing?.lookupObject) {
            loadLookupFields(col, existing.lookupObject);
          }
        });

        renderMigMissingFields();
      }

      function renderMigMissingFields() {
        const container = document.getElementById('mig-missing-fields-list');
        if (!container) return;
        const missing = [];
        for (const obj of migState.objects) {
          const existingFieldNames = new Set((obj._existingFieldNames || []).map((name) => String(name).toLowerCase()));
          for (const mapping of (obj.fieldMappings || [])) {
            const isMissing = !!mapping.targetField && !existingFieldNames.has(String(mapping.targetField).toLowerCase());
            mapping._isMissing = isMissing;
            if (isMissing) {
              missing.push({ obj, mapping });
            }
          }
        }
        if (!missing.length) {
          container.innerHTML = '<div class="alert alert-success">Alle gemappten Felder existieren in Salesforce – keine Aktion erforderlich.</div>';
          return;
        }
        container.innerHTML = '<table class="table table-sm"><thead><tr><th>Objekt</th><th>SF-Feld</th><th>Typ</th><th>Aktion</th></tr></thead><tbody>' +
          missing.map((item) =>
            '<tr><td>' + esc(item.obj.salesforceObject) + '</td>' +
            '<td><code>' + esc(item.mapping.targetField) + '</code></td>' +
            '<td><select class="form-select form-select-sm" data-field-type="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' +
            ['Text', 'Number', 'Date', 'DateTime', 'Checkbox', 'Currency', 'Percent', 'Email', 'Phone', 'Url'].map((t) => '<option>' + t + '</option>').join('') +
            '</select></td>' +
            '<td><button class="btn btn-sm btn-outline-primary" data-create-field-obj="' + esc(item.obj.id) + '" data-create-field-name="' + esc(item.mapping.targetField) + '">Anlegen</button></td></tr>'
          ).join('') + '</tbody></table>';

        container.querySelectorAll('[data-create-field-obj]').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const objId = btn.getAttribute('data-create-field-obj');
            const fieldName = btn.getAttribute('data-create-field-name');
            const typeKey = fieldName + '-' + objId;
            const typeSelect = container.querySelector('[data-field-type="' + typeKey + '"]');
            const fieldType = typeSelect ? typeSelect.value : 'Text';
            const obj = migState.objects.find((o) => o.id === objId);
            if (!obj) return;
            btn.disabled = true; btn.textContent = '…';
            try {
              const res = await fetch('/api/salesforce/create-field', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ objectApiName: obj.salesforceObject, fieldApiName: fieldName, fieldType, instanceId: state.instanceId })
              });
              const result = await res.json();
              if (!res.ok) throw new Error(result.error || 'Fehler');

              obj._existingFieldNames = Array.from(new Set([...(obj._existingFieldNames || []), String(fieldName).toLowerCase()]));
              const mapped = (obj.fieldMappings || []).find((m) => m.targetField === fieldName);
              if (mapped) {
                mapped._isMissing = false;
              }

              btn.className = 'btn btn-sm btn-success'; btn.textContent = '✓ Angelegt';
              const resultDiv = document.getElementById('mig-create-fields-result');
              if (resultDiv) resultDiv.innerHTML += '<div class="alert alert-success py-1 small mt-1">' + esc(obj.salesforceObject + '.' + fieldName) + ' erfolgreich angelegt.</div>';
              renderMigMissingFields();
            } catch (err) {
              btn.className = 'btn btn-sm btn-danger'; btn.textContent = 'Fehler';
              alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
            }
          });
        });
      }

      function renderMigDependencies() {
        const container = document.getElementById('mig-dependencies-list');
        if (!container) return;
        if (!migState.dependencies.length) {
          container.innerHTML = '<div class="text-secondary small">Keine Abhängigkeiten definiert.</div>';
          return;
        }
        container.innerHTML = migState.dependencies.map((dep, i) => {
          const fromObj = migState.objects.find((o) => o.id === dep.fromObjectId);
          const toObj = migState.objects.find((o) => o.id === dep.toObjectId);
          return '<div class="d-flex align-items-center gap-2 mb-1 p-2 border rounded">' +
            '<strong>' + esc(fromObj?.salesforceObject || dep.fromObjectId) + '</strong>' +
            ' → ' +
            '<strong>' + esc(toObj?.salesforceObject || dep.toObjectId) + '</strong>' +
            ' <span class="text-secondary small">(' + esc(dep.fromField) + ' ← ' + esc(dep.toField) + ')</span>' +
            '<button class="btn btn-sm btn-outline-danger ms-auto" data-remove-dep="' + i + '">✕</button>' +
            '</div>';
        }).join('');
        container.querySelectorAll('[data-remove-dep]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const idx = Number(btn.getAttribute('data-remove-dep'));
            migState.dependencies.splice(idx, 1);
            renderMigDependencies();
          });
        });
      }

      function renderMigOrderList() {
        const list = document.getElementById('mig-order-list');
        if (!list) return;
        // Build plan if empty
        if (!migState.executionPlan.length && migState.objects.length) {
          migState.executionPlan = migState.objects.map((obj, idx) => ({ order: idx + 1, objectId: obj.id }));
        }
        const ordered = [...migState.executionPlan].sort((a, b) => a.order - b.order);
        list.innerHTML = ordered.map((step, i) => {
          const obj = migState.objects.find((o) => o.id === step.objectId);
          return '<li class="list-group-item d-flex align-items-center gap-2">' +
            '<span class="badge bg-secondary">' + (i + 1) + '</span>' +
            '<span class="flex-grow-1">' + esc(obj?.salesforceObject || step.objectId) + '</span>' +
            '<div class="btn-group btn-group-sm">' +
            '<button class="btn btn-outline-secondary" data-order-up="' + i + '" ' + (i === 0 ? 'disabled' : '') + '>↑</button>' +
            '<button class="btn btn-outline-secondary" data-order-down="' + i + '" ' + (i === ordered.length - 1 ? 'disabled' : '') + '>↓</button>' +
            '</div></li>';
        }).join('');
        list.querySelectorAll('[data-order-up]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const i = Number(btn.getAttribute('data-order-up'));
            if (i <= 0) return;
            [migState.executionPlan[i - 1], migState.executionPlan[i]] = [migState.executionPlan[i], migState.executionPlan[i - 1]];
            migState.executionPlan.forEach((s, idx) => { s.order = idx + 1; });
            renderMigOrderList();
          });
        });
        list.querySelectorAll('[data-order-down]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const i = Number(btn.getAttribute('data-order-down'));
            if (i >= migState.executionPlan.length - 1) return;
            [migState.executionPlan[i], migState.executionPlan[i + 1]] = [migState.executionPlan[i + 1], migState.executionPlan[i]];
            migState.executionPlan.forEach((s, idx) => { s.order = idx + 1; });
            renderMigOrderList();
          });
        });
      }

      function renderMigReview() {
        const el = document.getElementById('mig-review-summary');
        if (!el) return;
        const ordered = [...migState.executionPlan].sort((a, b) => a.order - b.order);
        el.innerHTML = '<div class="card soft-card"><div class="card-body"><h6>' + esc(migState.name) + '</h6>' +
          '<p class="text-secondary small">' + esc(migState.description || '') + '</p>' +
          '<strong>Ausführungsplan:</strong><ol class="mt-1">' +
          ordered.map((step) => {
            const obj = migState.objects.find((o) => o.id === step.objectId);
            if (!obj) return '';
            return '<li>' + esc(obj.salesforceObject) + ' — ' + esc(obj.operation) +
              ' — Datei: <code>' + esc(obj.filePath || '(keine)') + '</code>' +
              ' — Felder gemappt: ' + (obj.fieldMappings || []).length + '</li>';
          }).join('') +
          '</ol>' +
          (migState.dependencies.length ? '<strong>Abhängigkeiten:</strong><ul>' +
            migState.dependencies.map((dep) => {
              const from = migState.objects.find((o) => o.id === dep.fromObjectId);
              const to = migState.objects.find((o) => o.id === dep.toObjectId);
              return '<li>' + esc(from?.salesforceObject || '') + ' → ' + esc(to?.salesforceObject || '') + '</li>';
            }).join('') + '</ul>' : '') +
          '</div></div>';
      }

      function renderMigDepSelects() {
        ['mig-dep-from', 'mig-dep-to'].forEach((id) => {
          const sel = document.getElementById(id);
          if (!sel) return;
          sel.innerHTML = migState.objects.map((obj) =>
            '<option value="' + esc(obj.id) + '">' + esc(obj.salesforceObject) + '</option>'
          ).join('');
        });
      }

      async function migSave() {
        const nameEl = document.getElementById('mig-name');
        const descEl = document.getElementById('mig-description');
        if (nameEl) migState.name = nameEl.value.trim() || migState.name;
        if (descEl) migState.description = descEl.value.trim();

        const payload = {
          id: migState.id,
          name: migState.name,
          description: migState.description,
          instanceId: state.instanceId || undefined,
          status: 'draft',
          objects: migState.objects,
          dependencies: migState.dependencies,
          executionPlan: migState.executionPlan
        };
        const method = migState.id ? 'PUT' : 'POST';
        const url = migState.id ? '/api/migrations/' + encodeURIComponent(migState.id) : '/api/migrations';
        const res = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const saved = await res.json();
        if (!migState.id) migState.id = saved.id;
        return saved;
      }

      function openMigWizard(migration) {
        migState.id = migration ? migration.id : null;
        migState.step = 1;
        migState.name = migration ? migration.name : '';
        migState.description = migration ? (migration.description || '') : '';
        migState.objects = migration ? JSON.parse(JSON.stringify(migration.objects || [])) : [];
        migState.dependencies = migration ? JSON.parse(JSON.stringify(migration.dependencies || [])) : [];
        migState.executionPlan = migration ? JSON.parse(JSON.stringify(migration.executionPlan || [])) : [];
        migState.sfObjects = [];

        const nameEl = document.getElementById('mig-name');
        const descEl = document.getElementById('mig-description');
        if (nameEl) nameEl.value = migState.name;
        if (descEl) descEl.value = migState.description;

        renderMigWizardSteps();
        renderMigSelectedObjects();

        const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('migration-modal'));
        document.getElementById('migration-modal-title').textContent = migration ? 'Migration bearbeiten: ' + migration.name : 'Neue Migration';
        modal.show();
      }

      async function renderMigrationList() {
        const body = document.getElementById('migration-list-body');
        if (!body) return;
        try {
          const res = await fetch('/api/migrations');
          const data = await res.json();
          const items = data.items || [];
          if (!items.length) {
            body.innerHTML = '<tr><td colspan="5" class="text-secondary">Keine Migrationen vorhanden.</td></tr>';
            return;
          }
          const statusBadge = (s) => {
            const map = { draft: 'secondary', ready: 'info', running: 'warning', done: 'success', error: 'danger' };
            return '<span class="badge bg-' + (map[s] || 'secondary') + '">' + esc(s) + '</span>';
          };
          body.innerHTML = items.map((mig) =>
            '<tr>' +
            '<td>' + esc(mig.name) + '</td>' +
            '<td>' + statusBadge(mig.status) + '</td>' +
            '<td>' + (mig.objects ? mig.objects.length : 0) + ' Objekte</td>' +
            '<td>' + (mig.lastRunAt ? formatDate(mig.lastRunAt, 'short') : '-') + '</td>' +
            '<td>' +
            '<div class="btn-group btn-group-sm">' +
            '<button class="btn btn-outline-primary" data-mig-edit="' + esc(mig.id) + '">Bearbeiten</button>' +
            '<button class="btn btn-outline-success" data-mig-run="' + esc(mig.id) + '" ' + (mig.status === 'running' ? 'disabled' : '') + '>▶ Starten</button>' +
            '<button class="btn btn-outline-danger" data-mig-delete="' + esc(mig.id) + '">✕</button>' +
            '</div></td></tr>'
          ).join('');

          body.querySelectorAll('[data-mig-edit]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-edit');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig);
            });
          });

          body.querySelectorAll('[data-mig-delete]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              if (!confirm('Migration wirklich löschen?')) return;
              await fetch('/api/migrations/' + encodeURIComponent(btn.getAttribute('data-mig-delete')), { method: 'DELETE' });
              renderMigrationList();
            });
          });

          body.querySelectorAll('[data-mig-run]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-run');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig);
              // Jump to last step for execution
              migState.step = migState.totalSteps;
              renderMigWizardSteps();
              renderMigReview();
            });
          });
        } catch (err) {
          body.innerHTML = '<tr><td colspan="5" class="text-danger">Fehler: ' + esc(String(err)) + '</td></tr>';
        }
      }

      // Wire up tab activation to load migration list
      document.querySelector('[data-bs-target="#tab-migration"]')?.addEventListener('click', () => {
        renderMigrationList();
      });

      document.getElementById('new-migration')?.addEventListener('click', () => {
        openMigWizard(null);
      });

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
          nextBtn.disabled = true; prevBtn.disabled = true;
          progressEl.classList.remove('d-none');
          resultEl.classList.add('d-none');
          try {
            await migSave();
            const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/run', { method: 'POST' });
            const result = await res.json();
            progressEl.classList.add('d-none');
            resultEl.classList.remove('d-none');
            const allOk = result.steps.every((s) => s.status !== 'error');
            resultEl.innerHTML = '<div class="alert ' + (allOk ? 'alert-success' : 'alert-warning') + '">' +
              (allOk ? '✓ Migration erfolgreich abgeschlossen.' : '⚠ Migration mit Fehlern abgeschlossen.') +
              '</div>' +
              (result.reportPath ? '<div class="alert alert-info py-2 small">Protokoll erzeugt: <code>' + esc(result.reportPath) + '</code></div>' : '') +
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

                    if (!records.length) {
                      detailsDiv.innerHTML = '<div class="alert alert-info">Keine fehlgeschlagenen Datensätze gefunden.</div>';
                      btn.textContent = 'Details laden';
                      return;
                    }

                    detailsDiv.innerHTML =
                      '<div class="d-flex align-items-center gap-2 mb-2 flex-wrap">' +
                        '<button class="btn btn-sm btn-primary" data-retry-failed-records data-mode="all" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Korrigierte Datensätze neu importieren</button>' +
                        '<button class="btn btn-sm btn-outline-primary" data-retry-failed-records data-mode="partial" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Nur erfolgreiche Korrekturen übernehmen</button>' +
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
                      if (str.includes('"') || str.includes('\n') || str.includes('\r') || str.includes(delimiter)) {
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
                      }
                    };

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
            progressEl.classList.add('d-none');
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

      // SF Objects loading
      document.getElementById('mig-load-sf-objects')?.addEventListener('click', async () => {
        const btn = document.getElementById('mig-load-sf-objects');
        btn.disabled = true; btn.textContent = '…';
        const listEl = document.getElementById('mig-sf-objects-list');
        const searchWrap = document.getElementById('mig-sf-objects-search-wrap');
        try {
          const res = await fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(state.instanceId || ''));
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
            migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: label, filePath: '', fileColumns: [], fieldMappings: [], operation: 'insert' });
            renderMigSelectedObjects();
            btn.className = 'btn btn-sm btn-success disabled me-1 mb-1';
          });
        });
      }

      document.getElementById('mig-add-manual-object')?.addEventListener('click', () => {
        const input = document.getElementById('mig-manual-object');
        const name = input ? input.value.trim() : '';
        if (!name) return;
        if (migState.objects.some((o) => o.salesforceObject === name)) { alert('Objekt bereits hinzugefügt.'); return; }
        migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: name, filePath: '', fileColumns: [], fieldMappings: [], operation: 'insert' });
        renderMigSelectedObjects();
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

      (async () => {
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
      })();

    </script>
  </body>
</html>`;
}

export function createAppServer(
  getHealthSnapshot: () => HealthSnapshot,
  adminDataService = new AdminDataService()
): http.Server {
  return http.createServer((req, res) => {
    void (async () => {
      const requestUrl = new URL(req.url || "/", "http://localhost");
      const sendJson = (statusCode: number, payload: unknown): void => {
        res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify(payload));
      };
      const sendFile = async (filePath: string, contentType: string): Promise<void> => {
        const file = await fs.readFile(filePath);
        res.writeHead(200, {
          "Content-Type": contentType,
          "Cache-Control": "public, max-age=31536000"
        });
        res.end(file);
      };

      const instanceId = requestUrl.searchParams.get("instanceId") || undefined;
      const connectorTestMatch = req.method === "POST" ? requestUrl.pathname.match(/^\/api\/connectors\/([^/]+)\/test$/) : null;
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

      if (req.method === "GET" && requestUrl.pathname === "/api/system/health") {
        sendJson(200, {
          ...getHealthSnapshot(),
          cpuLoadPercent: getCpuLoadPercent()
        });
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
        sendJson(200, result);
        return;
      }

      if (scheduleDeleteMatch) {
        const scheduleId = decodeURIComponent(scheduleDeleteMatch[1]);
        const result = await adminDataService.deleteSchedule(scheduleId, instanceId);
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
        const type = typeParam === "error" ? "error" : "all";
        const limit = Number(requestUrl.searchParams.get("limit") || 300);

        if (!start || !end) {
          sendJson(400, { error: "start und end sind erforderlich" });
          return;
        }

        const logs = await adminDataService.listLogsByRange(start, end, type, limit, instanceId);
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

      if (req.method === "POST" && requestUrl.pathname === "/api/salesforce/create-field") {
        const body = (await readJsonBody(req)) as { objectApiName?: string; fieldApiName?: string; fieldType?: string };
        if (!body.objectApiName || !body.fieldApiName) {
          sendJson(400, { error: "objectApiName and fieldApiName required" });
          return;
        }
        const result = await adminDataService.createSalesforceCustomField(
          body.objectApiName,
          body.fieldApiName,
          body.fieldType || "Text",
          instanceId
        );
        sendJson(200, result);
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
        sendJson(200, { items: adminDataService.listMigrations() });
        return;
      }

      const migrationIdMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)$/);
      const migrationRunMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/run$/);
      const migrationAnalyzeMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/analyze-file\/([^/]+)$/);
        const failedRecordsMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/failed-records\/([^/]+)$/);
        const retryFailedRecordsMatch = requestUrl.pathname.match(/^\/api\/migrations\/([^/]+)\/failed-records\/([^/]+)\/([^/]+)\/retry$/);

      if (req.method === "POST" && requestUrl.pathname === "/api/migrations/upload-file") {
        const body = (await readJsonBody(req)) as {
          migrationId?: string;
          objectId?: string;
          fileName?: string;
          contentBase64?: string;
        };

        const migrationId = String(body.migrationId || "").trim();
        const objectId = String(body.objectId || "").trim();
        const fileName = path.basename(String(body.fileName || "").trim());
        const contentBase64 = String(body.contentBase64 || "").trim();

        if (!migrationId || !objectId || !fileName || !contentBase64) {
          sendJson(400, { error: "migrationId, objectId, fileName und contentBase64 sind erforderlich" });
          return;
        }

        const migration = adminDataService.getMigration(migrationId);
        if (!migration) {
          sendJson(404, { error: "Migration not found" });
          return;
        }

        const obj = migration.objects.find((item) => item.id === objectId);
        if (!obj) {
          sendJson(404, { error: "Object not found" });
          return;
        }

        const fileBuffer = Buffer.from(contentBase64, "base64");
        const targetDir = path.resolve(process.cwd(), "artifacts/files/inbound/migrations", migrationId);
        await fs.mkdir(targetDir, { recursive: true });
        const absolutePath = path.resolve(targetDir, fileName);
        await fs.writeFile(absolutePath, fileBuffer);

        const analysis = adminDataService.analyzeFileBuffer(fileName, fileBuffer);
        const relativePath = path.relative(process.cwd(), absolutePath).split(path.sep).join("/");

        sendJson(200, {
          filePath: relativePath,
          fields: analysis.fields,
          rows: analysis.rows
        });
        return;
      }

      if (migrationRunMatch && req.method === "POST") {
        const migId = decodeURIComponent(migrationRunMatch[1]);
        const result = await adminDataService.runMigration(migId, instanceId || undefined);
        sendJson(200, result);
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
        if (!obj || !obj.filePath) {
          sendJson(404, { error: "Object or filePath not found" });
          return;
        }
        const absolutePath = path.isAbsolute(obj.filePath)
          ? obj.filePath
          : path.resolve(process.cwd(), obj.filePath);
        const fileBuffer = await fs.readFile(absolutePath);
        const fileName = path.basename(absolutePath);
        const analysis = adminDataService.analyzeFileBuffer(fileName, fileBuffer);
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
        };
        const result = await adminDataService.retryFailedMigrationRecords(
          migId,
          objectId,
          failedRecordsId,
          Array.isArray(body.records) ? body.records : [],
          instanceId || undefined
        );
        sendJson(200, result);
        return;
      }

      if (migrationIdMatch) {
        const migId = decodeURIComponent(migrationIdMatch[1]);
        if (req.method === "GET") {
          const m = adminDataService.getMigration(migId);
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
          sendJson(200, updated);
          return;
        }
        if (req.method === "DELETE") {
          const deleted = adminDataService.deleteMigration(migId);
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
          instanceId: body.instanceId || instanceId || undefined,
          status: body.status || "draft",
          objects: body.objects || [],
          dependencies: body.dependencies || [],
          executionPlan: body.executionPlan || []
        });
        sendJson(201, saved);
        return;
      }

      if (req.method === "GET" && requestUrl.pathname === "/") {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(htmlShell());
        return;
      }

      sendJson(404, { error: "Not Found" });
    })().catch((error) => {
      res.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify({ error: error instanceof Error ? error.message : "Unknown server error" }));
    });
  });
}
