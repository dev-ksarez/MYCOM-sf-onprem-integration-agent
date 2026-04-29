import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import {
  AdminDataService,
  SalesforceInstanceMutationInput,
  ConnectorMutationInput,
  ScheduleMutationInput,
  LogChartRange
} from "./admin-data-service";
import {
  ScheduleFormOptions
} from "./admin-data-service";

const BOOTSTRAP_CSS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/css/bootstrap.min.css");
const BOOTSTRAP_JS_FILE = path.resolve(process.cwd(), "node_modules/bootstrap/dist/js/bootstrap.bundle.min.js");
const CHART_JS_FILE = path.resolve(process.cwd(), "node_modules/chart.js/dist/chart.umd.js");
const APP_STYLE_CSS_FILE = path.resolve(process.cwd(), "src/css/style.css");
const AGENT_UI_CSS_FILE = path.resolve(process.cwd(), "src/css/agent-ui.css");

export interface HealthSnapshot {
  service: "ok" | "degraded";
  scheduler: "running" | "idle" | "error";
  startedAt: string;
  uptimeSeconds: number;
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
    <nav class="navbar navbar-expand-lg bg-white border-bottom">
      <div class="container-fluid px-4">
        <span class="navbar-brand fw-semibold">SF Integration Agent</span>
        <div class="d-flex gap-2 align-items-center ms-auto">
          <label class="small text-secondary">Instanz</label>
          <select id="instance-select" class="form-select form-select-sm" style="min-width: 240px;"></select>
          <button id="add-instance" class="btn btn-sm btn-outline-secondary">Instanz hinzufügen</button>
          <button id="refresh-all" class="btn btn-sm btn-outline-primary">Aktualisieren</button>
        </div>
      </div>
    </nav>

    <main class="container-fluid px-4 py-4">
      <div id="global-alert" class="alert alert-danger d-none" role="alert"></div>

      <ul class="nav nav-tabs mb-3" id="main-tabs" role="tablist">
        <li class="nav-item" role="presentation"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-overview" type="button">Übersicht</button></li>
        <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-schedulers" type="button">Scheduler</button></li>
        <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-connectors" type="button">Connectoren</button></li>
        <li class="nav-item" role="presentation"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-monitor" type="button">Monitoring</button></li>
      </ul>

      <div class="tab-content">
        <section class="tab-pane fade show active" id="tab-overview" role="tabpanel">
          <div class="row g-3 mb-3">
            <div class="col-md-3"><div class="card soft-card mini-kpi"><div class="card-body"><div class="text-secondary small">Service</div><h5 id="kpi-service" class="mb-0">-</h5></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi"><div class="card-body"><div class="text-secondary small">Scheduler</div><h5 id="kpi-scheduler" class="mb-0">-</h5></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi"><div class="card-body"><div class="text-secondary small">Aktive Scheduler</div><h5 id="kpi-schedules" class="mb-0">0</h5></div></div></div>
            <div class="col-md-3"><div class="card soft-card mini-kpi"><div class="card-body"><div class="text-secondary small">Connectoren</div><h5 id="kpi-connectors" class="mb-0">0</h5></div></div></div>
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
                  <div class="small text-secondary mt-2">Klick auf einen Knoten öffnet die passende Konfiguration im Modal.</div>
                </div>
              </div>
            </div>
            <div class="col-lg-5">
              <div class="card soft-card">
                <div class="card-header bg-white fw-semibold">Letzte Runs</div>
                <div class="card-body p-0">
                  <table class="table table-sm mb-0">
                    <thead><tr><th>Schedule</th><th>Status</th><th>Start</th></tr></thead>
                    <tbody id="overview-runs-body"></tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
          <div class="row g-3 mt-1">
            <div class="col-12">
              <div class="card soft-card">
                <div class="card-header bg-white d-flex justify-content-between align-items-center">
                  <span class="fw-semibold">Logs und Fehler</span>
                  <select id="log-chart-range" class="form-select form-select-sm" style="max-width: 220px;">
                    <option value="last_hour">Letzte Stunde</option>
                    <option value="last_24h" selected>Letzte 24h</option>
                    <option value="last_30d">Letzte 30 Tage</option>
                  </select>
                </div>
                <div class="card-body">
                  <div class="logs-chart-wrap">
                    <canvas id="logs-chart"></canvas>
                  </div>
                  <div class="small text-secondary mt-2">Klick auf einen Balken öffnet die zugehörige Logliste.</div>
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
    </main>

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
                  <div class="col-md-6"><label class="form-label">Source Type</label><select id="sch-source-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE_SOQL">SALESFORCE_SOQL</option><option value="MSSQL_SQL">MSSQL_SQL</option></select></div>
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
                  <div class="col-md-4"><label class="form-label">Target Type</label><select id="sch-target-type" class="form-select"><option value="">- Wählen -</option><option value="SALESFORCE">SALESFORCE</option><option value="SALESFORCE_GLOBAL_PICKLIST">SALESFORCE_GLOBAL_PICKLIST</option><option value="MSSQL">MSSQL</option></select></div>
                  <div class="col-md-4"><label class="form-label">Direction</label><select id="sch-direction" class="form-select"><option value="">- Wählen -</option></select></div>
                  <div class="col-md-12"><label class="form-label">Target Definition (JSON)</label><textarea id="sch-target-definition" class="form-control" rows="4" placeholder='{"fields":[...]}'></textarea></div>
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
                        <button id="sch-load-source-fields" type="button" class="btn btn-outline-secondary btn-sm">Felder laden</button>
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
          <div class="modal-header"><h5 class="modal-title">Connector konfigurieren</h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div>
          <div class="modal-body">
            <input id="con-id" type="hidden" />
            <div class="row g-2">
              <div class="col-md-4"><label class="form-label">Name</label><input id="con-name" class="form-control" /></div>
              <div class="col-md-4"><label class="form-label">Connector Type</label><input id="con-type" class="form-control" /></div>
              <div class="col-md-4"><label class="form-label">Target System</label><input id="con-target-system" class="form-control" /></div>
              <div class="col-md-4"><label class="form-label">Direction</label><input id="con-direction" class="form-control" /></div>
              <div class="col-md-4"><label class="form-label">Secret Key (ENV)</label><input id="con-secret" class="form-control" /></div>
              <div class="col-md-2"><label class="form-label">Timeout</label><input id="con-timeout" type="number" class="form-control" /></div>
              <div class="col-md-2"><label class="form-label">Retries</label><input id="con-retries" type="number" class="form-control" /></div>
              <div class="col-md-12"><label class="form-label">Beschreibung</label><textarea id="con-description" class="form-control" rows="2"></textarea></div>
              <div class="col-md-12"><label class="form-label">Parameters (JSON)</label><textarea id="con-parameters" class="form-control" rows="4" placeholder='{"server":"...","database":"..."}'></textarea></div>
              <div class="col-md-6 d-flex align-items-end"><div class="form-check"><input id="con-active" class="form-check-input" type="checkbox" checked /><label class="form-check-label">Aktiv</label></div></div>
            </div>
          </div>
          <div class="modal-footer">
            <button id="test-connector" type="button" class="btn btn-outline-secondary">Verbindung testen</button>
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
      const state = {
        instanceId: '',
        schedules: [],
        connectors: [],
        graphData: { nodes: [], edges: [] },
        overviewConnectorFilterId: '',
        schedulerConnectorFilterId: '',
        schedulerDirectionTab: 'all',
        runs: [],
        mappingFields: [],
        mappingRules: [],
        selectedMappingRuleId: '',
        logSummary: null,
        scheduleOptions: {
          objectNames: [],
          operations: [],
          sourceSystems: [],
          targetSystems: [],
          directions: []
        }
      };

      let logsChart;

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
          select.innerHTML = '<option value="">- Wählen -</option>';
          return;
        }

        // Clear select while loading
        select.innerHTML = '<option value="">Wird geladen...</option>';

        // Always use selected target object/table as base
        const targetObject = objectName;
        if (!targetObject) {
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
          const selectedClass = state.selectedMappingRuleId === rule.id ? ' class="mapping-rule-selected"' : '';
          const lookupLabel = rule.lookupEnabled ? 'Ja' : '-';
          const functionLabel = rule.transformFunction && rule.transformFunction !== 'NONE' ? rule.transformFunction : '-';
          const picklistCount = Array.isArray(rule.picklistMappings) ? rule.picklistMappings.length : 0;
          return (
            '<tr data-rule-id="' + esc(rule.id) + '"' + selectedClass + '>' +
              '<td>' + esc(rule.sourceField || '-') + '</td>' +
              '<td>' + esc(rule.targetField || '-') + '</td>' +
              '<td>' + esc(lookupLabel) + '</td>' +
              '<td>' + esc(functionLabel) + '</td>' +
              '<td>' + esc(picklistCount ? String(picklistCount) + ' Einträge' : '-') + '</td>' +
              '<td><button type="button" class="btn btn-sm btn-outline-danger" data-delete-map-rule="' + esc(rule.id) + '">Löschen</button></td>' +
            '</tr>'
          );
        }).join('');

        rulesBody.querySelectorAll('tr[data-rule-id]').forEach((row) => {
          row.addEventListener('click', (event) => {
            const target = event.target;
            if (target && target.closest && target.closest('button[data-delete-map-rule]')) {
              return;
            }
            state.selectedMappingRuleId = row.getAttribute('data-rule-id') || '';
            renderMappingRulesTable();
          });
        });

        rulesBody.querySelectorAll('button[data-delete-map-rule]').forEach((button) => {
          button.addEventListener('click', (event) => {
            event.stopPropagation();
            const ruleId = button.getAttribute('data-delete-map-rule');
            state.mappingRules = state.mappingRules.filter((item) => item.id !== ruleId);
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
                  targetField: String(item.targetField || item.sourceField || '').trim(),
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

        highlightWrap.classList.toggle('d-none', !isSql);
        if (isSql) {
          highlight.innerHTML = highlightSqlQuery(sourceDefinition || '-- keine SQL-Abfrage --');
          status.textContent = 'SQL-Abfrage kann direkt getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
        } else if (sourceType === 'SALESFORCE_SOQL') {
          highlight.textContent = '';
          status.textContent = 'SOQL-Abfrage kann direkt gegen Salesforce getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
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
          type: 'bar',
          data: {
            labels,
            datasets: [
              {
                label: 'Logs',
                data: totals,
                backgroundColor: 'rgba(62, 137, 189, 0.70)',
                borderColor: 'rgba(62, 137, 189, 1)',
                borderWidth: 1
              },
              {
                label: 'Fehler',
                data: errors,
                backgroundColor: 'rgba(208, 73, 73, 0.70)',
                borderColor: 'rgba(208, 73, 73, 1)',
                borderWidth: 1
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

      function renderOverview(healthData) {
        document.getElementById('kpi-service').textContent = healthData.service || '-';
        document.getElementById('kpi-scheduler').textContent = healthData.scheduler || '-';
        document.getElementById('kpi-schedules').textContent = String(state.schedules.length);
        document.getElementById('kpi-connectors').textContent = String(state.connectors.length);

        const body = document.getElementById('overview-runs-body');
        if (!state.runs.length) {
          body.innerHTML = '<tr><td colspan="3" class="text-secondary">Keine Runs gefunden.</td></tr>';
          return;
        }

        body.innerHTML = state.runs.slice(0, 8).map((run) =>
          '<tr><td class="text-truncate" title="' + esc(run.scheduleName || run.scheduleId || '-') + '">' + esc(run.scheduleName || run.scheduleId || '-') + '</td><td>' + getStatusBadge(run.status) + '</td><td>' + formatDate(run.startedAt, 'short') + '</td></tr>'
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
        renderSchedulerConnectorFilterOptions();

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
          const directionClass = node.kind === 'scheduler'
            ? (isInbound ? 'graph-inbound' : 'graph-outbound')
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
            '<g class="graph-node ' + directionClass + '" data-kind="' + esc(node.kind) + '" data-ref-id="' + esc(node.refId) + '" transform="translate(' + Number(node.x) + ',' + Number(node.y) + ')" title="' + esc(node.label) + '">' +
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
        updateSourceQueryAssist();
        setupMappingDropZone();
        hydrateMappingRulesFromDefinition();
        loadTransformFunctions();
        await loadTargetObjects(entry?.objectName || '');
        loadTargetFields();
        // Load mapping fields from backend metadata API
        loadMappingFields();
        scheduleModal.show();
      }

      function openConnectorModal(connectorId) {
        const entry = state.connectors.find((item) => item.id === connectorId);
        document.getElementById('con-id').value = entry?.id || '';
        document.getElementById('con-name').value = entry?.name || '';
        document.getElementById('con-type').value = entry?.connectorType || '';
        document.getElementById('con-target-system').value = entry?.targetSystem || '';
        document.getElementById('con-direction').value = entry?.direction || '';
        document.getElementById('con-secret').value = entry?.secretKey || '';
        document.getElementById('con-timeout').value = entry?.timeoutMs || '';
        document.getElementById('con-retries').value = entry?.maxRetries || '';
        document.getElementById('con-description').value = entry?.description || '';
        document.getElementById('con-parameters').value = JSON.stringify(entry?.parameters || {}, null, 2);
        document.getElementById('con-active').checked = entry ? !!entry.active : true;
        connectorModal.show();
      }

      async function saveSchedule() {
        clearError();
        const saveButton = document.getElementById('save-schedule');
        saveButton.disabled = true;

        try {
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
          showError(error.message || 'Quelle konnte nicht getestet werden');
        } finally {
          testButton.disabled = false;
        }
      }

      async function saveConnector() {
        let parsedParameters = {};
        const rawParameters = document.getElementById('con-parameters').value.trim();
        if (rawParameters) {
          parsedParameters = JSON.parse(rawParameters);
        }

        const payload = {
          id: document.getElementById('con-id').value || undefined,
          name: document.getElementById('con-name').value,
          active: document.getElementById('con-active').checked,
          connectorType: document.getElementById('con-type').value,
          targetSystem: document.getElementById('con-target-system').value || undefined,
          direction: document.getElementById('con-direction').value || undefined,
          secretKey: document.getElementById('con-secret').value || undefined,
          timeoutMs: Number(document.getElementById('con-timeout').value || 0) || undefined,
          maxRetries: Number(document.getElementById('con-retries').value || 0) || undefined,
          description: document.getElementById('con-description').value || undefined,
          parameters: parsedParameters
        };

        await requestJson('/api/connectors', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        connectorModal.hide();
        await refresh();
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
        await loadScheduleOptions();

        state.schedules = schedules.items || [];
        state.connectors = connectors.items || [];
        state.runs = runs.items || [];
        state.graphData = graph;

        renderOverview(healthData);
        renderSchedules();
        renderConnectors();
        renderRuns();
        renderOverviewConnectorFilter();
        drawGraph(buildFilteredOverviewGraph(state.graphData));
        if (shouldRefreshChart) {
          await loadLogSummary();
        }
      }

      document.getElementById('instance-select').addEventListener('change', async (event) => {
        state.instanceId = event.target.value;
        await refresh();
      });
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
      document.getElementById('refresh-all').addEventListener('click', refresh);
      document.getElementById('overview-connector-filter').addEventListener('change', (event) => {
        state.overviewConnectorFilterId = String(event.target?.value || '');
        drawGraph(buildFilteredOverviewGraph(state.graphData));
      });
      document.getElementById('log-chart-range').addEventListener('change', loadLogSummary);
      document.getElementById('sch-load-source-fields').addEventListener('click', loadMappingFields);
      document.getElementById('new-schedule').addEventListener('click', () => openScheduleModal(''));
      document.getElementById('new-connector').addEventListener('click', () => openConnectorModal(''));
      document.getElementById('save-schedule').addEventListener('click', saveSchedule);
      document.getElementById('sch-test-source').addEventListener('click', testScheduleSource);
      document.getElementById('sch-source-type').addEventListener('change', updateSourceQueryAssist);
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
      document.getElementById('save-connector').addEventListener('click', saveConnector);
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
      });
      document.getElementById('sch-target-type').addEventListener('change', () => {
        applyOperationOptions('');
      });
      document.getElementById('sch-object').addEventListener('change', loadTargetFields);
      document.getElementById('sch-connector').addEventListener('change', async () => {
        await loadTargetObjects(document.getElementById('sch-object').value || '');
        await loadTargetFields();
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
      document.getElementById('test-connector').addEventListener('click', async () => {
        const connectorId = document.getElementById('con-id').value;
        if (!connectorId) {
          return;
        }
        const result = await requestJson('/api/connectors/' + encodeURIComponent(connectorId) + '/test', { method: 'POST' });
        alert(result.message || (result.ok ? 'OK' : 'Fehler'));
      });

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
        } catch (error) {
          state.mappingFields = [];
          sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Feldmetadaten konnten nicht geladen werden.</td></tr>';
          showError(error.message || 'Feldmetadaten konnten nicht geladen werden');
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


      (async () => {
        restoreLogChartRange();
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
        sendJson(200, getHealthSnapshot());
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
