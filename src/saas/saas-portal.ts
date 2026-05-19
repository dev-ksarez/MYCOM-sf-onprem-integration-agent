import { SaasPortalSnapshot } from "./saas-repository";

function esc(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDate(value?: string): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString("de-DE", { dateStyle: "short", timeStyle: "short" });
}

function badgeClass(status: string): string {
  const normalized = status.toLowerCase();
  if (normalized === "online" || normalized === "active") {
    return "ok";
  }
  if (normalized === "trial" || normalized === "hybrid" || normalized === "pending") {
    return "warn";
  }
  return "bad";
}

function renderKpi(label: string, value: string | number, hint: string): string {
  return `
    <section class="kpi">
      <span>${esc(label)}</span>
      <strong>${esc(value)}</strong>
      <small>${esc(hint)}</small>
    </section>
  `;
}

export function renderSaasPortal(snapshot: SaasPortalSnapshot): string {
  const failedRate = snapshot.totals.schedulerRuns24h > 0
    ? `${Math.round((snapshot.totals.failedRuns24h / snapshot.totals.schedulerRuns24h) * 100)}%`
    : "0%";

  return `<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SF Integration SaaS Control Center</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #172033;
      --muted: #667085;
      --line: #d7dee8;
      --bg: #f4f7fb;
      --panel: #ffffff;
      --brand: #032d60;
      --brand-2: #0f766e;
      --ok: #047857;
      --warn: #b45309;
      --bad: #b91c1c;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--ink); background: var(--bg); }
    header { min-height: 88px; padding: 22px 32px; color: white; background: linear-gradient(90deg, var(--brand), var(--brand-2)); display: flex; justify-content: space-between; gap: 24px; align-items: center; }
    header h1 { margin: 0; font-size: 24px; letter-spacing: 0; }
    header p { margin: 4px 0 0; color: rgba(255,255,255,.78); }
    main { max-width: 1280px; margin: 0 auto; padding: 24px; }
    .grid { display: grid; gap: 16px; }
    .kpis { grid-template-columns: repeat(6, minmax(0, 1fr)); }
    .two { grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); align-items: start; }
    .kpi, .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; box-shadow: 0 8px 24px rgba(15, 23, 42, .05); }
    .kpi { padding: 14px 16px; min-height: 108px; display: grid; gap: 4px; align-content: center; }
    .kpi span, .kpi small { color: var(--muted); }
    .kpi strong { font-size: 28px; }
    .panel { overflow: hidden; }
    .panel h2 { font-size: 16px; margin: 0; padding: 14px 16px; border-bottom: 1px solid var(--line); }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #eef2f6; vertical-align: top; }
    th { color: var(--muted); font-weight: 700; font-size: 12px; text-transform: uppercase; }
    tr:last-child td { border-bottom: 0; }
    .badge { display: inline-flex; align-items: center; min-height: 24px; padding: 2px 8px; border-radius: 999px; font-weight: 700; font-size: 12px; background: #e5e7eb; color: #374151; }
    .badge.ok { background: #d1fae5; color: var(--ok); }
    .badge.warn { background: #fef3c7; color: var(--warn); }
    .badge.bad { background: #fee2e2; color: var(--bad); }
    .muted { color: var(--muted); }
    @media (max-width: 1050px) {
      .kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .two { grid-template-columns: 1fr; }
    }
    @media (max-width: 680px) {
      header { padding: 18px; display: block; }
      main { padding: 16px; }
      .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      th:nth-child(4), td:nth-child(4) { display: none; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>SF Integration SaaS Control Center</h1>
      <p>Zentrale Projekt-, Agenten- und Lizenzuebersicht</p>
    </div>
    <div>Stand: ${esc(formatDate(snapshot.generatedAt))}</div>
  </header>
  <main class="grid">
    <div class="grid kpis">
      ${renderKpi("Tenants", snapshot.totals.tenants, "aktive Mandanten")}
      ${renderKpi("Projekte", snapshot.totals.projects, "alle Modi")}
      ${renderKpi("Agenten", snapshot.totals.agents, "registriert")}
      ${renderKpi("Online", snapshot.totals.onlineAgents, "Heartbeat < 15 min")}
      ${renderKpi("Runs 24h", snapshot.totals.schedulerRuns24h, "Scheduler")}
      ${renderKpi("Fehlerquote", failedRate, "Runs 24h")}
    </div>

    <div class="grid two">
      <section class="panel">
        <h2>SaaS Admin Ansicht</h2>
        <table>
          <thead><tr><th>Tenant</th><th>Plan</th><th>Status</th><th>Projekte</th><th>Agenten</th><th>Letzter Heartbeat</th></tr></thead>
          <tbody>
            ${snapshot.tenants.map((tenant) => `
              <tr>
                <td><strong>${esc(tenant.name)}</strong><br><span class="muted">${esc(tenant.tenantKey)}</span></td>
                <td>${esc(tenant.plan)}</td>
                <td><span class="badge ${badgeClass(tenant.status)}">${esc(tenant.status)}</span></td>
                <td>${esc(tenant.projectCount)}</td>
                <td>${esc(tenant.onlineAgentCount)} / ${esc(tenant.agentCount)}</td>
                <td>${esc(formatDate(tenant.lastHeartbeatAt))}</td>
              </tr>
            `).join("") || `<tr><td colspan="6" class="muted">Keine Tenants vorhanden.</td></tr>`}
          </tbody>
        </table>
      </section>

      <section class="panel">
        <h2>Kundenansicht Projekte</h2>
        <table>
          <thead><tr><th>Projekt</th><th>Tenant</th><th>Modus</th><th>Status</th><th>Agenten</th><th>Letzter Heartbeat</th></tr></thead>
          <tbody>
            ${snapshot.projects.map((project) => `
              <tr>
                <td><strong>${esc(project.name)}</strong><br><span class="muted">${esc(project.projectKey)}</span></td>
                <td>${esc(project.tenantKey)}</td>
                <td><span class="badge ${badgeClass(project.mode)}">${esc(project.mode)}</span></td>
                <td><span class="badge ${badgeClass(project.status)}">${esc(project.status)}</span></td>
                <td>${esc(project.onlineAgentCount)} / ${esc(project.agentCount)}</td>
                <td>${esc(formatDate(project.lastHeartbeatAt))}</td>
              </tr>
            `).join("") || `<tr><td colspan="6" class="muted">Keine Projekte vorhanden.</td></tr>`}
          </tbody>
        </table>
      </section>
    </div>

    <section class="panel">
      <h2>Registrierte Agenten</h2>
      <table>
        <thead><tr><th>Agent</th><th>Tenant</th><th>Projekt</th><th>Status</th><th>Version</th><th>Letzter Heartbeat</th></tr></thead>
        <tbody>
          ${snapshot.recentAgents.map((agent) => `
            <tr>
              <td><strong>${esc(agent.name)}</strong><br><span class="muted">${esc(agent.agentId)}</span></td>
              <td>${esc(agent.tenantKey)}</td>
              <td>${esc(agent.projectKey)}</td>
              <td><span class="badge ${badgeClass(agent.status)}">${esc(agent.status)}</span></td>
              <td>${esc(agent.agentVersion || "-")}</td>
              <td>${esc(formatDate(agent.lastHeartbeatAt))}</td>
            </tr>
          `).join("") || `<tr><td colspan="6" class="muted">Keine Agenten vorhanden.</td></tr>`}
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h2>Scheduler Runs (letzte 50)</h2>
      <table>
        <thead><tr><th>Scheduler</th><th>Projekt</th><th>Richtung</th><th>Status</th><th>Gelesen</th><th>Geschrieben</th><th>Fehler</th><th>Gestartet</th></tr></thead>
        <tbody>
          ${snapshot.recentRuns.map((run) => `
            <tr>
              <td>${esc(run.schedulerKey)}</td>
              <td><span class="muted">${esc(run.tenantKey)}</span> / ${esc(run.projectKey)}</td>
              <td>${esc(run.direction)}</td>
              <td><span class="badge ${badgeClass(run.status)}">${esc(run.status)}</span></td>
              <td>${esc(run.readRecords)}</td>
              <td>${esc(run.writtenRecords)}</td>
              <td>${run.failedRecords > 0 ? `<span class="badge bad">${esc(run.failedRecords)}</span>` : esc(run.failedRecords)}</td>
              <td>${esc(formatDate(run.startedAt))}</td>
            </tr>
          `).join("") || `<tr><td colspan="8" class="muted">Keine Runs vorhanden.</td></tr>`}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>`;
}
