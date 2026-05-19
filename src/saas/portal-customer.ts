import { PortalSession } from "./saas-auth";
import { CustomerDashboard, RegistrationTokenInfo } from "./saas-repository";
import { layout, esc, formatDate, badge } from "./portal-layout";

export function renderCustomerDashboard(data: CustomerDashboard, session: PortalSession): string {
  const s = data.stats;
  const lic = data.license;
  const content = `
<div class="grid kpis">
  <div class="kpi"><span>Agenten</span><strong>${s.agentCount}</strong><small>${s.onlineAgentCount} online</small></div>
  <div class="kpi"><span>Runs (24h)</span><strong>${s.runs24h}</strong><small>${s.failed24h} fehlgeschlagen</small></div>
  <div class="kpi"><span>Lizenz</span><strong>${esc(lic?.plan || "–")}</strong><small>${lic ? badge(lic.status) : ""}</small></div>
  ${lic?.validUntil ? `<div class="kpi"><span>Gültig bis</span><strong style="font-size:16px">${formatDate(lic.validUntil)}</strong></div>` : ""}
</div>

<div class="grid two" style="margin-top:20px">
  <div class="panel">
    <h2>Agenten <a href="/portal/customer/agents">Alle</a></h2>
    <table>
      <thead><tr><th>Name</th><th>Projekt</th><th>Status</th><th>Letzter Heartbeat</th></tr></thead>
      <tbody>
        ${data.agents.slice(0, 10).map((a) => `<tr>
          <td>${esc(a.name)}</td>
          <td class="muted"><code>${esc(a.projectKey)}</code></td>
          <td>${badge(a.status)}</td>
          <td class="muted">${formatDate(a.lastHeartbeatAt)}</td>
        </tr>`).join("")}
        ${data.agents.length === 0 ? `<tr><td colspan="4" class="muted">Keine Agenten registriert</td></tr>` : ""}
      </tbody>
    </table>
  </div>

  <div class="panel">
    <h2>Letzte Runs <a href="/portal/customer/runs">Alle</a></h2>
    <table>
      <thead><tr><th>Scheduler</th><th>Status</th><th>Gestartet</th></tr></thead>
      <tbody>
        ${data.recentRuns.slice(0, 10).map((r) => `<tr>
          <td><code>${esc(r.schedulerKey)}</code></td>
          <td>${badge(r.status)}</td>
          <td class="muted">${formatDate(r.startedAt)}</td>
        </tr>`).join("")}
        ${data.recentRuns.length === 0 ? `<tr><td colspan="3" class="muted">Keine Runs</td></tr>` : ""}
      </tbody>
    </table>
  </div>
</div>`;

  return layout({ title: "Dashboard", session, current: "/portal/customer", content });
}

export function renderCustomerAgents(agents: CustomerDashboard["agents"], session: PortalSession): string {
  const content = `
<div class="panel">
  <h2>Agenten</h2>
  <table>
    <thead><tr>
      <th>Name</th><th>Projekt</th><th>Modus</th><th>Status</th><th>Version</th><th>Letzter Heartbeat</th>
    </tr></thead>
    <tbody>
      ${agents.map((a) => `<tr>
        <td>${esc(a.name)}</td>
        <td><code>${esc(a.projectKey)}</code></td>
        <td>${esc(a.mode)}</td>
        <td>${badge(a.status)}</td>
        <td class="muted">${esc(a.agentVersion || "–")}</td>
        <td class="muted">${formatDate(a.lastHeartbeatAt)}</td>
      </tr>`).join("")}
      ${agents.length === 0 ? `<tr><td colspan="6" class="muted">Keine Agenten registriert.<br>Erstellen Sie einen Registration Token unter <a href="/portal/customer/tokens">Registration Tokens</a>.</td></tr>` : ""}
    </tbody>
  </table>
</div>`;

  return layout({ title: "Agenten", session, current: "/portal/customer/agents", content });
}

export function renderCustomerRuns(runs: CustomerDashboard["recentRuns"], session: PortalSession): string {
  const content = `
<div class="panel">
  <h2>Runs</h2>
  <table>
    <thead><tr>
      <th>Scheduler</th><th>Projekt</th><th>Richtung</th><th>Status</th>
      <th>Gelesen</th><th>Geschrieben</th><th>Fehler</th><th>Gestartet</th><th>Beendet</th>
    </tr></thead>
    <tbody>
      ${runs.map((r) => `<tr>
        <td><code>${esc(r.schedulerKey)}</code></td>
        <td class="muted"><code>${esc(r.projectKey)}</code></td>
        <td>${esc(r.direction)}</td>
        <td>${badge(r.status)}</td>
        <td>${r.readRecords}</td>
        <td>${r.writtenRecords}</td>
        <td>${r.failedRecords > 0 ? `<span style="color:var(--bad)">${r.failedRecords}</span>` : "0"}</td>
        <td class="muted">${formatDate(r.startedAt)}</td>
        <td class="muted">${formatDate(r.finishedAt)}</td>
      </tr>`).join("")}
      ${runs.length === 0 ? `<tr><td colspan="9" class="muted">Keine Runs vorhanden</td></tr>` : ""}
    </tbody>
  </table>
</div>`;

  return layout({ title: "Runs", session, current: "/portal/customer/runs", content });
}

export function renderCustomerContract(dashboard: CustomerDashboard, session: PortalSession): string {
  const lic = dashboard.license;
  const content = lic ? `
<div class="panel">
  <h2>Lizenz & Vertrag</h2>
  <div style="padding:18px 20px">
    <div class="grid two" style="gap:16px">
      <div>
        <div class="form-row"><label>Status</label><div>${badge(lic.status)}</div></div>
        <div class="form-row"><label>Plan</label><div>${esc(lic.plan)}</div></div>
        <div class="form-row"><label>Vertragsreferenz</label><div>${esc(lic.contractReference || "–")}</div></div>
        <div class="form-row"><label>Gültig von</label><div>${formatDate(lic.validFrom)}</div></div>
        <div class="form-row"><label>Gültig bis</label><div>${formatDate(lic.validUntil)}</div></div>
      </div>
      <div>
        <div class="form-row"><label>Max. Connectors</label><div>${lic.maxConnectors}</div></div>
        <div class="form-row"><label>Max. Scheduler</label><div>${lic.maxSchedulers}</div></div>
        <div class="form-row"><label>Max. Records/Monat</label><div>${lic.maxRecordsPerMonth.toLocaleString("de-DE")}</div></div>
        <div class="form-row"><label>Features</label>
          <div style="display:flex;flex-wrap:wrap;gap:4px">
            ${lic.featureAi ? badge("KI") : ""}
            ${lic.featureMigration ? badge("Migration") : ""}
            ${lic.featureCustomConnector ? badge("Custom Connector") : ""}
            ${lic.featureCustomScheduler ? badge("Custom Scheduler") : ""}
          </div>
        </div>
      </div>
    </div>
  </div>
</div>` : `
<div class="panel">
  <h2>Lizenz & Vertrag</h2>
  <div style="padding:24px 20px;text-align:center;color:var(--muted)">
    Keine aktive Lizenz. Bitte wenden Sie sich an den Support.
  </div>
</div>`;

  return layout({ title: "Vertrag & Lizenz", session, current: "/portal/customer/contract", content });
}

export function renderCustomerTokens(
  tokens: RegistrationTokenInfo[],
  projects: Array<{ id: string; projectKey: string; name: string }>,
  session: PortalSession,
  flash?: { type: "ok" | "err"; msg: string },
  newToken?: string
): string {
  const newTokenBox = newToken ? `
<div class="panel" style="margin-bottom:20px">
  <h2>Neuer Registration Token</h2>
  <div style="padding:14px 18px">
    <p>Bitte kopieren Sie diesen Token jetzt – er wird <strong>nicht erneut angezeigt</strong>.</p>
    <div class="token-box">${esc(newToken)}</div>
  </div>
</div>` : "";

  const createForm = projects.length > 0 ? `
<div class="panel" style="margin-top:20px;padding:18px 20px">
  <h2 style="border:0;padding:0 0 12px">Neuen Token erstellen</h2>
  <form method="POST" action="/portal/customer/tokens" style="display:flex;gap:8px;align-items:flex-end">
    <div class="form-row" style="flex:1;margin:0"><label>Projekt</label>
      <select name="projectId">
        ${projects.map((p) => `<option value="${esc(p.id)}">${esc(p.projectKey)} – ${esc(p.name)}</option>`).join("")}
      </select>
    </div>
    <button type="submit" class="btn btn-primary">Token erstellen</button>
  </form>
</div>` : `<p class="muted" style="margin-top:16px">Keine Projekte vorhanden. Bitte wenden Sie sich an den Administrator.</p>`;

  const list = `
<div class="panel">
  <h2>Registration Tokens</h2>
  <table>
    <thead><tr>
      <th>Projekt</th><th>Status</th><th>Ablaufdatum</th><th>Erstellt</th>
    </tr></thead>
    <tbody>
      ${tokens.map((t) => `<tr>
        <td><code>${esc(t.projectKey)}</code></td>
        <td>${badge(t.status)}</td>
        <td class="muted">${formatDate(t.expiresAt)}</td>
        <td class="muted">${formatDate(t.createdAt)}</td>
      </tr>`).join("")}
      ${tokens.length === 0 ? `<tr><td colspan="4" class="muted">Noch keine Tokens erstellt</td></tr>` : ""}
    </tbody>
  </table>
</div>`;

  return layout({ title: "Registration Tokens", session, current: "/portal/customer/tokens", content: newTokenBox + list + createForm, flash });
}
