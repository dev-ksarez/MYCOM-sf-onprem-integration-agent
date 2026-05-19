import { PortalSession } from "./saas-auth";
import { CustomerDashboard, RegistrationTokenInfo, RunDetail, ConnectorConfigRecord, SchedulerConfigRecord } from "./saas-repository";
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
        <td><a href="/portal/customer/runs/${esc(r.runId)}"><code>${esc(r.schedulerKey)}</code></a></td>
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

export function renderCustomerRunDetail(detail: RunDetail, session: PortalSession): string {
  const r = detail.run;
  const levelIcon = (l: string) => l === "error" ? "🔴" : l === "warn" ? "🟡" : "⚪";

  const header = `
<div style="margin-bottom:16px">
  <a href="/portal/customer/runs" style="font-size:13px;color:var(--muted);text-decoration:none">← Alle Runs</a>
  <div style="margin-top:8px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
    <span style="font-size:17px;font-weight:700"><code>${esc(r.schedulerKey)}</code></span>
    ${badge(r.status)}
    <span class="muted">${esc(r.direction)} · ${esc(r.projectKey)}</span>
  </div>
  <div class="muted" style="font-size:12px;margin-top:4px">
    ${formatDate(r.startedAt)} – ${formatDate(r.finishedAt)}
    &nbsp;·&nbsp; Gelesen: ${r.readRecords} · Geschrieben: ${r.writtenRecords} · Fehler: ${r.failedRecords}
    ${r.errorCategory ? `&nbsp;·&nbsp; <span style="color:var(--bad)">${esc(r.errorCategory)}: ${esc(r.errorMessage || "")}</span>` : ""}
  </div>
</div>`;

  const logsPanel = `
<div class="panel">
  <h2>Log-Ereignisse (${detail.logs.length})</h2>
  ${detail.logs.length === 0
    ? `<div style="padding:16px 18px" class="muted">Keine Log-Einträge vorhanden</div>`
    : `<div style="font-family:monospace;font-size:12px;overflow-x:auto">
      <table>
        <thead><tr><th>Zeit</th><th>Level</th><th>Code</th><th>Nachricht</th></tr></thead>
        <tbody>
          ${detail.logs.map((l) => `<tr style="${l.level === "error" ? "background:#fff5f5" : l.level === "warn" ? "background:#fffbeb" : ""}">
            <td style="white-space:nowrap" class="muted">${formatDate(l.occurredAt)}</td>
            <td>${levelIcon(l.level)} ${esc(l.level)}</td>
            <td class="muted">${esc(l.code || "")}</td>
            <td style="word-break:break-word;max-width:600px">${esc(l.message)}</td>
          </tr>`).join("")}
        </tbody>
      </table>
    </div>`}
</div>`;

  const failedPanel = detail.failedRecords.length === 0 ? "" : `
<div class="panel" style="margin-top:16px">
  <h2>Fehlgeschlagene Datensätze (${detail.failedRecords.length})</h2>
  <table>
    <thead><tr><th>Record-Key</th><th>Status</th><th>Fehlercode</th><th>Nachricht</th><th>Zeit</th></tr></thead>
    <tbody>
      ${detail.failedRecords.map((f) => `<tr>
        <td><code>${esc(f.recordKey || "–")}</code></td>
        <td class="muted">${esc(f.status || "–")}</td>
        <td class="muted">${esc(f.errorCode || "–")}</td>
        <td style="word-break:break-word;max-width:400px">${esc(f.message || "")}</td>
        <td class="muted">${formatDate(f.createdAt)}</td>
      </tr>`).join("")}
    </tbody>
  </table>
</div>`;

  return layout({ title: `Run: ${r.schedulerKey}`, session, current: "/portal/customer/runs", content: header + logsPanel + failedPanel });
}

export function renderCustomerConnectors(
  connectors: ConnectorConfigRecord[],
  projects: Array<{ id: string; projectKey: string; name: string }>,
  session: PortalSession,
  flash?: { type: "ok" | "err"; msg: string }
): string {
  const list = `
<div class="panel">
  <h2>Connectors</h2>
  <table>
    <thead><tr><th>Key</th><th>Typ</th><th>Name</th><th>Projekt</th><th>Secret-Policy</th><th>Status</th><th></th></tr></thead>
    <tbody>
      ${connectors.map((c) => `<tr>
        <td><code>${esc(c.connectorKey)}</code></td>
        <td class="muted">${esc(c.type)}</td>
        <td>${esc(c.displayName)}</td>
        <td class="muted"><code>${esc(c.projectKey)}</code></td>
        <td class="muted">${esc(c.secretPolicy)}</td>
        <td>${badge(c.status)}</td>
        <td>
          <form method="POST" action="/portal/customer/connectors/${esc(c.id)}/delete" class="inline">
            <button class="btn btn-danger btn-sm" onclick="return confirm('Connector löschen?')">Löschen</button>
          </form>
        </td>
      </tr>`).join("")}
      ${connectors.length === 0 ? `<tr><td colspan="7" class="muted">Noch keine Connectors angelegt</td></tr>` : ""}
    </tbody>
  </table>
</div>`;

  const form = projects.length === 0 ? "" : `
<div class="panel" style="margin-top:20px;padding:18px 20px">
  <h2 style="border:0;padding:0 0 12px">Neuen Connector anlegen</h2>
  <form method="POST" action="/portal/customer/connectors">
    <div class="grid two" style="gap:10px">
      <div class="form-row"><label>Projekt</label>
        <select name="projectId">
          ${projects.map((p) => `<option value="${esc(p.id)}">${esc(p.projectKey)} – ${esc(p.name)}</option>`).join("")}
        </select>
      </div>
      <div class="form-row"><label>Connector-Key (z.B. sage100-prod)</label>
        <input name="connectorKey" required pattern="[a-z0-9_-]+" placeholder="sage100-prod"/></div>
      <div class="form-row"><label>Typ</label>
        <select name="type">
          <option value="salesforce">Salesforce</option>
          <option value="sage100">Sage 100</option>
          <option value="mssql">MS SQL Server</option>
          <option value="rest">REST API</option>
          <option value="file">Datei/SFTP</option>
          <option value="other">Sonstige</option>
        </select>
      </div>
      <div class="form-row"><label>Anzeigename</label>
        <input name="displayName" required placeholder="z.B. Sage 100 Produktion"/></div>
      <div class="form-row" style="grid-column:1/-1"><label>Konfiguration (JSON – keine Passwörter hier eintragen)</label>
        <textarea name="metadataJson" rows="4" style="font-family:monospace;font-size:12px">{}</textarea>
      </div>
      <div class="form-row"><label>Secret-Policy</label>
        <select name="secretPolicy">
          <option value="local-only">local-only (Credentials nur lokal)</option>
          <option value="available-local">available-local</option>
        </select>
      </div>
    </div>
    <button type="submit" class="btn btn-primary" style="margin-top:8px">Anlegen</button>
  </form>
</div>`;

  return layout({ title: "Connectors", session, current: "/portal/customer/connectors", content: list + form, flash });
}

export function renderCustomerSchedulers(
  schedulers: SchedulerConfigRecord[],
  connectors: ConnectorConfigRecord[],
  projects: Array<{ id: string; projectKey: string; name: string }>,
  session: PortalSession,
  flash?: { type: "ok" | "err"; msg: string }
): string {
  const list = `
<div class="panel">
  <h2>Scheduler</h2>
  <table>
    <thead><tr><th>Key</th><th>Name</th><th>Projekt</th><th>Richtung</th><th>Aktiv</th><th>Intervall</th><th>Connector</th><th></th></tr></thead>
    <tbody>
      ${schedulers.map((s) => `<tr>
        <td><code>${esc(s.schedulerKey)}</code></td>
        <td>${esc(s.name)}</td>
        <td class="muted"><code>${esc(s.projectKey)}</code></td>
        <td>${esc(s.direction)}</td>
        <td>${s.active ? badge("active") : badge("paused")}</td>
        <td class="muted">${esc(s.scheduleExpression || "–")}</td>
        <td class="muted">${esc(s.connectorKey || "–")}</td>
        <td>
          <form method="POST" action="/portal/customer/schedulers/${esc(s.id)}/delete" class="inline">
            <button class="btn btn-danger btn-sm" onclick="return confirm('Scheduler löschen?')">Löschen</button>
          </form>
        </td>
      </tr>`).join("")}
      ${schedulers.length === 0 ? `<tr><td colspan="8" class="muted">Noch keine Scheduler angelegt</td></tr>` : ""}
    </tbody>
  </table>
</div>`;

  const form = projects.length === 0 ? "" : `
<div class="panel" style="margin-top:20px;padding:18px 20px">
  <h2 style="border:0;padding:0 0 12px">Neuen Scheduler anlegen</h2>
  <form method="POST" action="/portal/customer/schedulers">
    <div class="grid two" style="gap:10px">
      <div class="form-row"><label>Projekt</label>
        <select name="projectId">
          ${projects.map((p) => `<option value="${esc(p.id)}">${esc(p.projectKey)} – ${esc(p.name)}</option>`).join("")}
        </select>
      </div>
      <div class="form-row"><label>Scheduler-Key (z.B. accounts-outbound)</label>
        <input name="schedulerKey" required pattern="[a-z0-9_-]+" placeholder="accounts-outbound"/></div>
      <div class="form-row"><label>Name</label>
        <input name="name" required placeholder="z.B. Konten → Sage 100"/></div>
      <div class="form-row"><label>Richtung</label>
        <select name="direction">
          <option value="outbound">outbound (SF → Ziel)</option>
          <option value="inbound">inbound (Quelle → SF)</option>
        </select>
      </div>
      <div class="form-row"><label>Intervall (Cron, z.B. */30 * * * *)</label>
        <input name="scheduleExpression" placeholder="*/30 * * * *"/></div>
      <div class="form-row"><label>Connector</label>
        <select name="connectorKey">
          <option value="">– keiner –</option>
          ${connectors.map((c) => `<option value="${esc(c.connectorKey)}">${esc(c.displayName)} (${esc(c.connectorKey)})</option>`).join("")}
        </select>
      </div>
      <div class="form-row"><label>Objekt / Entität</label>
        <input name="objectName" placeholder="z.B. Account"/></div>
      <div class="form-row"><label>Aktiv</label>
        <select name="active">
          <option value="true">Ja</option>
          <option value="false">Nein</option>
        </select>
      </div>
    </div>
    <button type="submit" class="btn btn-primary" style="margin-top:8px">Anlegen</button>
  </form>
</div>`;

  return layout({ title: "Scheduler", session, current: "/portal/customer/schedulers", content: list + form, flash });
}
