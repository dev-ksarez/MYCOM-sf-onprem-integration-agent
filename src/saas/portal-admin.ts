import { PortalSession } from "./saas-auth";
import { TenantRecord, TenantDetails, SaasPortalSnapshot } from "./saas-repository";
import { layout, esc, formatDate, badge } from "./portal-layout";

export function renderAdminDashboard(snapshot: SaasPortalSnapshot, session: PortalSession): string {
  const t = snapshot.totals;
  const kpis = `
<div class="grid kpis">
  <div class="kpi"><span>Mandanten</span><strong>${t.tenants}</strong></div>
  <div class="kpi"><span>Projekte</span><strong>${t.projects}</strong></div>
  <div class="kpi"><span>Agenten</span><strong>${t.agents}</strong><small>${t.onlineAgents} online</small></div>
  <div class="kpi"><span>Runs (24h)</span><strong>${t.schedulerRuns24h}</strong><small>${t.failedRuns24h} fehlgeschlagen</small></div>
</div>`;

  const tenantsTable = `
<div class="panel" style="margin-top:20px">
  <h2>Mandanten <a href="/portal/admin/tenants/new">+ Neu</a></h2>
  <table>
    <thead><tr>
      <th>Key</th><th>Name</th><th>Status</th><th>Projekte</th>
      <th>Agenten</th><th>Erstellt</th><th></th>
    </tr></thead>
    <tbody>
      ${snapshot.tenants.map((tn) => `<tr>
        <td><code>${esc(tn.tenantKey)}</code></td>
        <td>${esc(tn.name)}</td>
        <td>${badge(tn.status)}</td>
        <td>${tn.projectCount}</td>
        <td>${tn.onlineAgentCount}/${tn.agentCount}</td>
        <td class="muted">${formatDate(tn.lastHeartbeatAt)}</td>
        <td><a class="btn btn-sm btn-primary" href="/portal/admin/tenants/${esc(tn.tenantId)}">Details</a></td>
      </tr>`).join("")}
    </tbody>
  </table>
</div>`;

  const runsTable = snapshot.recentRuns.length === 0 ? "" : `
<div class="panel" style="margin-top:20px">
  <h2>Letzte Runs</h2>
  <table>
    <thead><tr>
      <th>Mandant</th><th>Projekt</th><th>Scheduler</th><th>Richtung</th>
      <th>Status</th><th>Lesen</th><th>Schreiben</th><th>Fehler</th><th>Gestartet</th>
    </tr></thead>
    <tbody>
      ${snapshot.recentRuns.map((r) => `<tr>
        <td class="muted">${esc(r.tenantKey)}</td>
        <td class="muted">${esc(r.projectKey)}</td>
        <td><code>${esc(r.schedulerKey)}</code></td>
        <td>${esc(r.direction)}</td>
        <td>${badge(r.status)}</td>
        <td>${r.readRecords}</td>
        <td>${r.writtenRecords}</td>
        <td>${r.failedRecords}</td>
        <td class="muted">${formatDate(r.startedAt)}</td>
      </tr>`).join("")}
    </tbody>
  </table>
</div>`;

  return layout({ title: "Admin Dashboard", session, current: "/portal/admin", content: kpis + tenantsTable + runsTable });
}

export function renderAdminTenants(tenants: TenantRecord[], session: PortalSession, flash?: { type: "ok" | "err"; msg: string }): string {
  const list = `
<div class="panel">
  <h2>Alle Mandanten</h2>
  <table>
    <thead><tr>
      <th>Key</th><th>Name</th><th>Status</th><th>Erstellt</th><th></th>
    </tr></thead>
    <tbody>
      ${tenants.map((tn) => `<tr>
        <td><code>${esc(tn.tenantKey)}</code></td>
        <td>${esc(tn.name)}</td>
        <td>${badge(tn.status)}</td>
        <td class="muted">${formatDate(tn.createdAt)}</td>
        <td><a class="btn btn-sm btn-primary" href="/portal/admin/tenants/${esc(tn.id)}">Details</a></td>
      </tr>`).join("")}
    </tbody>
  </table>
</div>
<div class="panel" style="margin-top:20px;padding:18px 20px">
  <h2 style="border:0;padding:0 0 12px">Neuen Mandanten anlegen</h2>
  <form method="POST" action="/portal/admin/tenants">
    <div class="form-row"><label>Mandant-Key (z.B. acme)</label>
      <input name="tenantKey" required pattern="[a-z0-9_-]+" title="Kleinbuchstaben, Zahlen, - oder _"/></div>
    <div class="form-row"><label>Name</label><input name="name" required/></div>
    <button type="submit" class="btn btn-primary">Anlegen</button>
  </form>
</div>`;

  return layout({ title: "Mandanten", session, current: "/portal/admin/tenants", content: list, flash });
}

export function renderAdminTenantDetail(details: TenantDetails, session: PortalSession, flash?: { type: "ok" | "err"; msg: string }): string {
  const lic = details.license;
  const licPanel = `
<div class="panel">
  <h2>Lizenz</h2>
  <div style="padding:14px 18px">
    ${lic ? `
      <p>${badge(lic.status)} ${esc(lic.plan)}
        ${lic.contractReference ? ` · Ref: ${esc(lic.contractReference)}` : ""}
      </p>
      <p class="muted">Gültig: ${formatDate(lic.validFrom)} – ${formatDate(lic.validUntil)}</p>
      <p class="muted">Connectors: ${lic.maxConnectors} · Scheduler: ${lic.maxSchedulers} · Records/Monat: ${lic.maxRecordsPerMonth.toLocaleString("de-DE")}</p>
    ` : "<p class=\"muted\">Keine aktive Lizenz</p>"}
    <form method="POST" action="/portal/admin/tenants/${esc(details.id)}/license" style="margin-top:12px">
      <div class="grid two" style="gap:10px">
        <div class="form-row"><label>Plan</label>
          <input name="plan" value="${esc(lic?.plan || "pilot")}" required/></div>
        <div class="form-row"><label>Status</label>
          <select name="status">
            ${["trial","active","expired","suspended"].map((s) =>
              `<option value="${s}"${lic?.status === s ? " selected" : ""}>${s}</option>`
            ).join("")}
          </select>
        </div>
        <div class="form-row"><label>Gültig von</label>
          <input type="date" name="validFrom" value="${lic?.validFrom ? lic.validFrom.slice(0,10) : ""}"/></div>
        <div class="form-row"><label>Gültig bis</label>
          <input type="date" name="validUntil" value="${lic?.validUntil ? lic.validUntil.slice(0,10) : ""}"/></div>
        <div class="form-row"><label>Max. Connectors</label>
          <input type="number" name="maxConnectors" value="${lic?.maxConnectors ?? 5}" required/></div>
        <div class="form-row"><label>Max. Scheduler</label>
          <input type="number" name="maxSchedulers" value="${lic?.maxSchedulers ?? 10}" required/></div>
        <div class="form-row"><label>Max. Records/Monat</label>
          <input type="number" name="maxRecordsPerMonth" value="${lic?.maxRecordsPerMonth ?? 10000}" required/></div>
        <div class="form-row"><label>Vertragsreferenz</label>
          <input name="contractReference" value="${esc(lic?.contractReference || "")}"/></div>
      </div>
      <div style="display:flex;gap:8px;margin-top:4px">
        <label><input type="checkbox" name="featureAi"${lic?.featureAi ? " checked" : ""}> KI</label>
        <label><input type="checkbox" name="featureMigration"${lic?.featureMigration ? " checked" : ""}> Migration</label>
        <label><input type="checkbox" name="featureCustomConnector"${lic?.featureCustomConnector ? " checked" : ""}> Custom Connector</label>
        <label><input type="checkbox" name="featureCustomScheduler"${lic?.featureCustomScheduler ? " checked" : ""}> Custom Scheduler</label>
      </div>
      <button type="submit" class="btn btn-primary" style="margin-top:12px">Lizenz speichern</button>
    </form>
  </div>
</div>`;

  const membersPanel = `
<div class="panel">
  <h2>Mitglieder</h2>
  <table>
    <thead><tr><th>E-Mail</th><th>Name</th><th>Rolle</th></tr></thead>
    <tbody>
      ${details.members.length === 0
        ? `<tr><td colspan="3" class="muted">Keine Mitglieder</td></tr>`
        : details.members.map((m) => `<tr>
            <td>${esc(m.email)}</td>
            <td>${esc(m.displayName)}</td>
            <td><code>${esc(m.role)}</code></td>
          </tr>`).join("")}
    </tbody>
  </table>
  <div style="padding:14px 18px;border-top:1px solid var(--line)">
    <form method="POST" action="/portal/admin/tenants/${esc(details.id)}/members" style="display:flex;gap:8px;align-items:flex-end">
      <div class="form-row" style="flex:1;margin:0"><label>E-Mail</label><input name="email" type="email" required/></div>
      <div class="form-row" style="margin:0"><label>Rolle</label>
        <select name="role">
          <option value="owner">owner</option>
          <option value="admin">admin</option>
          <option value="operator">operator</option>
          <option value="viewer">viewer</option>
        </select>
      </div>
      <button type="submit" class="btn btn-primary">Hinzufügen</button>
    </form>
  </div>
</div>`;

  const projectsPanel = `
<div class="panel">
  <h2>Projekte</h2>
  <table>
    <thead><tr><th>Key</th><th>Name</th><th>Modus</th><th>Status</th></tr></thead>
    <tbody>
      ${details.projects.length === 0
        ? `<tr><td colspan="4" class="muted">Keine Projekte</td></tr>`
        : details.projects.map((p) => `<tr>
            <td><code>${esc(p.projectKey)}</code></td>
            <td>${esc(p.name)}</td>
            <td>${esc(p.mode)}</td>
            <td>${badge(p.status)}</td>
          </tr>`).join("")}
    </tbody>
  </table>
  <div style="padding:14px 18px;border-top:1px solid var(--line)">
    <form method="POST" action="/portal/admin/tenants/${esc(details.id)}/projects" style="display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap">
      <div class="form-row" style="flex:1;margin:0"><label>Projekt-Key</label><input name="projectKey" required pattern="[a-z0-9_-]+"/></div>
      <div class="form-row" style="flex:1;margin:0"><label>Name</label><input name="name" required/></div>
      <div class="form-row" style="margin:0"><label>Modus</label>
        <select name="mode"><option value="hybrid">hybrid</option><option value="saas">saas</option><option value="legacy">legacy</option></select>
      </div>
      <div class="form-row" style="margin:0"><label>Zeitzone</label><input name="timezone" value="Europe/Berlin"/></div>
      <button type="submit" class="btn btn-primary">Anlegen</button>
    </form>
  </div>
</div>`;

  const statusPanel = `
<div class="panel">
  <h2>Mandant-Status</h2>
  <div style="padding:14px 18px;display:flex;gap:8px">
    <form method="POST" action="/portal/admin/tenants/${esc(details.id)}/status" class="inline">
      <input type="hidden" name="status" value="active"/>
      <button class="btn btn-primary btn-sm">Aktivieren</button>
    </form>
    <form method="POST" action="/portal/admin/tenants/${esc(details.id)}/status" class="inline">
      <input type="hidden" name="status" value="suspended"/>
      <button class="btn btn-danger btn-sm">Sperren</button>
    </form>
  </div>
</div>`;

  const content = `
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
  <div>
    <div style="font-size:18px;font-weight:700">${esc(details.name)}</div>
    <div class="muted" style="font-size:13px"><code>${esc(details.tenantKey)}</code> · ${badge(details.status)} · ${details.agentCount} Agenten (${details.onlineAgentCount} online)</div>
  </div>
</div>
<div class="grid two">
  ${licPanel}
  <div style="display:flex;flex-direction:column;gap:16px">
    ${membersPanel}
    ${statusPanel}
  </div>
</div>
<div style="margin-top:16px">${projectsPanel}</div>`;

  return layout({ title: `Mandant: ${details.name}`, session, current: "/portal/admin/tenants", content, flash });
}

export function renderAdminUsers(users: Array<{ id: string; email: string; displayName: string; isSystemAdmin: boolean; status: string }>, session: PortalSession, flash?: { type: "ok" | "err"; msg: string }): string {
  const content = `
<div class="panel">
  <h2>Benutzer</h2>
  <table>
    <thead><tr><th>E-Mail</th><th>Name</th><th>System-Admin</th><th>Status</th></tr></thead>
    <tbody>
      ${users.map((u) => `<tr>
        <td>${esc(u.email)}</td>
        <td>${esc(u.displayName)}</td>
        <td>${u.isSystemAdmin ? badge("admin") : ""}</td>
        <td>${badge(u.status)}</td>
      </tr>`).join("")}
    </tbody>
  </table>
</div>
<div class="panel" style="margin-top:20px;padding:18px 20px">
  <h2 style="border:0;padding:0 0 12px">Neuen Benutzer anlegen</h2>
  <form method="POST" action="/portal/admin/users">
    <div class="grid two">
      <div class="form-row"><label>E-Mail</label><input type="email" name="email" required/></div>
      <div class="form-row"><label>Anzeigename</label><input name="displayName" required/></div>
      <div class="form-row"><label>Passwort</label><input type="password" name="password" required minlength="8"/></div>
      <div class="form-row"><label>Mandant-Key (optional)</label><input name="tenantKey" placeholder="z.B. acme"/></div>
      <div class="form-row"><label>Rolle</label>
        <select name="role">
          <option value="owner">owner</option>
          <option value="admin">admin</option>
          <option value="operator">operator</option>
          <option value="viewer">viewer</option>
        </select>
      </div>
    </div>
    <button type="submit" class="btn btn-primary">Anlegen</button>
  </form>
</div>`;

  return layout({ title: "Benutzer", session, current: "/portal/admin/users", content, flash });
}
