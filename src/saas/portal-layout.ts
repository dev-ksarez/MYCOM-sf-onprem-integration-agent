import { PortalSession } from "./saas-auth";

export function esc(v: unknown): string {
  return String(v ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

export function formatDate(v?: string | null): string {
  if (!v) return "-";
  const d = new Date(v);
  return isNaN(d.getTime()) ? "-" : d.toLocaleString("de-DE", { dateStyle: "short", timeStyle: "short" });
}

export function badge(status: string): string {
  const n = status.toLowerCase();
  const cls = (n === "online" || n === "active" || n === "success") ? "ok"
    : (n === "trial" || n === "hybrid" || n === "pending" || n === "partial" || n === "running") ? "warn"
    : "bad";
  return `<span class="badge ${cls}">${esc(status)}</span>`;
}

const CSS = `
:root{--sb:#1e2432;--sb-tx:#c9d1e3;--sb-ac:#4f86f7;--bg:#f4f7fb;--panel:#fff;
  --ink:#172033;--muted:#667085;--line:#d7dee8;--ok:#047857;--warn:#b45309;--bad:#b91c1c;}
*{box-sizing:border-box;}
body{margin:0;display:grid;grid-template-columns:220px 1fr;min-height:100vh;
  font:14px/1.5 system-ui,-apple-system,sans-serif;color:var(--ink);background:var(--bg);}
.sidebar{background:var(--sb);color:var(--sb-tx);display:flex;flex-direction:column;
  min-height:100vh;position:sticky;top:0;}
.sidebar .logo{padding:20px 18px 16px;font-size:15px;font-weight:700;color:#fff;
  border-bottom:1px solid rgba(255,255,255,.1);line-height:1.3;}
.sidebar .logo small{display:block;font-weight:400;font-size:11px;opacity:.6;margin-top:2px;}
.sidebar nav{flex:1;padding:10px 0;}
.sidebar nav a{display:flex;align-items:center;gap:8px;padding:9px 18px;color:var(--sb-tx);
  text-decoration:none;font-size:13px;border-left:3px solid transparent;}
.sidebar nav a:hover,.sidebar nav a.active{color:#fff;background:rgba(255,255,255,.07);
  border-left-color:var(--sb-ac);}
.sidebar .user-info{padding:14px 18px;border-top:1px solid rgba(255,255,255,.1);font-size:12px;}
.sidebar .user-info strong{display:block;color:#fff;margin-bottom:2px;}
.sidebar .user-info a{color:var(--sb-tx);text-decoration:none;font-size:11px;}
.main{display:flex;flex-direction:column;min-height:100vh;}
.topbar{background:var(--panel);border-bottom:1px solid var(--line);padding:14px 28px;
  display:flex;align-items:center;justify-content:space-between;}
.topbar h1{margin:0;font-size:20px;}
.content{padding:24px 28px;flex:1;}
.grid{display:grid;gap:16px;}
.kpis{grid-template-columns:repeat(auto-fill,minmax(160px,1fr));}
.two{grid-template-columns:1fr 1fr;align-items:start;}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;
  box-shadow:0 2px 8px rgba(15,23,42,.04);overflow:hidden;}
.panel h2{font-size:15px;margin:0;padding:14px 18px;border-bottom:1px solid var(--line);
  display:flex;align-items:center;justify-content:space-between;}
.panel h2 a,.panel h2 button{font-size:12px;font-weight:600;padding:4px 10px;border-radius:5px;
  background:var(--sb-ac);color:#fff;text-decoration:none;border:none;cursor:pointer;}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:8px;
  padding:14px 16px;min-height:96px;display:grid;gap:4px;align-content:center;}
.kpi span,.kpi small{color:var(--muted);}
.kpi strong{font-size:26px;}
table{width:100%;border-collapse:collapse;}
th,td{padding:9px 12px;text-align:left;border-bottom:1px solid #eef2f6;vertical-align:top;}
th{color:var(--muted);font-weight:700;font-size:11px;text-transform:uppercase;}
tr:last-child td{border-bottom:0;}
.badge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:999px;
  font-weight:700;font-size:11px;background:#e5e7eb;color:#374151;}
.badge.ok{background:#d1fae5;color:var(--ok);}
.badge.warn{background:#fef3c7;color:var(--warn);}
.badge.bad{background:#fee2e2;color:var(--bad);}
.muted{color:var(--muted);}
.btn{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border-radius:6px;
  font-size:13px;font-weight:600;cursor:pointer;border:none;text-decoration:none;}
.btn-primary{background:var(--sb-ac);color:#fff;}
.btn-danger{background:#ef4444;color:#fff;}
.btn-sm{padding:4px 10px;font-size:12px;}
form.inline{display:inline;}
.form-row{display:grid;gap:6px;margin-bottom:14px;}
.form-row label{font-size:12px;font-weight:600;color:var(--muted);}
.form-row input,.form-row select,.form-row textarea{
  width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:6px;font-size:13px;}
.alert{padding:12px 16px;border-radius:6px;margin-bottom:16px;font-size:13px;}
.alert-err{background:#fee2e2;color:var(--bad);}
.alert-ok{background:#d1fae5;color:var(--ok);}
.token-box{font-family:monospace;font-size:13px;padding:10px 14px;background:#f1f5f9;
  border:1px solid var(--line);border-radius:6px;word-break:break-all;margin:8px 0;}
@media(max-width:820px){body{grid-template-columns:1fr;}
  .sidebar{display:none;} .two{grid-template-columns:1fr;}}
`;

function navLink(href: string, label: string, current: string): string {
  const active = current.startsWith(href) && (href !== "/" || current === "/") ? " active" : "";
  return `<a href="${href}" class="${active}">${esc(label)}</a>`;
}

function adminNav(current: string): string {
  return `
    ${navLink("/portal/admin", "Dashboard", current)}
    ${navLink("/portal/admin/tenants", "Mandanten", current)}
    ${navLink("/portal/admin/users", "Benutzer", current)}
  `;
}

function customerNav(current: string): string {
  return `
    ${navLink("/portal/customer", "Dashboard", current)}
    ${navLink("/portal/customer/agents", "Agenten", current)}
    ${navLink("/portal/customer/runs", "Runs", current)}
    ${navLink("/portal/customer/contract", "Vertrag & Lizenz", current)}
    ${navLink("/portal/customer/tokens", "Registration Tokens", current)}
  `;
}

export function layout(opts: {
  title: string;
  session: PortalSession;
  current: string;
  content: string;
  flash?: { type: "ok" | "err"; msg: string };
}): string {
  const nav = opts.session.isSystemAdmin ? adminNav(opts.current) : customerNav(opts.current);
  const tenantLabel = opts.session.isSystemAdmin ? "System Admin" : (opts.session.tenantKey || "");
  const flash = opts.flash
    ? `<div class="alert alert-${opts.flash.type}">${esc(opts.flash.msg)}</div>`
    : "";

  return `<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>${esc(opts.title)} – SF Integration SaaS</title>
  <style>${CSS}</style>
</head>
<body>
<aside class="sidebar">
  <div class="logo">SF Integration<small>SaaS Control Center</small></div>
  <nav>${nav}</nav>
  <div class="user-info">
    <strong>${esc(opts.session.displayName || opts.session.email)}</strong>
    ${esc(tenantLabel)} · ${esc(opts.session.role || (opts.session.isSystemAdmin ? "admin" : ""))}
    <br><a href="/portal/logout">Abmelden</a>
  </div>
</aside>
<div class="main">
  <div class="topbar"><h1>${esc(opts.title)}</h1></div>
  <div class="content">${flash}${opts.content}</div>
</div>
</body></html>`;
}

export function loginPage(error?: string): string {
  return `<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Anmelden – SF Integration SaaS</title>
  <style>
    ${CSS}
    body{display:flex;align-items:center;justify-content:center;background:var(--sb);min-height:100vh;grid-template-columns:1fr;}
    .login-box{background:#fff;border-radius:12px;padding:36px 40px;width:100%;max-width:400px;box-shadow:0 8px 32px rgba(0,0,0,.3);}
    .login-box h1{margin:0 0 6px;font-size:22px;}
    .login-box p{color:var(--muted);margin:0 0 24px;font-size:13px;}
    .login-box button{width:100%;padding:10px;background:var(--sb-ac);color:#fff;border:none;
      border-radius:6px;font-size:14px;font-weight:600;cursor:pointer;margin-top:8px;}
    .err{color:var(--bad);font-size:13px;margin-bottom:12px;}
    .sidebar,.topbar{display:none;}
  </style>
</head>
<body>
  <div class="login-box">
    <h1>SF Integration SaaS</h1>
    <p>Bitte mit E-Mail und Passwort anmelden.</p>
    ${error ? `<p class="err">${esc(error)}</p>` : ""}
    <form method="POST" action="/api/portal/v1/auth/login">
      <div class="form-row"><label>E-Mail</label><input type="email" name="email" required autofocus/></div>
      <div class="form-row"><label>Passwort</label><input type="password" name="password" required/></div>
      <button type="submit">Anmelden</button>
    </form>
  </div>
</body></html>`;
}
