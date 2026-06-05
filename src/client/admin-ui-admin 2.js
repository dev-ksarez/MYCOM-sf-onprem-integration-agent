// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-admin
// Source lines: 7138–7510
// ──────────────────────────────────────────────────────────────────────

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

function renderProjectMembershipControls() {
  const projectSelect = document.getElementById('admin-membership-project');
  const userSelect = document.getElementById('admin-membership-user');
  if (!projectSelect || !userSelect) {
    return;
  }

  const activeProjects = (state.projects || []).filter((item) => item.archived !== true);
  if (!activeProjects.length) {
    projectSelect.innerHTML = '<option value="">Keine Projekte verfügbar</option>';
    projectSelect.value = '';
    state.selectedMembershipProjectId = '';
  } else {
    projectSelect.innerHTML = activeProjects.map((item) =>
      '<option value="' + esc(String(item.id || '')) + '">' + esc(String(item.name || item.id)) + '</option>'
    ).join('');
    const hasSelected = activeProjects.some((item) => String(item.id || '') === String(state.selectedMembershipProjectId || ''));
    state.selectedMembershipProjectId = hasSelected
      ? String(state.selectedMembershipProjectId || '')
      : String((activeProjects[0] && activeProjects[0].id) || '');
    projectSelect.value = state.selectedMembershipProjectId;
  }

  const users = state.adminUsers || [];
  if (!users.length) {
    userSelect.innerHTML = '<option value="">Keine Benutzer verfügbar</option>';
    userSelect.value = '';
  } else {
    userSelect.innerHTML = users.map((user) =>
      '<option value="' + esc(String(user.id || '')) + '">' + esc(String(user.displayName || user.username || user.id)) + '</option>'
    ).join('');
    userSelect.value = String((users[0] && users[0].id) || '');
  }
}

function renderProjectMemberships() {
  const body = document.getElementById('admin-memberships-body');
  if (!body) return;
  const items = state.projectMemberships || [];
  if (!items.length) {
    body.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine Zuordnungen für dieses Projekt.</td></tr>';
    return;
  }

  body.innerHTML = items.map((item) => {
    return '<tr>' +
      '<td><div class="fw-semibold">' + esc(String(item.displayName || item.username || item.userId || '-')) + '</div><div class="small text-secondary">' + esc(String(item.username || item.userId || '-')) + '</div></td>' +
      '<td>' + esc(String(item.roleInProject || '-')) + '</td>' +
      '<td>' + esc(formatDate(item.assignedAt, 'short')) + '</td>' +
      '<td class="text-nowrap"><button class="btn btn-sm btn-outline-danger" data-membership-revoke="' + esc(String(item.userId || '')) + '">Entziehen</button></td>' +
    '</tr>';
  }).join('');

  body.querySelectorAll('[data-membership-revoke]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const userId = String(btn.getAttribute('data-membership-revoke') || '').trim();
      const projectId = String(state.selectedMembershipProjectId || '').trim();
      if (!projectId || !userId) {
        return;
      }
      if (!window.confirm('Projektzuordnung wirklich entziehen?')) {
        return;
      }
      await requestJson('/api/admin/projects/' + encodeURIComponent(projectId) + '/members/' + encodeURIComponent(userId), {
        method: 'DELETE'
      });
      await loadProjectMemberships();
    });
  });
}

async function loadProjectMemberships() {
  renderProjectMembershipControls();
  const projectId = String(state.selectedMembershipProjectId || '').trim();
  if (!projectId) {
    state.projectMemberships = [];
    renderProjectMemberships();
    return;
  }

  const result = await safeRequest('/api/admin/projects/' + encodeURIComponent(projectId) + '/members', { items: [] });
  state.projectMemberships = Array.isArray(result.items) ? result.items : [];
  renderProjectMemberships();
}

async function assignProjectMembershipFromForm() {
  const projectId = String(document.getElementById('admin-membership-project') && document.getElementById('admin-membership-project').value || '').trim();
  const userId = String(document.getElementById('admin-membership-user') && document.getElementById('admin-membership-user').value || '').trim();
  const roleInProject = String(document.getElementById('admin-membership-role') && document.getElementById('admin-membership-role').value || 'viewer').trim();
  if (!projectId) {
    throw new Error('Projekt ist erforderlich');
  }
  if (!userId) {
    throw new Error('Benutzer ist erforderlich');
  }

  await requestJson('/api/admin/projects/' + encodeURIComponent(projectId) + '/members/' + encodeURIComponent(userId), {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ roleInProject })
  });
  state.selectedMembershipProjectId = projectId;
  await loadProjectMemberships();
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

function renderRolloutProjectOptions() {
  const select = document.getElementById('admin-rollout-project');
  if (!select) {
    return;
  }

  const projects = Array.isArray(state.projects) ? state.projects : [];
  if (!projects.length) {
    select.innerHTML = '<option value="default-project">Default-Projekt</option>';
    select.value = 'default-project';
    return;
  }

  select.innerHTML = projects
    .filter((item) => item.archived !== true)
    .map((item) => '<option value="' + esc(String(item.id || '')) + '">' + esc(String(item.name || item.id || '-')) + '</option>')
    .join('');

  const current = String(state.headerProjectId || '').trim();
  const hasCurrent = Array.from(select.options).some((entry) => String(entry.value || '') === current);
  select.value = hasCurrent ? current : String(select.options[0]?.value || 'default-project');
}

function renderRolloutKpiSnapshot(snapshot) {
  const status = document.getElementById('admin-rollout-status');
  const decisionBadge = document.getElementById('admin-rollout-decision-badge');
  const tableBody = document.getElementById('admin-rollout-kpi-body');
  const output = document.getElementById('admin-rollout-output');
  if (!status || !output || !tableBody || !decisionBadge) {
    return;
  }

  if (!snapshot || typeof snapshot !== 'object') {
    status.textContent = 'Keine KPI-Daten vorhanden.';
    decisionBadge.textContent = 'unavailable';
    decisionBadge.className = 'badge text-bg-secondary';
    tableBody.innerHTML = '<tr><td colspan="4" class="text-secondary">Keine KPI-Daten vorhanden.</td></tr>';
    output.textContent = '{}';
    return;
  }

  const unavailable = Array.isArray(snapshot?.notes?.unavailableKpis)
    ? snapshot.notes.unavailableKpis.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const decisionStatus = String(snapshot?.decision?.status || 'unknown').trim() || 'unknown';
  const modelVersion = String(snapshot?.dataModelVersion || '-').trim() || '-';
  const storageMode = String(snapshot?.storageMode || '-').trim() || '-';
  status.textContent = 'Entscheidung: ' + decisionStatus + ' · Modell: ' + modelVersion + ' · Modus: ' + storageMode
    + (unavailable.length ? ' · Fehlende KPIs: ' + unavailable.join(', ') : '');

  const decisionClassByStatus = {
    go: 'text-bg-success',
    'conditional-go': 'text-bg-warning',
    'insufficient-data': 'text-bg-secondary',
    'no-go': 'text-bg-danger'
  };
  decisionBadge.textContent = decisionStatus;
  decisionBadge.className = 'badge ' + (decisionClassByStatus[decisionStatus] || 'text-bg-secondary');

  const kpiMeta = [
    { key: 'legacyJsonDivergenceRatePct', label: 'Divergenzrate Legacy/JSON' },
    { key: 'healthPulseSuccessRatePct', label: 'Erfolgsrate Health-Pulse' },
    { key: 'commandAckSuccessRatePct', label: 'Erfolgsrate Command-Ack' },
    { key: 'idempotencyConflictRatePct', label: 'Idempotenzkonfliktquote' },
    { key: 'dailyLogBucketSegmentsPerInstance', label: 'Segmente pro Tag/Instanz' },
    { key: 'logRedeliveryWithin24hRatePct', label: 'Log-Nachlieferung in 24h' },
    { key: 'storageModeAuditCoveragePct', label: 'Audit-Abdeckung StorageMode' },
    { key: 'openCriticalMigrationIncidents', label: 'Offene kritische Incidents' }
  ];

  const thresholds = snapshot?.thresholds && typeof snapshot.thresholds === 'object' ? snapshot.thresholds : {};
  const kpis = snapshot?.kpis && typeof snapshot.kpis === 'object' ? snapshot.kpis : {};

  const formatValue = (key, value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return 'n/a';
    }
    const numeric = Number(value);
    if (key.endsWith('Pct')) {
      return numeric.toFixed(2) + '%';
    }
    return String(Number.isInteger(numeric) ? numeric : Number(numeric.toFixed(2)));
  };

  const formatThreshold = (threshold) => {
    if (!threshold || typeof threshold !== 'object') {
      return '-';
    }

    const items = [];
    if (threshold.goMin !== undefined) {
      items.push('go >= ' + String(threshold.goMin));
    }
    if (threshold.goMax !== undefined) {
      items.push('go <= ' + String(threshold.goMax));
    }
    if (threshold.rollbackTrigger !== undefined) {
      items.push('rollback ' + (threshold.goMin !== undefined ? '< ' : '> ') + String(threshold.rollbackTrigger));
    }
    return items.join(' · ') || '-';
  };

  const evaluateKpi = (value, threshold) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return { text: 'unavailable', badgeClass: 'text-bg-secondary' };
    }

    const numeric = Number(value);
    const hasGoMin = threshold && typeof threshold === 'object' && threshold.goMin !== undefined;
    const hasGoMax = threshold && typeof threshold === 'object' && threshold.goMax !== undefined;
    const hasRollback = threshold && typeof threshold === 'object' && threshold.rollbackTrigger !== undefined;

    if (hasGoMin) {
      if (hasRollback && numeric < Number(threshold.rollbackTrigger)) {
        return { text: 'no-go', badgeClass: 'text-bg-danger' };
      }
      if (numeric >= Number(threshold.goMin)) {
        return { text: 'go', badgeClass: 'text-bg-success' };
      }
      return { text: 'warning', badgeClass: 'text-bg-warning' };
    }

    if (hasGoMax) {
      if (hasRollback && numeric > Number(threshold.rollbackTrigger)) {
        return { text: 'no-go', badgeClass: 'text-bg-danger' };
      }
      if (numeric <= Number(threshold.goMax)) {
        return { text: 'go', badgeClass: 'text-bg-success' };
      }
      return { text: 'warning', badgeClass: 'text-bg-warning' };
    }

    return { text: 'info', badgeClass: 'text-bg-secondary' };
  };

  tableBody.innerHTML = kpiMeta.map((item) => {
    const value = kpis[item.key];
    const threshold = thresholds[item.key];
    const signal = evaluateKpi(value, threshold);
    return '<tr>'
      + '<td>' + esc(item.label) + '</td>'
      + '<td>' + esc(formatValue(item.key, value)) + '</td>'
      + '<td class="small text-secondary">' + esc(formatThreshold(threshold)) + '</td>'
      + '<td><span class="badge ' + esc(signal.badgeClass) + '">' + esc(signal.text) + '</span></td>'
      + '</tr>';
  }).join('');

  output.textContent = JSON.stringify(snapshot, null, 2);
}

async function loadRolloutKpisFromAdminPanel() {
  const projectSelect = document.getElementById('admin-rollout-project');
  const envSelect = document.getElementById('admin-rollout-env');
  const windowInput = document.getElementById('admin-rollout-window-days');
  if (!projectSelect || !envSelect || !windowInput) {
    return;
  }

  const projectId = String(projectSelect.value || '').trim() || 'default-project';
  const targetEnv = String(envSelect.value || '').trim() === 'production' ? 'production' : 'test';
  const windowDays = Math.max(1, Math.min(90, Number(windowInput.value || 14) || 14));
  windowInput.value = String(windowDays);

  const snapshot = await safeRequest(
    '/api/admin/projects/'
      + encodeURIComponent(projectId)
      + '/rollout/kpis?targetEnv=' + encodeURIComponent(targetEnv)
      + '&windowDays=' + encodeURIComponent(String(windowDays)),
    null
  );

  state.rolloutKpiSnapshot = snapshot;
  renderRolloutKpiSnapshot(snapshot);
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
    await loadProjectMemberships();
    renderAuditHistory();
    renderRolloutProjectOptions();
    await loadRolloutKpisFromAdminPanel();
  } else if (currentUserHasModule('projects') || currentUserHasModule('deployment')) {
    await loadProjectMemberships();
    renderRolloutProjectOptions();
    await loadRolloutKpisFromAdminPanel();
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

