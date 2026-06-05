// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-settings
// Source lines: 12148–13793
// ──────────────────────────────────────────────────────────────────────

async function loadInstances() {
  const select = document.getElementById('instance-select');
  const response = await safeRequest('/api/instances', { items: [] });
  const items = response.items || [];
  state.instances = items;
  const defaultInstance = items.find((item) => item.isDefault);

  if (!items.length) {
    select.innerHTML = '<option value="">Keine Instanzen konfiguriert</option>';
    state.instanceId = '';
    return;
  }

  const previousInstanceId = String(state.instanceId || '').trim();
  const previousProjectId = String(state.headerProjectId || '').trim() || 'default-project';
  const previousTargetEnv = state.headerTargetEnv === 'production' ? 'production' : 'test';
  select.innerHTML = items.map((item) => {
    const projectPart = item.projectName ? (' [' + String(item.projectName) + ']') : '';
    const rolePart = item.role === 'production' ? ' (Produktion)' : ' (Test)';
    const label = item.isDefault ? (String(item.name || item.id) + ' (Default aus .env)') : String(item.name || item.id);
    return '<option value="' + esc(item.id) + '">' + esc(label + projectPart + rolePart) + '</option>';
  }).join('');

  const hasCurrent = items.some((item) => String(item.id || '') === previousInstanceId);
  const contextInstanceId = findInstanceForContext(previousProjectId, previousTargetEnv);
  if (hasCurrent) {
    state.instanceId = previousInstanceId;
  } else if (contextInstanceId) {
    state.instanceId = contextInstanceId;
  } else if (previousProjectId && previousProjectId !== 'default-project') {
    state.instanceId = '';
  } else if (defaultInstance) {
    state.instanceId = String(defaultInstance.id || '').trim();
  } else {
    state.instanceId = String(items[0].id || '').trim();
  }
  select.value = state.instanceId;

  syncHeaderContextFromSelectedInstance({ updateFromSelectedInstance: Boolean(state.instanceId) });

  populateProjectInstanceSelectors(state.editingProjectId || String(document.getElementById('prj-id')?.value || '').trim());
  renderProjectTable();
  renderInstancePanels();
}

function populateHeaderContextProjectOptions() {
  const projectSelect = document.getElementById('context-project-select');
  if (!projectSelect) {
    return;
  }

  const projects = (Array.isArray(state.projects) ? state.projects : []).filter((item) => item.archived !== true);
  const options = projects.length
    ? projects.map((item) => ({ id: String(item.id || ''), name: String(item.name || item.id || '') }))
    : Array.from(new Map((Array.isArray(state.instances) ? state.instances : []).map((item) => [
      String(item.projectId || 'default-project'),
      { id: String(item.projectId || 'default-project'), name: String(item.projectName || item.projectId || 'default-project') }
    ])).values());

  if (!options.length) {
    projectSelect.innerHTML = '<option value="default-project">Default-Projekt</option>';
    state.headerProjectId = 'default-project';
    projectSelect.value = 'default-project';
    return;
  }

  projectSelect.innerHTML = options
    .map((item) => '<option value="' + esc(item.id) + '">' + esc(item.name) + '</option>')
    .join('');

  const currentId = String(state.headerProjectId || '').trim();
  const resolvedId = options.some((item) => item.id === currentId) ? currentId : options[0].id;
  state.headerProjectId = resolvedId;
  projectSelect.value = resolvedId;
}

function renderContextSelectionSummary() {
  const summary = document.getElementById('context-selection-summary');
  const pill = document.getElementById('active-context-pill');
  const projectEl = document.getElementById('active-context-project');
  const envEl = document.getElementById('active-context-env');
  const instanceEl = document.getElementById('active-context-instance');
  const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(state.instanceId || ''));
  if (!selectedInstance) {
    const activeProject = (state.projects || []).find((item) => String(item.id || '') === String(state.headerProjectId || ''));
    const projectName = String(activeProject?.name || state.headerProjectId || 'Default-Projekt');
    const envLabel = state.headerTargetEnv === 'production' ? 'Produktion' : 'Test';
    if (summary) summary.textContent = 'Projekt: ' + projectName + ' · Umgebung: ' + envLabel + ' · keine passende Instanz konfiguriert.';
    if (projectEl) projectEl.textContent = 'Projekt: ' + projectName;
    if (envEl) envEl.textContent = envLabel;
    if (instanceEl) instanceEl.textContent = 'Keine Instanz';
    if (pill) {
      pill.classList.remove('active-context-production', 'active-context-test');
      pill.classList.add('active-context-none');
    }
    document.body.classList.remove('context-production', 'context-test');
    document.body.classList.add('context-none');
    return;
  }

  const projectName = String(selectedInstance.projectName || state.headerProjectId || 'Default-Projekt');
  const envLabel = selectedInstance.role === 'production' ? 'Produktion' : 'Test';
  const instanceName = String(selectedInstance.name || selectedInstance.id || '-');
  if (summary) summary.textContent = 'Projekt: ' + projectName + ' · Umgebung: ' + envLabel + ' · Instanz: ' + instanceName;
  if (projectEl) projectEl.textContent = 'Projekt: ' + projectName;
  if (envEl) envEl.textContent = envLabel;
  if (instanceEl) instanceEl.textContent = 'Instanz: ' + instanceName;
  if (pill) {
    pill.classList.remove('active-context-production', 'active-context-test', 'active-context-none');
    pill.classList.add(selectedInstance.role === 'production' ? 'active-context-production' : 'active-context-test');
  }
  document.body.classList.remove('context-production', 'context-test', 'context-none');
  document.body.classList.add(selectedInstance.role === 'production' ? 'context-production' : 'context-test');
}

function resolveReadinessBadgeClass(status) {
  const normalized = String(status || '').trim();
  if (normalized === 'ready') {
    return 'text-bg-success';
  }
  if (normalized === 'setup-required') {
    return 'text-bg-warning';
  }
  if (normalized === 'setup-running') {
    return 'text-bg-info';
  }
  if (normalized === 'setup-failed') {
    return 'text-bg-danger';
  }
  return 'text-bg-secondary';
}

function setInstanceSetupBusy(isBusy, label) {
  ['admin-instance-readiness-check', 'admin-instance-msd-setup-dry', 'admin-instance-msd-setup-apply'].forEach((id) => {
    const button = document.getElementById(id);
    if (!button) {
      return;
    }
    if (!button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.textContent || '';
    }
    button.disabled = !!isBusy;
    button.textContent = isBusy && label ? label : button.dataset.defaultLabel;
  });
}

function renderInstanceReadinessSnapshot(snapshot) {
  const badge = document.getElementById('ins-readiness-status');
  const output = document.getElementById('ins-readiness-output');
  const progress = document.getElementById('ins-readiness-progress');
  if (!badge || !output || !progress) {
    return;
  }

  const status = String(snapshot?.status || 'n/a').trim() || 'n/a';
  badge.textContent = status;
  badge.className = 'badge ' + resolveReadinessBadgeClass(status);

  if (snapshot && snapshot.instanceId) {
    state.instanceReadinessSnapshots[String(snapshot.instanceId)] = snapshot;
  }

  const missingArtifacts = Array.isArray(snapshot?.missingArtifacts) ? snapshot.missingArtifacts : [];
  const warnings = Array.isArray(snapshot?.warnings) ? snapshot.warnings.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const applied = Array.isArray(snapshot?.applied) ? snapshot.applied.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const capabilities = snapshot?.capabilities && typeof snapshot.capabilities === 'object' ? snapshot.capabilities : {};
  const criticalArtifactCount = missingArtifacts.filter((item) => String(item?.severity || '') === 'critical').length;

  const setupRows = applied.map((name) => ({
    label: 'Angelegt / aktualisiert',
    state: 'passed',
    detail: name
  })).concat(warnings.map((message) => ({
    label: 'Setup-Warnung',
    state: 'warning',
    detail: message
  })));

  const rows = [
    { label: 'Salesforce-Verbindung', state: snapshot ? (criticalArtifactCount ? 'warning' : 'passed') : 'pending', detail: snapshot ? 'Readiness-Check wurde ausgeführt.' : 'Noch nicht geprüft.' },
    { label: 'Health-Pulse Fähigkeit', state: capabilities.healthPulse ? 'passed' : (snapshot ? 'failed' : 'pending'), detail: capabilities.healthPulse ? 'Verfügbar' : 'Nicht bestätigt' },
    { label: 'Remote-Commands Fähigkeit', state: capabilities.remoteCommands ? 'passed' : (snapshot ? 'failed' : 'pending'), detail: capabilities.remoteCommands ? 'Verfügbar' : 'Nicht bestätigt' },
    { label: 'Log-Upload Fähigkeit', state: capabilities.logUpload ? 'passed' : (snapshot ? 'failed' : 'pending'), detail: capabilities.logUpload ? 'Verfügbar' : 'Nicht bestätigt' }
  ].concat(setupRows).concat(missingArtifacts.map((item) => ({
    label: String(item?.name || item?.type || 'Artefakt'),
    state: String(item?.severity || '') === 'critical' ? 'failed' : 'warning',
    detail: String(item?.message || item?.type || '')
  })));

  const failedCount = rows.filter((row) => row.state === 'failed').length;
  const warningCount = rows.filter((row) => row.state === 'warning').length;
  const passedCount = rows.filter((row) => row.state === 'passed').length;
  const percent = !snapshot
    ? 0
    : status === 'ready'
      ? 100
      : Math.max(0, Math.min(100, Math.round((passedCount / Math.max(1, rows.length)) * 100)));
  progress.style.width = String(percent) + '%';
  progress.className = 'progress-bar ' + (status === 'ready' ? 'bg-success' : failedCount ? 'bg-danger' : warningCount ? 'bg-warning' : 'bg-secondary');

  output.innerHTML = rows.map((row) => {
    const state = String(row.state || 'pending');
    const icon = state === 'passed' ? '✓' : state === 'failed' ? '!' : state === 'warning' ? '!' : '…';
    const badgeClass = state === 'passed' ? 'text-bg-success' : state === 'failed' ? 'text-bg-danger' : state === 'warning' ? 'text-bg-warning' : 'text-bg-secondary';
    return '<div class="instance-readiness-row">' +
      '<span class="badge ' + badgeClass + '">' + esc(icon) + '</span>' +
      '<div><div class="fw-semibold">' + esc(row.label) + '</div><div class="small text-secondary">' + esc(row.detail || '-') + '</div></div>' +
    '</div>';
  }).join('');
}

function resolveInstanceIdForReadiness(preferredInstanceId) {
  const preferred = String(preferredInstanceId || '').trim();
  if (preferred) {
    return preferred;
  }
  const instanceIdFromForm = String(document.getElementById('ins-id')?.value || '').trim();
  if (instanceIdFromForm) {
    return instanceIdFromForm;
  }
  return String(state.instanceId || '').trim();
}

async function runInstanceReadinessCheckFromAdminForm(preferredInstanceId) {
  const instanceId = resolveInstanceIdForReadiness(preferredInstanceId);
  if (!instanceId) {
    showInstanceModalError('Bitte zuerst eine Instanz-ID angeben oder eine Instanz auswählen.');
    return;
  }

  const projectId = String(document.getElementById('ins-project-id')?.value || '').trim() || undefined;
  const targetEnv = String(document.getElementById('ins-role')?.value || '').trim() === 'production' ? 'production' : 'test';
  const payload = {
    projectId,
    targetEnv,
    mode: 'validate-only',
    requestedBy: String(state.adminMe?.user?.username || 'admin-ui').trim() || 'admin-ui'
  };

  setInstanceSetupBusy(true, 'Prüfe...');
  renderInstanceReadinessSnapshot({
    instanceId,
    projectId,
    status: 'setup-running',
    missingArtifacts: [],
    warnings: ['Readiness-Check läuft. Salesforce wird geprüft...'],
    capabilities: { healthPulse: false, remoteCommands: false, logUpload: false }
  });
  try {
    const snapshot = await requestJson('/api/admin/sf-instances/' + encodeURIComponent(instanceId) + '/readiness-check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    clearInstanceModalError();
    renderInstanceReadinessSnapshot(snapshot);
    renderInstancePanels();
    showInfo('Readiness-Check abgeschlossen.');
  } catch (error) {
    renderInstanceReadinessSnapshot({ instanceId, projectId, status: 'setup-failed', missingArtifacts: [], warnings: [], capabilities: { healthPulse: false, remoteCommands: false, logUpload: false } });
    throw error;
  } finally {
    setInstanceSetupBusy(false);
  }
}

async function runInstanceMsdSetupFromAdminForm(mode, preferredInstanceId) {
  const setupMode = mode === 'dry-run' ? 'dry-run' : 'apply';
  const instanceId = resolveInstanceIdForReadiness(preferredInstanceId);
  if (!instanceId) {
    showInstanceModalError('Bitte zuerst eine Instanz-ID angeben oder eine Instanz auswählen.');
    return;
  }

  const projectId = String(document.getElementById('ins-project-id')?.value || '').trim() || undefined;
  const targetEnv = String(document.getElementById('ins-role')?.value || '').trim() === 'production' ? 'production' : 'test';
  const payload = {
    projectId,
    targetEnv,
    mode: setupMode,
    requestedBy: String(state.adminMe?.user?.username || 'admin-ui').trim() || 'admin-ui'
  };

  setInstanceSetupBusy(true, setupMode === 'dry-run' ? 'Dry-Run läuft...' : 'Setup läuft...');
  renderInstanceReadinessSnapshot({
    instanceId,
    projectId,
    status: 'setup-running',
    missingArtifacts: [],
    warnings: [setupMode === 'dry-run' ? 'Dry-Run läuft. Es werden keine Änderungen in Salesforce vorgenommen...' : 'MSD Setup läuft. Salesforce-Metadaten werden angelegt...'],
    applied: [],
    capabilities: { healthPulse: false, remoteCommands: false, logUpload: false }
  });
  try {
    const snapshot = await requestJson('/api/admin/sf-instances/' + encodeURIComponent(instanceId) + '/msd-setup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    renderInstanceReadinessSnapshot(snapshot);
    renderInstancePanels();
    if (snapshot?.status === 'ready') {
      clearInstanceModalError();
      showInfo(setupMode === 'dry-run' ? 'MSD Setup Dry-Run abgeschlossen.' : 'MSD Setup ausgeführt.');
    } else if (setupMode === 'apply') {
      showInstanceModalWarning('MSD Setup wurde ausgeführt, aber die Instanz ist noch nicht bereit. Details stehen unten in der Checkliste.');
    } else {
      showInstanceModalWarning('MSD Setup Dry-Run abgeschlossen. Es wurden keine Änderungen in Salesforce vorgenommen.');
    }
  } catch (error) {
    renderInstanceReadinessSnapshot({ instanceId, projectId, status: 'setup-failed', missingArtifacts: [], warnings: [], capabilities: { healthPulse: false, remoteCommands: false, logUpload: false } });
    throw error;
  } finally {
    setInstanceSetupBusy(false);
  }
}

async function refreshInstanceMetadataFromAdminPanel(instanceId) {
  const normalizedInstanceId = String(instanceId || '').trim();
  if (!normalizedInstanceId) {
    return;
  }

  try {
    showInfo('Metadaten-Refresh läuft...');
    const snapshot = await requestJson('/api/admin/sf-instances/' + encodeURIComponent(normalizedInstanceId) + '/metadata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        objectNames: ['Account', 'Contact', 'Lead', 'Opportunity', 'Order', 'Product2', 'Pricebook2', 'PricebookEntry'],
        includeAllFields: false,
        maxFieldObjects: 40
      })
    });
    state.instanceMetadataSnapshots[normalizedInstanceId] = snapshot;
    renderInstancePanels();
    showInfo('Metadaten gespeichert: ' + String(snapshot.objectCount || 0) + ' Objekte, ' + String(snapshot.fieldCount || 0) + ' Felder.');
  } catch (error) {
    showError('Metadaten-Refresh fehlgeschlagen: ' + (error.message || 'Unbekannter Fehler'));
  }
}

function syncHeaderContextFromSelectedInstance(options = {}) {
  const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(state.instanceId || ''));
  const updateFromSelectedInstance = options.updateFromSelectedInstance !== false;
  if (selectedInstance && updateFromSelectedInstance) {
    state.headerProjectId = String(selectedInstance.projectId || 'default-project').trim() || 'default-project';
    state.headerTargetEnv = selectedInstance.role === 'production' ? 'production' : 'test';
  }

  populateHeaderContextProjectOptions();

  const envSelect = document.getElementById('context-target-env-select');
  if (envSelect) {
    envSelect.value = state.headerTargetEnv === 'production' ? 'production' : 'test';
  }

  persistHeaderContext();
  renderContextSelectionSummary();
}

function findInstanceForContext(projectId, targetEnv) {
  const normalizedProjectId = String(projectId || '').trim() || 'default-project';
  const expectedRole = String(targetEnv || 'test') === 'production' ? 'production' : 'test';
  const candidates = (state.instances || []).filter((item) => String(item.projectId || 'default-project').trim() === normalizedProjectId);
  const byRole = candidates.find((item) => String(item.role || 'test') === expectedRole);
  if (byRole) {
    return String(byRole.id || '').trim();
  }
  return '';
}

function getRuntimeContextUnavailableMessage() {
  const normalizedProjectId = String(state.headerProjectId || '').trim() || 'default-project';
  const expectedRole = state.headerTargetEnv === 'production' ? 'production' : 'test';
  const activeProject = (state.projects || []).find((item) => String(item.id || '') === normalizedProjectId);
  const projectName = String(activeProject?.name || normalizedProjectId || 'Default-Projekt');
  const envLabel = expectedRole === 'production' ? 'Produktion' : 'Test';
  const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(state.instanceId || '').trim());
  if (!selectedInstance) {
    return 'Fuer Projekt "' + projectName + '" und Umgebung "' + envLabel + '" ist keine passende Salesforce-Instanz konfiguriert.';
  }
  const selectedProjectId = String(selectedInstance.projectId || 'default-project').trim() || 'default-project';
  const selectedRole = selectedInstance.role === 'production' ? 'production' : 'test';
  if (selectedProjectId !== normalizedProjectId || selectedRole !== expectedRole) {
    return 'Die ausgewaehlte Salesforce-Instanz passt nicht zum Header-Kontext "' + projectName + ' / ' + envLabel + '".';
  }
  return '';
}

function persistHeaderContext() {
  try {
    window.localStorage.setItem(HEADER_CONTEXT_STORAGE_KEY, JSON.stringify({
      projectId: String(state.headerProjectId || 'default-project').trim() || 'default-project',
      targetEnv: state.headerTargetEnv === 'production' ? 'production' : 'test',
      instanceId: String(state.instanceId || '').trim()
    }));
  } catch {
    // localStorage is optional; the in-memory context still drives the current session.
  }
}

function restoreHeaderContext() {
  try {
    const raw = window.localStorage.getItem(HEADER_CONTEXT_STORAGE_KEY);
    if (!raw) {
      return;
    }
    const parsed = JSON.parse(raw);
    state.headerProjectId = String(parsed?.projectId || state.headerProjectId || 'default-project').trim() || 'default-project';
    state.headerTargetEnv = parsed?.targetEnv === 'production' ? 'production' : 'test';
    state.instanceId = String(parsed?.instanceId || state.instanceId || '').trim();
  } catch {
    state.headerProjectId = state.headerProjectId || 'default-project';
    state.headerTargetEnv = state.headerTargetEnv === 'production' ? 'production' : 'test';
  }
}

function populateProjectInstanceSelectors(projectId) {
  const testSelect = document.getElementById('prj-test-instance-id');
  const productionSelect = document.getElementById('prj-production-instance-id');
  if (!testSelect || !productionSelect) {
    return;
  }

  const normalizedProjectId = String(projectId || state.editingProjectId || '').trim();
  const allInstances = Array.isArray(state.instances) ? state.instances : [];
  const projectInstances = normalizedProjectId
    ? allInstances.filter((item) => {
      const assignedProjectId = String(item.projectId || '').trim();
      return !assignedProjectId || assignedProjectId === 'default-project' || assignedProjectId === normalizedProjectId;
    })
    : allInstances;

  const buildOptions = () => {
    const base = ['<option value="">Nicht zugeordnet</option>'];
    return base.concat(projectInstances.map((item) => {
      const roleLabel = item.role === 'production' ? 'Produktion' : 'Test';
      const projectLabel = item.projectName ? (' [' + String(item.projectName) + ']') : '';
      return '<option value="' + esc(String(item.id || '')) + '">' + esc(String(item.name || item.id) + ' (' + roleLabel + ')' + projectLabel) + '</option>';
    }));
  };

  const currentTest = String(testSelect.value || '').trim();
  const currentProduction = String(productionSelect.value || '').trim();
  testSelect.innerHTML = buildOptions().join('');
  productionSelect.innerHTML = buildOptions().join('');
  testSelect.value = projectInstances.some((item) => String(item.id || '') === currentTest) ? currentTest : '';
  productionSelect.value = projectInstances.some((item) => String(item.id || '') === currentProduction) ? currentProduction : '';
}

async function loadProjects() {
  const response = await safeRequest('/api/projects', { items: [] });
  state.projects = Array.isArray(response.items) ? response.items : [];
  const select = document.getElementById('ins-project-id');
  if (!select) {
    return;
  }

  const activeProjects = state.projects.filter((item) => item.archived !== true);

  if (!activeProjects.length) {
    select.innerHTML = '<option value="default-project">Default-Projekt</option>';
    select.value = 'default-project';
    populateHeaderContextProjectOptions();
    renderContextSelectionSummary();
    populateProjectInstanceSelectors(state.editingProjectId || String(document.getElementById('prj-id')?.value || '').trim());
    renderProjectTable();
    return;
  }

  select.innerHTML = activeProjects.map((item) =>
    '<option value="' + esc(item.id) + '">' + esc(String(item.name || item.id)) + '</option>'
  ).join('');

  const hasDefault = activeProjects.some((item) => String(item.id || '') === 'default-project');
  select.value = hasDefault ? 'default-project' : String(activeProjects[0].id || 'default-project');

  populateHeaderContextProjectOptions();
  renderContextSelectionSummary();
  populateProjectInstanceSelectors(state.editingProjectId || String(document.getElementById('prj-id')?.value || '').trim());
  renderProjectTable();
}

function resetProjectForm() {
  state.editingProjectId = '';
  state.projectWizardStep = 1;
  document.getElementById('prj-id').value = '';
  document.getElementById('prj-name').value = '';
  document.getElementById('prj-description').value = '';
  document.getElementById('prj-confluence-base-url').value = '';
  document.getElementById('prj-confluence-username').value = '';
  document.getElementById('prj-confluence-api-token').value = '';
  document.getElementById('prj-confluence-space-key').value = '';
  document.getElementById('prj-confluence-parent-page-id').value = '';
  document.getElementById('prj-confluence-title-prefix').value = '';
  document.getElementById('prj-confluence-token-status').textContent = 'Noch kein Projekttoken hinterlegt.';
  document.getElementById('prj-lookup-cache-enabled').checked = true;
  document.getElementById('prj-lookup-cache-ttl-minutes').value = '15';
  document.getElementById('prj-log-batching-enabled').checked = true;
  document.getElementById('prj-log-sync-interval-minutes').value = '5';
  document.getElementById('prj-log-batch-size').value = '200';
  document.getElementById('prj-log-buffer-max-entries').value = '10000';
  document.getElementById('prj-production-write-protection').checked = true;
  const testSelect = document.getElementById('prj-test-instance-id');
  const productionSelect = document.getElementById('prj-production-instance-id');
  if (testSelect) testSelect.value = '';
  if (productionSelect) productionSelect.value = '';
  populateProjectInstanceSelectors('');
  renderProjectWizardMeta(null);
  renderProjectReview();
  goToProjectWizardStep(1);
}

function resetInstanceForm() {
  const hasDefault = (state.projects || []).some((item) => String(item.id || '') === 'default-project' && item.archived !== true);
  clearInstanceModalError();
  document.getElementById('ins-id').value = '';
  document.getElementById('ins-id').disabled = false;
  document.getElementById('ins-name').value = '';
  document.getElementById('ins-project-id').value = hasDefault ? 'default-project' : String((state.projects[0] && state.projects[0].id) || 'default-project');
  document.getElementById('ins-role').value = 'test';
  document.getElementById('ins-login-url').value = 'https://test.salesforce.com';
  document.getElementById('ins-client-id').value = '';
  document.getElementById('ins-client-secret').value = '';
  document.getElementById('ins-query-limit').value = '';
  const title = document.getElementById('instance-modal-title');
  const meta = document.getElementById('ins-modal-meta');
  if (title) title.textContent = 'Neue Instanz';
  if (meta) meta.textContent = 'Neue Instanz · noch nicht gespeichert';
  renderInstanceReadinessSnapshot(null);
}

function getDefaultInstanceLoginUrlForRole(role) {
  return String(role || '').trim() === 'production'
    ? 'https://login.salesforce.com'
    : 'https://test.salesforce.com';
}

function syncInstanceLoginUrlWithRole() {
  var roleEl = document.getElementById('ins-role');
  var loginUrlEl = document.getElementById('ins-login-url');
  if (!roleEl || !loginUrlEl) {
    return;
  }

  var currentValue = String(loginUrlEl.value || '').trim();
  if (currentValue.endsWith('/')) {
    currentValue = currentValue.slice(0, -1);
  }
  var defaultUrls = ['https://login.salesforce.com', 'https://test.salesforce.com'];
  if (!currentValue || defaultUrls.includes(currentValue)) {
    loginUrlEl.value = getDefaultInstanceLoginUrlForRole(roleEl.value);
  }
}

function fillInstanceForm(instance) {
  clearInstanceModalError();
  document.getElementById('ins-id').value = String(instance?.id || '');
  document.getElementById('ins-id').disabled = Boolean(instance?.id);
  document.getElementById('ins-name').value = String(instance?.name || instance?.id || '');
  document.getElementById('ins-project-id').value = String(instance?.projectId || 'default-project');
  document.getElementById('ins-role').value = instance?.role === 'production' ? 'production' : 'test';
  document.getElementById('ins-login-url').value = String((instance && instance.loginUrl) || getDefaultInstanceLoginUrlForRole(instance && instance.role));
  document.getElementById('ins-client-id').value = '';
  document.getElementById('ins-client-secret').value = '';
  document.getElementById('ins-query-limit').value = instance && instance.queryLimit ? String(instance.queryLimit) : '';
  const title = document.getElementById('instance-modal-title');
  const meta = document.getElementById('ins-modal-meta');
  if (title) title.textContent = 'Instanz bearbeiten: ' + String(instance?.name || instance?.id || '-');
  if (meta) meta.textContent = 'Instanz: ' + String(instance?.id || '-') + ' · Projekt: ' + String(instance?.projectName || instance?.projectId || '-');
  renderInstanceReadinessSnapshot(state.instanceReadinessSnapshots[String(instance?.id || '')] || null);
}

function openInstanceModal(instanceId) {
  const instance = (state.instances || []).find((item) => String(item.id || '') === String(instanceId || ''));
  if (instance) {
    fillInstanceForm(instance);
  } else {
    resetInstanceForm();
  }
  const modalEl = document.getElementById('instance-modal');
  if (modalEl && window.bootstrap?.Modal) {
    window.bootstrap.Modal.getOrCreateInstance(modalEl).show();
  }
}

function renderInstancePanels() {
  const panels = document.getElementById('instances-panels');
  const summary = document.getElementById('instances-summary');
  if (!panels) {
    return;
  }
  const filterValue = String(document.getElementById('instances-filter')?.value || '').trim().toLowerCase();
  const instances = filterValue
    ? (state.instances || []).filter((item) => [
      item.id,
      item.name,
      item.projectName,
      item.projectId,
      item.role
    ].map((value) => String(value || '').toLowerCase()).some((value) => value.includes(filterValue)))
    : (state.instances || []);
  if (summary) {
    const productionCount = (state.instances || []).filter((item) => item.role === 'production').length;
    const testCount = Math.max(0, (state.instances || []).length - productionCount);
    summary.textContent = String((state.instances || []).length) + ' Instanzen, ' + String(testCount) + ' Test, ' + String(productionCount) + ' Produktion';
  }
  if (!instances.length) {
    panels.innerHTML = '<div class="col-12"><div class="alert alert-light border text-secondary mb-0">Keine Instanzen gefunden.</div></div>';
    return;
  }
  panels.innerHTML = instances.map((item) => {
    const snapshot = state.instanceReadinessSnapshots[String(item.id || '')] || null;
    const metadataSnapshot = state.instanceMetadataSnapshots[String(item.id || '')] || null;
    const status = String(snapshot?.status || 'nicht geprüft');
    const statusClass = snapshot ? resolveReadinessBadgeClass(snapshot.status) : 'text-bg-secondary';
    const metadataLabel = metadataSnapshot?.status === 'success'
      ? String(metadataSnapshot.objectCount || 0) + ' Objekte'
      : 'Nicht geladen';
    const roleLabel = item.role === 'production' ? 'Produktion' : 'Test';
    const roleClass = item.role === 'production' ? 'text-bg-danger' : 'text-bg-info';
    return '<div class="col-12 col-xl-6" data-instance-panel>' +
      '<div class="card h-100 border-0 shadow-sm bg-body-tertiary">' +
        '<div class="card-body d-flex flex-column gap-3">' +
          '<div class="d-flex justify-content-between align-items-start gap-3">' +
            '<div>' +
              '<div class="d-flex flex-wrap gap-2 align-items-center mb-1"><strong>' + esc(String(item.name || item.id)) + '</strong><span class="badge ' + roleClass + '">' + esc(roleLabel) + '</span><span class="badge ' + statusClass + '">' + esc(status) + '</span></div>' +
              '<div class="small text-secondary">' + esc(String(item.id || '-')) + ' · ' + esc(String(item.projectName || item.projectId || 'Default-Projekt')) + '</div>' +
            '</div>' +
            '<div class="d-flex flex-wrap gap-1 justify-content-end">' +
              '<button class="btn btn-sm btn-outline-primary" data-edit-instance="' + esc(String(item.id || '')) + '">Öffnen</button>' +
              '<button class="btn btn-sm btn-outline-secondary" data-check-instance="' + esc(String(item.id || '')) + '">Readiness</button>' +
              '<button class="btn btn-sm btn-outline-secondary" data-refresh-instance-metadata="' + esc(String(item.id || '')) + '">Metadaten</button>' +
            '</div>' +
          '</div>' +
          '<div class="project-kpi-grid">' +
            '<div class="project-kpi"><span>Projekt</span><strong>' + esc(String(item.projectName || item.projectId || '-')) + '</strong></div>' +
            '<div class="project-kpi"><span>Umgebung</span><strong>' + esc(roleLabel) + '</strong></div>' +
            '<div class="project-kpi"><span>Default</span><strong>' + esc(item.isDefault ? 'Ja' : 'Nein') + '</strong></div>' +
            '<div class="project-kpi"><span>Metadaten</span><strong>' + esc(metadataLabel) + '</strong></div>' +
          '</div>' +
        '</div>' +
      '</div>' +
    '</div>';
  }).join('');
  panels.querySelectorAll('[data-edit-instance]').forEach((button) => {
    button.addEventListener('click', () => openInstanceModal(button.getAttribute('data-edit-instance')));
  });
  panels.querySelectorAll('[data-check-instance]').forEach((button) => {
    button.addEventListener('click', async () => {
      const instanceId = button.getAttribute('data-check-instance');
      openInstanceModal(instanceId);
      try {
        await runInstanceReadinessCheckFromAdminForm(instanceId);
      } catch (error) {
        showInstanceModalError(error.message || 'Readiness-Check fehlgeschlagen');
      }
    });
  });
  panels.querySelectorAll('[data-refresh-instance-metadata]').forEach((button) => {
    button.addEventListener('click', async () => {
      await refreshInstanceMetadataFromAdminPanel(button.getAttribute('data-refresh-instance-metadata'));
    });
  });
}

function getProjectKpis(project) {
  const projectId = String(project?.id || '').trim();
  const projectInstances = (state.instances || []).filter((entry) => String(entry.projectId || '').trim() === projectId);
  const testInstance = projectInstances.find((entry) => entry.role !== 'production');
  const productionInstance = projectInstances.find((entry) => entry.role === 'production');
  const projectMigrations = (state.migrations || []).filter((entry) => String(entry.projectId || '').trim() === projectId);
  const activeSchedules = (state.schedules || []).filter((entry) => entry.active === true).length;
  return {
    projectInstances,
    testInstance,
    productionInstance,
    migrationCount: projectMigrations.length,
    activeSchedules,
    cacheEnabled: project.lookupCacheEnabled !== false,
    logBatchingEnabled: project.logBatchingEnabled !== false
  };
}

function getProjectHealthSummary(kpis) {
  const projectInstances = Array.isArray(kpis?.projectInstances) ? kpis.projectInstances : [];
  const rows = projectInstances.map((instance) => {
    const snapshot = state.instanceReadinessSnapshots[String(instance.id || '')] || null;
    const status = String(snapshot?.status || 'nicht geprüft').trim() || 'nicht geprüft';
    return {
      instance,
      status,
      badgeClass: snapshot ? resolveReadinessBadgeClass(status) : 'text-bg-secondary'
    };
  });
  if (!rows.length) {
    return {
      label: 'Keine Instanz',
      badgeClass: 'text-bg-secondary',
      details: ['Keine Projektinstanz zugeordnet.']
    };
  }
  if (rows.some((row) => row.status === 'setup-failed')) {
    return {
      label: 'Fehler',
      badgeClass: 'text-bg-danger',
      details: rows.map((row) => String(row.instance.name || row.instance.id || '-') + ': ' + row.status)
    };
  }
  if (rows.some((row) => row.status === 'setup-required')) {
    return {
      label: 'Setup erforderlich',
      badgeClass: 'text-bg-warning',
      details: rows.map((row) => String(row.instance.name || row.instance.id || '-') + ': ' + row.status)
    };
  }
  if (rows.some((row) => row.status === 'setup-running')) {
    return {
      label: 'Prüfung läuft',
      badgeClass: 'text-bg-info',
      details: rows.map((row) => String(row.instance.name || row.instance.id || '-') + ': ' + row.status)
    };
  }
  if (rows.every((row) => row.status === 'ready')) {
    return {
      label: 'Bereit',
      badgeClass: 'text-bg-success',
      details: rows.map((row) => String(row.instance.name || row.instance.id || '-') + ': ready')
    };
  }
  return {
    label: 'Nicht geprüft',
    badgeClass: 'text-bg-secondary',
    details: rows.map((row) => String(row.instance.name || row.instance.id || '-') + ': ' + row.status)
  };
}

function renderProjectWizardMeta(project) {
  const meta = document.getElementById('prj-wizard-meta');
  if (!meta) {
    return;
  }
  if (!project) {
    meta.textContent = 'Neues Projekt · noch nicht gespeichert';
    return;
  }
  meta.textContent = 'Projekt: ' + String(project.name || project.id || '-') + ' · Aktualisiert: ' + String(project.updatedAt || '-');
}

function updateProjectInstanceSummary() {
  const summary = document.getElementById('prj-instance-summary');
  if (!summary) {
    return;
  }
  const testId = String(document.getElementById('prj-test-instance-id')?.value || '').trim();
  const productionId = String(document.getElementById('prj-production-instance-id')?.value || '').trim();
  const testInstance = (state.instances || []).find((item) => String(item.id || '') === testId);
  const productionInstance = (state.instances || []).find((item) => String(item.id || '') === productionId);
  summary.textContent = 'Test: ' + String(testInstance?.name || '-') + ' · Produktion: ' + String(productionInstance?.name || '-');
}

function renderProjectReview() {
  const review = document.getElementById('prj-review');
  if (!review) {
    return;
  }
  const name = String(document.getElementById('prj-name')?.value || '').trim() || '-';
  const projectId = String(document.getElementById('prj-id')?.value || state.editingProjectId || '').trim() || 'automatisch';
  const testId = String(document.getElementById('prj-test-instance-id')?.value || '').trim();
  const productionId = String(document.getElementById('prj-production-instance-id')?.value || '').trim();
  const testInstance = (state.instances || []).find((item) => String(item.id || '') === testId);
  const productionInstance = (state.instances || []).find((item) => String(item.id || '') === productionId);
  const cacheLabel = document.getElementById('prj-lookup-cache-enabled')?.checked ? 'aktiv' : 'aus';
  const logLabel = document.getElementById('prj-log-batching-enabled')?.checked ? 'Batching' : 'direkt';
  const writeProtection = document.getElementById('prj-production-write-protection')?.checked ? 'aktiv' : 'inaktiv';
  const confluence = [
    String(document.getElementById('prj-confluence-base-url')?.value || '').trim(),
    String(document.getElementById('prj-confluence-username')?.value || '').trim(),
    String(document.getElementById('prj-confluence-space-key')?.value || '').trim(),
    String(document.getElementById('prj-confluence-parent-page-id')?.value || '').trim(),
    String(document.getElementById('prj-confluence-title-prefix')?.value || '').trim()
  ].filter(Boolean).join(' · ') || '-';

  review.innerHTML = [
    ['Projekt', name],
    ['Projekt-ID', projectId],
    ['Test-Instanz', testInstance ? String(testInstance.name || testInstance.id) : '-'],
    ['Produktions-Instanz', productionInstance ? String(productionInstance.name || productionInstance.id) : '-'],
    ['Produktionsschutz', writeProtection],
    ['Lookup-Cache', cacheLabel + ' · TTL ' + String(document.getElementById('prj-lookup-cache-ttl-minutes')?.value || '15') + 'm'],
    ['Logs', logLabel + ' · Sync ' + String(document.getElementById('prj-log-sync-interval-minutes')?.value || '5') + 'm'],
    ['Confluence', confluence]
  ].map((item) => '<div class="project-review-item"><div class="small text-secondary">' + esc(item[0]) + '</div><div class="fw-semibold">' + esc(item[1]) + '</div></div>').join('');
}

function getProjectPrimaryInstanceId(projectId, preferredRole) {
  const instances = (state.instances || []).filter((entry) => String(entry.projectId || '').trim() === String(projectId || '').trim());
  const role = String(preferredRole || '').trim();
  if (role) {
    const byRole = instances.find((entry) => String(entry.role || 'test') === role);
    if (byRole) {
      return String(byRole.id || '').trim();
    }
  }
  return String((instances[0] && instances[0].id) || state.instanceId || '').trim();
}

function setProjectOperationResult(projectId, result) {
  const key = String(projectId || '').trim();
  if (!key) {
    return;
  }
  state.projectOperationResults[key] = {
    ...(result || {}),
    updatedAt: new Date().toISOString()
  };
  renderProjectTable();
}

function renderProjectOperationResult(projectId) {
  const result = state.projectOperationResults[String(projectId || '').trim()];
  if (!result) {
    return '';
  }
  const status = String(result.status || 'info');
  const badgeClass = status === 'success' ? 'text-bg-success' : status === 'error' ? 'text-bg-danger' : status === 'warning' ? 'text-bg-warning' : 'text-bg-info';
  const details = Array.isArray(result.details) ? result.details : [];
  return '<div class="project-operation-result">' +
    '<div class="d-flex align-items-center gap-2 flex-wrap">' +
      '<span class="badge ' + badgeClass + '">' + esc(status) + '</span>' +
      '<strong>' + esc(String(result.title || 'Projektaktion')) + '</strong>' +
      '<span class="small text-secondary">' + esc(String(result.updatedAt || '')) + '</span>' +
    '</div>' +
    '<div class="small text-secondary mt-1">' + esc(String(result.message || '-')) + '</div>' +
    (details.length ? '<ul class="project-operation-details">' + details.slice(0, 8).map((item) => '<li>' + esc(String(item || '')) + '</li>').join('') + '</ul>' : '') +
  '</div>';
}

function describeRolloutSnapshot(snapshot) {
  const decision = String(snapshot?.decision?.status || 'unknown');
  const unavailable = Array.isArray(snapshot?.notes?.unavailableKpis) ? snapshot.notes.unavailableKpis : [];
  return 'Entscheidung: ' + decision + (unavailable.length ? ' · Fehlende KPIs: ' + unavailable.join(', ') : '');
}

function summarizeCompareRun(run) {
  const summary = run?.summary || {};
  return 'Critical: ' + String(summary.critical || 0) + ' · Warning: ' + String(summary.warning || 0) + ' · Info: ' + String(summary.info || 0);
}

function summarizePrecheckRun(run) {
  const checks = Array.isArray(run?.checks) ? run.checks : [];
  const failed = checks.filter((item) => item.status === 'failed').length;
  return String(run?.status || 'unknown') + ' · Checks: ' + String(checks.length) + ' · Fehler: ' + String(failed);
}

function formatProjectSetupVersionLabel(version) {
  const date = version?.createdAt ? new Date(String(version.createdAt)) : null;
  const dateLabel = date && Number.isFinite(date.getTime())
    ? date.toLocaleString('de-DE', { dateStyle: 'short', timeStyle: 'short' })
    : String(version?.createdAt || '-');
  const author = String(version?.author || 'unbekannt').trim() || 'unbekannt';
  return 'v' + String(version?.version || '-') + ' · ' + dateLabel + ' · ' + author;
}

function formatProjectVersionShort(version) {
  if (!version) {
    return '-';
  }
  return 'v' + String(version.version || '-') + ' · ' + String(version.author || 'unbekannt');
}

function formatProjectDateTime(value) {
  const date = value ? new Date(String(value)) : null;
  return date && Number.isFinite(date.getTime())
    ? date.toLocaleString('de-DE', { dateStyle: 'short', timeStyle: 'short' })
    : '-';
}

function renderProjectVersionOptions(projectId) {
  const versions = Array.isArray(state.projectSetupVersions?.[projectId]) ? state.projectSetupVersions[projectId] : [];
  const summary = state.projectSummaries?.[projectId] || {};
  const currentTestVersionId = String(summary?.testVersion?.id || '').trim();
  if (!versions.length) {
    return '<option value="">Keine Version vorhanden</option>';
  }
  return versions
    .slice()
    .sort((a, b) => Number(b.version || 0) - Number(a.version || 0))
    .map((version, index) =>
      '<option value="' + esc(String(version.id || '')) + '"' + ((currentTestVersionId ? String(version.id || '') === currentTestVersionId : index === 0) ? ' selected' : '') + '>' + esc(formatProjectSetupVersionLabel(version)) + '</option>'
    )
    .join('');
}

function renderProjectVersionDiffBadge(summary) {
  const hasTest = !!summary?.testVersion;
  const hasProduction = !!summary?.productionVersion;
  if (hasTest && hasProduction && summary.versionsDiffer !== true) {
    return '<span class="project-version-state project-version-state-ok">Test = Produktion</span>';
  }
  if (hasTest && hasProduction) {
    return '<span class="project-version-state project-version-state-diff">Test ≠ Produktion</span>';
  }
  return '<span class="project-version-state project-version-state-missing">Prod-Version offen</span>';
}

function renderProjectInsightCards(projectId, kpis, summary) {
  const connectorCount = summary && summary.connectorCount !== null && summary.connectorCount !== undefined ? String(summary.connectorCount) : '-';
  const scheduleCount = summary && summary.scheduleCount !== null && summary.scheduleCount !== undefined ? String(summary.scheduleCount) : '-';
  const versionCount = summary ? String(summary.versionCount || 0) : '-';
  const latestVersion = summary?.latestVersion ? 'v' + String(summary.latestVersion.version || '-') : '-';
  const testVersion = formatProjectVersionShort(summary?.testVersion);
  const productionVersion = formatProjectVersionShort(summary?.productionVersion);
  const lastDeployment = summary?.lastDeployment
    ? String(summary.lastDeployment.status || '-') + ' · ' + formatProjectDateTime(summary.lastDeployment.startedAt)
    : 'Noch kein Deployment';
  const deploymentItems = Array.isArray(summary?.lastDeployment?.deployItems) && summary.lastDeployment.deployItems.length
    ? summary.lastDeployment.deployItems.join(', ')
    : '-';

  return '<div class="project-kpi-grid project-insight-grid mt-3">' +
    '<div class="project-kpi project-kpi-accent-blue"><span>Connectoren</span><strong>' + esc(connectorCount) + '</strong><small>verfügbar im aktuellen Setup</small></div>' +
    '<div class="project-kpi project-kpi-accent-cyan"><span>Scheduler</span><strong>' + esc(scheduleCount) + '</strong><small>verfügbar im aktuellen Setup</small></div>' +
    '<div class="project-kpi project-kpi-accent-violet"><span>Versionen</span><strong>' + esc(versionCount) + '</strong><small>Aktuell: ' + esc(latestVersion) + '</small></div>' +
    '<div class="project-kpi project-kpi-accent-green"><span>Testversion</span><strong>' + esc(testVersion) + '</strong><small>Auswahl kann etabliert werden</small></div>' +
    '<div class="project-kpi project-kpi-accent-amber"><span>Test/Prod</span><strong>' + renderProjectVersionDiffBadge(summary) + '</strong><small>Prod: ' + esc(productionVersion) + '</small></div>' +
    '<div class="project-kpi project-kpi-accent-slate"><span>Letztes Deployment</span><strong>' + esc(lastDeployment) + '</strong><small>' + esc(deploymentItems) + '</small></div>' +
  '</div>';
}

async function loadProjectSummary(projectId, options = {}) {
  const normalizedProjectId = String(projectId || '').trim();
  if (!normalizedProjectId) {
    return null;
  }
  const force = options.force === true;
  if (!force && state.projectSummaries?.[normalizedProjectId]) {
    return state.projectSummaries[normalizedProjectId];
  }
  const summary = await safeRequest('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/summary', null);
  if (!summary) {
    return null;
  }
  state.projectSummaries = {
    ...(state.projectSummaries || {}),
    [normalizedProjectId]: summary
  };
  const select = document.querySelector('[data-project-version-select="' + normalizedProjectId.replace(/"/g, '\"') + '"]');
  if (select) {
    select.innerHTML = renderProjectVersionOptions(normalizedProjectId);
  }
  renderProjectTable();
  return summary;
}

async function loadProjectSetupVersions(projectId, options = {}) {
  const normalizedProjectId = String(projectId || '').trim();
  if (!normalizedProjectId) {
    return [];
  }
  const force = options.force === true;
  if (!force && Array.isArray(state.projectSetupVersions?.[normalizedProjectId])) {
    return state.projectSetupVersions[normalizedProjectId];
  }
  const response = await safeRequest('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/setup/versions', { items: [] });
  const items = Array.isArray(response.items) ? response.items : [];
  state.projectSetupVersions = {
    ...(state.projectSetupVersions || {}),
    [normalizedProjectId]: items
  };
  const select = document.querySelector('[data-project-version-select="' + normalizedProjectId.replace(/"/g, '\"') + '"]');
  if (select) {
    select.innerHTML = renderProjectVersionOptions(normalizedProjectId);
  }
  return items;
}

function getSelectedProjectVersionId(projectId) {
  const select = document.querySelector('[data-project-version-select="' + String(projectId || '').replace(/"/g, '\"') + '"]');
  return String(select?.value || '').trim();
}

function getSelectedProjectDeployItems(projectId) {
  return Array.from(document.querySelectorAll('[data-project-deploy-item][data-project-id="' + String(projectId || '').replace(/"/g, '\"') + '"]'))
    .filter((input) => input.checked)
    .map((input) => String(input.value || '').trim())
    .filter(Boolean);
}

function getProjectSetupVersionNote(projectId) {
  const textarea = document.querySelector('[data-project-setup-note="' + String(projectId || '').replace(/"/g, '\"') + '"]');
  return String(textarea?.value || '').trim();
}

function setProjectSetupVersionNote(projectId, note) {
  const textarea = document.querySelector('[data-project-setup-note="' + String(projectId || '').replace(/"/g, '\"') + '"]');
  if (textarea) {
    textarea.value = String(note || '');
  }
}

async function establishProjectTestVersion(projectId) {
  const normalizedProjectId = String(projectId || '').trim();
  const selectedVersionId = getSelectedProjectVersionId(normalizedProjectId);
  const versions = Array.isArray(state.projectSetupVersions?.[normalizedProjectId]) ? state.projectSetupVersions[normalizedProjectId] : [];
  const version = versions.find((item) => String(item.id || '') === selectedVersionId);
  if (!normalizedProjectId || !version) {
    setProjectOperationResult(normalizedProjectId, {
      status: 'warning',
      title: 'Testversion nicht gesetzt',
      message: 'Bitte zuerst eine Setup-Version auswählen.'
    });
    return;
  }
  const label = formatProjectSetupVersionLabel(version);
  if (!window.confirm('Diese Setup-Version als aktuelle Testversion etablieren?\n\n' + label)) {
    return;
  }
  setProjectOperationResult(normalizedProjectId, { status: 'info', title: 'Testversion wird gesetzt', message: label });
  const result = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/setup/current-version', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ targetEnv: 'test', versionId: selectedVersionId })
  });
  if (result?.summary) {
    state.projectSummaries = {
      ...(state.projectSummaries || {}),
      [normalizedProjectId]: result.summary
    };
  } else {
    await loadProjectSummary(normalizedProjectId, { force: true });
  }
  if (result?.targetInstance?.id) {
    state.instanceId = String(result.targetInstance.id || '').trim();
    state.headerProjectId = normalizedProjectId;
    state.headerTargetEnv = 'test';
    const instanceSelect = document.getElementById('instance-select');
    if (instanceSelect) {
      instanceSelect.value = state.instanceId;
    }
    persistHeaderContext();
    renderContextSelectionSummary();
  }
  const importResult = result?.importResult || {};
  setProjectOperationResult(normalizedProjectId, {
    status: 'success',
    title: 'Testversion gesetzt',
    message: 'Aktuelle Testversion geladen: ' + formatProjectSetupVersionLabel(result?.version || version),
    details: [
      'Connectoren: ' + String(importResult.connectorsCreated || 0) + ' erstellt, ' + String(importResult.connectorsUpdated || 0) + ' aktualisiert',
      'Scheduler: ' + String(importResult.schedulesCreated || 0) + ' erstellt, ' + String(importResult.schedulesUpdated || 0) + ' aktualisiert'
    ]
  });
  await refresh({ refreshChart: false, includeGraph: false, includeSalesforceOverview: false, includeRecordsSummary: false });
}

async function runProjectOperation(projectId, operation) {
  const normalizedProjectId = String(projectId || '').trim();
  if (!normalizedProjectId) {
    return;
  }
  const selectedVersionId = getSelectedProjectVersionId(normalizedProjectId);
  const selectedDeployItems = getSelectedProjectDeployItems(normalizedProjectId);
  const setupVersionNote = getProjectSetupVersionNote(normalizedProjectId);
  setProjectOperationResult(normalizedProjectId, { status: 'info', title: 'Projektaktion läuft', message: operation });
  try {
    if (operation === 'establish-test-version') {
      await establishProjectTestVersion(normalizedProjectId);
      return;
    }

    if (operation === 'api-forecast') {
      const env = state.headerTargetEnv === 'production' ? 'production' : 'test';
      const snapshot = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/rollout/kpis?targetEnv=' + encodeURIComponent(env) + '&windowDays=14');
      const decisionStatus = String(snapshot?.decision?.status || 'unknown');
      setProjectOperationResult(normalizedProjectId, {
        status: decisionStatus === 'no-go' ? 'error' : decisionStatus === 'go' ? 'success' : decisionStatus === 'conditional-go' ? 'warning' : 'info',
        title: 'API-Prognose ' + env,
        message: describeRolloutSnapshot(snapshot),
        details: Array.isArray(snapshot?.notes?.unavailableKpis) ? snapshot.notes.unavailableKpis.map((item) => 'Nicht verfügbar: ' + item) : []
      });
      return;
    }

    if (operation === 'setup-version') {
      const instanceId = getProjectPrimaryInstanceId(normalizedProjectId, 'test') || getProjectPrimaryInstanceId(normalizedProjectId, 'production');
      const result = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/setup/versions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ instanceId, note: setupVersionNote, generateNote: !setupVersionNote })
      });
      setProjectOperationResult(normalizedProjectId, {
        status: 'success',
        title: 'Setup-Version erzeugt',
        message: 'Version ' + String(result?.record?.version || '-') + ' · Artefakt ' + String(result?.record?.artifactRef || '-'),
        details: result?.record?.note ? [String(result.record.note)] : []
      });
      await loadProjectSetupVersions(normalizedProjectId, { force: true });
      await loadProjectSummary(normalizedProjectId, { force: true });
      return;
    }

    if (operation === 'setup-note-suggest') {
      const instanceId = getProjectPrimaryInstanceId(normalizedProjectId, 'test') || getProjectPrimaryInstanceId(normalizedProjectId, 'production');
      const result = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/setup/version-note-suggestion', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ instanceId })
      });
      setProjectOperationResult(normalizedProjectId, {
        status: 'success',
        title: 'KI-Vorschlag erzeugt',
        message: String(result?.note || 'Kein Vorschlag erzeugt.')
      });
      setProjectSetupVersionNote(normalizedProjectId, result?.note || '');
      return;
    }

    if (operation === 'compare-test-production' || operation === 'compare-production-test') {
      const direction = operation === 'compare-production-test' ? 'production-to-test' : 'test-to-production';
      const run = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/deploy/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ direction })
      });
      setProjectOperationResult(normalizedProjectId, {
        status: (run?.summary?.critical || 0) > 0 ? 'error' : (run?.summary?.warning || 0) > 0 ? 'warning' : 'success',
        title: 'Abgleich ' + direction,
        message: summarizeCompareRun(run),
        details: (Array.isArray(run?.diffs) ? run.diffs : []).map((item) => String(item.severity || '-') + ': ' + String(item.message || item.code || '-'))
      });
      return;
    }

    if (operation === 'precheck-production' || operation === 'precheck-test') {
      const targetEnv = operation === 'precheck-test' ? 'test' : 'production';
      const run = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/deploy/precheck', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targetEnv })
      });
      setProjectOperationResult(normalizedProjectId, {
        status: run?.status === 'passed' ? 'success' : 'error',
        title: 'preDeployment ' + targetEnv,
        message: summarizePrecheckRun(run),
        details: (Array.isArray(run?.checks) ? run.checks : []).map((item) => String(item.group || '-') + ': ' + String(item.status || '-') + ' - ' + String(item.message || '-'))
      });
      return;
    }

    if (operation === 'deploy-start') {
      const result = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/deploy/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourceVersionId: selectedVersionId || undefined, deployItems: selectedDeployItems })
      });
      setProjectOperationResult(normalizedProjectId, {
        status: 'success',
        title: 'Deployment gestartet',
        message: 'Run ' + String(result?.deploymentRunId || '-') + ' · Version ' + String(result?.sourceVersionId || '-') + ' · Bestandteile: ' + (Array.isArray(result?.deployItems) ? result.deployItems.join(', ') : '-'),
        details: ['Compare ' + String(result?.compareRunId || '-'), 'Precheck ' + String(result?.precheckRunId || '-')]
      });
      await loadProjectSummary(normalizedProjectId, { force: true });
      return;
    }

    if (operation === 'publish-confluence') {
      const instanceId = getProjectPrimaryInstanceId(normalizedProjectId, state.headerTargetEnv || 'test');
      const result = await requestJson('/api/admin/projects/' + encodeURIComponent(normalizedProjectId) + '/documentation/publish-confluence', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ instanceId: instanceId || undefined })
      });
      const publishResult = result?.publishResult || {};
      const missingConfig = Array.isArray(publishResult.missingConfig) ? publishResult.missingConfig : [];
      setProjectOperationResult(normalizedProjectId, {
        status: publishResult.published ? 'success' : (publishResult.error ? 'error' : 'warning'),
        title: 'Confluence-Dokumentation',
        message: publishResult.published
          ? 'Veröffentlicht: ' + String(publishResult.url || publishResult.pageId || '-')
          : publishResult.error
            ? 'Confluence-Publikation fehlgeschlagen: ' + String(publishResult.error)
            : 'Dry-Run: Confluence ist nicht vollständig konfiguriert. Dokumentation wurde vorbereitet.',
        details: (result?.html ? ['Dokumentationsumfang: ' + String(result.html.length) + ' HTML-Zeichen'] : [])
          .concat(missingConfig.length ? ['Fehlende Konfiguration: ' + missingConfig.map(String).join(', ')] : [])
      });
    }
  } catch (error) {
    setProjectOperationResult(normalizedProjectId, {
      status: 'error',
      title: 'Projektaktion fehlgeschlagen',
      message: error.message || String(error || 'Unbekannter Fehler')
    });
  }
}

function goToProjectWizardStep(step) {
  const nextStep = Math.max(1, Math.min(5, Number(step || 1) || 1));
  state.projectWizardStep = nextStep;
  document.querySelectorAll('#prj-wizard-steps [data-prj-step]').forEach((button) => {
    const current = Number(button.getAttribute('data-prj-step') || '1') || 1;
    button.classList.toggle('is-active', current === nextStep);
    button.classList.toggle('is-complete', current < nextStep);
  });
  document.querySelectorAll('[data-prj-step-panel]').forEach((panel) => {
    panel.classList.toggle('d-none', Number(panel.getAttribute('data-prj-step-panel') || '1') !== nextStep);
  });
  const back = document.getElementById('prj-wizard-back');
  const next = document.getElementById('prj-wizard-next');
  if (back) {
    back.disabled = nextStep <= 1;
  }
  if (next) {
    next.textContent = nextStep >= 5 ? 'Speichern' : 'Weiter';
  }
  updateProjectInstanceSummary();
  renderProjectReview();
}

function validateProjectWizardStep(step) {
  const currentStep = Number(step || state.projectWizardStep || 1) || 1;
  if (currentStep === 1) {
    const name = String(document.getElementById('prj-name')?.value || '').trim();
    if (!name) {
      throw new Error('Projektname ist erforderlich.');
    }
  }
  if (currentStep === 2) {
    const testInstanceId = String(document.getElementById('prj-test-instance-id')?.value || '').trim();
    const productionInstanceId = String(document.getElementById('prj-production-instance-id')?.value || '').trim();
    if (testInstanceId && productionInstanceId && testInstanceId === productionInstanceId) {
      throw new Error('Test- und Produktionsinstanz muessen unterschiedlich sein.');
    }
  }
}

async function advanceProjectWizardStep() {
  try {
    clearProjectModalError();
    validateProjectWizardStep(state.projectWizardStep);
    if (state.projectWizardStep >= 5) {
      await saveProject();
      return;
    }
    goToProjectWizardStep(state.projectWizardStep + 1);
  } catch (error) {
    showProjectModalError(error.message || 'Projekt-Schritt konnte nicht abgeschlossen werden');
  }
}

function fillProjectForm(project) {
  state.editingProjectId = String(project?.id || '').trim();
  document.getElementById('prj-id').value = String(project?.id || '');
  document.getElementById('prj-name').value = String(project?.name || '');
  document.getElementById('prj-description').value = String(project?.description || '');
  document.getElementById('prj-confluence-base-url').value = String(project?.confluenceBaseUrl || '');
  document.getElementById('prj-confluence-username').value = String(project?.confluenceUsername || '');
  document.getElementById('prj-confluence-api-token').value = '';
  document.getElementById('prj-confluence-space-key').value = String(project?.confluenceSpaceKey || '');
  document.getElementById('prj-confluence-parent-page-id').value = String(project?.confluenceParentPageId || '');
  document.getElementById('prj-confluence-title-prefix').value = String(project?.confluencePageTitlePrefix || '');
  document.getElementById('prj-confluence-token-status').textContent = project?.confluenceApiTokenConfigured
    ? 'API Token ist hinterlegt. Leer lassen, um ihn beizubehalten.'
    : 'Noch kein Projekttoken hinterlegt.';
  document.getElementById('prj-lookup-cache-enabled').checked = project?.lookupCacheEnabled !== false;
  document.getElementById('prj-lookup-cache-ttl-minutes').value = String(project?.lookupCacheTtlMinutes || 15);
  document.getElementById('prj-log-batching-enabled').checked = project?.logBatchingEnabled !== false;
  document.getElementById('prj-log-sync-interval-minutes').value = String(project?.logSyncIntervalMinutes || 5);
  document.getElementById('prj-log-batch-size').value = String(project?.logBatchSize || 200);
  document.getElementById('prj-log-buffer-max-entries').value = String(project?.logBufferMaxEntries || 10000);
  document.getElementById('prj-production-write-protection').checked = project?.productionWriteProtection !== false;

  const projectId = String(project?.id || '').trim();
  populateProjectInstanceSelectors(projectId);
  const projectInstances = (state.instances || []).filter((entry) => String(entry.projectId || '').trim() === projectId);
  const testInstance = projectInstances.find((entry) => entry.role !== 'production');
  const productionInstance = projectInstances.find((entry) => entry.role === 'production');
  document.getElementById('prj-test-instance-id').value = String(testInstance?.id || '');
  document.getElementById('prj-production-instance-id').value = String(productionInstance?.id || '');
  renderProjectWizardMeta(project || null);
  renderProjectReview();
}

function openProjectWizard(project) {
  clearError();
  clearProjectModalError();
  if (project) {
    fillProjectForm(project);
  } else {
    resetProjectForm();
  }
  goToProjectWizardStep(1);
  const modalEl = document.getElementById('project-modal');
  if (modalEl && window.bootstrap?.Modal) {
    window.bootstrap.Modal.getOrCreateInstance(modalEl).show();
  }
}

function renderProjectTable() {
  const panelList = document.getElementById('project-table-body');
  if (!panelList) {
    return;
  }
  const summary = document.getElementById('projects-summary');
  const filterValue = String(document.getElementById('projects-filter')?.value || '').trim().toLowerCase();
  const visibleProjects = filterValue
    ? state.projects.filter((item) => [
      item.name,
      item.id,
      item.description
    ].map((value) => String(value || '').toLowerCase()).some((value) => value.includes(filterValue)))
    : state.projects;

  if (summary) {
    const activeCount = (state.projects || []).filter((item) => item.archived !== true).length;
    const instanceCount = (state.instances || []).length;
    summary.textContent = String(state.projects.length) + ' Projekte, ' + String(activeCount) + ' aktiv, ' + String(instanceCount) + ' Instanzen';
  }

  if (!state.projects.length) {
    panelList.innerHTML = '<div class="text-secondary small">Keine Projekte vorhanden.</div>';
    return;
  }
  if (!visibleProjects.length) {
    panelList.innerHTML = '<div class="text-secondary small">Keine Projekte passend zur Suche.</div>';
    return;
  }

  panelList.innerHTML = visibleProjects.map((item) => {
    const archived = item.archived === true;
    const status = archived
      ? '<span class="badge text-bg-secondary">Archiviert</span>'
      : '<span class="badge text-bg-primary">Aktiv</span>';
    const isDefault = String(item.id || '') === 'default-project';
    const projectId = String(item.id || '');
    const isActiveProject = projectId === String(state.headerProjectId || '').trim();
    const kpis = getProjectKpis(item);
    const health = getProjectHealthSummary(kpis);
    const projectSummary = state.projectSummaries?.[projectId] || null;
    const testLabel = kpis.testInstance ? String(kpis.testInstance.name || kpis.testInstance.id || '-') : '-';
    const productionLabel = kpis.productionInstance ? String(kpis.productionInstance.name || kpis.productionInstance.id || '-') : '-';
    const description = String(item.description || '').trim();
    const currentVersionLabel = projectSummary?.testVersion
      ? 'v' + String(projectSummary.testVersion.version || '-')
      : projectSummary?.latestVersion
        ? 'v' + String(projectSummary.latestVersion.version || '-')
        : '-';
    const updatedAtLabel = formatProjectDateTime(item.updatedAt);
    return '<section class="project-panel' + (isActiveProject ? ' project-panel-active' : '') + '">' +
      '<div class="project-panel-main">' +
        '<div class="project-panel-compact">' +
          '<div class="project-panel-identity">' +
            '<div class="d-flex align-items-center gap-2 flex-wrap">' +
              (isActiveProject ? '<span class="badge text-bg-success">Aktives Projekt</span>' : '') +
              status +
              '<div class="fw-semibold project-panel-title">' + esc(String(item.name || item.id)) + '</div>' +
            '</div>' +
            '<div class="project-panel-description">' + esc(description || String(item.id || '')) + '</div>' +
          '</div>' +
          '<div class="project-compact-kpis">' +
            '<div class="project-compact-kpi"><span>Letzte Änderung</span><strong>' + esc(updatedAtLabel) + '</strong></div>' +
            '<div class="project-compact-kpi"><span>Aktuelle Version</span><strong>' + esc(currentVersionLabel) + '</strong></div>' +
            '<div class="project-compact-kpi"><span>Test/Prod</span><strong>' + renderProjectVersionDiffBadge(projectSummary) + '</strong></div>' +
            '<div class="project-compact-kpi"><span>Health</span><strong><span class="badge ' + health.badgeClass + '">' + esc(health.label) + '</span></strong></div>' +
          '</div>' +
        '</div>' +
        '<details class="project-operations-details project-panel-details mt-2">' +
          '<summary>Weitere Informationen und Aktionen</summary>' +
          '<div class="project-context-strip mt-3">' +
            '<span><strong>Instanzen</strong> ' + esc(String(kpis.projectInstances.length)) + '</span>' +
            '<span><strong>Test</strong> ' + esc(testLabel) + '</span>' +
            '<span><strong>Produktion</strong> ' + esc(productionLabel) + '</span>' +
            '<span><strong>Health</strong> ' + esc(health.details.join(' · ')) + '</span>' +
            '<span><strong>Produktionsschutz</strong> ' + esc(item.productionWriteProtection === false ? 'inaktiv' : 'aktiv') + '</span>' +
            '<span><strong>Cache</strong> ' + esc(kpis.cacheEnabled ? 'an' : 'aus') + '</span>' +
            '<span><strong>Logs</strong> ' + esc(kpis.logBatchingEnabled ? 'Batch' : 'direkt') + '</span>' +
          '</div>' +
          renderProjectInsightCards(projectId, kpis, projectSummary) +
          '<div class="project-deployment-config mt-2">' +
            '<div class="row g-2 align-items-end">' +
              '<div class="col-lg-5">' +
                '<label class="form-label form-label-sm mb-1">Setup-Version</label>' +
                '<div class="input-group input-group-sm">' +
                  '<select class="form-select" data-project-version-select="' + esc(projectId) + '">' + renderProjectVersionOptions(projectId) + '</select>' +
                  '<button type="button" class="btn btn-outline-success" data-project-op="establish-test-version" data-project-id="' + esc(projectId) + '">Als Testversion setzen</button>' +
                '</div>' +
              '</div>' +
              '<div class="col-lg-7">' +
                '<label class="form-label form-label-sm mb-1">Deployment-Bestandteile</label>' +
                '<div class="project-deploy-item-list">' +
                  '<label><input class="form-check-input me-1" type="checkbox" value="project" data-project-deploy-item data-project-id="' + esc(projectId) + '" checked />Projekt</label>' +
                  '<label><input class="form-check-input me-1" type="checkbox" value="connectors" data-project-deploy-item data-project-id="' + esc(projectId) + '" checked />Connectoren</label>' +
                  '<label><input class="form-check-input me-1" type="checkbox" value="schedules" data-project-deploy-item data-project-id="' + esc(projectId) + '" checked />Scheduler</label>' +
                  '<label><input class="form-check-input me-1" type="checkbox" value="migrations" data-project-deploy-item data-project-id="' + esc(projectId) + '" />Migrationen</label>' +
                  '<label><input class="form-check-input me-1" type="checkbox" value="documentation" data-project-deploy-item data-project-id="' + esc(projectId) + '" />Doku</label>' +
                '</div>' +
              '</div>' +
              '<div class="col-12">' +
                '<div class="d-flex justify-content-between align-items-center gap-2 mb-1 flex-wrap">' +
                  '<label class="form-label form-label-sm mb-0">Beschreibung fuer neue Setup-Version</label>' +
                  '<button type="button" class="btn btn-sm btn-outline-secondary" data-project-op="setup-note-suggest" data-project-id="' + esc(projectId) + '">KI-Vorschlag</button>' +
                '</div>' +
                '<textarea class="form-control form-control-sm project-setup-note" rows="2" data-project-setup-note="' + esc(projectId) + '" placeholder="Aenderungen dieser Version dokumentieren, z. B. neue Scheduler, Mapping-Anpassungen oder Connector-Updates"></textarea>' +
              '</div>' +
            '</div>' +
          '</div>' +
          '<div class="project-operation-toolbar mt-3">' +
            '<button type="button" class="btn btn-sm btn-outline-secondary" data-project-op="api-forecast" data-project-id="' + esc(projectId) + '">API-Prognose</button>' +
            '<button type="button" class="btn btn-sm btn-outline-secondary" data-project-op="setup-version" data-project-id="' + esc(projectId) + '">Setup-Version</button>' +
            '<button type="button" class="btn btn-sm btn-outline-primary" data-project-op="compare-test-production" data-project-id="' + esc(projectId) + '">Abgleich Test → Prod</button>' +
            '<button type="button" class="btn btn-sm btn-outline-primary" data-project-op="compare-production-test" data-project-id="' + esc(projectId) + '">Abgleich Prod → Test</button>' +
            '<button type="button" class="btn btn-sm btn-outline-warning" data-project-op="precheck-test" data-project-id="' + esc(projectId) + '">preDeployment Test</button>' +
            '<button type="button" class="btn btn-sm btn-outline-warning" data-project-op="precheck-production" data-project-id="' + esc(projectId) + '">preDeployment Prod</button>' +
            '<button type="button" class="btn btn-sm btn-outline-success" data-project-op="deploy-start" data-project-id="' + esc(projectId) + '">Deploy starten</button>' +
            '<button type="button" class="btn btn-sm btn-outline-info" data-project-op="publish-confluence" data-project-id="' + esc(projectId) + '">Doku Confluence</button>' +
          '</div>' +
        '</details>' +
        renderProjectOperationResult(projectId) +
      '</div>' +
      '<div class="project-panel-actions">' +
        '<button type="button" class="btn btn-sm btn-outline-primary" data-project-edit="' + esc(projectId) + '">Bearbeiten</button>' +
        '<button type="button" class="btn btn-sm btn-outline-warning" data-project-archive="' + esc(projectId) + '" data-project-archived="' + (archived ? '1' : '0') + '"' + (isDefault ? ' disabled' : '') + '>' + (archived ? 'Aktivieren' : 'Archivieren') + '</button>' +
        '<button type="button" class="btn btn-sm btn-outline-danger" data-project-delete="' + esc(projectId) + '"' + (isDefault ? ' disabled' : '') + '>Löschen</button>' +
      '</div>' +
    '</section>';
  }).join('');

  visibleProjects.forEach((item) => {
    const projectId = String(item.id || '');
    void loadProjectSetupVersions(projectId);
    void loadProjectSummary(projectId);
  });

  panelList.querySelectorAll('[data-project-edit]').forEach((button) => {
    button.addEventListener('click', () => {
      const projectId = String(button.getAttribute('data-project-edit') || '').trim();
      const project = state.projects.find((item) => String(item.id || '').trim() === projectId);
      if (!project) {
        return;
      }

      openProjectWizard(project);
    });
  });

  panelList.querySelectorAll('[data-project-op]').forEach((button) => {
    button.addEventListener('click', async () => {
      const projectId = String(button.getAttribute('data-project-id') || '').trim();
      const operation = String(button.getAttribute('data-project-op') || '').trim();
      await runProjectOperation(projectId, operation);
    });
  });

  panelList.querySelectorAll('[data-project-archive]').forEach((button) => {
    button.addEventListener('click', async () => {
      const projectId = String(button.getAttribute('data-project-archive') || '').trim();
      const archived = String(button.getAttribute('data-project-archived') || '') === '1';
      const nextArchived = !archived;
      const question = nextArchived
        ? 'Projekt wirklich archivieren?'
        : 'Projekt wirklich reaktivieren?';

      if (!window.confirm(question)) {
        return;
      }

      try {
        await requestJson('/api/projects/' + encodeURIComponent(projectId) + '/archive', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ archived: nextArchived })
        });
        await loadProjects();
        await loadInstances();
      } catch (error) {
        showError(error.message || 'Projektstatus konnte nicht geändert werden');
      }
    });
  });

  panelList.querySelectorAll('[data-project-delete]').forEach((button) => {
    button.addEventListener('click', async () => {
      const projectId = String(button.getAttribute('data-project-delete') || '').trim();
      if (!window.confirm('Projekt wirklich löschen?')) {
        return;
      }

      try {
        await requestJson('/api/projects/' + encodeURIComponent(projectId), {
          method: 'DELETE'
        });
        await loadProjects();
        await loadInstances();
        if (state.editingProjectId === projectId) {
          resetProjectForm();
        }
      } catch (error) {
        showError(error.message || 'Projekt konnte nicht gelöscht werden');
      }
    });
  });
}

async function saveProject() {
  clearError();
  clearProjectModalError();

  const id = String(document.getElementById('prj-id').value || '').trim();
  const name = String(document.getElementById('prj-name').value || '').trim();
  const description = String(document.getElementById('prj-description').value || '').trim();
  const confluenceBaseUrl = String(document.getElementById('prj-confluence-base-url').value || '').trim();
  const confluenceUsername = String(document.getElementById('prj-confluence-username').value || '').trim();
  const confluenceApiToken = String(document.getElementById('prj-confluence-api-token').value || '').trim();
  const confluenceSpaceKey = String(document.getElementById('prj-confluence-space-key').value || '').trim();
  let confluenceParentPageId = String(document.getElementById('prj-confluence-parent-page-id').value || '').trim();
  // If user pasted a full Confluence page URL, try to extract the numeric page ID
  try {
    const urlMatch = confluenceParentPageId.match(/(?:pages\/|pageId=)([0-9]+)/i);
    if (urlMatch && urlMatch[1]) {
      confluenceParentPageId = urlMatch[1];
    }
  } catch (e) {
    // ignore and keep original value
  }
  const confluencePageTitlePrefix = String(document.getElementById('prj-confluence-title-prefix').value || '').trim();
  const lookupCacheEnabled = document.getElementById('prj-lookup-cache-enabled').checked;
  const lookupCacheTtlMinutes = Math.max(1, Number(document.getElementById('prj-lookup-cache-ttl-minutes').value || 15) || 15);
  const logBatchingEnabled = document.getElementById('prj-log-batching-enabled').checked;
  const logSyncIntervalMinutes = Math.max(1, Number(document.getElementById('prj-log-sync-interval-minutes').value || 5) || 5);
  const logBatchSize = Math.max(1, Number(document.getElementById('prj-log-batch-size').value || 200) || 200);
  const logBufferMaxEntries = Math.max(100, Number(document.getElementById('prj-log-buffer-max-entries').value || 10000) || 10000);
  const productionWriteProtection = document.getElementById('prj-production-write-protection').checked;
  const testInstanceId = String(document.getElementById('prj-test-instance-id').value || '').trim();
  const productionInstanceId = String(document.getElementById('prj-production-instance-id').value || '').trim();

  if (!name) {
    showProjectModalError('Projektname ist erforderlich');
    return;
  }

  if (testInstanceId && productionInstanceId && testInstanceId === productionInstanceId) {
    showProjectModalError('Test- und Produktionsinstanz müssen unterschiedlich sein.');
    return;
  }

  const payload = {
    id: id || (state.editingProjectId || undefined),
    name,
    description: description || undefined,
    archived: state.projects.find((item) => String(item.id || '') === String(id || state.editingProjectId || ''))?.archived === true,
    productionWriteProtection,
    lookupCacheEnabled,
    lookupCacheTtlMinutes,
    logBatchingEnabled,
    logSyncIntervalMinutes,
    logBatchSize,
    logBufferMaxEntries,
    confluenceBaseUrl: confluenceBaseUrl || undefined,
    confluenceUsername: confluenceUsername || undefined,
    confluenceApiToken: confluenceApiToken || undefined,
    confluenceSpaceKey: confluenceSpaceKey || undefined,
    confluenceParentPageId: confluenceParentPageId || undefined,
    confluencePageTitlePrefix: confluencePageTitlePrefix || undefined
  };

  const savedProject = await requestJson('/api/projects', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const projectId = String((savedProject && savedProject.id) || id || state.editingProjectId || '').trim();
  if (projectId) {
    const projectInstances = (state.instances || []).filter((entry) => String(entry.projectId || '').trim() === projectId);
    const currentProduction = projectInstances.find((entry) => entry.role === 'production');
    if (productionInstanceId && currentProduction && String(currentProduction.id || '') !== productionInstanceId) {
      await requestJson('/api/admin/sf-instances/' + encodeURIComponent(String(currentProduction.id || '')), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectId, role: 'test' })
      });
    }

    if (testInstanceId) {
      await requestJson('/api/admin/sf-instances/' + encodeURIComponent(testInstanceId), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectId, role: 'test' })
      });
    }

    if (productionInstanceId) {
      await requestJson('/api/admin/sf-instances/' + encodeURIComponent(productionInstanceId), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ projectId, role: 'production' })
      });
    }
  }

  await loadProjects();
  await loadInstances();
  resetProjectForm();
  const modalEl = document.getElementById('project-modal');
  if (modalEl && window.bootstrap?.Modal) {
    window.bootstrap.Modal.getOrCreateInstance(modalEl).hide();
  }
}

async function saveInstance() {
  clearError();
  clearInstanceModalError();
  try {
    const payload = {
      id: document.getElementById('ins-id').value,
      name: document.getElementById('ins-name').value || undefined,
      projectId: document.getElementById('ins-project-id').value || 'default-project',
      role: document.getElementById('ins-role').value === 'production' ? 'production' : 'test',
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

    state.instanceId = result.id;
    await loadInstances();
    renderInstanceReadinessSnapshot({ status: 'setup-required', instanceId: result.id, message: 'Instanz gespeichert. Readiness-Check läuft...' });
    await runInstanceReadinessCheckFromAdminForm(result.id);
    await refresh();
    const modalEl = document.getElementById('instance-modal');
    if (modalEl && window.bootstrap?.Modal) {
      window.bootstrap.Modal.getOrCreateInstance(modalEl).hide();
    }
    resetInstanceForm();
  } catch (error) {
    showInstanceModalError(error.message || 'Instanz konnte nicht gespeichert werden');
  }
}

