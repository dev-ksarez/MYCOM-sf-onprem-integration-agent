// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-init
// Source lines: 13794–14750
// ──────────────────────────────────────────────────────────────────────

const AUTO_REFRESH_INTERVAL_MS = 7000;

async function refresh(options = {}) {
  const shouldRefreshChart = options.refreshChart !== false;
  const includeGraph = options.includeGraph !== false;
  const includeSalesforceOverview = options.includeSalesforceOverview !== false;
  const includeScheduleOptions = options.includeScheduleOptions !== false;
  const includeRecordsSummary = options.includeRecordsSummary !== false;
  clearError();

  await loadAdminData();
  const healthData = await safeRequest('/api/system/health', {});
  const installerSummary = await safeRequest('/api/installer/summary', null);
  state.installerSummary = installerSummary;
  renderInstallerSummary();
  applyInstallerScenarioDefaults();
  state.runtimeContextUnavailableMessage = getRuntimeContextUnavailableMessage();
  if (state.runtimeContextUnavailableMessage) {
    state.schedules = [];
    state.connectors = [];
    state.runs = [];
    state.staleRuns = [];
    state.migrations = [];
    state.graphData = { nodes: [], edges: [] };
    state.salesforceOverview = null;
    if (includeSalesforceOverview) {
      renderSalesforceOverview({});
    }
    renderContextSelectionSummary();
    renderOverview(healthData);
    renderInstallerSummary();
    renderSchedules();
    renderConnectors();
    renderRuns();
    renderStaleRuns();
    renderOverviewConnectorFilter();
    redrawOverviewGraph();
    if (includeRecordsSummary) {
      await loadRecordsSummary();
    }
    if (shouldRefreshChart) {
      await loadLogSummary();
    }
    return;
  }
  const schedules = await safeRequest('/api/schedules', { items: [] });
  const connectors = await safeRequest('/api/connectors', { items: [] });
  const runs = await safeRequest('/api/runs', { items: [] });
  const staleRuns = await safeRequest('/api/runs/stale', { items: [] });
  const migrations = currentUserHasModule('migration') ? await safeRequest('/api/migrations', { items: [] }) : { items: [] };
  const graph = includeGraph
    ? await safeRequest('/api/graph', { nodes: [], edges: [] })
    : state.graphData || { nodes: [], edges: [] };
  const salesforceOverview = includeSalesforceOverview
    ? await safeRequest('/api/salesforce/overview', {})
    : null;
  if (includeScheduleOptions) {
    await loadScheduleOptions();
  }

  state.schedules = schedules.items || [];
  state.connectors = connectors.items || [];
  state.runs = runs.items || [];
  state.staleRuns = staleRuns.items || [];
  state.migrations = migrations.items || [];
  state.graphData = graph;
  state.runtimeContextUnavailableMessage = '';
  if (includeSalesforceOverview) {
    renderSalesforceOverview(salesforceOverview || {});
  }

  renderOverview(healthData);
  renderInstallerSummary();
  renderSchedules();
  renderConnectors();
  renderRuns();
  renderStaleRuns();
  renderOverviewConnectorFilter();
  redrawOverviewGraph();
  if (includeRecordsSummary) {
    await loadRecordsSummary();
  }
  await loadOverviewUpdateStatus();
  if (shouldRefreshChart) {
    await loadLogSummary();
  }
}

window.refreshSchedules = async function refreshSchedulesFromExternalChange(options = {}) {
  await refresh({
    includeGraph: options.includeGraph === true,
    includeSalesforceOverview: options.includeSalesforceOverview === true,
    includeRecordsSummary: options.includeRecordsSummary === true,
    includeScheduleOptions: options.includeScheduleOptions !== false,
    refreshChart: options.refreshChart === true
  });
};

function activateMainTab(tabTarget) {
  const trigger = document.querySelector('#main-tabs [data-bs-target="' + tabTarget + '"]');
  if (!trigger) {
    return;
  }

  try {
    if (window.bootstrap?.Tab) {
      window.bootstrap.Tab.getOrCreateInstance(trigger).show();
      return;
    }
  } catch {
    // fall back to a native click if Bootstrap Tab is not available
  }

  trigger.click();
  syncHeaderMenuTabState(tabTarget);
}

function syncHeaderMenuTabState(activeTabTarget) {
  const currentTabTarget = activeTabTarget || document.querySelector('#main-tabs .nav-link.active')?.getAttribute('data-bs-target') || '';
  document.querySelectorAll('[data-menu-tab]').forEach((button) => {
    button.classList.toggle('is-active', button.getAttribute('data-menu-tab') === currentTabTarget);
  });
}

function bindEventListenerOnce(elementId, eventName, handler) {
  const element = document.getElementById(elementId);
  if (!element) {
    return;
  }

  const marker = 'bound_' + eventName;
  if (element.dataset[marker] === '1') {
    return;
  }

  element.addEventListener(eventName, handler);
  element.dataset[marker] = '1';
}

function activateAdminTab(tabTarget) {
  const trigger = document.querySelector('#admin-tabs [data-bs-target="' + tabTarget + '"]');
  if (!trigger) {
    return;
  }

  try {
    if (window.bootstrap?.Tab) {
      window.bootstrap.Tab.getOrCreateInstance(trigger).show();
      return;
    }
  } catch {
    // fallback to click
  }

  trigger.click();
}

function openAdminModal(initialTabTarget) {
  if (!(currentUserHasPermission('admin') || currentUserHasModule('projects') || currentUserHasModule('deployment'))) {
    showError('Admin- oder Projektmodul-Berechtigung fehlt');
    return;
  }
  const modalEl = document.getElementById('admin-modal');
  if (!modalEl || !window.bootstrap?.Modal) {
    return;
  }
  window.bootstrap.Modal.getOrCreateInstance(modalEl).show();
  if (initialTabTarget) {
    window.setTimeout(() => activateAdminTab(initialTabTarget), 120);
  }
}

function focusElementLater(elementId) {
  const targetId = String(elementId || '').trim();
  if (!targetId) {
    return;
  }

  window.setTimeout(() => {
    const element = document.getElementById(targetId);
    if (!element) {
      return;
    }

    element.scrollIntoView({ behavior: 'smooth', block: 'center' });
    if (typeof element.focus === 'function') {
      element.focus({ preventScroll: true });
    }
  }, 220);
}

async function openProjectManagement() {
  clearError();
  await loadProjects();
  resetProjectForm();
  activateMainTab('#tab-projects');
  focusElementLater('project-table-body');
}

// Boot data loading before the large listener block so the UI still initializes
// even if a later non-critical listener registration fails.
(async () => {
  try {
    try {
      initializeUiTheme();
    } catch {
      // never block initial data load because of theme handling
    }
    restoreLogChartRange();
    restoreOverviewStatsRange();
    restoreHeaderContext();

    // Set project-specific logo on page load
    const sidebarLogoImg = document.getElementById('agent-sidebar-logo-img');
    if (sidebarLogoImg) {
      sidebarLogoImg.src = '/assets/custom-logo?projectId=' + (state.headerProjectId || 'default-project') + '&t=' + Date.now();
    }
    await loadProjects();
    await loadInstances();

    // Re-apply theme of the currently active project
    const activeProject = (state.projects || []).find((item) => String(item.id || '') === String(state.headerProjectId || ''));
    const projectTheme = activeProject?.theme || 'corporate';
    const themeSelect = document.getElementById('theme-select');
    if (themeSelect) {
      themeSelect.value = projectTheme;
    }
    applyUiTheme(projectTheme);
    await refreshHeaderLayerOptions();
    await refresh();
    updateWeekdayChips();
    initializeTableFilters();
    setInterval(() => {
      // Polling only while tab is visible; use light mode to avoid heavy Salesforce calls.
      if (document.hidden) {
        return;
      }
      void refresh({
        refreshChart: false,
        includeGraph: false,
        includeSalesforceOverview: false,
        includeScheduleOptions: false,
        includeRecordsSummary: false
      });
    }, AUTO_REFRESH_INTERVAL_MS);
  } catch (error) {
    console.error('UI bootstrap failed', error);
    showError(error?.message || 'UI bootstrap failed');
  }
})();

bindEventListenerOnce('new-schedule', 'click', () => openScheduleModal(''));
bindEventListenerOnce('open-admin-modal-sidebar', 'click', () => openAdminModal());
bindEventListenerOnce('open-admin-modal-menu', 'click', () => openAdminModal());
bindEventListenerOnce('admin-user-save', 'click', async () => {
  try {
    await saveAdminUserFromForm();
  } catch (error) {
    showError(error.message || 'Benutzer konnte nicht gespeichert werden');
  }
});
bindEventListenerOnce('admin-user-reset', 'click', resetAdminUserForm);
bindEventListenerOnce('admin-users-refresh', 'click', loadAdminData);
bindEventListenerOnce('admin-memberships-refresh', 'click', loadProjectMemberships);
bindEventListenerOnce('admin-membership-assign', 'click', async () => {
  try {
    await assignProjectMembershipFromForm();
  } catch (error) {
    showError(error.message || 'Projektzuordnung konnte nicht gespeichert werden');
  }
});
bindEventListenerOnce('admin-membership-project', 'change', async (event) => {
  state.selectedMembershipProjectId = String(event && event.target && 'value' in event.target ? event.target.value : '').trim();
  await loadProjectMemberships();
});
bindEventListenerOnce('admin-audit-refresh', 'click', loadAdminData);
bindEventListenerOnce('admin-ai-config-refresh', 'click', loadAIConfig);
bindEventListenerOnce('admin-ai-config-save', 'click', async () => {
  try {
    await saveAIConfigFromForm();
  } catch (error) {
    showError(error.message || 'KI-Konfiguration konnte nicht gespeichert werden');
  }
});
bindEventListenerOnce('admin-rollout-refresh', 'click', async () => {
  await loadRolloutKpisFromAdminPanel();
});
bindEventListenerOnce('admin-rollout-project', 'change', async () => {
  await loadRolloutKpisFromAdminPanel();
});
bindEventListenerOnce('admin-rollout-env', 'change', async () => {
  await loadRolloutKpisFromAdminPanel();
});
bindEventListenerOnce('admin-rollout-window-days', 'change', async () => {
  await loadRolloutKpisFromAdminPanel();
});
bindEventListenerOnce('failed-records-export-csv', 'click', exportFailedRecordsAsCsv);
bindEventListenerOnce('failed-records-export-json', 'click', exportFailedRecordsAsJson);
document.querySelectorAll('[data-menu-tab]').forEach((button) => {
  if (button.dataset.boundMenuTab === '1') {
    return;
  }

  button.dataset.boundMenuTab = '1';
  button.addEventListener('click', () => {
    const tabTarget = String(button.getAttribute('data-menu-tab') || '').trim();
    if (tabTarget) {
      activateMainTab(tabTarget);
    }
  });
});
document.querySelectorAll('#main-tabs .nav-link').forEach((button) => {
  if (button.dataset.boundMenuSync === '1') {
    return;
  }

  button.dataset.boundMenuSync = '1';
  button.addEventListener('click', () => {
    const tabTarget = String(button.getAttribute('data-bs-target') || '').trim();
    window.setTimeout(() => syncHeaderMenuTabState(tabTarget), 0);
  });
});
const headerMenu = document.getElementById('agent-header-menu');
if (headerMenu && headerMenu.dataset.boundOffcanvasShown !== '1') {
  headerMenu.dataset.boundOffcanvasShown = '1';
  headerMenu.addEventListener('shown.bs.offcanvas', () => syncHeaderMenuTabState());
}
bindEventListenerOnce('new-schedule-from-template', 'click', async () => {
  try {
    await createFromTemplate('schedule');
  } catch (error) {
    showError(error.message || 'Scheduler-Vorlage konnte nicht geladen werden');
  }
});
bindEventListenerOnce('sch-target-system', 'change', async () => {
  applyOperationOptions('');
  await loadTargetObjects('');
  await loadTargetFields();
  toggleCreateObjectFromSourceUi();
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-target-type', 'change', async () => {
  applyOperationOptions('');
  toggleCreateObjectFromSourceUi();
  ensureSalesforceTargetDefinition();
  await loadTargetFields();
  await syncSchedulerExternalIdUi();
  updateScheduleFilePathSummaries();
});
bindEventListenerOnce('sch-object', 'change', async () => {
  renderSchedulerMappingAssistant();
  await loadTargetFields();
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-object-filter', 'input', () => {
  renderTargetObjectOptions(state.schedulerTargetObjects || [], document.getElementById('sch-object')?.value || '');
});
bindEventListenerOnce('sch-refresh-sf-metadata', 'click', refreshSchedulerSalesforceMetadata);
bindEventListenerOnce('sch-operation', 'change', async () => {
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-external-id-field', 'change', () => {
  ensureSalesforceTargetDefinition();
  updateSchedulerExternalIdValidationState();
});
bindEventListenerOnce('sch-pricebook2id', 'change', async () => {
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-missing-product-strategy', 'change', () => {
  ensureSalesforceTargetDefinition();
  updateSchedulerExternalIdValidationState();
});
bindEventListenerOnce('sch-target-definition', 'change', async () => {
  await syncSchedulerExternalIdUi();
  updateScheduleFilePathSummaries();
});
bindEventListenerOnce('sch-mapping', 'input', () => {
  state.rawMappingEditorDirty = true;
});
bindEventListenerOnce('sch-mapping', 'change', async () => {
  hydrateMappingRulesFromDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-connector', 'change', async () => {
  const selectedConnectorId = String(document.getElementById('sch-connector')?.value || '').trim();
  applyScheduleSourceSystemFromConnector(selectedConnectorId, { force: true });
  applyScheduleSourceTypeFromConnector(selectedConnectorId, { force: true });
  applyScheduleSourceFieldPolicy(selectedConnectorId);
  updateSourceQueryAssist();
  updateScheduleTypeUi();
  await loadTargetObjects(document.getElementById('sch-object').value || '');
  await loadTargetFields();
  await syncSchedulerExternalIdUi();
  updateScheduleFilePathSummaries();
  const srcType = document.getElementById('sch-source-type').value;
  if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
    loadMappingFields();
  }
});

document.getElementById('instance-select').addEventListener('change', async (event) => {
  state.instanceId = event.target.value;
  syncHeaderContextFromSelectedInstance();
  await refresh();
});
bindEventListenerOnce('context-project-select', 'change', async (event) => {
  const nextProjectId = String(event?.target?.value || '').trim() || 'default-project';
  state.headerProjectId = nextProjectId;
  state.headerLayerId = '';
  const nextInstanceId = findInstanceForContext(nextProjectId, state.headerTargetEnv);
  state.instanceId = nextInstanceId || '';
  const instanceSelect = document.getElementById('instance-select');
  if (instanceSelect) {
    instanceSelect.value = nextInstanceId || '';
  }
  syncHeaderContextFromSelectedInstance({ updateFromSelectedInstance: false });

  // Update project-specific logo and dynamic palette
  const sidebarLogoImg = document.getElementById('agent-sidebar-logo-img');
  if (sidebarLogoImg) {
    sidebarLogoImg.src = '/assets/custom-logo?projectId=' + nextProjectId + '&t=' + Date.now();
  }
  const selectedProj = (state.projects || []).find((item) => String(item.id || '') === nextProjectId);
  const nextTheme = selectedProj?.theme || 'corporate';
  const themeSelect = document.getElementById('theme-select');
  if (themeSelect) {
    themeSelect.value = nextTheme;
  }
  applyUiTheme(nextTheme);
  if (window.updateLogoPreviewState) {
    window.updateLogoPreviewState();
  }

  await refreshHeaderLayerOptions();
  await refresh();
});
bindEventListenerOnce('context-target-env-select', 'change', async (event) => {
  const nextEnv = String(event?.target?.value || '').trim() === 'production' ? 'production' : 'test';
  state.headerTargetEnv = nextEnv;
  state.headerLayerId = '';
  const nextInstanceId = findInstanceForContext(state.headerProjectId, nextEnv);
  state.instanceId = nextInstanceId || '';
  const instanceSelect = document.getElementById('instance-select');
  if (instanceSelect) {
    instanceSelect.value = nextInstanceId || '';
  }
  syncHeaderContextFromSelectedInstance();
  await refreshHeaderLayerOptions();
  await refresh();
});
bindEventListenerOnce('context-layer-select', 'change', (event) => {
  state.headerLayerId = String(event?.target?.value || '').trim();
  persistHeaderContext();
  renderContextSelectionSummary();
});
bindEventListenerOnce('context-layer-create', 'click', async () => {
  try {
    await createHeaderLayerFromCurrentSetup();
  } catch (error) {
    showError(error.message || 'Layer konnte nicht erstellt werden');
  }
});
bindEventListenerOnce('context-layer-apply', 'click', async () => {
  try {
    await applyHeaderLayerToCurrentEnvironment();
  } catch (error) {
    showError(error.message || 'Layer konnte nicht gesetzt werden');
  }
});
const themeSelect = document.getElementById('theme-select');
if (themeSelect) {
  themeSelect.addEventListener('change', (event) => {
    const target = event && event.target ? event.target : null;
    applyUiTheme(String(target && 'value' in target ? target.value : 'corporate'));
  });
}
bindEventListenerOnce('add-instance', 'click', () => {
  resetInstanceForm();
  clearError();
  activateMainTab('#tab-projects');
  focusElementLater('ins-id');
});
bindEventListenerOnce('save-instance', 'click', saveInstance);
bindEventListenerOnce('ins-role', 'change', syncInstanceLoginUrlWithRole);
bindEventListenerOnce('manage-projects', 'click', async () => {
  try {
    await openProjectManagement();
  } catch (error) {
    showError(error.message || 'Projekte konnten nicht geladen werden');
  }
});
bindEventListenerOnce('admin-open-users', 'click', () => {
  openAdminModal('#admin-tab-users');
  focusElementLater('admin-user-username');
});
bindEventListenerOnce('admin-manage-projects', 'click', async () => {
  try {
    await openProjectManagement();
  } catch (error) {
    showError(error.message || 'Projekte konnten nicht geladen werden');
  }
});
bindEventListenerOnce('admin-open-deployment', 'click', async () => {
  try {
    await openProjectManagement();
    focusElementLater('project-table-body');
  } catch (error) {
    showError(error.message || 'Deployment-Konfiguration konnte nicht geöffnet werden');
  }
});
bindEventListenerOnce('admin-open-documentation', 'click', async () => {
  try {
    await openProjectManagement();
    focusElementLater('prj-confluence-space-key');
  } catch (error) {
    showError(error.message || 'Dokumentations-Konfiguration konnte nicht geöffnet werden');
  }
});
bindEventListenerOnce('admin-open-history', 'click', async () => {
  openAdminModal('#admin-tab-history');
  try {
    await loadAdminData();
  } catch {
    // UI bleibt nutzbar; Historie wird ggf. manuell aktualisiert.
  }
  focusElementLater('admin-audit-refresh');
});
bindEventListenerOnce('admin-projects-refresh', 'click', async () => {
  await loadProjects();
  await loadInstances();
});
bindEventListenerOnce('instances-refresh', 'click', async () => {
  await loadInstances();
});
bindEventListenerOnce('instances-filter', 'input', renderInstancePanels);
bindEventListenerOnce('new-instance', 'click', () => openInstanceModal(''));
bindEventListenerOnce('projects-filter', 'input', renderProjectTable);
bindEventListenerOnce('admin-project-new', 'click', () => openProjectWizard(null));
bindEventListenerOnce('admin-project-reset', 'click', resetProjectForm);
bindEventListenerOnce('prj-wizard-back', 'click', () => goToProjectWizardStep(state.projectWizardStep - 1));
bindEventListenerOnce('prj-wizard-next', 'click', advanceProjectWizardStep);
document.querySelectorAll('#prj-wizard-steps [data-prj-step]').forEach((button) => {
  button.addEventListener('click', () => {
    const nextStep = Number(button.getAttribute('data-prj-step') || '1') || 1;
    if (nextStep > state.projectWizardStep) {
      try {
        validateProjectWizardStep(state.projectWizardStep);
      } catch (error) {
        showProjectModalError(error.message || 'Projekt-Schritt konnte nicht abgeschlossen werden');
        return;
      }
    }
    goToProjectWizardStep(nextStep);
  });
});
['prj-test-instance-id', 'prj-production-instance-id'].forEach((id) => {
  bindEventListenerOnce(id, 'change', () => {
    updateProjectInstanceSummary();
    renderProjectReview();
  });
});
['prj-name', 'prj-description', 'prj-confluence-base-url', 'prj-confluence-username', 'prj-confluence-api-token', 'prj-confluence-space-key', 'prj-confluence-parent-page-id', 'prj-confluence-title-prefix', 'prj-lookup-cache-ttl-minutes', 'prj-log-sync-interval-minutes', 'prj-log-batch-size', 'prj-log-buffer-max-entries'].forEach((id) => {
  bindEventListenerOnce(id, 'input', renderProjectReview);
});
['prj-production-write-protection', 'prj-lookup-cache-enabled', 'prj-log-batching-enabled'].forEach((id) => {
  bindEventListenerOnce(id, 'change', renderProjectReview);
});
bindEventListenerOnce('admin-instance-reset', 'click', resetInstanceForm);
bindEventListenerOnce('admin-instance-readiness-check', 'click', async () => {
  try {
    clearError();
    clearInstanceModalError();
    await runInstanceReadinessCheckFromAdminForm();
  } catch (error) {
    showInstanceModalError(error.message || 'Readiness-Check fehlgeschlagen');
  }
});
bindEventListenerOnce('admin-instance-msd-setup-dry', 'click', async () => {
  try {
    clearError();
    clearInstanceModalError();
    await runInstanceMsdSetupFromAdminForm('dry-run');
  } catch (error) {
    showInstanceModalError(error.message || 'MSD Setup Dry-Run fehlgeschlagen');
  }
});
bindEventListenerOnce('admin-instance-msd-setup-apply', 'click', async () => {
  try {
    clearError();
    clearInstanceModalError();
    await runInstanceMsdSetupFromAdminForm('apply');
  } catch (error) {
    showInstanceModalError(error.message || 'MSD Setup fehlgeschlagen');
  }
});
bindEventListenerOnce('prj-id', 'input', (event) => {
  const projectId = String(event?.target?.value || '').trim();
  populateProjectInstanceSelectors(projectId);
  renderProjectReview();
});
document.getElementById('save-project').addEventListener('click', async () => {
  try {
    await saveProject();
  } catch (error) {
    showProjectModalError(error.message || 'Projekt konnte nicht gespeichert werden');
  }
});
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
document.getElementById('publish-project-documentation').addEventListener('click', async () => {
  try {
    clearError();
    await publishProjectDocumentation();
  } catch (error) {
    showError(error.message || 'Dokumentation konnte nicht veröffentlicht werden');
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
document.getElementById('installer-generate-files')?.addEventListener('click', generateInstallerFilesFromUi);
document.getElementById('installer-scenario')?.addEventListener('change', () => {
  applyInstallerScenarioDefaults();
  renderInstallerSummary();
});
document.getElementById('logout-admin')?.addEventListener('click', async () => {
  try {
    await fetch('/auth/logout', { method: 'POST' });
  } finally {
    window.location.href = '/';
  }
});
document.getElementById('overview-check-update').addEventListener('click', async () => {
  await loadOverviewUpdateStatus(true, true);
});
document.getElementById('overview-run-update').addEventListener('click', async () => {
  try {
    await triggerOverviewUpdate();
  } catch (error) {
    showError(error.message || 'Update konnte nicht gestartet werden');
  }
});
(function attachOverviewConnectorFilterHandler() {
  const handleChange = async (value) => {
    state.overviewConnectorFilterId = String(value || '');
    try {
      redrawOverviewGraph();
      await Promise.all([loadLogSummary(), loadRecordsSummary()]);
    } catch (err) {
      console.error('Error refreshing overview after connector change:', err);
    }
  };

  const selectEl = document.getElementById('overview-connector-filter');
  if (selectEl) {
    selectEl.addEventListener('change', (event) => {
      const val = event.target?.value;
      console.debug('[DBG] overview-connector-filter change ->', val);
      console.debug('[DBG] state.connectors:', (state.connectors || []).map((c) => ({ id: c.id, name: c.name, connectorType: c.connectorType })));
      console.debug('[DBG] graph connector nodes:', (state.graphData?.nodes || []).filter((n) => n.kind === 'connector').map((n) => ({ id: n.id, refId: n.refId, connectorId: n.connectorId, label: n.label, connectorType: n.connectorType })));
      handleChange(val);
    });
    selectEl.addEventListener('input', (event) => handleChange(event.target?.value));
    return;
  }

  // Fallback: delegated listener in case the element is rendered later or replaced
  document.addEventListener('change', (event) => {
    const target = event.target;
    if (target && target.id === 'overview-connector-filter') {
      handleChange(target.value);
    }
  });
})();
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
document.getElementById('template-picker-search').addEventListener('input', renderTemplatePicker);
document.getElementById('template-picker-apply').addEventListener('click', applySelectedTemplate);
document.getElementById('template-picker-modal')?.addEventListener('hidden.bs.modal', () => {
  resolveTemplatePicker(null);
});
document.getElementById('sch-load-source-fields')?.addEventListener('click', loadMappingFields);
document.getElementById('sch-automapping')?.addEventListener('click', autoMapByName);
bindEventListenerOnce('sch-mapping-assistant-profile', 'change', () => {
  state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || 'standard').trim() || 'standard';
  renderSchedulerMappingAssistant();
});
bindEventListenerOnce('sch-mapping-assistant-apply', 'click', async () => {
  state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || state.scheduleMappingAssistantProfile || 'standard').trim() || 'standard';
  await autoMapByName();
});
// Event delegation for dynamically rendered manager buttons
document.getElementById('sch-mapping-manager')?.addEventListener('click', async (event) => {
  const target = event.target;
  if (!target) return;
  if (target.id === 'sch-manager-load-fields') {
    await loadMappingFields();
  } else if (target.id === 'sch-automapping') {
    await autoMapByName();
  } else if (target.id === 'sch-mapping-assistant-apply') {
    state.scheduleMappingAssistantProfile = String(document.getElementById('sch-mapping-assistant-profile')?.value || state.scheduleMappingAssistantProfile || 'standard').trim() || 'standard';
    await autoMapByName();
  } else if (target.id === 'sch-mapping-preview-btn') {
    await showMappingPreview();
  } else if (target.id === 'sch-mapping-preview-close') {
    closeSchedulerMappingPreview();
  }
});
document.addEventListener('keydown', (event) => {
  if (event.key !== 'Escape') {
    return;
  }
  const section = document.getElementById('sch-mapping-preview-section');
  if (!section || !section.classList.contains('is-visible')) {
    return;
  }
  closeSchedulerMappingPreview();
  event.preventDefault();
});
document.getElementById('sch-mapping-manager')?.addEventListener('change', (event) => {
  const target = event.target;
  if (!target) return;
  if (target.id === 'sch-mapping-assistant-profile') {
    state.scheduleMappingAssistantProfile = String(target.value || 'standard').trim() || 'standard';
    renderSchedulerMappingAssistant();
  }
});
bindEventListenerOnce('new-schedule', 'click', () => openScheduleModal(''));
bindEventListenerOnce('new-schedule-from-template', 'click', async () => {
  try {
    await createFromTemplate('schedule');
  } catch (error) {
    showError(error.message || 'Scheduler-Vorlage konnte nicht geladen werden');
  }
});
document.getElementById('new-connector').addEventListener('click', () => openConnectorModal(''));
document.getElementById('new-connector-from-template').addEventListener('click', async () => {
  try {
    await createFromTemplate('connector');
  } catch (error) {
    showError(error.message || 'Connector-Vorlage konnte nicht geladen werden');
  }
});
document.getElementById('sch-wizard-back').addEventListener('click', () => {
  goToScheduleWizardStep(state.scheduleWizardStep - 1);
});
document.getElementById('sch-wizard-next').addEventListener('click', advanceScheduleWizardStep);
document.querySelectorAll('#sch-wizard-steps [data-sch-step]').forEach((button) => {
  button.addEventListener('click', () => {
    const nextStep = Number(button.getAttribute('data-sch-step') || '1');
    if (nextStep > state.scheduleWizardStep) {
      try {
        validateScheduleWizardStep(state.scheduleWizardStep);
      } catch (error) {
        showModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
        return;
      }
    }
    goToScheduleWizardStep(nextStep);
  });
});
bindEventListenerOnce('save-schedule', 'click', saveSchedule);
document.getElementById('save-schedule-template').addEventListener('click', async () => {
  try {
    await saveCurrentAsTemplate('schedule');
  } catch (error) {
    showError(error.message || 'Scheduler-Vorlage konnte nicht gespeichert werden');
  }
});
document.getElementById('sch-test-source').addEventListener('click', testScheduleSource);
bindEventListenerOnce('sch-active', 'change', renderRequiredSchedulerFieldStatus);
document.getElementById('sch-source-type').addEventListener('change', () => {
  updateSourceQueryAssist();
  updateScheduleTypeUi();
  const srcType = document.getElementById('sch-source-type').value;
  if (srcType === 'ENDPOINT' && !String(document.getElementById('sch-source-definition').value || '').trim()) {
    document.getElementById('sch-source-definition').value = parseScheduleSourceDefinition('ENDPOINT', '').queryText;
  }
  if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
    loadMappingFields();
  }
});
document.getElementById('sch-source-definition').addEventListener('change', () => {
  updateSourceQueryAssist();
  updateScheduleTypeUi();
  const srcType = document.getElementById('sch-source-type').value;
  if (srcType === 'FILE_CSV' || srcType === 'FILE_EXCEL' || srcType === 'FILE_JSON') {
    loadMappingFields();
  }
});
document.getElementById('sch-source-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-source-archive-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-type').addEventListener('change', () => {
  updateScheduleFilePathSummaries();
  updateScheduleTypeUi();
});
document.getElementById('sch-target-definition').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-archive-relative-directory').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-file-name').addEventListener('input', () => {
  const currentValue = String(document.getElementById('sch-target-file-name')?.value || '').trim();
  if (!currentValue) {
    state.scheduleTargetFileNameDirty = false;
    state.scheduleTargetFileNameLastAuto = '';
  } else if (String(state.scheduleTargetFileNameLastAuto || '').trim() === currentValue) {
    state.scheduleTargetFileNameDirty = false;
  } else {
    state.scheduleTargetFileNameDirty = true;
  }
  updateScheduleFilePathSummaries();
});
document.getElementById('sch-target-file-charset').addEventListener('change', updateScheduleFilePathSummaries);
document.getElementById('sch-target-file-delimiter').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-file-text-qualifier').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-target-file-sheet-name').addEventListener('input', updateScheduleFilePathSummaries);
document.getElementById('sch-source-definition').addEventListener('input', updateSourceQueryAssist);
document.getElementById('sch-source-delta-strategy').addEventListener('change', updateSourceQueryAssist);
document.getElementById('sch-source-delta-field').addEventListener('input', updateSourceQueryAssist);
document.getElementById('sch-source-delta-current').addEventListener('input', updateSourceQueryAssist);
document.getElementById('sch-source-delta-record-id').addEventListener('input', updateSourceQueryAssist);
document.getElementById('sch-source-after-export').addEventListener('input', updateSourceQueryAssist);
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
document.getElementById('save-connector-template').addEventListener('click', async () => {
  try {
    await saveCurrentAsTemplate('connector');
  } catch (error) {
    showError(error.message || 'Connector-Vorlage konnte nicht gespeichert werden');
  }
});
document.getElementById('con-type').addEventListener('input', updateConnectorConfigUi);
document.getElementById('con-wizard-type').addEventListener('change', () => applyConnectorWizardSelection(false));
document.getElementById('con-rest-auth-type').addEventListener('change', updateRestAuthUi);
document.getElementById('con-rest-generate-bearer-token')?.addEventListener('click', generateConnectorBearerToken);
document.getElementById('load-logs').addEventListener('click', loadLogs);
document.getElementById('analyze-run-error').addEventListener('click', analyzeCurrentRunError);
document.getElementById('migration-ai-analyze')?.addEventListener('click', analyzeMigrationSource);
setupMigrationAnalysisDropzone();
document.getElementById('sch-refresh-recent-logs')?.addEventListener('click', async () => {
  await renderScheduleRecentLogs(document.getElementById('sch-id')?.value || '');
});
document.getElementById('refresh-stale-runs')?.addEventListener('click', async () => {
  await refresh({ refreshChart: false });
});
document.getElementById('release-all-stale-runs')?.addEventListener('click', async () => {
  try {
    const result = await requestJson('/api/runs/release-stale', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    if (Number(result.releasedCount || 0) <= 0) {
      showError('Keine stale Runs zum Freigeben gefunden');
      return;
    }
    await refresh({ refreshChart: false });
  } catch (error) {
    showError(error.message || 'Stale Runs konnten nicht freigegeben werden');
  }
});
document.getElementById('sch-map-detail-apply').addEventListener('click', applySelectedMappingDetailChanges);
document.getElementById('sch-map-detail-delete').addEventListener('click', deleteSelectedMappingRule);
document.getElementById('sch-map-detail-picklist-add').addEventListener('click', addPicklistMappingEntry);
document.getElementById('sch-validate-config')?.addEventListener('click', async () => {
  try {
    await validateCurrentScheduleConfiguration();
  } catch (error) {
    showModalError(error.message || 'Konfiguration konnte nicht geprüft werden');
  }
});
document.getElementById('sch-refresh-history')?.addEventListener('click', () => {
  loadEntityHistory('schedule', document.getElementById('sch-id')?.value || '', 'sch-history-list', 'sch-history-meta', 'Scheduler noch nicht gespeichert.');
});
document.getElementById('con-refresh-history')?.addEventListener('click', () => {
  loadEntityHistory('connector', document.getElementById('con-id')?.value || '', 'con-history-list', 'con-history-meta', 'Connector noch nicht gespeichert.');
});
document.getElementById('mig-refresh-history')?.addEventListener('click', () => {
  loadEntityHistory('migration', migState.id || '', 'mig-history-list', 'mig-history-meta', 'Migration noch nicht gespeichert.');
});
bindEventListenerOnce('sch-target-system', 'change', async () => {
  applyOperationOptions('');
  await loadTargetObjects('');
  await loadTargetFields();
  toggleCreateObjectFromSourceUi();
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-target-type', 'change', async () => {
  applyOperationOptions('');
  updateScheduleTypeUi();
  toggleCreateObjectFromSourceUi();
  ensureSalesforceTargetDefinition();
  await loadTargetFields();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-object', 'change', async () => {
  await loadTargetFields();
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-operation', 'change', async () => {
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-external-id-field', 'change', () => {
  ensureSalesforceTargetDefinition();
  updateSchedulerExternalIdValidationState();
});
bindEventListenerOnce('sch-pricebook2id', 'change', async () => {
  ensureSalesforceTargetDefinition();
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-target-definition', 'change', async () => {
  await syncSchedulerExternalIdUi();
});
bindEventListenerOnce('sch-mapping', 'input', () => {
  state.rawMappingEditorDirty = true;
});
bindEventListenerOnce('sch-mapping', 'change', async () => {
  hydrateMappingRulesFromDefinition();
  await syncSchedulerExternalIdUi();
});
document.getElementById('sch-create-custom-object').addEventListener('click', createSalesforceCustomObjectFromSource);
bindEventListenerOnce('sch-connector', 'change', async () => {
  const selectedConnectorId = String(document.getElementById('sch-connector')?.value || '').trim();
  applyScheduleSourceSystemFromConnector(selectedConnectorId, { force: true });
  applyScheduleSourceTypeFromConnector(selectedConnectorId, { force: true });
  applyScheduleSourceFieldPolicy(selectedConnectorId);
  updateSourceQueryAssist();
  updateScheduleTypeUi();
  await loadTargetObjects(document.getElementById('sch-object').value || '');
  await loadTargetFields();
  await syncSchedulerExternalIdUi();
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
document.getElementById('duplicate-reverse-schedule').addEventListener('click', async () => {
  const scheduleId = document.getElementById('sch-id').value;
  if (!scheduleId) {
    return;
  }
  const confirmed = window.confirm('Scheduler duplizieren und Quelle/Ziel sowie Mapping-Richtung drehen? Es wird ein neuer Entwurf im Assistenten geöffnet.');
  if (!confirmed) {
    return;
  }
  syncMappingDefinitionFromRules();
  await openReverseDirectionScheduleDraft(scheduleId, collectScheduleFormPayload());
});
document.getElementById('test-connector').addEventListener('click', saveAndValidateConnector);

// ===== LOGO UPLOAD & THEME EVENTS =====
(function initLogoThemeEvents() {
  const logoUploadBtn = document.getElementById('logo-upload-btn');
  const logoUploadInput = document.getElementById('logo-upload-input');
  const logoUploadRemove = document.getElementById('logo-upload-remove');
  const logoUploadPreviewWrap = document.getElementById('logo-upload-preview-wrap');
  const logoUploadPreview = document.getElementById('logo-upload-preview');
  const logoUploadStatus = document.getElementById('logo-upload-status');
  const themeSelect = document.getElementById('theme-select');
  const sidebarLogoImg = document.getElementById('agent-sidebar-logo-img');

  if (!logoUploadBtn || !logoUploadInput) return;

  // Render initial preview if theme is logo
  const updateLogoPreviewState = () => {
    const projectId = state.editingProjectId;
    if (!projectId) {
      if (logoUploadPreviewWrap) logoUploadPreviewWrap.classList.add('d-none');
      return;
    }
    const savedPalette = localStorage.getItem('custom-logo-palette-' + projectId);
    if (savedPalette) {
      if (logoUploadPreviewWrap) logoUploadPreviewWrap.classList.remove('d-none');
      if (logoUploadPreview) logoUploadPreview.src = `/assets/custom-logo?projectId=${projectId}&t=` + Date.now();
    } else {
      if (logoUploadPreviewWrap) logoUploadPreviewWrap.classList.add('d-none');
    }
  };

  // Expose to window so we can trigger it from project changes
  window.updateLogoPreviewState = updateLogoPreviewState;

  // Run on load to set preview if needed
  updateLogoPreviewState();

  // Open file dialog
  logoUploadBtn.addEventListener('click', () => {
    logoUploadInput.click();
  });

  // Handle file select
  logoUploadInput.addEventListener('change', async () => {
    const file = logoUploadInput.files?.[0];
    if (!file) return;

    const projectId = state.editingProjectId;
    if (!projectId) {
      if (logoUploadStatus) logoUploadStatus.textContent = 'Fehler: Keine Projekt-ID vorhanden.';
      return;
    }
    if (logoUploadStatus) logoUploadStatus.textContent = 'Lade Logo...';
    logoUploadBtn.disabled = true;

    const reader = new FileReader();
    reader.onload = async (e) => {
      const dataUrl = e.target.result;
      const base64Content = dataUrl.split(',')[1];

      try {
        // 1. Extract colors client-side first
        if (logoUploadStatus) logoUploadStatus.textContent = 'Analysiere Farben...';
        const primaryHex = await extractColorsFromLogo(dataUrl);
        const palette = generateLogoThemePalette(primaryHex);

        // 2. Save palette locally and apply immediately if this is the active project
        localStorage.setItem('custom-logo-palette-' + projectId, JSON.stringify(palette));
        
        if (projectId === state.headerProjectId) {
          injectLogoThemeStyle(palette);
          
          // Force apply theme logo
          if (themeSelect) {
            themeSelect.value = 'logo';
            applyUiTheme('logo');
          }
        }

        // 3. Upload to server
        if (logoUploadStatus) logoUploadStatus.textContent = 'Speichere Logo auf Server...';
        
        const response = await fetch('/api/admin/settings/logo', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || ''
          },
          body: JSON.stringify({
            fileName: file.name,
            contentBase64: base64Content,
            projectId: projectId
          })
        });

        if (!response.ok) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.error || 'Server-Upload fehlgeschlagen');
        }

        if (logoUploadStatus) logoUploadStatus.textContent = 'Branding erfolgreich angepasst!';
        
        // Update sidebar logo if this is the active project
        if (projectId === state.headerProjectId && sidebarLogoImg) {
          sidebarLogoImg.src = `/assets/custom-logo?projectId=${projectId}&t=` + Date.now();
        }

        updateLogoPreviewState();
      } catch (err) {
        if (logoUploadStatus) logoUploadStatus.textContent = 'Fehler: ' + err.message;
        console.error('Logo process failed', err);
      } finally {
        logoUploadBtn.disabled = false;
        logoUploadInput.value = '';
      }
    };
    reader.readAsDataURL(file);
  });

  // Handle remove logo
  if (logoUploadRemove) {
    logoUploadRemove.addEventListener('click', async () => {
      const confirmed = window.confirm('Benutzerdefiniertes Logo entfernen und Branding zurücksetzen?');
      if (!confirmed) return;

      const projectId = state.editingProjectId;
      if (!projectId) return;
      if (logoUploadStatus) logoUploadStatus.textContent = 'Entferne Logo...';
      logoUploadRemove.disabled = true;

      try {
        const response = await fetch('/api/admin/settings/logo?projectId=' + encodeURIComponent(projectId), {
          method: 'DELETE',
          headers: {
            'X-CSRF-Token': document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || ''
          }
        });

        if (!response.ok) {
          throw new Error('Fehler beim Löschen des Logos');
        }

        localStorage.removeItem('custom-logo-palette-' + projectId);
        
        if (projectId === state.headerProjectId) {
          const styleEl = document.getElementById('dynamic-logo-theme-style');
          if (styleEl) styleEl.remove();

          // Fallback to corporate theme
          if (themeSelect) {
            themeSelect.value = 'corporate';
            applyUiTheme('corporate');
          }

          if (sidebarLogoImg) {
            sidebarLogoImg.src = `/assets/custom-logo?projectId=${projectId}&t=` + Date.now();
          }
        }

        if (logoUploadStatus) logoUploadStatus.textContent = 'Logo erfolgreich entfernt.';
        updateLogoPreviewState();
      } catch (err) {
        if (logoUploadStatus) logoUploadStatus.textContent = 'Fehler: ' + err.message;
      } finally {
        logoUploadRemove.disabled = false;
      }
    });
  }

  // Handle Project Wizard theme change
  const prjThemeSelect = document.getElementById('prj-theme');
  if (prjThemeSelect) {
    prjThemeSelect.addEventListener('change', () => {
      if (window.updateProjectWizardLogoSection) {
        window.updateProjectWizardLogoSection();
      }
    });
  }
})();

// ===== CONNECTOR WIZARD TYPE SELECT CARD EVENT =====
(function initConnectorWizardCards() {
  document.addEventListener('click', (event) => {
    const card = event.target.closest('.connector-type-card');
    if (!card) return;
    
    const type = card.getAttribute('data-type');
    const select = document.getElementById('con-wizard-type');
    if (select) {
      select.value = type;
      select.dispatchEvent(new Event('change'));
    }
    
    // Update active class
    const container = card.closest('.connector-type-cards-grid');
    if (container) {
      container.querySelectorAll('.connector-type-card').forEach((c) => {
        c.classList.toggle('is-selected', c === card);
      });
    }
  });
})();
