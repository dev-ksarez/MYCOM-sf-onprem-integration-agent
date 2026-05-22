// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-scheduler-fields
// Source lines: 14751–15150
// ──────────────────────────────────────────────────────────────────────

async function loadMappingFields() {
  const loadSeq = Number(state.mappingFieldsLoadSeq || 0) + 1;
  state.mappingFieldsLoadSeq = loadSeq;
  const sourceType = document.getElementById('sch-source-type').value;
  const sourceDefinition = buildScheduleSourceDefinitionValue() || '';
  const objectName = sourceType === 'SALESFORCE_SOQL'
    ? ''
    : document.getElementById('sch-object').value;
  const connectorId = document.getElementById('sch-connector').value || undefined;
  const sourceFieldsBody = document.getElementById('sch-mapping-source-fields');
  if (!sourceFieldsBody) {
    return;
  }

  state.mappingFields = [];
  renderSchedulerMappingManager();

  if (!sourceType || !sourceDefinition.trim()) {
    sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Keine Quellmetadaten verfügbar.</td></tr>';
    renderSchedulerMappingManager();
    return;
  }

  try {
    const result = await requestJson('/api/sources/fields', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sourceType, sourceDefinition, objectName, connectorId })
    });

    const fields = Array.isArray(result.fields) ? result.fields : [];
    if (loadSeq !== state.mappingFieldsLoadSeq) {
      return;
    }
    state.mappingFields = fields;
    reconcileMappingRuleSourceFields();
    refreshSchedulerMappingCompatibilityState();

    // Also fetch sample rows for inline examples in manager
    requestJson('/api/sources/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sourceType, sourceDefinition, connectorId, limit: 5 })
    }).then((previewResult) => {
      if (loadSeq !== state.mappingFieldsLoadSeq) return;
      state.sourcePreviewRows = Array.isArray(previewResult.rows) ? previewResult.rows : [];
      renderSchedulerMappingManager();
    }).catch(() => {
      state.sourcePreviewRows = [];
    });

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

    renderMappingRulesTable();

    renderCreateObjectFieldOverrides();
  } catch (error) {
    if (loadSeq !== state.mappingFieldsLoadSeq) {
      return;
    }
    state.mappingFields = [];
    sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Feldmetadaten konnten nicht geladen werden.</td></tr>';
    showModalError(error.message || 'Feldmetadaten konnten nicht geladen werden');
    renderSchedulerMappingManager();
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

function closeSchedulerMappingPreview() {
  const section = document.getElementById('sch-mapping-preview-section');
  if (!section) {
    return;
  }
  section.classList.remove('is-visible');
  section.setAttribute('aria-hidden', 'true');
}

async function showMappingPreview() {
  const section = document.getElementById('sch-mapping-preview-section');
  const statusEl = document.getElementById('sch-mapping-preview-status');
  const head = document.getElementById('sch-mapping-preview-head');
  const body = document.getElementById('sch-mapping-preview-body');
  if (!section || !statusEl || !head || !body) return;

  const managerShell = document.querySelector('#sch-mapping-manager .sch-mapping-manager-shell');
  const toolbar = managerShell?.querySelector ? managerShell.querySelector('.scheduler-mapping-toolbar') : null;
  const assistantBar = managerShell?.querySelector ? managerShell.querySelector('.scheduler-mapping-assistant-bar') : null;
  let previewTop = 8;
  if (toolbar && Number.isFinite(toolbar.offsetHeight)) {
    previewTop += toolbar.offsetHeight;
  }
  if (assistantBar && Number.isFinite(assistantBar.offsetHeight)) {
    previewTop += assistantBar.offsetHeight;
  }
  section.style.top = String(previewTop) + 'px';
  section.classList.add('is-visible');
  section.setAttribute('aria-hidden', 'false');

  const previewRows = Array.isArray(state.sourcePreviewRows) ? state.sourcePreviewRows : [];
  if (!previewRows.length) {
    statusEl.textContent = 'Keine Quelldaten verfügbar. Bitte zuerst Felder laden.';
    head.innerHTML = '';
    body.innerHTML = '<tr><td class="text-secondary">Keine Daten</td></tr>';
    return;
  }

  syncMappingDefinitionFromRules();
  const mappingDefinition = String(document.getElementById('sch-mapping')?.value || '').trim();
  if (!mappingDefinition) {
    statusEl.textContent = 'Kein Mapping definiert.';
    head.innerHTML = '';
    body.innerHTML = '<tr><td class="text-secondary">Kein Mapping</td></tr>';
    return;
  }

  statusEl.textContent = 'Vorschau wird berechnet...';
  head.innerHTML = '';
  body.innerHTML = '<tr><td class="text-secondary">Wird geladen...</td></tr>';

  try {
    const result = await requestJson('/api/mappings/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mappingDefinition, sourceData: previewRows })
    });
    const fields = Array.isArray(result.fields) ? result.fields : [];
    const rows = Array.isArray(result.rows) ? result.rows : [];
    if (!fields.length) {
      statusEl.textContent = 'Keine gemappten Felder gefunden.';
      head.innerHTML = '';
      body.innerHTML = '<tr><td class="text-secondary">Keine Daten</td></tr>';
      return;
    }
    statusEl.textContent = rows.length + ' Datensätze (Vorschau, max. 5)';
    head.innerHTML = '<tr>' + fields.map((f) => '<th class="small">' + esc(String(f)) + '</th>').join('') + '</tr>';
    body.innerHTML = rows.slice(0, 5).map((row) =>
      '<tr>' + fields.map((f) => '<td class="small">' + esc(String(row[f] ?? '')) + '</td>').join('') + '</tr>'
    ).join('');
  } catch (error) {
    statusEl.textContent = 'Vorschau konnte nicht berechnet werden: ' + (error.message || '');
    head.innerHTML = '';
    body.innerHTML = '<tr><td class="text-secondary">Fehler</td></tr>';
  }
}

function applySchedulerTableClientFilters() {
  const schedulersFilter = document.getElementById('schedulers-filter');
  const query = String(schedulersFilter?.value || '').trim().toLowerCase();
  const activeFilter = String(state.schedulerActiveFilter || 'all').trim().toLowerCase();
  const rows = document.querySelectorAll('#schedules-body tr');

  rows.forEach((row) => {
    const text = String(row.textContent || '').toLowerCase();
    const toggle = row.querySelector('input[data-toggle-schedule-active]');
    const rowActiveState = toggle && toggle.checked ? 'active' : 'inactive';
    const matchesQuery = !query || text.includes(query);
    const matchesActive = activeFilter === 'all' || rowActiveState === activeFilter;
    row.style.display = matchesQuery && matchesActive ? '' : 'none';
  });
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
      applySchedulerTableClientFilters();

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
      directionTabs.forEach((button) => {
        button.classList.remove('is-active');
        button.setAttribute('aria-pressed', 'false');
      });
      tabButton.classList.add('is-active');
      tabButton.setAttribute('aria-pressed', 'true');
      renderSchedules();
    });
  });

  const schedulersActiveFilter = document.getElementById('schedulers-active-filter');
  if (schedulersActiveFilter && schedulersActiveFilter.dataset.bound !== '1') {
    schedulersActiveFilter.dataset.bound = '1';
    schedulersActiveFilter.addEventListener('change', (e) => {
      const value = String(e.target.value || 'all').trim().toLowerCase();
      state.schedulerActiveFilter = ['all', 'active', 'inactive'].includes(value) ? value : 'all';
      try {
        localStorage.setItem(TABLE_STORAGE_KEY + '.schedulers.active', state.schedulerActiveFilter);
      } catch (error) {
        // Ignore storage errors
      }
      renderSchedules();
      applySchedulerTableClientFilters();
    });

    try {
      const storedActiveFilter = localStorage.getItem(TABLE_STORAGE_KEY + '.schedulers.active');
      if (storedActiveFilter) {
        state.schedulerActiveFilter = ['all', 'active', 'inactive'].includes(storedActiveFilter) ? storedActiveFilter : 'all';
      }
    } catch (error) {
      // Ignore storage errors
    }

    schedulersActiveFilter.value = state.schedulerActiveFilter || 'all';
    applySchedulerTableClientFilters();
  }

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

    renderSchedulerConnectorFilterOptions();
  }

  function applyConnectorsPanelFilter(query) {
    const normalizedQuery = String(query || '').toLowerCase();
    const panels = document.querySelectorAll('#connectors-panels [data-connector-panel]');
    panels.forEach((panel) => {
      const text = String(panel.textContent || '').toLowerCase();
      const isMatch = text.includes(normalizedQuery);
      panel.style.display = isMatch ? '' : 'none';
    });
  }

  // Connectors table filter
  const connectorsFilter = document.getElementById('connectors-filter');
  if (connectorsFilter && connectorsFilter.dataset.bound !== '1') {
    connectorsFilter.dataset.bound = '1';
    connectorsFilter.addEventListener('input', (e) => {
      const query = String(e.target.value || '');
      applyConnectorsPanelFilter(query);
      try {
        localStorage.setItem(TABLE_STORAGE_KEY + '.connectors', query.toLowerCase());
      } catch (e) {
        // Ignore storage errors
      }
    });
    try {
      const stored = localStorage.getItem(TABLE_STORAGE_KEY + '.connectors');
      if (stored) {
        connectorsFilter.value = stored;
      }
    } catch (e) {
      // Ignore storage errors
    }
  }
  if (connectorsFilter) {
    applyConnectorsPanelFilter(connectorsFilter.value || '');
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

