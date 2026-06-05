// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-utilities
// Source lines: 1713–3164
// ──────────────────────────────────────────────────────────────────────

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
  const intervalMinutes = Math.max(1, Number(document.getElementById('sch-timing-interval').value || 0) || 0);
  const selectedWeekdays = Array.from(document.querySelectorAll('#sch-weekdays input:checked'))
    .map((input) => Number(input.value))
    .filter((value) => !Number.isNaN(value));

  if (!startValue || selectedWeekdays.length === 0) {
    throw new Error('Bitte Startdatum und mindestens einen Wochentag auswählen.');
  }

  const [hour, minute] = timeValue.split(':').map((item) => Number(item));
  const startDate = new Date(startValue + 'T00:00:00');
  const now = new Date();
  const notBefore = startDate > now ? startDate : now;
  const intervalMs = intervalMinutes * 60 * 1000;
  const maxDays = 60;

  for (let offset = 0; offset <= maxDays; offset += 1) {
    const dayStart = new Date(notBefore);
    dayStart.setDate(notBefore.getDate() + offset);
    dayStart.setHours(hour || 0, minute || 0, 0, 0);

    if (!selectedWeekdays.includes(dayStart.getDay())) {
      continue;
    }

    if (intervalMs >= 24 * 60 * 60 * 1000) {
      if (dayStart <= notBefore) {
        continue;
      }
      return {
        nextRunAtIso: dayStart.toISOString(),
        weekdayList: selectedWeekdays.slice().sort((a, b) => a - b).join(', '),
        timeValue
      };
    }

    if (dayStart > notBefore) {
      return {
        nextRunAtIso: dayStart.toISOString(),
        weekdayList: selectedWeekdays.slice().sort((a, b) => a - b).join(', '),
        timeValue
      };
    }

    const endOfDay = new Date(dayStart);
    endOfDay.setHours(23, 59, 59, 999);
    const elapsedMs = notBefore.getTime() - dayStart.getTime();
    const intervalsElapsed = Math.floor(elapsedMs / intervalMs) + 1;
    const candidate = new Date(dayStart.getTime() + intervalsElapsed * intervalMs);
    if (candidate > notBefore && candidate <= endOfDay) {
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
  alert.classList.remove('alert-info');
  alert.classList.add('alert-danger');
  alert.classList.remove('d-none');
}

function showInfo(message) {
  const alert = document.getElementById('global-alert');
  alert.textContent = message;
  alert.classList.remove('alert-danger');
  alert.classList.add('alert-info');
  alert.classList.remove('d-none');
}

function clearError() {
  const alert = document.getElementById('global-alert');
  alert.textContent = '';
  alert.classList.add('d-none');
}

function showModalError(message) {
  const el = document.getElementById('sch-modal-error');
  if (!el) { showError(message); return; }
  el.textContent = message;
  el.classList.remove('d-none');
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function clearModalError() {
  const el = document.getElementById('sch-modal-error');
  if (el) { el.textContent = ''; el.classList.add('d-none'); }
}

function showConnectorModalError(message) {
  const el = document.getElementById('con-modal-error');
  if (!el) {
    showError(message);
    return;
  }
  el.textContent = message;
  el.classList.remove('d-none');
}

function showProjectModalError(message) {
  const el = document.getElementById('prj-modal-error');
  if (!el) {
    showError(message);
    return;
  }
  el.textContent = message;
  el.classList.remove('d-none');
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function clearProjectModalError() {
  const el = document.getElementById('prj-modal-error');
  if (el) {
    el.textContent = '';
    el.classList.add('d-none');
  }
}

function showInstanceModalError(message) {
  const el = document.getElementById('ins-modal-error');
  if (!el) {
    showError(message);
    return;
  }
  el.className = 'alert alert-danger';
  el.textContent = message;
  el.classList.remove('d-none');
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function showInstanceModalWarning(message) {
  const el = document.getElementById('ins-modal-error');
  if (!el) {
    showError(message);
    return;
  }
  el.className = 'alert alert-warning';
  el.textContent = message;
  el.classList.remove('d-none');
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function clearInstanceModalError() {
  const el = document.getElementById('ins-modal-error');
  if (el) {
    el.textContent = '';
    el.className = 'alert alert-danger d-none';
  }
}

function clearConnectorModalError() {
  const el = document.getElementById('con-modal-error');
  if (el) {
    el.textContent = '';
    el.classList.add('d-none');
  }
}

function showMigrationModalError(message) {
  const el = document.getElementById('mig-modal-error');
  if (!el) {
    showError(message);
    return;
  }
  el.textContent = message;
  el.classList.remove('d-none');
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function clearMigrationModalError() {
  const el = document.getElementById('mig-modal-error');
  if (el) {
    el.textContent = '';
    el.classList.add('d-none');
  }
}

function withInstance(path) {
  const url = new URL(path, window.location.origin);
  url.searchParams.set('projectId', String(state.headerProjectId || 'default-project').trim() || 'default-project');
  url.searchParams.set('targetEnv', state.headerTargetEnv === 'production' ? 'production' : 'test');
  if (state.instanceId) {
    url.searchParams.set('instanceId', state.instanceId);
  }
  return url.pathname + url.search;
}

async function requestJson(path, options) {
  const requestOptions = options && typeof options === 'object' ? options : {};
  const response = await fetch(withInstance(path), requestOptions);
  if (response.status === 401) {
    window.location.href = '/';
    throw new Error('Sitzung abgelaufen');
  }
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

function getMigrationReportUrl(migrationId, asDownload) {
  return withInstance('/api/migrations/' + encodeURIComponent(migrationId) + '/report' + (asDownload ? '?download=1' : ''));
}

function isFileConnectorType(connectorType) {
  const normalized = String(connectorType || '').toLowerCase();
  return normalized.includes('file') || normalized.includes('csv') || normalized.includes('excel') || normalized.includes('xlsx') || normalized.includes('json');
}

function normalizeConnectorType(connectorType) {
  const normalized = String(connectorType || '').trim().toLowerCase();
  if (!normalized) {
    return '';
  }
  if (normalized === 'mssql' || normalized === 'ms sql' || normalized === 'ms_sql' || normalized.includes('sqlserver')) {
    return 'MSSQL';
  }
  if (normalized === 'postgresql' || normalized === 'postgres' || normalized === 'pgsql') {
    return 'POSTGRESQL';
  }
  if (normalized === 'mysql') {
    return 'MYSQL';
  }
  if (normalized.includes('binary') && normalized.includes('file')) {
    return 'FILE_BINARY_SF_IMPORT';
  }
  if (normalized.includes('rest') || normalized.includes('http') || normalized.includes('api')) {
    return 'REST_API';
  }
  if (normalized.includes('file') || normalized.includes('csv') || normalized.includes('excel') || normalized.includes('xlsx') || normalized.includes('json') || normalized.includes('txt')) {
    return 'FILE';
  }
  return String(connectorType || '').trim().toUpperCase();
}

function isSqlConnectorType(connectorType) {
  const normalized = normalizeConnectorType(connectorType);
  return normalized === 'MSSQL' || normalized === 'POSTGRESQL' || normalized === 'MYSQL';
}

function isRestConnectorType(connectorType) {
  return normalizeConnectorType(connectorType) === 'REST_API';
}

function isBinaryImportConnectorType(connectorType) {
  return normalizeConnectorType(connectorType) === 'FILE_BINARY_SF_IMPORT';
}

function isMssqlConnectorType(connectorType) {
  return normalizeConnectorType(connectorType) === 'MSSQL';
}

function pickFirstAvailableSelectValue(selectEl, candidates) {
  if (!selectEl || !Array.isArray(candidates)) {
    return '';
  }

  const normalizeComparable = (value) => String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[s_-]+/g, '');

  const options = Array.from(selectEl.options || []);
  const normalizedCandidates = candidates
    .map((candidate) => ({
      raw: String(candidate || '').trim(),
      normalized: normalizeComparable(candidate)
    }))
    .filter((item) => item.raw && item.normalized);

  const exactByValue = normalizedCandidates.find((candidate) =>
    options.some((option) => String(option.value || '').trim() === candidate.raw)
  );
  if (exactByValue) {
    return exactByValue.raw;
  }

  const byComparable = normalizedCandidates.find((candidate) =>
    options.some((option) => {
      const optionValueComparable = normalizeComparable(option.value);
      const optionTextComparable = normalizeComparable(option.textContent || '');
      if (!optionValueComparable && !optionTextComparable) {
        return false;
      }
      return optionValueComparable === candidate.normalized
        || optionTextComparable === candidate.normalized
        || optionTextComparable.includes(candidate.normalized)
        || (optionTextComparable && candidate.normalized.includes(optionTextComparable));
    })
  );

  if (!byComparable) {
    return '';
  }

  const matchedOption = options.find((option) => {
    const optionValueComparable = normalizeComparable(option.value);
    const optionTextComparable = normalizeComparable(option.textContent || '');
    if (!optionValueComparable && !optionTextComparable) {
      return false;
    }
    return optionValueComparable === byComparable.normalized
      || optionTextComparable === byComparable.normalized
      || optionTextComparable.includes(byComparable.normalized)
      || (optionTextComparable && byComparable.normalized.includes(optionTextComparable));
  });

  return String(matchedOption?.value || '').trim();
}

function inferScheduleSourceTypeFromConnector(connectorId) {
  const normalizedConnectorId = String(connectorId || '').trim();
  if (!normalizedConnectorId) {
    return '';
  }

  const connector = (state.connectors || []).find((item) => String(item?.id || '').trim() === normalizedConnectorId);
  if (!connector) {
    return '';
  }

  const sourceTypeSelect = document.getElementById('sch-source-type');
  const normalizedConnectorType = normalizeConnectorType(connector.connectorType);

  if (normalizedConnectorType === 'REST_API') {
    return pickFirstAvailableSelectValue(sourceTypeSelect, ['REST_API']);
  }

  if (normalizedConnectorType === 'FILE' || normalizedConnectorType === 'FILE_BINARY_SF_IMPORT') {
    return pickFirstAvailableSelectValue(sourceTypeSelect, ['FILE_CSV', 'FILE_JSON', 'FILE_EXCEL', 'FILE_XLSX']);
  }

  if (isSqlConnectorType(normalizedConnectorType)) {
    return pickFirstAvailableSelectValue(sourceTypeSelect, ['MSSQL_SQL', 'MSSQL']);
  }

  return '';
}

function inferScheduleSourceSystemFromConnector(connectorId) {
  const normalizedConnectorId = String(connectorId || '').trim();
  if (!normalizedConnectorId) {
    return '';
  }

  const connector = (state.connectors || []).find((item) => String(item?.id || '').trim() === normalizedConnectorId);
  if (!connector) {
    return '';
  }

  const sourceSystemSelect = document.getElementById('sch-source-system');
  const normalizedConnectorType = normalizeConnectorType(connector.connectorType);

  if (normalizedConnectorType === 'REST_API') {
    return pickFirstAvailableSelectValue(sourceSystemSelect, ['REST API', 'REST_API', 'REST', 'API']);
  }

  if (normalizedConnectorType === 'FILE' || normalizedConnectorType === 'FILE_BINARY_SF_IMPORT') {
    return pickFirstAvailableSelectValue(sourceSystemSelect, ['File', 'FILE', 'Datei', 'Dateisystem']);
  }

  if (isSqlConnectorType(normalizedConnectorType)) {
    return pickFirstAvailableSelectValue(sourceSystemSelect, ['MS SQL', 'MSSQL', 'SQL', 'MS-SQL', 'Datenbank']);
  }

  return '';
}

function applyScheduleSourceTypeFromConnector(connectorId, options) {
  const sourceTypeSelect = document.getElementById('sch-source-type');
  if (!sourceTypeSelect) {
    return;
  }

  const nextSourceType = inferScheduleSourceTypeFromConnector(connectorId);
  if (!nextSourceType) {
    return;
  }

  const currentSourceType = String(sourceTypeSelect.value || '').trim();
  const force = options?.force === true;
  if (!force && currentSourceType) {
    return;
  }

  if (currentSourceType === nextSourceType) {
    return;
  }

  sourceTypeSelect.value = nextSourceType;
}

function applyScheduleSourceSystemFromConnector(connectorId, options) {
  const sourceSystemSelect = document.getElementById('sch-source-system');
  if (!sourceSystemSelect) {
    return;
  }

  const nextSourceSystem = inferScheduleSourceSystemFromConnector(connectorId);
  if (!nextSourceSystem) {
    return;
  }

  const currentSourceSystem = String(sourceSystemSelect.value || '').trim();
  const force = options?.force === true;
  if (!force && currentSourceSystem) {
    return;
  }

  if (currentSourceSystem === nextSourceSystem) {
    return;
  }

  sourceSystemSelect.value = nextSourceSystem;
}

function applyScheduleSourceFieldPolicy(connectorId) {
  const sourceSystemSelect = document.getElementById('sch-source-system');
  const sourceTypeSelect = document.getElementById('sch-source-type');
  const sourceSystemLabel = document.querySelector('label[for="sch-source-system"]');
  const sourceTypeLabel = document.querySelector('label[for="sch-source-type"]');

  if (!sourceSystemSelect || !sourceTypeSelect) {
    return;
  }

  const connector = (state.connectors || []).find((item) => String(item?.id || '').trim() === String(connectorId || '').trim());
  if (!connector) {
    sourceSystemSelect.disabled = false;
    sourceTypeSelect.disabled = false;
    if (sourceSystemLabel) sourceSystemLabel.innerHTML = sourceSystemLabel.innerHTML.replace(' <small class="text-muted">(vom Connector abgeleitet)</small>', '');
    if (sourceTypeLabel) sourceTypeLabel.innerHTML = sourceTypeLabel.innerHTML.replace(' <small class="text-muted">(nur für File-Quellen editierbar)</small>', '');
    return;
  }

  const normalizedConnectorType = normalizeConnectorType(connector.connectorType);
  const isFileConnector = normalizedConnectorType === 'FILE' || normalizedConnectorType === 'FILE_BINARY_SF_IMPORT';

  sourceSystemSelect.disabled = true;
  if (sourceSystemLabel && !sourceSystemLabel.innerHTML.includes('(vom Connector abgeleitet)')) {
    sourceSystemLabel.innerHTML += ' <small class="text-muted">(vom Connector abgeleitet)</small>';
  }

  sourceTypeSelect.disabled = !isFileConnector;
  if (isFileConnector) {
    if (sourceTypeLabel && sourceTypeLabel.innerHTML.includes('(vom Connector abgeleitet)')) {
      sourceTypeLabel.innerHTML = sourceTypeLabel.innerHTML.replace(' <small class="text-muted">(vom Connector abgeleitet)</small>', '');
    }
    if (sourceTypeLabel && !sourceTypeLabel.innerHTML.includes('(nur für File-Quellen editierbar)')) {
      sourceTypeLabel.innerHTML += ' <small class="text-muted">(nur für File-Quellen editierbar)</small>';
    }
  } else {
    if (sourceTypeLabel && sourceTypeLabel.innerHTML.includes('(nur für File-Quellen editierbar)')) {
      sourceTypeLabel.innerHTML = sourceTypeLabel.innerHTML.replace(' <small class="text-muted">(nur für File-Quellen editierbar)</small>', '');
    }
    if (sourceTypeLabel && !sourceTypeLabel.innerHTML.includes('(vom Connector abgeleitet)')) {
      sourceTypeLabel.innerHTML += ' <small class="text-muted">(vom Connector abgeleitet)</small>';
    }
  }
}

function getConnectorWizardTypeFromConnectorType(connectorType) {
  const normalized = normalizeConnectorType(connectorType);
  if (!normalized) {
    return 'MSSQL';
  }
  if (['MSSQL', 'POSTGRESQL', 'MYSQL', 'FILE', 'REST_API', 'FILE_BINARY_SF_IMPORT'].includes(normalized)) {
    return normalized;
  }
  return 'CUSTOM';
}

async function fileToBase64(file) {
  const buffer = await file.arrayBuffer();
  let binary = '';
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;

  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, Math.min(index + chunkSize, bytes.length));
    binary += String.fromCharCode(...chunk);
  }

  return window.btoa(binary);
}

async function exportSetup() {
  const result = await requestJson('/api/setup/export');
  const payload = JSON.stringify(result, null, 2);
  const blob = new Blob([payload], { type: 'application/json;charset=utf-8' });
  const href = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  const instanceSuffix = state.instanceId ? '-' + state.instanceId : '';
  anchor.href = href;
  anchor.download = 'sf-agent-setup' + instanceSuffix + '.json';
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(href);
}

async function publishProjectDocumentation() {
  const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(state.instanceId || ''));
  const projectId = String((selectedInstance && selectedInstance.projectId) || '').trim();
  if (!projectId) {
    throw new Error('Für die Dokumentationspublikation muss zuerst eine Projektinstanz ausgewählt sein.');
  }

  const result = await requestJson('/api/admin/projects/' + encodeURIComponent(projectId) + '/documentation/publish-confluence', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ instanceId: state.instanceId || undefined })
  });

  const mode = result && result.publishResult ? String(result.publishResult.mode || 'dry-run') : 'dry-run';
  const pageId = result && result.publishResult ? String(result.publishResult.pageId || '') : '';
  window.alert(
    mode === 'dry-run'
      ? 'Dokumentation vorbereitet. Confluence ist nicht konfiguriert, deshalb wurde nur die Vorschau erzeugt.'
      : 'Dokumentation veröffentlicht' + (pageId ? ' (Seite ' + pageId + ')' : '') + '.'
  );
  return result;
}

async function importSetupDocument(documentBody) {
  const result = await requestJson('/api/setup/import', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(documentBody)
  });

  window.alert(
    'Import abgeschlossen. Connectoren: +' + result.connectorsCreated + ' / ~' + result.connectorsUpdated +
    ', Scheduler: +' + result.schedulesCreated + ' / ~' + result.schedulesUpdated
  );
  await refresh();
}

async function importSetupFromFile(file) {
  if (!file) {
    return;
  }

  const raw = await file.text();
  let documentBody;
  try {
    documentBody = JSON.parse(raw);
  } catch {
    throw new Error('Import-Datei ist kein gueltiges JSON');
  }

  await importSetupDocument(documentBody);
}

async function createSchedulerFromDroppedFile(connectorId, file) {
  if (!connectorId || !file) {
    return;
  }

  const base64 = await fileToBase64(file);
  const analysis = await requestJson('/api/files/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ connectorId, fileName: file.name, contentBase64: base64 })
  });

  const objectName = (state.scheduleOptions?.objectNames || [])[0] || 'Account';
  const targetDefinition = {
    objectApiName: objectName,
    operation: 'upsert',
    externalIdField: 'ExternalId__c',
    picklists: []
  };

  const payload = {
    active: false,
    sourceSystem: 'File',
    targetSystem: 'Salesforce',
    objectName,
    operation: 'Upsert',
    connectorId,
    direction: 'Inbound',
    sourceType: analysis.sourceType,
    targetType: 'SALESFORCE',
    sourceDefinition: analysis.sourceDefinition,
    targetDefinition: JSON.stringify(targetDefinition, null, 2),
    mappingDefinition: analysis.mappingDefinition,
    batchSize: 100,
    timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: '09:00' }),
    name: 'File Import ' + analysis.fileName
  };

  const created = await requestJson('/api/schedules', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  await refresh();
  await openScheduleModal(created.id);
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

function setTemplatePickerError(message) {
  const element = document.getElementById('template-picker-error');
  if (!element) {
    return;
  }
  if (!message) {
    element.textContent = '';
    element.classList.add('d-none');
    return;
  }
  element.textContent = String(message);
  element.classList.remove('d-none');
}

function resolveTemplatePicker(selection) {
  if (typeof templatePickerState.resolver === 'function') {
    const resolver = templatePickerState.resolver;
    templatePickerState.resolver = null;
    resolver(selection || null);
  }
}

function applySelectedTemplate() {
  const selected = templatePickerState.items.find((item) => item.id === templatePickerState.selectedTemplateId) || null;
  resolveTemplatePicker(selected);
  templatePickerModal.hide();
}

function renderTemplatePicker() {
  const list = document.getElementById('template-picker-list');
  const tagsWrap = document.getElementById('template-picker-tags');
  const summary = document.getElementById('template-picker-summary');
  const applyButton = document.getElementById('template-picker-apply');
  const searchInput = document.getElementById('template-picker-search');
  if (!list || !tagsWrap || !summary || !applyButton || !searchInput) {
    return;
  }

  const searchValue = String(searchInput.value || '').trim().toLowerCase();
  const availableTags = Array.from(new Set(templatePickerState.items.flatMap((item) => Array.isArray(item.tags) ? item.tags : []).map((tag) => String(tag || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b, 'de', { sensitivity: 'base' }));
  if (templatePickerState.selectedTag && !availableTags.includes(templatePickerState.selectedTag)) {
    templatePickerState.selectedTag = '';
  }
  tagsWrap.innerHTML = ['<button type="button" class="btn btn-sm ' + (templatePickerState.selectedTag ? 'btn-outline-secondary' : 'btn-secondary') + '" data-template-tag="">Alle</button>']
    .concat(availableTags.map((tag) => '<button type="button" class="btn btn-sm ' + (templatePickerState.selectedTag === tag ? 'btn-secondary' : 'btn-outline-secondary') + '" data-template-tag="' + esc(tag) + '">' + esc(tag) + '</button>'))
    .join('');
  tagsWrap.querySelectorAll('[data-template-tag]').forEach((button) => {
    button.addEventListener('click', () => {
      templatePickerState.selectedTag = button.getAttribute('data-template-tag') || '';
      renderTemplatePicker();
    });
  });

  const filteredItems = templatePickerState.items.filter((item) => {
    if (templatePickerState.selectedTag && !(Array.isArray(item.tags) && item.tags.includes(templatePickerState.selectedTag))) {
      return false;
    }
    if (!searchValue) {
      return true;
    }
    const haystack = [item.name, item.description, ...(Array.isArray(item.tags) ? item.tags : [])]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(searchValue);
  });
  templatePickerState.filteredItems = filteredItems;

  if (!filteredItems.some((item) => item.id === templatePickerState.selectedTemplateId)) {
    templatePickerState.selectedTemplateId = filteredItems[0]?.id || '';
  }

  summary.textContent = filteredItems.length
    ? String(filteredItems.length) + ' Vorlagen verfügbar'
    : 'Keine passende Vorlage gefunden.';
  applyButton.disabled = !templatePickerState.selectedTemplateId;

  list.innerHTML = filteredItems.length
    ? '<div class="template-app-grid">' + filteredItems.map((item) => {
        const isSelected = item.id === templatePickerState.selectedTemplateId;
        const scopeLabel = item.scope === 'system' ? 'System' : 'Eigene Vorlage';
        const kindLabel = item.kind === 'bundle' ? 'Komplettset' : (item.kind === 'connector' ? 'Connector' : 'Scheduler');
        const accent = getTemplateAccent(item);
        const symbol = getTemplateSymbol(item);
        const heroLabel = getTemplateHeroLabel(item);
        const tags = Array.isArray(item.tags) && item.tags.length
          ? '<div class="template-app-card__tags">' + item.tags.slice(0, 5).map((tag) => '<span class="badge text-bg-light border">' + esc(tag) + '</span>').join('') + '</div>'
          : '';
        const previewTitle = getTemplatePreviewTitle(item);
        return '<button type="button" class="template-app-card' + (isSelected ? ' is-selected' : '') + '" data-template-id="' + esc(item.id) + '">' +
          '<div class="template-app-card__body">' +
            '<div class="template-app-card__top">' +
              '<div class="template-app-card__app">' +
                '<div class="template-app-card__icon" style="background:linear-gradient(135deg,' + accent.start + ',' + accent.end + ');">' +
                  '<div class="template-app-card__glaze" style="background:' + accent.glaze + ';"></div>' +
                  '<span class="template-app-card__symbol">' + esc(symbol) + '</span>' +
                '</div>' +
                '<div class="template-app-card__meta">' +
                  '<div class="template-app-card__eyebrow">' + esc(heroLabel) + '</div>' +
                  '<div class="template-app-card__title">' + esc(item.name) + '</div>' +
                  '<div class="template-app-card__badges">' +
                    '<span class="badge ' + (isSelected ? 'text-bg-primary' : 'text-bg-secondary-subtle border text-secondary-emphasis') + '">' + esc(scopeLabel) + '</span>' +
                    '<span class="badge ' + (isSelected ? 'text-bg-info' : 'text-bg-info-subtle border text-info-emphasis') + '">' + esc(kindLabel) + '</span>' +
                  '</div>' +
                '</div>' +
              '</div>' +
              '<span class="template-app-card__install">' + (isSelected ? 'Ausgewählt' : 'Öffnen') + '</span>' +
            '</div>' +
            '<div class="template-app-card__hero" style="background:linear-gradient(135deg,' + accent.start + ',' + accent.end + ');">' +
              '<div class="template-app-card__hero-art"></div>' +
              '<div class="template-app-card__hero-copy">' +
                '<div class="template-app-card__hero-label">' + esc(previewTitle) + '</div>' +
                '<div class="template-app-card__hero-name">' + esc(symbol) + ' · ' + esc(heroLabel) + '</div>' +
              '</div>' +
            '</div>' +
            '<div class="template-app-card__description">' + esc(item.description || 'Keine Beschreibung') + '</div>' +
            '<div class="template-app-card__footer">' +
              tags +
            '</div>' +
          '</div>' +
        '</button>';
      }).join('') + '</div>'
    : '<div class="text-secondary small border rounded p-3">Keine Vorlagen gefunden.</div>';

  list.querySelectorAll('[data-template-id]').forEach((button) => {
    button.addEventListener('click', () => {
      templatePickerState.selectedTemplateId = button.getAttribute('data-template-id') || '';
      renderTemplatePicker();
    });
    button.addEventListener('dblclick', () => {
      applySelectedTemplate();
    });
  });
}

async function pickTemplate(kind) {
  setTemplatePickerError('');
  document.getElementById('template-picker-title').textContent = (kind === 'connector' ? 'Connector' : 'Scheduler') + '-Vorlage wählen';
  document.getElementById('template-picker-search').value = '';
  templatePickerState.kind = kind;
  templatePickerState.items = [];
  templatePickerState.filteredItems = [];
  templatePickerState.selectedTemplateId = '';
  templatePickerState.selectedTag = '';

  const result = await requestJson('/api/templates?kind=' + encodeURIComponent(kind), null);
  templatePickerState.items = Array.isArray(result.items) ? result.items : [];
  if (!templatePickerState.items.length) {
    window.alert((kind === 'connector' ? 'Connector' : 'Scheduler') + '-Vorlagen sind noch nicht vorhanden.');
    return null;
  }

  templatePickerState.selectedTemplateId = templatePickerState.items[0]?.id || '';
  renderTemplatePicker();
  templatePickerModal.show();

  return await new Promise((resolve) => {
    templatePickerState.resolver = resolve;
  });
}

async function createFromTemplate(kind) {
  const template = await pickTemplate(kind);
  if (!template) {
    return;
  }

  if (template.kind === 'bundle') {
    const result = await requestJson('/api/templates/' + encodeURIComponent(template.id) + '/apply', {
      method: 'POST'
    });
    await refresh();
    if (kind === 'schedule' && result.schedule?.id) {
      await openScheduleModal(result.schedule.id);
      return;
    }
    if (result.connector?.id) {
      openConnectorModal(result.connector.id);
      return;
    }
    window.alert('Komplettvorlage angelegt.');
    return;
  }

  if (kind === 'schedule') {
    await openScheduleModal('', template.schedule || {});
    return;
  }

  openConnectorModal('', template.connector || {});
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

function getWizardUserLabel(entry, prefix) {
  if (!entry) return '-';
  const name = String(entry[prefix + 'ByName'] || entry[prefix + 'ByUsername'] || entry[prefix + 'By'] || '').trim();
  return name || '-';
}

function renderWizardMetadata(elementId, entry, emptyLabel) {
  const element = document.getElementById(elementId);
  if (!element) return;

  const createdAt = entry?.createdAt || '';
  const modifiedAt = entry?.lastModifiedAt || entry?.updatedAt || '';
  if (!createdAt && !modifiedAt) {
    element.textContent = emptyLabel || 'Noch nicht gespeichert';
    return;
  }

  const createdBy = getWizardUserLabel(entry, 'created');
  const modifiedBy = String(entry?.lastModifiedByName || entry?.lastModifiedByUsername || entry?.updatedByName || entry?.updatedBy || '').trim() || '-';
  element.innerHTML =
    '<div class="meta-row">' +
      '<div class="meta-item"><span class="meta-label"><strong>Erstellt:</strong></span><span class="meta-value">' + esc(formatDate(createdAt, 'short')) + ' · ' + esc(createdBy) + '</span></div>' +
      '<div class="meta-item"><span class="meta-label"><strong>Letzte Änderung:</strong></span><span class="meta-value">' + esc(formatDate(modifiedAt, 'short')) + ' · ' + esc(modifiedBy) + '</span></div>' +
    '</div>';
  // Re-attach edit toggles for any newly inserted inputs inside this element
  try { attachFieldEditToggles(element); } catch (e) {}
}

function formatDurationMinSec(milliseconds) {
  const numericValue = Number(milliseconds);
  if (!Number.isFinite(numericValue) || numericValue < 0) {
    return '-';
  }

  const totalSeconds = Math.round(numericValue / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes >= 60) {
    const hours = Math.floor(minutes / 60);
    const restMinutes = minutes % 60;
    return hours + ':' + String(restMinutes).padStart(2, '0') + ':' + String(seconds).padStart(2, '0');
  }
  return minutes + ':' + String(seconds).padStart(2, '0');
}

function getRunDurationMs(run) {
  if (!run?.startedAt) {
    return null;
  }

  const startedAt = new Date(run.startedAt).getTime();
  if (Number.isNaN(startedAt)) {
    return null;
  }

  const finishedAt = run?.finishedAt ? new Date(run.finishedAt).getTime() : Date.now();
  if (Number.isNaN(finishedAt) || finishedAt < startedAt) {
    return null;
  }

  return finishedAt - startedAt;
}

function getConnectorNameById(connectorId) {
  if (!connectorId) return '-';
  const connector = state.connectors?.find((item) => item.id === connectorId);
  return connector ? connector.name : connectorId;
}

function normalizeRunStatus(status) {
  return String(status || '').trim().toLowerCase();
}

function getStatusBadge(status) {
  if (!status) return '<span class="badge bg-secondary">Unbekannt</span>';
  const lowerStatus = normalizeRunStatus(status);
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
  if (value.includes('rest')) return '🌐';
  if (value.includes('mssql') || value.includes('sql')) return '🗄';
  if (value.includes('file') || value.includes('csv') || value.includes('excel') || value.includes('json')) return '📄';
  if (value.includes('mock') || value.includes('test')) return '🧪';
  if (value.includes('sage')) return '📚';
  return '⚙';
}

function getConnectorGraphClass(connectorType, connectorName) {
  const value = String(connectorType || connectorName || '').toLowerCase();
  if (value.includes('salesforce')) return 'graph-connector-salesforce';
  if (value.includes('rest')) return 'graph-connector-rest';
  if (value.includes('mssql') || value.includes('sql')) return 'graph-connector-mssql';
  if (value.includes('file') || value.includes('csv') || value.includes('excel') || value.includes('json')) return 'graph-connector-file';
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

function splitGraphTextByLine(value, lineCharLimits) {
  const text = String(value || '').trim();
  const limits = Array.isArray(lineCharLimits) ? lineCharLimits.map((item) => Math.max(1, Number(item) || 1)) : [];
  if (!text || !limits.length) return splitGraphText(text, 18, 2);

  const words = text.split(/\s+/).filter(Boolean);
  const lines = [];
  let currentLine = '';
  let lineIndex = 0;

  words.forEach((word) => {
    const currentLimit = limits[Math.min(lineIndex, limits.length - 1)];
    const candidate = currentLine ? currentLine + ' ' + word : word;
    if (candidate.length <= currentLimit) {
      currentLine = candidate;
      return;
    }

    if (currentLine) {
      lines.push(currentLine);
      lineIndex += 1;
    }

    const nextLimit = limits[Math.min(lineIndex, limits.length - 1)];
    if (word.length <= nextLimit) {
      currentLine = word;
      return;
    }

    lines.push(word.slice(0, Math.max(1, nextLimit - 1)).trimEnd() + '…');
    currentLine = '';
    lineIndex += 1;
  });

  if (currentLine) {
    lines.push(currentLine);
  }

  if (lines.length > limits.length) {
    const visibleLines = lines.slice(0, limits.length);
    const lastIndex = visibleLines.length - 1;
    const lastLimit = limits[lastIndex] || limits[limits.length - 1] || 18;
    visibleLines[lastIndex] = visibleLines[lastIndex].slice(0, Math.max(1, lastLimit - 1)).trimEnd() + '…';
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

function isSalesforceTargetSelection() {
  const targetSystem = resolveEffectiveTargetSystem();
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  return targetSystem === 'Salesforce' && targetType === 'SALESFORCE';
}

function formatUsageBlock(value, unit) {
  if (!value || !Number.isFinite(value.max) || value.max <= 0) {
    return '-';
  }
  const max = Number(value.max);
  const used = Number(value.used || 0);
  const remaining = Number(value.remaining || 0);
  const percentage = Math.round((used / max) * 100);
  return used + '/' + max + (unit ? ' ' + unit : '') + ' (' + percentage + '%, frei ' + remaining + ')';
}

function resolveRecordsGrowthMetric() {
  const daily = state.recordsSummary?.daily || {};
  const latest = Math.max(0, Number(daily.succeeded || 0) || 0);
  const previous = Math.max(0, Number(daily.previousSucceeded || 0) || 0);
  const absolute = Number.isFinite(Number(daily.growth)) ? Number(daily.growth) : latest - previous;
  const percent = Number.isFinite(Number(daily.growthPercent)) ? Number(daily.growthPercent) : (previous > 0 ? (absolute / previous) * 100 : null);
  return {
    latest,
    previous,
    absolute,
    percent,
    hasTrend: latest > 0 || previous > 0
  };
}

function resolveUsagePercentage(value) {
  if (!value || !Number.isFinite(value.max) || value.max <= 0) {
    return 0;
  }
  const max = Number(value.max);
  const used = Number(value.used || 0);
  return Math.max(0, Math.min(100, Math.round((used / max) * 100)));
}

function formatNumber(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) {
    return '-';
  }
  return number.toLocaleString('de-DE');
}

function formatUsageBasis(usage, unit) {
  if (!usage || !Number.isFinite(usage.max) || usage.max <= 0) {
    return '-';
  }
  const suffix = unit ? ' ' + unit : '';
  return formatNumber(usage.used) + suffix + ' genutzt von ' + formatNumber(usage.max) + suffix + ' · Rest ' + formatNumber(usage.remaining) + suffix;
}

function resolveApiThrottlePolicy(apiUsage) {
  const percentage = resolveUsagePercentage(apiUsage);
  if (!apiUsage || !Number.isFinite(apiUsage.max) || apiUsage.max <= 0) {
    return { label: 'Adaptive Cache: -', badgeClass: 'bg-secondary' };
  }

  if (percentage >= 95) {
    return { label: 'Adaptive Cache: 180s (kritisch)', badgeClass: 'bg-danger' };
  }
  if (percentage >= 90) {
    return { label: 'Adaptive Cache: 120s (hoch)', badgeClass: 'bg-danger' };
  }
  if (percentage >= 80) {
    return { label: 'Adaptive Cache: 60s (erhoeht)', badgeClass: 'bg-warning text-dark' };
  }
  if (percentage >= 65) {
    return { label: 'Adaptive Cache: 30s (moderat)', badgeClass: 'bg-warning text-dark' };
  }

  return { label: 'Adaptive Cache: 10s (normal)', badgeClass: 'bg-success' };
}

function renderApiThrottleBadge(apiUsage) {
  const badge = document.getElementById('sf-api-throttle-badge');
  if (!badge) {
    return;
  }

  const policy = resolveApiThrottlePolicy(apiUsage);
  badge.textContent = policy.label;
  badge.className = 'badge rounded-pill ' + policy.badgeClass;
}

function resolveGaugeColor(percentage, fallbackColor) {
  if (percentage >= 90) {
    return '#c24a4a';
  }
  if (percentage >= 70) {
    return '#c28b2c';
  }
  return fallbackColor;
}

function renderSalesforceGaugeChart(canvasId, percentage, color) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || typeof window.Chart !== 'function') {
    return;
  }

  const value = Math.max(0, Math.min(100, Number(percentage || 0)));
  const rest = Math.max(0, 100 - value);
  const data = [value, rest];
  const backgroundColor = [color, 'rgba(148, 163, 184, 0.22)'];

  if (salesforceGaugeCharts[canvasId]) {
    salesforceGaugeCharts[canvasId].data.datasets[0].data = data;
    salesforceGaugeCharts[canvasId].data.datasets[0].backgroundColor = backgroundColor;
    salesforceGaugeCharts[canvasId].update('none');
    return;
  }

  salesforceGaugeCharts[canvasId] = new window.Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: ['Genutzt', 'Verfuegbar'],
      datasets: [{
        data,
        backgroundColor,
        borderWidth: 0,
        hoverOffset: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      rotation: -90,
      circumference: 180,
      cutout: '72%',
      layout: {
        padding: 0
      },
      animation: {
        duration: 180
      },
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          enabled: false
        }
      }
    }
  });
}

function renderLimitGauge(gaugeId, valueId, usage, detailId, unit) {
  const valueEl = document.getElementById(valueId);
  const detailEl = detailId ? document.getElementById(detailId) : null;
  if (!valueEl) {
    return;
  }

  if (!usage || !Number.isFinite(usage.max) || usage.max <= 0) {
    renderSalesforceGaugeChart(gaugeId, 0, '#94a3b8');
    valueEl.textContent = '-';
    if (detailEl) {
      detailEl.textContent = '-';
    }
    return;
  }

  const percentage = resolveUsagePercentage(usage);
  renderSalesforceGaugeChart(gaugeId, percentage, resolveGaugeColor(percentage, gaugeId === 'sf-data-gauge' ? '#1f7d57' : '#2f69a8'));
  valueEl.textContent = percentage + '%';
  if (detailEl) {
    detailEl.textContent = formatUsageBasis(usage, unit);
    detailEl.title = 'Berechnung: genutzt / Limit * 100';
  }
}

function renderApiHourlyAverageGauge(apiUsage) {
  const valueEl = document.getElementById('sf-file-gauge-value');
  const detailEl = document.getElementById('sf-api-hourly-average');
  const used = Number(apiUsage?.used || 0);
  const max = Number(apiUsage?.max || 0);
  if (!Number.isFinite(used) || used <= 0 || !Number.isFinite(max) || max <= 0) {
    renderSalesforceGaugeChart('sf-file-gauge', 0, '#94a3b8');
    if (valueEl) {
      valueEl.textContent = '-';
    }
    if (detailEl) {
      detailEl.textContent = '-';
    }
    return;
  }

  const hourlyAverage = Math.round(used / 24);
  const usagePercentage = resolveUsagePercentage(apiUsage);
  renderSalesforceGaugeChart('sf-file-gauge', usagePercentage, resolveGaugeColor(usagePercentage, '#7b5ea7'));
  if (valueEl) {
    valueEl.textContent = String(hourlyAverage) + '/h';
  }
  if (detailEl) {
    detailEl.textContent = formatNumber(hourlyAverage) + ' Calls/h aus ' + formatNumber(used) + ' Calls/24h';
    detailEl.title = 'Berechnung: genutzte DailyApiRequests / 24';
  }
}

function renderDataGrowthGauge() {
  const valueEl = document.getElementById('sf-data-growth-gauge-value');
  const detailEl = document.getElementById('sf-data-growth');
  const growth = resolveRecordsGrowthMetric();
  if (!growth.hasTrend) {
    renderSalesforceGaugeChart('sf-data-growth-gauge', 0, '#94a3b8');
    if (valueEl) {
      valueEl.textContent = '-';
    }
    if (detailEl) {
      detailEl.textContent = '-';
      detailEl.title = 'Berechnung: heute erzeugte Datensätze aus MSD_RecordsSucceeded__c';
    }
    return;
  }

  const cappedGaugeValue = growth.percent === null
    ? Math.min(100, Math.abs(growth.absolute))
    : Math.min(100, Math.round(Math.abs(growth.percent)));
  renderSalesforceGaugeChart('sf-data-growth-gauge', cappedGaugeValue, growth.absolute > 0 ? '#c28b2c' : '#1f7d57');
  const prefix = growth.absolute >= 0 ? '+' : '';
  const percentText = growth.percent === null
    ? ''
    : ' (' + (growth.percent >= 0 ? '+' : '') + growth.percent.toFixed(1) + '%)';
  if (valueEl) {
    valueEl.textContent = formatNumber(growth.latest);
  }
  if (detailEl) {
    detailEl.textContent = formatNumber(growth.latest) + ' heute erzeugt · ' + prefix + formatNumber(growth.absolute) + ' ggü. gestern' + percentText;
    detailEl.title = 'Berechnung: heutige erfolgreiche Datensätze (MSD_RecordsSucceeded__c) minus gestrige erfolgreiche Datensätze';
  }
}

function resolveSalesforceOrgUrl(overview) {
  const raw = String(overview?.instanceUrl || overview?.domain || '').trim();
  if (!raw) {
    return '';
  }
  const withoutLeadingSlashes = raw.startsWith('//') ? raw.slice(2) : raw.replaceAll(' ', '');
  const normalizedHost = withoutLeadingSlashes.replaceAll('/', '');
  return raw.toLowerCase().startsWith('http://') || raw.toLowerCase().startsWith('https://')
    ? raw
    : 'https://' + (normalizedHost.startsWith(':') ? normalizedHost.slice(1) : normalizedHost);
}

function renderSalesforceOverview(overview) {
  state.salesforceOverview = overview || null;

  const setText = (id, value) => {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = String(value || '-');
    }
  };

  const orgUrl = resolveSalesforceOrgUrl(overview);
  const orgUrlEl = document.getElementById('sf-org-url');
  if (orgUrlEl) {
    if (orgUrl) {
      const orgUrlLabel = orgUrl
        .replace('https://', '')
        .replace('http://', '');
      orgUrlEl.textContent = orgUrlLabel.endsWith('/') ? orgUrlLabel.slice(0, -1) : orgUrlLabel;
      orgUrlEl.href = orgUrl;
      orgUrlEl.classList.remove('text-secondary');
    } else {
      orgUrlEl.textContent = '-';
      orgUrlEl.removeAttribute('href');
      orgUrlEl.classList.add('text-secondary');
    }
  }
  setText('sf-org-environment', overview?.environment || '-');
  setText('sf-domain', overview?.domain || overview?.instanceUrl || '-');
  setText('sf-environment', overview?.environment || '-');
  setText('sf-api-usage', formatUsageBlock(overview?.apiUsage));
  setText('sf-data-storage', formatUsageBlock(overview?.dataStorageMb, 'MB'));
  renderApiThrottleBadge(overview?.apiUsage);
  renderLimitGauge('sf-api-gauge', 'sf-api-gauge-value', overview?.apiUsage, 'sf-api-gauge-detail');
  renderLimitGauge('sf-data-gauge', 'sf-data-gauge-value', overview?.dataStorageMb, 'sf-data-gauge-detail', 'MB');
  renderApiHourlyAverageGauge(overview?.apiUsage);
  renderDataGrowthGauge();
}

