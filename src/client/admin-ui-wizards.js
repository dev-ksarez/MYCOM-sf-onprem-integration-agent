// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-wizards
// Source lines: 8134–9266
// ──────────────────────────────────────────────────────────────────────

function getConnectorWizardTotalSteps() {
  return 4;
}

function renderConnectorWizardStep() {
  const currentStep = Math.max(1, Math.min(getConnectorWizardTotalSteps(), Number(state.connectorWizardStep) || 1));
  state.connectorWizardStep = currentStep;

  document.querySelectorAll('[data-step-panel]').forEach((panel) => {
    const step = Number(panel.getAttribute('data-step-panel') || '0');
    panel.classList.toggle('d-none', step !== currentStep);
  });

  document.querySelectorAll('#con-wizard-steps .connector-wizard-step').forEach((button) => {
    const step = Number(button.getAttribute('data-step') || '0');
    button.classList.toggle('is-active', step === currentStep);
    button.classList.toggle('is-complete', step < currentStep);
  });

  const backButton = document.getElementById('con-wizard-back');
  const nextButton = document.getElementById('con-wizard-next');
  const saveButton = document.getElementById('save-connector');
  const validateButton = document.getElementById('test-connector');
  if (backButton) {
    backButton.disabled = currentStep === 1;
  }
  if (nextButton) {
    nextButton.classList.toggle('d-none', currentStep >= getConnectorWizardTotalSteps());
  }
  if (saveButton) {
    saveButton.classList.toggle('d-none', currentStep !== getConnectorWizardTotalSteps());
  }
  if (validateButton) {
    validateButton.classList.toggle('d-none', currentStep !== getConnectorWizardTotalSteps());
  }

  if (currentStep === getConnectorWizardTotalSteps()) {
    updateConnectorReviewStep();
  }
}

function collectConnectorParametersPreview() {
  let parsedParameters = {};
  const rawParameters = String(document.getElementById('con-parameters')?.value || '').trim();
  if (rawParameters) {
    parsedParameters = JSON.parse(rawParameters);
  }

  applyConnectorWizardSelection(true);
  const normalizedConnectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');

  if (normalizedConnectorType === 'FILE') {
    parsedParameters = mergeFileConnectorSettingsIntoParameters(parsedParameters);
  }
  if (normalizedConnectorType === 'FILE_BROWSE') {
    parsedParameters = mergeFileBrowseConnectorSettingsIntoParameters(parsedParameters);
  }
  if (isSqlConnectorType(normalizedConnectorType)) {
    parsedParameters = mergeMssqlConnectorSettingsIntoParameters(parsedParameters);
  }
  if (isRestConnectorType(normalizedConnectorType)) {
    parsedParameters = mergeRestConnectorSettingsIntoParameters(parsedParameters);
  }
  if (isEndpointConnectorType(normalizedConnectorType)) {
    parsedParameters = mergeRestConnectorSettingsIntoParameters(parsedParameters);
  }
  if (isBinaryImportConnectorType(normalizedConnectorType)) {
    parsedParameters = mergeBinaryImportConnectorSettingsIntoParameters(parsedParameters);
  }
  parsedParameters = mergeConnectorNotificationSettingsIntoParameters(parsedParameters);

  return {
    connectorType: normalizedConnectorType,
    parameters: parsedParameters
  };
}

function fillConnectorNotificationSettingsFromParameters(parameters) {
  const params = parameters || {};
  document.getElementById('con-task-notify-enabled').checked = params.notificationTaskEnabled === true;
  document.getElementById('con-task-owner-id').value = String(params.notificationTaskOwnerId || '');
  const errorClassSelect = document.getElementById('con-task-error-classes');
  const selectedClasses = Array.isArray(params.notificationTaskErrorClasses)
    ? params.notificationTaskErrorClasses
    : String(params.notificationTaskErrorClasses || '')
        .split(',')
        .map((value) => value.trim().toUpperCase())
        .filter(Boolean);
  Array.from(errorClassSelect?.options || []).forEach((option) => {
    option.selected = selectedClasses.includes(String(option.value || '').trim().toUpperCase());
  });
}

function mergeConnectorNotificationSettingsIntoParameters(parameters) {
  const merged = { ...(parameters || {}) };
  const enabled = !!document.getElementById('con-task-notify-enabled').checked;
  const ownerSelect = document.getElementById('con-task-owner-id');
  const ownerId = String(ownerSelect?.value || '').trim();
  const ownerUsername = String(ownerSelect?.selectedOptions?.[0]?.getAttribute('data-username') || '').trim();
  const errorClassSelect = document.getElementById('con-task-error-classes');
  const errorClasses = Array.from(errorClassSelect?.selectedOptions || [])
    .map((option) => String(option.value || '').trim().toUpperCase())
    .filter(Boolean);

  if (enabled && ownerId) {
    merged.notificationTaskEnabled = true;
    merged.notificationTaskOwnerId = ownerId;
    merged.notificationTaskOwnerUsername = ownerUsername;
    merged.notificationTaskErrorClasses = errorClasses.length ? errorClasses : connectorNotificationErrorClassOptions.slice();
  } else {
    delete merged.notificationTaskEnabled;
    delete merged.notificationTaskOwnerId;
    delete merged.notificationTaskOwnerUsername;
    delete merged.notificationTaskErrorClasses;
  }

  return merged;
}

function updateConnectorReviewStep() {
  const summaryEl = document.getElementById('con-review-summary');
  const jsonEl = document.getElementById('con-review-json');
  if (!summaryEl || !jsonEl) {
    return;
  }

  const preview = collectConnectorParametersPreview();
  const summaryItems = [
    ['Typ', preview.connectorType || '-'],
    ['Name', String(document.getElementById('con-name')?.value || '-').trim() || '-'],
    ['Target System', String(document.getElementById('con-target-system')?.value || '-').trim() || '-'],
    ['Direction', String(document.getElementById('con-direction')?.value || '-').trim() || '-'],
    ['Timeout', String(document.getElementById('con-timeout')?.value || '-').trim() || '-'],
    ['Retries', String(document.getElementById('con-retries')?.value || '-').trim() || '-']
  ];

  summaryEl.innerHTML = summaryItems.map((item) =>
    '<div class="connector-review-row"><span class="connector-review-label">' + esc(item[0]) + '</span><span class="connector-review-value">' + esc(item[1]) + '</span></div>'
  ).join('');
  jsonEl.textContent = JSON.stringify(preview.parameters, null, 2);
}

function validateConnectorWizardStep(step) {
  clearConnectorModalError();

  if (step === 1) {
    const wizardType = String(document.getElementById('con-wizard-type')?.value || '').trim();
    if (!wizardType) {
      throw new Error('Bitte zuerst einen Connectortyp auswählen.');
    }
    return;
  }

  if (step === 2) {
    const name = String(document.getElementById('con-name')?.value || '').trim();
    if (!name) {
      throw new Error('Bitte einen Connector-Namen eingeben.');
    }
    return;
  }

  if (step === 3) {
    const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
    if (isSqlConnectorType(connectorType)) {
      const mssqlAuthType = String(document.getElementById('con-mssql-auth-type')?.value || 'sql').trim();
      const mssqlDomain = String(document.getElementById('con-mssql-domain')?.value || '').trim();
      if (!String(document.getElementById('con-mssql-server')?.value || '').trim() ||
          !String(document.getElementById('con-mssql-database')?.value || '').trim() ||
          !String(document.getElementById('con-mssql-user')?.value || '').trim()) {
        throw new Error(connectorType === 'FILEMAKER'
          ? 'Bitte Base URL, Datenbank und Benutzer für den FileMaker-Connector angeben.'
          : connectorType === 'ORACLE'
            ? 'Bitte Hostname, Servicename/SID und Benutzername für den Oracle-Connector angeben.'
          : 'Bitte Host, Datenbank und Benutzer für den SQL-Connector angeben.');
      }
      if (connectorType === 'MSSQL' && mssqlAuthType === 'windows' && !mssqlDomain) {
        throw new Error('Bitte die Windows Domain für den MSSQL-Connector angeben.');
      }
    }
    if (connectorType === 'FILE') {
      if (!String(document.getElementById('con-file-base-path')?.value || '').trim()) {
        throw new Error('Bitte mindestens den Base Path für den Datei-Connector angeben.');
      }
    }
    if (connectorType === 'FILE_BROWSE') {
      if (!String(document.getElementById('con-filebrowse-base-path')?.value || '').trim()) {
        throw new Error('Bitte den Base Path für die Geräteakte angeben.');
      }
    }
    if (connectorType === 'REST_API') {
      if (!String(document.getElementById('con-rest-base-url')?.value || '').trim()) {
        throw new Error('Bitte eine Base URL für den REST-Connector angeben.');
      }
    }
    if (isEndpointConnectorType(connectorType)) {
      const rootPath = String(document.getElementById('con-rest-resource-path')?.value || document.getElementById('con-rest-base-url')?.value || '').trim();
      if (!rootPath) {
        throw new Error('Bitte einen Root Path für den Agent-Endpunkt angeben, z. B. /api/inbound/orders.');
      }
      if (!rootPath.startsWith('/')) {
        throw new Error('Der Root Path für Agent-Endpunkte muss mit / beginnen.');
      }
    }
    if (connectorType === 'FILE_BINARY_SF_IMPORT') {
      if (!String(document.getElementById('con-binary-base-path')?.value || '').trim()) {
        throw new Error('Bitte den Base Path für den Binärimport angeben.');
      }
    }
  }
}

function goToConnectorWizardStep(nextStep) {
  state.connectorWizardStep = Math.max(1, Math.min(getConnectorWizardTotalSteps(), nextStep));
  renderConnectorWizardStep();
}

function getScheduleWizardTotalSteps() {
  return 5;
}

function renderScheduleWizardStep() {
  const currentStep = Math.max(1, Math.min(getScheduleWizardTotalSteps(), Number(state.scheduleWizardStep) || 1));
  state.scheduleWizardStep = currentStep;

  if (currentStep === 2) {
    const selectedConnectorId = String(document.getElementById('sch-connector')?.value || '').trim();
    if (selectedConnectorId) {
      applyScheduleSourceSystemFromConnector(selectedConnectorId, { force: false });
      applyScheduleSourceTypeFromConnector(selectedConnectorId, { force: false });
      applyScheduleSourceFieldPolicy(selectedConnectorId);
    }
  }

  updateScheduleTypeUi();

  document.querySelectorAll('[data-sch-step-panel]').forEach((panel) => {
    const step = Number(panel.getAttribute('data-sch-step-panel') || '0');
    const isActive = step === currentStep;
    panel.classList.toggle('show', isActive);
    panel.classList.toggle('active', isActive);
    panel.classList.toggle('d-none', !isActive);
  });

  document.querySelectorAll('#sch-wizard-steps [data-sch-step]').forEach((button) => {
    const step = Number(button.getAttribute('data-sch-step') || '0');
    button.classList.toggle('is-active', step === currentStep);
    button.classList.toggle('is-complete', step < currentStep);
  });

  const backButton = document.getElementById('sch-wizard-back');
  const nextButton = document.getElementById('sch-wizard-next');
  const saveButton = document.getElementById('save-schedule');
  const hint = document.getElementById('sch-wizard-hint');
  if (backButton) {
    backButton.disabled = currentStep === 1;
  }
  if (nextButton) {
    nextButton.classList.toggle('d-none', currentStep >= getScheduleWizardTotalSteps());
  }
  if (saveButton) {
    saveButton.classList.toggle('d-none', currentStep !== getScheduleWizardTotalSteps());
  }
  if (hint) {
    const labels = {
      1: 'Basisdaten und Einordnung des Schedulers.',
      2: 'Quelle auswählen, Delta konfigurieren und Vorschau testen.',
      3: 'Zielsystem, Objekt und technische Zieldefinition festlegen.',
      4: 'Zeitsteuerung festlegen oder vom Parent übernehmen.',
      5: 'Mapping prüfen und den Scheduler speichern.'
    };
    hint.textContent = 'Assistent aktiv: ' + (labels[currentStep] || 'Scheduler Schritt für Schritt konfigurieren.');
  }
}

function validateScheduleWizardStep(step) {
  clearModalError();

  if (step === 1) {
    if (!String(document.getElementById('sch-name')?.value || '').trim()) {
      throw new Error('Bitte einen Namen für den Scheduler eingeben.');
    }
    if (!String(document.getElementById('sch-connector')?.value || '').trim()) {
      throw new Error('Bitte einen Connector auswählen.');
    }
    return;
  }

  if (step === 2) {
    if (!String(document.getElementById('sch-source-type')?.value || '').trim()) {
      throw new Error('Bitte einen Source Type wählen.');
    }
    if (!String(document.getElementById('sch-source-definition')?.value || '').trim()) {
      throw new Error('Bitte eine Source Definition oder Abfrage angeben.');
    }
    return;
  }

  if (step === 3) {
    if (!String(document.getElementById('sch-target-type')?.value || '').trim()) {
      throw new Error('Bitte einen Target Type wählen.');
    }
    const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
    if (!isFileScheduleTargetType(targetType) && !String(document.getElementById('sch-object')?.value || '').trim()) {
      throw new Error('Bitte ein Zielobjekt wählen.');
    }
    if (!isFileScheduleTargetType(targetType) && !String(document.getElementById('sch-operation')?.value || '').trim()) {
      throw new Error('Bitte eine Operation wählen.');
    }
    if (isFileScheduleTargetType(targetType) && !String(buildScheduleTargetDefinitionValue() || '').trim()) {
      throw new Error('Bitte eine Target Definition mit Dateiname oder Datei-JSON angeben.');
    }
    return;
  }

  if (step === 4 && !document.getElementById('sch-inherit-parent-timing')?.checked && String(document.getElementById('sch-source-type')?.value || '').trim().toUpperCase() !== 'ENDPOINT') {
    const hasWeekday = Array.from(document.querySelectorAll('#sch-weekdays input')).some((input) => input.checked);
    if (!hasWeekday) {
      throw new Error('Bitte mindestens einen Wochentag für die Zeitsteuerung auswählen.');
    }
    if (!String(document.getElementById('sch-timing-time')?.value || '').trim()) {
      throw new Error('Bitte eine Uhrzeit für die Zeitsteuerung wählen.');
    }
  }
}

function goToScheduleWizardStep(nextStep) {
  state.scheduleWizardStep = Math.max(1, Math.min(getScheduleWizardTotalSteps(), nextStep));
  renderScheduleWizardStep();
}

function advanceScheduleWizardStep() {
  try {
    validateScheduleWizardStep(state.scheduleWizardStep);
    goToScheduleWizardStep(state.scheduleWizardStep + 1);
  } catch (error) {
    showModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
  }
}

function advanceConnectorWizardStep() {
  try {
    validateConnectorWizardStep(state.connectorWizardStep);
    goToConnectorWizardStep(state.connectorWizardStep + 1);
  } catch (error) {
    showConnectorModalError(error?.message || 'Schritt konnte nicht abgeschlossen werden.');
  }
}

async function openLogsByBucket(bucket, type, connectorName) {
  const path = '/api/logs?start=' + encodeURIComponent(bucket.start) +
    '&end=' + encodeURIComponent(bucket.end) +
    '&type=' + encodeURIComponent(type) +
    '&connector=' + encodeURIComponent(connectorName || '') +
    '&limit=300';

  const result = await safeRequest(path, { items: [] });
  const rows = result.items || [];
  document.getElementById('logs-modal-title').textContent =
    'Logliste ' + (type === 'error' ? '(Fehler)' : '(Alle)') +
    (connectorName ? ' | ' + connectorName : '') +
    ' | ' + new Date(bucket.start).toLocaleString('de-DE') +
    ' - ' + new Date(bucket.end).toLocaleString('de-DE');

  const body = document.getElementById('logs-modal-body');
  const logsFilter = document.getElementById('logs-filter');
  if (logsFilter) {
    logsFilter.value = '';
    try {
      localStorage.removeItem(TABLE_STORAGE_KEY + '.logs');
    } catch (e) {
      // Ignore storage errors
    }
  }
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="6" class="text-secondary p-3">Keine Logs in diesem Zeitraum.</td></tr>';
    logsModal.show();
    return;
  }

  body.innerHTML = rows.map((entry) =>
    '<tr>' +
      '<td>' + esc(entry.createdAt ? new Date(entry.createdAt).toLocaleString('de-DE') : '-') + '</td>' +
      '<td>' + esc(entry.level || '-') + '</td>' +
      '<td>' + esc(entry.connectorName || '-') + '</td>' +
      '<td>' + esc(entry.scheduleName || '-') + '</td>' +
      '<td>' + esc(entry.step || '-') + '</td>' +
      '<td style="white-space: normal; word-break: break-word; overflow-wrap: anywhere;">' + esc(entry.message || '-') + '</td>' +
    '</tr>'
  ).join('');

  logsModal.show();
}

async function openFailedRecordsForRun(runId) {
  const normalizedRunId = String(runId || '').trim();
  if (!normalizedRunId) {
    return;
  }

  const result = await safeRequest('/api/runs/' + encodeURIComponent(normalizedRunId) + '/failed-records', {
    runId: normalizedRunId,
    total: 0,
    items: []
  });
  const rows = Array.isArray(result?.items) ? result.items : [];
  state.failedRunRecordsExport = {
    runId: normalizedRunId,
    scheduleName: String(result?.scheduleName || '').trim(),
    items: rows
  };
  updateFailedRecordsExportButtons();

  const title = document.getElementById('failed-records-modal-title');
  if (title) {
    const scheduleName = String(result?.scheduleName || '').trim();
    title.textContent = scheduleName
      ? 'Fehlgeschlagene Datensätze | ' + scheduleName + ' | Run ' + normalizedRunId
      : 'Fehlgeschlagene Datensätze | Run ' + normalizedRunId;
  }

  const body = document.getElementById('failed-records-modal-body');
  if (!body) {
    return;
  }

  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="6" class="text-secondary p-3">Keine fehlgeschlagenen Datensätze für diesen Run gespeichert.</td></tr>';
    failedRecordsModal.show();
    return;
  }

  const renderJsonCell = (value) => {
    if (!value || typeof value !== 'object') {
      return '<span class="text-secondary">-</span>';
    }
    return '<pre class="failed-record-json">' + esc(JSON.stringify(value, null, 2)) + '</pre>';
  };

  body.innerHTML = rows.map((item, idx) =>
    '<tr>' +
      '<td>' + esc(Number.isFinite(Number(item.rowIndex)) ? Number(item.rowIndex) + 1 : idx + 1) + '</td>' +
      '<td>' + esc(item.externalKey || '-') + '</td>' +
      '<td>' + esc(item.statusCode || '-') + '</td>' +
      '<td style="white-space: normal; word-break: break-word; overflow-wrap: anywhere;">' + esc(item.message || '-') + '</td>' +
      '<td>' + renderJsonCell(item.sourceRecord) + '</td>' +
      '<td>' + renderJsonCell(item.mappedRecord) + '</td>' +
    '</tr>'
  ).join('');

  failedRecordsModal.show();
}

function updateFailedRecordsExportButtons() {
  const csvButton = document.getElementById('failed-records-export-csv');
  const jsonButton = document.getElementById('failed-records-export-json');
  const hasRows = Array.isArray(state.failedRunRecordsExport?.items) && state.failedRunRecordsExport.items.length > 0;
  if (csvButton) {
    csvButton.disabled = !hasRows;
  }
  if (jsonButton) {
    jsonButton.disabled = !hasRows;
  }
}

function downloadTextAsFile(content, fileName, mimeType) {
  const blob = new Blob([content], { type: mimeType || 'text/plain;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function escapeCsvCell(value) {
  const raw = String(value ?? '');
  if (/[";\n\r]/.test(raw)) {
    return '"' + raw.replace(/"/g, '""') + '"';
  }
  return raw;
}

function exportFailedRecordsAsJson() {
  const payload = state.failedRunRecordsExport;
  if (!payload || !Array.isArray(payload.items) || !payload.items.length) {
    showError('Keine fehlgeschlagenen Datensätze zum Exportieren vorhanden.');
    return;
  }

  const fileName = 'failed-records-' + (payload.runId || 'run') + '.json';
  downloadTextAsFile(JSON.stringify(payload, null, 2), fileName, 'application/json;charset=utf-8');
}

function exportFailedRecordsAsCsv() {
  const payload = state.failedRunRecordsExport;
  if (!payload || !Array.isArray(payload.items) || !payload.items.length) {
    showError('Keine fehlgeschlagenen Datensätze zum Exportieren vorhanden.');
    return;
  }

  const header = ['rowIndex', 'externalKey', 'statusCode', 'message', 'retryable', 'sourceRecord', 'mappedRecord'];
  const lines = [header.join(';')];
  payload.items.forEach((item) => {
    lines.push([
      escapeCsvCell(Number.isFinite(Number(item.rowIndex)) ? Number(item.rowIndex) + 1 : ''),
      escapeCsvCell(item.externalKey || ''),
      escapeCsvCell(item.statusCode || ''),
      escapeCsvCell(item.message || ''),
      escapeCsvCell(item.retryable === true ? 'true' : 'false'),
      escapeCsvCell(item.sourceRecord ? JSON.stringify(item.sourceRecord) : ''),
      escapeCsvCell(item.mappedRecord ? JSON.stringify(item.mappedRecord) : '')
    ].join(';'));
  });

  const fileName = 'failed-records-' + (payload.runId || 'run') + '.csv';
  downloadTextAsFile(lines.join('\n'), fileName, 'text/csv;charset=utf-8');
}

function updateServiceCpuSparkline(cpuPercent) {
  const sparklinePath = document.getElementById('kpi-service-cpu-sparkline-path');
  const sparklineDot = document.getElementById('kpi-service-cpu-sparkline-dot');
  if (!sparklinePath || !sparklineDot) {
    return;
  }

  const hasCpuValue = Number.isFinite(cpuPercent);
  if (hasCpuValue) {
    state.cpuLoadHistory.push(Math.max(0, Math.min(100, Math.round(cpuPercent))));
  }

  const maxPoints = 18;
  if (state.cpuLoadHistory.length > maxPoints) {
    state.cpuLoadHistory = state.cpuLoadHistory.slice(-maxPoints);
  }

  if (!state.cpuLoadHistory.length) {
    sparklinePath.setAttribute('d', '');
    sparklineDot.setAttribute('cx', '0');
    sparklineDot.setAttribute('cy', '0');
    return;
  }

  const width = 120;
  const height = 20;
  const chartPadding = 1;
  const points = state.cpuLoadHistory.map((value, index, list) => {
    const x = list.length <= 1 ? chartPadding : chartPadding + (index * (width - chartPadding * 2)) / (list.length - 1);
    const y = height - chartPadding - (Math.max(0, Math.min(100, value)) / 100) * (height - chartPadding * 2);
    return { x, y };
  });

  const pathData = points
    .map((point, index) => (index === 0 ? 'M' : 'L') + point.x.toFixed(2) + ' ' + point.y.toFixed(2))
    .join(' ');
  sparklinePath.setAttribute('d', pathData);

  const lastPoint = points[points.length - 1];
  sparklineDot.setAttribute('cx', lastPoint.x.toFixed(2));
  sparklineDot.setAttribute('cy', lastPoint.y.toFixed(2));

  const lastValue = state.cpuLoadHistory[state.cpuLoadHistory.length - 1];
  sparklinePath.className.baseVal = 'kpi-sparkline-path';
  sparklineDot.className.baseVal = 'kpi-sparkline-dot';
  if (lastValue >= 80) {
    sparklinePath.classList.add('kpi-sparkline-danger');
    sparklineDot.classList.add('kpi-sparkline-danger');
  } else if (lastValue >= 55) {
    sparklinePath.classList.add('kpi-sparkline-warn');
    sparklineDot.classList.add('kpi-sparkline-warn');
  } else {
    sparklinePath.classList.add('kpi-sparkline-ok');
    sparklineDot.classList.add('kpi-sparkline-ok');
  }
}

function setAgentAnalysisStatusBadge(element, status) {
  if (!element) {
    return;
  }

  const normalized = String(status || '').toLowerCase();
  let className = 'badge bg-primary';
  if (normalized === 'gesund') {
    className = 'badge bg-success';
  } else if (normalized === 'stabil') {
    className = 'badge bg-primary';
  } else if (normalized === 'beobachten') {
    className = 'badge bg-warning text-dark';
  } else if (normalized === 'kritisch') {
    className = 'badge bg-danger';
  }

  element.className = className;
  element.textContent = status || '-';
}

function renderAIDashboardAnalysis(analysis) {
  if (!analysis || typeof analysis !== 'object') {
    return;
  }

  const scoreElement = document.getElementById('agent-analysis-score');
  const statusElement = document.getElementById('agent-analysis-status');
  const runtimeElement = document.getElementById('agent-analysis-runtime');
  const errorsElement = document.getElementById('agent-analysis-errors');
  const growthElement = document.getElementById('agent-analysis-growth');
  const updatedElement = document.getElementById('agent-analysis-updated');
  const summaryElement = document.getElementById('agent-analysis-summary');
  const recommendationsElement = document.getElementById('agent-analysis-recommendations');

  const score = Number(analysis.score);
  if (scoreElement && Number.isFinite(score)) {
    scoreElement.textContent = String(Math.round(score));
    scoreElement.classList.remove('text-success', 'text-warning', 'text-danger');
    if (score >= 85) {
      scoreElement.classList.add('text-success');
    } else if (score >= 45) {
      scoreElement.classList.add('text-warning');
    } else {
      scoreElement.classList.add('text-danger');
    }
  }

  setAgentAnalysisStatusBadge(statusElement, String(analysis.status || '-'));

  if (runtimeElement) {
    runtimeElement.textContent = String(analysis?.insights?.runtime || '-');
  }
  if (errorsElement) {
    errorsElement.textContent = String(analysis?.insights?.errors || '-');
  }
  if (growthElement) {
    growthElement.textContent = String(analysis?.insights?.dataGrowth || '-');
  }
  if (summaryElement) {
    summaryElement.textContent = String(analysis.summary || '-');
  }
  if (updatedElement) {
    updatedElement.textContent = formatDate(new Date().toISOString(), 'short');
  }

  if (recommendationsElement) {
    const recommendations = Array.isArray(analysis.recommendations)
      ? analysis.recommendations.filter((item) => String(item || '').trim().length > 0)
      : [];
    if (!recommendations.length) {
      recommendationsElement.innerHTML = '<li>Keine Empfehlungen</li>';
    } else {
      recommendationsElement.innerHTML = recommendations
        .slice(0, 4)
        .map((item) => '<li>' + esc(String(item)) + '</li>')
        .join('');
    }
  }
}

function ensureAIDashboardAnalysis(metrics) {
  const summaryElement = document.getElementById('agent-analysis-summary');
  const now = Date.now();
  const hasFreshCache = state.aiDashboardAnalysis && (now - Number(state.aiDashboardAnalysisAt || 0) < 60000);

  if (hasFreshCache) {
    renderAIDashboardAnalysis(state.aiDashboardAnalysis);
    return;
  }

  if (state.aiDashboardAnalysisPromise) {
    return;
  }

  if (summaryElement) {
    summaryElement.textContent = 'KI analysiert Dashboarddaten...';
  }

  state.aiDashboardAnalysisPromise = requestJson('/api/ai/analyze-dashboard', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(metrics || {})
  })
    .then((analysis) => {
      state.aiDashboardAnalysis = analysis;
      state.aiDashboardAnalysisAt = Date.now();
      renderAIDashboardAnalysis(analysis);
    })
    .catch((error) => {
      if (summaryElement) {
        summaryElement.textContent = 'KI-Analyse derzeit nicht verfügbar';
      }
      console.warn('Dashboard KI-Analyse fehlgeschlagen', error);
    })
    .finally(() => {
      state.aiDashboardAnalysisPromise = null;
    });
}

function renderOverview(healthData) {
  const previousSnapshot = state.previousOverviewSnapshot;
  const formatByteSize = (bytes) => {
    const numericBytes = Number(bytes);
    if (!Number.isFinite(numericBytes) || numericBytes < 0) {
      return null;
    }

    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = numericBytes;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }

    const digits = value >= 10 || unitIndex === 0 ? 0 : 1;
    return value.toFixed(digits) + ' ' + units[unitIndex];
  };

  const formatUsageMetric = (usedBytes, totalBytes) => {
    const used = Number(usedBytes);
    const total = Number(totalBytes);
    if (!Number.isFinite(used) || !Number.isFinite(total) || total <= 0) {
      return 'nicht verfuegbar';
    }

    const percentage = Math.max(0, Math.min(100, Math.round((used / total) * 100)));
    return formatByteSize(used) + ' / ' + formatByteSize(total) + ' (' + percentage + '%)';
  };

  renderOverviewStatsRangeButtons();
  state.health = healthData || {};
  renderOverviewLogRetentionStatus();
  document.getElementById('kpi-service').textContent = healthData.service || '-';
  const serviceVersionText = document.getElementById('kpi-service-version');
  if (serviceVersionText) {
    const agentVersion = String(healthData.agentVersion || '').trim().replace(/^v/i, '');
    if (agentVersion) {
      serviceVersionText.textContent = 'v' + agentVersion;
    }
  }

  const cpuPercent = Number(healthData.cpuLoadPercent);
  const hasCpuPercent = Number.isFinite(cpuPercent);
  const serviceCpuValue = document.getElementById('kpi-service-cpu-value');
  const serviceCpuText = document.getElementById('kpi-service-cpu-text');
  const normalizedCpuPercent = hasCpuPercent ? Math.max(0, Math.min(100, Math.round(cpuPercent))) : null;
  renderSalesforceGaugeChart('kpi-service-cpu-gauge', normalizedCpuPercent || 0, resolveGaugeColor(normalizedCpuPercent || 0, '#2f69a8'));
  if (serviceCpuValue) {
    serviceCpuValue.textContent = normalizedCpuPercent === null ? '-' : normalizedCpuPercent + '%';
  }
  if (serviceCpuText) {
    serviceCpuText.textContent = normalizedCpuPercent === null
      ? 'CPU Last: nicht verfuegbar'
      : 'CPU Last: ' + normalizedCpuPercent + '%';
    serviceCpuText.title = 'Berechnung: Host-CPU-Auslastung aus OS-CPU-Samples';
  }
  const serviceOsText = document.getElementById('kpi-service-os');
  const serviceOsKind = document.getElementById('kpi-service-os-kind');
  const operatingSystem = String(healthData.operatingSystem || 'nicht verfuegbar');
  if (serviceOsText) {
    serviceOsText.textContent = operatingSystem;
  }
  if (serviceOsKind) {
    serviceOsKind.textContent = operatingSystem.split(' ')[0] || '-';
  }
  const serviceMemoryText = document.getElementById('kpi-service-memory');
  const serviceMemoryValue = document.getElementById('kpi-service-memory-value');
  const memoryUsedBytes = Number(healthData.memoryUsedBytes);
  const memoryTotalBytes = Number(healthData.memoryTotalBytes);
  const memoryPercent = Number.isFinite(memoryUsedBytes) && Number.isFinite(memoryTotalBytes) && memoryTotalBytes > 0
    ? Math.max(0, Math.min(100, Math.round((memoryUsedBytes / memoryTotalBytes) * 100)))
    : null;
  renderSalesforceGaugeChart('kpi-service-memory-gauge', memoryPercent || 0, resolveGaugeColor(memoryPercent || 0, '#1f7d57'));
  if (serviceMemoryValue) {
    serviceMemoryValue.textContent = memoryPercent === null ? '-' : memoryPercent + '%';
  }
  if (serviceMemoryText) {
    serviceMemoryText.textContent = 'RAM: ' + formatUsageMetric(healthData.memoryUsedBytes, healthData.memoryTotalBytes);
    serviceMemoryText.title = 'Berechnung: genutzter RAM / gesamter RAM * 100';
  }
  const serviceDiskText = document.getElementById('kpi-service-disk');
  const serviceDiskValue = document.getElementById('kpi-service-disk-value');
  const diskUsedBytes = Number(healthData.diskUsedBytes);
  const diskTotalBytes = Number(healthData.diskTotalBytes);
  const diskPercent = Number.isFinite(diskUsedBytes) && Number.isFinite(diskTotalBytes) && diskTotalBytes > 0
    ? Math.max(0, Math.min(100, Math.round((diskUsedBytes / diskTotalBytes) * 100)))
    : null;
  renderSalesforceGaugeChart('kpi-service-disk-gauge', diskPercent || 0, resolveGaugeColor(diskPercent || 0, '#7b5ea7'));
  if (serviceDiskValue) {
    serviceDiskValue.textContent = diskPercent === null ? '-' : diskPercent + '%';
  }
  if (serviceDiskText) {
    serviceDiskText.textContent = 'Disk: ' + formatUsageMetric(healthData.diskUsedBytes, healthData.diskTotalBytes);
    serviceDiskText.title = 'Berechnung: genutzter Speicher / gesamter Speicher * 100';
  }

  const runs = Array.isArray(state.runs) ? state.runs : [];
  const now = new Date();
  const rangeStart = getOverviewRangeStartDate(now, state.overviewStatsRange);
  const scopedRuns = runs.filter((run) => {
    if (!run || !run.startedAt) {
      return false;
    }
    const startedAt = new Date(run.startedAt);
    return !Number.isNaN(startedAt.getTime()) && startedAt >= rangeStart;
  });
  const schedules = Array.isArray(state.schedules) ? state.schedules : [];
  const migrations = Array.isArray(state.migrations) ? state.migrations : [];
  const normalizeStatus = (value) => String(value || '').trim().toLowerCase();

  const successCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'success').length;
  const failedCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'failed' || normalizeStatus(run.status) === 'error').length;
  const runningCount = scopedRuns.filter((run) => normalizeStatus(run.status) === 'running').length;
  const totalCount = scopedRuns.length;
  const successRate = totalCount > 0 ? Math.round((successCount / totalCount) * 100) : 0;
  const errorRate = totalCount > 0 ? Math.round((failedCount / totalCount) * 100) : 0;

  const inboundCount = schedules.filter((schedule) => String(schedule.direction || '').toLowerCase() === 'inbound').length;
  const outboundCount = schedules.filter((schedule) => String(schedule.direction || '').toLowerCase() === 'outbound').length;
  const autoDisabledCount = schedules.filter((schedule) => schedule.autoDisabledDueToErrors && schedule.active === false).length;
  const completedRunDurations = scopedRuns
    .map((run) => getRunDurationMs(run))
    .filter((duration) => duration !== null);
  const averageRunDurationMs = completedRunDurations.length
    ? completedRunDurations.reduce((sum, duration) => sum + duration, 0) / completedRunDurations.length
    : null;

  const latestRun = scopedRuns
    .filter((run) => run && run.startedAt)
    .sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime())[0];

  const sqliteObjects = migrations
    .flatMap((migration) => Array.isArray(migration?.objects) ? migration.objects : [])
    .filter((obj) => String(obj?.processingMode || obj?.stagingMode || '').trim().toLowerCase() === 'sqlite');
  const sqliteStatusTotals = sqliteObjects.reduce((totals, obj) => {
    const summary = obj && obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : null;
    if (summary) {
      Object.entries(summary).forEach(([key, value]) => {
        const normalizedKey = String(key || '').trim().toLowerCase();
        totals[normalizedKey] = Number(totals[normalizedKey] || 0) + (Number(value || 0) || 0);
      });
      return totals;
    }

    const fallbackStatus = String(obj?.stagingStatus || '').trim().toLowerCase();
    if (fallbackStatus) {
      totals[fallbackStatus] = Number(totals[fallbackStatus] || 0) + 1;
    }
    return totals;
  }, {});
  const sqlitePendingCount = Number(sqliteStatusTotals.pending || 0) + Number(sqliteStatusTotals.ready || 0);
  const sqliteSuccessCount = Number(sqliteStatusTotals.success || 0) + Number(sqliteStatusTotals.done || 0);
  const sqliteErrorCount = Number(sqliteStatusTotals.mapping_error || 0) + Number(sqliteStatusTotals.salesforce_error || 0) + Number(sqliteStatusTotals.error || 0);

  const successRateLabel = document.getElementById('kpi-success-rate');
  const errorRateLabel = document.getElementById('kpi-error-rate');
  const successRateBar = document.getElementById('kpi-success-rate-bar');
  const errorRateBar = document.getElementById('kpi-error-rate-bar');
  const runsSuccess = document.getElementById('kpi-runs-success');
  const runsFailed = document.getElementById('kpi-runs-failed');
  const runsRunning = document.getElementById('kpi-runs-running');
  const runsTotal = document.getElementById('kpi-runs-total');
  const inboundCounter = document.getElementById('kpi-inbound-count');
  const outboundCounter = document.getElementById('kpi-outbound-count');
  const averageRunDuration = document.getElementById('kpi-average-run-duration');
  const dailyRecordCount = document.getElementById('kpi-daily-record-count');
  const autoDisabledCounter = document.getElementById('kpi-auto-disabled-count');
  const lastRunAt = document.getElementById('kpi-last-run-at');
  const sqliteObjectsCounter = document.getElementById('kpi-sqlite-objects');
  const sqlitePendingCounter = document.getElementById('kpi-sqlite-pending');
  const sqliteSuccessCounter = document.getElementById('kpi-sqlite-success');
  const sqliteErrorsCounter = document.getElementById('kpi-sqlite-errors');
  const agentAnalysisScore = document.getElementById('agent-analysis-score');
  const agentAnalysisStatus = document.getElementById('agent-analysis-status');
  const agentAnalysisRuntime = document.getElementById('agent-analysis-runtime');
  const agentAnalysisErrors = document.getElementById('agent-analysis-errors');
  const agentAnalysisGrowth = document.getElementById('agent-analysis-growth');
  const agentAnalysisUpdated = document.getElementById('agent-analysis-updated');
  const agentAnalysisSummary = document.getElementById('agent-analysis-summary');
  const agentAnalysisRecommendations = document.getElementById('agent-analysis-recommendations');

  const updateKpiTrend = (elementId, delta, positiveWhenUp, neutralText) => {
    const element = document.getElementById(elementId);
    if (!element) {
      return;
    }

    if (delta === null || Number.isNaN(delta)) {
      element.className = 'kpi-trend kpi-trend-neutral';
      element.textContent = '• ' + neutralText;
      return;
    }

    if (delta === 0) {
      element.className = 'kpi-trend kpi-trend-neutral';
      element.textContent = '→ unverändert';
      return;
    }

    const isUp = delta > 0;
    const isPositive = positiveWhenUp ? isUp : !isUp;
    element.className = 'kpi-trend ' + (isPositive ? 'kpi-trend-positive' : 'kpi-trend-negative');
    element.textContent = (isUp ? '↑ ' : '↓ ') + Math.abs(delta) + ' seit letztem Update';
  };

  if (successRateLabel) {
    successRateLabel.textContent = successRate + '%';
  }
  if (errorRateLabel) {
    errorRateLabel.textContent = errorRate + '%';
  }
  if (successRateBar) {
    successRateBar.style.width = Math.max(0, Math.min(100, successRate)) + '%';
  }
  if (errorRateBar) {
    errorRateBar.style.width = Math.max(0, Math.min(100, errorRate)) + '%';
  }
  if (runsSuccess) {
    runsSuccess.textContent = String(successCount);
  }
  if (runsFailed) {
    runsFailed.textContent = String(failedCount);
  }
  if (runsRunning) {
    runsRunning.textContent = String(runningCount);
  }
  if (runsTotal) {
    runsTotal.textContent = String(totalCount);
  }
  if (inboundCounter) {
    inboundCounter.textContent = String(inboundCount);
  }
  if (outboundCounter) {
    outboundCounter.textContent = String(outboundCount);
  }
  if (dailyRecordCount) {
    const daily = state.recordsSummary?.daily || {};
    const dailyTotal = Math.max(0, Number(daily.total || 0) || 0);
    const dailySucceeded = Math.max(0, Number(daily.succeeded || 0) || 0);
    const dailyFailed = Math.max(0, Number(daily.failed || 0) || 0);
    dailyRecordCount.textContent = formatNumber(dailyTotal);
    dailyRecordCount.title = formatNumber(dailySucceeded) + ' erfolgreich, ' + formatNumber(dailyFailed) + ' fehlerhaft';
  }
  if (averageRunDuration) {
    averageRunDuration.classList.remove('text-success', 'text-warning', 'text-danger');
    averageRunDuration.textContent = averageRunDurationMs === null ? '-' : formatDurationMinSec(averageRunDurationMs);
    if (averageRunDurationMs !== null) {
      if (averageRunDurationMs < 60_000) {
        averageRunDuration.classList.add('text-success');
      } else if (averageRunDurationMs < 5 * 60_000) {
        averageRunDuration.classList.add('text-warning');
      } else {
        averageRunDuration.classList.add('text-danger');
      }
    }
  }
  if (autoDisabledCounter) {
    autoDisabledCounter.textContent = String(autoDisabledCount);
  }
  if (lastRunAt) {
    lastRunAt.textContent = latestRun ? formatDate(latestRun.startedAt, 'short') : '-';
  }
  if (sqliteObjectsCounter) {
    sqliteObjectsCounter.textContent = String(sqliteObjects.length);
  }
  if (sqlitePendingCounter) {
    sqlitePendingCounter.textContent = String(sqlitePendingCount);
  }
  if (sqliteSuccessCounter) {
    sqliteSuccessCounter.textContent = String(sqliteSuccessCount);
  }
  if (sqliteErrorsCounter) {
    sqliteErrorsCounter.textContent = String(sqliteErrorCount);
  }

  const dailyRecordSummary = state.recordsSummary?.daily || {};
  const latestBucketTotal = Math.max(0, Number(dailyRecordSummary.succeeded || 0) || 0);
  const previousBucketTotal = Math.max(0, Number(dailyRecordSummary.previousSucceeded || 0) || 0);
  const growthAbsolute = Number.isFinite(Number(dailyRecordSummary.growth))
    ? Number(dailyRecordSummary.growth)
    : latestBucketTotal - previousBucketTotal;
  const growthPercent = Number.isFinite(Number(dailyRecordSummary.growthPercent))
    ? Number(dailyRecordSummary.growthPercent)
    : (previousBucketTotal > 0 ? (growthAbsolute / previousBucketTotal) * 100 : null);

  let healthScore = 100;
  if (String(healthData.service || '').toLowerCase() !== 'ok') {
    healthScore -= 30;
  }
  if (String(healthData.scheduler || '').toLowerCase() !== 'running') {
    healthScore -= 15;
  }
  healthScore -= Math.min(35, Math.round(errorRate * 0.8));
  if (averageRunDurationMs !== null) {
    if (averageRunDurationMs > 5 * 60_000) {
      healthScore -= 20;
    } else if (averageRunDurationMs > 2 * 60_000) {
      healthScore -= 10;
    }
  }
  if (normalizedCpuPercent !== null && normalizedCpuPercent > 85) {
    healthScore -= 10;
  }
  if (sqliteErrorCount > 0) {
    healthScore -= Math.min(15, sqliteErrorCount);
  }
  healthScore = Math.max(0, Math.min(100, Math.round(healthScore)));

  let healthStatusLabel = 'Stabil';
  let healthStatusClass = 'badge bg-primary';
  if (healthScore >= 85) {
    healthStatusLabel = 'Gesund';
    healthStatusClass = 'badge bg-success';
  } else if (healthScore >= 65) {
    healthStatusLabel = 'Stabil';
    healthStatusClass = 'badge bg-primary';
  } else if (healthScore >= 45) {
    healthStatusLabel = 'Beobachten';
    healthStatusClass = 'badge bg-warning text-dark';
  } else {
    healthStatusLabel = 'Kritisch';
    healthStatusClass = 'badge bg-danger';
  }

  const slowRunCount = completedRunDurations.filter((duration) => Number(duration || 0) > 5 * 60_000).length;
  const runtimeText = averageRunDurationMs === null
    ? 'Keine Laufdaten verfügbar'
    : ('Ø ' + formatDurationMinSec(averageRunDurationMs) + ' • Langläufer: ' + slowRunCount);
  const errorText = totalCount > 0
    ? (failedCount + ' von ' + totalCount + ' Runs fehlerhaft (' + errorRate + '%)')
    : 'Keine Runs im gewählten Zeitraum';
  const growthText = latestBucketTotal <= 0 && previousBucketTotal <= 0
    ? 'Heute noch keine erzeugten Datensätze'
    : (formatNumber(latestBucketTotal) + ' heute erzeugt · ' + (growthAbsolute >= 0 ? '+' : '') + formatNumber(growthAbsolute) + ' ggü. gestern' +
        (growthPercent === null ? '' : ' (' + (growthPercent >= 0 ? '+' : '') + growthPercent.toFixed(1) + '%)'));

  if (agentAnalysisScore) {
    agentAnalysisScore.textContent = String(healthScore);
    agentAnalysisScore.classList.remove('text-success', 'text-warning', 'text-danger');
    if (healthScore >= 85) {
      agentAnalysisScore.classList.add('text-success');
    } else if (healthScore >= 45) {
      agentAnalysisScore.classList.add('text-warning');
    } else {
      agentAnalysisScore.classList.add('text-danger');
    }
  }
  if (agentAnalysisStatus) {
    agentAnalysisStatus.className = healthStatusClass;
    agentAnalysisStatus.textContent = healthStatusLabel;
  }
  if (agentAnalysisRuntime) {
    agentAnalysisRuntime.textContent = runtimeText;
  }
  if (agentAnalysisErrors) {
    agentAnalysisErrors.textContent = errorText;
  }
  if (agentAnalysisGrowth) {
    agentAnalysisGrowth.textContent = growthText;
    agentAnalysisGrowth.classList.remove('text-success', 'text-warning', 'text-danger', 'text-secondary');
    if (latestBucketTotal <= 0 && previousBucketTotal <= 0) {
      agentAnalysisGrowth.classList.add('text-secondary');
    } else if (growthAbsolute > 0) {
      agentAnalysisGrowth.classList.add('text-warning');
    } else if (growthAbsolute < 0) {
      agentAnalysisGrowth.classList.add('text-success');
    } else {
      agentAnalysisGrowth.classList.add('text-secondary');
    }
  }
  if (agentAnalysisUpdated) {
    agentAnalysisUpdated.textContent = formatDate(new Date().toISOString(), 'short');
  }
  if (agentAnalysisSummary && !String(agentAnalysisSummary.textContent || '').trim()) {
    agentAnalysisSummary.textContent = 'Basisanalyse läuft...';
  }
  if (agentAnalysisRecommendations && !agentAnalysisRecommendations.children.length) {
    agentAnalysisRecommendations.innerHTML = '<li>Warte auf KI-Empfehlungen...</li>';
  }

  ensureAIDashboardAnalysis({
    serviceStatus: healthData.service,
    schedulerStatus: healthData.scheduler,
    runsTotal: totalCount,
    runsFailed: failedCount,
    errorRate,
    averageRunDurationMs,
    cpuLoadPercent: normalizedCpuPercent,
    dataGrowthAbsolute: growthAbsolute,
    dataGrowthPercent: growthPercent,
    sqliteErrors: sqliteErrorCount
  });

  const serviceTrend = document.getElementById('kpi-service-trend');
  if (serviceTrend) {
    const isOk = String(healthData.service || '').toLowerCase() === 'ok';
    if (isOk && normalizedCpuPercent !== null && normalizedCpuPercent < 55) {
      serviceTrend.className = 'limit-compact-detail kpi-trend kpi-trend-positive';
      serviceTrend.textContent = '↑ laeuft rund';
    } else if (isOk && normalizedCpuPercent !== null && normalizedCpuPercent < 80) {
      serviceTrend.className = 'limit-compact-detail kpi-trend kpi-trend-neutral';
      serviceTrend.textContent = '→ laeuft, aber leicht unter Last';
    } else if (isOk) {
      serviceTrend.className = 'limit-compact-detail kpi-trend kpi-trend-negative';
      serviceTrend.textContent = '↓ hoher CPU-Druck';
    } else {
      serviceTrend.className = 'limit-compact-detail kpi-trend kpi-trend-negative';
      serviceTrend.textContent = '↓ Service ist degraded';
    }
  }

  const schedulerTrend = document.getElementById('kpi-scheduler-trend');
  if (schedulerTrend) {
    const schedulerState = String(healthData.scheduler || '').toLowerCase();
    if (schedulerState === 'running') {
      schedulerTrend.className = 'kpi-trend kpi-trend-positive';
      schedulerTrend.textContent = '↑ aktiv';
    } else if (schedulerState === 'error') {
      const lastRunError = String(healthData.lastRunError || '').trim();
      schedulerTrend.className = 'kpi-trend kpi-trend-negative';
      schedulerTrend.textContent = lastRunError
        ? '↓ ' + (lastRunError.length > 44 ? lastRunError.slice(0, 41) + '...' : lastRunError)
        : '↓ Fehlerzustand';
      schedulerTrend.title = lastRunError || 'Fehlerzustand';
    } else {
      schedulerTrend.className = 'kpi-trend kpi-trend-neutral';
      schedulerTrend.textContent = '→ idle';
      schedulerTrend.title = '';
    }
  }

  updateKpiTrend(
    'kpi-schedules-trend',
    previousSnapshot ? (state.schedules.length - previousSnapshot.schedulesCount) : null,
    true,
    'warten auf Vergleich'
  );
  updateKpiTrend(
    'kpi-connectors-trend',
    previousSnapshot ? (state.connectors.length - previousSnapshot.connectorsCount) : null,
    true,
    'warten auf Vergleich'
  );

  state.previousOverviewSnapshot = {
    schedulesCount: state.schedules.length,
    connectorsCount: state.connectors.length
  };

  const body = document.getElementById('overview-runs-body');
  if (!body) {
    return;
  }
  if (!scopedRuns.length) {
    body.innerHTML = '<tr><td colspan="4" class="text-secondary">' + esc(state.runtimeContextUnavailableMessage || 'Keine Runs im gewählten Zeitraum gefunden.') + '</td></tr>';
    return;
  }

  body.innerHTML = scopedRuns.slice(0, 8).map((run) =>
    '<tr><td class="text-truncate" title="' + esc(run.scheduleName || run.scheduleId || '-') + '">' + esc(run.scheduleName || run.scheduleId || '-') + '</td><td>' + getStatusBadge(run.status) + '</td><td>' + formatDurationMinSec(getRunDurationMs(run)) + '</td><td>' + formatDate(run.startedAt, 'short') + '</td></tr>'
  ).join('');
}
