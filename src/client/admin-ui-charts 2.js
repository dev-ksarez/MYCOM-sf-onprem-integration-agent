// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-charts
// Source lines: 7511–8133
// ──────────────────────────────────────────────────────────────────────

function renderLogChart(summary) {
  const canvas = document.getElementById('logs-chart');
  if (!canvas || typeof window.Chart !== 'function') {
    return;
  }

  if (logsChart) {
    logsChart.destroy();
  }

  const labels = (summary?.buckets || []).map((item) => item.label);
  const connectorTotals = (Array.isArray(summary?.connectors) ? summary.connectors : [])
    .map((connectorName) => ({
      connectorName,
      total: (summary?.buckets || []).reduce((sum, item) => sum + Number(item?.connectorErrors?.[connectorName] || 0), 0)
    }))
    .filter((item) => item.total > 0)
    .sort((left, right) => right.total - left.total);
  const primaryConnectors = connectorTotals.slice(0, MAX_LOG_CONNECTOR_SERIES).map((item) => item.connectorName);
  const remainingConnectors = connectorTotals.slice(MAX_LOG_CONNECTOR_SERIES).map((item) => item.connectorName);
  const palette = [
    'rgba(208, 73, 73, 1)',
    'rgba(43, 122, 184, 1)',
    'rgba(31, 125, 87, 1)',
    'rgba(194, 106, 45, 1)',
    'rgba(123, 94, 167, 1)',
    'rgba(39, 145, 132, 1)',
    'rgba(153, 72, 122, 1)',
    'rgba(93, 110, 126, 1)'
  ];
  const datasets = primaryConnectors.map((connectorName, index) => ({
    label: connectorName,
    connectorName,
    data: (summary?.buckets || []).map((item) => Number(item?.connectorErrors?.[connectorName] || 0)),
    backgroundColor: palette[index % palette.length].replace(', 1)', ', 0.14)'),
    borderColor: palette[index % palette.length],
    borderWidth: 2,
    tension: 0.35,
    fill: false,
    pointRadius: 2,
    pointHoverRadius: 4
  })).filter((dataset) => dataset.data.some((value) => value > 0));

  if (remainingConnectors.length) {
    datasets.push({
      label: 'Sonstige',
      connectorName: '',
      data: (summary?.buckets || []).map((item) => remainingConnectors.reduce((sum, connectorName) => sum + Number(item?.connectorErrors?.[connectorName] || 0), 0)),
      backgroundColor: 'rgba(93, 110, 126, 0.12)',
      borderColor: 'rgba(93, 110, 126, 0.92)',
      borderWidth: 2,
      tension: 0.35,
      fill: false,
      pointRadius: 2,
      pointHoverRadius: 4
    });
  }

  if (!datasets.length) {
    datasets.push({
      label: 'Keine Fehler',
      connectorName: '',
      data: labels.map(() => 0),
      backgroundColor: 'rgba(93, 110, 126, 0.12)',
      borderColor: 'rgba(93, 110, 126, 0.85)',
      borderWidth: 2,
      tension: 0.35,
      fill: false,
      pointRadius: 2,
      pointHoverRadius: 4
    });
  }

  logsChart = new window.Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'top'
        }
      },
      scales: {
        x: {
          stacked: false
        },
        y: {
          beginAtZero: true,
          stacked: false,
          ticks: {
            precision: 0
          }
        }
      },
      onClick: async (event, elements) => {
        if (!elements || !elements.length) {
          return;
        }

        const point = elements[0];
        const bucket = summary?.buckets?.[point.index];
        if (!bucket) {
          return;
        }

        const connectorName = logsChart?.data?.datasets?.[point.datasetIndex]?.connectorName || '';
        await openLogsByBucket(bucket, 'error', connectorName);
      }
    }
  });
}

function getRecordsChartRange() {
  const range = String(state.overviewStatsRange || 'month').trim();
  if (range === 'day' || range === 'month' || range === 'year') {
    return range;
  }
  return 'month';
}

function renderRecordsTrendChart(summary) {
  const canvas = document.getElementById('records-chart');
  if (!canvas || typeof window.Chart !== 'function') {
    return;
  }

  if (recordsChart) {
    recordsChart.destroy();
  }

  const labels = (summary?.buckets || []).map((item) => item.label);
  const connectorTotals = (Array.isArray(summary?.connectors) ? summary.connectors : [])
    .map((connectorName) => ({
      connectorName,
      total: (summary?.buckets || []).reduce((sum, item) => sum + Number(item?.connectorTotals?.[connectorName] || 0), 0)
    }))
    .filter((item) => item.total > 0)
    .sort((left, right) => right.total - left.total);
  const primaryConnectors = connectorTotals.slice(0, MAX_RECORD_CONNECTOR_SERIES).map((item) => item.connectorName);
  const remainingConnectors = connectorTotals.slice(MAX_RECORD_CONNECTOR_SERIES).map((item) => item.connectorName);
  const palette = [
    'rgba(43, 122, 184, 1)',
    'rgba(31, 125, 87, 1)',
    'rgba(194, 106, 45, 1)',
    'rgba(123, 94, 167, 1)',
    'rgba(208, 73, 73, 1)',
    'rgba(39, 145, 132, 1)',
    'rgba(153, 72, 122, 1)'
  ];
  const datasets = primaryConnectors.map((connectorName, index) => ({
    label: connectorName,
    connectorName,
    data: (summary?.buckets || []).map((item) => Number(item?.connectorTotals?.[connectorName] || 0)),
    borderColor: palette[index % palette.length],
    backgroundColor: palette[index % palette.length].replace(', 1)', ', 0.12)'),
    borderWidth: 2,
    tension: 0.35,
    fill: false,
    pointRadius: 2,
    pointHoverRadius: 4
  })).filter((dataset) => dataset.data.some((value) => value > 0));

  // Add failed records as dashed line
  const failedData = (summary?.buckets || []).map((item) => Number(item?.failed || 0));
  if (failedData.some((value) => value > 0)) {
    datasets.push({
      label: 'Fehlgeschlagene Datensätze',
      connectorName: '__failed__',
      data: failedData,
      borderColor: 'rgba(208, 73, 73, 1)',
      backgroundColor: 'rgba(208, 73, 73, 0.12)',
      borderWidth: 2,
      borderDash: [5, 5],
      tension: 0.35,
      fill: false,
      pointRadius: 2,
      pointHoverRadius: 4
    });
  }

  if (remainingConnectors.length) {
    datasets.push({
      label: 'Sonstige',
      connectorName: '__other__',
      data: (summary?.buckets || []).map((item) => remainingConnectors.reduce((sum, connectorName) => sum + Number(item?.connectorTotals?.[connectorName] || 0), 0)),
      borderColor: 'rgba(93, 110, 126, 0.92)',
      backgroundColor: 'rgba(93, 110, 126, 0.12)',
      borderWidth: 2,
      tension: 0.35,
      fill: false,
      pointRadius: 2,
      pointHoverRadius: 4
    });
  }

  if (!datasets.length) {
    datasets.push({
      label: 'Keine Datensätze',
      connectorName: '',
      data: labels.map(() => 0),
      borderColor: 'rgba(93, 110, 126, 0.85)',
      backgroundColor: 'rgba(93, 110, 126, 0.12)',
      borderWidth: 2,
      tension: 0.35,
      fill: false,
      pointRadius: 2,
      pointHoverRadius: 4
    });
  }

  recordsChart = new window.Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'top'
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            precision: 0
          }
        }
      },
      onClick: async (event, elements) => {
        if (!elements || !elements.length) {
          return;
        }

        const point = elements[0];
        const bucket = summary?.buckets?.[point.index];
        if (!bucket) {
          return;
        }

        const connectorName = recordsChart?.data?.datasets?.[point.datasetIndex]?.connectorName || '';
        await openRecordSchedulersByBucket(bucket, connectorName);
      }
    }
  });
}

async function loadRecordsSummary() {
  const range = getRecordsChartRange();
  const fallback = { range, buckets: [], connectors: [] };
  if (state.runtimeContextUnavailableMessage) {
    state.recordsSummary = fallback;
    renderRecordsTrendChart(fallback);
    renderSalesforceOverview(state.salesforceOverview || {});
    redrawOverviewGraph();
    if (state.health) {
      renderOverview(state.health);
    }
    return;
  }
  const summary = await safeRequest('/api/dashboard/records-summary?range=' + encodeURIComponent(range), fallback);
  state.recordsSummary = summary;
  renderRecordsTrendChart(summary);
  renderSalesforceOverview(state.salesforceOverview || {});
  redrawOverviewGraph();
  if (state.health) {
    renderOverview(state.health);
  }
}

function renderOverviewUpdateStatus() {
  const statusEl = document.getElementById('overview-update-status');
  const progressWrap = document.getElementById('overview-update-progress-wrap');
  const progressStageEl = document.getElementById('overview-update-progress-stage');
  const progressPercentEl = document.getElementById('overview-update-progress-percent');
  const progressBarEl = document.getElementById('overview-update-progress-bar');
  const progressUpdatedAtEl = document.getElementById('overview-update-progress-updated-at');
  const checkButton = document.getElementById('overview-check-update');
  const runButton = document.getElementById('overview-run-update');
  const versionLabelEl = document.getElementById('agent-version-label');
  const menuUpdateBulletEl = document.getElementById('agent-menu-update-bullet');
  const installUpdateBulletEl = document.getElementById('agent-install-update-bullet');
  if (!statusEl || !progressWrap || !progressStageEl || !progressPercentEl || !progressBarEl || !progressUpdatedAtEl || !checkButton || !runButton) {
    return;
  }

  const status = state.updateStatus;
  if (!status) {
    statusEl.textContent = 'Update-Status unbekannt';
    progressWrap.classList.add('d-none');
    checkButton.disabled = false;
    runButton.disabled = true;
    return;
  }

  const getUpdateStageLabel = (stage) => {
    const normalized = String(stage || '').trim().toLowerCase();
    if (normalized === 'init' || normalized === 'start') return 'Vorbereitung';
    if (normalized === 'manifest') return 'Manifest laden';
    if (normalized === 'download') return 'Paket herunterladen';
    if (normalized === 'verify') return 'Paket prüfen';
    if (normalized === 'extract') return 'Paket entpacken';
    if (normalized === 'stop-service') return 'Dienst stoppen';
    if (normalized === 'apply') return 'Dateien einspielen';
    if (normalized === 'start-service') return 'Dienst starten';
    if (normalized === 'rollback') return 'Rollback';
    if (normalized === 'completed') return 'Abgeschlossen';
    if (normalized === 'failed') return 'Fehlgeschlagen';
    if (normalized === 'idle') return 'Bereit';
    return normalized || 'Unbekannt';
  };

  const isInProgress = !!status.inProgress;
  const progressPercent = Number(status.progressPercent);
  const normalizedProgressPercent = Number.isFinite(progressPercent)
    ? Math.max(0, Math.min(100, Math.round(progressPercent)))
    : 0;
  const hasFailureMessage = /fehlgeschlagen/i.test(String(status.message || ''));
  const currentVersion = String(status.currentVersion || '').trim() || '-';
  const hasUpdateAvailable = !!status.updateAvailable;
  statusEl.textContent = status.message || 'Update-Status unbekannt';
  if (versionLabelEl) {
    versionLabelEl.textContent = 'v' + currentVersion;
  }
  if (menuUpdateBulletEl) {
    menuUpdateBulletEl.classList.toggle('d-none', !hasUpdateAvailable);
  }
  if (installUpdateBulletEl) {
    installUpdateBulletEl.classList.toggle('d-none', !hasUpdateAvailable);
  }
  progressWrap.classList.toggle('d-none', !isInProgress && !status.stage && !Number.isFinite(progressPercent));
  progressStageEl.textContent = status.stage
    ? 'Schritt: ' + getUpdateStageLabel(status.stage)
    : (isInProgress ? 'Update wird ausgefuehrt.' : 'Kein laufendes Update');
  progressPercentEl.textContent = normalizedProgressPercent + '%';
  progressBarEl.style.width = normalizedProgressPercent + '%';
  progressBarEl.setAttribute('aria-valuenow', String(normalizedProgressPercent));
  progressBarEl.classList.toggle('bg-danger', hasFailureMessage);
  progressBarEl.classList.toggle('bg-success', !isInProgress && !hasFailureMessage && normalizedProgressPercent >= 100);
  progressUpdatedAtEl.textContent = status.updatedAt
    ? 'Stand: ' + formatDate(status.updatedAt, 'short')
    : '';

  checkButton.disabled = isInProgress;
  runButton.disabled = isInProgress || !status.updateAvailable;
  runButton.title = status.supported === false
    ? 'Der Direktstart richtet sich nach dem Agent-Host, nicht nach dem Browser-Client.'
    : '';

  if (isInProgress) {
    startOverviewUpdatePolling();
  } else {
    stopOverviewUpdatePolling();
  }
}

function stopOverviewUpdatePolling() {
  if (!state.updateStatusPollTimer) {
    return;
  }

  window.clearTimeout(state.updateStatusPollTimer);
  state.updateStatusPollTimer = null;
}

function startOverviewUpdatePolling() {
  if (state.updateStatusPollTimer) {
    return;
  }

  state.updateStatusPollTimer = window.setTimeout(async () => {
    state.updateStatusPollTimer = null;
    await loadOverviewUpdateStatus(true, false, true);
  }, 3000);
}

function renderOverviewLogRetentionStatus() {
  const retentionEl = document.getElementById('overview-log-retention-status');
  if (!retentionEl) {
    return;
  }

  const retentionDays = Number(state.health?.logRetentionDays || 0);
  retentionEl.textContent = retentionDays > 0
    ? 'Log-Retention: ' + retentionDays + ' Tage'
    : 'Log-Retention: deaktiviert';
}

async function loadOverviewUpdateStatus(force, notifyUser, silent) {
  if (!force && state.updateStatus && (Date.now() - Number(state.updateStatusCheckedAt || 0) < 60000)) {
    renderOverviewUpdateStatus();
    if (notifyUser) {
      window.alert(state.updateStatus?.message || 'Update-Status unbekannt.');
    }
    return;
  }

  let status = null;
  try {
    status = await requestJson('/api/system/update-status');
  } catch (error) {
    if (!silent) {
      showError(error.message || 'Update-Status konnte nicht geladen werden.');
    }

    if (state.updateStatus?.inProgress) {
      status = {
        ...state.updateStatus,
        message: 'Update wird ausgefuehrt. Verbindung zum Agenten wird neu aufgebaut.'
      };
    } else {
      status = {
        currentVersion: '-',
        updateAvailable: false,
        supported: false,
        manifestUrl: '',
        message: 'Update-Status konnte nicht geladen werden.'
      };
    }
  }

  state.updateStatus = status;
  state.updateStatusCheckedAt = Date.now();
  renderOverviewUpdateStatus();

  if (notifyUser) {
    window.alert(status?.message || (status?.updateAvailable ? 'Ein Update ist verfügbar.' : 'Kein Update verfügbar.'));
  }
}

async function triggerOverviewUpdate() {
  if (state.updateStatus?.supported === false) {
    window.alert(state.updateStatus?.message || 'Der Direktstart richtet sich nach dem Agent-Host, nicht nach dem Browser-Client.');
    return;
  }

  const result = await requestJson('/api/system/update-now', {
    method: 'POST'
  });
  window.alert(result.message || (result.ok ? 'Update gestartet.' : 'Update fehlgeschlagen.'));
  if (result.output) {
    console.log('Update output:', result.output);
  }
  await loadOverviewUpdateStatus(true, false, true);
}

async function setScheduleActive(scheduleId, active) {
  await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/active', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ active: !!active })
  });
  await refresh({ refreshChart: false });
}

async function openRecordSchedulersByBucket(bucket, connectorName) {
  if (!bucket) {
    return;
  }

  const connectorSchedules = bucket?.connectorSchedules || {};
  const primaryConnectorNames = (state.recordsSummary?.connectors || []).slice(0, MAX_RECORD_CONNECTOR_SERIES);
  let effectiveConnectorName = String(connectorName || '').trim();
  let scheduleEntries = [];

  if (effectiveConnectorName === '__other__') {
    scheduleEntries = Object.entries(connectorSchedules)
      .filter(([name]) => !primaryConnectorNames.includes(name))
      .flatMap(([, entries]) => Array.isArray(entries) ? entries : []);
    effectiveConnectorName = 'Sonstige';
  } else {
    scheduleEntries = Array.isArray(connectorSchedules[effectiveConnectorName]) ? connectorSchedules[effectiveConnectorName] : [];
  }

  const normalizedEntries = scheduleEntries
    .filter((entry) => entry && (entry.scheduleId || entry.scheduleName))
    .sort((left, right) => Number(right.total || 0) - Number(left.total || 0));

  if (!normalizedEntries.length) {
    return;
  }

  if (normalizedEntries.length === 1 && normalizedEntries[0].scheduleId) {
    await openScheduleModal(normalizedEntries[0].scheduleId);
    return;
  }

  const title = document.getElementById('records-scheduler-modal-title');
  const summaryEl = document.getElementById('records-scheduler-modal-summary');
  const listEl = document.getElementById('records-scheduler-modal-list');
  if (!title || !summaryEl || !listEl) {
    return;
  }

  title.textContent = 'Scheduler für ' + (effectiveConnectorName || 'Datensätze');
  summaryEl.textContent = 'Zeitslot: ' + String(bucket.label || '-') + ' · Scheduler: ' + normalizedEntries.length;
  listEl.innerHTML = normalizedEntries.map((entry) => {
    const label = String(entry.scheduleName || entry.scheduleId || 'Unbekannter Scheduler');
    const count = Number(entry.total || 0);
    const failed = Number(entry.failed || 0);
    return '<button type="button" class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" data-open-record-scheduler="' + esc(entry.scheduleId || '') + '">' +
      '<span><strong>' + esc(label) + '</strong><span class="d-block small text-secondary">Connector: ' + esc(entry.connectorName || effectiveConnectorName || '-') + '</span></span>' +
      '<span class="text-end small"><span class="d-block">Datensätze: ' + count + '</span><span class="d-block text-danger">Fehler: ' + failed + '</span></span>' +
    '</button>';
  }).join('');

  listEl.querySelectorAll('[data-open-record-scheduler]').forEach((button) => {
    button.addEventListener('click', async () => {
      const scheduleId = String(button.getAttribute('data-open-record-scheduler') || '').trim();
      if (!scheduleId) {
        return;
      }
      recordsSchedulerModal.hide();
      await openScheduleModal(scheduleId);
    });
  });

  recordsSchedulerModal.show();
}

function isEmptyLogSummary(summary) {
  const buckets = Array.isArray(summary?.buckets) ? summary.buckets : [];
  const connectors = Array.isArray(summary?.connectors) ? summary.connectors : [];
  if (connectors.length > 0) {
    return false;
  }
  return !buckets.some((bucket) => Number(bucket?.errors || 0) > 0);
}

async function loadLogSummary() {
  const range = document.getElementById('log-chart-range').value || 'last_24h';
  try {
    window.localStorage.setItem(LOG_CHART_RANGE_STORAGE_KEY, range);
  } catch {
    // Ignore storage errors in restricted browser contexts.
  }
  if (state.runtimeContextUnavailableMessage) {
    const fallback = { range, buckets: [], connectors: [] };
    state.logSummary = fallback;
    renderLogChart(fallback);
    return;
  }
  let summary = await safeRequest('/api/logs/summary?range=' + encodeURIComponent(range), { range, buckets: [], connectors: [] });
  if (isEmptyLogSummary(summary)) {
    const retryDelaysMs = [750, 1500, 2500];
    for (const delayMs of retryDelaysMs) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      const retried = await safeRequest('/api/logs/summary?range=' + encodeURIComponent(range), summary);
      if (!isEmptyLogSummary(retried)) {
        summary = retried;
        break;
      }
    }
  }
  state.logSummary = summary;
  renderLogChart(summary);
}

function restoreLogChartRange() {
  const select = document.getElementById('log-chart-range');
  if (!select) {
    return;
  }

  try {
    const stored = window.localStorage.getItem(LOG_CHART_RANGE_STORAGE_KEY);
    if (stored && Array.from(select.options).some((option) => option.value === stored)) {
      select.value = stored;
    }
  } catch {
    // Ignore storage errors in restricted browser contexts.
  }
}

function restoreOverviewStatsRange() {
  try {
    const stored = window.localStorage.getItem(OVERVIEW_STATS_RANGE_STORAGE_KEY);
    if (stored && ['day', 'month', 'year'].includes(stored)) {
      state.overviewStatsRange = stored;
    }
  } catch {
    // Ignore storage errors in restricted browser contexts.
  }
}

function renderOverviewStatsRangeButtons() {
  const group = document.getElementById('overview-stats-range');
  if (!group) {
    return;
  }

  group.querySelectorAll('[data-range]').forEach((button) => {
    const range = String(button.getAttribute('data-range') || '').trim();
    button.classList.toggle('active', range === state.overviewStatsRange);
  });
}

function getOverviewRangeStartDate(now, range) {
  const start = new Date(now);
  if (range === 'day') {
    start.setHours(0, 0, 0, 0);
    return start;
  }

  if (range === 'year') {
    start.setMonth(0, 1);
    start.setHours(0, 0, 0, 0);
    return start;
  }

  start.setDate(1);
  start.setHours(0, 0, 0, 0);
  return start;
}

