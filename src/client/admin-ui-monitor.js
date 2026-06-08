// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-monitor
// Source lines: 9267–10613
// ──────────────────────────────────────────────────────────────────────

function renderSchedulerConnectorFilterOptions() {
  const select = document.getElementById('schedulers-connector-filter');
  if (!select) {
    return;
  }

  const desiredConnectorFilterId = String(select.value || state.schedulerConnectorFilterId || '').trim();
  if (desiredConnectorFilterId) {
    state.schedulerConnectorFilterId = desiredConnectorFilterId;
  }

  const connectorIds = new Set(
    (state.schedules || [])
      .map((schedule) => String(schedule.connectorId || '').trim())
      .filter(Boolean)
  );

  const options = (state.connectors || [])
    .filter((connector) => connectorIds.has(String(connector.id || '').trim()))
    .map((connector) => {
      const connectorId = String(connector.id || '').trim();
      return {
        connectorId,
        name: String(connector.name || connectorId || '-'),
        count: (state.schedules || []).filter((schedule) => String(schedule.connectorId || '').trim() === connectorId).length
      };
    })
    .sort((a, b) => a.name.localeCompare(b.name, 'de', { sensitivity: 'base' }));

  if (!options.length && connectorIds.size) {
    Array.from(connectorIds)
      .map((connectorId) => ({
        connectorId,
        name: getConnectorNameById(connectorId),
        count: (state.schedules || []).filter((schedule) => String(schedule.connectorId || '').trim() === connectorId).length
      }))
      .sort((a, b) => a.name.localeCompare(b.name, 'de', { sensitivity: 'base' }))
      .forEach((option) => options.push(option));
  }

  select.innerHTML = '<option value="">Alle Connectoren</option>' + options.map((option) =>
    '<option value="' + esc(option.connectorId) + '">' + esc(option.name) + ' (' + option.count + ')</option>'
  ).join('');

  if (desiredConnectorFilterId && options.some((option) => option.connectorId === desiredConnectorFilterId)) {
    state.schedulerConnectorFilterId = desiredConnectorFilterId;
    select.value = desiredConnectorFilterId;
  } else {
    state.schedulerConnectorFilterId = '';
    select.value = '';
  }
}

function getRunSortTime(run) {
  return new Date(run?.startedAt || run?.finishedAt || 0).getTime();
}

function getLatestRunForSchedule(scheduleId) {
  return (state.runs || [])
    .filter((run) => String(run.scheduleId || '').trim() === String(scheduleId || '').trim())
    .sort((left, right) => getRunSortTime(right) - getRunSortTime(left))[0] || null;
}

function getLatestFailedRunForSchedule(scheduleId) {
  const latestRun = getLatestRunForSchedule(scheduleId);
  if (latestRun && normalizeRunStatus(latestRun.status) !== 'failed') {
    return null;
  }
  return (state.runs || [])
    .filter((run) => String(run.scheduleId || '').trim() === String(scheduleId || '').trim() && normalizeRunStatus(run.status) === 'failed')
    .sort((left, right) => getRunSortTime(right) - getRunSortTime(left))[0] || null;
}

function buildRunProgressMetrics(run) {
  if (!run) {
    return null;
  }

  const recordsRead = Math.max(0, Number(run.recordsRead ?? 0) || 0);
  const recordsProcessed = Math.max(0, Number(run.recordsProcessed ?? 0) || 0);
  const recordsSucceeded = Math.max(0, Number(run.recordsSucceeded ?? 0) || 0);
  const recordsFailed = Math.max(0, Number(run.recordsFailed ?? 0) || 0);
  const plannedTotalRecords = Math.max(0, Number(run.totalRecords ?? run.recordCount ?? 0) || 0);
  const normalizedStatus = normalizeRunStatus(run.status);
  const total = normalizedStatus === 'running' && recordsRead <= 0
    ? plannedTotalRecords
    : Math.max(plannedTotalRecords, recordsRead, recordsProcessed, recordsSucceeded + recordsFailed);
  const completed = Math.max(recordsProcessed, recordsSucceeded + recordsFailed);
  const pending = Math.max(0, total - recordsSucceeded - recordsFailed);
  const completedPercent = total > 0 ? Math.max(0, Math.min(100, (completed / total) * 100)) : 0;
  const successPercent = total > 0 ? Math.max(0, Math.min(100, (recordsSucceeded / total) * 100)) : 0;
  const failedPercent = total > 0 ? Math.max(0, Math.min(100, (recordsFailed / total) * 100)) : 0;
  const pendingPercent = total > 0 ? Math.max(0, Math.min(100, 100 - successPercent - failedPercent)) : 0;

  return {
    recordsRead,
    recordsProcessed,
    recordsSucceeded,
    recordsFailed,
    total,
    completed,
    pending,
    completedPercent,
    successPercent,
    failedPercent,
    pendingPercent,
    normalizedStatus
  };
}

function renderRunProgressMarkup(run, options) {
  const viewOptions = options || {};
  const compact = Boolean(viewOptions.compact);
  const metrics = buildRunProgressMetrics(run);

  if (!metrics) {
    return '<div class="run-mini-gauge-empty">Noch kein Lauf</div>';
  }

  const wrapperClassName = compact ? 'run-mini-gauge-wrap is-compact' : 'run-mini-gauge-wrap';
  const metaPrefix = compact ? 'Letzter Lauf: ' : '';

  if (metrics.total > 0) {
    const title = metrics.recordsSucceeded + ' erfolgreich, ' + metrics.recordsFailed + ' fehlerhaft, ' + metrics.pending + ' offen von ' + metrics.total;
    const summary = metrics.normalizedStatus === 'running'
      ? Math.round(metrics.completedPercent) + '% • ' + metrics.completed + ' / ' + metrics.total + ' verarbeitet'
      : metrics.recordsSucceeded + ' ok / ' + metrics.recordsFailed + ' fail / ' + metrics.total + ' gesamt';

    return '<div class="' + wrapperClassName + '">' +
      '<div class="run-mini-gauge" title="' + esc(title) + '">' +
        '<span class="run-mini-gauge-segment is-success" style="width:' + metrics.successPercent.toFixed(2) + '%"></span>' +
        '<span class="run-mini-gauge-segment is-failed" style="width:' + metrics.failedPercent.toFixed(2) + '%"></span>' +
        '<span class="run-mini-gauge-segment is-pending" style="width:' + metrics.pendingPercent.toFixed(2) + '%"></span>' +
      '</div>' +
      '<div class="run-mini-gauge-meta">' + esc(metaPrefix + summary) + '</div>' +
    '</div>';
  }

  if (metrics.normalizedStatus === 'running') {
    const runningSummary = metrics.recordsProcessed > 0
      ? 'läuft, ' + metrics.recordsProcessed + ' verarbeitet, Gesamtzahl noch unbekannt'
      : 'läuft, Gesamtzahl noch unbekannt';
    return '<div class="' + wrapperClassName + '">' +
      '<div class="run-mini-gauge run-mini-gauge-activity" title="Gesamtmenge aktuell noch unbekannt">' +
        '<span class="run-mini-gauge-activity-indicator"></span>' +
      '</div>' +
      '<div class="run-mini-gauge-meta">' + esc(metaPrefix + runningSummary) + '</div>' +
    '</div>';
  }

  return '<div class="' + wrapperClassName + '"><div class="run-mini-gauge-empty">' + esc(metaPrefix + metrics.recordsSucceeded + ' ok / ' + metrics.recordsFailed + ' fail') + '</div></div>';
}

function renderSchedules() {
  const body = document.getElementById('schedules-body');
  const autoDisabledWarning = document.getElementById('schedulers-auto-disabled-warning');
  renderSchedulerConnectorFilterOptions();

  const autoDisabledCount = (state.schedules || []).filter((item) => item.autoDisabledDueToErrors && item.active === false).length;
  if (autoDisabledWarning) {
    if (autoDisabledCount > 0) {
      autoDisabledWarning.textContent = autoDisabledCount + ' Scheduler wurden wegen fortlaufender Fehler automatisch deaktiviert.';
      autoDisabledWarning.classList.remove('d-none');
    } else {
      autoDisabledWarning.textContent = '';
      autoDisabledWarning.classList.add('d-none');
    }
  }

  const filteredSchedules = (state.schedules || []).filter((item) => {
    const direction = String(item.direction || '').toLowerCase();
    if (state.schedulerDirectionTab === 'inbound' && direction !== 'inbound') {
      return false;
    }
    if (state.schedulerDirectionTab === 'outbound' && direction !== 'outbound') {
      return false;
    }
    if (state.schedulerActiveFilter === 'active' && !item.active) {
      return false;
    }
    if (state.schedulerActiveFilter === 'inactive' && item.active) {
      return false;
    }
    if (!state.schedulerConnectorFilterId) {
      return true;
    }
    return String(item.connectorId || '').trim() === state.schedulerConnectorFilterId;
  });

  if (!filteredSchedules.length) {
    body.innerHTML = '<tr><td colspan="6" class="text-secondary">' + esc(state.runtimeContextUnavailableMessage || 'Keine Scheduler gefunden.') + '</td></tr>';
    return;
  }

  const scheduleById = new Map(filteredSchedules.map((item) => [item.id, item]));
  const childrenByParent = new Map();
  const roots = [];

  filteredSchedules.forEach((item) => {
    const parentId = String(item.parentScheduleId || '').trim();
    if (parentId && parentId !== item.id && scheduleById.has(parentId)) {
      if (!childrenByParent.has(parentId)) {
        childrenByParent.set(parentId, []);
      }
      childrenByParent.get(parentId).push(item);
      return;
    }
    roots.push(item);
  });

  const ordered = [];
  const depthById = new Map();
  const visited = new Set();

  function visit(node, depth, trail) {
    if (!node || visited.has(node.id) || trail.has(node.id)) {
      return;
    }
    trail.add(node.id);
    visited.add(node.id);
    depthById.set(node.id, depth);
    ordered.push(node);

    const children = (childrenByParent.get(node.id) || []).slice().sort((a, b) =>
      String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' })
    );
    children.forEach((child) => visit(child, depth + 1, trail));
    trail.delete(node.id);
  }

  roots
    .slice()
    .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' }))
    .forEach((root) => visit(root, 0, new Set()));

  filteredSchedules.forEach((item) => {
    if (!visited.has(item.id)) {
      visit(item, 0, new Set());
    }
  });

  body.innerHTML = ordered.map((item) => {
      const depth = Number(depthById.get(item.id) || 0);
      const indent = depth * 18;
      const parentName = item.parentScheduleId && scheduleById.get(item.parentScheduleId)
        ? String(scheduleById.get(item.parentScheduleId).name || item.parentScheduleId)
        : '-';
      const objectIcon = getObjectIcon(item.objectName);
      const connectorName = getConnectorNameById(item.connectorId);
      const intervalLabel = getScheduleIntervalLabel(item, scheduleById, false, new Set());
      const hierarchyBadge = depth > 0
        ? '<span class="badge bg-light text-dark border ms-1">Level ' + (depth + 1) + '</span>'
        : '<span class="badge bg-secondary-subtle text-secondary border ms-1">Root</span>';
      const activeHint = item.autoDisabledDueToErrors
        ? '<span class="badge bg-warning-subtle text-warning border mt-1" title="Automatisch wegen Fehlern deaktiviert">auto deaktiviert</span>'
        : '<span class="small text-secondary">' + (item.active ? 'aktiv' : 'inaktiv') + '</span>';
      const latestRun = getLatestRunForSchedule(item.id);
      const lastFailedRun = getLatestFailedRunForSchedule(item.id);
      const progressMarkup = renderRunProgressMarkup(latestRun, { compact: true });
      const errorMarkup = lastFailedRun
        ? '<button class="btn btn-sm btn-outline-danger mt-2" title="Letzter Fehler: ' + esc(lastFailedRun.errorMessage || 'Unbekannter Fehler') + '" data-show-run-logs="' + esc(lastFailedRun.id) + '">Fehlerdetails</button>'
        : '<span class="small text-secondary d-block mt-2">keine offenen Fehler</span>';
      
      return '<tr data-schedule-active="' + (item.active ? 'active' : 'inactive') + '">' +
        '<td><div style="padding-left:' + indent + 'px"><strong class="text-truncate d-block" title="' + esc(item.name) + '">' + esc(item.name) + hierarchyBadge + '</strong><div class="small text-secondary text-truncate" title="' + esc(item.objectName) + ' / ' + esc(item.operation) + '">' + objectIcon + ' ' + esc(item.objectName) + ' / ' + esc(item.operation) + '</div><div class="small text-secondary text-truncate mt-1" title="' + esc(parentName) + '">Parent: ' + esc(parentName) + (item.inheritTimingFromParent ? ' <span class="badge bg-primary-subtle text-primary border">inherits</span>' : '') + '</div></div></td>' +
        '<td><div class="form-check form-switch mb-1"><input class="form-check-input" type="checkbox" role="switch" data-toggle-schedule-active="' + esc(item.id) + '"' + (item.active ? ' checked' : '') + '></div>' + activeHint + '</td>' +
        '<td>' + getStatusBadge(item.status) + progressMarkup + errorMarkup + '</td>' +
        '<td><div class="fw-semibold text-truncate" title="' + esc(connectorName) + '">' + esc(connectorName) + '</div><div class="small text-secondary">' + esc(item.direction || '-') + '</div></td>' +
        '<td><div class="fw-semibold">' + esc(intervalLabel) + '</div><div class="small text-secondary">Nächster Lauf: ' + formatDate(item.nextRunAt, 'short') + '</div></td>' +
        '<td><div class="d-flex flex-wrap gap-1">' +
          '<button class="btn btn-sm btn-outline-primary" data-edit-schedule="' + esc(item.id) + '">Öffnen</button>' +
          '<button class="btn btn-sm btn-outline-secondary" data-dup-schedule="' + esc(item.id) + '">Dupl.</button>' +
          '<button class="btn btn-sm btn-outline-secondary" data-dup-reverse-schedule="' + esc(item.id) + '">Richtung dupl.</button>' +
          '<button class="btn btn-sm btn-outline-success" data-run-now="' + esc(item.id) + '">Run</button>' +
          '<button class="btn btn-sm btn-outline-info" data-dry-run="' + esc(item.id) + '">Dry-Run</button>' +
          '<button class="btn btn-sm btn-outline-danger" data-delete-schedule="' + esc(item.id) + '">Löschen</button>' +
        '</div>' +
        '</td>' +
      '</tr>';
  }).join('');

  body.querySelectorAll('button[data-edit-schedule]').forEach((button) => {
    button.addEventListener('click', () => openScheduleModal(button.getAttribute('data-edit-schedule')));
  });

  body.querySelectorAll('input[data-toggle-schedule-active]').forEach((input) => {
    input.addEventListener('change', async () => {
      const scheduleId = String(input.getAttribute('data-toggle-schedule-active') || '').trim();
      const nextActive = Boolean(input.checked);
      if (!scheduleId) {
        return;
      }
      try {
        await setScheduleActive(scheduleId, nextActive);
      } catch (error) {
        input.checked = !nextActive;
        showError(error.message || 'Scheduler-Status konnte nicht geändert werden');
      }
    });
  });

  body.querySelectorAll('button[data-dup-schedule]').forEach((button) => {
    button.addEventListener('click', async () => {
      await requestJson('/api/schedules/' + encodeURIComponent(button.getAttribute('data-dup-schedule')) + '/duplicate', { method: 'POST' });
      await refresh();
    });
  });

  body.querySelectorAll('button[data-dup-reverse-schedule]').forEach((button) => {
    button.addEventListener('click', async () => {
      await openReverseDirectionScheduleDraft(button.getAttribute('data-dup-reverse-schedule'));
    });
  });

  body.querySelectorAll('button[data-run-now]').forEach((button) => {
    button.addEventListener('click', async () => {
      try {
        const result = await requestJson('/api/schedules/' + encodeURIComponent(button.getAttribute('data-run-now')) + '/run', { method: 'POST' });
        showToast(result.message || 'Manueller Lauf gestartet.');
        await refresh();
      } catch (error) {
        showError(error.message || 'Manueller Lauf konnte nicht gestartet werden.');
      }
    });
  });

  body.querySelectorAll('button[data-dry-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      const scheduleId = button.getAttribute('data-dry-run');
      if (!scheduleId) {
        return;
      }

      const result = await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/dry-run', { method: 'POST' });
      const summary = result.ok
        ? 'OK: ' + (result.message || 'Quelle erreichbar')
        : 'Fehler: ' + (result.message || 'Dry-Run fehlgeschlagen');
      window.alert(summary);
    });
  });

  body.querySelectorAll('button[data-delete-schedule]').forEach((button) => {
    button.addEventListener('click', async () => {
      const scheduleId = button.getAttribute('data-delete-schedule');
      if (!scheduleId) {
        return;
      }

      const schedule = (state.schedules || []).find((item) => item.id === scheduleId);
      const confirmed = window.confirm('Scheduler "' + (schedule?.name || scheduleId) + '" inkl. Child-Scheduler löschen?');
      if (!confirmed) {
        return;
      }

      await requestJson('/api/schedules/' + encodeURIComponent(scheduleId), { method: 'DELETE' });
      await refresh();
    });
  });

  body.querySelectorAll('button[data-show-run-logs]').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = button.getAttribute('data-show-run-logs');
      if (!runId) {
        return;
      }
      const logs = await requestJson('/api/runs/' + encodeURIComponent(runId) + '/logs', {});
      const logList = (logs.items || []).map((log) => {
        return '[' + (log.level || 'INFO') + '] ' + (log.step || '') + ': ' + (log.message || '');
      }).join('\n');
      window.alert('Fehlerlog für Run ' + runId + ':\n\n' + (logList || 'Keine Logs vorhanden'));
    });
  });

  applySchedulerTableClientFilters();

  setTimeout(() => initializeTableFilters(), 100);
}

function renderRuns() {
  const body = document.getElementById('runs-body');
  const select = document.getElementById('log-run-select');
  if (!state.runs.length) {
    body.innerHTML = '<tr><td colspan="7" class="text-secondary">' + esc(state.runtimeContextUnavailableMessage || 'Keine Runs gefunden.') + '</td></tr>';
    select.innerHTML = '<option value="">Keine Runs</option>';
    return;
  }

  body.innerHTML = state.runs.map((item) =>
    (function() {
      const canCancel = String(item.status || '') === 'Running';
      const actionMarkup = canCancel
        ? '<button class="btn btn-sm btn-outline-danger" data-cancel-run="' + esc(item.id) + '">Abbrechen</button>'
        : '<span class="text-secondary small">-</span>';
      const failedCount = Math.max(0, Number(item.recordsFailed || 0) || 0);
      const failedButtonMarkup = '<button class="btn btn-sm ' + (failedCount > 0 ? 'btn-outline-danger' : 'btn-outline-secondary') + '" data-failed-run="' + esc(item.id) + '">Fehlerdaten' + (failedCount > 0 ? ' (' + failedCount + ')' : '') + '</button>';
      const durationMs = getRunDurationMs(item);
      return '<tr>' +
        '<td class="text-truncate" title="' + esc(item.scheduleName || item.scheduleId || '-') + '">' + esc(item.scheduleName || item.scheduleId || '-') + '</td>' +
        '<td>' + getStatusBadge(item.status) + '</td>' +
        '<td>' + esc(formatDate(item.startedAt || item.finishedAt, 'short')) + '</td>' +
        '<td>' + esc(formatDurationMinSec(durationMs)) + '</td>' +
        '<td>' + renderRunProgressMarkup(item) + '</td>' +
        '<td><div class="d-flex gap-1"><button class="btn btn-sm btn-outline-primary" data-log-run="' + esc(item.id) + '">Logs</button>' + failedButtonMarkup + '</div></td>' +
        '<td>' + actionMarkup + '</td>' +
        '</tr>';
    })()
  ).join('');

  select.innerHTML = state.runs.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.scheduleName || item.id) + '</option>').join('');

  body.querySelectorAll('button[data-log-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      select.value = button.getAttribute('data-log-run');
      await loadLogs();
    });
  });

  body.querySelectorAll('button[data-cancel-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = button.getAttribute('data-cancel-run');
      if (!runId) {
        return;
      }
      const run = (state.runs || []).find((item) => item.id === runId);
      const label = run?.scheduleName || runId;
      if (!confirm('Laufenden Run für ' + label + ' wirklich abbrechen?')) {
        return;
      }
      try {
        await requestJson('/api/runs/' + encodeURIComponent(runId) + '/cancel', {
          method: 'POST'
        });
        await refresh({ refreshChart: false });
      } catch (error) {
        showError(error.message || 'Run konnte nicht abgebrochen werden');
      }
    });
  });

  body.querySelectorAll('button[data-failed-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = button.getAttribute('data-failed-run');
      if (!runId) {
        return;
      }
      await openFailedRecordsForRun(runId);
    });
  });
}

function formatRunAgeMinutes(ageMinutes) {
  const totalMinutes = Math.max(0, Number(ageMinutes) || 0);
  if (totalMinutes >= 60 * 24) {
    const days = Math.floor(totalMinutes / (60 * 24));
    const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
    return days + 'd ' + hours + 'h';
  }
  if (totalMinutes >= 60) {
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return hours + 'h ' + minutes + 'm';
  }
  return totalMinutes + ' min';
}

function renderStaleRuns() {
  const body = document.getElementById('stale-runs-body');
  if (!body) {
    return;
  }

  const staleRuns = Array.isArray(state.staleRuns) ? state.staleRuns : [];
  if (!staleRuns.length) {
    body.innerHTML = '<tr><td colspan="4" class="text-secondary">' + esc(state.runtimeContextUnavailableMessage || 'Keine stale Runs gefunden.') + '</td></tr>';
    return;
  }

  body.innerHTML = staleRuns.map((item) =>
    '<tr>' +
    '<td class="text-truncate" title="' + esc(item.scheduleName || item.scheduleId || item.id) + '">' + esc(item.scheduleName || item.scheduleId || item.id) + '</td>' +
    '<td>' + esc(formatDate(item.startedAt, 'short')) + '</td>' +
    '<td><span class="badge text-bg-warning">' + esc(formatRunAgeMinutes(item.ageMinutes)) + '</span></td>' +
    '<td><button class="btn btn-sm btn-outline-danger" data-release-stale-run="' + esc(item.id) + '">Freigeben</button></td>' +
    '</tr>'
  ).join('');

  body.querySelectorAll('button[data-release-stale-run]').forEach((button) => {
    button.addEventListener('click', async () => {
      try {
        await requestJson('/api/runs/release-stale', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ runIds: [button.getAttribute('data-release-stale-run')] })
        });
        await refresh({ refreshChart: false });
      } catch (error) {
        showError(error.message || 'Stale Run konnte nicht freigegeben werden');
      }
    });
  });
}

function getConnectorIdsWithSchedulers() {
  return new Set(
    (state.schedules || [])
      .filter((schedule) => Boolean(schedule && schedule.active))
      .map((schedule) => String(schedule.connectorId || '').trim())
      .filter(Boolean)
  );
}

function parseTimingIntervalMinutes(timingDefinition) {
  if (!timingDefinition) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(String(timingDefinition));
    if (Number.isFinite(parsed?.intervalMinutes) && Number(parsed.intervalMinutes) > 0) {
      return Number(parsed.intervalMinutes);
    }

    if (Array.isArray(parsed?.rules)) {
      const ruleWithInterval = parsed.rules.find((rule) => Number.isFinite(rule?.intervalMinutes) && Number(rule.intervalMinutes) > 0);
      if (ruleWithInterval) {
        return Number(ruleWithInterval.intervalMinutes);
      }
    }
  } catch {
    return undefined;
  }

  return undefined;
}

function getScheduleIntervalLabel(schedule, scheduleById, fromParent, trail) {
  const visited = trail || new Set();
  if (!schedule || visited.has(schedule.id)) {
    return fromParent ? 'Parent' : '-';
  }

  visited.add(schedule.id);

  const ownInterval = parseTimingIntervalMinutes(schedule.timingDefinition);
  if (Number.isFinite(ownInterval) && ownInterval > 0) {
    return String(ownInterval) + ' min' + (fromParent ? ' (Parent)' : '');
  }

  const parentId = String(schedule.parentScheduleId || '').trim();
  if (schedule.inheritTimingFromParent && parentId) {
    const parent = scheduleById.get(parentId);
    if (!parent) {
      return 'Parent';
    }
    return getScheduleIntervalLabel(parent, scheduleById, true, visited);
  }

  return fromParent ? 'Parent' : '-';
}

function renderOverviewConnectorFilter() {
  const select = document.getElementById('overview-connector-filter');
  if (!select) {
    return;
  }

  const connectorIdsWithSchedulers = getConnectorIdsWithSchedulers();
  const selectableConnectors = (state.connectors || [])
    .filter((connector) => connectorIdsWithSchedulers.has(String(connector.id || '').trim()))
    .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'de', { sensitivity: 'base' }));

  const schedulerCountByConnectorId = new Map();
  (state.schedules || []).forEach((schedule) => {
    const connectorId = String(schedule.connectorId || '').trim();
    if (!connectorId) {
      return;
    }
    schedulerCountByConnectorId.set(connectorId, (schedulerCountByConnectorId.get(connectorId) || 0) + 1);
  });

  if (!selectableConnectors.some((connector) => String(connector.id) === state.overviewConnectorFilterId)) {
    state.overviewConnectorFilterId = '';
  }

  const options = ['<option value="">Alle mit Schedulern</option>'];
  selectableConnectors.forEach((connector) => {
    const connectorId = String(connector.id || '');
    const scheduleCount = Number(schedulerCountByConnectorId.get(connectorId) || 0);
    const label = String(connector.name || connectorId) + ' (' + scheduleCount + ')';
    options.push('<option value="' + esc(connectorId) + '">' + esc(label) + '</option>');
  });

  select.innerHTML = options.join('');
  select.value = state.overviewConnectorFilterId || '';
  select.disabled = selectableConnectors.length === 0;
}

function buildFilteredOverviewGraph(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];
  const selectedConnectorId = String(state.overviewConnectorFilterId || '').trim();

  const activeSchedules = (state.schedules || []).filter((schedule) => Boolean(schedule?.active));
  const activeScheduleIds = new Set(
    activeSchedules
      .map((schedule) => String(schedule.id || '').trim())
      .filter(Boolean)
  );
  const activeConnectorIds = new Set(
    activeSchedules
      .map((schedule) => String(schedule.connectorId || '').trim())
      .filter(Boolean)
  );
  const connectorActiveById = new Map(
    (state.connectors || []).map((connector) => [String(connector.id || '').trim(), Boolean(connector.active)])
  );

  const baseNodes = nodes.filter((node) => {
    if (node.kind === 'scheduler') {
      return activeScheduleIds.has(String(node.refId || '').trim());
    }
    if (node.kind === 'connector') {
      const connectorId = String(node.refId || '').trim();
      return activeConnectorIds.has(connectorId) && connectorActiveById.get(connectorId) !== false;
    }
    return true;
  });
  const baseNodeIds = new Set(baseNodes.map((node) => String(node.id || '')));
  const baseEdges = edges.filter(
    (edge) =>
      baseNodeIds.has(String(edge.from || '')) &&
      baseNodeIds.has(String(edge.to || ''))
  );

  if (!selectedConnectorId) {
    return { nodes: baseNodes, edges: baseEdges };
  }

  const selectedConnectorNode = baseNodes.find((node) => {
    if (node.kind !== 'connector') return false;
    const refId = String(node.refId || '').trim();
    const connectorIdField = String(node.connectorId || '').trim();
    // exact matches first
    if (refId === selectedConnectorId || connectorIdField === selectedConnectorId) return true;
    // node.id might directly equal connector id
    if (String(node.id || '').trim() === selectedConnectorId) return true;
    // allow prefixed forms like "project:connectorId" or "instance|connectorId" by splitting on common separators
    if (refId) {
      const parts = refId.split(/[:|]/).map((p) => p.trim()).filter(Boolean);
      if (parts.length && parts.includes(selectedConnectorId)) return true;
    }
    return false;
  });

  if (!selectedConnectorNode) {
    // Try to find the connector node in the full node set if it was filtered out because of active-only filtering
    const fallback = nodes.find((node) => node.kind === 'connector' && (
      String(node.refId || '').trim() === selectedConnectorId ||
      String(node.connectorId || '').trim() === selectedConnectorId ||
      String(node.id || '').trim() === selectedConnectorId
    ));
    if (fallback) {
      selectedConnectorNode = fallback;
    }
  }

  if (!selectedConnectorNode) {
    return { nodes: baseNodes, edges: baseEdges };
  }

  const outgoingEdgesByNode = new Map();
  // build using all edges so we can traverse to nodes that were filtered out earlier
  edges.forEach((edge) => {
    const fromId = String(edge.from || '');
    if (!outgoingEdgesByNode.has(fromId)) {
      outgoingEdgesByNode.set(fromId, []);
    }
    outgoingEdgesByNode.get(fromId).push(edge);
  });

  // Start with the selected connector node
  const keepNodeIds = new Set([selectedConnectorNode.id]);

  // Also include any scheduler nodes that belong to this connector (even if inactive)
  const relatedScheduleIds = new Set(
    (state.schedules || []).filter((s) => String(s.connectorId || '').trim() === selectedConnectorId).map((s) => String(s.id || '').trim())
  );
  if (relatedScheduleIds.size) {
    nodes.forEach((node) => {
      if (node.kind === 'scheduler' && relatedScheduleIds.has(String(node.refId || '').trim())) {
        keepNodeIds.add(node.id);
      }
    });
  }
  const queue = [selectedConnectorNode.id];

  while (queue.length) {
    const currentNodeId = queue.shift();
    const outgoing = outgoingEdgesByNode.get(String(currentNodeId || '')) || [];
    outgoing.forEach((edge) => {
      const toId = String(edge.to || '');
      if (!toId || keepNodeIds.has(toId)) {
        return;
      }
      keepNodeIds.add(toId);
      queue.push(toId);
    });
  }

  return {
    nodes: baseNodes.filter((node) => keepNodeIds.has(String(node.id || ''))),
    edges: baseEdges.filter(
      (edge) =>
        keepNodeIds.has(String(edge.from || '')) &&
        keepNodeIds.has(String(edge.to || ''))
    )
  };
}

function relayoutOverviewGraph(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes.map((node) => ({ ...node })) : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges.slice() : [];
  if (!nodes.length) {
    return { nodes, edges };
  }

  const nodeWidth = 330;
  const nodeHeight = 118;
  const hGap = 24;
  const vGap = 70;
  const connectorPad = 48;
  const baseY = 26;

  const connectors = nodes.filter((node) => node.kind === 'connector');
  const schedulers = nodes.filter((node) => node.kind === 'scheduler');
  const schedulerById = new Map(schedulers.map((node) => [String(node.id || ''), node]));
  const schedulerIds = new Set(schedulers.map((node) => String(node.id || '')));

  const childrenByParent = new Map();
  schedulers.forEach((node) => childrenByParent.set(String(node.id || ''), []));

  const rootSchedulersByConnector = new Map();
  connectors.forEach((node) => rootSchedulersByConnector.set(String(node.id || ''), []));

  edges.forEach((edge) => {
    const from = String(edge.from || '');
    const to = String(edge.to || '');
    if (schedulerIds.has(from) && schedulerIds.has(to)) {
      childrenByParent.get(from)?.push(to);
      return;
    }
    const fromNode = nodes.find((node) => String(node.id || '') === from);
    if (fromNode?.kind === 'connector' && schedulerIds.has(to)) {
      rootSchedulersByConnector.get(from)?.push(to);
    }
  });

  childrenByParent.forEach((childIds) => {
    childIds.sort((leftId, rightId) =>
      String(schedulerById.get(leftId)?.label || '').localeCompare(
        String(schedulerById.get(rightId)?.label || ''),
        'de',
        { sensitivity: 'base' }
      )
    );
  });

  rootSchedulersByConnector.forEach((rootIds) => {
    rootIds.sort((leftId, rightId) =>
      String(schedulerById.get(leftId)?.label || '').localeCompare(
        String(schedulerById.get(rightId)?.label || ''),
        'de',
        { sensitivity: 'base' }
      )
    );
  });

  const subtreeWidthCache = new Map();
  const computeSubtreeWidth = (id) => {
    if (subtreeWidthCache.has(id)) return subtreeWidthCache.get(id);
    const childIds = childrenByParent.get(id) || [];
    let width;
    if (!childIds.length) {
      width = nodeWidth;
    } else {
      const totalChildWidth = childIds.reduce((sum, childId, index) => {
        return sum + computeSubtreeWidth(childId) + (index < childIds.length - 1 ? hGap : 0);
      }, 0);
      width = Math.max(nodeWidth, totalChildWidth);
    }
    subtreeWidthCache.set(id, width);
    return width;
  };

  const placeSchedule = (id, centerX, topY) => {
    const node = schedulerById.get(id);
    if (!node) return;
    node.x = centerX - nodeWidth / 2;
    node.y = topY;
    const childIds = childrenByParent.get(id) || [];
    if (childIds.length) {
      const totalChildWidth = childIds.reduce((sum, childId, index) => {
        return sum + computeSubtreeWidth(childId) + (index < childIds.length - 1 ? hGap : 0);
      }, 0);
      let cursorX = centerX - totalChildWidth / 2;
      childIds.forEach((childId) => {
        const childWidth = computeSubtreeWidth(childId);
        placeSchedule(childId, cursorX + childWidth / 2, topY + nodeHeight + vGap);
        cursorX += childWidth + hGap;
      });
    }
  };

  connectors.sort((left, right) =>
    String(left.label || '').localeCompare(String(right.label || ''), 'de', { sensitivity: 'base' })
  );

  let cursorX = 30;
  const connectorY = baseY;
  const schedulerStartY = connectorY + nodeHeight + vGap;

  connectors.forEach((connectorNode, index) => {
    const connectorId = String(connectorNode.id || '');
    const rootIds = rootSchedulersByConnector.get(connectorId) || [];

    const totalRootsWidth = rootIds.length
      ? rootIds.reduce((sum, rootId, i) => {
          return sum + computeSubtreeWidth(rootId) + (i < rootIds.length - 1 ? hGap : 0);
        }, 0)
      : 0;

    const groupWidth = Math.max(nodeWidth, totalRootsWidth);
    const groupCenterX = cursorX + groupWidth / 2;

    connectorNode.x = groupCenterX - nodeWidth / 2;
    connectorNode.y = connectorY;

    let schedulerCursorX = groupCenterX - totalRootsWidth / 2;
    rootIds.forEach((rootId) => {
      const rootWidth = computeSubtreeWidth(rootId);
      placeSchedule(rootId, schedulerCursorX + rootWidth / 2, schedulerStartY);
      schedulerCursorX += rootWidth + hGap;
    });

    cursorX += groupWidth + (index < connectors.length - 1 ? connectorPad : 0);
  });

  return { nodes, edges };
}

function redrawOverviewGraph() {
  const filteredGraph = buildFilteredOverviewGraph(state.graphData);
  drawGraph(relayoutOverviewGraph(filteredGraph));
}

function drawGraph(graph) {
  const svg = document.getElementById('graph');
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  const visibleScheduleCountElement = document.getElementById('overview-visible-schedule-count');
  if (visibleScheduleCountElement) {
    const visibleScheduleCount = nodes.filter((node) => node.kind === 'scheduler').length;
    visibleScheduleCountElement.textContent = String(visibleScheduleCount) + ' Scheduler sichtbar';
  }
  const latestRunByScheduleId = new Map();
  (state.runs || []).forEach((run) => {
    const scheduleId = String(run.scheduleId || '').trim();
    if (!scheduleId) {
      return;
    }

    const previous = latestRunByScheduleId.get(scheduleId);
    const previousTime = previous
      ? new Date(previous.startedAt || previous.finishedAt || 0).getTime()
      : -Infinity;
    const currentTime = new Date(run.startedAt || run.finishedAt || 0).getTime();
    if (!previous || currentTime >= previousTime) {
      latestRunByScheduleId.set(scheduleId, run);
    }
  });
  const scheduleById = new Map((state.schedules || []).map((schedule) => [String(schedule.id || ''), schedule]));
  const getLatestFailedRun = (scheduleId) => {
    return (state.runs || [])
      .filter((run) => String(run.scheduleId || '') === String(scheduleId || '') && normalizeRunStatus(run.status) === 'failed')
      .sort((a, b) => {
        const timeA = new Date(a.finishedAt || a.startedAt || 0).getTime();
        const timeB = new Date(b.finishedAt || b.startedAt || 0).getTime();
        return timeB - timeA;
      })[0] || null;
  };
  const getLatestRunningRun = (scheduleId) => {
    return (state.runs || [])
      .filter((run) => String(run.scheduleId || '') === String(scheduleId || '') && normalizeRunStatus(run.status) === 'running')
      .sort((a, b) => {
        const timeA = new Date(a.startedAt || a.finishedAt || 0).getTime();
        const timeB = new Date(b.startedAt || b.finishedAt || 0).getTime();
        return timeB - timeA;
      })[0] || null;
  };
  const getAverageScheduleRunDurationMs = (scheduleId) => {
    const durations = (state.runs || [])
      .filter((run) => String(run.scheduleId || '') === String(scheduleId || '') && run.finishedAt && run.startedAt)
      .map((run) => getRunDurationMs(run))
      .filter((duration) => duration !== null);
    if (!durations.length) {
      return null;
    }
    return durations.reduce((sum, duration) => sum + duration, 0) / durations.length;
  };
  const formatGraphDateShort = (value) => {
    if (!value) {
      return '-';
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return '-';
    }
    return date.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit' }) + ' ' +
      date.toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
  };
  const getGraphProgressLabel = (run) => {
    const metrics = buildRunProgressMetrics(run);
    if (!metrics || metrics.normalizedStatus !== 'running') {
      return '';
    }
    if (metrics.total > 0) {
      return metrics.completed + '/' + metrics.total + ' (' + Math.round(metrics.completedPercent) + '%)';
    }
    return metrics.recordsProcessed > 0
      ? metrics.recordsProcessed + ' verarbeitet'
      : 'wartet auf Daten';
  };
  const getGraphRecordCountLabel = (run) => {
    const metrics = buildRunProgressMetrics(run);
    if (!metrics || metrics.total <= 0) {
      return '-';
    }
    return String(metrics.total);
  };
  const getScheduleGraphStatus = (scheduleId) => {
    const schedule = scheduleById.get(String(scheduleId || ''));
    const latestRun = latestRunByScheduleId.get(String(scheduleId || ''));
    const latestRunStatus = normalizeRunStatus(latestRun?.status);
    if (latestRunStatus === 'running' || latestRunStatus === 'in-progress') {
      return { key: 'running', label: 'Läuft' };
    }
    if (latestRunStatus === 'failed' || latestRunStatus === 'error') {
      return { key: 'failed', label: 'Fehler' };
    }
    const scheduleStatus = String(schedule?.status || '').trim().toLowerCase();
    if (!schedule?.active || scheduleStatus === 'inactive') {
      return { key: 'inactive', label: 'Inaktiv' };
    }
    if (scheduleStatus === 'due') {
      return { key: 'due', label: 'Fällig' };
    }
    if (latestRunStatus === 'success' || latestRunStatus === 'succeeded') {
      return { key: 'success', label: 'OK' };
    }
    return { key: 'scheduled', label: 'Geplant' };
  };
  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  const nodeWidth = 330;
  const nodeHeight = 118;
  const maxY = Math.max(360, ...nodes.map((node) => Number(node.y) + nodeHeight + 20));
  const maxX = Math.max(920, ...nodes.map((node) => Number(node.x) + nodeWidth + 24));
  svg.setAttribute('height', String(maxY));
  svg.setAttribute('width', String(maxX));
  svg.setAttribute('viewBox', '0 0 ' + String(maxX) + ' ' + String(maxY));
  svg.setAttribute('preserveAspectRatio', 'xMinYMin meet');

  const defs = '<defs>' +
    '<marker id="arrowInbound" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
      '<polygon points="0,1 0,9 12,5" style="fill:#2276d2" />' +
    '</marker>' +
    '<marker id="arrowOutbound" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
      '<polygon points="0,1 0,9 12,5" style="fill:#2e9b4d" />' +
    '</marker>' +
    '<marker id="arrowGeneric" markerWidth="14" markerHeight="10" refX="12" refY="5" orient="auto" markerUnits="userSpaceOnUse">' +
      '<polygon points="0,1 0,9 12,5" style="fill:#7f8b95" />' +
    '</marker>' +
  '</defs>';

  const edgeMarkup = edges.map((edge) => {
    const from = nodeMap.get(edge.from);
    const to = nodeMap.get(edge.to);
    if (!from || !to) return '';

    const normalizedDirection = String(edge.direction || '').toLowerCase();
    const isInbound = normalizedDirection === 'inbound';
    const isOutbound = normalizedDirection === 'outbound';
    const edgeColor = isInbound ? '#2276d2' : isOutbound ? '#2e9b4d' : '#7f8b95';
    const markerId = isInbound ? 'arrowInbound' : isOutbound ? 'arrowOutbound' : 'arrowGeneric';
    // Top-down layout: from bottom-center of source to top-center of target
    const startX = Number(from.x) + nodeWidth / 2;
    const startY = Number(from.y) + nodeHeight;
    const endX = Number(to.x) + nodeWidth / 2;
    const endY = Number(to.y) - 14;
    const controlOffset = Math.max(40, Math.abs(endY - startY) / 2);
    const pathData = 'M ' + startX + ' ' + startY +
      ' C ' + startX + ' ' + (startY + controlOffset) + ', ' + endX + ' ' + (endY - controlOffset) + ', ' + endX + ' ' + endY;
    const fromStatus = from.kind === 'scheduler' ? getScheduleGraphStatus(from.refId).key : '';
    const toStatus = to.kind === 'scheduler' ? getScheduleGraphStatus(to.refId).key : '';
    const edgeClasses = ['graph-edge'];
    if (fromStatus === 'running' || toStatus === 'running') {
      edgeClasses.push('graph-edge-running');
    }
    if (fromStatus === 'failed' || toStatus === 'failed') {
      edgeClasses.push('graph-edge-failed');
    }

    return '<path class="' + edgeClasses.join(' ') + '" style="stroke:' + edgeColor + ';stroke-width:2.5;fill:none;opacity:0.9;stroke-linecap:round;stroke-linejoin:round" marker-end="url(#' + markerId + ')" d="' + pathData + '" />';
  }).join('');

  const nodeMarkup = nodes.map((node) => {
    const isInbound = String(node.direction || '').toLowerCase() === 'inbound';
    const schedulerUsesFile = String(node.sourceType || '').toUpperCase().startsWith('FILE_') || String(node.targetType || '').toUpperCase().startsWith('FILE_');
    const schedulerStatus = node.kind === 'scheduler'
      ? getScheduleGraphStatus(node.refId)
      : null;
    const directionClass = node.kind === 'scheduler'
      ? ((isInbound ? 'graph-inbound' : 'graph-outbound') + ' ' + (schedulerUsesFile ? 'graph-scheduler-file' : 'graph-scheduler-db') + ' graph-node-status-' + schedulerStatus.key)
      : 'graph-connector ' + getConnectorGraphClass(node.connectorType, node.label);
    const titleLines = node.kind === 'scheduler'
      ? splitGraphTextByLine(node.label, [16, 18])
      : splitGraphText(node.label, 24, 2);
    const subtitleLines = splitGraphText(node.subtitle, 28, 2);
    const icon = node.kind === 'scheduler'
      ? getObjectIcon(node.objectName)
      : getConnectorIcon(node.connectorType, node.label);
    const metaLabel = node.kind === 'scheduler'
      ? (isInbound ? 'Inbound' : 'Outbound')
      : String(node.connectorType || 'Connector').toUpperCase();
    const metaLines = splitGraphText(metaLabel, 22, 1);
    const failedRun = node.kind === 'scheduler' && schedulerStatus?.key === 'failed'
      ? getLatestFailedRun(node.refId)
      : null;
    const runningRun = node.kind === 'scheduler' && schedulerStatus?.key === 'running'
      ? getLatestRunningRun(node.refId)
      : null;
    const failedRunErrorMessage = failedRun?.errorMessage
      ? String(failedRun.errorMessage).trim()
      : '';
    const titleY = 26;
    const subtitleY = titleLines.length > 1 ? 55 : 48;
    const metaY = titleLines.length > 1 ? 68 : 61;
    const titleMarkup = renderGraphText('graph-title', 58, titleY, titleLines, 14);
    const subtitleMarkup = renderGraphText('graph-subtitle', 58, subtitleY, subtitleLines, 13);
    const metaMarkup = renderGraphText('graph-meta', 58, metaY, metaLines, 12);
    const pillRun = schedulerStatus?.key === 'running' ? runningRun : failedRun;
    const pillAction = schedulerStatus?.key === 'running' ? 'cancel' : (failedRun?.id ? 'logs' : '');
    const statusColorMap = {
      running: '#1d7ed6',
      failed: '#d13f45',
      inactive: '#8793a0',
      due: '#d78c1d',
      success: '#2e9b4d',
      scheduled: '#5f6b76'
    };
    const statusColor = statusColorMap[schedulerStatus?.key] || statusColorMap.scheduled;
    const statusIconMarkup = node.kind === 'scheduler'
      ? '<g class="graph-status-pill graph-status-pill-' + esc(schedulerStatus.key) + (pillRun?.id ? ' graph-status-pill-clickable' : '') + '" transform="translate(302,14)"' +
          (pillRun?.id ? ' data-run-id="' + esc(pillRun.id) + '"' : '') +
          (pillAction ? ' data-pill-action="' + esc(pillAction) + '"' : '') +
          (failedRunErrorMessage ? ' data-error-message="' + esc(failedRunErrorMessage) + '"' : '') +
          (pillRun?.id ? ' data-schedule-name="' + esc(node.label || node.refId || '') + '"' : '') +
          '>' +
          '<title>' + esc(schedulerStatus.label) + '</title>' +
          '<circle cx="10" cy="10" r="9" style="fill:' + statusColor + ';stroke:rgba(47,64,80,0.18);stroke-width:1" />' +
          '<circle cx="10" cy="10" r="3" style="fill:#ffffff;opacity:0.92" />' +
        '</g>'
      : '';
    const directionBadgeMarkup = node.kind === 'scheduler'
      ? '<g class="graph-direction-badge" transform="translate(188,10)">' +
          '<rect width="102" height="26" rx="8" style="fill:' + (isInbound ? '#dcebff' : '#dcf3e0') + ';stroke:' + (isInbound ? '#2276d2' : '#2e9b4d') + ';stroke-width:1" />' +
          '<text x="51" y="17" text-anchor="middle" style="fill:' + (isInbound ? '#1d62ad' : '#1f7c3d') + ';font-size:11px;font-weight:850">' + esc(isInbound ? 'Inbound' : 'Outbound') + '</text>' +
        '</g>'
      : '';
    const schedule = node.kind === 'scheduler' ? scheduleById.get(String(node.refId || '')) : null;
    const latestRun = node.kind === 'scheduler' ? latestRunByScheduleId.get(String(node.refId || '')) : null;
    const averageDurationMs = node.kind === 'scheduler' ? getAverageScheduleRunDurationMs(node.refId) : null;
    const averageDurationLabel = averageDurationMs === null ? '-' : formatDurationMinSec(averageDurationMs);
    const progressLabel = node.kind === 'scheduler' ? getGraphProgressLabel(runningRun) : '';
    const nextRunLabel = node.kind === 'scheduler'
      ? formatGraphDateShort(node.nextRunAt || schedule?.nextRunAt)
      : '-';
    const statusKpiLabel = progressLabel || schedulerStatus?.label || '-';
    const recordCountLabel = getGraphRecordCountLabel(runningRun || latestRun);
    const schedulerStatsMarkup = node.kind === 'scheduler'
      ? '<g class="graph-scheduler-stats" transform="translate(58,74)">' +
          '<rect width="260" height="34" rx="7" style="fill:rgba(255,255,255,0.78);stroke:rgba(47,64,80,0.12);stroke-width:1" />' +
          '<text x="8" y="14" style="fill:#2f4050;font-size:10.5px;font-weight:850">' + esc(statusKpiLabel + ' · Ø ' + averageDurationLabel) + '</text>' +
          '<text x="8" y="28" style="fill:#4f5f6d;font-size:10px;font-weight:750">' + esc('DS ' + recordCountLabel + ' · Start ' + nextRunLabel) + '</text>' +
        '</g>'
      : '';

    // Inline styles to guarantee fill even when external CSS is not applied to inline SVG
    let cardBgStyle, accentStyle, badgeStyle, iconStyle, metaStyle;
    if (node.kind === 'scheduler') {
      if (isInbound) {
        cardBgStyle = 'fill:#edf5ff;stroke:#2276d2;stroke-width:2';
        accentStyle = 'fill:#2276d2;stroke:none';
        badgeStyle = 'fill:#dcebff;stroke:rgba(34,118,210,0.15);stroke-width:1';
        iconStyle = 'fill:#2276d2';
        metaStyle = 'fill:#2276d2';
      } else {
        cardBgStyle = 'fill:#eefaf0;stroke:#2e9b4d;stroke-width:2';
        accentStyle = 'fill:#2e9b4d;stroke:none';
        badgeStyle = 'fill:#dcf3e0;stroke:rgba(46,155,77,0.15);stroke-width:1';
        iconStyle = 'fill:#2e9b4d';
        metaStyle = 'fill:#2e9b4d';
      }
    } else {
      const connectorClass = getConnectorGraphClass(node.connectorType, node.label);
      const colorMap = {
        'graph-connector-salesforce': { card: 'fill:#eef8ff;stroke:#3a8dde', accent: '#2d7dd2', badge: '#dceefe' },
        'graph-connector-mssql':      { card: 'fill:#f3f8f3;stroke:#2f8f5b', accent: '#2f8f5b', badge: '#e0f1e6' },
        'graph-connector-file':       { card: 'fill:#fff8ec;stroke:#d78c1d', accent: '#d78c1d', badge: '#ffefcf' },
        'graph-connector-mock':       { card: 'fill:#f6eefc;stroke:#8a56c2', accent: '#8a56c2', badge: '#eadcf8' },
        'graph-connector-erp':        { card: 'fill:#fff2ea;stroke:#c7683b', accent: '#c7683b', badge: '#fde2d6' },
        'graph-connector-generic':    { card: 'fill:#f7f8f9;stroke:#7f8b95', accent: '#7f8b95', badge: '#e9edf0' },
      };
      const colors = colorMap[connectorClass] || colorMap['graph-connector-generic'];
      cardBgStyle = colors.card + ';stroke-width:2';
      accentStyle = 'fill:' + colors.accent + ';stroke:none';
      badgeStyle = 'fill:' + colors.badge + ';stroke:rgba(47,64,80,0.1);stroke-width:1';
      iconStyle = 'fill:' + colors.accent;
      metaStyle = 'fill:#5f6b76';
    }

    return (
      '<g class="graph-node ' + directionClass + '" data-kind="' + esc(node.kind) + '" data-ref-id="' + esc(node.refId) + '" data-connector-type="' + esc(node.connectorType || '') + '" transform="translate(' + Number(node.x) + ',' + Number(node.y) + ')" title="' + esc(node.label) + '">' +
        '<rect class="graph-card-bg" style="' + cardBgStyle + '" width="' + nodeWidth + '" height="' + nodeHeight + '" rx="8" />' +
        '<rect class="graph-accent" style="' + accentStyle + '" width="10" height="' + nodeHeight + '" rx="8" />' +
        '<circle class="graph-icon-badge" style="' + badgeStyle + '" cx="30" cy="52" r="20" />' +
        '<text class="graph-icon" style="' + iconStyle + '" x="30" y="59">' + esc(icon) + '</text>' +
        directionBadgeMarkup +
        statusIconMarkup +
        schedulerStatsMarkup +
        titleMarkup.replace('<text ', '<text style="fill:#2f4050;font-weight:800;font-size:13px" ') +
        subtitleMarkup.replace('<text ', '<text style="fill:#66717d;font-size:11.5px;font-weight:600" ') +
        (node.kind === 'scheduler' ? '' : metaMarkup.replace('<text ', '<text style="' + metaStyle + ';font-size:10.5px;font-weight:800;letter-spacing:0.6px" ')) +
      '</g>'
    );
  }).join('');

  svg.innerHTML = defs + edgeMarkup + nodeMarkup;
  svg.querySelectorAll('g.graph-status-pill[data-run-id]').forEach((pillEl) => {
    pillEl.addEventListener('click', async (event) => {
      event.stopPropagation();
      const runId = String(pillEl.getAttribute('data-run-id') || '').trim();
      const action = String(pillEl.getAttribute('data-pill-action') || '').trim();
      const errorMessage = String(pillEl.getAttribute('data-error-message') || '').trim();
      const scheduleName = String(pillEl.getAttribute('data-schedule-name') || '').trim();
      if (!runId) {
        return;
      }
      if (action === 'cancel') {
        if (!window.confirm('Laufenden Run für ' + (scheduleName || 'diesen Scheduler') + ' wirklich abbrechen?')) {
          return;
        }
        try {
          await requestJson('/api/runs/' + encodeURIComponent(runId) + '/cancel', {
            method: 'POST'
          });
          await refresh({ refreshChart: false });
        } catch (error) {
          showError(error.message || 'Run konnte nicht abgebrochen werden');
        }
        return;
      }
      if (errorMessage) {
        window.alert('Letzter Fehler für ' + (scheduleName || 'diesen Scheduler') + ':\n\n' + errorMessage);
        return;
      }

      const logs = await requestJson('/api/runs/' + encodeURIComponent(runId) + '/logs', {});
      const logList = (logs.items || []).map((log) => {
        return '[' + (log.level || 'INFO') + '] ' + (log.step || '') + ': ' + (log.message || '');
      }).join('\n');
      window.alert('Fehlerdetails für ' + (scheduleName || 'diesen Scheduler') + ':\n\n' + (logList || 'Keine Fehlerdetails vorhanden.'));
    });
  });
  svg.querySelectorAll('g.graph-node').forEach((nodeEl) => {
    nodeEl.addEventListener('click', () => {
      const kind = nodeEl.getAttribute('data-kind');
      const refId = nodeEl.getAttribute('data-ref-id');
      if (kind === 'connector') {
        openConnectorModal(refId);
      }
      if (kind === 'scheduler') {
        openScheduleModal(refId);
      }
    });

    const kind = nodeEl.getAttribute('data-kind');
    const refId = nodeEl.getAttribute('data-ref-id');
    const connectorType = nodeEl.getAttribute('data-connector-type');
    if (kind === 'connector' && isFileConnectorType(connectorType)) {
      nodeEl.addEventListener('dragover', (event) => {
        event.preventDefault();
        nodeEl.classList.add('graph-drop-target');
      });

      nodeEl.addEventListener('dragleave', () => {
        nodeEl.classList.remove('graph-drop-target');
      });

      nodeEl.addEventListener('drop', async (event) => {
        event.preventDefault();
        nodeEl.classList.remove('graph-drop-target');
        const file = event.dataTransfer?.files?.[0];
        if (!file) {
          return;
        }

        try {
          await createSchedulerFromDroppedFile(refId, file);
        } catch (error) {
          showError(error.message || 'Datei konnte nicht als Scheduler importiert werden');
        }
      });
    }
  });
}

function collectScheduleFormPayload() {
  const sourceType = document.getElementById('sch-source-type').value || undefined;
  const targetType = document.getElementById('sch-target-type').value || undefined;
  const isFileSource = isFileScheduleSourceType(String(sourceType || '').trim().toUpperCase());
  const isFileTarget = isFileScheduleTargetType(String(targetType || '').trim().toUpperCase());
  const selectedWeekdays = Array.from(document.querySelectorAll('#sch-weekdays input:checked'))
    .map((input) => Number(input.value))
    .filter((value) => !Number.isNaN(value));
  const timingDefinition = {
    days: selectedWeekdays,
    intervalMinutes: Number(document.getElementById('sch-timing-interval').value || 2),
    startTime: document.getElementById('sch-timing-time').value || '09:00'
  };

  return {
    id: document.getElementById('sch-id').value || undefined,
    name: document.getElementById('sch-name').value || undefined,
    active: document.getElementById('sch-active').checked,
    sourceSystem: isFileSource ? 'File' : normalizeSystemValue(document.getElementById('sch-source-system').value),
    targetSystem: isFileTarget ? 'File' : normalizeSystemValue(document.getElementById('sch-target-system').value),
    objectName: isFileTarget ? 'FileExport' : document.getElementById('sch-object').value,
    operation: isFileTarget ? 'Write' : normalizeOperationValue(document.getElementById('sch-operation').value),
    connectorId: document.getElementById('sch-connector').value || undefined,
    parentScheduleId: document.getElementById('sch-parent-schedule').value || undefined,
    inheritTimingFromParent: document.getElementById('sch-inherit-parent-timing').checked,
    sourceType,
    targetType,
    direction: document.getElementById('sch-direction').value || undefined,
    batchSize: Number(document.getElementById('sch-batch-size').value || 100),
    nextRunAt: localDateTimeInputToIso(document.getElementById('sch-next-run').value),
    lastRunAt: localDateTimeInputToIso(document.getElementById('sch-last-run').value),
    sourceDefinition: buildScheduleSourceDefinitionValue(),
    targetDefinition: buildScheduleTargetDefinitionValue(),
    mappingDefinition: (document.getElementById('sch-mapping').value || undefined),
    timingDefinition: JSON.stringify(timingDefinition)
  };
}

async function saveCurrentAsTemplate(kind) {
  const payload = kind === 'schedule'
    ? { kind, schedule: collectScheduleFormPayload() }
    : { kind, connector: collectConnectorFormPayload() };
  const defaultName = kind === 'schedule'
    ? String(payload.schedule?.name || document.getElementById('sch-object').value || 'Neue Scheduler Vorlage').trim()
    : String(payload.connector?.name || document.getElementById('con-name').value || 'Neue Connector Vorlage').trim();
  const name = window.prompt('Vorlagenname', defaultName || 'Neue Vorlage');
  if (name === null) {
    return;
  }
  const description = window.prompt('Kurzbeschreibung (optional)', '');
  const result = await requestJson('/api/templates', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      ...payload,
      name,
      description: description === null ? undefined : description
    })
  });
  window.alert('Vorlage gespeichert: ' + (result.name || name));
}

function normalizeAffectedMappingField(rawFieldName) {
  const raw = String(rawFieldName || '').trim();
  if (!raw) {
    return '';
  }
  const token = raw.includes('.') ? raw.split('.').pop() : raw;
  return String(token || '').trim();
}

function focusScheduleMappingField(fieldName) {
  const normalized = normalizeFieldKey(fieldName);
  if (!normalized) {
    return false;
  }

  const matchedRule = (Array.isArray(state.mappingRules) ? state.mappingRules : []).find((rule) => {
    const targetMatch = normalizeFieldKey(rule?.targetField) === normalized;
    const sourceMatch = normalizeFieldKey(rule?.sourceField) === normalized;
    return targetMatch || sourceMatch;
  });

  if (!matchedRule?.id) {
    return false;
  }

  state.selectedMappingRuleId = matchedRule.id;
  renderMappingRulesTable();

  const row = document.querySelector('#sch-mapping-rules tr[data-rule-id="' + matchedRule.id + '"]');
  if (row && typeof row.scrollIntoView === 'function') {
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
  return true;
}

async function openReverseDirectionScheduleDraft(scheduleId, scheduleDraftOverride) {
  const normalizedScheduleId = String(scheduleId || '').trim();
  if (!normalizedScheduleId) {
    return;
  }

  const requestOptions = {
    method: 'POST'
  };
  if (scheduleDraftOverride && typeof scheduleDraftOverride === 'object') {
    requestOptions.headers = { 'Content-Type': 'application/json' };
    requestOptions.body = JSON.stringify({ schedule: scheduleDraftOverride });
  }
  const draftResult = await requestJson('/api/schedules/' + encodeURIComponent(normalizedScheduleId) + '/duplicate-reverse-draft', requestOptions);
  const draft = draftResult && typeof draftResult === 'object' ? draftResult.schedule : null;
  if (!draft || typeof draft !== 'object') {
    throw new Error('Reverse-Draft konnte nicht erzeugt werden.');
  }

  await openScheduleModal('', draft, { wizardStep: 1 });
  const warnings = Array.isArray(draftResult.warnings) ? draftResult.warnings.filter(Boolean) : [];
  if (warnings.length) {
    showInfo('Entwurf mit gedrehter Richtung erzeugt. Hinweis: ' + warnings.join(' '));
  } else {
    showToast('Entwurf mit gedrehter Richtung erzeugt.');
  }
}
