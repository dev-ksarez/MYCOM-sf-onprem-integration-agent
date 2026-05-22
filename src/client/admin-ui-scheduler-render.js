// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-scheduler-render
// Source lines: 16819–17050
// ──────────────────────────────────────────────────────────────────────

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
  const roots = [];
  const childrenByParent = new Map();

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
      const filePathLines = buildScheduleFilePathLines(item);
      const hierarchyBadge = depth > 0
        ? '<span class="badge bg-light text-dark border ms-1">Level ' + (depth + 1) + '</span>'
        : '<span class="badge bg-secondary-subtle text-secondary border ms-1">Root</span>';
      const activeHint = item.autoDisabledDueToErrors
        ? '<span class="badge bg-warning-subtle text-warning border mt-1" title="Automatisch wegen Fehlern deaktiviert">auto deaktiviert</span>'
        : '<span class="small text-secondary">' + (item.active ? 'aktiv' : 'inaktiv') + '</span>';
      const lastFailedRun = (state.runs || [])
        .filter((run) => run.scheduleId === item.id && run.status === 'Failed')
        .sort((a, b) => {
          const timeA = new Date(a.finishedAt || 0).getTime();
          const timeB = new Date(b.finishedAt || 0).getTime();
          return timeB - timeA;
        })
        [0];
      const errorMarkup = lastFailedRun
        ? '<button class="btn btn-sm btn-outline-danger mt-2" title="Letzter Fehler: ' + esc(lastFailedRun.errorMessage || 'Unbekannter Fehler') + '" data-show-run-logs="' + esc(lastFailedRun.id) + '">Fehlerdetails</button>'
        : '<span class="small text-secondary d-block mt-2">keine offenen Fehler</span>';
      
      return '<tr data-schedule-active="' + (item.active ? 'active' : 'inactive') + '">' +
        '<td><div style="padding-left:' + indent + 'px"><strong class="text-truncate d-block" title="' + esc(item.name) + '">' + esc(item.name) + hierarchyBadge + '</strong><div class="small text-secondary text-truncate" title="' + esc(item.objectName) + ' / ' + esc(item.operation) + '">' + objectIcon + ' ' + esc(item.objectName) + ' / ' + esc(item.operation) + '</div><div class="small text-secondary text-truncate mt-1" title="' + esc(parentName) + '">Parent: ' + esc(parentName) + (item.inheritTimingFromParent ? ' <span class="badge bg-primary-subtle text-primary border">inherits</span>' : '') + '</div></div></td>' +
        '<td><div class="form-check form-switch mb-1"><input class="form-check-input" type="checkbox" role="switch" data-toggle-schedule-active="' + esc(item.id) + '"' + (item.active ? ' checked' : '') + '></div>' + activeHint + '</td>' +
        '<td>' + getStatusBadge(item.status) + errorMarkup + '</td>' +
        '<td><div class="fw-semibold text-truncate" title="' + esc(connectorName) + '">' + esc(connectorName) + '</div><div class="small text-secondary">' + esc(item.direction || '-') + '</div>' + (filePathLines.length ? '<div class="small text-secondary mt-1">' + renderScheduleFilePathLines(filePathLines) + '</div>' : '') + '</td>' +
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


