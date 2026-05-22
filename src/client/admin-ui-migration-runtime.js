// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-migration-runtime
// Source lines: 17051–18044
// ──────────────────────────────────────────────────────────────────────

function renderMigPreflightWarnings() {
  const el = document.getElementById('mig-preflight-summary');
  if (!el) return;

  if (migState.preflightWarningsLoading) {
    el.innerHTML = '<div class="alert alert-light border small mb-0">Pruefe Salesforce-Dubletten fuer Upserts…</div>';
    return;
  }

  const items = Array.isArray(migState.preflightWarnings?.items) ? migState.preflightWarnings.items : [];
  if (!items.length) {
    el.innerHTML = '<div class="alert alert-success small mb-0">Vorab-Check: Keine mehrdeutigen External-ID-Treffer in Salesforce gefunden.</div>';
    return;
  }

  el.innerHTML = '<div class="alert alert-warning small mb-0">' +
    '<strong>Vorab-Check:</strong> Es wurden ' + items.reduce((sum, item) => sum + Number(item.affectedRecordCount || 0), 0) +
    ' Datensaetze mit mehrdeutiger External ID in Salesforce gefunden.' +
    items.map((item) => {
      const preview = (item.conflicts || []).slice(0, 10).map((conflict) =>
        '<li><code>' + esc(conflict.value) + '</code> — Zeilen ' + esc((conflict.rowIndexes || []).join(', ')) +
        ' — Salesforce IDs: ' + esc((conflict.existingIds || []).join(', ')) + '</li>'
      ).join('');
      const remaining = Math.max(0, Number(item.conflictCount || 0) - 10);
      return '<div class="mt-2"><strong>' + esc(item.salesforceObject) + '</strong> via <code>' + esc(item.externalIdField) + '</code>: ' +
        Number(item.conflictCount || 0) + ' Konflikte / ' + Number(item.affectedRecordCount || 0) + ' betroffene Zeilen' +
        '<ul class="mt-1 mb-0">' + preview + (remaining ? '<li>… und ' + remaining + ' weitere</li>' : '') + '</ul></div>';
    }).join('') +
    '</div>';
}

async function loadMigPreflightWarnings(force) {
  if (!migState.id) {
    migState.preflightWarnings = { items: [] };
    renderMigPreflightWarnings();
    return migState.preflightWarnings;
  }
  if (!force && migState.preflightWarnings) {
    renderMigPreflightWarnings();
    return migState.preflightWarnings;
  }
  if (migState.preflightWarningsLoading) {
    return migState.preflightWarnings;
  }

  migState.preflightWarningsLoading = true;
  renderMigPreflightWarnings();
  try {
    const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/preflight');
    const result = await res.json();
    if (!res.ok) throw new Error(result.error || 'Fehler');
    migState.preflightWarnings = result;
    return result;
  } catch (err) {
    migState.preflightWarnings = { items: [], error: err instanceof Error ? err.message : String(err) };
    const el = document.getElementById('mig-preflight-summary');
    if (el) {
      el.innerHTML = '<div class="alert alert-secondary small mb-0">Vorab-Check konnte nicht geladen werden: ' + esc(migState.preflightWarnings.error) + '</div>';
    }
    return migState.preflightWarnings;
  } finally {
    migState.preflightWarningsLoading = false;
    if (!migState.preflightWarnings?.error) {
      renderMigPreflightWarnings();
    }
  }
}


function getMigProgressSteps() {
  const progressSteps = Array.isArray(migState.lastRunResult?.steps) ? migState.lastRunResult.steps : [];
  if (progressSteps.length) {
    return progressSteps;
  }

  return getMigOrderedObjects().map((obj) => ({
    objectId: obj.id,
    salesforceObject: obj.salesforceObject,
    status: 'pending',
    recordsProcessed: 0,
    recordsSucceeded: 0,
    recordsFailed: 0
  }));
}

function renderMigRunProgress() {
  const progressEl = document.getElementById('mig-run-progress');
  const stepsEl = document.getElementById('mig-run-steps');
  const progressTitleEl = document.getElementById('mig-run-status-title');
  const spinnerEl = document.getElementById('mig-run-status-spinner');
  if (!progressEl || !stepsEl) return;

  const steps = getMigProgressSteps();
  const totalSteps = Math.max(1, steps.length || 1);
  const totalProcessed = steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsProcessed || 0) || 0), 0);
  const totalSucceeded = steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsSucceeded || 0) || 0), 0);
  const totalFailed = steps.reduce((sum, step) => sum + Math.max(0, Number(step.recordsFailed || 0) || 0), 0);
  const completedUnits = steps.reduce((sum, step) => {
    if (step.status === 'done' || step.status === 'error') {
      return sum + 1;
    }
    if (step.status === 'running') {
      const totalRecords = Math.max(0, Number(step.recordsProcessed || 0) || 0);
      const completedRecords = Math.max(0, Number(step.recordsSucceeded || 0) + Number(step.recordsFailed || 0));
      return sum + (totalRecords > 0 ? Math.min(1, completedRecords / totalRecords) : 0);
    }
    return sum;
  }, 0);
  const percent = Math.max(0, Math.min(100, Math.round((completedUnits / totalSteps) * 100)));
  const badgeClassByStatus = { pending: 'secondary', running: 'warning', done: 'success', error: 'danger' };
  const completedSteps = steps.filter((step) => step.status === 'done' || step.status === 'error').length;
  const summaryText = steps.length
    ? steps.map((step) => {
        const totalRecords = Math.max(0, Number(step.recordsProcessed || 0) || 0);
        const completedRecords = Math.max(0, Number(step.recordsSucceeded || 0) + Number(step.recordsFailed || 0));
        return String(step.salesforceObject || 'Objekt') + ': ' + String(step.status || 'pending') + (totalRecords ? (' ' + completedRecords + '/' + totalRecords) : '');
      }).join(' • ')
    : 'Migration läuft...';

  progressEl.classList.remove('d-none');
  if (spinnerEl) {
    spinnerEl.classList.toggle('d-none', migState.status !== 'running');
  }
  if (progressTitleEl) {
    progressTitleEl.textContent = migState.status === 'running' ? 'Migration läuft...' : 'Ausführungsergebnis';
  }
  stepsEl.innerHTML =
    '<div class="mb-3">' +
      '<div class="progress" role="progressbar" aria-valuenow="' + percent + '" aria-valuemin="0" aria-valuemax="100">' +
        '<div class="progress-bar progress-bar-striped' + (migState.status === 'running' ? ' progress-bar-animated' : '') + '" style="width:' + percent + '%">' + percent + '%</div>' +
      '</div>' +
      '<div class="d-flex flex-wrap gap-2 mt-2 small">' +
        '<span class="badge text-bg-light border">' + completedSteps + '/' + steps.length + ' Objekte</span>' +
        '<span class="badge text-bg-light border">' + totalSucceeded + ' OK</span>' +
        '<span class="badge text-bg-light border">' + totalFailed + ' Fehler</span>' +
        (totalProcessed ? ('<span class="badge text-bg-light border">' + totalProcessed + ' Datensätze</span>') : '') +
      '</div>' +
      '<div class="small text-secondary mt-2">' + esc(summaryText) + '</div>' +
    '</div>' +
    '<div class="vstack gap-2">' +
      steps.map((step) => {
        const totalRecords = Math.max(0, Number(step.recordsProcessed || 0) || 0);
        const completedRecords = Math.max(0, Number(step.recordsSucceeded || 0) + Number(step.recordsFailed || 0));
        const itemPercent = totalRecords > 0
          ? Math.max(0, Math.min(100, Math.round((completedRecords / totalRecords) * 100)))
          : (step.status === 'done' || step.status === 'error' ? 100 : 0);
        return '<div class="border rounded px-2 py-2">' +
          '<div class="d-flex flex-wrap align-items-center gap-2">' +
            '<strong class="me-auto">' + esc(String(step.salesforceObject || 'Objekt')) + '</strong>' +
            '<span class="badge bg-' + esc(badgeClassByStatus[step.status] || 'secondary') + '">' + esc(String(step.status || 'pending')) + '</span>' +
            '<span class="small text-secondary">' + completedRecords + (totalRecords ? ('/' + totalRecords) : '') + '</span>' +
            '<span class="small text-success">OK ' + Math.max(0, Number(step.recordsSucceeded || 0)) + '</span>' +
            '<span class="small text-danger">Fehler ' + Math.max(0, Number(step.recordsFailed || 0)) + '</span>' +
          '</div>' +
          '<div class="progress mt-2" style="height:6px" role="progressbar" aria-valuenow="' + itemPercent + '" aria-valuemin="0" aria-valuemax="100">' +
            '<div class="progress-bar bg-' + esc(step.status === 'error' ? 'danger' : (step.status === 'done' ? 'success' : 'warning')) + '" style="width:' + itemPercent + '%"></div>' +
          '</div>' +
          (step.errorMessage ? '<div class="small text-danger mt-1">' + esc(String(step.errorMessage || '')) + '</div>' : '') +
        '</div>';
      }).join('') +
    '</div>';
}

function renderMigReview() {
  const el = document.getElementById('mig-review-summary');
  if (!el) return;
  resetMigTransientUi();
  const ordered = [...migState.executionPlan].sort((a, b) => a.order - b.order);
  const runHistory = Array.isArray(migState.runHistory) ? migState.runHistory : [];
  const renderRunHistory = () => {
    if (!runHistory.length) {
      return '<div class="small text-secondary mt-3">Noch keine Laufhistorie vorhanden.</div>';
    }
    return '<div class="mt-3"><strong>Laufhistorie:</strong><div class="list-group list-group-flush mt-2">' +
      runHistory.map((run, index) => {
        const steps = Array.isArray(run && run.steps) ? run.steps : [];
        const hasErrors = steps.some((step) => step && step.status === 'error');
        const statusLabel = hasErrors ? 'Fehler' : 'Erfolgreich';
        const statusClass = hasErrors ? 'danger' : 'success';
        const processed = steps.reduce((sum, step) => sum + Math.max(0, Number(step?.recordsProcessed || 0)), 0);
        return '<div class="list-group-item px-0">' +
          '<div class="d-flex flex-wrap align-items-center gap-2">' +
            '<span class="badge bg-' + statusClass + '">' + statusLabel + '</span>' +
            '<strong>Lauf ' + (index + 1) + '</strong>' +
            '<span class="small text-secondary">' + esc(formatDate(run.startedAt, 'short')) + '</span>' +
            (run.finishedAt ? '<span class="small text-secondary">bis ' + esc(formatDate(run.finishedAt, 'short')) + '</span>' : '') +
            (processed ? '<span class="small text-secondary">' + processed + ' Datensätze</span>' : '') +
          '</div>' +
          (run.reportPath ? '<div class="small mt-1"><a href="' + esc(getMigrationReportUrl(migState.id, true)) + '">Protokolldatei</a></div>' : '') +
        '</div>';
      }).join('') +
    '</div></div>';
  };
  el.innerHTML = '<div class="card soft-card"><div class="card-body"><h6>' + esc(migState.name) + '</h6>' +
    '<p class="text-secondary small">' + esc(migState.description || '') + '</p>' +
    '<strong>Ausführungsplan:</strong><ol class="mt-1">' +
    ordered.map((step) => {
      const obj = migState.objects.find((o) => o.id === step.objectId);
      if (!obj) return '';
      const fileSummary = renderMigFileSummary(obj);
      return '<li>' + esc(obj.salesforceObject) + ' — ' + esc(obj.operation) +
        ' — Modus: ' + esc(obj.processingMode === 'file' ? 'Datei direkt' : 'SQLite-Staging') +
        ' — Datei: <code>' + esc(obj.filePath || '(keine)') + '</code>' +
        ' — Felder gemappt: ' + (obj.fieldMappings || []).length +
        (fileSummary ? '<div class="small text-secondary mt-1">' + esc(fileSummary) + '</div>' : '') +
        '</li>';
    }).join('') +
    '</ol>' +
    (migState.dependencies.length ? '<strong>Abhängigkeiten:</strong><ul>' +
      migState.dependencies.map((dep) => {
        const from = migState.objects.find((o) => o.id === dep.fromObjectId);
        const to = migState.objects.find((o) => o.id === dep.toObjectId);
        return '<li>' + esc(from?.salesforceObject || '') + ' → ' + esc(to?.salesforceObject || '') + '</li>';
      }).join('') + '</ul>' : '') +
    renderRunHistory() +
    '<div id="mig-preflight-summary" class="mt-3"></div>' +
    '</div></div>';
  renderMigPreflightWarnings();
  loadMigPreflightWarnings();
  if (migState.status === 'running' || migState.activeRunVisible) {
    renderMigRunProgress();
    renderMigRunResult();
  }
  if (migState.status === 'running') {
    pollMigRunProgress();
  }
}


function renderMigFailedRecordsCards(failedSteps) {
  return failedSteps.map((step) => {
    const detailsId = 'mig-errors-' + step.failedRecordsId;
    return '<div class="card mt-3">' +
      '<div class="card-header d-flex gap-2 align-items-center">' +
      '<strong class="me-auto">Fehlerhafte Datensätze: ' + esc(step.salesforceObject) + '</strong>' +
      '<button class="btn btn-sm btn-outline-danger" data-load-failed-records="' + esc(migState.id) + '" data-object-id="' + esc(step.objectId) + '" data-failed-records-id="' + esc(step.failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Details laden</button>' +
      '</div>' +
      '<div id="' + esc(detailsId) + '" class="card-body" style="display:none;"></div>' +
    '</div>';
  }).join('');
}

function bindMigFailedRecordsDetails(btn) {
  btn.addEventListener('click', async () => {
    const migId = btn.getAttribute('data-load-failed-records');
    const objectId = btn.getAttribute('data-object-id');
    const failedRecordsId = btn.getAttribute('data-failed-records-id');
    const detailsId = btn.getAttribute('data-details-id');
    const detailsDiv = document.getElementById(detailsId);
    if (!detailsDiv) return;

    btn.disabled = true;
    btn.textContent = 'Lade…';
    detailsDiv.style.display = '';

    try {
      const failedRes = await fetch('/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(failedRecordsId));
      if (!failedRes.ok) throw new Error('Fehler beim Laden der Fehlerdetails');
      const failedData = await failedRes.json();
      const records = Array.isArray(failedData.records) ? failedData.records : [];
      const migrationObject = (migState.objects || []).find((item) => item && item.id === objectId);
      const allowStageSave = !!migrationObject && (migrationObject.stagingMode === 'sqlite' || migrationObject.processingMode === 'sqlite');

      if (!records.length) {
        detailsDiv.innerHTML = '<div class="alert alert-info">Keine fehlgeschlagenen Datensätze gefunden.</div>';
        btn.textContent = 'Details laden';
        return;
      }

      detailsDiv.innerHTML =
        '<div class="d-flex align-items-center gap-2 mb-2 flex-wrap">' +
          '<button class="btn btn-sm btn-primary" data-retry-failed-records data-mode="all" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Korrigierte Datensätze neu importieren</button>' +
          '<button class="btn btn-sm btn-outline-primary" data-retry-failed-records data-mode="partial" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Nur erfolgreiche Korrekturen übernehmen</button>' +
          (allowStageSave
            ? '<button class="btn btn-sm btn-outline-secondary" data-save-failed-corrections data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '">Korrekturen ins Staging übernehmen</button>'
            : '') +
          '<button class="btn btn-sm btn-outline-secondary" data-export-failed-csv>Restfehler als CSV exportieren</button>' +
          '<span class="small text-secondary" data-retry-status></span>' +
        '</div>' +
        '<p class="small text-secondary mb-2">Feldwerte direkt korrigieren und anschließend neu importieren.</p>' +
        '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr><th>Zeile</th><th>Fehlertyp</th><th>Fehler</th><th>Korrigierbare Feldwerte</th></tr></thead><tbody>' +
        records.map((rec, idx) => {
          const sourceObj = rec.sourceRecord || {};
          const sourceEntries = Object.entries(sourceObj);
          const previewPairs = sourceEntries.slice(0, 3)
            .map(([key, value]) => '<span class="badge text-bg-light border me-1 mb-1">' + esc(String(key)) + ': ' + esc(String(value ?? '')) + '</span>')
            .join('');
          return '<tr data-failed-row="' + idx + '" data-row-index="' + esc(String(rec.rowIndex || 0)) + '" data-error="' + esc(String(rec.error || '')) + '" data-error-type="' + esc(String(rec.errorType || 'mapping')) + '">' +
            '<td><strong>' + esc(String(rec.rowIndex)) + '</strong></td>' +
            '<td><span class="badge bg-' + (rec.errorType === 'salesforce' ? 'warning' : 'danger') + '">' + esc(String(rec.errorType || 'mapping')) + '</span></td>' +
            '<td class="text-danger small">' + esc(String(rec.error || '')) + '</td>' +
            '<td>' +
              '<div class="small text-secondary mb-1">' + sourceEntries.length + ' Felder</div>' +
              '<div class="mb-1">' + previewPairs + (sourceEntries.length > 3 ? '<span class="small text-secondary">…</span>' : '') + '</div>' +
              '<details class="border rounded p-2 bg-body-tertiary">' +
                '<summary class="small" style="cursor:pointer">Felder bearbeiten</summary>' +
                '<div class="vstack gap-1 mt-2" style="max-height: 260px; overflow:auto;">' +
                  sourceEntries.map(([key, value]) =>
                    '<div class="input-group input-group-sm">' +
                      '<span class="input-group-text" style="min-width: 180px">' + esc(String(key)) + '</span>' +
                      '<input class="form-control" data-retry-field data-field-name="' + esc(String(key)) + '" value="' + esc(String(value ?? '')) + '" />' +
                    '</div>'
                  ).join('') +
                '</div>' +
              '</details>' +
            '</td>' +
          '</tr>';
        }).join('') +
        '</tbody></table></div>';

      const retryButtons = Array.from(detailsDiv.querySelectorAll('[data-retry-failed-records]'));
      const saveCorrectionsBtn = detailsDiv.querySelector('[data-save-failed-corrections]');
      const exportCsvBtn = detailsDiv.querySelector('[data-export-failed-csv]');
      const retryStatus = detailsDiv.querySelector('[data-retry-status]');

      const collectEditedRows = () => {
        const rows = Array.from(detailsDiv.querySelectorAll('[data-failed-row]'));
        return rows.map((row) => {
          const rowIndex = Number(row.getAttribute('data-row-index') || '0');
          const sourceRecord = {};
          row.querySelectorAll('[data-retry-field]').forEach((input) => {
            const key = input.getAttribute('data-field-name') || '';
            sourceRecord[key] = input.value;
          });
          return {
            rowIndex,
            error: row.getAttribute('data-error') || '',
            errorType: row.getAttribute('data-error-type') || 'mapping',
            sourceRecord
          };
        });
      };

      const csvEscape = (value) => {
        const delimiter = ';';
        const str = String(value ?? '');
        if (str.includes('"') || str.includes('\n') || str.includes('\r') || str.includes(delimiter)) {
          return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
      };

      if (exportCsvBtn) {
        exportCsvBtn.addEventListener('click', () => {
          const editedRows = collectEditedRows();
          if (!editedRows.length) {
            alert('Keine Restfehler zum Exportieren vorhanden.');
            return;
          }
          const sourceKeys = Array.from(new Set(editedRows.flatMap((row) => Object.keys(row.sourceRecord || {}))));
          const header = ['rowIndex', 'errorType', 'error', ...sourceKeys];
          const delimiter = ';';
          const lines = [header.map(csvEscape).join(delimiter)];
          editedRows.forEach((row) => {
            const values = [row.rowIndex, row.errorType, row.error, ...sourceKeys.map((key) => row.sourceRecord[key] ?? '')];
            lines.push(values.map(csvEscape).join(delimiter));
          });
          const bom = '\uFEFF';
          const blob = new Blob([bom + lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = 'restfehler-' + objectId + '-' + failedRecordsId + '.csv';
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
        });
      }

      const runRetry = async (mode) => {
        const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
        retryButtons.forEach((button) => { button.disabled = true; });
        if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = true;
        if (retryStatus) {
          retryStatus.textContent = mode === 'partial'
            ? 'Neuimport läuft (nur erfolgreiche Korrekturen werden übernommen)...'
            : 'Neuimport läuft...';
        }
        try {
          const retryRes = await fetch(
            '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ records: payloadRecords, mode })
            }
          );
          const retryResult = await retryRes.json();
          if (!retryRes.ok) throw new Error(retryResult.error || 'Retry fehlgeschlagen');

          if (retryStatus) {
            retryStatus.textContent = 'Neuimport abgeschlossen: ' + retryResult.recordsSucceeded + ' OK, ' + retryResult.recordsFailed + ' Fehler.';
          }

          if (retryResult.failedRecordsId) {
            btn.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
            retryButtons.forEach((button) => {
              button.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
            });
            btn.click();
          }
        } catch (err) {
          if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
        } finally {
          retryButtons.forEach((button) => { button.disabled = false; });
          if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = false;
        }
      };

      if (saveCorrectionsBtn) {
        saveCorrectionsBtn.addEventListener('click', async () => {
          const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
          retryButtons.forEach((button) => { button.disabled = true; });
          saveCorrectionsBtn.disabled = true;
          if (retryStatus) {
            retryStatus.textContent = 'Korrekturen werden ins SQLite-Staging übernommen...';
          }

          try {
            const saveRes = await fetch(
              '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
              {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ records: payloadRecords, mode: 'stage' })
              }
            );
            const saveResult = await saveRes.json();
            if (!saveRes.ok) throw new Error(saveResult.error || 'Speichern ins Staging fehlgeschlagen');

            if (retryStatus) {
              retryStatus.textContent = (saveResult.updatedRows || 0) + ' Zeilen im Staging aktualisiert.';
            }

            if (migrationObject) {
              migrationObject.statusSummary = saveResult.statusSummary || migrationObject.statusSummary || {};
              await loadMigObjectPreview(migrationObject, migrationObject.previewOffset || 0, migrationObject.previewLimit || 10);
              renderMigMappingPanel();
            }
          } catch (err) {
            if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
          } finally {
            retryButtons.forEach((button) => { button.disabled = false; });
            saveCorrectionsBtn.disabled = false;
          }
        });
      }

      retryButtons.forEach((button) => {
        button.addEventListener('click', () => {
          const mode = button.getAttribute('data-mode') || 'all';
          runRetry(mode);
        });
      });

      btn.textContent = 'Details aktualisieren';
    } catch (err) {
      detailsDiv.innerHTML = '<div class="alert alert-danger">Fehler: ' + esc(err instanceof Error ? err.message : String(err)) + '</div>';
      btn.textContent = 'Details laden';
    } finally {
      btn.disabled = false;
    }
  });
}


function renderMigRunResult() {
  const resultEl = document.getElementById('mig-run-result');
  if (!resultEl) return;

  const result = migState.lastRunResult;
  const steps = Array.isArray(result && result.steps) ? result.steps : [];
  if (!result || !steps.length || migState.status === 'running') {
    resultEl.classList.add('d-none');
    resultEl.innerHTML = '';
    return;
  }

  const allOk = steps.every((step) => step.status !== 'error');
  resultEl.classList.remove('d-none');
  resultEl.innerHTML = '<div class="alert ' + (allOk ? 'alert-success' : 'alert-warning') + '">' +
    (allOk ? '✓ Migration erfolgreich abgeschlossen.' : '⚠ Migration mit Fehlern abgeschlossen.') +
    '</div>' +
    (result.reportPath ? '<div class="alert alert-info py-2 small">Protokoll erzeugt: <a href="' + esc(getMigrationReportUrl(migState.id, true)) + '">Datei öffnen</a><div class="text-secondary mt-1"><code>' + esc(result.reportPath) + '</code></div></div>' : '') +
    '<table class="table table-sm"><thead><tr><th>Objekt</th><th>Verarbeitet</th><th>OK</th><th>Fehler</th><th>Status</th></tr></thead><tbody>' +
    steps.map((step) =>
      '<tr><td>' + esc(step.salesforceObject) + '</td><td>' + (step.recordsProcessed || 0) +
      '</td><td>' + (step.recordsSucceeded || 0) + '</td><td>' + (step.recordsFailed || 0) +
      '</td><td><span class="badge bg-' + (step.status === 'done' ? 'success' : 'danger') + '">' + esc(step.status) + '</span>' +
      (step.errorMessage ? '<div class="text-danger small">' + esc(step.errorMessage) + '</div>' : '') +
      '</td></tr>'
    ).join('') + '</tbody></table>';

  const failedSteps = steps.filter((step) => step.failedRecordsId);
  if (failedSteps.length) {
    resultEl.innerHTML += renderMigFailedRecordsCards(failedSteps);
    resultEl.querySelectorAll('[data-load-failed-records]').forEach((btn) => bindMigFailedRecordsDetails(btn));
  }
}


document.getElementById('mig-wizard-prev')?.addEventListener('click', () => {
  if (migState.step <= 1) return;
  migState.step--;
  renderMigWizardSteps();
  if (migState.step === 2) renderMigFileAssignments();
  if (migState.step === 3) renderMigMappingObjectSelect();
  if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
  if (migState.step === 5) renderMigOrderList();
  if (migState.step === 6) renderMigMissingFields();
  if (migState.step === 7) renderMigReview();
});

document.getElementById('mig-wizard-next')?.addEventListener('click', async () => {
  if (migState.step === migState.totalSteps) {
    // Execute migration
    const progressEl = document.getElementById('mig-run-progress');
    const resultEl = document.getElementById('mig-run-result');
    const nextBtn = document.getElementById('mig-wizard-next');
    const prevBtn = document.getElementById('mig-wizard-prev');
    const createFieldsResultEl = document.getElementById('mig-create-fields-result');
    nextBtn.disabled = true; prevBtn.disabled = true;
    progressEl.classList.remove('d-none');
    resultEl.classList.add('d-none');
    try {
      const autoCreatedFields = await autoCreateMigMissingFields();
      if (createFieldsResultEl) {
        createFieldsResultEl.innerHTML = autoCreatedFields.length
          ? autoCreatedFields.map((item) => '<div class="alert alert-success py-1 small mt-1">' + esc(item.objectApiName + '.' + item.fieldName) + (item.action === 'exists' ? ' existiert bereits.' : ' automatisch angelegt.') + '</div>').join('')
          : '';
      }
      await migSave();
      await loadMigPreflightWarnings(true);
      migState.status = 'running';
      migState.activeRunVisible = true;
      const orderedObjects = getMigOrderedObjects();
      migState.lastRunResult = {
        startedAt: new Date().toISOString(),
        steps: orderedObjects.map((obj, index) => ({
          objectId: obj.id,
          salesforceObject: obj.salesforceObject,
          status: index === 0 ? 'running' : 'pending',
          recordsProcessed: index === 0 ? Math.max(0, Number(obj.fileRecordCount || 0) || 0) : 0,
          recordsSucceeded: 0,
          recordsFailed: 0
        }))
      };
      renderMigRunProgress();
      pollMigRunProgress();
      const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/run', { method: 'POST' });
      const result = await res.json();
      if (!res.ok) {
        throw new Error(result.error || 'Migration konnte nicht gestartet werden');
      }
      if (result && result.accepted) {
        migState.status = 'running';
        migState.activeRunVisible = true;
        if (result.lastRunResult) {
          migState.lastRunResult = result.lastRunResult;
        }
        renderMigRunProgress();
        pollMigRunProgress();
        resultEl.classList.remove('d-none');
        resultEl.innerHTML = '<div class="alert alert-info">Migration wurde gestartet. Fortschritt wird automatisch aktualisiert.</div>';
        return;
      }
      migState.lastRunResult = result;
      migState.status = result.steps.every((s) => s.status !== 'error') ? 'done' : 'error';
      migState.activeRunVisible = true;
      stopMigRunProgressPolling();
      renderMigRunProgress();
      resultEl.classList.remove('d-none');
      const allOk = result.steps.every((s) => s.status !== 'error');
      resultEl.innerHTML = '<div class="alert ' + (allOk ? 'alert-success' : 'alert-warning') + '">' +
        (allOk ? '✓ Migration erfolgreich abgeschlossen.' : '⚠ Migration mit Fehlern abgeschlossen.') +
        '</div>' +
        (result.reportPath ? '<div class="alert alert-info py-2 small">Protokoll erzeugt: <a href="' + esc(getMigrationReportUrl(migState.id, true)) + '">Datei öffnen</a><div class="text-secondary mt-1"><code>' + esc(result.reportPath) + '</code></div></div>' : '') +
        '<table class="table table-sm"><thead><tr><th>Objekt</th><th>Verarbeitet</th><th>OK</th><th>Fehler</th><th>Status</th></tr></thead><tbody>' +
        (result.steps || []).map((s) =>
          '<tr><td>' + esc(s.salesforceObject) + '</td><td>' + (s.recordsProcessed || 0) +
          '</td><td>' + (s.recordsSucceeded || 0) + '</td><td>' + (s.recordsFailed || 0) +
          '</td><td><span class="badge bg-' + (s.status === 'done' ? 'success' : 'danger') + '">' + esc(s.status) + '</span>' +
          (s.errorMessage ? '<div class="text-danger small">' + esc(s.errorMessage) + '</div>' : '') +
          '</td></tr>'
        ).join('') + '</tbody></table>';
      const failedSteps = (result.steps || []).filter((s) => s.failedRecordsId);
      if (failedSteps.length) {
        resultEl.innerHTML += failedSteps.map((s) => {
          const detailsId = 'mig-errors-' + s.failedRecordsId;
          return '<div class="card mt-3">' +
            '<div class="card-header d-flex gap-2 align-items-center">' +
            '<strong class="me-auto">Fehlerhafte Datensätze: ' + esc(s.salesforceObject) + '</strong>' +
            '<button class="btn btn-sm btn-outline-danger" data-load-failed-records="' + esc(migState.id) + '" data-object-id="' + esc(s.objectId) + '" data-failed-records-id="' + esc(s.failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Details laden</button>' +
            '</div>' +
            '<div id="' + esc(detailsId) + '" class="card-body" style="display:none;"></div>' +
          '</div>';
        }).join('');

        const bindLoadFailedDetails = (btn) => {
          btn.addEventListener('click', async () => {
            const migId = btn.getAttribute('data-load-failed-records');
            const objectId = btn.getAttribute('data-object-id');
            const failedRecordsId = btn.getAttribute('data-failed-records-id');
            const detailsId = btn.getAttribute('data-details-id');
            const detailsDiv = document.getElementById(detailsId);
            if (!detailsDiv) return;

            btn.disabled = true;
            btn.textContent = 'Lade…';
            detailsDiv.style.display = '';

            try {
              const failedRes = await fetch('/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(failedRecordsId));
              if (!failedRes.ok) throw new Error('Fehler beim Laden der Fehlerdetails');
              const failedData = await failedRes.json();
              const records = Array.isArray(failedData.records) ? failedData.records : [];
              const migrationObject = (migState.objects || []).find((item) => item && item.id === objectId);
              const allowStageSave = !!migrationObject && (migrationObject.stagingMode === 'sqlite' || migrationObject.processingMode === 'sqlite');

              if (!records.length) {
                detailsDiv.innerHTML = '<div class="alert alert-info">Keine fehlgeschlagenen Datensätze gefunden.</div>';
                btn.textContent = 'Details laden';
                return;
              }

              detailsDiv.innerHTML =
                '<div class="d-flex align-items-center gap-2 mb-2 flex-wrap">' +
                  '<button class="btn btn-sm btn-primary" data-retry-failed-records data-mode="all" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Korrigierte Datensätze neu importieren</button>' +
                  '<button class="btn btn-sm btn-outline-primary" data-retry-failed-records data-mode="partial" data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Nur erfolgreiche Korrekturen übernehmen</button>' +
                  (allowStageSave
                    ? '<button class="btn btn-sm btn-outline-secondary" data-save-failed-corrections data-mig-id="' + esc(migId) + '" data-object-id="' + esc(objectId) + '" data-failed-records-id="' + esc(failedRecordsId) + '">Korrekturen ins Staging übernehmen</button>'
                    : '') +
                  '<button class="btn btn-sm btn-outline-secondary" data-export-failed-csv>Restfehler als CSV exportieren</button>' +
                  '<span class="small text-secondary" data-retry-status></span>' +
                '</div>' +
                '<p class="small text-secondary mb-2">Feldwerte direkt korrigieren und anschließend neu importieren.</p>' +
                '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr><th>Zeile</th><th>Fehlertyp</th><th>Fehler</th><th>Korrigierbare Feldwerte</th></tr></thead><tbody>' +
                records.map((rec, idx) => {
                  const sourceObj = rec.sourceRecord || {};
                  const sourceEntries = Object.entries(sourceObj);
                  const previewPairs = sourceEntries.slice(0, 3)
                    .map(([key, value]) => '<span class="badge text-bg-light border me-1 mb-1">' + esc(String(key)) + ': ' + esc(String(value ?? '')) + '</span>')
                    .join('');
                  return '<tr data-failed-row="' + idx + '" data-row-index="' + esc(String(rec.rowIndex || 0)) + '" data-error="' + esc(String(rec.error || '')) + '" data-error-type="' + esc(String(rec.errorType || 'mapping')) + '">' +
                    '<td><strong>' + esc(String(rec.rowIndex)) + '</strong></td>' +
                    '<td><span class="badge bg-' + (rec.errorType === 'salesforce' ? 'warning' : 'danger') + '">' + esc(String(rec.errorType || 'mapping')) + '</span></td>' +
                    '<td class="text-danger small">' + esc(String(rec.error || '')) + '</td>' +
                    '<td>' +
                      '<div class="small text-secondary mb-1">' + sourceEntries.length + ' Felder</div>' +
                      '<div class="mb-1">' + previewPairs + (sourceEntries.length > 3 ? '<span class="small text-secondary">…</span>' : '') + '</div>' +
                      '<details class="border rounded p-2 bg-body-tertiary">' +
                        '<summary class="small" style="cursor:pointer">Felder bearbeiten</summary>' +
                        '<div class="vstack gap-1 mt-2" style="max-height: 260px; overflow:auto;">' +
                          sourceEntries.map(([key, value]) =>
                            '<div class="input-group input-group-sm">' +
                              '<span class="input-group-text" style="min-width: 180px">' + esc(String(key)) + '</span>' +
                              '<input class="form-control" data-retry-field data-field-name="' + esc(String(key)) + '" value="' + esc(String(value ?? '')) + '" />' +
                            '</div>'
                          ).join('') +
                        '</div>' +
                      '</details>' +
                    '</td>' +
                  '</tr>';
                }).join('') +
                '</tbody></table></div>';

              const retryButtons = Array.from(detailsDiv.querySelectorAll('[data-retry-failed-records]'));
              const saveCorrectionsBtn = detailsDiv.querySelector('[data-save-failed-corrections]');
              const exportCsvBtn = detailsDiv.querySelector('[data-export-failed-csv]');
              const retryStatus = detailsDiv.querySelector('[data-retry-status]');

              const collectEditedRows = () => {
                const rows = Array.from(detailsDiv.querySelectorAll('[data-failed-row]'));
                return rows.map((row) => {
                  const rowIndex = Number(row.getAttribute('data-row-index') || '0');
                  const sourceRecord = {};
                  row.querySelectorAll('[data-retry-field]').forEach((input) => {
                    const key = input.getAttribute('data-field-name') || '';
                    sourceRecord[key] = input.value;
                  });
                  return {
                    rowIndex,
                    error: row.getAttribute('data-error') || '',
                    errorType: row.getAttribute('data-error-type') || 'mapping',
                    sourceRecord
                  };
                });
              };

              const csvEscape = (value) => {
                const delimiter = ';';
                const str = String(value ?? '');
                if (str.includes('"') || str.includes('\n') || str.includes('\r') || str.includes(delimiter)) {
                  return '"' + str.replace(/"/g, '""') + '"';
                }
                return str;
              };

              if (exportCsvBtn) {
                exportCsvBtn.addEventListener('click', () => {
                  const editedRows = collectEditedRows();
                  if (!editedRows.length) {
                    alert('Keine Restfehler zum Exportieren vorhanden.');
                    return;
                  }
                  const sourceKeys = Array.from(new Set(editedRows.flatMap((row) => Object.keys(row.sourceRecord || {}))));
                  const header = ['rowIndex', 'errorType', 'error', ...sourceKeys];
                  const delimiter = ';';
                  const lines = [header.map(csvEscape).join(delimiter)];
                  editedRows.forEach((row) => {
                    const values = [row.rowIndex, row.errorType, row.error, ...sourceKeys.map((key) => row.sourceRecord[key] ?? '')];
                    lines.push(values.map(csvEscape).join(delimiter));
                  });
                  const bom = '\uFEFF';
                  const blob = new Blob([bom + lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement('a');
                  a.href = url;
                  a.download = 'restfehler-' + objectId + '-' + failedRecordsId + '.csv';
                  document.body.appendChild(a);
                  a.click();
                  document.body.removeChild(a);
                  URL.revokeObjectURL(url);
                });
              }

              const runRetry = async (mode) => {
                const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
                retryButtons.forEach((button) => { button.disabled = true; });
                if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = true;
                if (retryStatus) {
                  retryStatus.textContent = mode === 'partial'
                    ? 'Neuimport läuft (nur erfolgreiche Korrekturen werden übernommen)...'
                    : 'Neuimport läuft...';
                }
                try {
                  const retryRes = await fetch(
                    '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
                    {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ records: payloadRecords, mode })
                    }
                  );
                  const retryResult = await retryRes.json();
                  if (!retryRes.ok) throw new Error(retryResult.error || 'Retry fehlgeschlagen');

                  if (retryStatus) {
                    retryStatus.textContent = 'Neuimport abgeschlossen: ' + retryResult.recordsSucceeded + ' OK, ' + retryResult.recordsFailed + ' Fehler.';
                  }

                  if (retryResult.failedRecordsId) {
                    btn.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
                    retryButtons.forEach((button) => {
                      button.setAttribute('data-failed-records-id', retryResult.failedRecordsId);
                    });
                    btn.click();
                  }
                } catch (err) {
                  if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
                } finally {
                  retryButtons.forEach((button) => { button.disabled = false; });
                  if (saveCorrectionsBtn) saveCorrectionsBtn.disabled = false;
                }
              };

              if (saveCorrectionsBtn) {
                saveCorrectionsBtn.addEventListener('click', async () => {
                  const payloadRecords = collectEditedRows().map((row) => ({ rowIndex: row.rowIndex, sourceRecord: row.sourceRecord }));
                  retryButtons.forEach((button) => { button.disabled = true; });
                  saveCorrectionsBtn.disabled = true;
                  if (retryStatus) {
                    retryStatus.textContent = 'Korrekturen werden ins SQLite-Staging übernommen...';
                  }

                  try {
                    const saveRes = await fetch(
                      '/api/migrations/' + encodeURIComponent(migId) + '/failed-records/' + encodeURIComponent(objectId) + '/' + encodeURIComponent(failedRecordsId) + '/retry',
                      {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ records: payloadRecords, mode: 'stage' })
                      }
                    );
                    const saveResult = await saveRes.json();
                    if (!saveRes.ok) throw new Error(saveResult.error || 'Speichern ins Staging fehlgeschlagen');

                    if (retryStatus) {
                      retryStatus.textContent = (saveResult.updatedRows || 0) + ' Zeilen im Staging aktualisiert.';
                    }

                    if (migrationObject) {
                      migrationObject.statusSummary = saveResult.statusSummary || migrationObject.statusSummary || {};
                      await loadMigObjectPreview(migrationObject, migrationObject.previewOffset || 0, migrationObject.previewLimit || 10);
                      renderMigMappingPanel();
                    }
                  } catch (err) {
                    if (retryStatus) retryStatus.textContent = 'Fehler: ' + (err instanceof Error ? err.message : String(err));
                  } finally {
                    retryButtons.forEach((button) => { button.disabled = false; });
                    saveCorrectionsBtn.disabled = false;
                  }
                });
              }

              retryButtons.forEach((button) => {
                button.addEventListener('click', () => {
                  const mode = button.getAttribute('data-mode') || 'all';
                  runRetry(mode);
                });
              });

              btn.textContent = 'Details aktualisieren';
            } catch (err) {
              detailsDiv.innerHTML = '<div class="alert alert-danger">Fehler: ' + esc(err instanceof Error ? err.message : String(err)) + '</div>';
              btn.textContent = 'Details laden';
            } finally {
              btn.disabled = false;
            }
          });
        };

        resultEl.querySelectorAll('[data-load-failed-records]').forEach((btn) => bindLoadFailedDetails(btn));
      }
      renderMigrationList();
      return;
    } catch (err) {
      migState.status = 'error';
      stopMigRunProgressPolling();
      resultEl.classList.remove('d-none');
      resultEl.innerHTML = '<div class="alert alert-danger">Fehler: ' + esc(err instanceof Error ? err.message : String(err)) + '</div>';
    } finally {
      nextBtn.disabled = false; prevBtn.disabled = false;
    }
    return;
  }

  // Validate step 1
  if (migState.step === 1) {
    const nameEl = document.getElementById('mig-name');
    if (!nameEl || !nameEl.value.trim()) { alert('Bitte Migrationsname eingeben.'); return; }
    if (!migState.objects.length) { alert('Bitte mindestens ein Salesforce-Objekt auswählen.'); return; }
  }

  await migSave();
  migState.step++;
  renderMigWizardSteps();
  if (migState.step === 2) renderMigFileAssignments();
  if (migState.step === 3) renderMigMappingObjectSelect();
  if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
  if (migState.step === 5) renderMigOrderList();
  if (migState.step === 6) renderMigMissingFields();
  if (migState.step === 7) renderMigReview();
});

document.getElementById('mig-wizard-save')?.addEventListener('click', async () => {
  await migSave();
  showToast('Migration gespeichert.');
});

document.getElementById('mig-login-environment')?.addEventListener('change', () => {
  syncMigSalesforceLoginFromForm();
  migState.sfObjects = [];
  const listEl = document.getElementById('mig-sf-objects-list');
  const searchWrap = document.getElementById('mig-sf-objects-search-wrap');
  if (listEl) {
    listEl.innerHTML = '<div class="text-secondary small">Login-Umgebung gewechselt. Bitte Salesforce-Objekte fuer diese Migration neu laden.</div>';
  }
  if (searchWrap) {
    searchWrap.classList.add('d-none');
  }
});

document.getElementById('mig-login-auth-type')?.addEventListener('change', () => {
  syncMigSalesforceLoginFromForm();
});

document.getElementById('mig-instance-source')?.addEventListener('change', () => {
  syncMigSalesforceLoginFromForm();
});

document.getElementById('mig-existing-instance')?.addEventListener('change', () => {
  syncMigSalesforceLoginFromForm();
});

document.getElementById('mig-login-url')?.addEventListener('input', () => {
  syncMigSalesforceLoginFromForm();
});

document.getElementById('mig-login-authorize')?.addEventListener('click', async () => {
  showToast('Migrationen nutzen Projektinstanzen. Ein separater Salesforce-Login ist nicht mehr erforderlich.');
});

// SF Objects loading
document.getElementById('mig-load-sf-objects')?.addEventListener('click', async () => {
  const btn = document.getElementById('mig-load-sf-objects');
  btn.disabled = true; btn.textContent = '…';
  const listEl = document.getElementById('mig-sf-objects-list');
  const searchWrap = document.getElementById('mig-sf-objects-search-wrap');
  try {
    const res = await fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''));
    if (!res.ok) throw new Error(await res.text());
    migState.sfObjects = await res.json();
    renderMigSfObjectsList(migState.sfObjects);
    searchWrap.classList.remove('d-none');
    document.getElementById('mig-sf-objects-search').addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase();
      renderMigSfObjectsList(migState.sfObjects.filter((o) =>
        o.name.toLowerCase().includes(q) || (o.label || '').toLowerCase().includes(q)
      ));
    });
  } catch (err) {
    listEl.innerHTML = '<div class="text-danger small">Fehler: ' + esc(String(err)) + '</div>';
  } finally {
    btn.disabled = false; btn.textContent = 'SF-Objekte laden';
  }
});

function renderMigSfObjectsList(objects) {
  const listEl = document.getElementById('mig-sf-objects-list');
  if (!listEl) return;
  if (!objects.length) { listEl.innerHTML = '<div class="text-secondary small">Keine Objekte gefunden.</div>'; return; }
  listEl.innerHTML = objects.map((obj) => {
    const alreadySelected = migState.objects.some((o) => o.salesforceObject === obj.name);
    return '<button type="button" class="btn btn-sm ' + (alreadySelected ? 'btn-success disabled' : 'btn-outline-secondary') + ' me-1 mb-1" data-sf-obj="' + esc(obj.name) + '" data-sf-label="' + esc(obj.label || obj.name) + '">' +
      esc(obj.label || obj.name) + ' <span class="text-secondary small">(' + esc(obj.name) + ')</span></button>';
  }).join('');
  listEl.querySelectorAll('[data-sf-obj]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const name = btn.getAttribute('data-sf-obj');
      const label = btn.getAttribute('data-sf-label');
      if (migState.objects.some((o) => o.salesforceObject === name)) return;
      migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: label, processingMode: 'sqlite', filePath: '', fileSheetName: '', availableSheetNames: [], fileColumns: [], fieldMappings: [], operation: 'insert' });
      renderMigSelectedObjects();
      consumePendingMigrationImportIfPossible().catch((error) => {
        alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
      });
      btn.className = 'btn btn-sm btn-success disabled me-1 mb-1';
    });
  });
}

document.getElementById('mig-add-manual-object')?.addEventListener('click', () => {
  const input = document.getElementById('mig-manual-object');
  const name = input ? input.value.trim() : '';
  if (!name) return;
  if (migState.objects.some((o) => o.salesforceObject === name)) { alert('Objekt bereits hinzugefügt.'); return; }
  migState.objects.push({ id: migUuidV4(), salesforceObject: name, salesforceObjectLabel: name, processingMode: 'sqlite', filePath: '', fileSheetName: '', availableSheetNames: [], fileColumns: [], fieldMappings: [], operation: 'insert' });
  renderMigSelectedObjects();
  consumePendingMigrationImportIfPossible().catch((error) => {
    alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
  });
  if (input) input.value = '';
});

document.getElementById('mig-mapping-object-select')?.addEventListener('change', () => {
  renderMigMappingPanel();
});

document.getElementById('mig-add-dependency')?.addEventListener('click', () => {
  const form = document.getElementById('mig-dependency-form');
  if (form) form.classList.toggle('d-none');
  renderMigDepSelects();
});

document.getElementById('mig-save-dependency')?.addEventListener('click', () => {
  const from = document.getElementById('mig-dep-from')?.value;
  const to = document.getElementById('mig-dep-to')?.value;
  const fromField = document.getElementById('mig-dep-from-field')?.value.trim();
  const toField = document.getElementById('mig-dep-to-field')?.value.trim();
  if (!from || !to || !fromField || !toField) { alert('Bitte alle Felder ausfüllen.'); return; }
  migState.dependencies.push({ fromObjectId: from, toObjectId: to, fromField, toField });
  renderMigDependencies();
  document.getElementById('mig-dependency-form').classList.add('d-none');
});

document.getElementById('mig-cancel-dependency')?.addEventListener('click', () => {
  document.getElementById('mig-dependency-form').classList.add('d-none');
});

function showToast(message) {
  const existing = document.getElementById('mig-toast');
  if (existing) existing.remove();
  const toast = document.createElement('div');
  toast.id = 'mig-toast';
  toast.className = 'position-fixed bottom-0 end-0 m-3 alert alert-success shadow';
  toast.style.zIndex = '9999';
  toast.textContent = message;
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 2500);
}


