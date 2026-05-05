export function renderMigrationUiModule(): string {
  return `
      async function startMigrationImportFromFiles(files) {
        const importFiles = Array.isArray(files) ? files.filter(Boolean) : [];
        if (!importFiles.length) {
          return;
        }

        const unsupportedFile = importFiles.find((file) => !isSupportedMigrationImportFile(file));
        if (unsupportedFile) {
          throw new Error('Unterstuetzte Dateitypen sind CSV, TXT, JSON und Excel.');
        }

        const importEntries = (await Promise.all(importFiles.map(async (file) => {
          const analysis = await requestJson('/api/migrations/analyze-import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              fileName: file.name,
              contentBase64: await fileToBase64(file)
            })
          });
          const sheetAnalyses = Array.isArray(analysis && analysis.sheets) && analysis.sheets.length
            ? analysis.sheets
            : [analysis];

          return sheetAnalyses.map((sheetAnalysis) => {
            const suggestions = Array.isArray(sheetAnalysis && sheetAnalysis.suggestions) ? sheetAnalysis.suggestions : [];
            const topSuggestion = suggestions[0] || null;
            const hasStrongSuggestion = !!topSuggestion && Number(topSuggestion.score || 0) >= 70;
            return {
              id: migUuidV4(),
              file,
              fileName: file.name,
              sourceFileName: file.name,
              sheetName: sheetAnalysis && sheetAnalysis.sheetName ? sheetAnalysis.sheetName : '',
              includeInMigration: true,
              analysis: {
                format: analysis && analysis.format ? analysis.format : '',
                charset: analysis && analysis.charset ? analysis.charset : 'utf8',
                delimiter: analysis && analysis.delimiter ? analysis.delimiter : ';',
                headers: Array.isArray(sheetAnalysis && sheetAnalysis.headers) ? sheetAnalysis.headers.slice() : [],
                recordCount: sheetAnalysis && typeof sheetAnalysis.recordCount === 'number' ? sheetAnalysis.recordCount : 0,
                sheetName: sheetAnalysis && sheetAnalysis.sheetName ? sheetAnalysis.sheetName : ''
              },
              suggestions,
              recommendedObjectApiName: hasStrongSuggestion ? topSuggestion.objectApiName : '',
              recommendedObjectLabel: hasStrongSuggestion ? (topSuggestion.label || topSuggestion.objectApiName) : ''
            };
          });
        }))).flat();

        const recommendationCounts = importEntries.reduce((acc, entry) => {
          const key = String(entry.recommendedObjectApiName || '');
          if (!key) {
            return acc;
          }
          acc[key] = (acc[key] || 0) + 1;
          return acc;
        }, {});

        const initialObjects = importEntries
          .filter((entry) => entry.recommendedObjectApiName && recommendationCounts[entry.recommendedObjectApiName] === 1)
          .map((entry) => ({
            id: migUuidV4(),
            salesforceObject: entry.recommendedObjectApiName,
            salesforceObjectLabel: entry.recommendedObjectLabel || entry.recommendedObjectApiName,
            processingMode: 'sqlite',
            filePath: '',
            fileSheetName: entry.sheetName || '',
            availableSheetNames: entry.sheetName ? [entry.sheetName] : [],
            fileColumns: [],
            fieldMappings: [],
            operation: 'insert'
          }));

        const primaryFile = importEntries[0];
        const baseName = importEntries.length === 1
          ? (getMigImportDisplayName(primaryFile.fileName) || 'Dateiimport')
          : (importEntries.length + ' Dateien');
        openMigWizard(null, {
          name: 'Import ' + baseName,
          description: importEntries.length === 1
            ? ('Erstellt aus Importquelle ' + getMigPendingImportLabel(primaryFile))
            : ('Erstellt aus ' + importEntries.length + ' Importquellen'),
          pendingImports: importEntries,
          pendingImportSuggestions: importEntries.length === 1 ? importEntries[0].suggestions : [],
          pendingImportAnalysis: importEntries.length === 1 ? importEntries[0].analysis : null
        });
        if (initialObjects.length) {
          migState.objects = initialObjects;
          renderMigSelectedObjects();
        }
        await migSave();
        showToast(importEntries.length === 1
          ? 'Migrationsentwurf angelegt. Waehle jetzt Objekt und Mappen-Zuordnung aus.'
          : 'Migrationsentwurf fuer ' + importEntries.length + ' Importquellen angelegt.');
      }

      function setupMigrationDropzone() {
        const dropzone = document.getElementById('migration-dropzone');
        const input = document.getElementById('migration-dropzone-input');
        const pickButton = document.getElementById('migration-dropzone-pick');
        if (!dropzone || !input || dropzone.dataset.bound === '1') {
          return;
        }

        dropzone.dataset.bound = '1';

        const handleFiles = async (files) => {
          try {
            await startMigrationImportFromFiles(files);
          } catch (error) {
            alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
          }
        };

        pickButton?.addEventListener('click', () => {
          input.value = '';
          input.click();
        });

        input.addEventListener('change', async () => {
          const files = input.files ? Array.from(input.files) : [];
          if (!files.length) {
            return;
          }

          await handleFiles(files);
          input.value = '';
        });

        ['dragenter', 'dragover'].forEach((eventName) => {
          dropzone.addEventListener(eventName, (event) => {
            event.preventDefault();
            dropzone.classList.add('is-active');
          });
        });

        ['dragleave', 'dragend'].forEach((eventName) => {
          dropzone.addEventListener(eventName, () => {
            dropzone.classList.remove('is-active');
          });
        });

        dropzone.addEventListener('drop', async (event) => {
          event.preventDefault();
          dropzone.classList.remove('is-active');
          const files = event.dataTransfer && event.dataTransfer.files
            ? Array.from(event.dataTransfer.files).filter((file) => isSupportedMigrationImportFile(file))
            : [];
          if (!files.length) {
            return;
          }

          await handleFiles(files);
        });
      }

      async function renderMigrationList() {
        const body = document.getElementById('migration-list-body');
        if (!body) return;
        try {
          const res = await fetch('/api/migrations');
          const data = await res.json();
          const items = data.items || [];
          if (!items.length) {
            body.innerHTML = '<tr><td colspan="5" class="text-secondary">Keine Migrationen vorhanden.</td></tr>';
            return;
          }
          const statusBadge = (s) => {
            const map = { draft: 'secondary', ready: 'info', running: 'warning', done: 'success', error: 'danger' };
            return '<span class="badge bg-' + (map[s] || 'secondary') + '">' + esc(s) + '</span>';
          };
          body.innerHTML = items.map((mig) =>
            '<tr>' +
            '<td>' + esc(mig.name) + '</td>' +
            '<td>' + statusBadge(mig.status) + '</td>' +
            '<td>' + (mig.objects ? mig.objects.length : 0) + ' Objekte</td>' +
            '<td>' +
            (mig.lastRunAt ? formatDate(mig.lastRunAt, 'short') : '-') +
            (mig.lastRunAt ? '<div><a href="' + esc(getMigrationReportUrl(mig.id, true)) + '">Protokolldatei</a></div>' : '') +
            '</td>' +
            '<td>' +
            '<div class="btn-group btn-group-sm">' +
            '<button class="btn btn-outline-primary" data-mig-edit="' + esc(mig.id) + '">Bearbeiten</button>' +
            '<button class="btn btn-outline-success" data-mig-run="' + esc(mig.id) + '" ' + (mig.status === 'running' ? 'disabled' : '') + '>▶ Starten</button>' +
            '<button class="btn btn-outline-danger" data-mig-delete="' + esc(mig.id) + '">✕</button>' +
            '</div></td></tr>'
          ).join('');

          body.querySelectorAll('[data-mig-edit]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-edit');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig);
            });
          });

          body.querySelectorAll('[data-mig-delete]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              if (!confirm('Migration wirklich loeschen?')) return;
              await fetch('/api/migrations/' + encodeURIComponent(btn.getAttribute('data-mig-delete')), { method: 'DELETE' });
              renderMigrationList();
            });
          });

          body.querySelectorAll('[data-mig-run]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-run');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig);
              migState.step = migState.totalSteps;
              renderMigWizardSteps();
              renderMigReview();
            });
          });
        } catch (err) {
          body.innerHTML = '<tr><td colspan="5" class="text-danger">Fehler: ' + esc(String(err)) + '</td></tr>';
        }
      }

      function initMigrationTabUi() {
        const migrationTabButton = document.querySelector('[data-bs-target="#tab-migration"]');
        migrationTabButton?.addEventListener('click', () => {
          renderMigrationList();
        });
        migrationTabButton?.addEventListener('shown.bs.tab', () => {
          renderMigrationList();
        });
        if (migrationTabButton?.classList.contains('active')) {
          renderMigrationList();
        }

        document.getElementById('new-migration')?.addEventListener('click', () => {
          openMigWizard(null);
        });

        setupMigrationDropzone();
      }

      initMigrationTabUi();
`;
}

export function renderMigrationRunResultModule(): string {
  return `
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
          resultEl.innerHTML += failedSteps.map((step) => {
            const detailsId = 'mig-errors-' + step.failedRecordsId;
            return '<div class="card mt-3">' +
              '<div class="card-header d-flex gap-2 align-items-center">' +
              '<strong class="me-auto">Fehlerhafte Datensätze: ' + esc(step.salesforceObject) + '</strong>' +
              '<button class="btn btn-sm btn-outline-danger" data-load-failed-records="' + esc(migState.id) + '" data-object-id="' + esc(step.objectId) + '" data-failed-records-id="' + esc(step.failedRecordsId) + '" data-details-id="' + esc(detailsId) + '">Details laden</button>' +
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
                  if (str.includes('"') || str.includes('\\n') || str.includes('\\r') || str.includes(delimiter)) {
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
                    const bom = '\\uFEFF';
                    const blob = new Blob([bom + lines.join('\\r\\n')], { type: 'text/csv;charset=utf-8;' });
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
      }
`;
}