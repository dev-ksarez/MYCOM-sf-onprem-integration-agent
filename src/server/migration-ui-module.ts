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
          const hasLastRunResult = (mig) => {
            const steps = Array.isArray(mig && mig.lastRunResult && mig.lastRunResult.steps) ? mig.lastRunResult.steps : [];
            return steps.length > 0 || !!String(mig && mig.lastRunResult && mig.lastRunResult.reportPath || '').trim();
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
            (hasLastRunResult(mig)
              ? '<button class="btn btn-outline-secondary" data-mig-last-run="' + esc(mig.id) + '">Letzter Lauf</button>'
              : '') +
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

          body.querySelectorAll('[data-mig-last-run]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-last-run');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig, { startStep: 7, showRunSummary: true });
            });
          });

          body.querySelectorAll('[data-mig-run]').forEach((btn) => {
            btn.addEventListener('click', async () => {
              const id = btn.getAttribute('data-mig-run');
              const res = await fetch('/api/migrations/' + encodeURIComponent(id));
              const mig = await res.json();
              openMigWizard(mig, { startStep: 7, showRunSummary: true });
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

export function renderMigrationPlanningModule(): string {
  return `
      function renderMigDependencies() {
        const container = document.getElementById('mig-dependencies-list');
        if (!container) return;
        if (!migState.dependencies.length) {
          container.innerHTML = '<div class="text-secondary small">Keine Abhängigkeiten definiert.</div>';
          return;
        }
        container.innerHTML = migState.dependencies.map((dep, i) => {
          const fromObj = migState.objects.find((o) => o.id === dep.fromObjectId);
          const toObj = migState.objects.find((o) => o.id === dep.toObjectId);
          return '<div class="d-flex align-items-center gap-2 mb-1 p-2 border rounded">' +
            '<strong>' + esc(fromObj?.salesforceObject || dep.fromObjectId) + '</strong>' +
            ' → ' +
            '<strong>' + esc(toObj?.salesforceObject || dep.toObjectId) + '</strong>' +
            ' <span class="text-secondary small">(' + esc(dep.fromField) + ' ← ' + esc(dep.toField) + ')</span>' +
            '<button class="btn btn-sm btn-outline-danger ms-auto" data-remove-dep="' + i + '">✕</button>' +
            '</div>';
        }).join('');
        container.querySelectorAll('[data-remove-dep]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const idx = Number(btn.getAttribute('data-remove-dep'));
            migState.dependencies.splice(idx, 1);
            renderMigDependencies();
          });
        });
      }

      function renderMigOrderList() {
        const list = document.getElementById('mig-order-list');
        if (!list) return;
        if (!migState.executionPlan.length && migState.objects.length) {
          migState.executionPlan = migState.objects.map((obj, idx) => ({ order: idx + 1, objectId: obj.id }));
        }
        const ordered = [...migState.executionPlan].sort((a, b) => a.order - b.order);
        list.innerHTML = ordered.map((step, i) => {
          const obj = migState.objects.find((o) => o.id === step.objectId);
          return '<li class="list-group-item d-flex align-items-center gap-2">' +
            '<span class="badge bg-secondary">' + (i + 1) + '</span>' +
            '<span class="flex-grow-1">' + esc(obj?.salesforceObject || step.objectId) + '</span>' +
            '<div class="btn-group btn-group-sm">' +
            '<button class="btn btn-outline-secondary" data-order-up="' + i + '" ' + (i === 0 ? 'disabled' : '') + '>↑</button>' +
            '<button class="btn btn-outline-secondary" data-order-down="' + i + '" ' + (i === ordered.length - 1 ? 'disabled' : '') + '>↓</button>' +
            '</div></li>';
        }).join('');
        list.querySelectorAll('[data-order-up]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const i = Number(btn.getAttribute('data-order-up'));
            if (i <= 0) return;
            [migState.executionPlan[i - 1], migState.executionPlan[i]] = [migState.executionPlan[i], migState.executionPlan[i - 1]];
            migState.executionPlan.forEach((s, idx) => { s.order = idx + 1; });
            renderMigOrderList();
          });
        });
        list.querySelectorAll('[data-order-down]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const i = Number(btn.getAttribute('data-order-down'));
            if (i >= migState.executionPlan.length - 1) return;
            [migState.executionPlan[i], migState.executionPlan[i + 1]] = [migState.executionPlan[i + 1], migState.executionPlan[i]];
            migState.executionPlan.forEach((s, idx) => { s.order = idx + 1; });
            renderMigOrderList();
          });
        });
      }

      function renderMigDepSelects() {
        ['mig-dep-from', 'mig-dep-to'].forEach((id) => {
          const sel = document.getElementById(id);
          if (!sel) return;
          sel.innerHTML = migState.objects.map((obj) =>
            '<option value="' + esc(obj.id) + '">' + esc(obj.salesforceObject) + '</option>'
          ).join('');
        });
      }
`;
}

export function renderMigrationPreflightModule(): string {
  return `
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
`;
}

export function renderMigrationProgressModule(): string {
  return `
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
`;
}

export function renderMigrationFailedRecordsModule(): string {
  return `
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
      }
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
          resultEl.innerHTML += renderMigFailedRecordsCards(failedSteps);
          resultEl.querySelectorAll('[data-load-failed-records]').forEach((btn) => bindMigFailedRecordsDetails(btn));
        }
      }
`;
}