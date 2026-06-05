// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-migration-ui
// Source lines: 16251–16532
// ──────────────────────────────────────────────────────────────────────

function getMigrationInstanceStatusMeta(status) {
  const normalized = String(status || 'never');
  if (normalized === 'connected') {
    return { badge: 'success', label: 'Verbunden' };
  }
  if (normalized === 'error') {
    return { badge: 'danger', label: 'Fehler' };
  }
  return { badge: 'secondary', label: 'Noch nicht verbunden' };
}

async function renderMigrationInstances() {
  return;
}

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
    body.innerHTML = items.map((mig) => {
      const targetInstance = (state.instances || []).find((i) => String(i.id || '') === String(mig.instanceId || ''));
      const targetInstanceLabel = targetInstance ? (targetInstance.name || targetInstance.id) : (mig.instanceId || '-');
      const totalRecords = Array.isArray(mig.objects) ? mig.objects.reduce((acc, o) => acc + (Number(o.fileRecordCount || o.filteredRecordCount || 0) || 0), 0) : 0;
      const sourceLabel = totalRecords ? (String(totalRecords) + ' Datensätze') : '-';
      const targetObjects = Array.isArray(mig.objects) ? mig.objects.map((o) => o.salesforceObject || o.salesforceObjectLabel || o.id).filter(Boolean).slice(0,3).join(', ') : '-';

      return (
        '<tr>' +
        '<td>' + esc(mig.name) + '</td>' +
        '<td>' + statusBadge(mig.status) + '</td>' +
        '<td>' + esc(String(targetInstanceLabel || '-')) + '</td>' +
        '<td>' + esc(sourceLabel) + '</td>' +
        '<td>' + esc(targetObjects) + '</td>' +
        '<td>' + (mig.lastRunAt ? formatDate(mig.lastRunAt, 'short') : '-') + (mig.lastRunAt ? '<div><a href="' + esc(getMigrationReportUrl(mig.id, true)) + '">Protokolldatei</a></div>' : '') + '</td>' +
        '<td>' +
          '<div class="btn-group btn-group-sm">' +
          '<button class="btn btn-outline-primary" data-mig-edit="' + esc(mig.id) + '">Bearbeiten</button>' +
          (hasLastRunResult(mig) ? '<button class="btn btn-outline-secondary" data-mig-last-run="' + esc(mig.id) + '">Letzter Lauf</button>' : '') +
          '<button class="btn btn-outline-success" data-mig-run="' + esc(mig.id) + '" ' + (mig.status === 'running' ? 'disabled' : '') + '>▶ Starten</button>' +
          '<button class="btn btn-outline-danger" data-mig-delete="' + esc(mig.id) + '">✕</button>' +
          '</div>' +
        '</td></tr>'
      );
    }).join('');

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
        renderMigrationInstances();
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
    renderMigrationInstances();
    renderMigrationList();
  });
  migrationTabButton?.addEventListener('shown.bs.tab', () => {
    renderMigrationInstances();
    renderMigrationList();
  });
  if (migrationTabButton?.classList.contains('active')) {
    renderMigrationInstances();
    renderMigrationList();
  }

  document.getElementById('new-migration')?.addEventListener('click', () => {
    openMigWizard(null);
  });

  setupMigrationDropzone();
}

initMigrationTabUi();


