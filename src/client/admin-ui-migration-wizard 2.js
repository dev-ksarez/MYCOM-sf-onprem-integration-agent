// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-migration-wizard
// Source lines: 15151–16250
// ──────────────────────────────────────────────────────────────────────

function migUuidV4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function renderMigWizardSteps() {
  document.querySelectorAll('#mig-wizard-steps [data-mig-step]').forEach((btn) => {
    const step = Number(btn.getAttribute('data-mig-step'));
    btn.classList.toggle('is-active', step === migState.step);
    btn.classList.toggle('is-complete', step < migState.step);
  });
  document.querySelectorAll('.mig-wizard-panel').forEach((panel) => {
    const step = Number(panel.getAttribute('data-mig-step-panel'));
    panel.classList.toggle('d-none', step !== migState.step);
  });
  const prev = document.getElementById('mig-wizard-prev');
  const next = document.getElementById('mig-wizard-next');
  if (prev) prev.disabled = migState.step <= 1;
  if (next) {
    if (migState.step === migState.totalSteps) {
      next.textContent = 'Migration starten ▶';
      next.className = 'btn btn-success';
    } else {
      next.textContent = 'Weiter →';
      next.className = 'btn btn-primary';
    }
  }
}

function renderMigSelectedObjects() {
  const container = document.getElementById('mig-selected-objects');
  if (!container) return;
  renderMigImportSuggestions();
  if (!migState.objects.length) {
    container.innerHTML = '<span class="text-secondary small">Noch keine Objekte ausgewählt.</span>';
    return;
  }
  container.innerHTML = migState.objects.map((obj) =>
    '<span class="badge bg-primary d-flex align-items-center gap-1" style="font-size:0.85em">' +
    esc(getMigObjectDisplayName(obj)) +
    '<button type="button" class="btn-close btn-close-white" style="font-size:0.6em" data-remove-obj="' + esc(obj.id) + '" aria-label="Entfernen"></button></span>'
  ).join('');
  container.querySelectorAll('[data-remove-obj]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const id = btn.getAttribute('data-remove-obj');
      migState.objects = migState.objects.filter((o) => o.id !== id);
      migState.dependencies = migState.dependencies.filter((d) => d.fromObjectId !== id && d.toObjectId !== id);
      migState.executionPlan = migState.executionPlan.filter((s) => s.objectId !== id);
      renderMigSelectedObjects();
    });
  });
}

async function renderMigFileAssignments() {
  const container = document.getElementById('mig-file-assignment-list');
  if (!container) return;
  renderMigPendingImportHint();
  if (!migState.objects.length) {
    container.innerHTML = '<div class="text-secondary small">Bitte zuerst Objekte in Schritt 1 auswählen.</div>';
    return;
  }

  await Promise.all(migState.objects.map((obj) => loadMigExternalIdOptions(obj)));

  container.innerHTML = migState.objects.map((obj) => {
    const safeId = esc(obj.id);
    const availableSheetNames = Array.isArray(obj.availableSheetNames) ? obj.availableSheetNames : [];
    const externalIdOptions = (Array.isArray(obj._externalIdFields) ? obj._externalIdFields : [])
      .map((field) => {
        const name = String(field?.name || '').trim();
        const label = String(field?.label || '').trim();
        const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
        return '<option value="' + esc(name) + '"' + (String(obj.externalIdField || '').trim() === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
      })
      .join('');
    const sheetOptions = availableSheetNames.length
      ? ('<option value="">Primäre Mappe</option>' +
          availableSheetNames.map((sheetName) =>
            '<option value="' + esc(sheetName) + '"' + (String(obj.fileSheetName || '') === sheetName ? ' selected' : '') + '>' + esc(sheetName) + '</option>'
          ).join(''))
      : '<option value="">Nicht erforderlich</option>';
    return '<div class="card soft-card mb-2"><div class="card-body"><div class="d-flex justify-content-between align-items-center mb-2">' +
      '<strong>' + esc(getMigObjectDisplayName(obj)) + '</strong>' +
      '<select class="form-select form-select-sm w-auto" style="min-width:120px" data-op-select="' + safeId + '">' +
      '<option value="insert"' + (obj.operation === 'insert' ? ' selected' : '') + '>Insert</option>' +
      '<option value="upsert"' + (obj.operation === 'upsert' ? ' selected' : '') + '>Upsert</option>' +
      '<option value="update"' + (obj.operation === 'update' ? ' selected' : '') + '>Update</option>' +
      '</select></div>' +
      '<div class="mb-2"><label class="form-label small mb-1">Upsert-Feld (External ID)</label>' +
      '<select class="form-select form-select-sm" data-external-id-select="' + safeId + '"' + (obj.operation === 'upsert' ? '' : ' disabled') + '>' +
      '<option value="">- Bitte wählen -</option>' + externalIdOptions +
      '</select>' +
      '<div class="small text-secondary mt-1">Wird nur für Upsert verwendet.</div></div>' +
      '<div class="input-group mb-1">' +
      '<input type="text" class="form-control form-control-sm" placeholder="Noch keine Datei ausgewählt" value="' + esc(obj.filePath || '') + '" data-file-path="' + safeId + '" readonly />' +
      '<input type="file" class="d-none" data-file-dialog="' + safeId + '" accept=".csv,.txt,.json,.xlsx,.xls" />' +
      '<button class="btn btn-sm btn-outline-primary" data-pick-file="' + safeId + '">Datei wählen</button>' +
      '<button class="btn btn-sm btn-outline-secondary" data-analyze-file="' + safeId + '">Analysieren</button>' +
      '</div>' +
      '<div class="row g-2 mb-2">' +
      '<div class="col-md-4"><label class="form-label small mb-1">Charset</label><select class="form-select form-select-sm" data-file-charset="' + safeId + '">' +
      '<option value="utf8"' + ((obj.fileCharset || 'utf8') === 'utf8' ? ' selected' : '') + '>UTF-8</option>' +
      '<option value="windows-1252"' + ((obj.fileCharset || '') === 'windows-1252' ? ' selected' : '') + '>Windows-1252 (ANSI)</option>' +
      '<option value="latin1"' + (obj.fileCharset === 'latin1' ? ' selected' : '') + '>Latin-1</option>' +
      '<option value="utf-16le"' + (obj.fileCharset === 'utf-16le' ? ' selected' : '') + '>UTF-16 LE</option>' +
      '<option value="ascii"' + (obj.fileCharset === 'ascii' ? ' selected' : '') + '>ASCII</option>' +
      '</select></div>' +
      '<div class="col-md-4"><label class="form-label small mb-1">Trennzeichen</label><select class="form-select form-select-sm" data-file-delimiter="' + safeId + '">' +
      '<option value=""' + ((!obj.fileDelimiter || obj.fileDelimiter === 'auto') ? ' selected' : '') + '>Automatisch erkennen</option>' +
      '<option value=";"' + ((obj.fileDelimiter || ';') === ';' ? ' selected' : '') + '>Semikolon (;)</option>' +
      '<option value=","' + (obj.fileDelimiter === ',' ? ' selected' : '') + '>Komma (,)</option>' +
      '<option value="|"' + (obj.fileDelimiter === '|' ? ' selected' : '') + '>Pipe (|)</option>' +
      '<option value="	"' + (obj.fileDelimiter === '	' ? ' selected' : '') + '>Tabulator</option>' +
      '</select></div>' +
      '<div class="col-md-4"><label class="form-label small mb-1">Textqualifier</label><select class="form-select form-select-sm" data-file-text-qualifier="' + safeId + '">' +
      '<option value="""' + ((obj.fileTextQualifier || '"') === '"' ? ' selected' : '') + '>Doppelte Anführungszeichen (")</option>' +
      '<option value="&#39;"' + (obj.fileTextQualifier === "'" ? ' selected' : '') + '>Einfache Anführungszeichen (&#39;)</option>' +
      '<option value=""' + (obj.fileTextQualifier === '' ? ' selected' : '') + '>Keiner</option>' +
      '</select></div>' +
      '<div class="col-md-4"><label class="form-label small mb-1">Excel-Mappe</label><select class="form-select form-select-sm" data-file-sheet="' + safeId + '"' + (availableSheetNames.length ? '' : ' disabled') + '>' + sheetOptions + '</select></div>' +
      '<div class="col-md-4"><label class="form-label small mb-1">Verarbeitungsmodus</label><select class="form-select form-select-sm" data-processing-mode="' + safeId + '">' +
      '<option value="sqlite"' + (((obj.processingMode || obj.stagingMode || 'sqlite') === 'sqlite') ? ' selected' : '') + '>SQLite-Staging</option>' +
      '<option value="file"' + (obj.processingMode === 'file' ? ' selected' : '') + '>Datei direkt</option>' +
      '</select></div>' +
      '</div>' +
      '<div id="mig-file-cols-' + safeId + '" class="small text-secondary">' +
      esc(renderMigFileSummary(obj)) +
      '</div></div></div>';
  }).join('');

  migState.objects.forEach((obj) => {
    const fileInput = container.querySelector('[data-file-path="' + obj.id + '"]');
    const fileDialog = container.querySelector('[data-file-dialog="' + obj.id + '"]');
    const pickBtn = container.querySelector('[data-pick-file="' + obj.id + '"]');
    const opSelect = container.querySelector('[data-op-select="' + obj.id + '"]');
    const externalIdSelect = container.querySelector('[data-external-id-select="' + obj.id + '"]');
    if (opSelect) {
      opSelect.addEventListener('change', () => {
        obj.operation = opSelect.value;
        if (externalIdSelect) {
          externalIdSelect.disabled = obj.operation !== 'upsert';
        }
        if (obj.operation !== 'upsert') {
          obj.externalIdField = '';
          if (externalIdSelect) {
            externalIdSelect.value = '';
          }
        }
      });
    }
    if (externalIdSelect) {
      externalIdSelect.addEventListener('change', () => {
        obj.externalIdField = externalIdSelect.value || '';
      });
    }
    const analyzeBtn = container.querySelector('[data-analyze-file="' + obj.id + '"]');
    const charsetInput = container.querySelector('[data-file-charset="' + obj.id + '"]');
    const delimiterInput = container.querySelector('[data-file-delimiter="' + obj.id + '"]');
    const textQualifierInput = container.querySelector('[data-file-text-qualifier="' + obj.id + '"]');
    const sheetInput = container.querySelector('[data-file-sheet="' + obj.id + '"]');
    const processingModeInput = container.querySelector('[data-processing-mode="' + obj.id + '"]');

    const syncCsvOptions = () => {
      obj.fileCharset = charsetInput ? charsetInput.value.trim() || 'utf8' : (obj.fileCharset || 'utf8');
      obj.fileDelimiter = delimiterInput ? delimiterInput.value || ';' : (obj.fileDelimiter || ';');
      obj.fileTextQualifier = textQualifierInput ? textQualifierInput.value || '"' : (obj.fileTextQualifier || '"');
      obj.fileSheetName = sheetInput ? sheetInput.value || '' : (obj.fileSheetName || '');
      obj.processingMode = processingModeInput ? processingModeInput.value || 'sqlite' : (obj.processingMode || 'sqlite');
    };

    [charsetInput, delimiterInput, textQualifierInput, sheetInput, processingModeInput].forEach((input) => {
      if (!input) return;
      input.addEventListener('change', syncCsvOptions);
    });

    if (pickBtn && fileDialog) {
      pickBtn.addEventListener('click', () => {
        fileDialog.click();
      });

      fileDialog.addEventListener('change', async () => {
        const file = fileDialog.files && fileDialog.files[0] ? fileDialog.files[0] : null;
        if (!file) return;

        pickBtn.disabled = true;
        if (analyzeBtn) analyzeBtn.disabled = true;
        pickBtn.textContent = 'Upload…';

        try {
          syncCsvOptions();
          await migSave();
          await uploadMigrationObjectFile(obj, file);

          if (fileInput) fileInput.value = obj.filePath || '';
          const colDiv = document.getElementById('mig-file-cols-' + obj.id);
          if (colDiv) {
            colDiv.textContent = renderMigFileSummary(obj);
          }

          await migSave();
          renderMigMappingObjectSelect();
        } catch (err) {
          alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
        } finally {
          pickBtn.disabled = false;
          if (analyzeBtn) analyzeBtn.disabled = false;
          pickBtn.textContent = 'Datei wählen';
          fileDialog.value = '';
        }
      });
    }

    if (analyzeBtn) {
      analyzeBtn.addEventListener('click', async () => {
        const pathEl = container.querySelector('[data-file-path="' + obj.id + '"]');
        obj.filePath = pathEl ? pathEl.value.trim() : obj.filePath;
        syncCsvOptions();
        if (!obj.filePath) { alert('Bitte zuerst eine Datei auswählen.'); return; }
        analyzeBtn.disabled = true;
        analyzeBtn.textContent = '…';
        try {
          await migSave();
          await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
          const colDiv = document.getElementById('mig-file-cols-' + obj.id);
          if (colDiv) colDiv.textContent = renderMigFileSummary(obj);
          await migSave();
          renderMigMappingObjectSelect();
        } catch (err) {
          alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
        } finally {
          analyzeBtn.disabled = false;
          analyzeBtn.textContent = 'Analysieren';
        }
      });
    }
  });

  if (getMigSelectedPendingImports(migState.pendingImports).length && !migState.pendingImportInProgress) {
    queueMicrotask(() => {
      consumePendingMigrationImportIfPossible().catch((error) => {
        alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
      });
    });
  }
}

function renderMigMappingObjectSelect() {
  const sel = document.getElementById('mig-mapping-object-select');
  if (!sel) return;
  sel.innerHTML = migState.objects.map((obj) =>
    '<option value="' + esc(obj.id) + '">' + esc(getMigObjectDisplayName(obj)) + '</option>'
  ).join('');
  renderMigMappingPanel();
}

async function renderMigMappingPanel() {
  const sel = document.getElementById('mig-mapping-object-select');
  const panel = document.getElementById('mig-mapping-panel');
  const assistantShell = document.getElementById('mig-mapping-assistant-shell');
  if (!sel || !panel) return;
  const objectId = sel.value;
  const obj = migState.objects.find((o) => o.id === objectId);
  if (!obj) {
    if (assistantShell) assistantShell.innerHTML = '';
    panel.innerHTML = '<div class="text-secondary small">Kein Objekt ausgewählt.</div>';
    return;
  }
  if (!obj.fileColumns || !obj.fileColumns.length) {
    if (assistantShell) assistantShell.innerHTML = '';
    panel.innerHTML = '<div class="text-secondary small">Bitte zuerst die Datei in Schritt 2 analysieren.</div>'; return;
  }

  if (obj.stagingMode === 'sqlite' && (!obj.previewRows || !obj.previewRows.length) && Number(obj.fileRecordCount || 0) >= 0) {
    try {
      await loadMigObjectPreview(obj, obj.previewOffset || 0, obj.previewLimit || 10);
    } catch {
      // preview bootstrap falls back to current state
    }
  }

  if (getMigLatestFailedStep(obj.id) && !obj.failedPreviewLoadedFor) {
    try {
      await loadMigLatestFailedPreview(obj);
    } catch {
      obj.failedPreviewRecords = [];
    }
  }

  panel.innerHTML = '<div class="spinner-border spinner-border-sm me-2"></div>Salesforce-Felder laden…';
  let sfFields = [];
  let sfObjects = [];
  try {
    const [fieldsRes, objectsRes] = await Promise.all([
      fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject) + '&instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || '')),
      fetch('/api/salesforce/objects?instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''))
    ]);
    if (fieldsRes.ok) sfFields = await fieldsRes.json();
    if (objectsRes.ok) sfObjects = await objectsRes.json();
  } catch { /* ignore */ }

  const existingFieldNames = new Set([
    ...(sfFields || []).map((f) => String(f.name || '').toLowerCase())
  ]);
  obj._existingFieldNames = Array.from(existingFieldNames);
  renderMigrationMappingAssistant(obj);

  const autoMappedCount = await autoPopulateMigFieldMappings(obj, sfFields);
  const externalIdChanged = autoSelectMigExternalIdField(obj, sfFields);
  if (autoMappedCount > 0 || externalIdChanged) {
    await migSave();
  }

  // Mark mapping entries that point to fields not yet existing in Salesforce.
  (obj.fieldMappings || []).forEach((mapping) => {
    const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, sfFields);
    const sfField = (sfFields || []).find((field) => String(field.name || '').toLowerCase() === resolvedTargetField.toLowerCase());
    if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
      mapping.targetField = resolvedTargetField;
      mapping.targetFieldLabel = sfField?.label || resolvedTargetField;
      mapping.targetFieldType = sfField?.type || mapping.targetFieldType;
    }
    mapping._isMissing = !!resolvedTargetField && !existingFieldNames.has(resolvedTargetField.toLowerCase());
  });

  const parsePicklistText = (value) => String(value || '').split(';')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const idx = part.indexOf('=');
      if (idx < 0) return null;
      return { source: part.slice(0, idx).trim(), target: part.slice(idx + 1).trim() };
    })
    .filter((entry) => entry && (entry.source || entry.target));
  const renderMigPicklistMappingRows = (entries, sourceColumn) => {
    const normalizedEntries = Array.isArray(entries) ? entries : [];
    if (!normalizedEntries.length) {
      return '<tr><td colspan="3" class="migration-picklist-empty">Keine Picklist-Mappings.</td></tr>';
    }

    return normalizedEntries.map((entry, index) => (
      '<tr data-map-picklist-row="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '">' +
        '<td><input class="form-control form-control-sm" value="' + esc(String(entry?.source || '')) + '" data-map-picklist-source="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '" placeholder="Quellwert" /></td>' +
        '<td><input class="form-control form-control-sm" value="' + esc(String(entry?.target || '')) + '" data-map-picklist-target="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '" placeholder="Zielfeldwert" /></td>' +
        '<td class="text-end"><button type="button" class="btn btn-sm btn-outline-danger" data-map-picklist-delete="' + esc(sourceColumn) + '" data-map-picklist-index="' + esc(String(index)) + '">Löschen</button></td>' +
      '</tr>'
    )).join('');
  };

  // Build lookup object <select> options once (reused per row)
  const sfObjectOptHtml = '<option value="">- SF Objekt wählen -</option>' +
    (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '">' + esc(o.label || o.name) + '</option>').join('');

  const mappedColumns = obj.fileColumns.filter((col) => {
    const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
    return existing && String(existing.targetField || '').trim();
  });
  const sortedFileColumns = obj.fileColumns
    .slice()
    .sort((a, b) => String(a || '').localeCompare(String(b || ''), 'de', { sensitivity: 'base', numeric: true }));
  const missingTargetCount = (obj.fieldMappings || []).filter((mapping) => mapping && mapping._isMissing === true).length;
  const lookupCount = (obj.fieldMappings || []).filter((mapping) => mapping && mapping.lookupEnabled === true).length;

  panel.innerHTML =
    '<div class="migration-mapping-overview">' +
      '<div class="migration-mapping-stat">' +
        '<span class="migration-mapping-stat-value">' + esc(String(mappedColumns.length)) + '</span>' +
        '<span class="migration-mapping-stat-label">gemappt</span>' +
      '</div>' +
      '<div class="migration-mapping-stat">' +
        '<span class="migration-mapping-stat-value">' + esc(String(Math.max(0, obj.fileColumns.length - mappedColumns.length))) + '</span>' +
        '<span class="migration-mapping-stat-label">offen</span>' +
      '</div>' +
      '<div class="migration-mapping-stat">' +
        '<span class="migration-mapping-stat-value">' + esc(String(missingTargetCount)) + '</span>' +
        '<span class="migration-mapping-stat-label">neu anzulegen</span>' +
      '</div>' +
      '<div class="migration-mapping-stat">' +
        '<span class="migration-mapping-stat-value">' + esc(String(lookupCount)) + '</span>' +
        '<span class="migration-mapping-stat-label">Lookups</span>' +
      '</div>' +
    '</div>' +
    '<div class="migration-mapping-toolbar">' +
      '<div>' +
        '<div class="fw-semibold">Mappingmanager</div>' +
        '<div class="small text-secondary">Quellfelder links, Zielfelder rechts. Details nur öffnen, wenn Lookup, Transform oder Picklist gebraucht werden.</div>' +
      '</div>' +
      '<input class="form-control form-control-sm migration-mapping-search" type="search" placeholder="Quelle oder Ziel suchen" data-mig-map-filter>' +
    '</div>' +
    '<div class="migration-mapping-list">' +
    sortedFileColumns.map((col) => {
      const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
      const currentTarget = existing ? resolveMigTargetFieldApiName(existing.targetField, sfFields) : '';
      const targetMeta = (sfFields || []).find((f) => String(f.name || '').toLowerCase() === currentTarget.toLowerCase());
      const targetType = targetMeta?.type || (existing?._isMissing ? 'neu' : '');
      const transformFunction = String(existing?.transformFunction || 'NONE');
      const transformExpression = String(existing?.transformExpression || '');
      const isStatic = transformFunction === 'STATIC';
      const lookupEnabled = existing?.lookupEnabled === true;
      const lookupObject = String(existing?.lookupObject || '');
      const lookupField = String(existing?.lookupField || '');
      const picklistMappings = Array.isArray(existing?.picklistMappings) ? existing.picklistMappings : [];
      const isEmailTarget = String(targetMeta?.type || '').trim().toLowerCase() === 'email'
        || normalizeFieldKey(currentTarget).includes('email');
      const emailValidationEnabled = isEmailTarget && (existing?.emailValidation?.enabled === true || existing?.emailValidationEnabled === true);
      const emailInvalidAction = String(existing?.emailValidation?.invalidAction || existing?.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
      const rowSearchText = [col, currentTarget, targetType, transformFunction, lookupObject, lookupField, (emailValidationEnabled ? 'email' : '')].join(' ').toLowerCase();
      const rowStatus = currentTarget ? (existing?._isMissing ? 'new' : 'mapped') : 'open';
      const rowStatusLabel = currentTarget ? (existing?._isMissing ? 'Neues Feld' : 'Gemappt') : 'Offen';
      const rowStatusClass = currentTarget ? (existing?._isMissing ? 'text-bg-warning' : 'text-bg-success') : 'text-bg-light';
      const detailsOpen = lookupEnabled || picklistMappings.length > 0 || isStatic || isEmailTarget;
      const usesManualTarget = Boolean(currentTarget) && !targetMeta;

      // Lookup object options with pre-selected value
      const lookupObjOptions = '<option value="">- SF Objekt wählen -</option>' +
        (sfObjects || []).map((o) => '<option value="' + esc(o.name) + '"' + (lookupObject === o.name ? ' selected' : '') + '>' + esc(o.label || o.name) + '</option>').join('');

      return '<section class="migration-mapping-row" data-mig-map-row data-mig-map-status="' + esc(rowStatus) + '" data-mig-map-search="' + esc(rowSearchText) + '">' +
        '<div class="migration-mapping-row-main">' +
          '<div class="migration-mapping-source">' +
            '<span class="badge ' + rowStatusClass + '">' + esc(rowStatusLabel) + '</span>' +
            '<code>' + esc(col) + '</code>' +
          '</div>' +
          '<div class="migration-mapping-arrow" aria-hidden="true">&rarr;</div>' +
          '<div class="migration-mapping-target">' +
            '<div class="migration-mapping-target-inputs">' +
              '<select class="form-select form-select-sm" data-map-target-select="' + esc(col) + '">' + getMigMappingTargetOptions(sfFields, currentTarget) + '</select>' +
              '<input class="form-control form-control-sm' + (usesManualTarget ? '' : ' d-none') + '" placeholder="Neues Salesforce-Feld eingeben" value="' + esc(usesManualTarget ? currentTarget : '') + '" data-map-col="' + esc(col) + '" data-map-obj="' + esc(objectId) + '" />' +
            '</div>' +
            '<span class="badge bg-secondary migration-mapping-type" data-map-type="' + esc(col) + '">' + esc(targetType) + (targetMeta?.requiredOnCreate === true ? ' *' : '') + '</span>' +
          '</div>' +
          '<div class="migration-mapping-transform">' +
            '<label class="form-label form-label-sm mb-1">Umwandlung</label>' +
            '<select class="form-select form-select-sm" data-map-transform="' + esc(col) + '">' +
              ['NONE','TRIM','UPPERCASE','LOWERCASE','TO_INTEGER','TO_BOOLEAN','DATETIME_ISO','STATIC'].map((fn) =>
                '<option value="' + fn + '"' + (transformFunction === fn ? ' selected' : '') + '>' + fn + '</option>'
              ).join('') +
            '</select>' +
          '</div>' +
        '</div>' +
        '<details class="migration-mapping-details"' + (detailsOpen ? ' open' : '') + '>' +
          '<summary>Details</summary>' +
          '<div class="migration-mapping-detail-grid">' +
            '<div>' +
              '<label class="form-label form-label-sm mb-1">Statischer Wert</label>' +
              '<input class="form-control form-control-sm" placeholder="Nur bei STATIC" value="' + esc(transformExpression) + '" data-map-transform-expression="' + esc(col) + '"' + (isStatic ? '' : ' style="display:none"') + ' />' +
            '</div>' +
            '<div class="migration-mapping-lookup-box">' +
              '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-map-lookup-enabled="' + esc(col) + '"' + (lookupEnabled ? ' checked' : '') + '><label class="form-check-label small">Lookup aktivieren</label></div>' +
              '<div class="small text-secondary mb-2">Nur External-ID-Felder sind auswählbar.</div>' +
              '<div class="migration-mapping-detail-grid migration-mapping-detail-grid-compact">' +
                '<select class="form-select form-select-sm" data-map-lookup-object="' + esc(col) + '">' + lookupObjOptions + '</select>' +
                '<select class="form-select form-select-sm" data-map-lookup-field="' + esc(col) + '">' +
                  '<option value="">- Feld wählen -</option>' +
                  (lookupField ? '<option value="' + esc(lookupField) + '" selected>' + esc(lookupField) + '</option>' : '') +
                '</select>' +
              '</div>' +
              '<div class="small text-warning mt-1 d-none" data-map-lookup-status="' + esc(col) + '"></div>' +
            '</div>' +
            '<div>' +
              '<div class="d-flex justify-content-between align-items-center mb-1">' +
                '<label class="form-label form-label-sm mb-0">Picklist-Mapping</label>' +
                '<button type="button" class="btn btn-sm btn-outline-secondary" data-map-picklist-add="' + esc(col) + '">Eintrag hinzufügen</button>' +
              '</div>' +
              '<div class="table-responsive"><table class="table table-sm migration-picklist-table mb-0"><thead><tr><th>Quelle</th><th>Ziel</th><th></th></tr></thead><tbody data-map-picklist-table="' + esc(col) + '">' + renderMigPicklistMappingRows(picklistMappings, col) + '</tbody></table></div>' +
            '</div>' +
            '<div class="scheduler-email-options' + (isEmailTarget ? '' : ' d-none') + '" data-map-email-options="' + esc(col) + '" style="border-top: 1px solid #dee2e6; padding-top: 0.75rem; margin-top: 0.75rem;">' +
              '<label class="form-label form-label-sm mb-2" style="font-weight: 600;">E-Mail-Validierung</label>' +
              '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-map-email-enabled="' + esc(col) + '"' + (emailValidationEnabled ? ' checked' : '') + '><label class="form-check-label small">E-Mail-Adresse validieren</label></div>' +
              '<div class="ps-3">' +
                '<label class="form-label form-label-sm mb-2">Bei ungültiger E-Mail:</label>' +
                '<select class="form-select form-select-sm" data-map-email-action="' + esc(col) + '">' +
                  '<option value="EMPTY"' + (emailInvalidAction === 'EMPTY' ? ' selected' : '') + '>→ Leer übermitteln</option>' +
                  '<option value="ERROR"' + (emailInvalidAction === 'ERROR' ? ' selected' : '') + '>→ Datensatz als fehlerhaft kennzeichnen</option>' +
                '</select>' +
              '</div>' +
            '</div>' +
          '</div>' +
        '</details>' +
      '</section>';
    }).join('') +
    '</div>' +
    renderMigPreviewTable(obj);

  const updateMappingEntry = (col) => {
    const objId = objectId;
    const target = migState.objects.find((o) => o.id === objId);
    if (!target) return;
    if (!target.fieldMappings) target.fieldMappings = [];
    const idx = target.fieldMappings.findIndex((m) => m.sourceColumn === col);
    const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
    const fieldInput = panel.querySelector('[data-map-col="' + col + '"]');
    const transformSel = panel.querySelector('[data-map-transform="' + col + '"]');
    const transformExprEl = panel.querySelector('[data-map-transform-expression="' + col + '"]');
    const lookupEnabledEl = panel.querySelector('[data-map-lookup-enabled="' + col + '"]');
    const lookupObjectEl = panel.querySelector('[data-map-lookup-object="' + col + '"]');
    const lookupFieldEl = panel.querySelector('[data-map-lookup-field="' + col + '"]');
    const picklistRows = Array.from(panel.querySelectorAll('[data-map-picklist-row="' + col + '"]'));
    const emailEnabledEl = panel.querySelector('[data-map-email-enabled="' + col + '"]');
    const emailActionEl = panel.querySelector('[data-map-email-action="' + col + '"]');

    const usesManualTarget = String(targetSelectEl?.value || '') === '__manual__';
    const rawSelectedFieldName = usesManualTarget
      ? String(fieldInput?.value || '').trim()
      : String(targetSelectEl?.value || '').trim();
    if (!rawSelectedFieldName) {
      if (idx >= 0) target.fieldMappings.splice(idx, 1);
      const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
      if (typeBadge) typeBadge.textContent = '';
      const row = fieldInput?.closest ? fieldInput.closest('[data-mig-map-row]') : null;
      const statusBadge = row?.querySelector ? row.querySelector('.migration-mapping-source .badge') : null;
      if (row) {
        row.setAttribute('data-mig-map-status', 'open');
        row.setAttribute('data-mig-map-search', String(col || '').toLowerCase());
      }
      if (statusBadge) {
        statusBadge.className = 'badge text-bg-light';
        statusBadge.textContent = 'Offen';
      }
      renderMigMissingFields();
      return;
    }

    const selectedFieldName = resolveMigTargetFieldApiName(rawSelectedFieldName, sfFields);
    const sfField = (sfFields || []).find((f) => String(f.name || '').toLowerCase() === selectedFieldName.toLowerCase());
    if (fieldInput && fieldInput.value !== selectedFieldName) {
      fieldInput.value = selectedFieldName;
    }
    const lookupAllowedForTarget = String(sfField?.type || '').toLowerCase() === 'reference' || String(sfField?.type || '').toLowerCase() === 'id';
    if (!lookupAllowedForTarget && lookupEnabledEl?.checked) {
      lookupEnabledEl.checked = false;
    }
    const picklistMappings = picklistRows.map((row) => ({
      source: String(row.querySelector('[data-map-picklist-source="' + col + '"]')?.value || '').trim(),
      target: String(row.querySelector('[data-map-picklist-target="' + col + '"]')?.value || '').trim()
    })).filter((entry) => entry.source || entry.target);
    const current = idx >= 0 ? target.fieldMappings[idx] : { sourceColumn: col };
    const nextEntry = {
      ...current,
      sourceColumn: col,
      targetField: selectedFieldName,
      targetFieldLabel: sfField?.label || selectedFieldName,
      targetFieldType: sfField?.type,
      transformFunction: String(transformSel?.value || 'NONE'),
      transformExpression: String(transformExprEl?.value || '').trim(),
      lookupEnabled: lookupAllowedForTarget && Boolean(lookupEnabledEl?.checked),
      lookupObject: String(lookupObjectEl?.value || '').trim(),
      lookupField: String(lookupFieldEl?.value || '').trim(),
      picklistMappings,
      _isMissing: !sfField,
      emailValidation: ((String(sfField?.type || '').trim().toLowerCase() === 'email' || normalizeFieldKey(selectedFieldName).includes('email')) && emailEnabledEl && emailEnabledEl.checked)
        ? { enabled: true, invalidAction: String(emailActionEl?.value || 'EMPTY') }
        : undefined
    };

    if (idx >= 0) target.fieldMappings[idx] = nextEntry;
    else target.fieldMappings.push(nextEntry);

    const typeBadge = panel.querySelector('[data-map-type="' + col + '"]');
    if (typeBadge) {
      typeBadge.textContent = sfField ? String(sfField.type || '') : (selectedFieldName ? 'neu' : '');
    }
    const row = fieldInput?.closest ? fieldInput.closest('[data-mig-map-row]') : null;
    const statusBadge = row?.querySelector ? row.querySelector('.migration-mapping-source .badge') : null;
    const emailOptions = row?.querySelector ? row.querySelector('[data-map-email-options="' + col + '"]') : null;
    if (row) {
      row.setAttribute('data-mig-map-status', sfField ? 'mapped' : 'new');
      row.setAttribute('data-mig-map-search', [col, selectedFieldName, sfField?.type || 'neu', transformSel?.value || '', (emailEnabledEl && emailEnabledEl.checked ? 'email' : '')].join(' ').toLowerCase());
    }
    if (statusBadge) {
      statusBadge.className = 'badge ' + (sfField ? 'text-bg-success' : 'text-bg-warning');
      statusBadge.textContent = sfField ? 'Gemappt' : 'Neues Feld';
    }
    if (emailOptions) {
      const isEmailTarget = String(sfField?.type || '').trim().toLowerCase() === 'email' || normalizeFieldKey(selectedFieldName).includes('email');
      emailOptions.classList.toggle('d-none', !isEmailTarget);
      if (!isEmailTarget && emailEnabledEl) {
        emailEnabledEl.checked = false;
      }
    }

    renderMigMissingFields();
  };

  const addMigPicklistEntry = (col) => {
    const target = migState.objects.find((o) => o.id === objectId);
    if (!target) return;
    if (!target.fieldMappings) target.fieldMappings = [];
    let mapping = target.fieldMappings.find((entry) => entry.sourceColumn === col);
    if (!mapping) {
      const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
      const fieldInput = panel.querySelector('[data-map-col="' + col + '"]');
      const nextTargetField = String(targetSelectEl?.value || '') === '__manual__'
        ? String(fieldInput?.value || '').trim()
        : String(targetSelectEl?.value || '').trim();
      if (!nextTargetField) {
        return;
      }
      mapping = {
        sourceColumn: col,
        targetField: nextTargetField,
        picklistMappings: []
      };
      target.fieldMappings.push(mapping);
    }
    if (!Array.isArray(mapping.picklistMappings)) {
      mapping.picklistMappings = [];
    }
    mapping.picklistMappings.push({ source: '', target: '' });
    renderMigMappingPanel();
  };

  const deleteMigPicklistEntry = (col, index) => {
    const target = migState.objects.find((o) => o.id === objectId);
    const mapping = target?.fieldMappings?.find((entry) => entry.sourceColumn === col);
    if (!mapping || !Array.isArray(mapping.picklistMappings)) {
      return;
    }
    mapping.picklistMappings.splice(index, 1);
    renderMigMappingPanel();
  };

  const setLookupValidationState = (col, message) => {
    const fieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
    const statusEl = panel.querySelector('[data-map-lookup-status="' + col + '"]');
    if (fieldSel) {
      fieldSel.classList.toggle('is-invalid', Boolean(message));
    }
    if (statusEl) {
      statusEl.textContent = message || '';
      statusEl.classList.toggle('d-none', !message);
    }
  };

  // Helper: load lookup fields for a column's lookup-field <select>
  const loadLookupFields = async (col, selectedObject) => {
    const fieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
    if (!fieldSel) return;
    if (!selectedObject) {
      const cur = fieldSel.value;
      fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' + (cur ? '<option value="' + esc(cur) + '" selected>' + esc(cur) + '</option>' : '');
      setLookupValidationState(col, '');
      return;
    }
    try {
      const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(selectedObject) + '&instanceId=' + encodeURIComponent(await ensureMigRuntimeInstanceId() || ''));
      if (!res.ok) return;
      const fields = await res.json();
      const externalIdFields = (fields || []).filter((f) => f && f.isExternalId === true);
      const curVal = fieldSel.value;
      fieldSel.innerHTML = '<option value="">- Feld wählen -</option>' +
        externalIdFields.map((f) => '<option value="' + esc(f.name) + '"' + (f.name === curVal ? ' selected' : '') + '>' + esc(f.label && f.label !== f.name ? f.label + ' (' + f.name + ')' : f.name) + '</option>').join('');
      if (curVal && !externalIdFields.some((f) => f.name === curVal)) {
        fieldSel.value = '';
        setLookupValidationState(col, 'Gespeichertes Lookup-Feld ist keine External ID mehr und wurde geleert.');
        return;
      }
      setLookupValidationState(col, '');
    } catch { /* ignore */ }
  };

  const filterInput = panel.querySelector('[data-mig-map-filter]');
  if (filterInput) {
    filterInput.addEventListener('input', () => {
      const term = String(filterInput.value || '').trim().toLowerCase();
      panel.querySelectorAll('[data-mig-map-row]').forEach((row) => {
        const searchText = String(row.getAttribute('data-mig-map-search') || '').toLowerCase();
        row.classList.toggle('d-none', Boolean(term) && !searchText.includes(term));
      });
    });
  }

  obj.fileColumns.forEach((col) => {
    // Salesforce field select / manual input
    const targetSelectEl = panel.querySelector('[data-map-target-select="' + col + '"]');
    const sfFieldInput = panel.querySelector('[data-map-col="' + col + '"]');
    if (targetSelectEl && sfFieldInput) {
      targetSelectEl.addEventListener('change', () => {
        const showManualInput = targetSelectEl.value === '__manual__';
        sfFieldInput.classList.toggle('d-none', !showManualInput);
        if (!showManualInput) {
          sfFieldInput.value = '';
        }
        updateMappingEntry(col);
      });
    }
    if (sfFieldInput) {
      sfFieldInput.addEventListener('input', () => updateMappingEntry(col));
      sfFieldInput.addEventListener('change', () => updateMappingEntry(col));
    }

    // Transform select → show/hide expression input + update
    const transformEl = panel.querySelector('[data-map-transform="' + col + '"]');
    const exprEl = panel.querySelector('[data-map-transform-expression="' + col + '"]');
    if (transformEl) {
      transformEl.addEventListener('change', () => {
        if (exprEl) exprEl.style.display = transformEl.value === 'STATIC' ? '' : 'none';
        updateMappingEntry(col);
      });
    }
    // Transform expression input
    if (exprEl) {
      exprEl.addEventListener('input', () => updateMappingEntry(col));
      exprEl.addEventListener('change', () => updateMappingEntry(col));
    }

    // Lookup enabled checkbox
    const lookupEnabledEl = panel.querySelector('[data-map-lookup-enabled="' + col + '"]');
    if (lookupEnabledEl) {
      lookupEnabledEl.addEventListener('change', () => updateMappingEntry(col));
    }

    // Lookup object select → reload lookup fields, then update
    const lookupObjSel = panel.querySelector('[data-map-lookup-object="' + col + '"]');
    if (lookupObjSel) {
      lookupObjSel.addEventListener('change', async () => {
        await loadLookupFields(col, lookupObjSel.value);
        updateMappingEntry(col);
      });
    }

    // Email validation controls
    const emailEnabledEl = panel.querySelector('[data-map-email-enabled="' + col + '"]');
    const emailActionEl = panel.querySelector('[data-map-email-action="' + col + '"]');
    if (emailEnabledEl && emailActionEl) {
      emailEnabledEl.addEventListener('change', () => {
        emailActionEl.style.display = emailEnabledEl.checked ? '' : 'none';
        updateMappingEntry(col);
      });
      emailActionEl.addEventListener('change', () => updateMappingEntry(col));
    }

    // Lookup field select
    const lookupFieldSel = panel.querySelector('[data-map-lookup-field="' + col + '"]');
    if (lookupFieldSel) {
      lookupFieldSel.addEventListener('change', () => {
        setLookupValidationState(col, '');
        updateMappingEntry(col);
      });
    }

    // Picklist table editor
    panel.querySelector('[data-map-picklist-add="' + col + '"]')?.addEventListener('click', () => addMigPicklistEntry(col));
    panel.querySelectorAll('[data-map-picklist-delete="' + col + '"]').forEach((button) => {
      button.addEventListener('click', () => {
        const index = Number(button.getAttribute('data-map-picklist-index') || '-1');
        if (index >= 0) {
          deleteMigPicklistEntry(col, index);
        }
      });
    });
    panel.querySelectorAll('[data-map-picklist-source="' + col + '"], [data-map-picklist-target="' + col + '"]').forEach((input) => {
      input.addEventListener('input', () => updateMappingEntry(col));
      input.addEventListener('change', () => updateMappingEntry(col));
    });

    // Pre-load lookup fields for rows that already have a lookup object set
    const existing = (obj.fieldMappings || []).find((m) => m.sourceColumn === col);
    if (existing?.lookupObject) {
      loadLookupFields(col, existing.lookupObject);
    }
  });

  const assistantApplyButton = document.getElementById('mig-mapping-assistant-apply');
  assistantApplyButton?.addEventListener('click', async () => {
    const selectedProfile = String(document.getElementById('mig-mapping-assistant-profile')?.value || getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject)).trim();
    migState.mappingAssistantProfilesByObjectId[obj.id] = selectedProfile;
    const autoMapped = await autoPopulateMigFieldMappings(obj, sfFields);
    const externalIdUpdated = autoSelectMigExternalIdField(obj, sfFields);
    if (autoMapped > 0 || externalIdUpdated) {
      await migSave();
    }
    await renderMigMappingPanel();
  });

  panel.querySelectorAll('[data-preview-prev], [data-preview-next]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const isNext = btn.hasAttribute('data-preview-next');
      const pageSize = Math.max(1, Number(obj.previewLimit || 10) || 10);
      const currentOffset = Math.max(0, Number(obj.previewOffset || 0) || 0);
      const nextOffset = isNext ? currentOffset + pageSize : Math.max(0, currentOffset - pageSize);
      btn.disabled = true;
      try {
        await loadMigObjectPreview(obj, nextOffset, pageSize);
        renderMigMappingPanel();
      } catch (err) {
        alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
        btn.disabled = false;
      }
    });
  });

  panel.querySelectorAll('[data-preview-filter]').forEach((input) => {
    input.addEventListener('input', () => {
      obj.previewFilter = input.value || '';
      if (!isMigServerPreview(obj)) {
        renderMigMappingPanel();
        return;
      }

      window.clearTimeout(obj._previewFilterTimer || 0);
      obj._previewFilterTimer = window.setTimeout(async () => {
        try {
          await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
          renderMigMappingPanel();
        } catch (err) {
          alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
        }
      }, 250);
    });
  });

  panel.querySelectorAll('[data-preview-status-filter]').forEach((input) => {
    input.addEventListener('change', async () => {
      obj.previewStatusFilter = input.value || '';
      try {
        await loadMigObjectPreview(obj, 0, obj.previewLimit || 10);
        renderMigMappingPanel();
      } catch (err) {
        alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
      }
    });
  });

  renderMigMissingFields();
}

function renderMigMissingFields() {
  const container = document.getElementById('mig-missing-fields-list');
  if (!container) return;
  const missing = collectMigMissingFieldMappings();
  if (!missing.length) {
    container.innerHTML = '<div class="alert alert-success">Alle gemappten Felder existieren in Salesforce – keine Aktion erforderlich.</div>';
    return;
  }
  const buildPicklistText = (values) => (Array.isArray(values) ? values : []).map((value) => String(value || '').trim()).filter(Boolean).join('\n');
  const countPicklistValues = (value) => String(value || '').split(/\r?\n/).map((entry) => entry.trim()).filter(Boolean).length;
  container.innerHTML = '<table class="table table-sm"><thead><tr><th>Objekt</th><th>SF-Feld</th><th>Typ</th><th>Aktion</th></tr></thead><tbody>' +
    missing.map((item) =>
      '<tr><td>' + esc(item.obj.salesforceObject) + '</td>' +
      '<td><code>' + esc(item.mapping.targetField) + '</code></td>' +
      '<td>' +
        '<select class="form-select form-select-sm" data-field-type="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' +
        ['Text', 'Number', 'Date', 'DateTime', 'Checkbox', 'Currency', 'Percent', 'Email', 'Phone', 'Url', 'Picklist'].map((t) => '<option>' + t + '</option>').join('') +
        '</select>' +
        '<div class="mt-2 d-none" data-picklist-config="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' +
          '<div class="d-flex gap-2 align-items-center mb-2">' +
            '<button class="btn btn-sm btn-outline-secondary" type="button" data-picklist-autofill="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">AutoFill</button>' +
            '<span class="small text-secondary">Liest alle Varianten aus der Quellspalte.</span>' +
            '<span class="badge text-bg-light" data-picklist-count="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' + countPicklistValues(buildPicklistText(item.mapping.picklistValues)) + ' Werte</span>' +
          '</div>' +
          '<textarea class="form-control form-control-sm" rows="5" placeholder="Ein Wert pro Zeile" data-picklist-values="' + esc(item.mapping.targetField) + '-' + esc(item.obj.id) + '">' + esc(buildPicklistText(item.mapping.picklistValues)) + '</textarea>' +
          '<div class="small text-secondary mt-1">Jede Zeile wird als Picklist-Wert angelegt.</div>' +
        '</div>' +
      '</td>' +
      '<td><button class="btn btn-sm btn-outline-primary" data-create-field-obj="' + esc(item.obj.id) + '" data-create-field-name="' + esc(item.mapping.targetField) + '" data-create-source-column="' + esc(item.mapping.sourceColumn || '') + '">Anlegen</button></td></tr>'
    ).join('') + '</tbody></table>';

  const updatePicklistValueState = (typeKey) => {
    const textarea = container.querySelector('[data-picklist-values="' + typeKey + '"]');
    const countEl = container.querySelector('[data-picklist-count="' + typeKey + '"]');
    const values = String(textarea?.value || '').split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
    const createBtn = container.querySelector('[data-create-field-obj][data-field-type-key="' + typeKey + '"]');
    const fieldName = createBtn ? createBtn.getAttribute('data-create-field-name') : '';
    const objectId = createBtn ? createBtn.getAttribute('data-create-field-obj') : '';
    const obj = (migState.objects || []).find((entry) => entry.id === objectId);
    const mapping = obj ? (obj.fieldMappings || []).find((entry) => entry.targetField === fieldName) : null;
    if (mapping) {
      mapping.picklistValues = values;
    }
    if (countEl) {
      countEl.textContent = values.length + ' Werte';
    }
  };

  const togglePicklistConfig = (typeKey) => {
    const typeSelect = container.querySelector('[data-field-type="' + typeKey + '"]');
    const configPanel = container.querySelector('[data-picklist-config="' + typeKey + '"]');
    if (!typeSelect || !configPanel) return;
    configPanel.classList.toggle('d-none', typeSelect.value !== 'Picklist');
  };

  container.querySelectorAll('[data-field-type]').forEach((select) => {
    const typeKey = select.getAttribute('data-field-type');
    togglePicklistConfig(typeKey);
    select.addEventListener('change', () => togglePicklistConfig(typeKey));
  });

  container.querySelectorAll('[data-picklist-autofill]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const typeKey = btn.getAttribute('data-picklist-autofill');
      const createBtn = container.querySelector('[data-create-field-obj][data-create-field-name][data-field-type-key="' + typeKey + '"]');
      const objectId = createBtn ? createBtn.getAttribute('data-create-field-obj') : null;
      const sourceColumn = createBtn ? createBtn.getAttribute('data-create-source-column') : null;
      const textarea = container.querySelector('[data-picklist-values="' + typeKey + '"]');
      if (!objectId || !sourceColumn || !textarea) return;
      btn.disabled = true;
      const originalText = btn.textContent;
      btn.textContent = '…';
      try {
        const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/objects/' + encodeURIComponent(objectId) + '/distinct-values?column=' + encodeURIComponent(sourceColumn));
        const result = await res.json();
        if (!res.ok) throw new Error(result.error || 'Fehler');
        const values = Array.isArray(result.values) ? result.values : [];
        textarea.value = values.join('\n');
        updatePicklistValueState(typeKey);
      } catch (err) {
        alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
      } finally {
        btn.disabled = false;
        btn.textContent = originalText || 'AutoFill';
      }
    });
  });

  container.querySelectorAll('[data-picklist-values]').forEach((textarea) => {
    const typeKey = textarea.getAttribute('data-picklist-values');
    updatePicklistValueState(typeKey);
    textarea.addEventListener('input', () => updatePicklistValueState(typeKey));
  });

  container.querySelectorAll('[data-create-field-obj]').forEach((btn) => {
    const typeKey = btn.getAttribute('data-create-field-name') + '-' + btn.getAttribute('data-create-field-obj');
    btn.setAttribute('data-field-type-key', typeKey);
    btn.addEventListener('click', async () => {
      const objId = btn.getAttribute('data-create-field-obj');
      const fieldName = btn.getAttribute('data-create-field-name');
      const typeKey = fieldName + '-' + objId;
      const typeSelect = container.querySelector('[data-field-type="' + typeKey + '"]');
      const fieldType = typeSelect ? typeSelect.value : inferMigFieldCreationType({ targetField: fieldName });
      const picklistValuesEl = container.querySelector('[data-picklist-values="' + typeKey + '"]');
      const obj = migState.objects.find((o) => o.id === objId);
      if (!obj) return;
      const mapping = (obj.fieldMappings || []).find((m) => m.targetField === fieldName);
      if (!mapping) return;
      const picklistValues = fieldType === 'Picklist'
        ? String(picklistValuesEl?.value || '').split(/\r?\n/).map((value) => value.trim()).filter(Boolean)
        : [];
      if (fieldType === 'Picklist' && !picklistValues.length) {
        alert('Bitte zuerst Picklist-Werte eintragen oder per AutoFill laden.');
        return;
      }
      btn.disabled = true; btn.textContent = '…';
      try {
        mapping.picklistValues = picklistValues;
        const created = await createMigMissingField(obj, mapping, fieldType, picklistValues);

        btn.className = 'btn btn-sm btn-success'; btn.textContent = '✓ Angelegt';
        const resultDiv = document.getElementById('mig-create-fields-result');
        if (resultDiv) resultDiv.innerHTML += '<div class="alert alert-success py-1 small mt-1">' + esc(obj.salesforceObject + '.' + created.fullFieldName) + (created.result && created.result.action === 'exists' ? ' existiert bereits.' : ' erfolgreich angelegt.') + '</div>';
        renderMigMissingFields();
      } catch (err) {
        btn.className = 'btn btn-sm btn-danger'; btn.textContent = 'Fehler';
        alert('Fehler: ' + (err instanceof Error ? err.message : String(err)));
      }
    });
  });
}

async function migSave() {
  const nameEl = document.getElementById('mig-name');
  const descEl = document.getElementById('mig-description');
  const batchSizeEl = document.getElementById('mig-batch-size');
  if (nameEl) migState.name = nameEl.value.trim() || migState.name;
  if (descEl) migState.description = descEl.value.trim();
  if (batchSizeEl) {
    const parsedBatchSize = Number(batchSizeEl.value || migState.batchSize || 200);
    migState.batchSize = Number.isFinite(parsedBatchSize)
      ? Math.max(1, Math.min(200, Math.trunc(parsedBatchSize)))
      : 200;
    batchSizeEl.value = String(migState.batchSize);
  }
  migState.objects = sanitizeMigObjects(migState.objects);

  const effectiveInstanceId = String(migState.instanceId || state.instanceId || '').trim() || undefined;
  const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(effectiveInstanceId || ''));
  const projectId = String(migState.projectId || (selectedInstance && selectedInstance.projectId) || 'default-project').trim() || 'default-project';
  migState.projectId = projectId;

  const payload = {
    id: migState.id,
    name: migState.name,
    description: migState.description,
    batchSize: migState.batchSize || 200,
    projectId: projectId,
    instanceId: effectiveInstanceId,
    status: 'draft',
    objects: sanitizeMigObjects(migState.objects),
    dependencies: migState.dependencies,
    executionPlan: migState.executionPlan
  };
  const method = migState.id ? 'PUT' : 'POST';
  const url = migState.id ? '/api/migrations/' + encodeURIComponent(migState.id) : '/api/migrations';
  const res = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  const saved = await res.json();
  if (!migState.id) migState.id = saved.id;
  migState.status = String(saved.status || migState.status || 'draft');
  migState.createdAt = String(saved.createdAt || migState.createdAt || '');
  migState.updatedAt = String(saved.updatedAt || migState.updatedAt || '');
  renderWizardMetadata('mig-wizard-meta', migState, 'Neue Migration · noch nicht gespeichert');
  return saved;
}

function openMigWizard(migration, options) {
  clearMigrationModalError();
  const requestedStep = Number(options && options.startStep ? options.startStep : 1);
  const hasLastRunResult = !!(migration && migration.lastRunResult && (
    (Array.isArray(migration.lastRunResult.steps) && migration.lastRunResult.steps.length) ||
    String(migration.lastRunResult.reportPath || '').trim()
  ));
  migState.id = migration ? migration.id : null;
  migState.step = requestedStep >= 1 && requestedStep <= migState.totalSteps ? requestedStep : 1;
  migState.status = migration ? String(migration.status || 'draft') : 'draft';
  migState.activeRunVisible = migState.status === 'running' || hasLastRunResult || !!(options && options.showRunSummary);
  migState.name = migration ? migration.name : (options && options.name ? options.name : '');
  migState.description = migration ? (migration.description || '') : (options && options.description ? options.description : '');
  migState.createdAt = migration ? String(migration.createdAt || '') : '';
  migState.updatedAt = migration ? String(migration.updatedAt || '') : '';
  migState.createdByName = migration ? String(migration.createdByName || migration.createdBy || '') : '';
  migState.updatedByName = migration ? String(migration.updatedByName || migration.updatedBy || '') : '';
  migState.batchSize = migration ? Number(migration.batchSize || 200) : 200;
  if (!Number.isFinite(migState.batchSize) || migState.batchSize <= 0) {
    migState.batchSize = 200;
  }
  migState.batchSize = Math.max(1, Math.min(200, Math.trunc(migState.batchSize)));
  migState.instanceId = migration ? String(migration.instanceId || '') : String(state.instanceId || '');
  migState.projectId = migration
    ? String(migration.projectId || '')
    : String(((state.instances || []).find((item) => String(item.id || '') === String(migState.instanceId || '')) || {}).projectId || 'default-project');
  migState.salesforceLogin = null;
  migState.objects = migration ? sanitizeMigObjects(migration.objects || []) : [];
  migState.dependencies = migration ? JSON.parse(JSON.stringify(migration.dependencies || [])) : [];
  migState.executionPlan = migration ? JSON.parse(JSON.stringify(migration.executionPlan || [])) : [];
  migState.sfObjects = [];
  migState.lastRunResult = migration ? JSON.parse(JSON.stringify(migration.lastRunResult || null)) : null;
  migState.runHistory = migration ? JSON.parse(JSON.stringify(migration.runHistory || [])) : [];
  migState.preflightWarnings = null;
  migState.preflightWarningsLoading = false;
  migState.pendingImports = options && Array.isArray(options.pendingImports)
    ? options.pendingImports.slice()
    : [];
  migState.pendingImportInProgress = false;
  migState.pendingImportSuggestions = options && Array.isArray(options.pendingImportSuggestions)
    ? options.pendingImportSuggestions.slice()
    : [];
  migState.pendingImportAnalysis = options && options.pendingImportAnalysis
    ? options.pendingImportAnalysis
    : null;

  const nameEl = document.getElementById('mig-name');
  const descEl = document.getElementById('mig-description');
  const batchSizeEl = document.getElementById('mig-batch-size');
  const instanceSourceEl = document.getElementById('mig-instance-source');
  const existingInstanceEl = document.getElementById('mig-existing-instance');
  const loginEnvironmentEl = document.getElementById('mig-login-environment');
  const loginAuthTypeEl = document.getElementById('mig-login-auth-type');
  const loginUrlEl = document.getElementById('mig-login-url');
  const loginUsernameEl = document.getElementById('mig-login-username');
  const loginPasswordEl = document.getElementById('mig-login-password');
  const loginSecurityTokenEl = document.getElementById('mig-login-security-token');
  const loginClientIdEl = document.getElementById('mig-login-client-id');
  const loginClientSecretEl = document.getElementById('mig-login-client-secret');
  if (nameEl) nameEl.value = migState.name;
  if (descEl) descEl.value = migState.description;
  if (batchSizeEl) batchSizeEl.value = String(migState.batchSize || 200);
  if (instanceSourceEl) instanceSourceEl.value = 'existing';
  populateMigExistingInstanceOptions();
  if (existingInstanceEl) existingInstanceEl.value = String(migState.instanceId || state.instanceId || existingInstanceEl.value || '');
  if (loginEnvironmentEl) loginEnvironmentEl.value = String(migState.salesforceLogin && migState.salesforceLogin.environment || 'sandbox');
  if (loginAuthTypeEl) loginAuthTypeEl.value = String(migState.salesforceLogin && migState.salesforceLogin.authType || 'password');
  if (loginUrlEl) loginUrlEl.value = String(migState.salesforceLogin && migState.salesforceLogin.loginUrl || getMigLoginUrlForEnvironment(migState.salesforceLogin && migState.salesforceLogin.environment));
  if (loginUsernameEl) loginUsernameEl.value = String(migState.salesforceLogin && migState.salesforceLogin.username || '');
  if (loginPasswordEl) loginPasswordEl.value = String(migState.salesforceLogin && migState.salesforceLogin.password || '');
  if (loginSecurityTokenEl) loginSecurityTokenEl.value = String(migState.salesforceLogin && migState.salesforceLogin.securityToken || '');
  if (loginClientIdEl) loginClientIdEl.value = String(migState.salesforceLogin && migState.salesforceLogin.clientId || '');
  if (loginClientSecretEl) loginClientSecretEl.value = String(migState.salesforceLogin && migState.salesforceLogin.clientSecret || '');
  renderMigSalesforceLoginStatus();

  resetMigTransientUi();

  renderMigWizardSteps();
  renderMigSelectedObjects();
  renderMigPendingImportHint();
  renderMigImportSuggestions();
  if (migState.step === 2) renderMigFileAssignments();
  if (migState.step === 3) renderMigMappingObjectSelect();
  if (migState.step === 4) { renderMigDependencies(); renderMigDepSelects(); }
  if (migState.step === 5) renderMigOrderList();
  if (migState.step === 6) renderMigMissingFields();
  if (migState.step === 7) renderMigReview();
  renderWizardMetadata('mig-wizard-meta', migState, 'Neue Migration · noch nicht gespeichert');
  void loadEntityHistory('migration', migState.id || '', 'mig-history-list', 'mig-history-meta', 'Migration noch nicht gespeichert.');

  const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('migration-modal'));
  document.getElementById('migration-modal-title').textContent = migration ? 'Migration bearbeiten: ' + migration.name : 'Neue Migration';
  modal.show();
}


