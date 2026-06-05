// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-migration-helpers
// Source lines: 126–1310
// ──────────────────────────────────────────────────────────────────────

function getMigLoginUrlForEnvironment(environment) {
  return String(environment || 'sandbox') === 'production'
    ? 'https://login.salesforce.com'
    : 'https://test.salesforce.com';
}

function populateMigExistingInstanceOptions() {
  const select = document.getElementById('mig-existing-instance');
  const globalSelect = document.getElementById('instance-select');
  if (!select) {
    return;
  }

  const options = globalSelect
    ? Array.from(globalSelect.options).filter((option) => String(option.value || '').trim())
    : [];

  if (!options.length) {
    select.innerHTML = '<option value="">Keine bestehenden Instanzen konfiguriert</option>';
    select.value = '';
    migState.instanceId = '';
    return;
  }

  select.innerHTML = options.map((option) =>
    '<option value="' + esc(option.value) + '">' + esc(option.textContent || option.value) + '</option>'
  ).join('');

  const desiredInstanceId = String(migState.instanceId || state.instanceId || '').trim();
  const hasDesiredInstance = options.some((option) => String(option.value || '').trim() === desiredInstanceId);
  select.value = hasDesiredInstance ? desiredInstanceId : String(options[0].value || '').trim();
  migState.instanceId = String(select.value || '').trim();
}

function renderMigSalesforceLoginStatus() {
  const statusEl = document.getElementById('mig-login-status');
  const authorizeButton = document.getElementById('mig-login-authorize');
  const instanceSourceEl = document.getElementById('mig-instance-source');
  const existingInstanceWrap = document.getElementById('mig-existing-instance-wrap');
  const existingInstanceEl = document.getElementById('mig-existing-instance');
  const environmentWrap = document.getElementById('mig-login-environment-wrap');
  const authTypeWrap = document.getElementById('mig-login-auth-type-wrap');
  const loginUrlWrap = document.getElementById('mig-login-url-wrap');
  const statusWrap = document.getElementById('mig-login-status-wrap');
  const authorizeWrap = document.getElementById('mig-login-authorize-wrap');
  const authTypeEl = document.getElementById('mig-login-auth-type');
  const usernameWrap = document.getElementById('mig-login-username-wrap');
  const passwordWrap = document.getElementById('mig-login-password-wrap');
  const securityTokenWrap = document.getElementById('mig-login-security-token-wrap');
  const clientIdWrap = document.getElementById('mig-login-client-id-wrap');
  const clientSecretWrap = document.getElementById('mig-login-client-secret-wrap');
  if (!statusEl) {
    return;
  }

  const instanceSource = 'existing';
  if (instanceSourceEl) {
    instanceSourceEl.value = 'existing';
  }
  const isExistingInstanceMode = true;
  if (existingInstanceWrap) existingInstanceWrap.classList.toggle('d-none', !isExistingInstanceMode);
  if (environmentWrap) environmentWrap.classList.toggle('d-none', isExistingInstanceMode);
  if (authTypeWrap) authTypeWrap.classList.toggle('d-none', isExistingInstanceMode);
  if (loginUrlWrap) loginUrlWrap.classList.toggle('d-none', isExistingInstanceMode);
  if (authorizeWrap) authorizeWrap.classList.toggle('d-none', isExistingInstanceMode);
  if (isExistingInstanceMode) {
    populateMigExistingInstanceOptions();
    if (usernameWrap) usernameWrap.classList.add('d-none');
    if (passwordWrap) passwordWrap.classList.add('d-none');
    if (instanceSourceEl) instanceSourceEl.value = 'existing';
    if (clientSecretWrap) clientSecretWrap.classList.add('d-none');
    if (statusWrap) statusWrap.classList.remove('d-none');
    const selectedLabel = String(existingInstanceEl && existingInstanceEl.selectedOptions && existingInstanceEl.selectedOptions[0] && existingInstanceEl.selectedOptions[0].textContent || '').trim();
    if (migState.instanceId) {
      statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
      statusEl.textContent = 'Verwendet bestehende Instanz: ' + String(selectedLabel || migState.instanceId);
    } else {
      statusEl.className = 'small text-danger border rounded-3 px-3 py-2 bg-danger-subtle';
      statusEl.textContent = 'Keine bestehende Instanz ausgewählt.';
    }
    return;
  }

  const login = migState.salesforceLogin;
  const authType = String(authTypeEl && authTypeEl.value || login && login.authType || 'password');
  const isPasswordMode = authType === 'password';
  const isClientCredentialsMode = authType === 'client_credentials';
  if (usernameWrap) usernameWrap.classList.toggle('d-none', !isPasswordMode);
  if (passwordWrap) passwordWrap.classList.toggle('d-none', !isPasswordMode);
  if (securityTokenWrap) securityTokenWrap.classList.toggle('d-none', !isPasswordMode);
  if (clientIdWrap) clientIdWrap.classList.toggle('d-none', !isClientCredentialsMode);
  if (clientSecretWrap) clientSecretWrap.classList.toggle('d-none', !isClientCredentialsMode);
  if (!login) {
    statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
    statusEl.textContent = isPasswordMode
      ? 'Noch keine Salesforce-Zugangsdaten hinterlegt.'
      : (isClientCredentialsMode
        ? 'Noch keine Client-ID und kein Client-Secret hinterlegt.'
        : 'Noch keine Salesforce-Freigabe vorhanden.');
    if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login testen' : 'Mit Salesforce verbinden';
    return;
  }

  if (String(login.lastConnectionStatus || 'never') === 'connected') {
    const orgName = login.orgOverview && (login.orgOverview.organizationName || login.orgOverview.organizationId || login.orgOverview.instanceUrl) || login.instanceUrl || login.loginUrl;
    statusEl.className = 'small text-success border rounded-3 px-3 py-2 bg-success-subtle';
    statusEl.textContent = 'Verbunden mit ' + String(orgName || 'Salesforce') + (login.lastConnectedAt ? ' am ' + formatDate(login.lastConnectedAt, 'short') : '') + '.';
    if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login erneut testen' : 'Erneut mit Salesforce verbinden';
    return;
  }

  if (String(login.lastConnectionStatus || 'never') === 'error' && login.lastConnectionError) {
    statusEl.className = 'small text-danger border rounded-3 px-3 py-2 bg-danger-subtle';
    statusEl.textContent = 'Letzter Loginfehler: ' + String(login.lastConnectionError || 'Unbekannter Fehler');
    if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login erneut testen' : 'Salesforce Login erneut starten';
    return;
  }

  statusEl.className = 'small text-secondary border rounded-3 px-3 py-2 bg-light-subtle';
  statusEl.textContent = isPasswordMode
    ? 'Noch nicht verbunden. Hinterlege Benutzername und Passwort und teste dann den Login.'
    : (isClientCredentialsMode
      ? 'Noch nicht verbunden. Hinterlege Client ID und Client Secret und teste dann den Login.'
      : 'Noch nicht verbunden. Der Login erfolgt ueber die Salesforce-Login-Seite mit anschliessendem Allow.');
  if (authorizeButton) authorizeButton.textContent = isPasswordMode || isClientCredentialsMode ? 'Login testen' : 'Mit Salesforce verbinden';
}

function syncMigSalesforceLoginFromForm() {
  const existingInstanceEl = document.getElementById('mig-existing-instance');
  migState.instanceId = String(existingInstanceEl && existingInstanceEl.value || migState.instanceId || state.instanceId || '').trim();
    migState.salesforceLogin = null;
  renderMigSalesforceLoginStatus();
}

async function ensureMigRuntimeInstanceId() {
  syncMigSalesforceLoginFromForm();
  const instanceId = String(migState.instanceId || state.instanceId || '').trim();
  if (!instanceId) {
    throw new Error('Bitte zuerst eine bestehende Projektinstanz auswaehlen.');
  }
  return instanceId;
}

function getCsrfToken() {
  return String(document.querySelector('meta[name="sf-agent-csrf-token"]')?.getAttribute('content') || '').trim();
}

// Field edit toggle (pencil icon) and readonly protection
function createFieldEditToggle(input) {
  if (!input || input.dataset.__hasEditToggle) return;
  input.dataset.__hasEditToggle = '1';
  const wrap = document.createElement('div');
  wrap.className = 'field-edit-wrap position-relative';
  input.parentNode && input.parentNode.replaceChild(wrap, input);
  wrap.appendChild(input);
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'field-edit-toggle btn btn-sm btn-light position-absolute';
  btn.innerHTML = '<span class="bi bi-pencil" aria-hidden="true"></span>';
  btn.title = 'Bearbeiten';
  btn.style.right = '6px';
  btn.style.top = '6px';
  btn.addEventListener('click', (e) => {
    e.preventDefault();
    toggleFieldEditable(input, btn);
  });
  wrap.appendChild(btn);
  // initial state
  if (!input.hasAttribute('readonly')) {
    btn.classList.add('active');
  }
}

function toggleFieldEditable(input, btn) {
  const isReadOnly = input.hasAttribute('readonly');
  if (isReadOnly) {
    input.removeAttribute('readonly');
    input.classList.add('editing');
    btn.classList.add('active');
    input.focus();
  } else {
    // blur -> save event can be triggered by existing handlers
    input.setAttribute('readonly', '');
    input.classList.remove('editing');
    btn.classList.remove('active');
    try { input.dispatchEvent(new Event('change')); } catch (e) {}
  }
}

function attachFieldEditToggles(root = document) {
  const inputs = Array.from(root.querySelectorAll('input[readonly], textarea[readonly], select[readonly], input[data-protected], textarea[data-protected], select[data-protected]'));
  inputs.forEach((el) => createFieldEditToggle(el));
}

// Call once initially and after dynamic modal inserts
setTimeout(() => attachFieldEditToggles(), 200);


window.fetch = (input, options = {}) => {
  const request = input instanceof Request ? input : null;
  const method = String(options.method || (request ? request.method : 'GET')).toUpperCase();
  if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
    return nativeFetch(input, options);
  }

  const headers = new Headers(request ? request.headers : undefined);
  const optionHeaders = new Headers(options.headers || {});
  optionHeaders.forEach((value, key) => headers.set(key, value));
  const csrfToken = getCsrfToken();
  if (csrfToken && !headers.has('X-CSRF-Token')) {
    headers.set('X-CSRF-Token', csrfToken);
  }

  return nativeFetch(input, {
    ...options,
    headers
  });
};

function getMigImportDisplayName(fileName) {
  return String(fileName || '')
    .replace(/.[^.]+$/, '')
    .replace(/[._-]+/g, ' ')
    .replace(/s+/g, ' ')
    .trim();
}

function isMigPendingImportSelected(item) {
  return !!item && item.includeInMigration !== false;
}

function getMigSelectedPendingImports(imports) {
  return (Array.isArray(imports) ? imports : []).filter((item) => isMigPendingImportSelected(item));
}

function getMigPendingImportLabel(item) {
  const fileLabel = String(item && (item.fileName || (item.file && item.file.name) || item.sourceFileName) || 'Datei').trim() || 'Datei';
  const sheetName = String(item && (item.sheetName || (item.analysis && item.analysis.sheetName)) || '').trim();
  return sheetName ? (fileLabel + ' / ' + sheetName) : fileLabel;
}

function isSupportedMigrationImportFile(file) {
  const fileName = String(file && file.name ? file.name : '').toLowerCase();
  return ['.csv', '.txt', '.json', '.xlsx', '.xls'].some((extension) => fileName.endsWith(extension));
}

function getMigObjectDisplayName(obj) {
  const baseName = String((obj && (obj.salesforceObjectLabel || obj.salesforceObject)) || 'Objekt');
  const sameObjects = (migState.objects || []).filter((item) => item && item.salesforceObject === obj.salesforceObject);
  if (sameObjects.length <= 1) {
    return baseName;
  }

  const index = sameObjects.findIndex((item) => item.id === obj.id);
  return index >= 0 ? (baseName + ' #' + (index + 1)) : baseName;
}

function countMigUnassignedObjectsByApiName(objectApiName) {
  return (migState.objects || []).filter((obj) =>
    obj && obj.salesforceObject === objectApiName && !String(obj.filePath || '').trim()
  ).length;
}

function resolveMigTargetFieldApiName(fieldName, availableFields) {
  const rawName = String(fieldName || '').trim();
  if (!rawName) {
    return '';
  }

  const fieldNames = (Array.isArray(availableFields) ? availableFields : [])
    .map((entry) => typeof entry === 'string' ? entry : String(entry && entry.name ? entry.name : ''))
    .map((entry) => entry.trim())
    .filter(Boolean);
  const namesByLower = new Map(fieldNames.map((entry) => [entry.toLowerCase(), entry]));
  const exactMatch = namesByLower.get(rawName.toLowerCase());
  if (exactMatch) {
    return exactMatch;
  }

  if (!rawName.toLowerCase().endsWith('__c')) {
    const customFieldMatch = namesByLower.get((rawName + '__c').toLowerCase());
    if (customFieldMatch) {
      return customFieldMatch;
    }
  }

  return rawName;
}

function isMigMappingTargetFieldVisible(field, selectedValue) {
  const name = String(field?.name || '').trim();
  if (!name) {
    return false;
  }
  if (selectedValue && normalizeFieldKey(name) === normalizeFieldKey(selectedValue)) {
    return true;
  }

  if (field?.createable === true || field?.updateable === true || field?.isExternalId === true) {
    return true;
  }

  const lowerName = name.toLowerCase();
  return ![
    'id',
    'createddate',
    'createdbyid',
    'lastmodifieddate',
    'lastmodifiedbyid',
    'systemmodstamp',
    'lastvieweddate',
    'lastreferenceddate',
    'isdeleted'
  ].includes(lowerName);
}

function getMigMappingTargetOptions(availableFields, selectedValue) {
  const selected = String(selectedValue || '').trim();
  const fields = Array.isArray(availableFields) ? availableFields : [];
  const visibleFields = fields.filter((field) => isMigMappingTargetFieldVisible(field, selected));
  const hasSelected = selected && visibleFields.some((field) => String(field?.name || '').trim() === selected);

  return '<option value=""' + (!selected ? ' selected' : '') + '>Zielfeld wählen</option>' +
    visibleFields.map((field) => {
      const name = String(field?.name || '').trim();
      const label = String(field?.label || '').trim();
      const display = label && label !== name ? label + ' - ' + name : name;
      const meta = [
        field?.requiredOnCreate === true ? 'Pflicht' : '',
        field?.isExternalId === true ? 'External ID' : ''
      ].filter(Boolean).join(', ');
      return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(display + (meta ? ' (' + meta + ')' : '')) + '</option>';
    }).join('') +
    '<option value="__manual__"' + (!hasSelected && selected ? ' selected' : '') + '>Manuell eingeben…</option>';
}

async function autoPopulateMigFieldMappings(obj, sfFields) {
  if (!obj || !Array.isArray(obj.fileColumns) || !obj.fileColumns.length) {
    return 0;
  }

  if (!obj.fieldMappings) {
    obj.fieldMappings = [];
  }

  const generatedMappings = await generateSalesforceMappings(
    obj.fileColumns.map((column) => ({ name: String(column || '').trim(), type: 'string' })),
    Array.isArray(sfFields) ? sfFields : [],
    {
      targetObjectApiName: obj.salesforceObject,
      profile: getMigrationMappingAssistantProfile(obj.id, obj.salesforceObject)
    }
  );

  let added = 0;
  generatedMappings.forEach((generated) => {
    const sourceName = String(generated?.sourceField || '').trim();
    if (!sourceName) {
      return;
    }

    const existing = obj.fieldMappings.find((mapping) => String(mapping?.sourceColumn || '').trim() === sourceName);
    if (existing && String(existing.targetField || '').trim()) {
      return;
    }

    const nextEntry = {
      ...(existing || {}),
      sourceColumn: sourceName,
      targetField: String(generated.targetField || ''),
      targetFieldLabel: String(generated.targetFieldLabel || generated.targetField || ''),
      targetFieldType: String(generated.targetFieldType || ''),
      targetType: generated.targetType || existing?.targetType,
      transformFunction: String(existing?.transformFunction || generated.transformFunction || 'NONE'),
      transformExpression: String(existing?.transformExpression || generated.transformExpression || ''),
      lookupEnabled: existing?.lookupEnabled === true ? true : generated.lookupEnabled === true,
      lookupObject: String(existing?.lookupObject || generated.lookupObject || ''),
      lookupField: String(existing?.lookupField || generated.lookupField || ''),
      picklistMappings: Array.isArray(existing?.picklistMappings) ? existing.picklistMappings : (Array.isArray(generated.picklistMappings) ? generated.picklistMappings : [])
    };

    if (existing) {
      Object.assign(existing, nextEntry);
    } else {
      obj.fieldMappings.push(nextEntry);
    }
    added += 1;
  });

  return added;
}

function autoSelectMigExternalIdField(obj, sfFields) {
  if (!obj || obj.operation !== 'upsert') {
    return false;
  }

  const mappedExternalIdFields = (Array.isArray(obj.fieldMappings) ? obj.fieldMappings : [])
    .map((mapping) => String(mapping?.targetField || '').trim())
    .filter(Boolean)
    .map((targetField) => resolveMigTargetFieldApiName(targetField, sfFields))
    .filter(Boolean)
    .filter((targetField, index, entries) => entries.indexOf(targetField) === index)
    .filter((targetField) => (Array.isArray(sfFields) ? sfFields : []).some((field) =>
      String(field?.name || '').trim().toLowerCase() === targetField.toLowerCase() && field?.isExternalId === true
    ));

  if (mappedExternalIdFields.length === 1) {
    const nextExternalIdField = mappedExternalIdFields[0];
    if (String(obj.externalIdField || '').trim() !== nextExternalIdField) {
      obj.externalIdField = nextExternalIdField;
      return true;
    }
    return false;
  }

  if (mappedExternalIdFields.length === 0 && String(obj.externalIdField || '').trim()) {
    obj.externalIdField = '';
    return true;
  }

  return false;
}

function sanitizeMigFieldMappings(fieldMappings) {
  return (Array.isArray(fieldMappings) ? fieldMappings : []).map((mapping) => {
    const normalizedMapping = mapping ? JSON.parse(JSON.stringify(mapping)) : {};
    delete normalizedMapping._isMissing;
    return normalizedMapping;
  });
}

function sanitizeMigObjects(objects) {
  return (Array.isArray(objects) ? objects : []).map((obj) => {
    const normalizedObject = obj ? JSON.parse(JSON.stringify(obj)) : {};
    delete normalizedObject.failedPreviewRecords;
    delete normalizedObject.failedPreviewLoadedFor;
    normalizedObject.fieldMappings = sanitizeMigFieldMappings(normalizedObject.fieldMappings);
    return normalizedObject;
  });
}

async function loadMigExternalIdOptions(obj) {
  if (!obj || !obj.salesforceObject) {
    return [];
  }

  if (Array.isArray(obj._externalIdFields) && obj._externalIdFields.length) {
    return obj._externalIdFields;
  }

  try {
    const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
    if (!res.ok) {
      obj._externalIdFields = [];
      return [];
    }
    const fields = await res.json();
    obj._externalIdFields = (Array.isArray(fields) ? fields : []).filter((field) => field && field.isExternalId === true);
    return obj._externalIdFields;
  } catch {
    obj._externalIdFields = [];
    return [];
  }
}

function collectMigMissingFieldMappings() {
  const missing = [];
  for (const obj of migState.objects) {
    const existingFieldNames = Array.from(new Set([
      ...(obj._existingFieldNames || [])
    ].map((name) => String(name).trim()).filter(Boolean)));
    const existingFieldNamesSet = new Set(existingFieldNames.map((name) => name.toLowerCase()));
    for (const mapping of (obj.fieldMappings || [])) {
      const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, existingFieldNames);
      if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
        mapping.targetField = resolvedTargetField;
      }
      const isMissing = !!resolvedTargetField && !existingFieldNamesSet.has(resolvedTargetField.toLowerCase());
      mapping._isMissing = isMissing;
      if (isMissing) {
        missing.push({ obj, mapping });
      }
    }
  }
  return missing;
}

async function collectMigMissingFieldMappingsLive() {
  const missing = [];
  for (const obj of migState.objects) {
    const fields = await requestJson('/api/salesforce/object-fields?object=' + encodeURIComponent(obj.salesforceObject));
    const existingFieldNames = Array.from(new Set((Array.isArray(fields) ? fields : [])
      .map((field) => String(field && field.name ? field.name : '').trim())
      .filter(Boolean)
      .map((name) => name.toLowerCase())));
    const existingFieldNamesSet = new Set(existingFieldNames);
    obj._existingFieldNames = existingFieldNames.slice();

    for (const mapping of (obj.fieldMappings || [])) {
      const resolvedTargetField = resolveMigTargetFieldApiName(mapping.targetField, existingFieldNames);
      if (resolvedTargetField && resolvedTargetField !== mapping.targetField) {
        mapping.targetField = resolvedTargetField;
      }
      const isMissing = !!resolvedTargetField && !existingFieldNamesSet.has(resolvedTargetField.toLowerCase());
      mapping._isMissing = isMissing;
      if (isMissing) {
        missing.push({ obj, mapping });
      }
    }
  }
  return missing;
}

function inferMigFieldCreationType(mapping) {
  const explicitPicklistValues = Array.isArray(mapping?.picklistValues) ? mapping.picklistValues.filter(Boolean) : [];
  if (explicitPicklistValues.length) {
    return 'Picklist';
  }

  const normalizedType = String(mapping?.targetFieldType || '').trim().toLowerCase();
  if (normalizedType === 'url') return 'Url';
  if (normalizedType === 'date') return 'Date';
  if (normalizedType === 'datetime') return 'DateTime';
  if (normalizedType === 'boolean') return 'Checkbox';
  if (normalizedType === 'email') return 'Email';
  if (normalizedType === 'phone') return 'Phone';
  if (normalizedType === 'currency') return 'Currency';
  if (normalizedType === 'percent') return 'Percent';
  if (normalizedType === 'double' || normalizedType === 'int' || normalizedType === 'integer' || normalizedType === 'number') return 'Number';

  const targetFieldName = String(mapping?.targetField || '').trim().toLowerCase();
  if (targetFieldName.includes('currency')) return 'Currency';
  if (targetFieldName.includes('percent')) return 'Percent';
  if (targetFieldName.includes('email')) return 'Email';
  if (targetFieldName.includes('phone') || targetFieldName.includes('mobile')) return 'Phone';
  if (targetFieldName.includes('date')) return 'Date';
  if (targetFieldName.includes('url') || targetFieldName.includes('website')) return 'Url';

  return 'Text';
}

async function createMigMissingField(obj, mapping, fieldType, picklistValues) {
  const result = await requestJson('/api/salesforce/create-field', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      objectApiName: obj.salesforceObject,
      fieldApiName: mapping.targetField,
      fieldType,
      picklistValues,
      instanceId: state.instanceId
    })
  });

  const fullFieldName = String(result && result.fullName ? result.fullName : '').split('.').pop()
    || (String(mapping.targetField).endsWith('__c') ? String(mapping.targetField) : String(mapping.targetField) + '__c');

  obj.confirmedSalesforceFields = Array.from(new Set([...(obj.confirmedSalesforceFields || []), fullFieldName]));
  obj._existingFieldNames = Array.from(new Set([...(obj._existingFieldNames || []), String(fullFieldName).toLowerCase()]));
  mapping.targetField = fullFieldName;
  mapping.targetFieldLabel = fullFieldName;
  mapping._isMissing = false;

  return { fullFieldName, result };
}

async function autoCreateMigMissingFields() {
  const missing = await collectMigMissingFieldMappingsLive();
  if (!missing.length) {
    return [];
  }

  const createdFields = [];
  for (const item of missing) {
    const fieldType = inferMigFieldCreationType(item.mapping);
    const picklistValues = fieldType === 'Picklist'
      ? (Array.isArray(item.mapping.picklistValues) ? item.mapping.picklistValues.map((value) => String(value || '').trim()).filter(Boolean) : [])
      : [];

    if (fieldType === 'Picklist' && !picklistValues.length) {
      throw new Error('Feld ' + item.mapping.targetField + ' kann nicht automatisch als Picklist angelegt werden, weil keine Werte hinterlegt sind.');
    }

    const created = await createMigMissingField(item.obj, item.mapping, fieldType, picklistValues);
    createdFields.push({
      objectApiName: item.obj.salesforceObject,
      fieldName: created.fullFieldName,
      action: created.result && created.result.action ? created.result.action : 'created'
    });
  }

  return createdFields;
}

function resetMigTransientUi() {
  stopMigRunProgressPolling();
  const progressEl = document.getElementById('mig-run-progress');
  const resultEl = document.getElementById('mig-run-result');
  const stepsEl = document.getElementById('mig-run-steps');
  const createFieldsResultEl = document.getElementById('mig-create-fields-result');
  const progressTitleEl = document.getElementById('mig-run-status-title');
  const spinnerEl = document.getElementById('mig-run-status-spinner');

  if (progressEl) {
    progressEl.classList.add('d-none');
  }
  if (spinnerEl) {
    spinnerEl.classList.remove('d-none');
  }
  if (progressTitleEl) {
    progressTitleEl.textContent = 'Migration läuft...';
  }
  if (resultEl) {
    resultEl.classList.add('d-none');
    resultEl.innerHTML = '';
  }
  if (stepsEl) {
    stepsEl.innerHTML = '';
  }
  if (createFieldsResultEl) {
    createFieldsResultEl.innerHTML = '';
  }
}

function stopMigRunProgressPolling() {
  if (migState.progressPollTimer) {
    clearTimeout(migState.progressPollTimer);
    migState.progressPollTimer = null;
  }
}

function getMigOrderedObjects() {
  const ordered = [...(migState.executionPlan || [])]
    .sort((a, b) => a.order - b.order)
    .map((step) => (migState.objects || []).find((obj) => obj.id === step.objectId))
    .filter(Boolean);

  (migState.objects || []).forEach((obj) => {
    if (!ordered.find((entry) => entry.id === obj.id)) {
      ordered.push(obj);
    }
  });

  return ordered;
}

async function pollMigRunProgress() {
  if (!migState.id) return;
  stopMigRunProgressPolling();
  const refresh = async () => {
    try {
      const res = await fetch('/api/migrations/' + encodeURIComponent(migState.id));
      if (res.ok) {
        const migration = await res.json();
        migState.status = String(migration?.status || migState.status || 'draft');
        migState.lastRunResult = migration?.lastRunResult || migState.lastRunResult;
        if (migState.step === migState.totalSteps) {
          renderMigRunProgress();
          renderMigRunResult();
        }
      }
    } catch { /* ignore polling errors */ }

    if (migState.status === 'running') {
      migState.progressPollTimer = setTimeout(refresh, 1000);
    } else {
      stopMigRunProgressPolling();
    }
  };
  await refresh();
}

function getMigPendingRecommendationCounts(imports) {
  return getMigSelectedPendingImports(imports).reduce((acc, item) => {
    const key = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
    if (!key) {
      return acc;
    }

    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

function createMigObject(name, label, options) {
  const allowDuplicate = !!(options && options.allowDuplicate);
  if (!allowDuplicate) {
    const existing = (migState.objects || []).find((obj) => obj && obj.salesforceObject === name);
    if (existing) {
      return existing;
    }
  }

  const migrationObject = {
    id: migUuidV4(),
    salesforceObject: name,
    salesforceObjectLabel: label || name,
    processingMode: 'sqlite',
    filePath: '',
    fileSheetName: '',
    availableSheetNames: [],
    fileColumns: [],
    fieldMappings: [],
    operation: 'insert'
  };
  migState.objects.push(migrationObject);
  return migrationObject;
}

function ensureMigObjectsForPendingImports(imports) {
  const groupedImports = (Array.isArray(imports) ? imports : []).reduce((acc, item) => {
    const objectApiName = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
    if (!objectApiName) {
      return acc;
    }

    if (!acc[objectApiName]) {
      acc[objectApiName] = {
        label: item.recommendedObjectLabel || objectApiName,
        items: []
      };
    }
    acc[objectApiName].items.push(item);
    return acc;
  }, {});

  let createdCount = 0;
  Object.keys(groupedImports).forEach((objectApiName) => {
    const group = groupedImports[objectApiName];
    const availableCount = countMigUnassignedObjectsByApiName(objectApiName);
    const missingCount = Math.max(0, group.items.length - availableCount);
    for (let index = 0; index < missingCount; index += 1) {
      createMigObject(objectApiName, group.label, { allowDuplicate: true });
      createdCount += 1;
    }
  });

  return createdCount;
}

function getPendingMigrationImportText() {
  const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
  const selectedImports = getMigSelectedPendingImports(pendingImports);
  if (!selectedImports.length) {
    return '';
  }

  const labels = selectedImports.map((item) => getMigPendingImportLabel(item)).join(', ');
  if (selectedImports.length === 1) {
    return 'Import-Datei vorgemerkt: ' + labels + '. Wähle genau ein Salesforce-Objekt aus, dann wird die Datei automatisch übernommen.';
  }

  return selectedImports.length + ' Importquellen vorgemerkt: ' + labels + '. Bitte nacheinander je Objekt zuordnen.';
}

function renderMigPendingImportHint() {
  const stepOneHint = document.getElementById('mig-pending-import-hint');
  const stepTwoHint = document.getElementById('mig-file-import-hint');
  const pendingText = getPendingMigrationImportText();

  if (stepOneHint) {
    stepOneHint.textContent = pendingText;
    stepOneHint.classList.toggle('d-none', !pendingText);
  }

  if (stepTwoHint) {
    stepTwoHint.textContent = pendingText;
    stepTwoHint.classList.toggle('d-none', !pendingText);
  }
}

function renderMigImportSuggestions() {
  const container = document.getElementById('mig-import-suggestions');
  if (!container) {
    return;
  }

  const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
  const selectedImports = getMigSelectedPendingImports(pendingImports);
  const recommendationCounts = getMigPendingRecommendationCounts(pendingImports);
  const recommendedImports = selectedImports.filter((item) => String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim());
  const needsAdditionalObjects = Object.keys(recommendationCounts).some((objectApiName) => recommendationCounts[objectApiName] > countMigUnassignedObjectsByApiName(objectApiName));
  if (!pendingImports.length) {
    container.classList.add('d-none');
    container.innerHTML = '';
    return;
  }
  container.classList.remove('d-none');
  container.innerHTML = '<div class="small fw-semibold mb-2">Objektvorschläge aus den Importdateien</div>' +
    (pendingImports.length !== selectedImports.length
      ? '<div class="small text-secondary mb-2">' + esc(String(selectedImports.length)) + ' von ' + esc(String(pendingImports.length)) + ' Importquellen sind aktuell ausgewählt.</div>'
      : '') +
    (recommendedImports.length
      ? '<div class="d-flex flex-wrap gap-2 mb-2">' +
          '<button type="button" class="btn btn-sm btn-primary" data-mig-create-all-suggested>Empfohlene Objekte gesammelt anlegen</button>' +
          (needsAdditionalObjects
            ? '<span class="small text-secondary align-self-center">Mehrere Dateien zeigen teils auf dasselbe Zielobjekt. Die Sammelanlage legt dafür getrennte Import-Slots an.</span>'
            : '<span class="small text-secondary align-self-center">Passende Zielobjekte können direkt gesammelt angelegt und zugeordnet werden.</span>') +
        '</div>'
      : '') +
    pendingImports.map((item) => {
      const analysis = item && item.analysis && typeof item.analysis === 'object' ? item.analysis : null;
      const suggestions = Array.isArray(item && item.suggestions) ? item.suggestions : [];
      const recommendedName = String(item && item.recommendedObjectApiName ? item.recommendedObjectApiName : '').trim();
      const isSelected = isMigPendingImportSelected(item);
      const duplicateRecommendationCount = recommendedName ? (recommendationCounts[recommendedName] || 0) : 0;
      const summaryBits = [];
      if (analysis && analysis.format) summaryBits.push('Format: ' + String(analysis.format).toUpperCase());
      if (analysis && analysis.sheetName) summaryBits.push('Mappe: ' + analysis.sheetName);
      if (analysis && typeof analysis.recordCount === 'number') summaryBits.push('Datensaetze: ' + analysis.recordCount);
      if (analysis && Array.isArray(analysis.headers) && analysis.headers.length) {
        summaryBits.push('Felder: ' + analysis.headers.slice(0, 6).join(', ') + (analysis.headers.length > 6 ? ' …' : ''));
      }

      return '<div class="alert ' + (isSelected ? 'alert-light' : 'alert-secondary') + ' border py-2 mb-2">' +
        '<div class="d-flex justify-content-between align-items-start gap-3 mb-1">' +
          '<div class="small fw-semibold">' + esc(getMigPendingImportLabel(item)) + '</div>' +
          '<div class="form-check form-switch m-0">' +
            '<input class="form-check-input" type="checkbox" role="switch" data-mig-import-toggle="' + esc(item.id || item.fileName || '') + '"' + (isSelected ? ' checked' : '') + '>' +
          '</div>' +
        '</div>' +
        (summaryBits.length ? '<div class="small text-secondary mb-2">' + esc(summaryBits.join(' | ')) + '</div>' : '') +
        (!isSelected
          ? '<div class="small text-secondary">Diese Mappe wird aktuell nicht in den Migrationsentwurf übernommen.</div>'
          : recommendedName
          ? '<div class="d-flex flex-wrap align-items-center gap-2 mb-2">' +
              '<span class="badge text-bg-primary">Empfohlen: ' + esc((item.recommendedObjectLabel || recommendedName) + ' (' + recommendedName + ')') + '</span>' +
              '<button type="button" class="btn btn-sm btn-outline-primary" data-mig-create-import="' + esc(item.id || item.fileName || '') + '">Objekt anlegen</button>' +
              (duplicateRecommendationCount > 1
                ? '<span class="small text-secondary">Diese Empfehlung tritt in ' + esc(String(duplicateRecommendationCount)) + ' Importquellen auf.</span>'
                : '') +
            '</div>'
          : '') +
        (isSelected && suggestions.length
          ? '<div class="d-flex flex-wrap gap-2">' +
              suggestions.map((suggestion) =>
                '<button type="button" class="btn btn-sm btn-outline-primary" data-mig-suggestion-import="' + esc(item.id || item.fileName || '') + '" data-mig-suggestion="' + esc(suggestion.objectApiName) + '" data-mig-suggestion-label="' + esc(suggestion.label || suggestion.objectApiName) + '">' +
                esc((suggestion.label || suggestion.objectApiName) + ' (' + suggestion.objectApiName + ')') +
                '</button>'
              ).join('') +
            '</div>' +
            '<div class="small text-secondary mt-2">' +
              suggestions.map((suggestion) => esc((suggestion.label || suggestion.objectApiName) + ': ' + (suggestion.reason || 'Heuristik'))).join(' | ') +
            '</div>'
          : (isSelected ? '<div class="small text-secondary">Keine eindeutige Objektempfehlung gefunden.</div>' : '')) +
      '</div>';
    }).join('');

  container.querySelectorAll('[data-mig-import-toggle]').forEach((input) => {
    input.addEventListener('change', () => {
      const importId = input.getAttribute('data-mig-import-toggle');
      const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
      if (!pendingImport) {
        return;
      }

      pendingImport.includeInMigration = !!input.checked;
      renderMigPendingImportHint();
      renderMigImportSuggestions();
    });
  });

  container.querySelectorAll('[data-mig-create-all-suggested]').forEach((button) => {
    button.addEventListener('click', () => {
      const createdCount = ensureMigObjectsForPendingImports(recommendedImports);
      renderMigSelectedObjects();
      if (createdCount > 0) {
        showToast(createdCount + ' Zielobjekte für vorgemerkte Importdateien angelegt.');
      }
      consumePendingMigrationImportIfPossible().catch((error) => {
        alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
      });
    });
  });

  container.querySelectorAll('[data-mig-create-import]').forEach((button) => {
    button.addEventListener('click', () => {
      const importId = button.getAttribute('data-mig-create-import');
      const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
      if (!pendingImport || !pendingImport.recommendedObjectApiName) {
        return;
      }

      ensureMigObjectsForPendingImports([pendingImport]);
      renderMigSelectedObjects();
      consumePendingMigrationImportIfPossible().catch((error) => {
        alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
      });
    });
  });

  container.querySelectorAll('[data-mig-suggestion]').forEach((button) => {
    button.addEventListener('click', () => {
      const name = button.getAttribute('data-mig-suggestion');
      const label = button.getAttribute('data-mig-suggestion-label') || name;
      const importId = button.getAttribute('data-mig-suggestion-import');
      if (!name) {
        return;
      }

      const pendingImport = (migState.pendingImports || []).find((item) => String(item.id || item.fileName || '') === String(importId || ''));
      if (pendingImport) {
        pendingImport.recommendedObjectApiName = name;
        pendingImport.recommendedObjectLabel = label;
        ensureMigObjectsForPendingImports([pendingImport]);
      }

      renderMigSelectedObjects();
      consumePendingMigrationImportIfPossible().catch((error) => {
        alert('Fehler: ' + (error instanceof Error ? error.message : String(error)));
      });
    });
  });
}

function renderMigFileSummary(obj) {
  const details = [];
  if (obj.fileFormat) details.push('Format: ' + obj.fileFormat.toUpperCase());
  if (obj.fileSheetName) details.push('Mappe: ' + obj.fileSheetName);
  if (typeof obj.fileRecordCount === 'number') details.push('Datensaetze: ' + obj.fileRecordCount);
  if (obj.fileCharset) details.push('Charset: ' + obj.fileCharset);
  if (obj.fileDelimiter) details.push('Trennzeichen: ' + (obj.fileDelimiter === '	' ? 'TAB' : obj.fileDelimiter));
  if (obj.fileTextQualifier) details.push('Textqualifier: ' + obj.fileTextQualifier);
  if (obj.processingMode) details.push('Verarbeitung: ' + (obj.processingMode === 'sqlite' ? 'SQLite-Staging' : 'Datei direkt'));
  if (obj.stagingMode) details.push('Staging: ' + String(obj.stagingMode).toUpperCase());
  if (obj.stagingStatus) details.push('Staging-Status: ' + formatMigStageStatus(obj.stagingStatus));
  if (obj.stagingDatabasePath) details.push('SQLite: ' + obj.stagingDatabasePath);
  if (obj.fileColumns && obj.fileColumns.length) details.push('Spalten: ' + obj.fileColumns.join(', '));
  const statusSummary = renderMigStatusSummaryText(obj);
  if (statusSummary) details.push(statusSummary);
  return details.length ? details.join(' | ') : '';
}

function formatMigStageStatus(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'ready') return 'Bereit';
  if (normalized === 'processing') return 'Verarbeitung';
  if (normalized === 'done') return 'Fertig';
  if (normalized === 'error') return 'Fehler';
  if (normalized === 'success') return 'Erfolgreich';
  if (normalized === 'mapping_error') return 'Mapping-Fehler';
  if (normalized === 'salesforce_error') return 'Salesforce-Fehler';
  if (normalized === 'pending') return 'Offen';
  return status || '-';
}

function renderMigStatusSummaryText(obj) {
  const summary = obj && obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : null;
  if (!summary) return '';
  const keys = Object.keys(summary).filter((key) => Number(summary[key] || 0) > 0);
  if (!keys.length) return '';
  return keys.sort().map((key) => formatMigStageStatus(key) + ': ' + summary[key]).join(', ');
}

function isMigServerPreview(obj) {
  return (obj?.processingMode || obj?.stagingMode || '') === 'sqlite';
}

function getMigLatestFailedStep(objectId) {
  const steps = Array.isArray(migState.lastRunResult && migState.lastRunResult.steps) ? migState.lastRunResult.steps : [];
  return steps.find((step) => step && step.objectId === objectId && step.failedRecordsId) || null;
}

async function loadMigLatestFailedPreview(obj) {
  const failedStep = getMigLatestFailedStep(obj.id);
  if (!failedStep || obj.failedPreviewLoadedFor === failedStep.failedRecordsId) return;
  const failedRes = await fetch('/api/migrations/' + encodeURIComponent(migState.id) + '/failed-records/' + encodeURIComponent(failedStep.failedRecordsId));
  if (!failedRes.ok) throw new Error('Fehler beim Laden der Fehlerdetails');
  const failedData = await failedRes.json();
  obj.failedPreviewRecords = Array.isArray(failedData.records) ? failedData.records.slice(0, 5) : [];
  obj.failedPreviewLoadedFor = failedStep.failedRecordsId;
}

function applyMigAnalysisData(obj, data) {
  obj.filePath = data.filePath || obj.filePath || '';
  obj.fileFormat = data.format || obj.fileFormat || 'csv';
  obj.fileSheetName = typeof data.sheetName === 'string' ? data.sheetName : (obj.fileSheetName || '');
  obj.availableSheetNames = Array.isArray(data.availableSheetNames) ? data.availableSheetNames.slice() : (obj.availableSheetNames || []);
  obj.fileCharset = data.charset || obj.fileCharset || 'utf8';
  obj.fileDelimiter = data.delimiter || obj.fileDelimiter || ';';
  obj.fileTextQualifier = data.textQualifier || obj.fileTextQualifier || '"';
  obj.fileRecordCount = typeof data.recordCount === 'number' ? data.recordCount : obj.fileRecordCount;
  obj.fileColumns = data.fields || [];
  obj.previewRows = Array.isArray(data.rows) ? data.rows.slice(0, 10) : [];
  obj.processingMode = data.processingMode || obj.processingMode || 'sqlite';
  obj.stagingMode = data.stagingMode || obj.stagingMode || '';
  obj.stagingDatabasePath = data.stagingDatabasePath || obj.stagingDatabasePath || '';
  obj.stagingImportedAt = data.stagingImportedAt || obj.stagingImportedAt;
  obj.stagingStatus = data.stagingStatus || obj.stagingStatus || '';
  obj.previewOffset = typeof data.previewOffset === 'number' ? data.previewOffset : (obj.previewOffset || 0);
  obj.previewLimit = typeof data.previewLimit === 'number' ? data.previewLimit : (obj.previewLimit || 10);
  obj.filteredRecordCount = typeof data.filteredRecordCount === 'number' ? data.filteredRecordCount : (obj.filteredRecordCount ?? obj.fileRecordCount ?? 0);
  obj.previewFilter = typeof data.previewFilter === 'string' ? data.previewFilter : (obj.previewFilter || '');
  obj.previewStatusFilter = typeof data.previewStatusFilter === 'string' ? data.previewStatusFilter : (obj.previewStatusFilter || '');
  obj.statusSummary = data.statusSummary || obj.statusSummary || {};
}

async function uploadMigrationObjectFile(obj, file) {
  if (!obj || !file) {
    return null;
  }

  const contentBase64 = await fileToBase64(file);
  const res = await fetch('/api/migrations/upload-file', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      migrationId: migState.id,
      objectId: obj.id,
      fileName: file.name,
      contentBase64,
      sheetName: obj.fileSheetName,
      charset: obj.fileCharset,
      delimiter: obj.fileDelimiter,
      textQualifier: obj.fileTextQualifier
    })
  });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data.error || 'Datei konnte nicht hochgeladen werden');
  }

  applyMigAnalysisData(obj, data);
  return data;
}

async function consumePendingMigrationImportIfPossible() {
  const pendingImports = Array.isArray(migState.pendingImports) ? migState.pendingImports : [];
  const selectedPendingImports = getMigSelectedPendingImports(pendingImports);
  if (!selectedPendingImports.length || migState.pendingImportInProgress) {
    renderMigPendingImportHint();
    return false;
  }
  migState.pendingImportInProgress = true;

  try {
    await migSave();

    let hasProgress = false;
    const remainingImports = [];

    for (const pendingImport of pendingImports) {
      if (!isMigPendingImportSelected(pendingImport)) {
        remainingImports.push(pendingImport);
        continue;
      }

      const unassignedObjects = (migState.objects || []).filter((obj) => obj && !String(obj.filePath || '').trim());
      let targetObject = null;

      if (pendingImport.recommendedObjectApiName) {
        targetObject = unassignedObjects.find((obj) => obj.salesforceObject === pendingImport.recommendedObjectApiName) || null;
      }

      if (!targetObject && selectedPendingImports.length === 1 && unassignedObjects.length === 1) {
        targetObject = unassignedObjects[0];
      }

      if (!targetObject) {
        remainingImports.push(pendingImport);
        continue;
      }

      targetObject.fileSheetName = pendingImport.sheetName || targetObject.fileSheetName || '';
      await uploadMigrationObjectFile(targetObject, pendingImport.file);
      hasProgress = true;
      showToast('Importquelle ' + getMigPendingImportLabel(pendingImport) + ' wurde dem Objekt ' + targetObject.salesforceObject + ' zugeordnet.');
    }

    if (hasProgress) {
      migState.pendingImports = remainingImports;
      await migSave();
      renderMigSelectedObjects();
      renderMigFileAssignments();
      renderMigMappingObjectSelect();
    }

    return hasProgress;
  } finally {
    migState.pendingImportInProgress = false;
    renderMigPendingImportHint();
  }
}

async function loadMigObjectPreview(obj, offset, limit) {
  const previewOffset = Math.max(0, Number(offset || 0) || 0);
  const previewLimit = Math.max(1, Math.min(100, Number(limit || obj.previewLimit || 10) || 10));
  const previewFilter = String(obj.previewFilter || '').trim();
  const previewStatusFilter = String(obj.previewStatusFilter || '').trim();
  const res = await fetch(
    '/api/migrations/' + encodeURIComponent(migState.id) + '/analyze-file/' + encodeURIComponent(obj.id) +
    '?offset=' + encodeURIComponent(String(previewOffset)) +
    '&limit=' + encodeURIComponent(String(previewLimit)) +
    '&filter=' + encodeURIComponent(previewFilter) +
    '&status=' + encodeURIComponent(previewStatusFilter)
  );
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  applyMigAnalysisData(obj, data);
  return data;
}

function renderMigPreviewTable(obj) {
  if (!obj.previewRows || !obj.previewRows.length || !obj.fileColumns || !obj.fileColumns.length) {
    return '';
  }

  const previewOffset = Math.max(0, Number(obj.previewOffset || 0) || 0);
  const previewLimit = Math.max(1, Number(obj.previewLimit || 10) || 10);
  const serverPreview = isMigServerPreview(obj);
  const totalRows = Math.max(0, Number(obj.fileRecordCount || 0) || 0);
  const summary = obj.statusSummary && typeof obj.statusSummary === 'object' ? obj.statusSummary : {};
  const summaryKeys = Object.keys(summary).filter((key) => Number(summary[key] || 0) > 0);
  const filterValue = String(obj.previewFilter || '');
  const statusFilterValue = String(obj.previewStatusFilter || '');
  const filteredRows = serverPreview
    ? obj.previewRows
    : String(filterValue).toLowerCase()
    ? obj.previewRows.filter((row) => obj.fileColumns.some((column) => String(row[column] ?? '').toLowerCase().includes(filterValue)))
    : obj.previewRows;
  const filteredTotal = serverPreview
    ? Math.max(0, Number(obj.filteredRecordCount ?? totalRows) || 0)
    : (filterValue ? filteredRows.length : totalRows);
  const fromRow = filteredTotal > 0 ? previewOffset + 1 : 0;
  const toRow = filteredTotal > 0 ? Math.min(previewOffset + filteredRows.length, filteredTotal) : filteredRows.length;
  const failedStep = getMigLatestFailedStep(obj.id);
  const failedPreviewRecords = Array.isArray(obj.failedPreviewRecords) ? obj.failedPreviewRecords : [];
  const previewLabel = serverPreview && (filterValue || statusFilterValue)
    ? 'Vorschau (' + fromRow + ' - ' + toRow + ' von ' + filteredTotal + ' Treffer' + (filteredTotal !== totalRows ? ', ' + totalRows + ' gesamt' : '') + ')'
    : 'Vorschau (' + fromRow + ' - ' + toRow + ' von ' + totalRows + ')';

  return '<div class="small text-secondary mt-2">' +
    '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
      '<span>' + esc(previewLabel) + '</span>' +
      '<span class="d-flex gap-1 flex-wrap">' +
        (summaryKeys.length
          ? summaryKeys.map((key) => '<span class="badge text-bg-light border">' + esc(formatMigStageStatus(key) + ': ' + String(summary[key])) + '</span>').join('')
          : '<span class="badge text-bg-light border">' + esc(formatMigStageStatus(obj.stagingStatus || 'ready')) + '</span>') +
      '</span>' +
    '</div>' +
    '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
      '<span class="small">Seitenlaenge: ' + previewLimit + '</span>' +
      '<input type="search" class="form-control form-control-sm" style="max-width: 240px" placeholder="Vorschau filtern" value="' + esc(obj.previewFilter || '') + '" data-preview-filter="' + esc(obj.id) + '" />' +
      (serverPreview
        ? '<select class="form-select form-select-sm" style="max-width: 220px" data-preview-status-filter="' + esc(obj.id) + '">' +
            '<option value="">Alle Status</option>' +
            '<option value="pending"' + (statusFilterValue === 'pending' ? ' selected' : '') + '>Offen</option>' +
            '<option value="success"' + (statusFilterValue === 'success' ? ' selected' : '') + '>Erfolg</option>' +
            '<option value="mapping_error"' + (statusFilterValue === 'mapping_error' ? ' selected' : '') + '>Mapping-Fehler</option>' +
            '<option value="salesforce_error"' + (statusFilterValue === 'salesforce_error' ? ' selected' : '') + '>Salesforce-Fehler</option>' +
          '</select>'
        : '') +
      '<div class="btn-group btn-group-sm">' +
        '<button type="button" class="btn btn-outline-secondary" data-preview-prev="' + esc(obj.id) + '"' + (previewOffset <= 0 ? ' disabled' : '') + '>Zurück</button>' +
        '<button type="button" class="btn btn-outline-secondary" data-preview-next="' + esc(obj.id) + '"' + (previewOffset + previewLimit >= filteredTotal ? ' disabled' : '') + '>Weiter</button>' +
      '</div>' +
    '</div>' +
    '<table class="table table-sm table-bordered"><thead><tr>' +
      obj.fileColumns.map((c) => '<th class="small">' + esc(c) + '</th>').join('') +
    '</tr></thead><tbody>' +
      (filteredRows.length
        ? filteredRows.map((row) => '<tr>' + obj.fileColumns.map((c) => '<td class="small">' + esc(String(row[c] ?? '')) + '</td>').join('') + '</tr>').join('')
        : '<tr><td colspan="' + obj.fileColumns.length + '" class="text-secondary">Keine Datensätze für den aktuellen Filter.</td></tr>') +
    '</tbody></table>' +
    (failedStep
      ? '<div class="mt-3">' +
          '<div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-2">' +
            '<strong>Letzte Fehlerzeilen aus dem letzten Lauf</strong>' +
            '<span class="badge bg-danger">' + esc(String(failedStep.recordsFailed || failedPreviewRecords.length || 0)) + ' Fehler</span>' +
          '</div>' +
          (failedPreviewRecords.length
            ? '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr><th>Zeile</th><th>Typ</th><th>Fehler</th><th>Vorschau</th></tr></thead><tbody>' +
              failedPreviewRecords.map((record) => '<tr>' +
                '<td>' + esc(String(record.rowIndex || '')) + '</td>' +
                '<td><span class="badge bg-' + (record.errorType === 'salesforce' ? 'warning' : 'danger') + '">' + esc(String(record.errorType || 'mapping')) + '</span></td>' +
                '<td class="small text-danger">' + esc(String(record.error || '')) + '</td>' +
                '<td class="small">' + esc(Object.entries(record.sourceRecord || {}).slice(0, 3).map(([key, value]) => key + ': ' + String(value ?? '')).join(' | ')) + '</td>' +
              '</tr>').join('') + '</tbody></table></div>'
            : '<div class="text-secondary small">Fehlerdetails werden geladen oder sind nicht mehr verfügbar.</div>') +
        '</div>'
      : '') +
  '</div>';
}

