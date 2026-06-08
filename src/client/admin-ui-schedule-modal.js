// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-schedule-modal
// Source lines: 10614–12147
// ──────────────────────────────────────────────────────────────────────

async function openScheduleModal(scheduleId, templateDraft, options) {
  const modalOptions = options && typeof options === 'object' ? options : {};
  const entry = scheduleId
    ? state.schedules.find((item) => item.id === scheduleId)
    : (templateDraft || null);
  if (!state.scheduleOptions || !Array.isArray(state.scheduleOptions.objectNames) || !state.scheduleOptions.objectNames.length) {
    await loadScheduleOptions();
  }

  document.getElementById('sch-id').value = entry?.id || '';
  renderWizardMetadata('sch-wizard-meta', entry, 'Neuer Scheduler · noch nicht gespeichert');
  document.getElementById('sch-name').value = entry?.name || '';
  renderScheduleConnectorOptions(entry?.connectorId || '');
  renderScheduleParentOptions(entry?.id || '', entry?.parentScheduleId || '');
  document.getElementById('sch-inherit-parent-timing').checked = !!entry?.inheritTimingFromParent;
  renderSelectOptions('sch-source-system', state.scheduleOptions.sourceSystems, entry?.sourceSystem || '');
  renderSelectOptions('sch-target-system', state.scheduleOptions.targetSystems, entry?.targetSystem || '');
  renderSelectOptions('sch-direction', state.scheduleOptions.directions, entry?.direction || '');
  document.getElementById('sch-source-type').value = entry?.sourceType || '';
  document.getElementById('sch-target-type').value = entry?.targetType || '';
  applyOperationOptions(entry?.operation || '');
  document.getElementById('sch-batch-size').value = entry?.batchSize || 100;
  document.getElementById('sch-next-run').value = isoToLocalDateTimeInput(entry?.nextRunAt);
  document.getElementById('sch-last-run').value = isoToLocalDateTimeInput(entry?.lastRunAt);
  document.getElementById('sch-active').checked = entry ? !!entry.active : true;
  const parsedSourceDefinition = parseScheduleSourceDefinition(entry?.sourceType || '', entry?.sourceDefinition || '');
  document.getElementById('sch-source-definition').value = parsedSourceDefinition.queryText || '';
  document.getElementById('sch-source-relative-directory').value = parsedSourceDefinition.relativeDirectory || '';
  document.getElementById('sch-source-archive-relative-directory').value = parsedSourceDefinition.archiveRelativeDirectory || '';
  document.getElementById('sch-source-delta-strategy').value = parsedSourceDefinition.deltaStrategy || '';
  document.getElementById('sch-source-delta-field').value = parsedSourceDefinition.deltaField || '';
  document.getElementById('sch-source-after-export').value = parsedSourceDefinition.afterExportText || '';
  document.getElementById('sch-source-delta-current').value = String(entry?.currentDeltaCheckpoint || '');
  document.getElementById('sch-source-delta-record-id').value = String(entry?.currentDeltaRecordId || '');
  const parsedTargetDefinition = parseScheduleTargetDefinition(entry?.targetType || '', entry?.targetDefinition || '');
  document.getElementById('sch-target-definition').value = parsedTargetDefinition.editorText || '';
  document.getElementById('sch-pricebook2id').value = '';
  document.getElementById('sch-missing-product-strategy').value = 'error';
  document.getElementById('sch-target-relative-directory').value = parsedTargetDefinition.relativeDirectory || '';
  document.getElementById('sch-target-archive-relative-directory').value = parsedTargetDefinition.archiveRelativeDirectory || '';
  document.getElementById('sch-target-file-name').value = parsedTargetDefinition.fileName || '';
  document.getElementById('sch-target-file-charset').value = parsedTargetDefinition.charset || 'utf8';
  document.getElementById('sch-target-file-delimiter').value = parsedTargetDefinition.delimiter || ';';
  document.getElementById('sch-target-file-text-qualifier').value = parsedTargetDefinition.textQualifier || '"';
  document.getElementById('sch-target-file-sheet-name').value = parsedTargetDefinition.sheetName || '';
  const initialTargetFileName = String(document.getElementById('sch-target-file-name').value || '').trim();
  if (initialTargetFileName && isDefaultScheduleTargetFileName(initialTargetFileName)) {
    state.scheduleTargetFileNameDirty = false;
    state.scheduleTargetFileNameLastAuto = initialTargetFileName;
  } else {
    state.scheduleTargetFileNameDirty = !!initialTargetFileName;
    state.scheduleTargetFileNameLastAuto = '';
  }
  state.rawMappingEditorDirty = false;
  state.mappingFieldsLoadSeq = Number(state.mappingFieldsLoadSeq || 0) + 1;
  state.targetObjectsLoadSeq = Number(state.targetObjectsLoadSeq || 0) + 1;
  state.targetFieldsLoadSeq = Number(state.targetFieldsLoadSeq || 0) + 1;
  state.mappingFields = [];
  state.sourcePreviewRows = [];
  state.targetFields = [];
  state.hasIncompatibleScheduleMappings = false;
  state.schedulerLookupObjects = [];
  state.schedulerLookupObjectsLoaded = false;
  state.schedulerLookupObjectsLoadPromise = null;
  state.schedulerLookupExternalIdFieldsByObject = {};
  state.schedulerLookupExternalIdFieldPromises = {};
  const sourceFieldsBody = document.getElementById('sch-mapping-source-fields');
  if (sourceFieldsBody) {
    sourceFieldsBody.innerHTML = '<tr><td colspan="2" class="text-secondary">Quellfelder werden geladen.</td></tr>';
  }
  document.getElementById('sch-mapping').value = entry?.mappingDefinition || '';
  hydrateMappingRulesFromDefinition();
  await syncSchedulerExternalIdUi();
  state.customObjectFieldOverrides = {};
  setCreateObjectStatus('Bereit.', 'neutral');
  document.getElementById('sch-timing-start').value = new Date().toISOString().slice(0, 10);
  document.getElementById('sch-timing-time').value = '09:00';
  document.getElementById('sch-timing-interval').value = '2';
  
  // Load timing definition if available
  let timingData = { days: [], intervalMinutes: 2, startTime: '09:00' };
  if (entry?.timingDefinition) {
    try {
      timingData = JSON.parse(entry.timingDefinition);
    } catch (e) {
      console.warn('Failed to parse timing definition:', e);
    }
  }
  
  // Restore weekday checkboxes
  document.querySelectorAll('#sch-weekdays input').forEach((input) => {
    const dayValue = Number(input.value);
    input.checked = timingData.days && timingData.days.includes(dayValue);
  });
  
  // Restore timing values
  if (timingData.startTime) {
    document.getElementById('sch-timing-time').value = timingData.startTime;
  }
  if (timingData.intervalMinutes) {
    document.getElementById('sch-timing-interval').value = String(timingData.intervalMinutes);
  }
  updateWeekdayChips();
  document.getElementById('sch-timing-preview').textContent = entry?.nextRunAt
    ? 'Aktueller nächster Lauf: ' + new Date(entry.nextRunAt).toLocaleString('de-DE')
    : 'Noch keine Zeitsteuerung berechnet.';
  updateTimingInheritanceUi();
  document.getElementById('sch-source-test-status').textContent = 'Es werden bis zu 10 Datensätze angezeigt.';
  renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', []);
  clearModalError();
  const initialConnectorId = String(entry?.connectorId || '').trim();
  if (!String(entry?.sourceSystem || '').trim()) {
    applyScheduleSourceSystemFromConnector(initialConnectorId, { force: true });
  }
  if (!String(entry?.sourceType || '').trim()) {
    applyScheduleSourceTypeFromConnector(initialConnectorId, { force: true });
  }
  applyScheduleSourceFieldPolicy(initialConnectorId);

  updateSourceQueryAssist();
  updateScheduleFilePathSummaries();
  updateScheduleTypeUi();
  await renderScheduleRecentLogs(entry?.id || '');
  await loadEntityHistory('schedule', entry?.id || '', 'sch-history-list', 'sch-history-meta', 'Scheduler noch nicht gespeichert.');
  await loadScheduleCheckpoint(entry?.id || '');
  setupMappingDropZone();
  loadTransformFunctions();
  await loadTargetObjects(entry?.objectName || '');
  toggleCreateObjectFromSourceUi();
  await loadTargetFields();
  renderSchedulerMappingAssistant();
  await syncSchedulerExternalIdUi();
  const requestedStep = Number(modalOptions.wizardStep || 1);
  state.scheduleWizardStep = Math.max(1, Math.min(getScheduleWizardTotalSteps(), requestedStep));
  renderScheduleWizardStep();
  const focusField = normalizeAffectedMappingField(modalOptions.focusMappingField);
  if (focusField) {
    setTimeout(() => {
      const focused = focusScheduleMappingField(focusField);
      if (!focused) {
        const status = document.getElementById('sch-source-test-status');
        if (status) {
          status.textContent = 'Hinweis: Betroffenes Feld "' + focusField + '" ist aktuell nicht im Mapping vorhanden.';
        }
      }
    }, 0);
  }
  // Load mapping fields from backend metadata API
  // Use setTimeout to ensure all DOM values (source-type, connector) are applied before fetching
  setTimeout(() => loadMappingFields(), 0);
  scheduleModal.show();
}

async function createSalesforceCustomObjectFromSource() {
  clearModalError();
  setCreateObjectStatus('Objekt wird erstellt ...', 'warning');

  try {

  if (!isSalesforceTargetSelection()) {
    showModalError('Bitte Target System = Salesforce und Target Type = SALESFORCE wählen.');
    setCreateObjectStatus('Abbruch: Salesforce Ziel nicht aktiv.', 'error');
    return;
  }

  if (!Array.isArray(state.mappingFields) || state.mappingFields.length === 0) {
    await loadMappingFields();
  }

  if (!Array.isArray(state.mappingFields) || state.mappingFields.length === 0) {
    showModalError('Es konnten keine Quellfelder geladen werden.');
    setCreateObjectStatus('Abbruch: keine Quellfelder verfügbar.', 'error');
    return;
  }

  const objectApiNameInput = document.getElementById('sch-new-custom-object');
  const objectLabelInput = document.getElementById('sch-new-custom-object-label');
  const objectApiName = String(objectApiNameInput?.value || '').trim();
  const label = String(objectLabelInput?.value || '').trim();

  if (!objectApiName) {
    showModalError('Bitte einen Objekt API Namen angeben, z. B. SourceExchangeRate__c.');
    setCreateObjectStatus('Abbruch: Objekt API Name fehlt.', 'error');
    return;
  }

  const fieldOverrides = Object.entries(state.customObjectFieldOverrides || {}).map(([sourceName, type]) => ({
    sourceName,
    type: String(type || '').trim()
  })).filter((item) => item.sourceName && item.type);

  const result = await requestJson('/api/setup/create-custom-object-from-source', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      objectApiName,
      label: label || undefined,
      sourceFields: state.mappingFields,
      fieldOverrides
    })
  });

  await loadTargetObjects(result.objectApiName || objectApiName);
  const objectSelect = document.getElementById('sch-object');
  if (objectSelect) {
    objectSelect.value = result.objectApiName || objectApiName;
  }
  await loadTargetFields();
  await syncSchedulerExternalIdUi(result.objectApiName || objectApiName);
  ensureSalesforceTargetDefinition();
  setCreateObjectStatus(
    'Fertig: ' + (result.objectApiName || objectApiName) + ' (' + (result.fieldsCreated || 0) + ' Felder) und Tab bereit.',
    'success'
  );
  } catch (error) {
    const message = error?.message || 'Objekt konnte nicht erstellt werden';
    showModalError(message);
    setCreateObjectStatus('Fehler: ' + message, 'error');
  }
}

function updateConnectorConfigUi() {
  const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
  const fileWrap = document.getElementById('con-file-settings-wrap');
  const mssqlWrap = document.getElementById('con-mssql-settings-wrap');
  const restWrap = document.getElementById('con-rest-settings-wrap');
  const binaryWrap = document.getElementById('con-binary-settings-wrap');
  const hint = document.getElementById('con-wizard-hint');
  const sqlTitle = document.getElementById('con-sql-settings-title');
  const sqlText = document.getElementById('con-sql-settings-text');
  if (!fileWrap || !mssqlWrap || !restWrap || !binaryWrap) {
    return;
  }

  setClosestFieldVisible('con-secret', isSqlConnectorType(connectorType));

  if (connectorType === 'FILE') {
    fileWrap.classList.remove('d-none');
  } else {
    fileWrap.classList.add('d-none');
  }

  if (isSqlConnectorType(connectorType)) {
    mssqlWrap.classList.remove('d-none');
  } else {
    mssqlWrap.classList.add('d-none');
  }

  if (isRestConnectorType(connectorType)) {
    restWrap.classList.remove('d-none');
  } else {
    restWrap.classList.add('d-none');
  }

  if (isBinaryImportConnectorType(connectorType)) {
    binaryWrap.classList.remove('d-none');
  } else {
    binaryWrap.classList.add('d-none');
  }

  if (sqlTitle) {
    sqlTitle.textContent = connectorType === 'POSTGRESQL'
      ? 'PostgreSQL Verbindung'
      : connectorType === 'MYSQL'
        ? 'MySQL Verbindung'
        : connectorType === 'FILEMAKER'
          ? 'FileMaker Data API'
          : 'MSSQL Verbindung';
  }
  if (sqlText) {
    sqlText.textContent = connectorType === 'POSTGRESQL'
      ? 'Pflicht: Host, Datenbank und Benutzer. Standard-Port ist 5432.'
      : connectorType === 'MYSQL'
        ? 'Pflicht: Host, Datenbank und Benutzer. Standard-Port ist 3306.'
        : connectorType === 'FILEMAKER'
          ? 'Pflicht: Data-API-Base-URL, Datenbank und Benutzer. Passwort kann direkt eingegeben oder per Secret Key (ENV) gelesen werden.'
          : 'Pflicht: Server, Datenbank und Benutzer. Passwort kann direkt eingegeben werden. Alternativ kann das Passwort über Secret Key (ENV) aus einer Umgebungsvariable gelesen werden.';
  }

  if (hint) {
    const labels = {
      MSSQL: 'SQL-Parameter für MSSQL ausfüllen.',
      POSTGRESQL: 'SQL-Parameter für PostgreSQL ausfüllen.',
      MYSQL: 'SQL-Parameter für MySQL ausfüllen.',
      FILEMAKER: 'FileMaker Data API Verbindung und Datenbank erfassen.',
      FILE: 'Datei-Einstellungen inkl. Format auswählen.',
      REST_API: 'REST Endpunkt + gewünschte Authentifizierung erfassen.',
      FILE_BINARY_SF_IMPORT: 'Binary Import-Pfade + Salesforce Zielfelder setzen.',
      CUSTOM: 'Benutzerdefiniert: Parameter im JSON Bereich pflegen.'
    };
    hint.textContent = labels[connectorType] || labels.CUSTOM;
  }

  updateRestAuthUi();
}

function updateRestAuthUi() {
  const authType = String(document.getElementById('con-rest-auth-type')?.value || 'none').trim().toLowerCase();
  const toggle = (id, visible) => {
    const element = document.getElementById(id);
    if (element) {
      element.classList.toggle('d-none', !visible);
    }
  };

  toggle('con-rest-basic-user-wrap', authType === 'basic');
  toggle('con-rest-basic-password-wrap', authType === 'basic');
  toggle('con-rest-bearer-token-wrap', authType === 'bearer');
  toggle('con-rest-api-key-name-wrap', authType === 'api_key');
  toggle('con-rest-api-key-value-wrap', authType === 'api_key');
  toggle('con-rest-api-key-location-wrap', authType === 'api_key');
  toggle('con-rest-token-url-wrap', authType === 'oauth2');
  toggle('con-rest-grant-type-wrap', authType === 'oauth2');
  toggle('con-rest-client-id-wrap', authType === 'oauth2');
  toggle('con-rest-client-secret-wrap', authType === 'oauth2');
  toggle('con-rest-scope-wrap', authType === 'oauth2');
}

function applyConnectorWizardSelection(preserveValues) {
  const wizardType = String(document.getElementById('con-wizard-type')?.value || 'MSSQL').trim().toUpperCase();
  const connectorTypeInput = document.getElementById('con-type');
  if (!connectorTypeInput) {
    return;
  }

  const finalType = wizardType === 'CUSTOM'
    ? String(connectorTypeInput.value || '').trim() || 'CUSTOM'
    : wizardType;
  connectorTypeInput.value = finalType;

  if (!preserveValues) {
    if (wizardType === 'FILE_BINARY_SF_IMPORT') {
      if (!document.getElementById('con-target-system').value) {
        document.getElementById('con-target-system').value = 'Salesforce';
      }
      if (!document.getElementById('con-direction').value) {
        document.getElementById('con-direction').value = 'Inbound';
      }
    }
    if (wizardType === 'REST_API' && !document.getElementById('con-direction').value) {
      document.getElementById('con-direction').value = 'Outbound';
    }
    if (wizardType === 'FILEMAKER') {
      if (!document.getElementById('con-target-system').value) {
        document.getElementById('con-target-system').value = 'FileMaker';
      }
      if (!document.getElementById('con-direction').value) {
        document.getElementById('con-direction').value = 'Outbound';
      }
    }
  }

  updateConnectorConfigUi();
}

function fillMssqlConnectorSettingsFromParameters(parameters) {
  const params = parameters || {};
  document.getElementById('con-mssql-server').value = String(params.server || params.baseUrl || params.serverUrl || '');
  document.getElementById('con-mssql-port').value = params.port === undefined || params.port === null || params.port === '' ? '' : String(params.port);
  document.getElementById('con-mssql-database').value = String(params.database || '');
  document.getElementById('con-mssql-user').value = String(params.user || '');
  document.getElementById('con-mssql-password').value = '';
  document.getElementById('con-mssql-encrypt').checked = params.encrypt === undefined ? (params.ssl === undefined ? true : !!params.ssl) : !!params.encrypt;
  document.getElementById('con-mssql-trust-server-certificate').checked = params.trustServerCertificate === undefined ? false : !!params.trustServerCertificate;
}

function mergeMssqlConnectorSettingsIntoParameters(parameters) {
  const merged = { ...(parameters || {}) };
  const connectorType = normalizeConnectorType(document.getElementById('con-type')?.value || '');
  const server = String(document.getElementById('con-mssql-server').value || '').trim();
  const database = String(document.getElementById('con-mssql-database').value || '').trim();
  const user = String(document.getElementById('con-mssql-user').value || '').trim();
  const password = String(document.getElementById('con-mssql-password').value || '').trim();
  const portRaw = String(document.getElementById('con-mssql-port').value || '').trim();

  if (server) {
    merged.server = server;
  }
  if (database) {
    merged.database = database;
  }
  if (user) {
    merged.user = user;
  }
  if (password) {
    merged.password = password;
  }
  if (portRaw) {
    const parsedPort = Number(portRaw);
    if (!Number.isNaN(parsedPort)) {
      merged.port = parsedPort;
    }
  } else if (connectorType === 'POSTGRESQL') {
    merged.port = 5432;
  } else if (connectorType === 'MYSQL') {
    merged.port = 3306;
  }

  if (connectorType === 'MSSQL') {
    merged.encrypt = !!document.getElementById('con-mssql-encrypt').checked;
    merged.trustServerCertificate = !!document.getElementById('con-mssql-trust-server-certificate').checked;
  } else if (connectorType === 'FILEMAKER') {
    merged.baseUrl = server;
    delete merged.server;
    delete merged.encrypt;
    delete merged.trustServerCertificate;
    delete merged.ssl;
  } else {
    merged.ssl = !!document.getElementById('con-mssql-encrypt').checked;
  }

  return merged;
}

function fillFileConnectorSettingsFromParameters(parameters) {
  const params = parameters || {};
  document.getElementById('con-file-kind').value = String(params.fileKind || params.format || 'CSV').toUpperCase();
  document.getElementById('con-file-base-path').value = String(params.basePath || params.fileBasePath || 'artifacts/files');
  document.getElementById('con-file-import-path').value = String(params.importPath || 'inbound');
  document.getElementById('con-file-export-path').value = String(params.exportPath || 'outbound');
  document.getElementById('con-file-archive-path').value = String(params.archivePath || 'archive');
  document.getElementById('con-file-charset').value = String(params.defaultCharset || 'utf8');
  document.getElementById('con-file-delimiter').value = String(params.defaultDelimiter || ';');
  document.getElementById('con-file-archive-read').checked = params.archiveOnRead === undefined ? true : !!params.archiveOnRead;
  document.getElementById('con-file-archive-write').checked = !!params.archiveOnWrite;
}

function mergeFileConnectorSettingsIntoParameters(parameters) {
  const merged = { ...(parameters || {}) };
  merged.fileKind = String(document.getElementById('con-file-kind').value || 'CSV').toUpperCase();
  merged.basePath = document.getElementById('con-file-base-path').value || 'artifacts/files';
  merged.importPath = document.getElementById('con-file-import-path').value || 'inbound';
  merged.exportPath = document.getElementById('con-file-export-path').value || 'outbound';
  merged.archivePath = document.getElementById('con-file-archive-path').value || 'archive';
  merged.defaultCharset = document.getElementById('con-file-charset').value || 'utf8';
  merged.defaultDelimiter = document.getElementById('con-file-delimiter').value || ';';
  merged.archiveOnRead = document.getElementById('con-file-archive-read').checked;
  merged.archiveOnWrite = document.getElementById('con-file-archive-write').checked;
  return merged;
}

function fillRestConnectorSettingsFromParameters(parameters) {
  const params = parameters || {};
  document.getElementById('con-rest-base-url').value = String(params.baseUrl || '');
  document.getElementById('con-rest-resource-path').value = String(params.resourcePath || params.path || '');
  document.getElementById('con-rest-auth-type').value = String(params.authType || 'none').toLowerCase();
  document.getElementById('con-rest-token-url').value = String(params.tokenUrl || '');
  document.getElementById('con-rest-grant-type').value = String(params.grantType || 'client_credentials');
  document.getElementById('con-rest-method').value = String(params.method || 'GET').toUpperCase();
  document.getElementById('con-rest-basic-user').value = String(params.username || '');
  document.getElementById('con-rest-basic-password').value = '';
  document.getElementById('con-rest-bearer-token').value = '';
  document.getElementById('con-rest-api-key-name').value = String(params.apiKeyName || '');
  document.getElementById('con-rest-api-key-value').value = '';
  document.getElementById('con-rest-api-key-location').value = String(params.apiKeyLocation || 'header').toLowerCase();
  document.getElementById('con-rest-client-id').value = String(params.clientId || '');
  document.getElementById('con-rest-client-secret').value = '';
  document.getElementById('con-rest-scope').value = String(params.scope || '');
  document.getElementById('con-rest-audience').value = String(params.audience || '');
  document.getElementById('con-rest-extra-headers').value = params.extraHeaders ? JSON.stringify(params.extraHeaders) : '';
  updateRestAuthUi();
}

function mergeRestConnectorSettingsIntoParameters(parameters) {
  const merged = { ...(parameters || {}) };
  merged.baseUrl = String(document.getElementById('con-rest-base-url').value || '').trim();
  merged.resourcePath = String(document.getElementById('con-rest-resource-path').value || '').trim();
  merged.authType = String(document.getElementById('con-rest-auth-type').value || 'none').trim().toLowerCase();
  merged.method = String(document.getElementById('con-rest-method').value || 'GET').trim().toUpperCase();
  if (merged.authType === 'oauth2') {
    merged.tokenUrl = String(document.getElementById('con-rest-token-url').value || '').trim();
    merged.grantType = String(document.getElementById('con-rest-grant-type').value || 'client_credentials').trim();
    merged.clientId = String(document.getElementById('con-rest-client-id').value || '').trim();
    const clientSecret = String(document.getElementById('con-rest-client-secret').value || '').trim();
    if (clientSecret) {
      merged.clientSecret = clientSecret;
    }
    merged.scope = String(document.getElementById('con-rest-scope').value || '').trim();
  } else {
    delete merged.tokenUrl;
    delete merged.grantType;
    delete merged.clientId;
    delete merged.clientSecret;
    delete merged.scope;
  }
  if (merged.authType === 'basic') {
    merged.username = String(document.getElementById('con-rest-basic-user').value || '').trim();
    const password = String(document.getElementById('con-rest-basic-password').value || '').trim();
    if (password) {
      merged.password = password;
    }
  } else {
    delete merged.username;
    delete merged.password;
  }
  if (merged.authType === 'bearer') {
    const bearerToken = String(document.getElementById('con-rest-bearer-token').value || '').trim();
    if (bearerToken) {
      merged.bearerToken = bearerToken;
    }
  } else {
    delete merged.bearerToken;
  }
  if (merged.authType === 'api_key') {
    merged.apiKeyName = String(document.getElementById('con-rest-api-key-name').value || '').trim();
    merged.apiKeyLocation = String(document.getElementById('con-rest-api-key-location').value || 'header').trim().toLowerCase();
    const apiKeyValue = String(document.getElementById('con-rest-api-key-value').value || '').trim();
    if (apiKeyValue) {
      merged.apiKeyValue = apiKeyValue;
    }
  } else {
    delete merged.apiKeyName;
    delete merged.apiKeyLocation;
    delete merged.apiKeyValue;
  }
  merged.audience = String(document.getElementById('con-rest-audience').value || '').trim();
  const rawHeaders = String(document.getElementById('con-rest-extra-headers').value || '').trim();
  if (rawHeaders) {
    try {
      merged.extraHeaders = JSON.parse(rawHeaders);
    } catch {
      throw new Error('Zusätzliche Header müssen gültiges JSON sein');
    }
  } else {
    delete merged.extraHeaders;
  }
  return merged;
}

function fillBinaryImportConnectorSettingsFromParameters(parameters) {
  const params = parameters || {};
  document.getElementById('con-binary-base-path').value = String(params.basePath || 'artifacts/files');
  document.getElementById('con-binary-import-path').value = String(params.importPath || 'binary-inbound');
  document.getElementById('con-binary-archive-path').value = String(params.archivePath || 'archive');
  document.getElementById('con-binary-extensions').value = String(params.allowedExtensions || 'pdf,jpg,png,zip');
  document.getElementById('con-binary-sf-object').value = String(params.salesforceObject || 'ContentVersion');
  document.getElementById('con-binary-sf-binary-field').value = String(params.binaryField || 'VersionData');
  document.getElementById('con-binary-sf-filename-field').value = String(params.fileNameField || 'PathOnClient');
  document.getElementById('con-binary-title-prefix').value = String(params.titlePrefix || '');
}

function mergeBinaryImportConnectorSettingsIntoParameters(parameters) {
  const merged = { ...(parameters || {}) };
  merged.basePath = String(document.getElementById('con-binary-base-path').value || 'artifacts/files').trim();
  merged.importPath = String(document.getElementById('con-binary-import-path').value || 'binary-inbound').trim();
  merged.archivePath = String(document.getElementById('con-binary-archive-path').value || 'archive').trim();
  merged.allowedExtensions = String(document.getElementById('con-binary-extensions').value || 'pdf,jpg,png,zip').trim();
  merged.salesforceObject = String(document.getElementById('con-binary-sf-object').value || 'ContentVersion').trim();
  merged.binaryField = String(document.getElementById('con-binary-sf-binary-field').value || 'VersionData').trim();
  merged.fileNameField = String(document.getElementById('con-binary-sf-filename-field').value || 'PathOnClient').trim();
  merged.titlePrefix = String(document.getElementById('con-binary-title-prefix').value || '').trim();
  return merged;
}

function openConnectorModal(connectorId, templateDraft) {
  const entry = connectorId
    ? state.connectors.find((item) => item.id === connectorId)
    : (templateDraft || null);
  clearConnectorModalError();
  document.getElementById('con-id').value = entry?.id || '';
  renderWizardMetadata('con-wizard-meta', entry, 'Neuer Connector · noch nicht gespeichert');
  document.getElementById('con-name').value = entry?.name || '';
  document.getElementById('con-type').value = entry?.connectorType || 'MSSQL';
  document.getElementById('con-wizard-type').value = getConnectorWizardTypeFromConnectorType(entry?.connectorType || 'MSSQL');
  document.getElementById('con-target-system').value = entry?.targetSystem || '';
  document.getElementById('con-direction').value = entry?.direction || '';
  document.getElementById('con-secret').value = entry?.secretKey || '';
  document.getElementById('con-timeout').value = entry?.timeoutMs || '';
  document.getElementById('con-retries').value = entry?.maxRetries || '';
  document.getElementById('con-description').value = entry?.description || '';
  const parameters = entry?.parameters || {};
  document.getElementById('con-parameters').value = JSON.stringify(parameters, null, 2);
  fillConnectorNotificationSettingsFromParameters(parameters);
  void loadConnectorTaskOwnerOptions(parameters.notificationTaskOwnerId);
  fillMssqlConnectorSettingsFromParameters(parameters);
  fillFileConnectorSettingsFromParameters(parameters);
  fillRestConnectorSettingsFromParameters(parameters);
  fillBinaryImportConnectorSettingsFromParameters(parameters);
  applyConnectorWizardSelection(!!entry);
  updateConnectorConfigUi();
  document.getElementById('con-active').checked = entry ? !!entry.active : true;
  state.connectorWizardStep = 1;
  renderConnectorWizardStep();
  void loadEntityHistory('connector', entry?.id || '', 'con-history-list', 'con-history-meta', 'Connector noch nicht gespeichert.');
  connectorModal.show();
}

async function saveSchedule() {
  clearError();
  clearModalError();
  const saveButton = document.getElementById('save-schedule');
  saveButton.disabled = true;

  try {
    await ensureMssqlTargetObjectSelection();
    for (let step = 1; step < getScheduleWizardTotalSteps(); step += 1) {
      validateScheduleWizardStep(step);
    }
    ensureSalesforceTargetDefinition();
    const targetConstraintMessage = updateSchedulerExternalIdValidationState();
    if (targetConstraintMessage) {
      throw new Error(targetConstraintMessage);
    }
    const requiredFieldMessage = getRequiredSalesforceFieldSaveMessage();
    if (requiredFieldMessage) {
      throw new Error(requiredFieldMessage);
    }

    const payload = collectScheduleFormPayload();
    const scheduleId = payload.id;

    // Only include name for new schedules (Name is an auto-number field and cannot be updated)
    if (!scheduleId) {
      payload.name = document.getElementById('sch-name').value;
    } else {
      delete payload.name;
    }

    const validationResult = await validateCurrentScheduleConfiguration();
    const validationError = Array.isArray(validationResult?.issues)
      ? validationResult.issues.find((issue) => issue.severity === 'error')
      : null;
    if (validationError) {
      throw new Error(validationError.message || 'Scheduler-Konfiguration ist ungueltig.');
    }

    const result = await requestJson('/api/schedules', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    scheduleModal.hide();
    await saveScheduleCheckpoint(result?.id || scheduleId);
    await refresh();
  } catch (error) {
    showModalError(error.message || 'Scheduler konnte nicht gespeichert werden');
  } finally {
    saveButton.disabled = false;
  }
}

async function testScheduleSource() {
  clearError();
  clearModalError();
  const testButton = document.getElementById('sch-test-source');
  const sourceType = document.getElementById('sch-source-type').value;
  const sourceDefinition = buildScheduleSourceDefinitionValue() || '';
  const connectorId = document.getElementById('sch-connector').value || undefined;
  const status = document.getElementById('sch-source-test-status');

  testButton.disabled = true;
  status.textContent = 'Quelle wird getestet...';

  try {
    const result = await requestJson('/api/sources/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sourceType,
        sourceDefinition,
        connectorId,
        limit: 10
      })
    });

    renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', result.rows || []);
    status.textContent = (result.rowCount || 0) + ' Datensätze geladen.';
  } catch (error) {
    renderGenericPreviewTable('sch-source-preview-header', 'sch-source-preview-body', []);
    status.textContent = 'Quelltest fehlgeschlagen.';
    showModalError(error.message || 'Quelle konnte nicht getestet werden');
  } finally {
    testButton.disabled = false;
  }
}

function renderEntityHistory(containerId, metaId, items, emptyText) {
  const container = document.getElementById(containerId);
  const meta = document.getElementById(metaId);
  if (!container) return;
  const historyItems = Array.isArray(items) ? items : [];
  if (meta) {
    meta.textContent = historyItems.length
      ? historyItems.length + ' Historieneinträge geladen.'
      : 'Keine Historieneinträge gefunden.';
  }
  if (!historyItems.length) {
    container.innerHTML = '<div class="text-secondary">' + esc(emptyText || 'Keine Änderungshistorie vorhanden.') + '</div>';
    return;
  }
  container.innerHTML =
    '<div class="table-responsive"><table class="table table-sm mb-0">' +
      '<thead><tr><th>Zeit</th><th>Benutzer</th><th>Aktion</th><th>Status</th><th>Hinweis</th></tr></thead><tbody>' +
        historyItems.map((item) => {
          const actor = item.actor && item.actor.username ? item.actor.username : '-';
          return '<tr>' +
            '<td>' + esc(formatDate(item.at, 'short')) + '</td>' +
            '<td>' + esc(actor) + '</td>' +
            '<td>' + esc(item.action || '-') + '</td>' +
            '<td>' + esc(item.status || '-') + '</td>' +
            '<td>' + esc(item.message || item.entityName || '-') + '</td>' +
          '</tr>';
        }).join('') +
      '</tbody></table></div>';
}

async function loadEntityHistory(entityType, entityId, containerId, metaId, emptyText) {
  const normalizedId = String(entityId || '').trim();
  if (!normalizedId) {
    renderEntityHistory(containerId, metaId, [], emptyText || 'Noch nicht gespeichert.');
    return;
  }
  const result = await safeRequest(
    '/api/admin/audit-history?limit=50&entityType=' + encodeURIComponent(entityType) + '&entityId=' + encodeURIComponent(normalizedId),
    { items: [] }
  );
  renderEntityHistory(containerId, metaId, result.items || [], emptyText);
}

function renderScheduleValidationResult(result) {
  const issues = Array.isArray(result?.issues) ? result.issues : [];
  if (!issues.length) {
    clearModalError();
    const status = document.getElementById('sch-source-test-status');
    if (status) status.textContent = 'Konfiguration geprüft: keine strukturellen Probleme gefunden.';
    return;
  }
  const errors = issues.filter((issue) => issue.severity === 'error');
  const warnings = issues.filter((issue) => issue.severity !== 'error');
  const message = issues.map((issue) =>
    '[' + String(issue.severity || 'warning').toUpperCase() + '] ' + String(issue.area || 'general') + ': ' + String(issue.message || '')
  ).join('\n');
  if (errors.length) {
    showModalError('Konfigurationsprüfung fehlgeschlagen:\n' + message);
  } else {
    showModalError('Konfigurationsprüfung mit Warnungen:\n' + message);
  }
  const status = document.getElementById('sch-source-test-status');
  if (status) {
    status.textContent = errors.length
      ? errors.length + ' Fehler, ' + warnings.length + ' Warnungen gefunden.'
      : warnings.length + ' Warnungen gefunden.';
  }
}

async function validateCurrentScheduleConfiguration() {
  clearModalError();
  const payload = collectScheduleFormPayload();
  if (!payload.name && !payload.id) {
    payload.name = String(document.getElementById('sch-name')?.value || '').trim() || 'Neuer Scheduler';
  }
  const result = await requestJson('/api/schedules/validate-config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  renderScheduleValidationResult(result);
  return result;
}

function collectConnectorFormPayload() {
  const preview = collectConnectorParametersPreview();
  const connectorType = preview.connectorType || document.getElementById('con-type').value;
  const secretKey = isSqlConnectorType(normalizeConnectorType(connectorType))
    ? (document.getElementById('con-secret').value || undefined)
    : undefined;
  return {
    id: document.getElementById('con-id').value || undefined,
    name: document.getElementById('con-name').value,
    active: document.getElementById('con-active').checked,
    connectorType,
    targetSystem: document.getElementById('con-target-system').value || undefined,
    direction: document.getElementById('con-direction').value || undefined,
    secretKey,
    timeoutMs: Number(document.getElementById('con-timeout').value || 0) || undefined,
    maxRetries: Number(document.getElementById('con-retries').value || 0) || undefined,
    description: document.getElementById('con-description').value || undefined,
    parameters: preview.parameters
  };
}

async function persistConnector(options = {}) {
  const validateAfterSave = options.validateAfterSave === true;
  const payload = collectConnectorFormPayload();

  const saved = await requestJson('/api/connectors', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  document.getElementById('con-id').value = saved.id || payload.id || '';

  if (validateAfterSave && saved.id) {
    const result = await requestJson('/api/connectors/' + encodeURIComponent(saved.id) + '/test', { method: 'POST' });
    alert(result.message || (result.ok ? 'OK' : 'Fehler'));
  }

  await refresh();
  return saved;
}

async function saveConnector() {
  try {
    validateConnectorWizardStep(1);
    validateConnectorWizardStep(2);
    validateConnectorWizardStep(3);
    await persistConnector({ validateAfterSave: false });
    connectorModal.hide();
  } catch (error) {
    showConnectorModalError(error?.message || 'Connector konnte nicht gespeichert werden.');
  }
}

async function saveAndValidateConnector() {
  try {
    validateConnectorWizardStep(1);
    validateConnectorWizardStep(2);
    validateConnectorWizardStep(3);
    await persistConnector({ validateAfterSave: true });
    connectorModal.hide();
  } catch (error) {
    showConnectorModalError(error?.message || 'Connector konnte nicht validiert werden.');
  }
}

async function loadLogs() {
  const runId = document.getElementById('log-run-select').value;
  if (!runId) {
    return;
  }

  const logs = await safeRequest('/api/runs/' + encodeURIComponent(runId) + '/logs', { items: [] });
  const lines = (logs.items || []).map((entry) => '[' + formatDate(entry.createdAt, 'short') + '] [' + (entry.level || '-') + '] ' + (entry.step || '-') + ' | ' + (entry.message || ''));
  document.getElementById('logs-output').textContent = lines.join('\n') || 'Keine Logs gefunden.';
}

async function analyzeCurrentRunError() {
  const runId = document.getElementById('log-run-select').value;
  if (!runId) {
    showError('Bitte wähle zuerst einen Run aus');
    return;
  }

  try {
    const logsOutput = document.getElementById('logs-output').textContent || '';
    
    if (!logsOutput || logsOutput.includes('Noch keine Logs') || logsOutput.includes('Keine Logs gefunden')) {
      showError('Keine Logs zum Analysieren vorhanden');
      return;
    }

    // Finde Run-Details für zusätzliche Kontext
    const selectedRun = (state.runs || []).find((r) => r.id === runId);
    if (!selectedRun) {
      showError('Run-Details nicht gefunden');
      return;
    }
    const selectedScheduleId = String(selectedRun.scheduleId || '').trim();
    const selectedSchedule = selectedScheduleId
      ? (state.schedules || []).find((schedule) => String(schedule.id || '').trim() === selectedScheduleId)
      : null;
    const resolvedSourceSystem = String(selectedRun.sourceSystem || selectedSchedule?.sourceSystem || '').trim();
    const resolvedTargetSystem = String(selectedRun.targetSystem || selectedSchedule?.targetSystem || '').trim();
    const resolvedScheduleName = String(selectedRun.scheduleName || selectedSchedule?.name || '').trim();

    // Zeige Analyse-Status
    showInfo('Analysiere Fehler mit KI...');

    const analysis = await requestJson('/api/ai/analyze-error', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        runId: runId,
        scheduleName: resolvedScheduleName || 'Unbekannter Scheduler',
        sourceSystem: resolvedSourceSystem || 'Quellsystem',
        targetSystem: resolvedTargetSystem || 'Zielsystem',
        errorLog: logsOutput,
        errorCode: selectedRun.errorCode,
        recordsProcessed: selectedRun.recordsProcessed,
        failedRecords: selectedRun.failedRecords
      })
    });

    // Zeige Analyse-Ergebnis in Modal
    showErrorAnalysisModal(analysis, selectedRun);
  } catch (error) {
    showError('Fehleranalyse fehlgeschlagen: ' + (error.message || String(error)));
  }
}

function showErrorAnalysisModal(analysis, selectedRun) {
  const errorCategoryLabels = {
    connector_unavailable: 'Connector nicht erreichbar',
    authentication_failed: 'Authentifizierungsfehler',
    mapping_error: 'Mapping-Fehler',
    data_validation: 'Datenvalidierung',
    network_issue: 'Netzwerkproblem',
    timeout: 'Timeout',
    quota_exceeded: 'Quota überschritten',
    unknown: 'Unbekannt'
  };
  const errorCategoryLabel = errorCategoryLabels[String(analysis?.errorCategory || '').trim()] || String(analysis?.errorCategory || 'Unbekannt');
  const selectedScheduleId = String(selectedRun?.scheduleId || '').trim();
  const modalHtml = `
    <div class="modal fade" id="error-analysis-modal" tabindex="-1">
      <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
          <div class="modal-header bg-light">
            <h5 class="modal-title">KI-Fehleranalyse</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
          </div>
          <div class="modal-body">
            <div class="row g-3">
              <div class="col-12">
                <div class="d-flex align-items-center gap-2">
                  <span class="badge bg-${analysis.severity === 'critical' ? 'danger' : analysis.severity === 'error' ? 'warning' : 'info'}">${analysis.severity.toUpperCase()}</span>
                  <span class="badge bg-secondary">${htmlEscape(errorCategoryLabel)}</span>
                  <span class="badge bg-light text-dark">${Math.round(analysis.confidence * 100)}% Konfidenz</span>
                </div>
              </div>

              <div class="col-12">
                <strong>Root-Cause:</strong>
                <p class="small text-secondary mb-0">${htmlEscape(analysis.rootCause)}</p>
              </div>

              ${analysis.affectedFields && analysis.affectedFields.length > 0 ? `
                <div class="col-12">
                  <strong>Betroffene Felder:</strong>
                  <div class="small">
                    ${analysis.affectedFields.map((f) => `<code>${htmlEscape(f)}</code>`).join(', ')}
                  </div>
                </div>
              ` : ''}

              <div class="col-12">
                <strong>Handlungsempfehlungen:</strong>
                <ul class="small mb-0">
                  ${analysis.recommendations.map((rec) => `<li>${htmlEscape(rec)}</li>`).join('')}
                </ul>
              </div>

              ${analysis.suggestedFix ? `
                <div class="col-12">
                  <div class="alert alert-info mb-0 small">
                    <strong>Schnelle Lösung:</strong> ${htmlEscape(analysis.suggestedFix)}
                  </div>
                </div>
              ` : ''}
            </div>
          </div>
          <div class="modal-footer">
            ${selectedScheduleId ? '<button type="button" class="btn btn-primary" id="error-analysis-open-scheduler">Scheduler anpassen</button>' : ''}
            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Schließen</button>
          </div>
        </div>
      </div>
    </div>
  `;

  // Entferne altes Modal falls vorhanden
  const oldModal = document.getElementById('error-analysis-modal');
  if (oldModal) oldModal.remove();

  // Erstelle neues Modal
  const container = document.createElement('div');
  container.innerHTML = modalHtml;
  document.body.appendChild(container);

  // Zeige Modal
  const modalElement = document.getElementById('error-analysis-modal');
  const modal = new window.bootstrap.Modal(modalElement);
  const openSchedulerButton = modalElement?.querySelector('#error-analysis-open-scheduler');
  if (openSchedulerButton && selectedScheduleId) {
    openSchedulerButton.addEventListener('click', async () => {
      modal.hide();
      const preferredField = Array.isArray(analysis?.affectedFields) && analysis.affectedFields.length > 0
        ? String(analysis.affectedFields[0] || '').trim()
        : '';
      await openScheduleModal(selectedScheduleId, undefined, {
        wizardStep: 5,
        focusMappingField: preferredField
      });
    });
  }
  modal.show();
}

async function renderScheduleRecentLogs(scheduleId) {
  const outputEl = document.getElementById('sch-recent-logs-output');
  const metaEl = document.getElementById('sch-recent-logs-meta');
  if (!outputEl || !metaEl) {
    return;
  }

  const normalizedScheduleId = String(scheduleId || '').trim();
  if (!normalizedScheduleId) {
    metaEl.textContent = 'Scheduler noch nicht gespeichert.';
    outputEl.textContent = 'Logs stehen nach dem ersten Lauf zur Verfügung.';
    return;
  }

  const latestRun = (state.runs || [])
    .filter((run) => String(run.scheduleId || '').trim() === normalizedScheduleId)
    .sort((left, right) => {
      const leftTime = new Date(left.startedAt || left.finishedAt || 0).getTime();
      const rightTime = new Date(right.startedAt || right.finishedAt || 0).getTime();
      return rightTime - leftTime;
    })[0];

  if (!latestRun) {
    metaEl.textContent = 'Noch kein Run für diesen Scheduler gefunden.';
    outputEl.textContent = 'Keine Logs vorhanden.';
    return;
  }

  metaEl.textContent = [
    'Letzter Run: ' + formatDate(latestRun.startedAt || latestRun.finishedAt, 'short'),
    'Status: ' + String(latestRun.status || '-'),
    'Dauer: ' + formatDurationMinSec(getRunDurationMs(latestRun))
  ].join(' • ');

  outputEl.textContent = 'Logs werden geladen...';
  const logs = await safeRequest('/api/runs/' + encodeURIComponent(latestRun.id) + '/logs', { items: [] });
  const lines = (logs.items || []).slice(0, 30).map((entry) => {
    return '[' + formatDate(entry.createdAt, 'short') + '] [' + (entry.level || '-') + '] ' + (entry.step || '-') + ' | ' + (entry.message || '');
  });
  outputEl.textContent = lines.join('\n') || 'Keine Logs für den letzten Run gefunden.';
}

async function previewSql() {
  const connectorId = document.getElementById('sql-connector-select').value;
  const query = document.getElementById('sql-query').value;
  const result = await requestJson('/api/queries/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ connectorId, query, limit: 10 })
  });

  if (result.rows) {
    document.getElementById('mapping-source').value = JSON.stringify(result.rows.slice(0, 5), null, 2);
  }
  document.getElementById('mapping-output').textContent = JSON.stringify(result, null, 2);
}

async function previewMapping() {
  const mappingDefinition = document.getElementById('mapping-definition').value;
  const sourceData = JSON.parse(document.getElementById('mapping-source').value || '[]');
  const result = await requestJson('/api/mappings/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mappingDefinition, sourceData })
  });
  document.getElementById('mapping-output').textContent = JSON.stringify(result, null, 2);
}

function normalizeMigrationTargetObject(value) {
  const allowedTargets = ['Account', 'Contact', 'Lead', 'Opportunity', 'Order', 'Product2', 'PricebookEntry'];
  const normalized = String(value || '').trim();
  return allowedTargets.includes(normalized) ? normalized : 'Contact';
}

function getRequiredFieldsForMigrationTarget(targetObject) {
  const normalized = normalizeMigrationTargetObject(targetObject);
  const requiredByTarget = {
    Account: ['Name'],
    Contact: ['LastName'],
    Lead: ['LastName', 'Company'],
    Opportunity: ['Name', 'StageName', 'CloseDate'],
    Order: ['AccountId', 'EffectiveDate', 'Status'],
    Product2: ['Name'],
    PricebookEntry: ['Pricebook2Id', 'Product2Id', 'UnitPrice']
  };
  return Array.isArray(requiredByTarget[normalized]) ? requiredByTarget[normalized] : [];
}

function findMissingRequiredFieldsForMigrationTarget(targetObject, mappings) {
  const requiredFields = getRequiredFieldsForMigrationTarget(targetObject);
  if (!requiredFields.length) {
    return [];
  }

  const mappedTargetFields = new Set(
    (Array.isArray(mappings) ? mappings : [])
      .map((item) => String(item && item.targetField ? item.targetField : '').trim().toLowerCase())
      .filter(Boolean)
  );

  return requiredFields.filter((fieldName) => !mappedTargetFields.has(String(fieldName).toLowerCase()));
}

function mapAnalysisDataTypeToTargetFieldType(dataType) {
  const normalized = String(dataType || '').trim().toLowerCase();
  if (normalized === 'datetime' || normalized === 'date') {
    return normalized;
  }
  if (normalized === 'integer') {
    return 'int';
  }
  if (normalized === 'decimal') {
    return 'double';
  }
  if (normalized === 'boolean') {
    return 'boolean';
  }
  return 'string';
}

async function createMigrationProfileFromAnalysis() {
  try {
    clearError();

    const analysis = migState.migrationSourceAnalysis;
    const payload = migState.migrationSourcePayload || {};
    if (!analysis || !Array.isArray(analysis.suggestedMappings)) {
      showError('Bitte zuerst eine KI-Analyse durchführen.');
      return;
    }

    const selectedTargetObject = normalizeMigrationTargetObject(document.getElementById('migration-target-object')?.value);
    const effectiveTargetObject = normalizeMigrationTargetObject(selectedTargetObject || String(analysis.suggestedTargetObject || 'Contact'));

    const suggestedMappings = analysis.suggestedMappings
      .filter((item) => item && String(item.sourceField || '').trim())
      .filter((item) => String(item.targetField || '').trim())
      .filter((item) => String(item.privacyAction || '').trim() !== 'exclude');

    if (!suggestedMappings.length) {
      showError('Es konnten keine nutzbaren Feld-Mappings für ein Profil abgeleitet werden.');
      return;
    }

    const missingRequiredFields = findMissingRequiredFieldsForMigrationTarget(effectiveTargetObject, suggestedMappings);
    if (missingRequiredFields.length) {
      const proceed = window.confirm(
        'Hinweis: Für Salesforce ' + effectiveTargetObject + ' fehlen Pflichtfelder in den KI-Mappings:\n- ' +
        missingRequiredFields.join('\n- ') +
        '\n\nDas Profil wird erstellt, aber vor dem Run solltest du diese Felder ergänzen. Trotzdem fortfahren?'
      );
      if (!proceed) {
        showInfo('Profilerstellung abgebrochen. Bitte fehlende Pflichtfelder ergänzen und erneut erstellen.');
        return;
      }
    }

    const migrationObjectId = 'obj-' + String(effectiveTargetObject).toLowerCase() + '-' + Date.now();
    const migrationName = 'KI-Profil: ' + String(payload.sourceName || analysis.sourceName || 'Quelle') + ' → ' + effectiveTargetObject;
    const selectedInstance = (state.instances || []).find((item) => String(item.id || '') === String(state.instanceId || ''));
    const inferredProjectId = String(selectedInstance && selectedInstance.projectId || 'default-project');

    const migrationPayload = {
      name: migrationName,
      description: [
        'Automatisch aus KI-Analyse erstellt',
        payload.description ? String(payload.description) : ''
      ].filter(Boolean).join(' · '),
      batchSize: 200,
      projectId: inferredProjectId,
      instanceId: state.instanceId || undefined,
      status: 'draft',
      objects: [
        {
          id: migrationObjectId,
          salesforceObject: effectiveTargetObject,
          operation: 'upsert',
          fileColumns: suggestedMappings.map((item) => String(item.sourceField || '').trim()).filter(Boolean),
          fieldMappings: suggestedMappings.map((item) => ({
            sourceColumn: String(item.sourceField || '').trim(),
            targetField: String(item.targetField || '').trim(),
            targetFieldType: mapAnalysisDataTypeToTargetFieldType(item.dataType),
            transformFunction: 'NONE'
          }))
        }
      ],
      dependencies: [],
      executionPlan: [
        {
          order: 1,
          objectId: migrationObjectId,
          description: effectiveTargetObject + ' import'
        }
      ]
    };

    const savedMigration = await requestJson('/api/migrations', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(migrationPayload)
    });

    await refresh();
    openMigWizard(savedMigration, { startStep: 3, pendingImportAnalysis: analysis });
    showInfo('Migrationsprofil wurde aus der KI-Analyse erstellt.');
  } catch (error) {
    showError('Migrationsprofil konnte nicht erstellt werden: ' + (error instanceof Error ? error.message : String(error)));
  }
}

async function analyzeMigrationSource() {
  try {
    clearError();
    showInfo('Analysiere Datenquelle mit Datenschutz-Fokus...');

    const sourceName = document.getElementById('migration-source-name').value.trim();
    const sourceType = document.getElementById('migration-source-type').value;
    const targetObject = normalizeMigrationTargetObject(document.getElementById('migration-target-object')?.value);
    const fieldDefsStr = document.getElementById('migration-field-defs').value.trim();
    const estimatedRecords = Number(document.getElementById('migration-est-records').value || 0);
    const description = document.getElementById('migration-description').value;

    if (!sourceName) {
      showError('Bitte gebe einen Quellnamen ein');
      return;
    }

    let fieldDefinitions = [];
    if (fieldDefsStr) {
      try {
        fieldDefinitions = JSON.parse(fieldDefsStr);
      } catch (e) {
        showError('Feld-Definitionen sind kein gültiges JSON: ' + (e instanceof Error ? e.message : String(e)));
        return;
      }
    }

    const analysis = await requestJson('/api/ai/analyze-migration-source', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sourceName,
        sourceType,
        targetObject,
        fieldDefinitions,
        estimatedRecords: estimatedRecords > 0 ? estimatedRecords : undefined,
        description: description || undefined
      })
    });

    migState.migrationSourceAnalysis = analysis;
    migState.migrationSourcePayload = {
      sourceName,
      sourceType,
      targetObject,
      fieldDefinitions,
      estimatedRecords: estimatedRecords > 0 ? estimatedRecords : undefined,
      description: description || undefined
    };

    const effectiveTargetObject = normalizeMigrationTargetObject(String(analysis.suggestedTargetObject || targetObject || 'Contact'));
    const targetObjectSelect = document.getElementById('migration-target-object');
    if (targetObjectSelect) {
      targetObjectSelect.value = effectiveTargetObject;
    }

    const usableMappings = (Array.isArray(analysis.suggestedMappings) ? analysis.suggestedMappings : [])
      .filter((item) => item && String(item.sourceField || '').trim())
      .filter((item) => String(item.targetField || '').trim())
      .filter((item) => String(item.privacyAction || '').trim() !== 'exclude');
    const requiredFields = getRequiredFieldsForMigrationTarget(effectiveTargetObject);
    const missingRequiredFieldsPreview = findMissingRequiredFieldsForMigrationTarget(effectiveTargetObject, usableMappings);
    const mappedRequiredFieldsCount = Math.max(0, requiredFields.length - missingRequiredFieldsPreview.length);
    const requiredCoveragePercent = requiredFields.length
      ? Math.round((mappedRequiredFieldsCount / requiredFields.length) * 100)
      : 100;
    let requiredStatusClass = 'alert-success';
    let requiredStatusLabel = 'GRUEN';
    let requiredStatusText = 'Alle Pflichtfelder für ' + effectiveTargetObject + ' sind in den Vorschlägen enthalten.';
    if (requiredFields.length > 0 && missingRequiredFieldsPreview.length === requiredFields.length) {
      requiredStatusClass = 'alert-danger';
      requiredStatusLabel = 'ROT';
      requiredStatusText = 'Pflichtfelder fehlen vollständig. Profil nur als Roh-Entwurf verwenden.';
    } else if (missingRequiredFieldsPreview.length > 0) {
      requiredStatusClass = 'alert-warning';
      requiredStatusLabel = 'GELB';
      requiredStatusText = 'Pflichtfelder sind nur teilweise abgedeckt. Vor dem Run ergänzen.';
    }

    // Zeige Analyse-Ergebnis
    const resultDiv = document.getElementById('migration-analysis-result');
    if (resultDiv) {
      resultDiv.innerHTML = `
        <div class="card soft-card border-info">
          <div class="card-header bg-light d-flex justify-content-between align-items-center">
            <strong>Analyse-Ergebnis</strong>
            <div class="d-flex gap-2">
              <span class="badge bg-info">${Math.round(analysis.dataQualityScore * 100)}% Qualität</span>
              <span class="badge bg-${analysis.complianceIssues.length > 0 ? 'warning' : 'success'}">${analysis.sensitiveFields.length} sensitive Felder</span>
              <span class="badge bg-primary">Ziel: Salesforce ${htmlEscape(effectiveTargetObject)}</span>
            </div>
          </div>
          <div class="card-body">
            <div class="row g-3">
              <div class="col-12">
                <strong>Quelle:</strong> ${htmlEscape(analysis.sourceName)} (${htmlEscape(analysis.sourceType)})
                <br/>
                <small class="text-secondary">Gesamt-Felder: ${analysis.totalFields} | Zielobjekt: ${htmlEscape(effectiveTargetObject)} | Konfidenz: ${Math.round(analysis.confidence * 100)}%</small>
              </div>

              <div class="col-12">
                <div class="alert ${requiredStatusClass} py-2 mb-0">
                  <div class="d-flex justify-content-between align-items-center flex-wrap gap-2">
                    <strong>Pflichtfeld-Check ${requiredStatusLabel}</strong>
                    <span class="badge bg-secondary">Abdeckung: ${requiredCoveragePercent}%</span>
                  </div>
                  <div class="small mt-1">${htmlEscape(requiredStatusText)}</div>
                  <div class="small mt-1">Pflichtfelder: ${requiredFields.length ? requiredFields.map(function(field) { return htmlEscape(field); }).join(', ') : '-'}</div>
                  ${missingRequiredFieldsPreview.length ? `<div class="small mt-1 text-danger">Fehlend: ${missingRequiredFieldsPreview.map(function(field) { return htmlEscape(field); }).join(', ')}</div>` : ''}
                </div>
              </div>

              ${analysis.sensitiveFields.length > 0 ? `
                <div class="col-12">
                  <strong>🔒 Sensitive Felder (${analysis.sensitiveFields.length}):</strong>
                  <ul class="small mb-0">
                    ${analysis.sensitiveFields.map(function(f) { return `
                      <li>
                        <code>${htmlEscape(f.fieldName)}</code>
                        <span class="badge bg-danger">${f.category}</span>
                        <span class="badge bg-warning">Aktion: ${f.suggestedAction}</span>
                      </li>
                    `; }).join('')}
                  </ul>
                </div>
              ` : ''}

              ${analysis.complianceIssues.length > 0 ? `
                <div class="col-12">
                  <div class="alert alert-warning mb-0">
                    <strong>⚠️ Datenschutz-Hinweise:</strong>
                    <ul class="small mb-0">
                      ${analysis.complianceIssues.map(function(issue) { return `<li>${htmlEscape(issue)}</li>`; }).join('')}
                    </ul>
                  </div>
                </div>
              ` : ''}

              <div class="col-12">
                <strong>📋 Empfehlungen:</strong>
                <ul class="small mb-0">
                  ${analysis.recommendations.map(function(rec) { return `<li>${htmlEscape(rec)}</li>`; }).join('')}
                </ul>
              </div>

              <div class="col-12">
                <strong>🗺️ Vorgeschlagene Mappings:</strong>
                <div class="small bg-light p-2 rounded">
                  <table class="table table-sm mb-0">
                    <thead><tr><th>Source-Feld</th><th>Datentyp</th><th>Ziel-Feld</th><th>Privacy</th></tr></thead>
                    <tbody>
                      ${analysis.suggestedMappings.slice(0, 10).map(function(m) { return `
                        <tr>
                          <td><code>${htmlEscape(m.sourceField)}</code></td>
                          <td><small>${htmlEscape(m.dataType)}</small></td>
                          <td><code>${m.targetField ? htmlEscape(m.targetField) : '-'}</code></td>
                          <td>${m.isSensitive ? '🔒 ' + (m.privacyAction || '-') : '✓'}</td>
                        </tr>
                      `; }).join('')}
                    </tbody>
                  </table>
                  ${analysis.suggestedMappings.length > 10 ? `<div class="small text-secondary mt-2">... und ${analysis.suggestedMappings.length - 10} weitere Felder</div>` : ''}
                </div>
              </div>

              <div class="col-12 d-flex justify-content-end">
                <button id="migration-create-profile-from-analysis" type="button" class="btn btn-primary btn-sm">Migrationsprofil aus Analyse erstellen</button>
              </div>
              </div>
            </div>
          </div>
        </div>
      `;

      const createProfileButton = document.getElementById('migration-create-profile-from-analysis');
      if (createProfileButton) {
        createProfileButton.addEventListener('click', createMigrationProfileFromAnalysis);
      }
    }

    showInfo('Datenquelle analysiert - Datenschutz-Check abgeschlossen');
  } catch (error) {
    showError('Migrations-Analyse fehlgeschlagen: ' + (error instanceof Error ? error.message : String(error)));
  }
}

async function analyzeMigrationSourceFile(file) {
  if (!file) {
    showError('Bitte zuerst eine Datei auswählen.');
    return;
  }

  try {
    clearError();
    showInfo('Datei wird analysiert und Felddefinitionen werden vorbereitet...');

    const result = await requestJson('/api/migrations/analyze-import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fileName: file.name,
        contentBase64: await fileToBase64(file)
      })
    });

    const primary = Array.isArray(result.sheets) && result.sheets.length ? result.sheets[0] : result;
    const headers = Array.isArray(primary.headers) ? primary.headers : [];
    if (!headers.length) {
      showError('Aus der Datei konnten keine Feldnamen gelesen werden.');
      return;
    }

    const fieldDefinitions = headers.map((header) => ({
      name: String(header || '').trim(),
      type: 'string'
    })).filter((item) => item.name);

    const defsEl = document.getElementById('migration-field-defs');
    if (defsEl) {
      defsEl.value = JSON.stringify(fieldDefinitions, null, 2);
    }

    const sourceNameEl = document.getElementById('migration-source-name');
    if (sourceNameEl && !String(sourceNameEl.value || '').trim()) {
      sourceNameEl.value = String(file.name || '').replace(/.[^.]+$/, '');
    }

    const sourceTypeEl = document.getElementById('migration-source-type');
    if (sourceTypeEl) {
      const format = String(result.format || '').toLowerCase();
      sourceTypeEl.value = format === 'excel' ? 'FILE_XLSX' : format === 'csv' ? 'FILE_CSV' : 'OTHER';
    }

    const recordCountEl = document.getElementById('migration-est-records');
    if (recordCountEl && !String(recordCountEl.value || '').trim()) {
      const recordCount = Number(primary.recordCount || 0);
      if (recordCount > 0) {
        recordCountEl.value = String(recordCount);
      }
    }

    const metaEl = document.getElementById('migration-analysis-file-meta');
    if (metaEl) {
      metaEl.textContent = file.name + ' • ' + fieldDefinitions.length + ' Felder erkannt';
    }

    showInfo('Datei analysiert: Felddefinitionen wurden automatisch übernommen.');
  } catch (error) {
    showError('Datei-Analyse fehlgeschlagen: ' + (error instanceof Error ? error.message : String(error)));
  }
}

function setupMigrationAnalysisDropzone() {
  const dropzone = document.getElementById('migration-analysis-dropzone');
  const input = document.getElementById('migration-analysis-file');
  const pickButton = document.getElementById('migration-analysis-file-pick');
  if (!dropzone || !input || dropzone.dataset.bound === '1') {
    return;
  }

  dropzone.dataset.bound = '1';

  const setDropzoneState = (state) => {
    dropzone.classList.remove('is-active', 'border-success', 'border-danger', 'bg-success-subtle', 'bg-danger-subtle');
    if (state === 'active') {
      dropzone.classList.add('is-active');
      return;
    }
    if (state === 'success') {
      dropzone.classList.add('border-success', 'bg-success-subtle');
      return;
    }
    if (state === 'error') {
      dropzone.classList.add('border-danger', 'bg-danger-subtle');
    }
  };

  const flashDropzoneState = (state) => {
    setDropzoneState(state);
    window.setTimeout(() => {
      setDropzoneState('idle');
    }, 1800);
  };

  const handleFiles = async (files) => {
    const selected = Array.isArray(files) ? files.filter(Boolean) : [];
    if (!selected.length) {
      return;
    }

    const file = selected[0];
    if (!isSupportedMigrationImportFile(file)) {
      showError('Unterstützte Dateitypen sind CSV, TXT, JSON und Excel.');
      flashDropzoneState('error');
      return;
    }

    try {
      await analyzeMigrationSourceFile(file);
      flashDropzoneState('success');
    } catch {
      flashDropzoneState('error');
    }
  };

  pickButton.addEventListener('click', () => {
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
      setDropzoneState('active');
    });
  });

  ['dragleave', 'dragend'].forEach((eventName) => {
    dropzone.addEventListener(eventName, () => {
      setDropzoneState('idle');
    });
  });

  dropzone.addEventListener('drop', async (event) => {
    event.preventDefault();
    setDropzoneState('idle');
    const files = event.dataTransfer && event.dataTransfer.files
      ? Array.from(event.dataTransfer.files)
      : [];
    await handleFiles(files);
  });
}

async function previewMapping() {
  const mappingDefinition = document.getElementById('mapping-definition').value;
  const sourceData = JSON.parse(document.getElementById('mapping-source').value || '[]');
  const result = await requestJson('/api/mappings/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mappingDefinition, sourceData })
  });
  document.getElementById('mapping-output').textContent = JSON.stringify(result, null, 2);
}
