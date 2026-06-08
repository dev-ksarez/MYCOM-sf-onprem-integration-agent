// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-scheduler-mapping
// Source lines: 3165–7137
// ──────────────────────────────────────────────────────────────────────

function isSchedulerMssqlUpsertSelection() {
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const operation = normalizeOperationValue(document.getElementById('sch-operation')?.value || '');
  return targetType === 'MSSQL' && String(operation || '').toLowerCase() === 'upsert';
}

function ensureSalesforceTargetDefinition() {
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const targetSystem = resolveEffectiveTargetSystem();
  const isSalesforce = targetType === 'SALESFORCE' && targetSystem === 'Salesforce';
  const isMssql = isSchedulerMssqlUpsertSelection();
  if (!isSalesforce && !isMssql) {
    return;
  }

  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  if (!objectApiName && isSalesforce) {
    return;
  }

  const targetDefinitionInput = document.getElementById('sch-target-definition');
  const raw = String(targetDefinitionInput?.value || '').trim();
  const upsertField = String(document.getElementById('sch-external-id-field')?.value || '').trim();
  const pricebook2Id = String(document.getElementById('sch-pricebook2id')?.value || '').trim();
  const missingProductStrategy = String(document.getElementById('sch-missing-product-strategy')?.value || 'error').trim();
  const operation = String(normalizeOperationValue(document.getElementById('sch-operation')?.value || 'Upsert') || 'Upsert').toLowerCase();
  const nextDefinition = isSalesforce
    ? {
        objectApiName,
        operation
      }
    : {
        upsertKey: upsertField
      };

  if (isSalesforce && operation === 'upsert' && upsertField) {
    nextDefinition.externalIdField = upsertField;
  }

  if (isSalesforce && objectApiName === 'PricebookEntry' && pricebook2Id) {
    nextDefinition.pricebook2Id = pricebook2Id;
  }
  if (isSalesforce && objectApiName === 'PricebookEntry' && missingProductStrategy === 'skip') {
    nextDefinition.missingProductStrategy = 'skip';
  }

  if (isMssql && upsertField) {
    nextDefinition.upsertKey = upsertField;
  }

  if (!raw) {
    targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
    return;
  }

  try {
    const parsed = JSON.parse(raw);
    const targetDefinition = getSchedulerSelectedTargetDefinitionWritableContainer(parsed) || parsed;
    if (isSalesforce) {
      targetDefinition.objectApiName = objectApiName;
      targetDefinition.operation = operation;
      if (operation === 'upsert') {
        if (upsertField) {
          targetDefinition.externalIdField = upsertField;
        }
      } else if ('externalIdField' in targetDefinition) {
        delete targetDefinition.externalIdField;
      }
      if (Array.isArray(parsed.importProfiles)) {
        if (operation === 'upsert' && String(targetDefinition.externalIdField || '').trim()) {
          parsed.externalIdField = String(targetDefinition.externalIdField || '').trim();
        } else if ('externalIdField' in parsed) {
          delete parsed.externalIdField;
        }
      }
      if (objectApiName === 'PricebookEntry' && pricebook2Id) {
        targetDefinition.pricebook2Id = pricebook2Id;
      } else if ('pricebook2Id' in targetDefinition) {
        delete targetDefinition.pricebook2Id;
      }
      if (objectApiName === 'PricebookEntry' && missingProductStrategy === 'skip') {
        targetDefinition.missingProductStrategy = 'skip';
      } else if ('missingProductStrategy' in targetDefinition) {
        delete targetDefinition.missingProductStrategy;
      }
    }
    if (isMssql) {
      if (upsertField) {
        targetDefinition.upsertKey = upsertField;
      } else if ('upsertKey' in targetDefinition) {
        delete targetDefinition.upsertKey;
      }
    }
    targetDefinitionInput.value = JSON.stringify(parsed, null, 2);
  } catch {
    targetDefinitionInput.value = JSON.stringify(nextDefinition, null, 2);
  }
}

function isSchedulerSalesforceUpsertSelection() {
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const targetSystem = resolveEffectiveTargetSystem();
  const operation = normalizeOperationValue(document.getElementById('sch-operation')?.value || '');
  return targetType === 'SALESFORCE' && targetSystem === 'Salesforce' && String(operation || '').toLowerCase() === 'upsert';
}

function getSchedulerSelectedTargetDefinitionContainer(parsed) {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }

  const baseTargetDefinition = parsed;

  if (!Array.isArray(parsed.importProfiles) || !parsed.importProfiles.length) {
    return parsed;
  }

  const selectedName = String(parsed.selectedImportProfileName || '').trim();
  const selectedProfile = (selectedName
    ? parsed.importProfiles.find((profile) => String(profile?.name || '').trim() === selectedName)
    : parsed.importProfiles[0]) || parsed.importProfiles[0];

  if (!selectedProfile || typeof selectedProfile !== 'object' || Array.isArray(selectedProfile)) {
    return parsed;
  }

  if (selectedProfile.target && typeof selectedProfile.target === 'object' && !Array.isArray(selectedProfile.target)) {
    return {
      ...baseTargetDefinition,
      ...selectedProfile.target
    };
  }

  return {
    ...baseTargetDefinition,
    ...selectedProfile
  };
}

function getSchedulerSelectedTargetDefinitionWritableContainer(parsed) {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }

  if (!Array.isArray(parsed.importProfiles) || !parsed.importProfiles.length) {
    return parsed;
  }

  const selectedName = String(parsed.selectedImportProfileName || '').trim();
  const selectedProfile = (selectedName
    ? parsed.importProfiles.find((profile) => String(profile?.name || '').trim() === selectedName)
    : parsed.importProfiles[0]) || parsed.importProfiles[0];

  if (!selectedProfile || typeof selectedProfile !== 'object' || Array.isArray(selectedProfile)) {
    return parsed;
  }

  if (selectedProfile.target && typeof selectedProfile.target === 'object' && !Array.isArray(selectedProfile.target)) {
    return selectedProfile.target;
  }

  return selectedProfile;
}

function getSchedulerTargetDefinitionUpsertFieldValue() {
  const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (!raw) {
    return '';
  }

  try {
    const parsed = JSON.parse(raw);
    const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
    if (isSchedulerSalesforceUpsertSelection()) {
      return String(targetDefinition?.externalIdField || '').trim();
    }
    if (isSchedulerMssqlUpsertSelection()) {
      return String(targetDefinition?.upsertKey || '').trim();
    }
    return '';
  } catch {
    return '';
  }
}

function getSchedulerGlobalPicklistTargetFields() {
  const fallbackFields = [
    { name: 'ApiName', label: 'API Name', type: 'string', requiredOnCreate: true },
    { name: 'Label', label: 'Label', type: 'string', requiredOnCreate: true }
  ];
  const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (!raw) {
    return fallbackFields;
  }

  try {
    const parsed = JSON.parse(raw);
    const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
    const externalIdField = String(targetDefinition?.externalIdField || '').trim() || 'ApiName';
    const labelField = String(targetDefinition?.labelField || '').trim() || 'Label';
    const fields = [
      { name: externalIdField, label: externalIdField === 'ApiName' ? 'API Name' : externalIdField, type: 'string', requiredOnCreate: true },
      { name: labelField, label: labelField, type: 'string', requiredOnCreate: true }
    ];
    const seen = new Set();
    return fields.filter((field) => {
      const key = normalizeFieldKey(field.name);
      if (!key || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  } catch {
    return fallbackFields;
  }
}

function getSchedulerMappingRules() {
  const raw = String(document.getElementById('sch-mapping')?.value || '').trim();
  if (!raw || !raw.startsWith('[')) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function getSchedulerMappedSourceFieldsForTargetField(targetField) {
  const normalizedTargetField = normalizeFieldKey(targetField);
  if (!normalizedTargetField) {
    return [];
  }

  return getSchedulerMappingRules()
    .filter((rule) => normalizeFieldKey(rule?.targetField) === normalizedTargetField)
    .map((rule) => String(rule?.sourceField || '').trim())
    .filter(Boolean);
}

function getSchedulerSalesforceUpsertHeuristicWarning() {
  if (!isSchedulerSalesforceUpsertSelection()) {
    return '';
  }

  const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
  if (!upsertField) {
    return '';
  }

  const mappedSourceFields = getSchedulerMappedSourceFieldsForTargetField(upsertField);
  if (!mappedSourceFields.length) {
    return '';
  }

  const primarySourceField = mappedSourceFields[0];
  const normalizedUpsertField = normalizeFieldKey(upsertField);
  const normalizedPrimarySourceField = normalizeFieldKey(primarySourceField);
  const sourceDefinitionText = String(document.getElementById('sch-source-definition')?.value || '').trim().toLowerCase();

  let suggestedSourceField = '';
  if ((normalizedUpsertField === 'erpaccountnumberc' || normalizedUpsertField === 'erpcontactnumberc') && sourceDefinitionText.includes('external_key')) {
    suggestedSourceField = 'external_key';
  } else if (normalizedUpsertField === 'erpordernumberc' && sourceDefinitionText.includes('order_number')) {
    suggestedSourceField = 'order_number';
  } else if (normalizedUpsertField === 'erpquotenumberc' && sourceDefinitionText.includes('quote_number')) {
    suggestedSourceField = 'quote_number';
  } else if (normalizedUpsertField === 'erpproductcodec' && sourceDefinitionText.includes('product_code')) {
    suggestedSourceField = 'product_code';
  }

  if (!suggestedSourceField || normalizeFieldKey(suggestedSourceField) === normalizedPrimarySourceField) {
    return '';
  }

  return 'Hinweis: Das Upsert-Feld ' + upsertField + ' ist aktuell mit ' + primarySourceField + ' verknuepft. Die Quelle enthaelt auch ' + suggestedSourceField + ', was fuer dieses Feld typischer wirkt.';
}

function hasSchedulerPricebook2IdMapping() {
  return getSchedulerMappingRules().some((rule) => String(rule?.targetField || '').trim() === 'Pricebook2Id');
}

function getSchedulerMappedStaticPricebook2IdValue() {
  const match = getSchedulerMappingRules().find((rule) => (
    String(rule?.targetField || '').trim() === 'Pricebook2Id'
    && String(rule?.transformFunction || '').trim().toUpperCase() === 'STATIC'
    && String(rule?.transformExpression || '').trim()
  ));

  return String(match?.transformExpression || '').trim();
}

function getSchedulerTargetDefinitionPricebook2IdValue() {
  const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (!raw) {
    return '';
  }

  try {
    const parsed = JSON.parse(raw);
    const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
    return String(targetDefinition?.pricebook2Id || '').trim();
  } catch {
    return '';
  }
}

function getSchedulerTargetDefinitionMissingProductStrategyValue() {
  const raw = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (!raw) {
    return 'error';
  }

  try {
    const parsed = JSON.parse(raw);
    const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
    const strategy = String(targetDefinition?.missingProductStrategy || 'error').trim().toLowerCase();
    return strategy === 'skip' ? 'skip' : 'error';
  } catch {
    return 'error';
  }
}

function hasSchedulerPricebook2IdConfigured() {
  return Boolean(
    String(document.getElementById('sch-pricebook2id')?.value || '').trim()
    || getSchedulerTargetDefinitionPricebook2IdValue()
    || getSchedulerMappedStaticPricebook2IdValue()
    || hasSchedulerPricebook2IdMapping()
  );
}

function isSchedulerPricebookEntryProductCodeSelection() {
  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
  return isSchedulerSalesforceUpsertSelection() && objectApiName === 'PricebookEntry' && upsertField === 'ProductCode';
}

function syncSchedulerTargetDefinitionEditorState() {
  const targetDefinitionInput = document.getElementById('sch-target-definition');
  const targetDefinitionHelp = document.getElementById('sch-target-definition-help');
  const shouldMirrorReadonly = isSchedulerSalesforceUpsertSelection()
    && String(document.getElementById('sch-object')?.value || '').trim() === 'PricebookEntry';

  if (targetDefinitionInput) {
    targetDefinitionInput.readOnly = shouldMirrorReadonly;
    targetDefinitionInput.classList.toggle('bg-light', shouldMirrorReadonly);
  }

  if (targetDefinitionHelp) {
    targetDefinitionHelp.textContent = shouldMirrorReadonly
      ? 'Wird aus Objekt, Operation, Upsert-Feld und Pricebook2Id gespiegelt. Fuer PricebookEntry bitte die sichtbaren Felder oberhalb verwenden.'
      : '';
  }
}

function getSchedulerSalesforceUpsertConstraintMessage() {
  if (!isSchedulerSalesforceUpsertSelection()) {
    return '';
  }

  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  const upsertField = String(document.getElementById('sch-external-id-field')?.value || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();

  if (!upsertField) {
    return 'Bitte waehle ein Upsert-Feld fuer die Salesforce-Zielkonfiguration.';
  }

  const rawTargetDefinition = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (rawTargetDefinition) {
    try {
      const parsed = JSON.parse(rawTargetDefinition);
      if (Array.isArray(parsed?.importProfiles)) {
        const baseExternalIdField = String(parsed?.externalIdField || '').trim();
        if (baseExternalIdField && baseExternalIdField !== upsertField) {
          parsed.externalIdField = upsertField;
          document.getElementById('sch-target-definition').value = JSON.stringify(parsed, null, 2);
        }
      }
    } catch {
      // JSON validity is handled elsewhere.
    }
  }

  const providedTargetFieldKeys = getProvidedSchedulerTargetFieldKeys();
  if (!providedTargetFieldKeys.has(normalizeFieldKey(upsertField))) {
    return 'Das Upsert-Feld ' + upsertField + ' wird im Mapping oder als statischer Zielwert nicht bereitgestellt.';
  }

  if (objectApiName === 'PricebookEntry' && upsertField === 'ProductCode' && !hasSchedulerPricebook2IdConfigured()) {
    return 'ProductCode ist für PricebookEntry nur zulässig, wenn Pricebook2Id als Ziel-Feld oder Mapping gesetzt ist.';
  }

  return '';
}

function updateSchedulerExternalIdValidationState() {
  const select = document.getElementById('sch-external-id-field');
  const help = document.getElementById('sch-external-id-help');
  const pricebookInput = document.getElementById('sch-pricebook2id');
  const pricebookHelp = document.getElementById('sch-pricebook2id-help');
  const baseHelpText = String(help?.dataset.baseText || 'Wählen Sie das Feld, das für Upsert verwendet werden soll.');
  const message = getSchedulerSalesforceUpsertConstraintMessage();
  const warning = message ? '' : getSchedulerSalesforceUpsertHeuristicWarning();
  const requiresPricebook2Id = isSchedulerPricebookEntryProductCodeSelection();

  if (select) {
    select.classList.toggle('is-invalid', Boolean(message));
  }

  if (pricebookInput) {
    pricebookInput.classList.toggle('is-invalid', Boolean(message) && requiresPricebook2Id && !String(pricebookInput.value || '').trim() && !hasSchedulerPricebook2IdMapping());
  }

  if (help) {
    help.textContent = message || warning || baseHelpText;
    help.classList.toggle('text-danger', Boolean(message));
    help.classList.toggle('text-warning', !message && Boolean(warning));
  }

  if (pricebookHelp) {
    const basePricebookHelp = String(pricebookHelp.dataset.baseText || 'Optional als festes Ziel-Pricebook für PricebookEntry-Upserts.');
    pricebookHelp.textContent = message && requiresPricebook2Id ? message : basePricebookHelp;
    pricebookHelp.classList.toggle('text-danger', Boolean(message) && requiresPricebook2Id);
  }

  return message;
}

async function loadSchedulerPricebookOptions(preferredValue) {
  const pricebookSelect = document.getElementById('sch-pricebook2id');
  if (!pricebookSelect) {
    return [];
  }

  if (!isSchedulerSalesforceUpsertSelection() || String(document.getElementById('sch-object')?.value || '').trim() !== 'PricebookEntry') {
    pricebookSelect.innerHTML = '<option value="">- Pricebook wählen -</option>';
    pricebookSelect.value = '';
    return [];
  }

  const currentValue = String(preferredValue || getSchedulerTargetDefinitionPricebook2IdValue() || getSchedulerMappedStaticPricebook2IdValue() || '').trim();

  try {
    const res = await fetch('/api/salesforce/pricebooks?instanceId=' + encodeURIComponent(state.instanceId || ''));
    if (!res.ok) {
      pricebookSelect.innerHTML = '<option value="">Pricebooks konnten nicht geladen werden</option>';
      return [];
    }

    const payload = await res.json();
    const normalizedPricebooks = Array.isArray(payload) ? payload : [];
    pricebookSelect.innerHTML = '<option value="">- Pricebook wählen -</option>' + normalizedPricebooks.map((pricebook) => {
      const id = String(pricebook?.id || '').trim();
      const name = String(pricebook?.name || id).trim();
      const suffix = pricebook?.isStandard === true
        ? 'Standard'
        : pricebook?.isActive === true
          ? 'Aktiv'
          : 'Inaktiv';
      const label = name && name !== id ? name + ' (' + suffix + ')' : id;
      return '<option value="' + esc(id) + '"' + (currentValue === id ? ' selected' : '') + '>' + esc(label) + '</option>';
    }).join('');
    if (currentValue && !normalizedPricebooks.some((pricebook) => String(pricebook?.id || '').trim() === currentValue)) {
      pricebookSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
    }
    if (currentValue) {
      pricebookSelect.value = currentValue;
    }
    return normalizedPricebooks;
  } catch {
    pricebookSelect.innerHTML = '<option value="">Pricebooks konnten nicht geladen werden</option>';
    return [];
  }
}

async function loadConnectorTaskOwnerOptions(preferredValue) {
  const ownerSelect = document.getElementById('con-task-owner-id');
  if (!ownerSelect) {
    return [];
  }

  const currentValue = String(preferredValue || ownerSelect.value || '').trim();
  ownerSelect.innerHTML = '<option value="">- Benutzer wählen -</option>';

  try {
    const res = await fetch('/api/salesforce/users?instanceId=' + encodeURIComponent(state.instanceId || ''));
    if (!res.ok) {
      ownerSelect.innerHTML = '<option value="">Benutzer konnten nicht geladen werden</option>';
      if (currentValue) {
        ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (gespeichert)') + '</option>';
      }
      return [];
    }

    const payload = await res.json();
    const normalizedUsers = Array.isArray(payload) ? payload : [];
    ownerSelect.innerHTML = '<option value="">- Benutzer wählen -</option>' + normalizedUsers.map((user) => {
      const id = String(user?.id || '').trim();
      const name = String(user?.name || '').trim();
      const username = String(user?.username || '').trim();
      const labelBase = name && username && name !== username
        ? name + ' (' + username + ')'
        : (name || username || id);
      const label = user?.isActive === true ? labelBase : labelBase + ' (inaktiv)';
      return '<option value="' + esc(id) + '" data-username="' + esc(username) + '"' + (currentValue === id ? ' selected' : '') + '>' + esc(label) + '</option>';
    }).join('');
    if (currentValue && !normalizedUsers.some((user) => String(user?.id || '').trim() === currentValue)) {
      ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
    }
    if (currentValue) {
      ownerSelect.value = currentValue;
    }
    return normalizedUsers;
  } catch {
    ownerSelect.innerHTML = '<option value="">Benutzer konnten nicht geladen werden</option>';
    if (currentValue) {
      ownerSelect.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (gespeichert)') + '</option>';
      ownerSelect.value = currentValue;
    }
    return [];
  }
}

async function loadSchedulerExternalIdOptions(selectedValue) {
  const select = document.getElementById('sch-external-id-field');
  if (!select) {
    return [];
  }

  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  if (!objectApiName || !isSchedulerSalesforceUpsertSelection()) {
    select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
    select.value = '';
    return [];
  }

  try {
    const res = await fetch('/api/salesforce/object-fields?object=' + encodeURIComponent(objectApiName) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
    if (!res.ok) {
      select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
      return [];
    }

    const fields = await res.json();
    const externalIdFields = (Array.isArray(fields) ? fields : []).filter((field) => field && field.isExternalId === true);
    const currentValue = String(selectedValue || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
    const allowPricebookProductCode = objectApiName === 'PricebookEntry' && hasSchedulerPricebook2IdConfigured();
    select.innerHTML = '<option value="">- Upsert Feld wählen -</option>' + externalIdFields.map((field) => {
      const name = String(field?.name || '').trim();
      const label = String(field?.label || '').trim();
      const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
      return '<option value="' + esc(name) + '"' + (currentValue === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
    }).join('');
    if (objectApiName === 'PricebookEntry') {
      if (allowPricebookProductCode) {
        select.innerHTML += '<option value="ProductCode"' + (currentValue === 'ProductCode' ? ' selected' : '') + '>ProductCode (Composite Key mit Pricebook2Id)</option>';
      } else if (currentValue === 'ProductCode') {
        select.innerHTML += '<option value="ProductCode" selected>ProductCode (Pricebook2Id-Mapping fehlt)</option>';
      }
    }
    if (currentValue && !externalIdFields.some((field) => String(field?.name || '').trim() === currentValue)) {
      if (!(objectApiName === 'PricebookEntry' && currentValue === 'ProductCode')) {
        select.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
      }
    }
    if (currentValue) {
      select.value = currentValue;
    }
    return externalIdFields;
  } catch {
    select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
    return [];
  }
}

async function syncSchedulerExternalIdUi(selectedValue) {
  const wrap = document.getElementById('sch-external-id-wrap');
  const help = document.getElementById('sch-external-id-help');
  const label = document.getElementById('sch-external-id-label');
  const select = document.getElementById('sch-external-id-field');
  const pricebookWrap = document.getElementById('sch-pricebook2id-wrap');
  const pricebookInput = document.getElementById('sch-pricebook2id');
  const pricebookHelp = document.getElementById('sch-pricebook2id-help');
  const missingProductStrategyWrap = document.getElementById('sch-missing-product-strategy-wrap');
  const missingProductStrategyInput = document.getElementById('sch-missing-product-strategy');
  const isSalesforce = isSchedulerSalesforceUpsertSelection();
  const isMssql = isSchedulerMssqlUpsertSelection();
  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  const show = isSalesforce || isMssql;
  if (wrap) {
    wrap.classList.toggle('d-none', !show);
  }

  if (!show) {
    syncSchedulerTargetDefinitionEditorState();
    if (select) {
      select.innerHTML = '<option value="">- Upsert Feld wählen -</option>';
      select.value = '';
    }
    if (label) {
      label.textContent = 'Upsert Feld';
    }
    if (help) {
      help.textContent = 'Wählen Sie das Feld, das für Upsert verwendet werden soll.';
    }
    if (pricebookWrap) {
      pricebookWrap.classList.add('d-none');
    }
    if (pricebookInput) {
      pricebookInput.value = '';
    }
    if (missingProductStrategyWrap) {
      missingProductStrategyWrap.classList.add('d-none');
    }
    if (missingProductStrategyInput) {
      missingProductStrategyInput.value = 'error';
    }
    return;
  }

  const showPricebook2Id = isSalesforce && objectApiName === 'PricebookEntry';
  syncSchedulerTargetDefinitionEditorState();
  if (pricebookWrap) {
    pricebookWrap.classList.toggle('d-none', !showPricebook2Id);
  }
  if (pricebookInput && showPricebook2Id) {
    await loadSchedulerPricebookOptions(
      String(
        pricebookInput.value
        || getSchedulerTargetDefinitionPricebook2IdValue()
        || getSchedulerMappedStaticPricebook2IdValue()
        || ''
      ).trim()
    );
  } else if (pricebookInput) {
    pricebookInput.value = '';
  }
  if (pricebookHelp) {
    pricebookHelp.dataset.baseText = showPricebook2Id
      ? 'Festes Ziel-Pricebook für PricebookEntry-Upserts auswählen. Leer lassen, wenn Pricebook2Id aus dem Mapping kommt.'
      : 'Optional als festes Ziel-Pricebook für PricebookEntry-Upserts.';
    pricebookHelp.textContent = pricebookHelp.dataset.baseText;
  }
  if (missingProductStrategyWrap) {
    missingProductStrategyWrap.classList.toggle('d-none', !showPricebook2Id);
  }
  if (missingProductStrategyInput) {
    missingProductStrategyInput.value = showPricebook2Id
      ? getSchedulerTargetDefinitionMissingProductStrategyValue()
      : 'error';
  }

  if (isMssql) {
    const currentValue = String(selectedValue || getSchedulerTargetDefinitionUpsertFieldValue() || '').trim();
    const fields = Array.isArray(state.targetFields) ? state.targetFields : [];
    if (select) {
      select.innerHTML = '<option value="">- Upsert Feld wählen -</option>' + fields.map((field) => {
        const name = String(field?.name || '').trim();
        const optionLabel = String(field?.label || '').trim() || name;
        return '<option value="' + esc(name) + '"' + (currentValue === name ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
      }).join('');
      if (currentValue && !fields.some((field) => String(field?.name || '').trim() === currentValue)) {
        select.innerHTML += '<option value="' + esc(currentValue) + '" selected>' + esc(currentValue + ' (nicht mehr gefunden)') + '</option>';
      }
      if (currentValue) {
        select.value = currentValue;
      }
    }
    if (help) {
      help.dataset.baseText = fields.length
        ? 'Dieses Zieltabellen-Feld wird für Upsert als Match-Kriterium verwendet. Ohne Auswahl gilt der Connector-Default.'
        : 'Zuerst Zielobjekt und Connector wählen, damit die MSSQL-Felder geladen werden können.';
      help.textContent = help.dataset.baseText;
    }
    updateSchedulerExternalIdValidationState();
    return;
  }

  const options = await loadSchedulerExternalIdOptions(selectedValue);
  if (help) {
    help.dataset.baseText = options.length
      ? 'Nur echte Salesforce External-ID-Felder werden angeboten.'
      : 'Für dieses Objekt wurden keine External-ID-Felder gefunden.';
    help.textContent = help.dataset.baseText;
  }
  updateSchedulerExternalIdValidationState();
}

function toggleCreateObjectFromSourceUi() {
  const wrap = document.getElementById('sch-create-object-wrap');
  if (!wrap) {
    return;
  }
  wrap.classList.toggle('d-none', !isSalesforceTargetSelection());
}

function setCreateObjectStatus(message, level) {
  const el = document.getElementById('sch-create-object-status');
  if (!el) {
    return;
  }

  el.textContent = message || '';
  el.classList.remove('text-secondary', 'text-success', 'text-danger', 'text-warning');
  if (level === 'success') {
    el.classList.add('text-success');
    return;
  }
  if (level === 'error') {
    el.classList.add('text-danger');
    return;
  }
  if (level === 'warning') {
    el.classList.add('text-warning');
    return;
  }
  el.classList.add('text-secondary');
}

function mapSourceTypeToDefaultOverride(sourceType) {
  const normalized = String(sourceType || '').trim().toLowerCase();
  if (normalized === 'boolean' || normalized === 'bool') return 'Checkbox';
  if (normalized === 'date') return 'Date';
  if (normalized === 'datetime' || normalized === 'timestamp') return 'DateTime';
  if (normalized.includes('int') || normalized === 'number' || normalized === 'double' || normalized === 'float' || normalized === 'decimal') return 'Number';
  return 'Text';
}

function renderCreateObjectFieldOverrides() {
  const body = document.getElementById('sch-create-object-field-overrides');
  if (!body) {
    return;
  }

  const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
  if (!sourceFields.length) {
    body.innerHTML = '<tr><td colspan="3" class="text-secondary">Quellfelder laden, um Overrides zu setzen.</td></tr>';
    return;
  }

  const typeOptions = ['Text', 'Number', 'Date', 'DateTime', 'Checkbox'];
  body.innerHTML = sourceFields.map((field) => {
    const sourceName = String(field?.name || '').trim();
    const sourceType = String(field?.type || 'string').trim();
    const selected = String(state.customObjectFieldOverrides[sourceName] || mapSourceTypeToDefaultOverride(sourceType));
    return '<tr data-source-field="' + esc(sourceName) + '">' +
      '<td>' + esc(sourceName) + '</td>' +
      '<td>' + esc(sourceType) + '</td>' +
      '<td><select class="form-select form-select-sm sch-field-override-type">' +
        typeOptions.map((option) => '<option value="' + esc(option) + '" ' + (option === selected ? 'selected' : '') + '>' + esc(option) + '</option>').join('') +
      '</select></td>' +
    '</tr>';
  }).join('');

  body.querySelectorAll('.sch-field-override-type').forEach((selectEl) => {
    selectEl.addEventListener('change', (event) => {
      const row = event.target?.closest ? event.target.closest('tr[data-source-field]') : null;
      const sourceName = String(row?.getAttribute('data-source-field') || '').trim();
      if (!sourceName) {
        return;
      }
      state.customObjectFieldOverrides[sourceName] = String(event.target?.value || '').trim();
    });
  });
}

function normalizeFieldKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

async function generateSalesforceMappings(sourceFields, targetFields, options) {
  const targetObjectApiName = String(options?.targetObjectApiName || '').trim();
  const profile = String(options?.profile || (
    targetObjectApiName === 'PricebookEntry'
      ? 'salesforce-pricebook'
      : (targetObjectApiName === 'Product2' ? 'salesforce-product' : 'standard')
  )).trim();
  const response = await requestJson('/api/salesforce/generate-mapping', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sourceFields: Array.isArray(sourceFields) ? sourceFields : [],
      targetFields: Array.isArray(targetFields) ? targetFields : [],
      targetObjectApiName,
      profile
    })
  });
  return Array.isArray(response?.items) ? response.items : [];
}

function getSalesforceMappingAssistantProfiles(targetObjectApiName) {
  const objectName = String(targetObjectApiName || '').trim();
  const profiles = [
    { value: 'standard', label: 'Standard', hint: 'Allgemeines Salesforce-Feldmapping per Name, Label und Basis-Aliase.' },
    { value: 'salesforce-product', label: 'Produkt', hint: 'Erweitert das Mapping für Produktfelder wie Produktcode, Familie und Beschreibung.' }
  ];

  if (objectName === 'PricebookEntry' || objectName === 'Product2') {
    profiles.push({
      value: 'salesforce-pricebook',
      label: 'Preisbuch',
      hint: 'Berücksichtigt Preis-, Preisbuch- und Produkt-Lookup-Felder für Salesforce Pricebook/Preise.'
    });
  }

  return profiles;
}

function getDefaultSalesforceMappingAssistantProfile(targetObjectApiName) {
  const objectName = String(targetObjectApiName || '').trim();
  if (objectName === 'PricebookEntry') return 'salesforce-pricebook';
  if (objectName === 'Product2') return 'salesforce-product';
  return 'standard';
}

function renderSchedulerMappingAssistant() {
  const profileSelect = document.getElementById('sch-mapping-assistant-profile');
  const hint = document.getElementById('sch-mapping-assistant-hint');
  if (!profileSelect || !hint) {
    return;
  }

  const objectName = String(document.getElementById('sch-object')?.value || '').trim();
  const profiles = getSalesforceMappingAssistantProfiles(objectName);
  const currentProfile = String(state.scheduleMappingAssistantProfile || getDefaultSalesforceMappingAssistantProfile(objectName)).trim();
  const allowedProfile = profiles.some((profile) => profile.value === currentProfile)
    ? currentProfile
    : getDefaultSalesforceMappingAssistantProfile(objectName);

  profileSelect.innerHTML = profiles.map((profile) =>
    '<option value="' + esc(profile.value) + '">' + esc(profile.label) + '</option>'
  ).join('');
  profileSelect.value = allowedProfile;
  state.scheduleMappingAssistantProfile = allowedProfile;

  const selectedProfile = profiles.find((profile) => profile.value === allowedProfile) || profiles[0];
  hint.textContent = selectedProfile?.hint || 'Erzeugt Salesforce-Mapping-Vorschläge für das aktuelle Zielobjekt.';
}

function getMigrationMappingAssistantProfile(objectId, targetObjectApiName) {
  const storedProfile = String(migState.mappingAssistantProfilesByObjectId?.[objectId] || '').trim();
  if (storedProfile) {
    return storedProfile;
  }
  return getDefaultSalesforceMappingAssistantProfile(targetObjectApiName);
}

function renderMigrationMappingAssistant(obj) {
  const shell = document.getElementById('mig-mapping-assistant-shell');
  if (!shell || !obj) {
    return;
  }

  const profiles = getSalesforceMappingAssistantProfiles(obj.salesforceObject);
  const currentProfile = getMigrationMappingAssistantProfile(obj.id, obj.salesforceObject);
  const allowedProfile = profiles.some((profile) => profile.value === currentProfile)
    ? currentProfile
    : getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject);
  migState.mappingAssistantProfilesByObjectId[obj.id] = allowedProfile;

  const selectedProfile = profiles.find((profile) => profile.value === allowedProfile) || profiles[0];
  shell.innerHTML =
    '<div class="scheduler-mapping-assistant-bar">' +
      '<div class="fw-semibold small">Mapping-Assistent</div>' +
      '<div class="small text-secondary" id="mig-mapping-assistant-hint">' + esc(selectedProfile?.hint || '') + '</div>' +
      '<div class="d-flex gap-2 align-items-end ms-auto">' +
        '<select id="mig-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;">' +
          profiles.map((profile) => '<option value="' + esc(profile.value) + '"' + (profile.value === allowedProfile ? ' selected' : '') + '>' + esc(profile.label) + '</option>').join('') +
        '</select>' +
        '<button type="button" class="btn btn-primary btn-sm sch-btn-iconized" id="mig-mapping-assistant-apply"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
      '</div>' +
    '</div>';

  const profileSelect = document.getElementById('mig-mapping-assistant-profile');
  const hint = document.getElementById('mig-mapping-assistant-hint');
  profileSelect?.addEventListener('change', () => {
    const nextProfile = String(profileSelect.value || getDefaultSalesforceMappingAssistantProfile(obj.salesforceObject)).trim();
    migState.mappingAssistantProfilesByObjectId[obj.id] = nextProfile;
    const nextSelected = profiles.find((profile) => profile.value === nextProfile) || profiles[0];
    if (hint) {
      hint.textContent = nextSelected?.hint || '';
    }
  });
}

function resolveSourceFieldName(value) {
  const requested = String(value || '').trim();
  if (!requested) {
    return '';
  }

  const fields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
  if (!fields.length) {
    return requested;
  }

  const exact = fields.find((field) => String(field?.name || '').trim() === requested);
  if (exact?.name) {
    return String(exact.name).trim();
  }

  const requestedKey = normalizeFieldKey(requested);
  const normalizedMatch = fields.find((field) => normalizeFieldKey(field?.name) === requestedKey);
  if (normalizedMatch?.name) {
    return String(normalizedMatch.name).trim();
  }

  return requested;
}

function reconcileMappingRuleSourceFields() {
  if (!Array.isArray(state.mappingRules) || !state.mappingRules.length) {
    return;
  }

  state.mappingRules = state.mappingRules.map((rule) => ({
    ...rule,
    sourceField: resolveSourceFieldName(rule?.sourceField)
  }));
}

function parseLegacyLookupValue(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return null;
  }
  const normalized = raw.startsWith('LOOKUP[') && raw.endsWith(']')
    ? raw.slice(7, -1)
    : raw;
  const separator = normalized.includes('|') ? '|' : (normalized.includes('.') ? '.' : '');
  if (!separator) {
    return null;
  }
  const parts = normalized.split(separator);
  const lookupObject = String(parts[0] || '').trim();
  const lookupField = String(parts.slice(1).join(separator) || '').trim();
  if (!lookupObject || !lookupField) {
    return null;
  }
  return { lookupObject, lookupField };
}

function matchesKnownTargetField(targetField, targetFields) {
  const requested = String(targetField || '').trim();
  if (!requested) {
    return false;
  }

  const requestedKey = normalizeFieldKey(requested);
  return targetFields.some((field) => {
    const apiName = String(field?.name || '').trim();
    const label = String(field?.label || '').trim();
    return apiName === requested
      || normalizeFieldKey(apiName) === requestedKey
      || (label && normalizeFieldKey(label) === requestedKey);
  });
}

function refreshSchedulerMappingCompatibilityState() {
  const mappingRules = Array.isArray(state.mappingRules) ? state.mappingRules : [];
  const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
  const targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
  if (!mappingRules.length || !sourceFields.length || !targetFields.length) {
    return;
  }

  const compatibleRules = mappingRules.filter((rule) => {
    const sourceField = resolveSourceFieldName(rule?.sourceField);
    const hasSourceMatch = sourceFields.some((field) => String(field?.name || '').trim() === sourceField);
    const hasTargetMatch = matchesKnownTargetField(rule?.targetField, targetFields);
    return hasSourceMatch && hasTargetMatch;
  });

  state.hasIncompatibleScheduleMappings = compatibleRules.length !== mappingRules.length;
}

function getOperationOptionsForTarget() {
  const targetSystem = normalizeSystemValue(document.getElementById('sch-target-system')?.value || '');
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const baseOperations = Array.isArray(state.scheduleOptions?.operations) && state.scheduleOptions.operations.length
    ? state.scheduleOptions.operations.map((item) => normalizeOperationValue(item)).filter(Boolean)
    : ['Insert', 'Update', 'Upsert', 'Delete'];

  // Global picklist sync should be idempotent and is best handled as upsert.
  if (targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
    return ['Upsert'];
  }

  if (targetSystem === 'Salesforce' || targetSystem === 'MS SQL') {
    const allowed = new Set(['Insert', 'Update', 'Upsert', 'Delete']);
    const filtered = baseOperations.filter((item) => allowed.has(item));
    return filtered.length ? filtered : ['Insert', 'Update', 'Upsert', 'Delete'];
  }

  return baseOperations;
}

function applyOperationOptions(selectedValue) {
  const currentValue = normalizeOperationValue(selectedValue || document.getElementById('sch-operation')?.value || '');
  renderSelectOptions('sch-operation', getOperationOptionsForTarget(), currentValue);
}

function renderSelectOptions(selectId, values, selectedValue) {
  const select = document.getElementById(selectId);
  if (!select) {
    return;
  }

  const normalizedValues = Array.from(new Set((values || []).filter(Boolean)));
  const finalValues = normalizedValues.slice();
  if (selectedValue && !finalValues.includes(selectedValue)) {
    finalValues.unshift(selectedValue);
  }

  select.innerHTML = '<option value="">- Wählen -</option>' + finalValues.map((value) =>
    '<option value="' + esc(value) + '">' + esc(value) + '</option>'
  ).join('');

  if (selectedValue) {
    select.value = selectedValue;
  }
}

function generateMappingRuleId() {
  return 'map-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}

function createMappingRuleFromSource(sourceField) {
  const sourceName = resolveSourceFieldName(sourceField?.name || sourceField || '');
  const sourceKey = normalizeFieldKey(sourceName);
  const isExternalIdentifier = sourceKey.includes('externalid') || sourceKey.includes('externalkey');
  const externalIdTarget = (() => {
    try {
      const targetDefinitionRaw = String(document.getElementById('sch-target-definition')?.value || '').trim();
      if (!targetDefinitionRaw) {
        return '';
      }
      const parsed = JSON.parse(targetDefinitionRaw);
      const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
      return String(targetDefinition?.externalIdField || '').trim();
    } catch {
      return '';
    }
  })();

  return {
    id: generateMappingRuleId(),
    sourceField: sourceName,
    sourceType: String(sourceField?.type || 'string'),
    targetField: externalIdTarget || sourceName,
    lookupEnabled: isExternalIdentifier,
    lookupObject: isExternalIdentifier ? String(document.getElementById('sch-object')?.value || '').trim() : '',
    lookupField: isExternalIdentifier ? (externalIdTarget || sourceName) : '',
    transformFunction: 'NONE',
    transformExpression: '',
    picklistMappings: []
  };
}

function toStoredMappingRule(rule) {
  return {
    sourceField: rule.sourceField,
    sourceType: rule.sourceType,
    targetType: rule.targetType || getSchedulerMappingRuleTargetType(rule),
    targetField: rule.targetField,
    lookupEnabled: !!rule.lookupEnabled,
    lookupObject: rule.lookupObject || '',
    lookupField: rule.lookupField || '',
    transformFunction: rule.transformFunction || 'NONE',
    transformExpression: rule.transformExpression || '',
    emailValidationEnabled: rule.emailValidationEnabled === true,
    emailInvalidAction: String(rule.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY',
    picklistMappings: Array.isArray(rule.picklistMappings) ? rule.picklistMappings : []
  };
}

function getSchedulerMappingRuleTargetMeta(rule) {
  const targetField = String(rule?.targetField || '').trim();
  if (!targetField) {
    return null;
  }
  return (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
    normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
  ) || null;
}

function getSchedulerMappingRuleTargetType(rule) {
  const type = String(getSchedulerMappingRuleTargetMeta(rule)?.type || rule?.targetType || rule?.sourceType || 'string').trim().toLowerCase();
  if (['int', 'integer'].includes(type)) return 'integer';
  if (['double', 'currency', 'percent', 'number'].includes(type)) return 'number';
  if (['boolean', 'checkbox'].includes(type)) return 'boolean';
  if (['date', 'datetime'].includes(type)) return 'datetime';
  return 'string';
}

function extractLookupTransformDetails(value) {
  const rawValue = String(value || '').trim();
  const lookupMatch = rawValue.match(/^LOOKUP\[([^|\]]+)\|([^\]]+)\]$/);
  if (!lookupMatch) {
    return null;
  }
  return {
    lookupObject: String(lookupMatch[1] || '').trim(),
    lookupField: String(lookupMatch[2] || '').trim()
  };
}

function parsePicklistMappingsText(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const splitToken = line.includes('=>') ? '=>' : '=';
      const parts = line.split(splitToken);
      return {
        source: String(parts[0] || '').trim(),
        target: String(parts.slice(1).join(splitToken) || '').trim()
      };
    })
    .filter((item) => item.source || item.target);
}

function formatPicklistMappingsText(mappings) {
  if (!Array.isArray(mappings) || !mappings.length) {
    return '';
  }
  return mappings
    .map((item) => String(item?.source || '').trim() + ' => ' + String(item?.target || '').trim())
    .join('\n');
}

function syncMappingDefinitionFromRules() {
  const mappingInput = document.getElementById('sch-mapping');
  if (!mappingInput) {
    return;
  }

  if (state.rawMappingEditorDirty) {
    return;
  }

  mappingInput.value = JSON.stringify(state.mappingRules.map(toStoredMappingRule), null, 2);
  state.rawMappingEditorDirty = false;
}

function isSchedulerSalesforceInsertOrUpsertSelection() {
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const targetSystem = resolveEffectiveTargetSystem();
  const operation = String(normalizeOperationValue(document.getElementById('sch-operation')?.value || '') || '').trim().toLowerCase();
  return targetType === 'SALESFORCE' && targetSystem === 'Salesforce' && (operation === 'insert' || operation === 'upsert');
}

function getRequiredSalesforceTargetFields() {
  if (!isSchedulerSalesforceInsertOrUpsertSelection()) {
    return [];
  }

  return (Array.isArray(state.targetFields) ? state.targetFields : []).filter((field) => field && field.requiredOnCreate === true);
}

function getProvidedSchedulerTargetFieldKeys() {
  const provided = new Set(
    (Array.isArray(state.mappingRules) ? state.mappingRules : [])
      .map((rule) => normalizeFieldKey(rule?.targetField))
      .filter(Boolean)
  );

  const rawTargetDefinition = String(document.getElementById('sch-target-definition')?.value || '').trim();
  if (!rawTargetDefinition) {
    return provided;
  }

  try {
    const parsed = JSON.parse(rawTargetDefinition);
    const targetDefinition = getSchedulerSelectedTargetDefinitionContainer(parsed) || parsed;
    const knownFieldKeys = new Set(
      (Array.isArray(state.targetFields) ? state.targetFields : [])
        .map((field) => normalizeFieldKey(field?.name))
        .filter(Boolean)
    );

    Object.entries(targetDefinition || {}).forEach(([key, value]) => {
      const normalizedKey = normalizeFieldKey(key);
      if (!normalizedKey || !knownFieldKeys.has(normalizedKey)) {
        return;
      }
      if (typeof value === 'string') {
        if (value.trim()) {
          provided.add(normalizedKey);
        }
        return;
      }
      if (typeof value === 'number' || typeof value === 'boolean') {
        provided.add(normalizedKey);
      }
    });
  } catch {
    return provided;
  }

  return provided;
}

function isPricebookEntryProductCodeUpsertSelection() {
  const objectName = String(document.getElementById('sch-object')?.value || '').trim();
  const operation = String(normalizeOperationValue(document.getElementById('sch-operation')?.value || '') || '').trim().toLowerCase();
  const externalIdField = String(document.getElementById('sch-external-id-field')?.value || '').trim().toLowerCase();
  return objectName === 'PricebookEntry' && operation === 'upsert' && externalIdField === 'productcode';
}

function getMissingRequiredSchedulerTargetFields() {
  const requiredFields = getRequiredSalesforceTargetFields();
  if (!requiredFields.length) {
    return [];
  }

  const providedTargetFieldKeys = getProvidedSchedulerTargetFieldKeys();
  return requiredFields.filter((field) => {
    const fieldKey = normalizeFieldKey(field?.name);
    if (
      fieldKey === 'product2id' &&
      isPricebookEntryProductCodeUpsertSelection() &&
      providedTargetFieldKeys.has('productcode')
    ) {
      return false;
    }
    return !providedTargetFieldKeys.has(fieldKey);
  });
}

function getRequiredSalesforceFieldSaveMessage() {
  if (!document.getElementById('sch-active')?.checked) {
    return '';
  }

  const missingRequiredFields = getMissingRequiredSchedulerTargetFields();
  if (!missingRequiredFields.length) {
    return '';
  }

  const objectName = String(document.getElementById('sch-object')?.value || '').trim() || 'das Zielobjekt';
  return 'Aktivierung nicht moeglich: Fuer ' + objectName + ' fehlen erforderliche Zielfelder im Mapping oder in der Zielkonfiguration: '
    + missingRequiredFields.map((field) => String(field?.name || '').trim()).filter(Boolean).join(', ') + '.';
}

function renderRequiredSchedulerFieldStatus() {
  const status = document.getElementById('sch-mapping-required-status');
  if (!status) {
    return;
  }

  if (!isSchedulerSalesforceInsertOrUpsertSelection()) {
    status.className = 'small mt-2 text-secondary';
    status.textContent = 'Pflichtfelder werden fuer Salesforce Insert/Upsert je Zielobjekt gekennzeichnet.';
    return;
  }

  const requiredFields = getRequiredSalesforceTargetFields();
  if (!requiredFields.length) {
    status.className = 'small mt-2 text-secondary';
    status.textContent = 'Keine Pflichtfelder aus den aktuellen Salesforce-Metadaten erkannt.';
    return;
  }

  const missingRequiredFields = getMissingRequiredSchedulerTargetFields();
  const requiredLabels = requiredFields.map((field) => {
    const name = String(field?.name || '').trim();
    return name ? (name + ' *') : '';
  }).filter(Boolean).join(', ');

  if (missingRequiredFields.length) {
    const missingLabels = missingRequiredFields.map((field) => String(field?.name || '').trim()).filter(Boolean).join(', ');
    status.className = 'small mt-2 text-danger';
    status.textContent = 'Pflichtfelder: ' + requiredLabels + '. Noch offen: ' + missingLabels + '. Aktive Speicherung ist erst danach moeglich.';
    return;
  }

  status.className = 'small mt-2 text-success';
  status.textContent = 'Pflichtfelder gesetzt: ' + requiredLabels + '.';
}

function updateMappingDetailEditorState() {
  const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
  const status = document.getElementById('sch-map-detail-status');
  const source = document.getElementById('sch-map-detail-source');
  const target = document.getElementById('sch-map-detail-target');
  const transform = document.getElementById('sch-map-detail-transform');
  const expression = document.getElementById('sch-map-detail-transform-expression');
  const lookupEnabled = document.getElementById('sch-map-detail-lookup-enabled');
  const lookupObject = document.getElementById('sch-map-detail-lookup-object');
  const lookupField = document.getElementById('sch-map-detail-lookup-field');
  const applyButton = document.getElementById('sch-map-detail-apply');
  const deleteButton = document.getElementById('sch-map-detail-delete');

  if (!selectedRule) {
    status.textContent = 'Noch keine Mapping-Zeile ausgewählt.';
    source.value = '';
    target.value = '';
    transform.value = 'NONE';
    expression.value = '';
    lookupEnabled.checked = false;
    lookupObject.value = '';
    lookupField.value = '';
    applyButton.disabled = true;
    deleteButton.disabled = true;
    renderPicklistMappingTable([]);
    return;
  }

  status.textContent = 'Bearbeitung für Quellfeld: ' + selectedRule.sourceField;
  source.value = selectedRule.sourceField || '';
  if (selectedRule.targetField && !Array.from(target.options || []).some((option) => option.value === selectedRule.targetField)) {
    const fallbackOption = document.createElement('option');
    fallbackOption.value = selectedRule.targetField;
    fallbackOption.textContent = selectedRule.targetField;
    target.appendChild(fallbackOption);
  }
  target.value = selectedRule.targetField || '';
  transform.value = selectedRule.transformFunction || 'NONE';
  expression.value = selectedRule.transformExpression || '';
  lookupEnabled.checked = !!selectedRule.lookupEnabled;
  lookupObject.value = selectedRule.lookupObject || '';
  lookupField.value = selectedRule.lookupField || '';
  applyButton.disabled = false;
  deleteButton.disabled = false;
  renderPicklistMappingTable(selectedRule.picklistMappings || []);
}

function renderPicklistMappingTable(mappings) {
  const tableBody = document.getElementById('sch-map-detail-picklist-table');
  if (!tableBody) {
    return;
  }

  if (!Array.isArray(mappings) || !mappings.length) {
    tableBody.innerHTML = '<tr><td colspan="3" class="text-secondary">Keine Picklist-Mappings.</td></tr>';
    return;
  }

  tableBody.innerHTML = mappings.map((mapping, idx) => {
    return (
      '<tr data-picklist-idx="' + idx + '">' +
        '<td><input type="text" class="form-control form-control-sm picklist-source" value="' + esc(mapping.source || '') + '" /></td>' +
        '<td><input type="text" class="form-control form-control-sm picklist-target" value="' + esc(mapping.target || '') + '" /></td>' +
        '<td><button type="button" class="btn btn-sm btn-outline-danger btn-delete-picklist-entry" data-idx="' + idx + '">Löschen</button></td>' +
      '</tr>'
    );
  }).join('');

  tableBody.querySelectorAll('button.btn-delete-picklist-entry').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const idx = Number(btn.getAttribute('data-idx'));
      const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
      if (selectedRule && Array.isArray(selectedRule.picklistMappings)) {
        selectedRule.picklistMappings.splice(idx, 1);
        renderPicklistMappingTable(selectedRule.picklistMappings);
      }
    });
  });
}

function addPicklistMappingEntry() {
  const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
  if (!selectedRule) {
    return;
  }

  if (!Array.isArray(selectedRule.picklistMappings)) {
    selectedRule.picklistMappings = [];
  }

  selectedRule.picklistMappings.push({ source: '', target: '' });
  renderPicklistMappingTable(selectedRule.picklistMappings);
}

async function loadTransformFunctions() {
  try {
    const result = await requestJson('/api/mapping/transforms');
    const select = document.getElementById('sch-map-detail-transform');
    if (!select) {
      return;
    }

    const functions = Array.isArray(result.functions) ? result.functions : [];
    select.innerHTML = functions.map((fn) =>
      '<option value="' + esc(fn.id) + '" title="' + esc(fn.description || '') + '">' + esc(fn.label || fn.id) + '</option>'
    ).join('');
  } catch {
    const select = document.getElementById('sch-map-detail-transform');
    if (select) {
      select.innerHTML = '<option value="NONE">Fehler beim Laden</option>';
    }
  }
}

function renderTargetObjectOptions(objects, selectedValue) {
  const select = document.getElementById('sch-object');
  if (!select) {
    return;
  }

  const isShareObject = (item) => {
    const value = String(item?.name || '').trim();
    const label = String(item?.label || value).trim();
    return /__share$/i.test(value) || /share:/i.test(label);
  };
  const items = Array.isArray(objects) ? objects : [];
  state.schedulerTargetObjects = items;
  const selected = String(selectedValue || select.value || '').trim();
  const selectedIsShareObject = selected && items.some((item) => String(item?.name || '').trim() === selected && isShareObject(item));
  const selectableItems = items.filter((item) => !isShareObject(item) || String(item?.name || '').trim() === selected);
  const filterTerm = String(document.getElementById('sch-object-filter')?.value || '').trim().toLowerCase();
  const visibleItems = filterTerm
    ? selectableItems.filter((item) => {
        const value = String(item?.name || '').trim();
        const label = String(item?.label || value).trim();
        return (value + ' ' + label).toLowerCase().includes(filterTerm);
      })
    : selectableItems;
  const selectedItem = selected && !visibleItems.some((item) => String(item?.name || '').trim() === selected)
    ? items.find((item) => String(item?.name || '').trim() === selected)
    : null;
  const optionItems = selectedItem ? [selectedItem, ...visibleItems] : visibleItems;

  select.innerHTML = '<option value="">- Wählen -</option>' + optionItems.map((item) => {
    const value = String(item?.name || '').trim();
    const label = String(item?.label || value).trim();
    return '<option value="' + esc(value) + '">' + esc(label) + '</option>';
  }).join('');

  if (selected && (selectedIsShareObject || selectableItems.some((item) => String(item?.name || '') === selected))) {
    select.value = selected;
  } else if (selected) {
    select.innerHTML = '<option value="">- Wählen -</option><option value="' + esc(selected) + '">' + esc(selected) + '</option>' + visibleItems.map((item) => {
      const value = String(item?.name || '').trim();
      const label = String(item?.label || value).trim();
      return '<option value="' + esc(value) + '">' + esc(label) + '</option>';
    }).join('');
    select.value = selected;
  }
}

function getPreferredMssqlTargetObjectName(connectorId, selectedObjectName, objects) {
  const items = Array.isArray(objects) ? objects : [];
  const requested = String(selectedObjectName || '').trim();
  const connector = Array.isArray(state.connectors)
    ? state.connectors.find((item) => String(item?.id || '').trim() === String(connectorId || '').trim())
    : null;
  const configuredTable = String(connector?.parameters?.table || '').trim();

  if (configuredTable && items.some((item) => String(item?.name || '').trim() === configuredTable)) {
    return configuredTable;
  }

  return requested;
}

async function ensureMssqlTargetObjectSelection() {
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  if (targetType !== 'MSSQL') {
    return;
  }

  const select = document.getElementById('sch-object');
  if (!select) {
    return;
  }

  let optionValues = Array.from(select.options || [])
    .map((option) => String(option.value || '').trim())
    .filter(Boolean);
  if (!optionValues.length) {
    await loadTargetObjects(String(select.value || '').trim());
    optionValues = Array.from(select.options || [])
      .map((option) => String(option.value || '').trim())
      .filter(Boolean);
  }
  const currentValue = String(select.value || '').trim();
  if (currentValue && optionValues.includes(currentValue)) {
    return;
  }

  const connectorId = String(document.getElementById('sch-connector')?.value || '').trim();
  const preferredValue = getPreferredMssqlTargetObjectName(
    connectorId,
    currentValue,
    optionValues.map((name) => ({ name }))
  );
  const fallbackValue = String(preferredValue || optionValues[0] || '').trim();
  if (fallbackValue) {
    select.value = fallbackValue;
  }
}

async function loadTargetObjects(selectedObjectName) {
  const loadSeq = Number(state.targetObjectsLoadSeq || 0) + 1;
  state.targetObjectsLoadSeq = loadSeq;
  const targetSystem = resolveEffectiveTargetSystem();
  const connectorId = document.getElementById('sch-connector').value;

  if (!targetSystem) {
    renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
    renderSchedulerMappingAssistant();
    return;
  }

  try {
    const result = await requestJson('/api/targets/objects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        targetSystem,
        connectorId
      })
    });

    if (loadSeq !== state.targetObjectsLoadSeq) {
      return;
    }

    const objects = Array.isArray(result.objects) ? result.objects : [];
    if (!objects.length) {
      renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
      renderSchedulerMappingAssistant();
      return;
    }

    const preferredObjectName = normalizeSystemValue(targetSystem) === 'MS SQL'
      ? getPreferredMssqlTargetObjectName(connectorId, selectedObjectName || '', objects)
      : (selectedObjectName || '');
    renderTargetObjectOptions(objects, preferredObjectName);
    renderSchedulerMappingAssistant();
  } catch {
    if (loadSeq !== state.targetObjectsLoadSeq) {
      return;
    }
    renderSelectOptions('sch-object', state.scheduleOptions.objectNames || [], selectedObjectName || '');
    renderSchedulerMappingAssistant();
  }
}

async function refreshSchedulerSalesforceMetadata() {
  clearModalError();
  const instanceId = String(state.instanceId || '').trim();
  if (!instanceId) {
    showModalError('Bitte zuerst eine Salesforce-Instanz auswählen.');
    return;
  }

  const button = document.getElementById('sch-refresh-sf-metadata');
  const previousLabel = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Lade...';
  }

  try {
    const selectedObject = String(document.getElementById('sch-object')?.value || '').trim();
    const baseObjects = ['Account', 'Contact', 'Lead', 'Opportunity', 'Order', 'Product2', 'Pricebook2', 'PricebookEntry'];
    const objectNames = Array.from(new Set([selectedObject, ...baseObjects].map((item) => String(item || '').trim()).filter(Boolean)));
    const snapshot = await requestJson('/api/admin/sf-instances/' + encodeURIComponent(instanceId) + '/metadata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        objectNames,
        includeAllFields: false,
        maxFieldObjects: 80
      })
    });
    state.instanceMetadataSnapshots[instanceId] = snapshot;
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};
    await loadTargetObjects(selectedObject);
    await loadTargetFields();
    renderSchedulerMappingManager();
    showInfo('Metadaten aktualisiert: ' + String(snapshot.objectCount || 0) + ' Objekte, ' + String(snapshot.fieldCount || 0) + ' Felder.');
  } catch (error) {
    showModalError('Metadaten-Refresh fehlgeschlagen: ' + (error.message || 'Unbekannter Fehler'));
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = previousLabel || 'Metadaten neu laden';
    }
  }
}

async function loadTargetFields() {
  const loadSeq = Number(state.targetFieldsLoadSeq || 0) + 1;
  state.targetFieldsLoadSeq = loadSeq;
  const targetSystem = resolveEffectiveTargetSystem();
  const objectName = document.getElementById('sch-object').value;
  const connectorId = document.getElementById('sch-connector').value;
  const select = document.getElementById('sch-map-detail-target');
  const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
  const preferredField = String(selectedRule?.targetField || '').trim();

  if (!select || !targetSystem) {
    state.targetFields = [];
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};
    select.innerHTML = '<option value="">- Wählen -</option>';
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    return;
  }

  // Clear select while loading
  select.innerHTML = '<option value="">Wird geladen...</option>';

  // Always use selected target object/table as base
  const targetObject = objectName;
  if (!targetObject) {
    state.targetFields = [];
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};
    select.innerHTML = '<option value="">Zielobjekt wählen</option>';
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    return;
  }

  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  if (isFileScheduleTargetType(targetType)) {
    const sources = getSchedulerMappingManagerSources();
    const existingTargets = (Array.isArray(state.mappingRules) ? state.mappingRules : [])
      .map((rule) => String(rule?.targetField || '').trim())
      .filter(Boolean);
    const sourceDerivedFields = sources.map((source) => ({
      name: String(source?.name || '').trim(),
      label: String(source?.label || source?.name || '').trim(),
      requiredOnCreate: false,
      createable: true,
      updateable: true,
      type: String(source?.type || 'string').trim() || 'string'
    })).filter((field) => field.name);
    const mergedFieldsByKey = new Map();
    sourceDerivedFields.forEach((field) => {
      mergedFieldsByKey.set(normalizeFieldKey(field.name), field);
    });
    existingTargets.forEach((name) => {
      const key = normalizeFieldKey(name);
      if (!key || mergedFieldsByKey.has(key)) {
        return;
      }
      mergedFieldsByKey.set(key, {
        name,
        label: name,
        requiredOnCreate: false,
        createable: true,
        updateable: true,
        type: 'string'
      });
    });

    const fields = Array.from(mergedFieldsByKey.values()).sort((a, b) =>
      String(a?.label || a?.name || '').localeCompare(String(b?.label || b?.name || ''), 'de', { sensitivity: 'base', numeric: true })
    );

    state.targetFields = fields;
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};

    const currentValue = preferredField || select.value;
    select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
      '<option value="' + esc(field.name) + '">' + esc(field.label || field.name) + '</option>'
    ).join('');
    if (currentValue && !fields.some((field) => field.name === currentValue)) {
      select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
    }
    if (currentValue) {
      select.value = currentValue;
    }
    refreshSchedulerMappingCompatibilityState();
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    return;
  }

  if (targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
    const fields = getSchedulerGlobalPicklistTargetFields();
    state.targetFields = fields;
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};
    const currentValue = preferredField || select.value;
    select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
      '<option value="' + esc(field.name) + '">' + esc((field.label ? field.label : field.name) + (field.requiredOnCreate === true ? ' *' : '')) + '</option>'
    ).join('');
    if (currentValue && !fields.some((field) => field.name === currentValue)) {
      select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
    }
    if (currentValue) {
      select.value = currentValue;
    }
    refreshSchedulerMappingCompatibilityState();
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    return;
  }

  try {
    const result = await requestJson('/api/mapping/target-fields', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        targetSystem: targetSystem,
        targetObject: targetObject,
        connectorId: connectorId
      })
    });

    if (loadSeq !== state.targetFieldsLoadSeq) {
      return;
    }

    const fields = Array.isArray(result.fields) ? result.fields : [];
    state.targetFields = fields;
    const currentValue = preferredField || select.value;
    select.innerHTML = '<option value="">- Wählen -</option>' + fields.map((field) =>
      '<option value="' + esc(field.name) + '">' + esc((field.label ? field.label : field.name) + (field.requiredOnCreate === true ? ' *' : '')) + '</option>'
    ).join('');
    if (currentValue && !fields.some((f) => f.name === currentValue)) {
      select.innerHTML += '<option value="' + esc(currentValue) + '">' + esc(currentValue) + '</option>';
    }
    if (currentValue && fields.some((f) => f.name === currentValue)) {
      select.value = currentValue;
    } else if (currentValue) {
      select.value = currentValue;
    }

    if (canUseSchedulerLookupSelection()) {
      await loadSchedulerLookupObjects();
    } else {
      state.schedulerLookupObjects = [];
      state.schedulerLookupObjectsLoaded = false;
      state.schedulerLookupObjectsLoadPromise = null;
      state.schedulerLookupExternalIdFieldsByObject = {};
      state.schedulerLookupExternalIdFieldPromises = {};
    }

    refreshSchedulerMappingCompatibilityState();
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
  } catch (error) {
    if (loadSeq !== state.targetFieldsLoadSeq) {
      return;
    }
    state.targetFields = [];
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    state.schedulerLookupExternalIdFieldsByObject = {};
    state.schedulerLookupExternalIdFieldPromises = {};
    select.innerHTML = '<option value="">Fehler beim Laden</option>';
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    console.error('Error loading target fields:', error);
  }
}

function renderMappingRulesTable() {
  const rulesBody = document.getElementById('sch-mapping-rules');
  if (!rulesBody) {
    return;
  }

  if (!state.mappingRules.length) {
    rulesBody.innerHTML = '<tr><td colspan="6" class="text-secondary">Noch keine Mapping-Regeln. Ziehen Sie Quellfelder in diese Tabelle.</td></tr>';
    updateMappingDetailEditorState();
    syncMappingDefinitionFromRules();
    renderRequiredSchedulerFieldStatus();
    renderSchedulerMappingManager();
    return;
  }

  rulesBody.innerHTML = state.mappingRules.map((rule) => {
    const isSelected = rule.id === state.selectedMappingRuleId;
    const source = esc(rule.sourceField || '-');
    const target = esc(rule.targetField || '-');
    const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
      normalizeFieldKey(field?.name) === normalizeFieldKey(rule?.targetField)
    );
    const targetDisplay = targetMeta?.requiredOnCreate === true && target !== '-'
      ? target + ' <span class="badge bg-warning-subtle text-dark border">Pflicht</span>'
      : target;
    const lookup = rule.lookupEnabled
      ? esc((rule.lookupObject || '-') + '.' + (rule.lookupField || '-'))
      : '-';
    const transform = esc(rule.transformFunction || 'NONE');
    const picklistCount = Array.isArray(rule.picklistMappings) ? rule.picklistMappings.length : 0;
    const picklist = picklistCount > 0 ? String(picklistCount) + ' Mapping(s)' : '-';

    return (
      '<tr class="' + (isSelected ? 'mapping-rule-selected' : '') + '" data-rule-id="' + esc(rule.id) + '">' +
        '<td>' + source + '</td>' +
        '<td>' + targetDisplay + '</td>' +
        '<td>' + lookup + '</td>' +
        '<td>' + transform + '</td>' +
        '<td>' + esc(picklist) + '</td>' +
        '<td><button type="button" class="btn btn-sm btn-outline-danger" data-delete-rule="' + esc(rule.id) + '">Löschen</button></td>' +
      '</tr>'
    );
  }).join('');

  rulesBody.querySelectorAll('tr[data-rule-id]').forEach((row) => {
    row.addEventListener('click', () => {
      const ruleId = row.getAttribute('data-rule-id');
      state.selectedMappingRuleId = ruleId || '';
      renderMappingRulesTable();
    });
  });

  rulesBody.querySelectorAll('button[data-delete-rule]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.stopPropagation();
      const ruleId = button.getAttribute('data-delete-rule');
      state.mappingRules = state.mappingRules.filter((rule) => rule.id !== ruleId);
      if (state.selectedMappingRuleId === ruleId) {
        state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
      }
      renderMappingRulesTable();
    });
  });

  updateMappingDetailEditorState();
  syncMappingDefinitionFromRules();
  renderRequiredSchedulerFieldStatus();
  renderSchedulerMappingManager();
}

function setupMappingDropZone() {
  const dropzone = document.getElementById('sch-mapping-rules-dropzone');
  if (!dropzone || dropzone.dataset.dndBound === '1') {
    return;
  }

  dropzone.dataset.dndBound = '1';

  dropzone.addEventListener('dragover', (event) => {
    event.preventDefault();
    dropzone.classList.add('mapping-dropzone-active');
  });

  dropzone.addEventListener('dragleave', () => {
    dropzone.classList.remove('mapping-dropzone-active');
  });

  dropzone.addEventListener('drop', (event) => {
    event.preventDefault();
    dropzone.classList.remove('mapping-dropzone-active');
    const data = event.dataTransfer.getData('application/json') || event.dataTransfer.getData('text/plain');
    if (!data) {
      return;
    }

    let sourceField;
    try {
      sourceField = JSON.parse(data);
    } catch {
      sourceField = { name: String(data || '').trim(), type: 'string' };
    }

    if (!sourceField || !sourceField.name) {
      return;
    }

    const newRule = createMappingRuleFromSource(sourceField);
    state.mappingRules.push(newRule);
    state.selectedMappingRuleId = newRule.id;
    renderMappingRulesTable();
  });
}

function getSchedulerMappingManagerSources() {
  const byKey = new Map();
  const loadedFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
  loadedFields.forEach((field) => {
    const name = resolveSourceFieldName(field?.name || field || '');
    const key = normalizeFieldKey(name);
    if (name && key && !byKey.has(key)) {
      byKey.set(key, {
        name,
        label: String(field?.label || '').trim(),
        type: String(field?.type || 'string').trim() || 'string'
      });
    }
  });
  // Fallback: wenn keine Felder vom Connector geladen wurden (z.B. Remote-Agent),
  // Quellfelder aus den gespeicherten Mapping-Regeln (Salesforce) synthetisieren.
  if (byKey.size === 0) {
    (Array.isArray(state.mappingRules) ? state.mappingRules : []).forEach((rule) => {
      const name = resolveSourceFieldName(String(rule?.sourceField || '').trim());
      const key = normalizeFieldKey(name);
      if (name && key && !byKey.has(key)) {
        byKey.set(key, {
          name,
          label: '',
          type: String(rule?.sourceType || 'string').trim() || 'string'
        });
      }
    });
  }
  return Array.from(byKey.values()).sort((a, b) => {
    const left = String(a?.label || a?.name || '').trim();
    const right = String(b?.label || b?.name || '').trim();
    return left.localeCompare(right, 'de', { sensitivity: 'base', numeric: true });
  });
}

function parseCompactPicklistMappings(value) {
  return String(value || '')
    .split(/[;\n]/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const splitToken = part.includes('=>') ? '=>' : '=';
      const pieces = part.split(splitToken);
      return {
        source: String(pieces[0] || '').trim(),
        target: String(pieces.slice(1).join(splitToken) || '').trim()
      };
    })
    .filter((entry) => entry.source || entry.target);
}

function formatCompactPicklistMappings(mappings) {
  return (Array.isArray(mappings) ? mappings : [])
    .map((entry) => String(entry?.source || '').trim() + '=' + String(entry?.target || '').trim())
    .filter((part) => part !== '=')
    .join('; ');
}

function normalizeSchedulerCustomFieldApiName(value) {
  const base = String(value || '')
    .trim()
    .replace(/[^A-Za-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');
  const withStart = /^[A-Za-z]/.test(base) ? base : ('Field_' + base);
  const withoutSuffix = withStart.replace(/__c$/i, '');
  return (withoutSuffix || 'New_Field') + '__c';
}

function inferSchedulerFieldCreationType(source, rule) {
  const sourceType = String(source?.type || rule?.sourceType || '').trim().toLowerCase();
  const targetName = String(rule?.targetField || source?.name || '').trim().toLowerCase();
  if (sourceType.includes('date') && sourceType.includes('time')) return 'DateTime';
  if (sourceType.includes('date')) return 'Date';
  if (sourceType.includes('bool') || sourceType === 'bit') return 'Checkbox';
  if (sourceType.includes('money') || targetName.includes('currency')) return 'Currency';
  if (sourceType.includes('decimal') || sourceType.includes('number') || sourceType.includes('int') || sourceType.includes('float')) return 'Number';
  if (targetName.includes('email')) return 'Email';
  if (targetName.includes('phone') || targetName.includes('mobile')) return 'Phone';
  if (targetName.includes('url') || targetName.includes('website')) return 'Url';
  return 'Text';
}

function parseSchedulerSourceTypeDimensions(sourceType) {
  const normalized = String(sourceType || '').trim().toLowerCase();
  const parenMatch = normalized.match(/((d+)(?:s*,s*(d+))?)/);
  const first = parenMatch ? Number(parenMatch[1]) : null;
  const second = parenMatch && parenMatch[2] !== undefined ? Number(parenMatch[2]) : null;
  return {
    length: Number.isFinite(first) ? first : null,
    precision: Number.isFinite(first) ? first : null,
    scale: Number.isFinite(second) ? second : null
  };
}

function getSchedulerFieldCreationDefaults(source, rule) {
  const inferredType = inferSchedulerFieldCreationType(source, rule);
  const sourceDimensions = parseSchedulerSourceTypeDimensions(source?.type || rule?.sourceType || '');
  const sourceName = String(source?.name || rule?.sourceField || 'New_Field').trim();
  const fieldApiName = normalizeSchedulerCustomFieldApiName(sourceName);
  const length = inferredType === 'Text'
    ? Math.max(1, Math.min(255, Number(sourceDimensions.length || 255)))
    : 255;
  const precision = ['Number', 'Currency', 'Percent'].includes(inferredType)
    ? Math.max(1, Math.min(18, Number(sourceDimensions.precision || 18)))
    : 18;
  const scale = ['Currency', 'Percent'].includes(inferredType)
    ? Math.max(0, Math.min(precision - 1, Number(sourceDimensions.scale ?? 2)))
    : Math.max(0, Math.min(precision - 1, Number(sourceDimensions.scale ?? 0)));
  return {
    fieldApiName,
    fieldType: inferredType,
    length,
    precision,
    scale,
    picklistValues: getSchedulerSourceDistinctValues(sourceName, 80)
  };
}

function getSchedulerFieldCreationTypeOptions(selectedType) {
  return ['Text', 'Number', 'Date', 'DateTime', 'Checkbox', 'Currency', 'Percent', 'Email', 'Phone', 'Url', 'Picklist']
    .map((type) => '<option value="' + esc(type) + '"' + (type === selectedType ? ' selected' : '') + '>' + esc(type) + '</option>')
    .join('');
}

function syncSchedulerCreateFieldPanel(row) {
  const fieldType = String(row.querySelector('[data-sch-create-field-type]')?.value || 'Text').trim();
  row.querySelectorAll('[data-sch-create-config]').forEach((element) => {
    const configType = String(element.getAttribute('data-sch-create-config') || '').trim();
    const visible = configType === 'text'
      ? fieldType === 'Text'
      : configType === 'number'
        ? ['Number', 'Currency', 'Percent'].includes(fieldType)
        : configType === 'picklist'
          ? fieldType === 'Picklist'
          : false;
    element.classList.toggle('d-none', !visible);
  });
}

function getSchedulerSourceDistinctValues(sourceName, limit) {
  const normalizedSource = String(sourceName || '').trim();
  const values = [];
  const seen = new Set();
  (Array.isArray(state.sourcePreviewRows) ? state.sourcePreviewRows : []).forEach((row) => {
    const rawValue = row && Object.prototype.hasOwnProperty.call(row, normalizedSource) ? row[normalizedSource] : undefined;
    const value = String(rawValue ?? '').trim();
    const key = value.toLowerCase();
    if (value && !seen.has(key)) {
      seen.add(key);
      values.push(value);
    }
  });
  return values.slice(0, Number(limit || 80));
}

async function createSchedulerSalesforceFieldFromRow(row, source, rule) {
  clearModalError();
  const objectApiName = String(document.getElementById('sch-object')?.value || '').trim();
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  if (targetType !== 'SALESFORCE' || !objectApiName) {
    showModalError('Salesforce-Felder können nur bei Salesforce-Zielen mit gewähltem Objekt angelegt werden.');
    return;
  }

  const fieldApiName = normalizeSchedulerCustomFieldApiName(row.querySelector('[data-sch-create-field-api]')?.value || '');
  if (!fieldApiName) {
    showModalError('Bitte API-Name für das neue Salesforce-Feld angeben.');
    return;
  }

  const fieldType = String(row.querySelector('[data-sch-create-field-type]')?.value || 'Text').trim() || 'Text';
  const length = Number(row.querySelector('[data-sch-create-field-length]')?.value || 255);
  const precision = Number(row.querySelector('[data-sch-create-field-precision]')?.value || 18);
  const scale = Number(row.querySelector('[data-sch-create-field-scale]')?.value || 0);
  const picklistValues = fieldType === 'Picklist'
    ? String(row.querySelector('[data-sch-create-field-picklist]')?.value || '')
        .split(/[,\n\r]/)
        .map((value) => value.trim())
        .filter(Boolean)
    : [];
  if (fieldType === 'Picklist' && !picklistValues.length) {
    showModalError('Für Picklist-Felder müssen Werte angegeben werden.');
    return;
  }

  const button = row.querySelector('[data-sch-manager-create-field]');
  const previousLabel = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Lege an...';
  }

  try {
    await requestJson('/api/salesforce/create-field', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        objectApiName,
        fieldApiName,
        fieldType,
        length,
        precision,
        scale,
        picklistValues,
        instanceId: state.instanceId
      })
    });
    await loadTargetFields();
    const sourceName = String(source?.name || row.getAttribute('data-sch-map-source') || '').trim();
    const currentRule = findSchedulerMappingRuleBySource(sourceName) || createMappingRuleFromSource(source || { name: sourceName, type: 'string' });
    currentRule.targetField = fieldApiName;
    currentRule.targetType = fieldType === 'Picklist' ? 'picklist' : fieldType.toLowerCase();
    if (!findSchedulerMappingRuleBySource(sourceName)) {
      state.mappingRules.push(currentRule);
    }
    renderMappingRulesTable();
    showInfo(objectApiName + '.' + fieldApiName + ' angelegt und Berechtigung gesetzt.');
  } catch (error) {
    showModalError('Feld konnte nicht angelegt werden: ' + (error.message || 'Unbekannter Fehler'));
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = previousLabel || 'Feld anlegen';
    }
  }
}

function findSchedulerMappingRuleBySource(sourceName) {
  const sourceKey = normalizeFieldKey(sourceName);
  if (!sourceKey) {
    return null;
  }
  return (Array.isArray(state.mappingRules) ? state.mappingRules : []).find((rule) =>
    normalizeFieldKey(rule?.sourceField) === sourceKey
  ) || null;
}

function getSchedulerMappingTargetOptions(selectedValue, filterTerm) {
  const selected = String(selectedValue || '').trim();
  const filter = String(filterTerm || '').trim().toLowerCase();
  const fields = Array.isArray(state.targetFields) ? state.targetFields : [];
  const visibleFields = fields
    .filter((field) => isSchedulerMappingTargetFieldVisible(field, selected))
    .filter((field) => {
      if (!filter) {
        return true;
      }
      const name = String(field?.name || '').trim();
      const label = String(field?.label || name).trim();
      const type = String(field?.type || '').trim();
      const group = getSchedulerTargetFieldGroup(field);
      return [name, label, type, group].join(' ').toLowerCase().includes(filter);
    })
    .slice()
    .sort(compareSchedulerTargetFields);
  const hasSelected = selected && visibleFields.some((field) => String(field?.name || '') === selected);
  const groups = [
    { key: 'externalId', label: 'External IDs' },
    { key: 'lookup', label: 'Lookup-Felder' },
    { key: 'custom', label: 'Customfelder' },
    { key: 'standard', label: 'Standardfelder' },
    { key: 'other', label: 'Weitere Felder' }
  ];
  const groupedOptions = groups.map((group) => {
    const options = visibleFields
      .filter((field) => getSchedulerTargetFieldGroup(field) === group.key)
      .map((field) => getSchedulerTargetFieldOptionHtml(field, selected))
      .join('');
    return options ? '<optgroup label="' + esc(group.label) + '">' + options + '</optgroup>' : '';
  }).join('');
  return '<option value=""' + (!selected ? ' selected' : '') + '>Zielfeld wählen</option>' + groupedOptions + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>Manuell: ' + esc(selected) + '</option>' : '');
}

function getSchedulerTargetFieldOptionHtml(field, selected) {
    const name = String(field?.name || '').trim();
    const label = String(field?.label || '').trim();
    const display = label && label !== name ? label + ' - ' + name : name;
    const meta = [
      field?.requiredOnCreate === true ? 'Pflicht' : '',
      field?.isExternalId === true ? 'External ID' : '',
      getSchedulerTargetFieldGroup(field) === 'lookup' ? 'Lookup' : '',
      getSchedulerTargetFieldGroup(field) === 'custom' ? 'Custom' : ''
    ].filter(Boolean).join(', ');
    return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(display + (meta ? ' (' + meta + ')' : '')) + '</option>';
}

function getSchedulerTargetFieldGroup(field) {
  const name = String(field?.name || '').trim();
  const type = String(field?.type || '').trim().toLowerCase();
  if (!name) {
    return 'other';
  }
  if (field?.isExternalId === true) {
    return 'externalId';
  }
  if (type === 'reference' || Array.isArray(field?.referenceTo) && field.referenceTo.length > 0) {
    return 'lookup';
  }
  if (/__c$/i.test(name)) {
    return 'custom';
  }
  return 'standard';
}

function compareSchedulerTargetFields(a, b) {
  const groupOrder = { externalId: 0, lookup: 1, custom: 2, standard: 3, other: 4 };
  const leftGroup = groupOrder[getSchedulerTargetFieldGroup(a)] ?? 9;
  const rightGroup = groupOrder[getSchedulerTargetFieldGroup(b)] ?? 9;
  if (leftGroup !== rightGroup) {
    return leftGroup - rightGroup;
  }
  const left = String(a?.label || a?.name || '').trim();
  const right = String(b?.label || b?.name || '').trim();
  return left.localeCompare(right, 'de', { sensitivity: 'base', numeric: true });
}

function isSchedulerMappingTargetFieldVisible(field, selectedValue) {
  const name = String(field?.name || '').trim();
  if (!name) {
    return false;
  }
  if (selectedValue && normalizeFieldKey(name) === normalizeFieldKey(selectedValue)) {
    return true;
  }

  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  if (targetType !== 'SALESFORCE') {
    return true;
  }

  const createable = field?.createable === true;
  const updateable = field?.updateable === true;
  if (createable || updateable || field?.isExternalId === true) {
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

function canUseSchedulerLookupSelection() {
  return isSalesforceTargetSelection()
    && String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase() === 'SALESFORCE';
}

async function loadSchedulerLookupObjects() {
  if (!canUseSchedulerLookupSelection()) {
    state.schedulerLookupObjects = [];
    state.schedulerLookupObjectsLoaded = false;
    state.schedulerLookupObjectsLoadPromise = null;
    return [];
  }
  if (state.schedulerLookupObjectsLoaded && Array.isArray(state.schedulerLookupObjects) && state.schedulerLookupObjects.length) {
    return state.schedulerLookupObjects;
  }
  if (state.schedulerLookupObjectsLoadPromise) {
    return state.schedulerLookupObjectsLoadPromise;
  }

  state.schedulerLookupObjectsLoadPromise = (async () => {
    try {
      const result = await requestJson('/api/salesforce/objects?instanceId=' + encodeURIComponent(state.instanceId || ''));
      const objects = Array.isArray(result) ? result : [];
      state.schedulerLookupObjects = objects
        .map((item) => ({
          name: String(item?.name || '').trim(),
          label: String(item?.label || item?.name || '').trim()
        }))
        .filter((item) => item.name);
      state.schedulerLookupObjectsLoaded = true;
      return state.schedulerLookupObjects;
    } catch {
      state.schedulerLookupObjects = [];
      state.schedulerLookupObjectsLoaded = false;
      return [];
    } finally {
      state.schedulerLookupObjectsLoadPromise = null;
    }
  })();

  return state.schedulerLookupObjectsLoadPromise;
}

async function loadSchedulerLookupExternalIdFields(objectName) {
  const normalizedObject = String(objectName || '').trim();
  if (!normalizedObject || !canUseSchedulerLookupSelection()) {
    return [];
  }
  if (Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[normalizedObject])) {
    return state.schedulerLookupExternalIdFieldsByObject[normalizedObject];
  }
  if (state.schedulerLookupExternalIdFieldPromises?.[normalizedObject]) {
    return state.schedulerLookupExternalIdFieldPromises[normalizedObject];
  }

  state.schedulerLookupExternalIdFieldPromises[normalizedObject] = (async () => {
    try {
      const result = await requestJson('/api/salesforce/object-fields?object=' + encodeURIComponent(normalizedObject) + '&instanceId=' + encodeURIComponent(state.instanceId || ''));
      const allFields = (Array.isArray(result) ? result : [])
        .map((field) => ({
          name: String(field?.name || '').trim(),
          label: String(field?.label || field?.name || '').trim(),
          isExternalId: field?.isExternalId === true,
          calculated: field?.calculated === true
        }))
        .filter((field) => field.name && !field.calculated);

      const externalIdFields = allFields.filter((field) => field.isExternalId === true);
      const fields = (externalIdFields.length ? externalIdFields : allFields)
        .filter((field) => String(field.name || '').trim().toLowerCase() !== 'id')
        .map((field) => ({
          name: field.name,
          label: field.isExternalId ? (field.label + ' (External ID)') : field.label
        }));
      state.schedulerLookupExternalIdFieldsByObject[normalizedObject] = fields;
      return fields;
    } catch {
      state.schedulerLookupExternalIdFieldsByObject[normalizedObject] = [];
      return [];
    } finally {
      delete state.schedulerLookupExternalIdFieldPromises[normalizedObject];
    }
  })();

  return state.schedulerLookupExternalIdFieldPromises[normalizedObject];
}

function getSchedulerLookupObjectOptions(selectedValue) {
  const selected = String(selectedValue || '').trim();
  const objects = Array.isArray(state.schedulerLookupObjects) ? state.schedulerLookupObjects : [];
  const hasSelected = selected && objects.some((item) => String(item?.name || '') === selected);
  return '<option value="">- SF Objekt wählen -</option>' + objects.map((item) => {
    const name = String(item?.name || '').trim();
    const label = String(item?.label || name).trim();
    return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(label) + '</option>';
  }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>' + esc(selected + ' (gespeichert)') + '</option>' : '');
}

function getSchedulerLookupObjectDatalistOptions(selectedValue) {
  const selected = String(selectedValue || '').trim();
  const objects = Array.isArray(state.schedulerLookupObjects) ? state.schedulerLookupObjects : [];
  const hasSelected = selected && objects.some((item) => String(item?.name || '') === selected);
  return objects.map((item) => {
    const name = String(item?.name || '').trim();
    const label = String(item?.label || name).trim();
    const optionLabel = label && label !== name ? label : '';
    return '<option value="' + esc(name) + '"' + (optionLabel ? ' label="' + esc(optionLabel) + '"' : '') + '></option>';
  }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" label="gespeichert"></option>' : '');
}

function getSchedulerLookupFieldOptions(lookupObject, selectedValue) {
  const selected = String(selectedValue || '').trim();
  const normalizedObject = String(lookupObject || '').trim();
  const fields = normalizedObject && Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[normalizedObject])
    ? state.schedulerLookupExternalIdFieldsByObject[normalizedObject]
    : [];
  const hasSelected = selected && fields.some((item) => String(item?.name || '') === selected);
  const fallbackLabel = selected ? selected + ' (gespeichert)' : '';
  return '<option value="">- Lookup Feld wählen -</option>' + fields.map((item) => {
    const name = String(item?.name || '').trim();
    const label = String(item?.label || name).trim();
    const optionLabel = label && label !== name ? label + ' (' + name + ')' : name;
    return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(optionLabel) + '</option>';
  }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" selected>' + esc(fallbackLabel) + '</option>' : '');
}

function getSchedulerLookupFieldDatalistOptions(lookupObject, selectedValue) {
  const selected = String(selectedValue || '').trim();
  const normalizedObject = String(lookupObject || '').trim();
  const fields = normalizedObject && Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[normalizedObject])
    ? state.schedulerLookupExternalIdFieldsByObject[normalizedObject]
    : [];
  const hasSelected = selected && fields.some((item) => String(item?.name || '') === selected);
  return fields.map((item) => {
    const name = String(item?.name || '').trim();
    const label = String(item?.label || name).trim();
    const optionLabel = label && label !== name ? label : '';
    return '<option value="' + esc(name) + '"' + (optionLabel ? ' label="' + esc(optionLabel) + '"' : '') + '></option>';
  }).join('') + (!hasSelected && selected ? '<option value="' + esc(selected) + '" label="gespeichert"></option>' : '');
}

function renderSchedulerMappingManager() {
  const shell = document.getElementById('sch-mapping-manager');
  if (!shell) {
    return;
  }

  const sources = getSchedulerMappingManagerSources();
  const previousFilter = String(shell.querySelector('[data-sch-map-filter]')?.value || '').trim();
  const openDetailSources = new Set(
    Array.from(shell.querySelectorAll('[data-sch-map-row] details[open]'))
      .map((details) => String(details.closest('[data-sch-map-row]')?.getAttribute('data-sch-map-source') || '').trim())
      .filter(Boolean)
  );
  const targetFieldFilters = new Map(
    Array.from(shell.querySelectorAll('[data-sch-map-row]'))
      .map((row) => [
        String(row.getAttribute('data-sch-map-source') || '').trim(),
        String(row.querySelector('[data-sch-manager-target-filter]')?.value || '').trim()
      ])
      .filter(([sourceName]) => Boolean(sourceName))
  );
  const useLookupSelection = canUseSchedulerLookupSelection();
  if (useLookupSelection && !state.schedulerLookupObjectsLoaded && !state.schedulerLookupObjectsLoadPromise) {
    loadSchedulerLookupObjects().then(() => {
      renderSchedulerMappingManager();
    }).catch(() => {
      // keep current UI state when lookup metadata load fails
    });
  }

  const lookupObjectsToPreload = new Set(
    (Array.isArray(state.mappingRules) ? state.mappingRules : [])
      .map((rule) => String(rule?.lookupObject || '').trim())
      .filter(Boolean)
  );
  if (useLookupSelection && lookupObjectsToPreload.size) {
    lookupObjectsToPreload.forEach((objectName) => {
      if (Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[objectName]) || state.schedulerLookupExternalIdFieldPromises?.[objectName]) {
        return;
      }
      loadSchedulerLookupExternalIdFields(objectName).then(() => {
        renderSchedulerMappingManager();
      }).catch(() => {
        // ignore lookup preload errors in manager rendering
      });
    });
  }

  const mappedCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) =>
    String(rule?.sourceField || '').trim() && String(rule?.targetField || '').trim()
  ).length;
  const lookupCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) => rule?.lookupEnabled === true).length;
  const picklistCount = (Array.isArray(state.mappingRules) ? state.mappingRules : []).filter((rule) =>
    Array.isArray(rule?.picklistMappings) && rule.picklistMappings.length > 0
  ).length;
  const missingRequiredCount = getMissingRequiredSchedulerTargetFields().length;

  if (!sources.length) {
    shell.innerHTML =
      '<div class="migration-mapping-toolbar scheduler-mapping-toolbar">' +
        '<div>' +
          '<div class="fw-semibold">Mappingmanager</div>' +
          '<div class="small text-secondary">Quellfelder laden, um die zeilenweise Zuordnung zu bearbeiten.</div>' +
        '</div>' +
        '<div class="d-flex gap-2 align-items-center">' +
          '<button id="sch-automapping" type="button" class="btn btn-outline-success btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M7.1 1.2a.75.75 0 0 1 .8 0l4.5 2.6a.75.75 0 0 1 0 1.3L7.9 7.7a.75.75 0 0 1-.8 0L2.6 5.1a.75.75 0 0 1 0-1.3zm-3 5 3 1.7v3.4l-4.5-2.6a.75.75 0 0 1-.4-.65V6.2a.75.75 0 0 0 1.9 0zm7.8 0a.75.75 0 0 0 1.9 0v1.85a.75.75 0 0 1-.4.65l-4.5 2.6V7.9z"/></svg><span>Auto-Mapping</span></button>' +
          '<button id="sch-manager-load-fields" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1a7 7 0 1 0 6.3 10h-2.1A5 5 0 1 1 8 3v2.2l3.3-2.8L8 0z"/></svg><span>Felder laden</span></button>' +
        '</div>' +
      '</div>' +
      '<div class="scheduler-mapping-assistant-bar">' +
        '<div class="fw-semibold small">Mapping-Assistent</div>' +
        '<div class="small text-secondary" id="sch-mapping-assistant-hint">Wählt ein Salesforce-Profil und erzeugt Vorschläge für Ziel- und Lookup-Felder.</div>' +
        '<div class="d-flex gap-2 align-items-end ms-auto">' +
          '<select id="sch-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;"></select>' +
          '<button id="sch-mapping-assistant-apply" type="button" class="btn btn-primary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
        '</div>' +
      '</div>';
    document.getElementById('sch-manager-load-fields')?.addEventListener('click', loadMappingFields);
    renderSchedulerMappingAssistant();
    return;
  }

  shell.innerHTML =
    '<div class="migration-mapping-overview scheduler-mapping-overview">' +
      '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(mappedCount)) + '</span><span class="migration-mapping-stat-label">gemappt</span></div>' +
      '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(Math.max(0, sources.length - mappedCount))) + '</span><span class="migration-mapping-stat-label">offen</span></div>' +
      '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(lookupCount)) + '</span><span class="migration-mapping-stat-label">Lookups</span></div>' +
      '<div class="migration-mapping-stat"><span class="migration-mapping-stat-value">' + esc(String(missingRequiredCount)) + '</span><span class="migration-mapping-stat-label">Pflicht offen</span></div>' +
    '</div>' +
    '<div class="sch-mapping-manager-shell">' +
    '<div class="migration-mapping-toolbar scheduler-mapping-toolbar">' +
      '<div>' +
        '<div class="fw-semibold">Mappingmanager</div>' +
        '<div class="small text-secondary">Quellfelder links, Zielfelder rechts. Details nur öffnen, wenn Lookup, Transform oder Picklist gebraucht werden.</div>' +
      '</div>' +
      '<div class="d-flex gap-2 align-items-center">' +
        '<input class="form-control form-control-sm migration-mapping-search" type="search" placeholder="Quelle oder Ziel suchen" value="' + esc(previousFilter) + '" data-sch-map-filter>' +
        '<button id="sch-mapping-preview-btn" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 3C3.7 3 1.4 7.2 1 8c.4.8 2.7 5 7 5s6.6-4.2 7-5c-.4-.8-2.7-5-7-5m0 8a3 3 0 1 1 0-6 3 3 0 0 1 0 6"/></svg><span>Vorschau</span></button>' +
        '<button id="sch-automapping" type="button" class="btn btn-outline-success btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M7.1 1.2a.75.75 0 0 1 .8 0l4.5 2.6a.75.75 0 0 1 0 1.3L7.9 7.7a.75.75 0 0 1-.8 0L2.6 5.1a.75.75 0 0 1 0-1.3zm-3 5 3 1.7v3.4l-4.5-2.6a.75.75 0 0 1-.4-.65V6.2a.75.75 0 0 0 1.9 0zm7.8 0a.75.75 0 0 0 1.9 0v1.85a.75.75 0 0 1-.4.65l-4.5 2.6V7.9z"/></svg><span>Auto-Mapping</span></button>' +
        '<button id="sch-manager-load-fields" type="button" class="btn btn-outline-secondary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1a7 7 0 1 0 6.3 10h-2.1A5 5 0 1 1 8 3v2.2l3.3-2.8L8 0z"/></svg><span>Aktualisieren</span></button>' +
      '</div>' +
    '</div>' +
    '<div class="scheduler-mapping-assistant-bar">' +
      '<div class="fw-semibold small">Mapping-Assistent</div>' +
      '<div class="small text-secondary" id="sch-mapping-assistant-hint">Wählt ein Salesforce-Profil und erzeugt Vorschläge für Ziel- und Lookup-Felder.</div>' +
      '<div class="d-flex gap-2 align-items-end ms-auto">' +
        '<select id="sch-mapping-assistant-profile" class="form-select form-select-sm" style="min-width: 160px;"></select>' +
        '<button id="sch-mapping-assistant-apply" type="button" class="btn btn-primary btn-sm sch-btn-iconized"><svg class="sch-btn-svg" viewBox="0 0 16 16" aria-hidden="true"><path d="M1 8.2 4.6 12l1.2-1.2-2.4-2.6L2.2 7zM6.8 10 15 1.8 13.7.5 5.5 8.7z"/></svg><span>Vorschläge anwenden</span></button>' +
      '</div>' +
    '</div>' +
    '<div id="sch-mapping-preview-section" class="sch-mapping-preview-section" aria-hidden="true">' +
      '<div class="d-flex align-items-center justify-content-between mb-2">' +
        '<span class="fw-semibold small">Mapping-Vorschau</span>' +
        '<button type="button" class="btn-close btn-sm" id="sch-mapping-preview-close" aria-label="Schließen"></button>' +
      '</div>' +
      '<div id="sch-mapping-preview-status" class="small text-secondary mb-2">Vorschau wird geladen...</div>' +
      '<div style="max-height: 220px; overflow: auto;">' +
        '<table class="table table-sm table-bordered mb-0" id="sch-mapping-preview-table">' +
          '<thead id="sch-mapping-preview-head"></thead>' +
          '<tbody id="sch-mapping-preview-body"></tbody>' +
        '</table>' +
      '</div>' +
    '</div>' +
    '<div class="migration-mapping-list scheduler-mapping-list">' +
      sources.map((source) => {
        const rule = findSchedulerMappingRuleBySource(source.name);
        const targetField = String(rule?.targetField || '').trim();
        const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
          normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
        );
        const transformFunction = String(rule?.transformFunction || 'NONE').trim() || 'NONE';
        const transformExpression = String(rule?.transformExpression || '').trim();
        const lookupEnabled = rule?.lookupEnabled === true;
        const lookupObject = String(rule?.lookupObject || '').trim();
        const lookupField = String(rule?.lookupField || '').trim();
        const picklistText = formatCompactPicklistMappings(rule?.picklistMappings);
        const isEmailTarget = String(targetMeta?.type || '').trim().toLowerCase() === 'email'
          || normalizeFieldKey(targetField).includes('email');
        const emailValidationEnabled = isEmailTarget && rule?.emailValidationEnabled === true;
        const emailInvalidAction = String(rule?.emailInvalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
        const rowStatus = targetField ? 'mapped' : 'open';
        const rowStatusClass = targetField ? 'text-bg-success' : 'text-bg-light';
        const rowStatusLabel = targetField ? 'Gemappt' : 'Offen';
        const targetType = targetMeta?.type || (targetField ? 'manuell' : '');
        const targetFieldIsMissing = Boolean(targetField)
          && !(Array.isArray(state.targetFields) ? state.targetFields : []).some((field) => normalizeFieldKey(field?.name) === normalizeFieldKey(targetField));
        const canCreateTargetField = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase() === 'SALESFORCE'
          && String(document.getElementById('sch-object')?.value || '').trim();
        const createFieldDefaults = getSchedulerFieldCreationDefaults(source, rule || {});
        const isPicklistTarget = ['picklist', 'multipicklist'].includes(String(targetMeta?.type || '').trim().toLowerCase());
        const rowSearchText = [source.name, source.label, source.type, targetField, targetType, transformFunction, lookupObject, lookupField].join(' ').toLowerCase();
        const rowHidden = previousFilter && !rowSearchText.includes(previousFilter.toLowerCase());
        const targetFieldFilter = targetFieldFilters.get(source.name) || '';
        const lookupFieldMeta = lookupObject ? state.schedulerLookupExternalIdFieldsByObject?.[lookupObject] : null;
        const lookupFieldMissing = useLookupSelection
          && lookupEnabled
          && lookupObject
          && lookupField
          && Array.isArray(lookupFieldMeta)
          && !lookupFieldMeta.some((field) => String(field?.name || '').trim() === lookupField);
        // Example value from first source preview row
        const previewRows = Array.isArray(state.sourcePreviewRows) ? state.sourcePreviewRows : [];
        const exampleValue = previewRows.length > 0
          ? String(previewRows.find((r) => r[source.name] !== undefined && r[source.name] !== null && r[source.name] !== '')?.[source.name] ?? previewRows[0]?.[source.name] ?? '').trim()
          : '';
        const exampleHtml = exampleValue
          ? '<span class="sch-manager-example-value" title="Beispielwert aus Quelle">' + esc(exampleValue.length > 28 ? exampleValue.slice(0, 28) + '…' : exampleValue) + '</span>'
          : '';
        const lookupDatalistKey = String(source.name || '').replace(/[^A-Za-z0-9_-]/g, '_');
        const lookupObjectListId = 'sch-lookup-objects-' + lookupDatalistKey;
        const lookupFieldListId = 'sch-lookup-fields-' + lookupDatalistKey;
        return '<section class="migration-mapping-row scheduler-mapping-row' + (rowHidden ? ' d-none' : '') + '" data-sch-map-row data-sch-map-source="' + esc(source.name) + '" data-sch-map-search="' + esc(rowSearchText) + '">' +
          '<div class="migration-mapping-row-main">' +
            '<div class="migration-mapping-source">' +
              '<span class="badge ' + rowStatusClass + '">' + esc(rowStatusLabel) + '</span>' +
              '<code>' + esc(source.name) + '</code>' +
              '<span class="small text-secondary">' + esc(source.label || source.type || 'string') + '</span>' +
              exampleHtml +
            '</div>' +
            '<div class="migration-mapping-arrow" aria-hidden="true">&rarr;</div>' +
	                  '<div class="migration-mapping-target">' +
	                    '<div class="migration-mapping-target-inputs">' +
	                      '<input class="form-control form-control-sm" type="search" placeholder="Zielfeld filtern" value="' + esc(targetFieldFilter) + '" data-sch-manager-target-filter>' +
	                      '<select class="form-select form-select-sm" data-sch-manager-target>' + getSchedulerMappingTargetOptions(targetField, targetFieldFilter) + '</select>' +
	                    '</div>' +
	                    (canCreateTargetField
	                      ? '<details class="scheduler-create-field-panel mt-2">' +
	                          '<summary class="small fw-semibold">' + (targetFieldIsMissing ? 'Fehlendes Salesforce-Feld konfigurieren' : 'Neues Salesforce-Feld anlegen') + '</summary>' +
	                          '<div class="border rounded p-2 mt-2 bg-light-subtle">' +
	                            '<div class="row g-2 align-items-end">' +
	                              '<div class="col-md-5">' +
	                                '<label class="form-label form-label-sm mb-1">API-Name</label>' +
	                                '<input class="form-control form-control-sm" value="' + esc(createFieldDefaults.fieldApiName) + '" data-sch-create-field-api>' +
	                              '</div>' +
	                              '<div class="col-md-3">' +
	                                '<label class="form-label form-label-sm mb-1">Datentyp</label>' +
	                                '<select class="form-select form-select-sm" data-sch-create-field-type>' + getSchedulerFieldCreationTypeOptions(createFieldDefaults.fieldType) + '</select>' +
	                              '</div>' +
	                              '<div class="col-md-2" data-sch-create-config="text">' +
	                                '<label class="form-label form-label-sm mb-1">Länge</label>' +
	                                '<input class="form-control form-control-sm" type="number" min="1" max="255" value="' + esc(String(createFieldDefaults.length)) + '" data-sch-create-field-length>' +
	                              '</div>' +
	                              '<div class="col-md-2">' +
	                                '<button type="button" class="btn btn-sm btn-primary w-100" data-sch-manager-create-field>Anlegen</button>' +
	                              '</div>' +
	                            '</div>' +
	                            '<div class="row g-2 mt-1" data-sch-create-config="number">' +
	                              '<div class="col-md-3">' +
	                                '<label class="form-label form-label-sm mb-1">Gesamtstellen</label>' +
	                                '<input class="form-control form-control-sm" type="number" min="1" max="18" value="' + esc(String(createFieldDefaults.precision)) + '" data-sch-create-field-precision>' +
	                              '</div>' +
	                              '<div class="col-md-3">' +
	                                '<label class="form-label form-label-sm mb-1">Dezimalstellen</label>' +
	                                '<input class="form-control form-control-sm" type="number" min="0" max="17" value="' + esc(String(createFieldDefaults.scale)) + '" data-sch-create-field-scale>' +
	                              '</div>' +
	                              '<div class="col-md-6 small text-secondary d-flex align-items-end">Für Number, Currency und Percent.</div>' +
	                            '</div>' +
	                            '<div class="mt-2" data-sch-create-config="picklist">' +
	                              '<div class="d-flex align-items-center justify-content-between mb-1">' +
	                                '<label class="form-label form-label-sm mb-0">Picklist-Werte</label>' +
	                                '<span class="small text-secondary">Komma oder Zeilenumbruch</span>' +
	                              '</div>' +
	                              '<textarea class="form-control form-control-sm" rows="4" data-sch-create-field-picklist>' + esc(createFieldDefaults.picklistValues.join('\n')) + '</textarea>' +
	                            '</div>' +
	                            '<div class="small text-secondary mt-2">Name und Typ werden aus dem Quellfeld vorbelegt. Vor dem Anlegen können sie angepasst werden.</div>' +
	                          '</div>' +
	                        '</details>'
	                      : '') +
	                    '<span class="badge bg-secondary migration-mapping-type">' + esc(targetType) + (targetMeta?.requiredOnCreate === true ? ' *' : '') + '</span>' +
	                  '</div>' +
            '<div class="migration-mapping-transform">' +
              '<label class="form-label form-label-sm mb-1">Umwandlung</label>' +
              '<select class="form-select form-select-sm" data-sch-manager-transform>' +
                ['NONE','TRIM','UPPERCASE','LOWERCASE','TO_INTEGER','TO_DECIMAL','TO_BOOLEAN','DATETIME_ISO','STATIC'].map((fn) =>
                  '<option value="' + esc(fn) + '"' + (transformFunction === fn ? ' selected' : '') + '>' + esc(fn) + '</option>'
                ).join('') +
              '</select>' +
            '</div>' +
          '</div>' +
          '<details class="migration-mapping-details"' + (openDetailSources.has(source.name) ? ' open' : '') + '>' +
            '<summary>Details</summary>' +
            '<div class="migration-mapping-detail-grid">' +
              '<div>' +
                '<label class="form-label form-label-sm mb-1">Parameter / statischer Wert</label>' +
                '<input class="form-control form-control-sm" value="' + esc(transformExpression) + '" placeholder="Nur bei STATIC oder Transform mit Parameter" data-sch-manager-transform-expression>' +
              '</div>' +
              '<div class="migration-mapping-lookup-box">' +
                '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-sch-manager-lookup-enabled' + (lookupEnabled ? ' checked' : '') + '><label class="form-check-label small">Lookup aktivieren</label></div>' +
                '<div class="migration-mapping-detail-grid migration-mapping-detail-grid-compact">' +
                  (useLookupSelection
                    ? '<div><input class="form-control form-control-sm" list="' + esc(lookupObjectListId) + '" placeholder="SF Objekt suchen" value="' + esc(lookupObject) + '" data-sch-manager-lookup-object><datalist id="' + esc(lookupObjectListId) + '">' + getSchedulerLookupObjectDatalistOptions(lookupObject) + '</datalist></div>'
                    : '<input class="form-control form-control-sm" placeholder="Lookup Objekt" value="' + esc(lookupObject) + '" data-sch-manager-lookup-object>') +
                  (useLookupSelection
                    ? '<div><input class="form-control form-control-sm" list="' + esc(lookupFieldListId) + '" placeholder="Lookup Feld suchen" value="' + esc(lookupField) + '" data-sch-manager-lookup-field><datalist id="' + esc(lookupFieldListId) + '">' + getSchedulerLookupFieldDatalistOptions(lookupObject, lookupField) + '</datalist></div>'
                    : '<input class="form-control form-control-sm" placeholder="Lookup Feld / External ID" value="' + esc(lookupField) + '" data-sch-manager-lookup-field>') +
                '</div>' +
                (lookupFieldMissing ? '<div class="small text-warning mt-1">Gespeichertes Lookup-Feld ist keine External ID mehr.</div>' : '') +
              '</div>' +
              '<div>' +
                '<div class="d-flex align-items-center justify-content-between gap-2 mb-1">' +
                  '<label class="form-label form-label-sm mb-0">Picklist-Mapping</label>' +
                  (isPicklistTarget ? '<button type="button" class="btn btn-sm btn-outline-secondary" data-sch-manager-picklist-autofill>Werte übernehmen</button>' : '') +
                '</div>' +
                '<div style="max-height: 200px; overflow-y: auto; border: 1px solid #dee2e6; border-radius: 0.25rem; margin-bottom: 0.5rem;">' +
                  '<table class="table table-sm mb-0" data-sch-manager-picklist-table>' +
                    '<thead style="position: sticky; top: 0; background: #f8f9fa;">' +
                      '<tr><th style="width: 45%;">Quelle</th><th style="width: 45%;">Ziel</th><th style="width: 10%; text-align: center;">Aktion</th></tr>' +
                    '</thead>' +
                    '<tbody>' +
                      (Array.isArray(rule?.picklistMappings) && rule.picklistMappings.length > 0
                        ? rule.picklistMappings.map((entry, idx) =>
                            '<tr data-picklist-idx="' + esc(String(idx)) + '">' +
                              '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-source" type="text" value="' + esc(String(entry?.source || '')) + '" placeholder="Quellwert" style="font-size: 0.8rem;"></td>' +
                              '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-target" type="text" value="' + esc(String(entry?.target || '')) + '" placeholder="Zielwert" style="font-size: 0.8rem;"></td>' +
                              '<td style="padding: 0.25rem 0.5rem; text-align: center;"><button type="button" class="btn btn-sm btn-outline-danger picklist-remove" data-picklist-idx="' + esc(String(idx)) + '" style="padding: 0.1rem 0.3rem; font-size: 0.7rem;">×</button></td>' +
                            '</tr>'
                          ).join('')
                        : '<tr><td colspan="3" class="text-secondary text-center" style="padding: 0.5rem; font-size: 0.9rem;">Keine Picklist-Einträge</td></tr>') +
                    '</tbody>' +
                  '</table>' +
                '</div>' +
                '<button type="button" class="btn btn-sm btn-outline-secondary picklist-add-row" style="font-size: 0.85rem;">+ Eintrag hinzufügen</button>' +
              '</div>' +
              '<div class="scheduler-email-options' + (isEmailTarget ? '' : ' d-none') + '" data-sch-manager-email-options style="border-top: 1px solid #dee2e6; padding-top: 0.75rem; margin-top: 0.75rem;">' +
                '<label class="form-label form-label-sm mb-2" style="font-weight: 600;">E-Mail-Validierung</label>' +
                '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" data-sch-manager-email-validation' + (emailValidationEnabled ? ' checked' : '') + ' id="email-val-' + esc(source.name) + '"><label class="form-check-label small" for="email-val-' + esc(source.name) + '">E-Mail-Adresse validieren</label></div>' +
                '<div class="ps-3">' +
                  '<label class="form-label form-label-sm mb-2">Bei ungültiger E-Mail:</label>' +
                  '<select class="form-select form-select-sm" data-sch-manager-email-invalid-action>' +
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
    '</div>';

  const updateRuleFromRow = (row) => {
    const sourceName = String(row.getAttribute('data-sch-map-source') || '').trim();
    if (!sourceName) {
      return;
    }

    const source = sources.find((item) => normalizeFieldKey(item.name) === normalizeFieldKey(sourceName)) || { name: sourceName, type: 'string' };
    const targetField = String(row.querySelector('[data-sch-manager-target]')?.value || '').trim();
    const existing = findSchedulerMappingRuleBySource(sourceName);

    if (!targetField) {
      if (existing) {
        state.mappingRules = state.mappingRules.filter((rule) => rule.id !== existing.id);
        if (state.selectedMappingRuleId === existing.id) {
          state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
        }
      }
      renderMappingRulesTable();
      return;
    }

    const rule = existing || createMappingRuleFromSource(source);
    rule.sourceField = source.name;
    rule.sourceType = source.type || rule.sourceType || 'string';
    rule.targetField = targetField;
    const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
      normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
    );
    const lookupAllowedForTarget = ['reference', 'id'].includes(String(targetMeta?.type || '').trim().toLowerCase());
    rule.targetType = getSchedulerMappingRuleTargetType({ ...rule, targetField, targetType: targetMeta?.type || rule.targetType });
    rule.transformFunction = String(row.querySelector('[data-sch-manager-transform]')?.value || 'NONE').trim() || 'NONE';
    rule.transformExpression = String(row.querySelector('[data-sch-manager-transform-expression]')?.value || '').trim();
    rule.lookupObject = String(row.querySelector('[data-sch-manager-lookup-object]')?.value || '').trim();
    rule.lookupField = String(row.querySelector('[data-sch-manager-lookup-field]')?.value || '').trim();
    const targetReferenceObjects = Array.isArray(targetMeta?.referenceTo)
      ? targetMeta.referenceTo.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    if (lookupAllowedForTarget && targetReferenceObjects.length && (!rule.lookupObject || !targetReferenceObjects.includes(rule.lookupObject))) {
      rule.lookupObject = targetReferenceObjects[0];
    }
    rule.lookupEnabled = lookupAllowedForTarget && (Boolean(row.querySelector('[data-sch-manager-lookup-enabled]')?.checked) || Boolean(rule.lookupObject || rule.lookupField));
    if (!rule.lookupEnabled) {
      rule.lookupObject = '';
      rule.lookupField = '';
    }
    if (rule.lookupEnabled && canUseSchedulerLookupSelection()) {
      const availableLookupFields = Array.isArray(state.schedulerLookupExternalIdFieldsByObject?.[rule.lookupObject])
        ? state.schedulerLookupExternalIdFieldsByObject[rule.lookupObject]
        : null;
      if (availableLookupFields && rule.lookupField && !availableLookupFields.some((field) => String(field?.name || '').trim() === rule.lookupField)) {
        rule.lookupField = '';
      }
    }
    rule.emailValidationEnabled = Boolean(row.querySelector('[data-sch-manager-email-validation]')?.checked);
    rule.emailInvalidAction = String(row.querySelector('[data-sch-manager-email-invalid-action]')?.value || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY';
    
    // Read picklist mappings from table
    const picklistEntries = [];
    const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
    if (picklistTable) {
      picklistTable.querySelectorAll('tr[data-picklist-idx]').forEach((tableRow) => {
        const sourceInput = tableRow.querySelector('input.picklist-source');
        const targetInput = tableRow.querySelector('input.picklist-target');
        const source = sourceInput ? String(sourceInput.value || '').trim() : '';
        const target = targetInput ? String(targetInput.value || '').trim() : '';
        if (source || target) {
          picklistEntries.push({ source, target });
        }
      });
    }
    rule.picklistMappings = picklistEntries;
    
    if (!existing) {
      state.mappingRules.push(rule);
      state.selectedMappingRuleId = rule.id;
    }
    renderMappingRulesTable();
  };

  const fillPicklistMappingsFromTarget = (row) => {
    const sourceName = String(row.getAttribute('data-sch-map-source') || '').trim();
    const targetField = String(row.querySelector('[data-sch-manager-target]')?.value || '').trim();
    if (!sourceName || !targetField) {
      return;
    }
    const targetMeta = (Array.isArray(state.targetFields) ? state.targetFields : []).find((field) =>
      normalizeFieldKey(field?.name) === normalizeFieldKey(targetField)
    );
    const targetValues = Array.isArray(targetMeta?.picklistValues)
      ? targetMeta.picklistValues
          .map((entry) => String(entry?.value || entry?.label || '').trim())
          .filter(Boolean)
      : [];
    if (!targetValues.length) {
      showModalError('Für ' + targetField + ' wurden keine Picklist-Werte in den Salesforce-Metadaten gefunden. Bitte Metadaten neu laden.');
      return;
    }

    const sourceValues = getSchedulerSourceDistinctValues(sourceName, 200);
    const sourceValuesByKey = new Map(sourceValues.map((value) => [value.toLowerCase(), value]));
    const existing = findSchedulerMappingRuleBySource(sourceName);
    const source = sources.find((item) => normalizeFieldKey(item.name) === normalizeFieldKey(sourceName)) || { name: sourceName, type: 'string' };
    const rule = existing || createMappingRuleFromSource(source);
    const existingMappings = Array.isArray(rule.picklistMappings) ? rule.picklistMappings : [];
    const existingKeys = new Set(existingMappings.map((entry) => (String(entry?.source || '').trim() + '=>' + String(entry?.target || '').trim()).toLowerCase()));
    const generated = targetValues.map((targetValue) => ({
      source: sourceValuesByKey.get(targetValue.toLowerCase()) || targetValue,
      target: targetValue
    }));
    rule.picklistMappings = [
      ...existingMappings,
      ...generated.filter((entry) => !existingKeys.has((entry.source + '=>' + entry.target).toLowerCase()))
    ];
    rule.targetField = targetField;
    rule.targetType = getSchedulerMappingRuleTargetType({ ...rule, targetField, targetType: targetMeta?.type || rule.targetType });
    if (!existing) {
      state.mappingRules.push(rule);
    }
    renderMappingRulesTable();
  };

  shell.querySelector('[data-sch-map-filter]')?.addEventListener('input', (event) => {
    const term = String(event.target?.value || '').trim().toLowerCase();
    shell.querySelectorAll('[data-sch-map-row]').forEach((row) => {
      const searchText = String(row.getAttribute('data-sch-map-search') || '').toLowerCase();
      row.classList.toggle('d-none', Boolean(term) && !searchText.includes(term));
    });
  });
  document.getElementById('sch-manager-load-fields')?.addEventListener('click', loadMappingFields);
  shell.querySelectorAll('[data-sch-map-row]').forEach((row) => {
    syncSchedulerCreateFieldPanel(row);
    row.querySelector('[data-sch-create-field-type]')?.addEventListener('change', () => syncSchedulerCreateFieldPanel(row));
    row.querySelector('[data-sch-create-field-api]')?.addEventListener('blur', (event) => {
      event.target.value = normalizeSchedulerCustomFieldApiName(event.target.value || '');
    });
    row.querySelector('[data-sch-manager-target-filter]')?.addEventListener('input', (event) => {
      const targetSelect = row.querySelector('[data-sch-manager-target]');
      if (!targetSelect) {
        return;
      }
      const currentTarget = String(targetSelect.value || '').trim();
      targetSelect.innerHTML = getSchedulerMappingTargetOptions(currentTarget, event.target?.value || '');
      if (currentTarget) {
        targetSelect.value = currentTarget;
      }
    });
    row.querySelector('[data-sch-manager-create-field]')?.addEventListener('click', async () => {
      const sourceName = String(row.getAttribute('data-sch-map-source') || '').trim();
      const source = sources.find((item) => normalizeFieldKey(item.name) === normalizeFieldKey(sourceName)) || { name: sourceName, type: 'string' };
      const rule = findSchedulerMappingRuleBySource(sourceName);
      await createSchedulerSalesforceFieldFromRow(row, source, rule);
    });
    row.querySelector('[data-sch-manager-picklist-autofill]')?.addEventListener('click', () => {
      fillPicklistMappingsFromTarget(row);
    });

    row.querySelectorAll('[data-sch-manager-target], [data-sch-manager-transform], [data-sch-manager-transform-expression], [data-sch-manager-lookup-enabled], [data-sch-manager-lookup-field], [data-sch-manager-email-validation], [data-sch-manager-email-invalid-action]').forEach((field) => {
      field.addEventListener('change', () => updateRuleFromRow(row));
    });
    row.querySelectorAll('input[data-sch-manager-lookup-field]').forEach((field) => {
      field.addEventListener('blur', () => updateRuleFromRow(row));
    });

    // Picklist table inputs
    const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
    if (picklistTable) {
      picklistTable.querySelectorAll('input.picklist-source, input.picklist-target').forEach((input) => {
        input.addEventListener('change', () => updateRuleFromRow(row));
        input.addEventListener('blur', () => updateRuleFromRow(row));
      });
    }

    // Picklist remove buttons
    row.querySelectorAll('button.picklist-remove').forEach((btn) => {
      btn.addEventListener('click', (event) => {
        event.preventDefault();
        const idx = String(btn.getAttribute('data-picklist-idx') || '').trim();
        const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
        if (picklistTable) {
          const tableRow = picklistTable.querySelector('tr[data-picklist-idx="' + idx + '"]');
          if (tableRow) {
            tableRow.remove();
          }
        }
        updateRuleFromRow(row);
      });
    });

    // Picklist add row button
    const addBtn = row.querySelector('button.picklist-add-row');
    if (addBtn) {
      addBtn.addEventListener('click', (event) => {
        event.preventDefault();
        const picklistTable = row.querySelector('[data-sch-manager-picklist-table]');
        if (picklistTable) {
          const tbody = picklistTable.querySelector('tbody');
          if (tbody) {
            // Remove "no entries" row if exists
            const emptyRow = tbody.querySelector('tr td.text-secondary');
            if (emptyRow) {
              emptyRow.closest('tr').remove();
            }

            const newIdx = String(Date.now());
            const newRow = document.createElement('tr');
            newRow.setAttribute('data-picklist-idx', newIdx);
            newRow.innerHTML = '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-source" type="text" placeholder="Quellwert" style="font-size: 0.8rem;"></td>' +
              '<td style="padding: 0.25rem 0.5rem;"><input class="form-control form-control-sm picklist-target" type="text" placeholder="Zielwert" style="font-size: 0.8rem;"></td>' +
              '<td style="padding: 0.25rem 0.5rem; text-align: center;"><button type="button" class="btn btn-sm btn-outline-danger picklist-remove" data-picklist-idx="' + newIdx + '" style="padding: 0.1rem 0.3rem; font-size: 0.7rem;">×</button></td>';
            
            tbody.appendChild(newRow);

            // Add event listeners to new inputs
            newRow.querySelectorAll('input.picklist-source, input.picklist-target').forEach((input) => {
              input.addEventListener('change', () => updateRuleFromRow(row));
              input.addEventListener('blur', () => updateRuleFromRow(row));
            });

            // Add event listener to remove button
            newRow.querySelector('button.picklist-remove').addEventListener('click', (event) => {
              event.preventDefault();
              newRow.remove();
              updateRuleFromRow(row);
            });

            // Focus on first input
            newRow.querySelector('input.picklist-source').focus();
          }
        }
      });
    }

    const lookupObjectSelect = row.querySelector('[data-sch-manager-lookup-object]');
    if (lookupObjectSelect) {
      lookupObjectSelect.addEventListener('change', async () => {
        const selectedLookupObject = String(lookupObjectSelect.value || '').trim();
        if (selectedLookupObject && canUseSchedulerLookupSelection()) {
          await loadSchedulerLookupExternalIdFields(selectedLookupObject);
          const lookupFieldSelect = row.querySelector('[data-sch-manager-lookup-field]');
          if (lookupFieldSelect) {
            const currentLookupField = String(lookupFieldSelect.value || '').trim();
            const listId = String(lookupFieldSelect.getAttribute('list') || '').trim();
            const datalist = listId ? document.getElementById(listId) : null;
            if (datalist) {
              datalist.innerHTML = getSchedulerLookupFieldDatalistOptions(selectedLookupObject, currentLookupField);
            } else {
              lookupFieldSelect.innerHTML = getSchedulerLookupFieldOptions(selectedLookupObject, currentLookupField);
            }
          }
        }
        updateRuleFromRow(row);
      });
      lookupObjectSelect.addEventListener('blur', () => updateRuleFromRow(row));
    }
  });
}

async function autoMapByName() {
  clearModalError();

  const sourceFields = Array.isArray(state.mappingFields) ? state.mappingFields : [];
  if (!sourceFields.length) {
    showModalError('Bitte zuerst Quellfelder laden bevor Auto-Mapping ausgeführt wird.');
    return;
  }

  const selectedTargetObject = String(document.getElementById('sch-object')?.value || '').trim();
  let targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
  if (selectedTargetObject && !targetFields.length) {
    await loadTargetFields();
    targetFields = Array.isArray(state.targetFields) ? state.targetFields : [];
  }

  if (!targetFields.length) {
    showModalError(selectedTargetObject
      ? 'Zielfelder fuer ' + selectedTargetObject + ' konnten nicht geladen werden. Bitte Salesforce-Verbindung, Zielsystem und Objektberechtigungen pruefen.'
      : 'Bitte zuerst ein Zielobjekt wählen, damit Zielfelder geladen werden können.');
    return;
  }

  const generatedMappings = await generateSalesforceMappings(
    sourceFields,
    targetFields,
    {
      targetObjectApiName: selectedTargetObject,
      profile: String(state.scheduleMappingAssistantProfile || '').trim() || undefined
    }
  );

  const rulesBySourceKey = new Map();
  state.mappingRules.forEach((rule) => {
    const sourceKey = normalizeFieldKey(rule?.sourceField);
    if (!sourceKey) {
      return;
    }
    const bucket = rulesBySourceKey.get(sourceKey) || [];
    bucket.push(rule);
    rulesBySourceKey.set(sourceKey, bucket);
  });

  let updated = 0;
  let added = 0;

  for (const sourceField of sourceFields) {
    const sourceName = String(sourceField?.name || '').trim();
    const sourceKey = normalizeFieldKey(sourceName);
    if (!sourceName || !sourceKey) {
      continue;
    }

    const generated = generatedMappings.find((item) => normalizeFieldKey(item?.sourceField) === sourceKey);
    const matchedTarget = String(generated?.targetField || '').trim();
    if (!matchedTarget) {
      continue;
    }

    const existingRules = rulesBySourceKey.get(sourceKey) || [];
    const alreadyMappedToTarget = existingRules.some((rule) =>
      normalizeFieldKey(rule?.targetField) === normalizeFieldKey(matchedTarget)
    );
    if (alreadyMappedToTarget) {
      continue;
    }

    const placeholderRule = existingRules.find((rule) => {
      const target = String(rule?.targetField || '').trim();
      return !target || normalizeFieldKey(target) === sourceKey;
    });

    if (placeholderRule) {
      placeholderRule.targetField = matchedTarget;
      placeholderRule.sourceType = String(sourceField?.type || placeholderRule.sourceType || 'string');
      placeholderRule.lookupEnabled = generated?.lookupEnabled === true;
      placeholderRule.lookupObject = String(generated?.lookupObject || '');
      placeholderRule.lookupField = String(generated?.lookupField || '');
      updated += 1;
      continue;
    }

    const newRule = createMappingRuleFromSource(sourceField);
    newRule.targetField = matchedTarget;
    newRule.lookupEnabled = generated?.lookupEnabled === true;
    newRule.lookupObject = String(generated?.lookupObject || '');
    newRule.lookupField = String(generated?.lookupField || '');
    state.mappingRules.push(newRule);
    const bucket = rulesBySourceKey.get(sourceKey) || [];
    bucket.push(newRule);
    rulesBySourceKey.set(sourceKey, bucket);
    added += 1;
  }

  if (updated === 0 && added === 0) {
    showModalError('Keine gleichnamigen Felder zwischen Quelle und Ziel gefunden.');
    return;
  }

  if (!state.selectedMappingRuleId && state.mappingRules.length) {
    state.selectedMappingRuleId = state.mappingRules[0].id;
  }

  syncMappingDefinitionFromRules();
  renderMappingRulesTable();
}

function hydrateMappingRulesFromDefinition() {
  const mappingRaw = document.getElementById('sch-mapping').value || '';
  const raw = mappingRaw.trim();
  state.rawMappingEditorDirty = false;
  state.mappingRules = [];
  state.selectedMappingRuleId = '';

  if (!raw) {
    renderMappingRulesTable();
    return;
  }

  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      state.mappingRules = parsed
        .filter((item) => item && (item.sourceField || item.targetField))
        .map((item) => {
          const storedTransformFunction = String(item.transformFunction || 'NONE').trim() || 'NONE';
          const lookupDetails = extractLookupTransformDetails(storedTransformFunction);
          const legacyLookup = parseLegacyLookupValue(item.lookup || item.lookupPath || item.lookupValue);
          return {
            id: generateMappingRuleId(),
            sourceField: String(item.sourceField || '').trim(),
            sourceType: String(item.sourceType || 'string'),
            targetField: String(item.targetField || '').trim(),
            lookupEnabled: !!item.lookupEnabled || !!lookupDetails,
            lookupObject: lookupDetails ? lookupDetails.lookupObject : String(item.lookupObject || legacyLookup?.lookupObject || ''),
            lookupField: lookupDetails ? lookupDetails.lookupField : String(item.lookupField || legacyLookup?.lookupField || ''),
            transformFunction: lookupDetails ? 'NONE' : storedTransformFunction,
            transformExpression: String(item.transformExpression || ''),
            targetType: String(item.targetType || item.sourceType || 'string'),
            emailValidationEnabled: item.emailValidationEnabled === true || item?.emailValidation?.enabled === true,
            emailInvalidAction: String(item.emailInvalidAction || item?.emailValidation?.invalidAction || 'EMPTY').trim().toUpperCase() === 'ERROR' ? 'ERROR' : 'EMPTY',
            picklistMappings: Array.isArray(item.picklistMappings) ? item.picklistMappings.map((entry) => ({
              source: String(entry?.source || ''),
              target: String(entry?.target || '')
            })) : []
          };
        });
    }
  } catch {
    const dslRules = raw.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    state.mappingRules = dslRules.map((line) => {
      const [leftPart, rightPart] = line.split('=');
      const [targetField] = String(leftPart || '').split(';').map((item) => item.trim());
      const rightParts = String(rightPart || '').split(';').map((item) => item.trim());
      const sourceField = rightParts[0] || '';
      const transformFunction = rightParts[1] || 'NONE';
      const lookupDetails = extractLookupTransformDetails(transformFunction);
      return {
        id: generateMappingRuleId(),
        sourceField,
        sourceType: 'string',
        targetField: targetField || sourceField,
        lookupEnabled: !!lookupDetails,
        lookupObject: lookupDetails ? lookupDetails.lookupObject : '',
        lookupField: lookupDetails ? lookupDetails.lookupField : '',
        transformFunction: lookupDetails ? 'NONE' : transformFunction,
        transformExpression: '',
        targetType: 'string',
        emailValidationEnabled: false,
        emailInvalidAction: 'EMPTY',
        picklistMappings: []
      };
    });
  }

  if (state.mappingRules.length) {
    state.selectedMappingRuleId = state.mappingRules[0].id;
  }
  reconcileMappingRuleSourceFields();
  renderMappingRulesTable();
}

function applySelectedMappingDetailChanges() {
  const selectedRule = state.mappingRules.find((item) => item.id === state.selectedMappingRuleId);
  if (!selectedRule) {
    return;
  }

  selectedRule.targetField = String(document.getElementById('sch-map-detail-target').value || '').trim();
  selectedRule.transformFunction = String(document.getElementById('sch-map-detail-transform').value || 'NONE').trim() || 'NONE';
  selectedRule.transformExpression = String(document.getElementById('sch-map-detail-transform-expression').value || '').trim();
  selectedRule.lookupEnabled = !!document.getElementById('sch-map-detail-lookup-enabled').checked;
  selectedRule.lookupObject = String(document.getElementById('sch-map-detail-lookup-object').value || '').trim();
  selectedRule.lookupField = String(document.getElementById('sch-map-detail-lookup-field').value || '').trim();

  // Read picklist mappings from table
  const picklistEntries = [];
  const picklistTable = document.getElementById('sch-map-detail-picklist-table');
  if (picklistTable) {
    picklistTable.querySelectorAll('tr[data-picklist-idx]').forEach((row) => {
      const sourceInput = row.querySelector('input.picklist-source');
      const targetInput = row.querySelector('input.picklist-target');
      const source = sourceInput ? String(sourceInput.value || '').trim() : '';
      const target = targetInput ? String(targetInput.value || '').trim() : '';
      if (source || target) {
        picklistEntries.push({ source, target });
      }
    });
  }
  selectedRule.picklistMappings = picklistEntries;

  renderMappingRulesTable();
}

function deleteSelectedMappingRule() {
  const selectedRuleId = state.selectedMappingRuleId;
  if (!selectedRuleId) {
    return;
  }
  state.mappingRules = state.mappingRules.filter((item) => item.id !== selectedRuleId);
  state.selectedMappingRuleId = state.mappingRules[0]?.id || '';
  renderMappingRulesTable();
}

async function loadScheduleOptions() {
  const response = await safeRequest('/api/schedules/options', null);
  if (!response) {
    return;
  }

  state.scheduleOptions = response;
}

function formatPreviewCell(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

function renderGenericPreviewTable(headerId, bodyId, previewData) {
  const header = document.getElementById(headerId);
  const body = document.getElementById(bodyId);

  if (!header || !body) {
    return;
  }

  if (!Array.isArray(previewData) || previewData.length === 0) {
    header.innerHTML = '<tr><th>Keine Daten</th></tr>';
    body.innerHTML = '<tr><td class="text-secondary">Keine Vorschaudaten verfügbar</td></tr>';
    return;
  }

  const firstRecord = previewData[0] || {};
  const columns = Object.keys(firstRecord).slice(0, 10);

  header.innerHTML = '<tr>' + columns.map((col) => '<th>' + esc(col) + '</th>').join('') + '</tr>';
  body.innerHTML = previewData.slice(0, 10).map((record) =>
    '<tr>' + columns.map((col) => '<td>' + esc(formatPreviewCell(record[col])) + '</td>').join('') + '</tr>'
  ).join('');
}

function highlightSqlQuery(query) {
  return esc(query || '')
    .replace(/('[^']*')/g, '<span class="sql-string">$1</span>')
    .replace(/(SELECT|FROM|WHERE|AND|OR|ORDER|BY|GROUP|LIMIT|TOP|INNER|LEFT|RIGHT|JOIN|ON|AS|DISTINCT|INSERT|UPDATE|DELETE|INTO|VALUES|SET|LIKE|IS|NULL|NOT|ASC|DESC)/gi, '<span class="sql-keyword">$1</span>')
    .replace(/([0-9]+)/g, '<span class="sql-number">$1</span>');
}

function updateSourceQueryAssist() {
  const sourceType = document.getElementById('sch-source-type').value;
  const sourceDefinition = document.getElementById('sch-source-definition').value;
  const deltaWrap = document.getElementById('sch-source-delta-wrap');
  const deltaHelp = document.getElementById('sch-source-delta-help');
  const deltaStrategy = document.getElementById('sch-source-delta-strategy').value;
  const deltaField = document.getElementById('sch-source-delta-field').value;
  const deltaCurrentInput = document.getElementById('sch-source-delta-current');
  const deltaRecordIdInput = document.getElementById('sch-source-delta-record-id');
  const afterExportValue = String(document.getElementById('sch-source-after-export').value || '').trim();
  const afterExportWrap = document.getElementById('sch-source-after-export-wrap');
  const highlightWrap = document.getElementById('sch-source-sql-highlight-wrap');
  const highlight = document.getElementById('sch-source-sql-highlight');
  const status = document.getElementById('sch-source-test-status');
  const isSql = sourceType === 'MSSQL_SQL';
  const isFile = sourceType === 'FILE_CSV' || sourceType === 'FILE_EXCEL' || sourceType === 'FILE_JSON';
  const isRest = sourceType === 'REST_API';
  const supportsDelta = isSql || sourceType === 'SALESFORCE_SOQL';
  const supportsAfterExport = sourceType === 'SALESFORCE_SOQL';

  deltaWrap.classList.toggle('d-none', !supportsDelta);
  afterExportWrap.classList.toggle('d-none', !supportsAfterExport);
  document.getElementById('sch-source-relative-directory-wrap').classList.toggle('d-none', !isFile);
  document.getElementById('sch-source-archive-relative-directory-wrap').classList.toggle('d-none', !isFile);
  document.getElementById('sch-source-path-summary-wrap').classList.toggle('d-none', !isFile);
  updateScheduleTypeUi();
  if (deltaCurrentInput) {
    deltaCurrentInput.disabled = !supportsDelta;
  }
  if (deltaRecordIdInput) {
    deltaRecordIdInput.disabled = !supportsDelta || deltaStrategy !== 'datetime';
  }
  if (supportsDelta) {
    const normalizedDeltaField = String(deltaField || '').trim().toLowerCase();
    const usesMutableSalesforceTimestamp = sourceType === 'SALESFORCE_SOQL'
      && deltaStrategy === 'datetime'
      && (normalizedDeltaField === 'lastmodifieddate' || normalizedDeltaField === 'systemmodstamp')
      && !!afterExportValue;

    deltaHelp.textContent = usesMutableSalesforceTimestamp
      ? 'Warnung: After Export plus LastModifiedDate/SystemModstamp fuehrt auf demselben Salesforce-Objekt leicht zu Wiederholungsschleifen. Fuer produktive Exporte besser ID oder ein separates fachliches Delta-Feld verwenden.'
      : deltaStrategy && deltaField.trim()
        ? 'Delta aktiv: ' + deltaStrategy + ' auf Feld ' + deltaField.trim() + '. Ohne gespeicherten Checkpoint laeuft der erste Lauf voll; danach wird der letzte Ergebniswert gespeichert. Bei SQL muss das Feld in der SELECT-Liste enthalten sein, am besten mit eindeutigem Alias.'
        : 'Optional: Delta-Lauf ueber ein Feld aktivieren. Unterstuetzt Datum, Timestamp und ID.';
  }

  highlightWrap.classList.toggle('d-none', !isSql);
  if (isSql) {
    highlight.innerHTML = highlightSqlQuery(sourceDefinition || '-- keine SQL-Abfrage --');
    status.textContent = 'SQL-Abfrage kann direkt getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
  } else if (sourceType === 'SALESFORCE_SOQL') {
    highlight.textContent = '';
    status.textContent = 'SOQL-Abfrage kann direkt gegen Salesforce getestet werden. Es werden bis zu 10 Datensätze angezeigt.';
  } else if (sourceType === 'FILEMAKER_SQL') {
    highlight.textContent = '';
    status.textContent = 'FileMaker-Quelle: Source Definition z. B. {"layout":"Kontakte","limit":10}. Felder werden aus den Layout-Metadaten gelesen.';
  } else if (isRest) {
    highlight.textContent = '';
    status.textContent = 'REST-Quelle: Source Definition z. B. {"endpoint":"/api/customers","method":"GET","resultPath":"items"}. Es werden bis zu 10 Datensätze angezeigt.';
  } else if (isFile) {
    highlight.textContent = '';
    status.textContent = 'Datei-Quelle: Source Definition z. B. {"fileName":"datei.json","format":"json"} oder CSV/Excel. Connector muss ein Datei-Connector sein.';
  } else {
    highlight.textContent = '';
    status.textContent = 'Es werden bis zu 10 Datensätze angezeigt.';
  }

  updateScheduleFilePathSummaries();
}

function isFileScheduleSourceType(sourceType) {
  return sourceType === 'FILE_CSV' || sourceType === 'FILE_EXCEL' || sourceType === 'FILE_JSON';
}

function isFileScheduleTargetType(targetType) {
  return targetType === 'FILE_CSV' || targetType === 'FILE_EXCEL' || targetType === 'FILE_JSON';
}

function setClosestFieldVisible(elementId, visible) {
  const element = document.getElementById(elementId);
  const container = element?.closest('.col-md-12, .col-md-8, .col-md-6, .col-md-5, .col-md-4, .col-md-3, .col-md-2, .col-12');
  if (container) {
    container.classList.toggle('d-none', !visible);
  }
}

function updateScheduleTypeUi() {
  const sourceType = String(document.getElementById('sch-source-type')?.value || '').trim().toUpperCase();
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();
  const isFileSource = isFileScheduleSourceType(sourceType);
  const isFileTarget = isFileScheduleTargetType(targetType);
  const isSalesforceTarget = targetType === 'SALESFORCE' || targetType === 'SALESFORCE_GLOBAL_PICKLIST';
  const isMssqlTarget = targetType === 'MSSQL' || targetType === 'MSSQL_SQL';

  setClosestFieldVisible('sch-object', !isFileTarget);
  setClosestFieldVisible('sch-operation', !isFileTarget);
  setClosestFieldVisible('sch-target-system', !isFileTarget || isSalesforceTarget || isMssqlTarget);

  const mappingManager = document.getElementById('sch-mapping-manager');
  const mappingManagerWrap = mappingManager?.closest('.col-md-12');
  const mappingDefinition = document.getElementById('sch-mapping');
  const mappingDefinitionWrap = mappingDefinition?.closest('.col-md-12');
  const fileOptionsWrap = document.getElementById('sch-target-file-options-wrap');
  let fileTargetHint = document.getElementById('sch-file-target-mapping-hint');
  if (!fileTargetHint && mappingManagerWrap) {
    fileTargetHint = document.createElement('div');
    fileTargetHint.id = 'sch-file-target-mapping-hint';
    fileTargetHint.className = 'alert alert-light border small mb-3 d-none';
    fileTargetHint.textContent = 'Datei-Ziele schreiben die Quelldaten direkt. Mapping und Zielfeld-Auswahl sind dafür nicht erforderlich.';
    mappingManagerWrap.parentElement?.insertBefore(fileTargetHint, mappingManagerWrap);
  }
  if (mappingManagerWrap) {
    mappingManagerWrap.classList.toggle('d-none', false);
  }
  if (mappingDefinitionWrap) {
    mappingDefinitionWrap.classList.toggle('d-none', false);
  }
  if (fileTargetHint) {
    fileTargetHint.classList.toggle('d-none', !isFileTarget);
    fileTargetHint.textContent = 'Datei-Ziele verwenden das Mapping für Spaltennamen (Header) und Reihenfolge. Datei-Optionen (Charset, Separator, Qualifier, Dateiname) sind oben konfigurierbar.';
  }
  if (fileOptionsWrap) {
    fileOptionsWrap.classList.toggle('d-none', !isFileTarget);
  }

  if (isFileSource && !String(document.getElementById('sch-source-system')?.value || '').trim()) {
    document.getElementById('sch-source-system').value = 'File';
  }
  if (isFileTarget && !String(document.getElementById('sch-target-system')?.value || '').trim()) {
    document.getElementById('sch-target-system').value = 'File';
  }

  if (isFileTarget) {
    const fileNameInput = document.getElementById('sch-target-file-name');
    const charsetSelect = document.getElementById('sch-target-file-charset');
    const delimiterInput = document.getElementById('sch-target-file-delimiter');
    const qualifierInput = document.getElementById('sch-target-file-text-qualifier');
    const sheetNameInput = document.getElementById('sch-target-file-sheet-name');
    const defaultFileName = getDefaultScheduleTargetFileNameForType(targetType);
    const currentFileName = String(fileNameInput?.value || '').trim();
    const canAutoAdjustFileName = !state.scheduleTargetFileNameDirty
      && (!currentFileName
        || isDefaultScheduleTargetFileName(currentFileName)
        || (String(state.scheduleTargetFileNameLastAuto || '').trim() === currentFileName));
    if (charsetSelect && !String(charsetSelect.value || '').trim()) {
      charsetSelect.value = 'utf8';
    }
    if (fileNameInput && canAutoAdjustFileName) {
      fileNameInput.value = defaultFileName;
      state.scheduleTargetFileNameLastAuto = defaultFileName;
      state.scheduleTargetFileNameDirty = false;
    }
    if (targetType === 'FILE_EXCEL') {
      if (sheetNameInput && !String(sheetNameInput.value || '').trim()) {
        sheetNameInput.value = 'Sheet1';
      }
    } else {
      if (delimiterInput && !String(delimiterInput.value || '').trim()) {
        delimiterInput.value = ';';
      }
      if (qualifierInput && !String(qualifierInput.value || '').trim()) {
        qualifierInput.value = '"';
      }
    }
  }
}

function detectFileFormatFromName(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized.endsWith('.json')) {
    return 'json';
  }
  if (normalized.endsWith('.xlsx') || normalized.endsWith('.xls')) {
    return 'excel';
  }
  if (normalized.endsWith('.csv') || normalized.endsWith('.txt')) {
    return 'csv';
  }
  return '';
}

function getDefaultScheduleTargetFileNameForType(targetType) {
  const normalizedTargetType = String(targetType || '').trim().toUpperCase();
  if (normalizedTargetType === 'FILE_EXCEL') {
    return 'export_${date}_${time}.xlsx';
  }
  if (normalizedTargetType === 'FILE_JSON') {
    return 'export_${date}_${time}.json';
  }
  return 'export_${date}_${time}.csv';
}

function isDefaultScheduleTargetFileName(fileName) {
  const normalized = String(fileName || '').trim().toLowerCase();
  if (!normalized) {
    return false;
  }
  return [
    'export_${date}_${time}.csv',
    'export_${date}_${time}.xlsx',
    'export_${date}_${time}.json',
    'export_%date%_%time%.csv',
    'export_%date%_%time%.xlsx',
    'export_%date%_%time%.json'
  ].includes(normalized);
}

function normalizeRelativeDirectoryInput(value) {
  return String(value || '')
    .trim()
    .replace(/\\+/g, '/')
    .split('/')
    .map((segment) => String(segment || '').trim())
    .filter((segment) => segment && segment !== '.')
    .join('/');
}

function tryParseJsonObject(rawValue) {
  const trimmed = String(rawValue || '').trim();
  if (!trimmed) {
    return null;
  }

  try {
    const parsed = JSON.parse(trimmed);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function parseScheduleFileDefinition(rawDefinition) {
  const trimmed = String(rawDefinition || '').trim();
  if (!trimmed) {
    return {
      editorText: '',
      relativeDirectory: '',
      archiveRelativeDirectory: '',
      parsed: null
    };
  }

  const parsed = tryParseJsonObject(trimmed);
  if (!parsed) {
    return {
      editorText: trimmed,
      relativeDirectory: '',
      archiveRelativeDirectory: '',
      parsed: null
    };
  }

  const editable = { ...parsed };
  const relativeDirectory = normalizeRelativeDirectoryInput(editable.relativeDirectory || '');
  const archiveRelativeDirectory = normalizeRelativeDirectoryInput(editable.archiveRelativeDirectory || '');
  delete editable.relativeDirectory;
  delete editable.archiveRelativeDirectory;

  return {
    editorText: JSON.stringify(editable, null, 2),
    relativeDirectory,
    archiveRelativeDirectory,
    parsed
  };
}

function buildScheduleFileDefinitionValue(textareaId, relativeDirectoryId, archiveRelativeDirectoryId) {
  const rawValue = String(document.getElementById(textareaId)?.value || '').trim();
  const relativeDirectory = normalizeRelativeDirectoryInput(document.getElementById(relativeDirectoryId)?.value || '');
  const archiveRelativeDirectory = normalizeRelativeDirectoryInput(document.getElementById(archiveRelativeDirectoryId)?.value || '');
  if (!rawValue) {
    return undefined;
  }

  const parsed = tryParseJsonObject(rawValue);
  let definition = parsed ? { ...parsed } : null;

  if (!definition && (relativeDirectory || archiveRelativeDirectory)) {
    const looksLikePath = rawValue.includes('/') || rawValue.includes('\\');
    definition = looksLikePath ? { filePath: rawValue } : { fileName: rawValue };
    const detectedFormat = detectFileFormatFromName(rawValue);
    if (detectedFormat) {
      definition.format = detectedFormat;
    }
  }

  if (!definition) {
    return rawValue;
  }

  if (relativeDirectory) {
    definition.relativeDirectory = relativeDirectory;
  } else {
    delete definition.relativeDirectory;
  }
  if (archiveRelativeDirectory) {
    definition.archiveRelativeDirectory = archiveRelativeDirectory;
  } else {
    delete definition.archiveRelativeDirectory;
  }

  return JSON.stringify(definition, null, 2);
}

function joinAgentPath(basePath, relativePath) {
  const base = String(basePath || '').trim();
  const relative = normalizeRelativeDirectoryInput(relativePath || '');
  if (!base) {
    return relative;
  }
  if (!relative) {
    return base;
  }

  const separator = base.includes('\\') ? '\\' : '/';
  const normalizedBase = base.replace(/[\\/]+$/, '');
  const normalizedRelative = relative.replace(/\//g, separator);
  return normalizedBase + separator + normalizedRelative;
}

function resolveScheduleFilePathDetails(mode, connector, rawDefinition) {
  const filePaths = connector && connector.filePaths ? connector.filePaths : null;
  if (!filePaths) {
    return null;
  }

  const parsed = tryParseJsonObject(rawDefinition || '');
  const relativeDirectory = normalizeRelativeDirectoryInput(parsed?.relativeDirectory || '');
  const archiveRelativeDirectory = normalizeRelativeDirectoryInput(parsed?.archiveRelativeDirectory || relativeDirectory || '');
  const isRead = mode === 'read';
  const rootPath = isRead ? filePaths.importPath : filePaths.exportPath;
  const effectiveDirectory = joinAgentPath(rootPath, relativeDirectory);
  const archiveDirectory = joinAgentPath(filePaths.archivePath, archiveRelativeDirectory);
  const explicitPath = String(parsed?.filePath || '').trim();
  const fileName = String(parsed?.fileName || '').trim();
  let effectiveFilePath = '';

  if (explicitPath) {
    if (/^[a-zA-Z]:[\\/]/.test(explicitPath) || explicitPath.startsWith('\\\\') || explicitPath.startsWith('/')) {
      effectiveFilePath = explicitPath;
    } else {
      effectiveFilePath = joinAgentPath(filePaths.basePath, explicitPath);
    }
  } else if (fileName) {
    effectiveFilePath = joinAgentPath(effectiveDirectory, fileName);
  }

  return {
    rootPath,
    effectiveDirectory,
    archiveDirectory,
    effectiveFilePath
  };
}

function buildScheduleFilePathLines(schedule) {
  const connectorId = String(schedule?.connectorId || '').trim();
  const connector = (state.connectors || []).find((item) => String(item.id || '').trim() === connectorId);
  if (!connector || !connector.filePaths) {
    return [];
  }

  const lines = [];
  if (isFileScheduleSourceType(String(schedule?.sourceType || '').trim().toUpperCase())) {
    const sourceDetails = resolveScheduleFilePathDetails('read', connector, schedule?.sourceDefinition || '');
    if (sourceDetails) {
      lines.push('Quelle: ' + sourceDetails.effectiveDirectory);
      if (sourceDetails.effectiveFilePath) {
        lines.push('Quelldatei: ' + sourceDetails.effectiveFilePath);
      }
      lines.push('Archiv: ' + sourceDetails.archiveDirectory);
    }
  }
  if (isFileScheduleTargetType(String(schedule?.targetType || '').trim().toUpperCase())) {
    const targetDetails = resolveScheduleFilePathDetails('write', connector, schedule?.targetDefinition || '');
    if (targetDetails) {
      lines.push('Ziel: ' + targetDetails.effectiveDirectory);
      if (targetDetails.effectiveFilePath) {
        lines.push('Zieldatei: ' + targetDetails.effectiveFilePath);
      }
      lines.push('Archiv: ' + targetDetails.archiveDirectory);
    }
  }
  return lines;
}

function renderScheduleFilePathLines(lines) {
  const entries = Array.isArray(lines) ? lines.filter(Boolean) : [];
  if (!entries.length) {
    return '<span class="text-secondary">Keine Datei-Pfade aktiv.</span>';
  }
  return entries.map((line) => '<div>' + esc(line) + '</div>').join('');
}

function updateScheduleFilePathSummaries() {
  const connectorId = String(document.getElementById('sch-connector')?.value || '').trim();
  const connector = (state.connectors || []).find((item) => String(item.id || '').trim() === connectorId);
  const sourceSummary = document.getElementById('sch-source-path-summary');
  const targetSummary = document.getElementById('sch-target-path-summary');
  const sourceType = String(document.getElementById('sch-source-type')?.value || '').trim().toUpperCase();
  const targetType = String(document.getElementById('sch-target-type')?.value || '').trim().toUpperCase();

  document.getElementById('sch-target-relative-directory-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));
  document.getElementById('sch-target-archive-relative-directory-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));
  document.getElementById('sch-target-path-summary-wrap').classList.toggle('d-none', !isFileScheduleTargetType(targetType));
  updateScheduleTypeUi();

  if (sourceSummary) {
    if (!isFileScheduleSourceType(sourceType)) {
      sourceSummary.textContent = 'Keine Datei-Quelle aktiv.';
    } else if (!connector || !connector.filePaths) {
      sourceSummary.textContent = 'Für Datei-Pfade bitte einen File-Connector wählen.';
    } else {
      const details = resolveScheduleFilePathDetails('read', connector, buildScheduleSourceDefinitionValue() || '');
      const lines = details
        ? [
          'Importpfad: ' + details.effectiveDirectory,
          details.effectiveFilePath ? 'Quelldatei: ' + details.effectiveFilePath : '',
          'Archivpfad: ' + details.archiveDirectory
        ].filter(Boolean)
        : ['Datei-Definition ist noch nicht vollständig.'];
      sourceSummary.innerHTML = renderScheduleFilePathLines(lines);
    }
  }

  if (targetSummary) {
    if (!isFileScheduleTargetType(targetType)) {
      targetSummary.textContent = 'Kein Datei-Ziel aktiv.';
    } else if (!connector || !connector.filePaths) {
      targetSummary.textContent = 'Für Datei-Pfade bitte einen File-Connector wählen.';
    } else {
      const details = resolveScheduleFilePathDetails('write', connector, buildScheduleTargetDefinitionValue() || '');
      const lines = details
        ? [
          'Exportpfad: ' + details.effectiveDirectory,
          details.effectiveFilePath ? 'Zieldatei: ' + details.effectiveFilePath : '',
          'Archivpfad: ' + details.archiveDirectory
        ].filter(Boolean)
        : ['Datei-Definition ist noch nicht vollständig.'];
      targetSummary.innerHTML = renderScheduleFilePathLines(lines);
    }
  }
}

function parseScheduleSourceDefinition(sourceType, rawDefinition) {
  const trimmed = String(rawDefinition || '').trim();
  if (isFileScheduleSourceType(String(sourceType || '').trim().toUpperCase())) {
    const fileDefinition = parseScheduleFileDefinition(trimmed);
    return {
      queryText: fileDefinition.editorText,
      deltaStrategy: '',
      deltaField: '',
      afterExportText: '',
      relativeDirectory: fileDefinition.relativeDirectory,
      archiveRelativeDirectory: fileDefinition.archiveRelativeDirectory
    };
  }
  if ((sourceType !== 'MSSQL_SQL' && sourceType !== 'FILEMAKER_SQL' && sourceType !== 'SALESFORCE_SOQL') || !trimmed) {
    if (String(sourceType || '').trim().toUpperCase() === 'ENDPOINT' && !trimmed) {
      return {
        queryText: JSON.stringify({
          method: 'POST',
          path: '/records',
          contentType: 'application/json',
          recordMode: 'single',
          queryFields: [],
          headerFields: ['x-request-id'],
          response: { successStatus: 202, errorStatus: 422 },
          validation: { requiredBodyFields: [] }
        }, null, 2),
        deltaStrategy: '',
        deltaField: '',
        afterExportText: '',
        relativeDirectory: '',
        archiveRelativeDirectory: ''
      };
    }
    return { queryText: trimmed, deltaStrategy: '', deltaField: '', afterExportText: '', relativeDirectory: '', archiveRelativeDirectory: '' };
  }

  try {
    const parsed = JSON.parse(trimmed);
    const queryText = parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? String(parsed.queryText || parsed.soql || parsed.query || '').trim()
      : '';
    if (queryText) {
      const afterExportEntries = parsed.afterExport && typeof parsed.afterExport === 'object' && !Array.isArray(parsed.afterExport)
        ? Object.entries(parsed.afterExport).map(([key, value]) => String(key || '').trim() && String(value || '').trim() ? String(key).trim() + '=' + String(value).trim() : '').filter(Boolean)
        : [];
      return {
        queryText,
        deltaStrategy: String(parsed.delta && parsed.delta.strategy || '').trim(),
        deltaField: String(parsed.delta && parsed.delta.field || '').trim(),
        afterExportText: afterExportEntries.join(','),
        relativeDirectory: '',
        archiveRelativeDirectory: ''
      };
    }
  } catch {
    // Backward compatible: plain query text.
  }

  return { queryText: trimmed, deltaStrategy: '', deltaField: '', afterExportText: '', relativeDirectory: '', archiveRelativeDirectory: '' };
}

function parseAfterExportAssignments(rawValue) {
  return String(rawValue || '').split(',').map((entry) => entry.trim()).filter(Boolean).reduce((acc, entry) => {
    const separatorIndex = entry.indexOf('=');
    if (separatorIndex <= 0) {
      return acc;
    }
    const fieldName = entry.slice(0, separatorIndex).trim();
    const fieldValue = entry.slice(separatorIndex + 1).trim();
    if (fieldName && fieldValue) {
      acc[fieldName] = fieldValue;
    }
    return acc;
  }, {});
}

function buildScheduleSourceDefinitionValue() {
  const sourceType = document.getElementById('sch-source-type').value;
  if (isFileScheduleSourceType(String(sourceType || '').trim().toUpperCase())) {
    return buildScheduleFileDefinitionValue(
      'sch-source-definition',
      'sch-source-relative-directory',
      'sch-source-archive-relative-directory'
    );
  }

  const queryText = String(document.getElementById('sch-source-definition').value || '').trim();
  if (sourceType !== 'MSSQL_SQL' && sourceType !== 'FILEMAKER_SQL' && sourceType !== 'SALESFORCE_SOQL') {
    return queryText || undefined;
  }

  const deltaStrategy = String(document.getElementById('sch-source-delta-strategy').value || '').trim().toLowerCase();
  const deltaField = String(document.getElementById('sch-source-delta-field').value || '').trim();
  const afterExportUpdates = sourceType === 'SALESFORCE_SOQL'
    ? parseAfterExportAssignments(document.getElementById('sch-source-after-export').value)
    : {};
  if ((!deltaStrategy || !deltaField) && !Object.keys(afterExportUpdates).length) {
    return queryText || undefined;
  }

  const definition = {
    queryText
  };
  if (deltaStrategy && deltaField) {
    definition.delta = {
      strategy: deltaStrategy,
      field: deltaField
    };
  }
  if (Object.keys(afterExportUpdates).length) {
    definition.afterExport = afterExportUpdates;
  }

  return JSON.stringify(definition, null, 2);
}

function parseScheduleTargetDefinition(targetType, rawDefinition) {
  const trimmed = String(rawDefinition || '').trim();
  if (!isFileScheduleTargetType(String(targetType || '').trim().toUpperCase())) {
    return {
      editorText: trimmed,
      relativeDirectory: '',
      archiveRelativeDirectory: '',
      fileName: '',
      charset: 'utf8',
      delimiter: ';',
      textQualifier: '"',
      sheetName: ''
    };
  }

  const fileDefinition = parseScheduleFileDefinition(trimmed);
  const parsed = fileDefinition.parsed || {};
  const normalizedTargetType = String(targetType || '').trim().toUpperCase();
  const defaultFileName = normalizedTargetType === 'FILE_EXCEL'
    ? 'export_${date}_${time}.xlsx'
    : normalizedTargetType === 'FILE_JSON'
      ? 'export_${date}_${time}.json'
      : 'export_${date}_${time}.csv';
  return {
    editorText: fileDefinition.editorText,
    relativeDirectory: fileDefinition.relativeDirectory,
    archiveRelativeDirectory: fileDefinition.archiveRelativeDirectory,
    fileName: String(parsed.fileName || '').trim() || defaultFileName,
    charset: String(parsed.charset || 'utf8').trim() || 'utf8',
    delimiter: String(parsed.delimiter || ';').trim() || ';',
    textQualifier: String(parsed.textQualifier || '"').trim() || '"',
    sheetName: String(parsed.sheetName || '').trim() || (normalizedTargetType === 'FILE_EXCEL' ? 'Sheet1' : '')
  };
}

function buildScheduleTargetDefinitionValue() {
  const targetType = document.getElementById('sch-target-type').value;
  if (isFileScheduleTargetType(String(targetType || '').trim().toUpperCase())) {
    const rawValue = String(document.getElementById('sch-target-definition')?.value || '').trim();
    const parsed = tryParseJsonObject(rawValue);
    const definition = parsed ? { ...parsed } : {};
    const normalizedTargetType = String(targetType || '').trim().toUpperCase();
    const relativeDirectory = normalizeRelativeDirectoryInput(document.getElementById('sch-target-relative-directory')?.value || '');
    const archiveRelativeDirectory = normalizeRelativeDirectoryInput(document.getElementById('sch-target-archive-relative-directory')?.value || '');
    const fileName = String(document.getElementById('sch-target-file-name')?.value || '').trim();
    const charset = String(document.getElementById('sch-target-file-charset')?.value || '').trim();
    const delimiter = String(document.getElementById('sch-target-file-delimiter')?.value || '').trim();
    const textQualifier = String(document.getElementById('sch-target-file-text-qualifier')?.value || '').trim();
    const sheetName = String(document.getElementById('sch-target-file-sheet-name')?.value || '').trim();

    if (relativeDirectory) {
      definition.relativeDirectory = relativeDirectory;
    } else {
      delete definition.relativeDirectory;
    }
    if (archiveRelativeDirectory) {
      definition.archiveRelativeDirectory = archiveRelativeDirectory;
    } else {
      delete definition.archiveRelativeDirectory;
    }

    if (fileName) {
      definition.fileName = fileName;
    }
    if (charset) {
      definition.charset = charset;
    }

    if (normalizedTargetType === 'FILE_EXCEL') {
      definition.format = 'excel';
      if (sheetName) {
        definition.sheetName = sheetName;
      } else {
        delete definition.sheetName;
      }
      delete definition.delimiter;
      delete definition.textQualifier;
    } else if (normalizedTargetType === 'FILE_JSON') {
      definition.format = 'json';
      delete definition.sheetName;
      delete definition.delimiter;
      delete definition.textQualifier;
    } else {
      definition.format = 'csv';
      if (delimiter) {
        definition.delimiter = delimiter;
      } else {
        delete definition.delimiter;
      }
      if (textQualifier) {
        definition.textQualifier = textQualifier;
      } else {
        delete definition.textQualifier;
      }
      delete definition.sheetName;
    }

    definition.writeHeader = true;

    if (!String(definition.fileName || definition.filePath || '').trim()) {
      return undefined;
    }

    return JSON.stringify(definition, null, 2);
  }

  return String(document.getElementById('sch-target-definition').value || '').trim() || undefined;
}

async function loadScheduleCheckpoint(scheduleId) {
  if (!scheduleId) {
    document.getElementById('sch-source-delta-current').value = '';
    document.getElementById('sch-source-delta-record-id').value = '';
    return;
  }

  try {
    const checkpoint = await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/checkpoint');
    document.getElementById('sch-source-delta-current').value = String(checkpoint?.lastCheckpoint || '');
    document.getElementById('sch-source-delta-record-id').value = String(checkpoint?.lastRecordId || '');
  } catch {
    document.getElementById('sch-source-delta-current').value = '';
    document.getElementById('sch-source-delta-record-id').value = '';
  }
}

async function saveScheduleCheckpoint(scheduleId) {
  const deltaStrategy = String(document.getElementById('sch-source-delta-strategy').value || '').trim().toLowerCase();
  const deltaField = String(document.getElementById('sch-source-delta-field').value || '').trim();
  const lastCheckpoint = String(document.getElementById('sch-source-delta-current').value || '').trim();
  const lastRecordId = String(document.getElementById('sch-source-delta-record-id').value || '').trim();
  if (!scheduleId || !deltaStrategy || !deltaField || (!lastCheckpoint && !lastRecordId)) {
    return;
  }

  await requestJson('/api/schedules/' + encodeURIComponent(scheduleId) + '/checkpoint', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      lastCheckpoint: lastCheckpoint || undefined,
      lastRecordId: lastRecordId || undefined
    })
  });
}

async function safeRequest(path, fallback) {
  try {
    return await requestJson(path);
  } catch (error) {
    showError(error.message || 'API-Fehler');
    return fallback;
  }
}

function currentUserHasPermission(permission) {
  const permissions = (state.adminMe && state.adminMe.user && state.adminMe.user.permissions) || [];
  return permissions.includes('admin') || permissions.includes(permission);
}

function currentUserHasModule(moduleName) {
  const user = state.adminMe && state.adminMe.user;
  const permissions = (user && user.permissions) || [];
  const modules = (user && user.modules) || [];
  return permissions.includes('admin') || modules.includes(moduleName);
}

function applyAdminAccessUi() {
  const canUseMigration = currentUserHasModule('migration');
  const canUseProjectAdmin = currentUserHasPermission('admin') || currentUserHasModule('projects') || currentUserHasModule('deployment');
  document.querySelectorAll('[data-bs-target="#tab-migration"], [data-menu-tab="#tab-migration"]').forEach((el) => {
    el.classList.toggle('d-none', !canUseMigration);
  });
  document.querySelectorAll('[data-bs-target="#tab-projects"], [data-menu-tab="#tab-projects"]').forEach((el) => {
    el.classList.toggle('d-none', !canUseProjectAdmin);
  });
  document.querySelectorAll('[data-bs-target="#tab-instances"], [data-menu-tab="#tab-instances"]').forEach((el) => {
    el.classList.toggle('d-none', !canUseProjectAdmin);
  });
  const canAdmin = currentUserHasPermission('admin');
  document.querySelectorAll('#open-admin-modal-sidebar, #open-admin-modal-menu').forEach((el) => {
    el.classList.toggle('d-none', !canUseProjectAdmin);
  });
  document.querySelectorAll('#admin-open-users, #admin-tab-users-trigger, #admin-open-history, #admin-tab-history-trigger').forEach((el) => {
    el.classList.toggle('d-none', !canAdmin);
  });
  document.querySelectorAll('#admin-manage-projects, #admin-open-deployment, #admin-open-documentation, #add-instance, #manage-projects, #publish-project-documentation').forEach((el) => {
    el.classList.toggle('d-none', !canUseProjectAdmin);
  });
}

function resetAdminUserForm() {
  document.getElementById('admin-user-id').value = '';
  document.getElementById('admin-user-username').value = '';
  document.getElementById('admin-user-display-name').value = '';
  document.getElementById('admin-user-password').value = '';
  document.querySelectorAll('[data-admin-permission]').forEach((el) => { el.checked = el.getAttribute('data-admin-permission') === 'read'; });
  document.querySelectorAll('[data-admin-module]').forEach((el) => { el.checked = false; });
}

function editAdminUser(user) {
  document.getElementById('admin-user-id').value = user.id || '';
  document.getElementById('admin-user-username').value = user.username || '';
  document.getElementById('admin-user-display-name').value = user.displayName || '';
  document.getElementById('admin-user-password').value = '';
  const permissions = Array.isArray(user.permissions) ? user.permissions : [];
  const modules = Array.isArray(user.modules) ? user.modules : [];
  document.querySelectorAll('[data-admin-permission]').forEach((el) => {
    el.checked = permissions.includes(el.getAttribute('data-admin-permission'));
  });
  document.querySelectorAll('[data-admin-module]').forEach((el) => {
    el.checked = modules.includes(el.getAttribute('data-admin-module'));
  });
}
