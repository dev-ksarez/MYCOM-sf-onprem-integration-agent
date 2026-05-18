import { AISchedulerService, AIGenerationResult, AISchedulerRequest } from "./ai-scheduler-service";
import { AdminDataService } from "./admin-data-service";

/**
 * UI-Modul für KI-Scheduler-Assistent
 * Rendert das Interface für Benutzer-Prompts
 */

export function renderAISchedulerAssistantModule(): string {
  return `
    <div id="ai-scheduler-container" class="container-fluid py-4">
      <div class="row">
        <div class="col-12">
          <div class="card border-0 shadow-sm bg-gradient-primary text-white mb-4">
            <div class="card-body py-4">
              <div class="d-flex align-items-center gap-3">
                <div class="ai-icon" style="font-size: 2.5rem; opacity: 0.9;">⚡</div>
                <div>
                  <h4 class="card-title mb-1">KI-Scheduler-Assistent</h4>
                  <p class="card-text mb-0 small">Beschreiben Sie Ihre Integrationsanforderung in natürlicher Sprache - der Assistent konfiguriert automatisch Connectoren, Mappings und Timing.</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="row">
        <div class="col-lg-8">
          <div class="card border-0 shadow-sm mb-4">
            <div class="card-header bg-white border-bottom">
              <h5 class="card-title mb-0">Anforderung eingeben</h5>
            </div>
            <div class="card-body">
              <div class="mb-3">
                <label for="ai-prompt-input" class="form-label fw-semibold">Was möchten Sie synchronisieren?</label>
                <textarea
                  id="ai-prompt-input"
                  class="form-control ai-prompt-textarea"
                  rows="4"
                  placeholder="Beispiel: Alle Kontakte aus unserer MSSQL-Datenbank nach Salesforce synchronisieren, täglich um 22 Uhr. Email als eindeutige ID verwenden."
                  data-ai-input="prompt"
                ></textarea>
                <div class="form-text mt-2">
                  💡 <strong>Tipps:</strong> Je detaillierter Ihre Beschreibung, desto bessere Ergebnisse.
                  Nennen Sie:
                  <ul class="mb-0 mt-2">
                    <li>Quell- und Zielsystem</li>
                    <li>Welche Objekte/Tabellen</li>
                    <li>Gewünschte Häufigkeit</li>
                    <li>Spezielle Anforderungen (z.B. eindeutige IDs)</li>
                  </ul>
                </div>
              </div>

              <div class="row g-3 mb-3">
                <div class="col-md-6">
                  <label for="ai-connector-select" class="form-label fw-semibold">Connector (optional)</label>
                  <select id="ai-connector-select" class="form-select" data-ai-input="connector">
                    <option value="">Auto-Erkennung</option>
                  </select>
                  <div class="form-text">Wenn leer, wird der beste Connector automatisch ausgewählt.</div>
                </div>

                <div class="col-md-6">
                  <label for="ai-target-select" class="form-label fw-semibold">Ziel-System (optional)</label>
                  <select id="ai-target-select" class="form-select" data-ai-input="targetSystem">
                    <option value="">Auto-Erkennung</option>
                    <option value="Salesforce">Salesforce</option>
                    <option value="MSSQL">MSSQL</option>
                    <option value="REST_API">REST API</option>
                    <option value="FILE">Dateiensystem</option>
                  </select>
                </div>
              </div>

              <div class="d-grid gap-2 d-md-flex justify-content-md-between">
                <button
                  id="ai-generate-btn"
                  class="btn btn-primary btn-lg"
                  type="button"
                  data-ai-action="generate"
                >
                  <span class="spinner-border spinner-border-sm me-2 d-none" id="ai-loading-spinner"></span>
                  <span id="ai-btn-text">⚡ Konfiguration generieren</span>
                </button>
                <button
                  id="ai-clear-btn"
                  class="btn btn-outline-secondary"
                  type="button"
                  data-ai-action="clear"
                >
                  Zurücksetzen
                </button>
              </div>
            </div>
          </div>

          <div id="ai-result-container" class="d-none">
            <div class="card border-0 shadow-sm mb-4">
              <div class="card-header bg-white border-bottom d-flex justify-content-between align-items-center">
                <h5 class="card-title mb-0">Generierte Konfiguration</h5>
                <div id="ai-confidence-badge" class="badge" style="font-size: 1rem;"></div>
              </div>
              <div class="card-body">
                <!-- Reasoning -->
                <div class="alert alert-info mb-3" id="ai-reasoning-alert"></div>
                <div class="small text-secondary mb-3" id="ai-metadata-basis"></div>

                <!-- Issues -->
                <div id="ai-issues-container"></div>

                <!-- Existing config diff -->
                <div id="ai-diff-container" class="mb-3 d-none">
                  <h6 class="fw-semibold mb-2">Änderungsvorschlag (Diff)</h6>
                  <div class="table-responsive ai-diff-preview rounded border" id="ai-diff-preview"></div>
                </div>

                <!-- Preview -->
                <div class="mb-3">
                  <h6 class="fw-semibold mb-2">Scheduler-Vorschau</h6>
                  <div class="ai-config-preview p-3 bg-light rounded border" id="ai-config-preview"></div>
                </div>

                <!-- Source Preview -->
                <div class="mb-3">
                  <h6 class="fw-semibold mb-2">Quelle (SQL/Definition)</h6>
                  <div class="ai-source-preview p-3 bg-light rounded border" id="ai-source-preview"></div>
                  <div id="ai-sql-traffic-light" class="small mt-2 d-none"></div>
                  <div class="mt-2 d-flex gap-2 flex-wrap">
                    <button id="ai-sql-autofix-btn" type="button" class="btn btn-sm btn-outline-warning d-none">SQL Auto-Fix anwenden</button>
                    <button id="ai-apply-delta-btn" type="button" class="btn btn-sm btn-outline-info d-none">Delta-Vorschlag übernehmen</button>
                  </div>
                </div>

                <!-- Mapping Preview -->
                <div class="mb-3">
                  <h6 class="fw-semibold mb-2">Feld-Zuordnung (Mapping)</h6>
                  <div class="ai-mapping-preview p-3 bg-light rounded border" id="ai-mapping-preview"></div>
                </div>

                <div id="ai-test-status" class="d-none"></div>

                <!-- Buttons -->
                <div class="d-grid gap-2 d-md-flex">
                  <button
                    id="ai-save-btn"
                    class="btn btn-success btn-lg"
                    type="button"
                    data-ai-action="save"
                  >
                    ✓ Speichern und verwenden
                  </button>
                  <button
                    id="ai-refine-btn"
                    class="btn btn-outline-primary"
                    type="button"
                    data-ai-action="refine"
                  >
                    🔧 Abfrage testen + Mapping aufbauen
                  </button>
                  <button
                    id="ai-cancel-btn"
                    class="btn btn-outline-secondary"
                    type="button"
                    data-ai-action="cancel"
                  >
                    Abbrechen
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Sidebar: Tipps & Beispiele -->
        <div class="col-lg-4">
          <div class="card border-0 shadow-sm mb-4 bg-light">
            <div class="card-header bg-white border-bottom">
              <h5 class="card-title mb-0">📚 Beispiele</h5>
            </div>
            <div class="card-body">
              <div class="mb-3">
                <p class="small fw-semibold text-muted mb-1">Inbound (externe Daten → Salesforce):</p>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start ai-example-btn" data-example="inbound-1">
                  <small>MSSQL Accounts → SF</small>
                </button>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start mt-1 ai-example-btn" data-example="inbound-2">
                  <small>REST-API Kontakte → SF</small>
                </button>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start mt-1 ai-example-btn" data-example="sage-products">
                  <small>SAGE100 Produkte → SF</small>
                </button>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start mt-1 ai-example-btn" data-example="sage-customers">
                  <small>SAGE100 Kunden → SF</small>
                </button>
              </div>

              <div class="mb-3">
                <p class="small fw-semibold text-muted mb-1">Outbound (Salesforce → extern):</p>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start ai-example-btn" data-example="outbound-1">
                  <small>SF Contacts → Newsletter</small>
                </button>
                <button class="btn btn-sm btn-outline-secondary w-100 text-start mt-1 ai-example-btn" data-example="outbound-2">
                  <small>SF Opportunities → ERP</small>
                </button>
              </div>

              <hr />

              <p class="small mb-2"><strong>✨ Was der Assistent macht:</strong></p>
              <ul class="small list-unstyled">
                <li>✓ Analysiert Ihre Anforderung</li>
                <li>✓ Wählt passenden Connector</li>
                <li>✓ Erstellt Feld-Zuordnung</li>
                <li>✓ Konfiguriert Timing</li>
                <li>✓ Prüft auf Fehler</li>
              </ul>
            </div>
          </div>

          <div class="card border-0 shadow-sm bg-info-subtle">
            <div class="card-body">
              <p class="small mb-2"><strong>💡 Noch Fragen?</strong></p>
              <p class="small text-muted mb-0">
                Der Assistent ist nicht perfekt - überprüfen Sie die generierte Konfiguration 
                immer vor dem Speichern. Bei komplexen Anforderungen können Sie manuell anpassen.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>

    <style>
      .bg-gradient-primary {
        background: linear-gradient(135deg, #0f6ab4 0%, #2980c2 100%);
      }

      .ai-prompt-textarea {
        min-height: 120px;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        resize: vertical;
      }

      .ai-config-preview {
        font-family: monospace;
        font-size: 0.85rem;
        max-height: 250px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
      }

      .ai-mapping-preview {
        font-family: monospace;
        font-size: 0.85rem;
        max-height: 200px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-all;
      }

      .ai-source-preview {
        font-family: monospace;
        font-size: 0.85rem;
        max-height: 220px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
      }

      .ai-diff-preview {
        font-size: 0.85rem;
        max-height: 260px;
        overflow-y: auto;
      }

      .ai-diff-preview table {
        margin-bottom: 0;
      }

      .ai-diff-preview code {
        white-space: pre-wrap;
        word-break: break-word;
      }

      .ai-diff-added {
        background: #e8f7ee !important;
      }

      .ai-diff-before {
        background: #fff3cd !important;
      }

      .ai-sql-added-column {
        background: #bbf7d0;
        border: 1px solid #22c55e;
        border-radius: 0.25rem;
        padding: 0 0.15rem;
        font-weight: 700;
      }

      #ai-sql-traffic-light .badge {
        min-width: 110px;
      }

      .ai-example-btn {
        text-align: left !important;
        white-space: normal;
        padding: 0.5rem 0.75rem !important;
      }

      #ai-confidence-badge {
        min-width: 80px;
        text-align: center;
        font-weight: bold;
      }

      .ai-icon {
        animation: pulse 2s infinite;
      }

      @keyframes pulse {
        0%, 100% { opacity: 0.8; }
        50% { opacity: 1; }
      }
    </style>

    <script>
      // Globaler State für AI-Generierung
      window.aiSchedulerState = {
        currentResult: null,
        existingConnectors: [],
        isLoading: false,
        currentSqlAssessment: null,
        currentDeltaSuggestion: null
      };

      function aiWithInstance(path) {
        const url = new URL(path, window.location.origin);
        const instanceSelect = document.getElementById('instance-select');
        const instanceId = String(instanceSelect?.value || '').trim();
        if (instanceId) {
          url.searchParams.set('instanceId', instanceId);
        }
        return url.pathname + url.search;
      }

      async function aiRequestJson(path, options) {
        const requestOptions = options && typeof options === 'object' ? options : {};
        const response = await fetch(aiWithInstance(path), requestOptions);
        if (response.status === 401) {
          window.location.href = '/';
          throw new Error('Sitzung abgelaufen');
        }

        let data;
        try {
          data = await response.json();
        } catch {
          data = { error: 'Ungueltige Antwort vom Server' };
        }

        if (!response.ok) {
          throw new Error(data.error || data.message || 'Request failed');
        }

        return data;
      }

      // Beispiel-Prompts
      const examplePrompts = {
        "inbound-1": "Alle Accounts aus unserer MSSQL-Datenbank nach Salesforce synchronisieren. Verwende diese SQL: SELECT Kundennummer AS ExternalKey, Name1 AS Name, Telefon AS Phone, Webseite AS Website FROM KHKAdressen WHERE Aktiv = 1. Täglich um 08:00 Uhr.",
        "inbound-2": "REST-API Kundenaktualisierungen nach Salesforce Contact Object. Stündliche Synchronisation.",
        "sage-products": "Produkte aus SAGE100 nach Salesforce Product2 synchronisieren. Operation: Upsert. Zeitsteuerung: jeden Tag alle 5 Minuten.",
        "sage-customers": "Kunden aus SAGE100 nach Salesforce Account synchronisieren. Operation: Upsert. Zeitsteuerung: werktags alle 15 Minuten ab 08:00.",
        "outbound-1": "Alle aktiven Salesforce Contacts mit Newsletter-Opt-in nach Brevo/Newsletter-System exportieren. Täglicher Export um 22 Uhr.",
        "outbound-2": "Abgeschlossene Opportunities aus Salesforce als Orders in unser SAGE100-System synchronisieren. Jeden Tag alle 5 Minuten."
      };

      function initializeAIAssistant(connectors) {
        window.aiSchedulerState.existingConnectors = connectors || [];

        // Connector-Dropdown füllen
        const connectorSelect = document.getElementById('ai-connector-select');
        if (connectorSelect && connectors) {
          connectorSelect.innerHTML += connectors
            .map(c => '<option value="' + esc(c.id) + '">' + esc(c.name) + ' (' + esc(c.connectorType) + ')</option>')
            .join('');
        }

        // Event-Listener
        document.getElementById('ai-generate-btn')?.addEventListener('click', generateScheduler);
        document.getElementById('ai-clear-btn')?.addEventListener('click', clearForm);
        document.getElementById('ai-save-btn')?.addEventListener('click', saveScheduler);
        document.getElementById('ai-cancel-btn')?.addEventListener('click', hideResult);
        document.getElementById('ai-refine-btn')?.addEventListener('click', refineScheduler);
        document.getElementById('ai-sql-autofix-btn')?.addEventListener('click', autoFixSqlQuery);
        document.getElementById('ai-apply-delta-btn')?.addEventListener('click', applyDeltaSuggestion);

        // Beispiel-Buttons
        document.querySelectorAll('.ai-example-btn').forEach(btn => {
          btn.addEventListener('click', () => {
            const example = examplePrompts[btn.getAttribute('data-example')];
            if (example) {
              document.getElementById('ai-prompt-input').value = example;
              generateScheduler();
            }
          });
        });

        // Enter zum Generieren
        document.getElementById('ai-prompt-input')?.addEventListener('keydown', (e) => {
          if (e.ctrlKey && e.key === 'Enter') {
            generateScheduler();
          }
        });

      }

      async function bootAIAssistant() {
        if (window.aiSchedulerState._initialized) {
          return;
        }
        window.aiSchedulerState._initialized = true;

        let connectors = [];
        try {
          const connectorResponse = await aiRequestJson('/api/connectors');
          connectors = Array.isArray(connectorResponse?.items) ? connectorResponse.items : [];
        } catch (error) {
          console.warn('KI-Assistent: Connector-Liste konnte nicht geladen werden', error);
        }

        initializeAIAssistant(connectors);
      }

      async function generateScheduler() {
        const prompt = String(document.getElementById('ai-prompt-input')?.value || '').trim();
        if (!prompt) {
          alert('Bitte geben Sie Ihre Anforderung ein.');
          return;
        }

        const connectorId = document.getElementById('ai-connector-select')?.value || undefined;
        const targetSystem = document.getElementById('ai-target-select')?.value || undefined;

        showLoading(true);
        try {
          const response = await aiRequestJson('/api/ai/generate-scheduler', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              userPrompt: prompt,
              connectorId,
              targetSystem,
              existingConnectors: window.aiSchedulerState.existingConnectors
            })
          });

          window.aiSchedulerState.currentResult = response;
          displayResult(response);
        } catch (error) {
          alert('Fehler bei der Generierung: ' + (error.message || 'Unbekannter Fehler'));
          console.error(error);
        } finally {
          showLoading(false);
        }
      }

      function displayResult(result) {
        const container = document.getElementById('ai-result-container');
        const reasoningAlert = document.getElementById('ai-reasoning-alert');
        const metadataBasisEl = document.getElementById('ai-metadata-basis');
        const issuesContainer = document.getElementById('ai-issues-container');
        const diffContainer = document.getElementById('ai-diff-container');
        const diffPreview = document.getElementById('ai-diff-preview');
        const configPreview = document.getElementById('ai-config-preview');
        const sourcePreview = document.getElementById('ai-source-preview');
        const sqlTrafficLight = document.getElementById('ai-sql-traffic-light');
        const sqlAutofixButton = document.getElementById('ai-sql-autofix-btn');
        const applyDeltaButton = document.getElementById('ai-apply-delta-btn');
        const mappingPreview = document.getElementById('ai-mapping-preview');
        const confidenceBadge = document.getElementById('ai-confidence-badge');

        const confidence = (result.confidence * 100).toFixed(0);
        const confidenceClass = result.confidence > 0.75 ? 'bg-success' : result.confidence > 0.5 ? 'bg-warning' : 'bg-danger';
        confidenceBadge.className = 'badge ' + confidenceClass;
        confidenceBadge.textContent = confidence + '%';

        reasoningAlert.innerHTML = '✓ ' + esc(result.reasoning);
        if (metadataBasisEl) {
          metadataBasisEl.innerHTML = formatMetadataBasis(result.metadataBasis);
        }

        if (result.issues && result.issues.length > 0) {
          issuesContainer.innerHTML = result.issues
            .map(issue => '<div class="alert alert-' + (issue.severity === 'error' ? 'danger' : 'warning') + ' py-2 mb-2">' +
              (issue.severity === 'error' ? '❌' : '⚠️') + ' ' + esc(issue.message) + '</div>')
            .join('');
        } else {
          issuesContainer.innerHTML = '';
        }

        if (diffContainer && diffPreview) {
          const diffHtml = formatConfigDiff(result.configDiff);
          if (diffHtml) {
            diffPreview.innerHTML = diffHtml;
            diffContainer.classList.remove('d-none');
          } else {
            diffPreview.innerHTML = '';
            diffContainer.classList.add('d-none');
          }
        }

        const schedule = result.schedule;
        const addedSqlColumns = collectAddedSourceColumns(result.configDiff);
        configPreview.textContent =
          'Name: ' + esc(schedule.name) + '\\n' +
          (result.mode === 'update' && result.existingSchedule ? 'Modus: Bestehenden Scheduler aktualisieren (' + esc(result.existingSchedule.id) + ')\\n' : '') +
          'Quelle: ' + esc(schedule.sourceSystem) + ' (' + esc(schedule.sourceType) + ')\\n' +
          'Ziel: ' + esc(schedule.targetSystem) + ' (' + esc(schedule.targetType) + ')\\n' +
          'Object: ' + esc(schedule.objectName) + ' / ' + esc(schedule.operation) + '\\n' +
          'Richtung: ' + esc(schedule.direction) + '\\n' +
          'Batch-Size: ' + (schedule.batchSize || 100) + '\\n' +
          'Timing: ' + esc(schedule.timingDefinition || '-');

        const rawSourceDefinition = String(schedule.sourceDefinition || '').trim();
        let sourceQueryText = '';
        if (!rawSourceDefinition) {
          sourcePreview.textContent = 'Keine Source-Definition vorhanden';
        } else {
          try {
            const parsedSource = JSON.parse(rawSourceDefinition);
            if (parsedSource && typeof parsedSource === 'object' && typeof parsedSource.queryText === 'string' && parsedSource.queryText.trim()) {
              sourceQueryText = String(parsedSource.queryText).trim();
              sourcePreview.innerHTML = highlightSqlAddedColumns(sourceQueryText, addedSqlColumns);
            } else {
              sourcePreview.textContent = 'Keine SQL-Abfrage in der Source-Definition vorhanden';
            }
          } catch {
            sourcePreview.innerHTML = highlightSqlAddedColumns(rawSourceDefinition, addedSqlColumns);
            sourceQueryText = rawSourceDefinition;
          }
        }

        const sqlStatus = evaluateSqlTrafficLight(schedule.sourceType, sourceQueryText);
        if (!sqlStatus || !sqlTrafficLight) {
          if (sqlTrafficLight) {
            sqlTrafficLight.className = 'small mt-2 d-none';
            sqlTrafficLight.innerHTML = '';
          }
          if (sqlAutofixButton) {
            sqlAutofixButton.classList.add('d-none');
            sqlAutofixButton.disabled = true;
          }
          window.aiSchedulerState.currentSqlAssessment = null;
        } else {
          const badgeClass = sqlStatus.level === 'green'
            ? 'bg-success'
            : sqlStatus.level === 'yellow'
              ? 'bg-warning text-dark'
              : 'bg-danger';
          sqlTrafficLight.className = 'small mt-2';
          sqlTrafficLight.innerHTML = '<span class="badge ' + badgeClass + '">SQL Ampel: ' + esc(sqlStatus.label) + '</span>'
            + '<span class="ms-2 text-secondary">' + esc(sqlStatus.message) + '</span>';

          window.aiSchedulerState.currentSqlAssessment = {
            ...sqlStatus,
            sourceQueryText,
            sourceType: schedule.sourceType
          };

          if (sqlAutofixButton) {
            const canAutofix = sqlStatus.canAutoFix === true;
            sqlAutofixButton.classList.toggle('d-none', !canAutofix);
            sqlAutofixButton.disabled = !canAutofix;
          }
        }

        const deltaSuggestion = window.aiSchedulerState.currentDeltaSuggestion;
        if (applyDeltaButton) {
          const canApplyDelta = !!deltaSuggestion && !hasDeltaConfig(schedule.sourceDefinition);
          applyDeltaButton.classList.toggle('d-none', !canApplyDelta);
          applyDeltaButton.disabled = !canApplyDelta;
          if (canApplyDelta) {
            applyDeltaButton.textContent = 'Delta übernehmen: ' + deltaSuggestion.field + ' (' + deltaSuggestion.strategy + ')';
          } else {
            applyDeltaButton.textContent = 'Delta-Vorschlag übernehmen';
          }
        }

        mappingPreview.textContent = esc(schedule.mappingDefinition || 'Keine Zuordnung definiert');
        container.classList.remove('d-none');
      }

      function formatConfigDiff(configDiff) {
        const entries = Array.isArray(configDiff) ? configDiff : [];
        if (!entries.length) {
          return '';
        }

        const rows = [];
        entries.forEach((entry) => {
          const area = String(entry.area || 'general').toUpperCase();
          const label = String(entry.label || 'Aenderung');
          const added = Array.isArray(entry.added) ? entry.added.filter(Boolean) : [];
          added.forEach((item) => {
            const detail = entry.area === 'source'
              ? '<code class="ai-sql-added-column">' + esc(item) + '</code>'
              : '<code>' + esc(item) + '</code>';
            rows.push(
              '<tr class="ai-diff-added">' +
                '<td><span class="badge bg-success">Neu</span></td>' +
                '<td>' + esc(area) + '</td>' +
                '<td>' + esc(label) + '</td>' +
                '<td>' + detail + '</td>' +
              '</tr>'
            );
          });
          if (entry.before || entry.after) {
            rows.push(
              '<tr class="ai-diff-before">' +
                '<td><span class="badge bg-warning text-dark">Vorher</span></td>' +
                '<td>' + esc(area) + '</td>' +
                '<td>' + esc(label) + '</td>' +
                '<td><details><summary>Vorher anzeigen</summary><code>' + esc(String(entry.before || '-')) + '</code></details></td>' +
              '</tr>'
            );
            rows.push(
              '<tr class="ai-diff-added">' +
                '<td><span class="badge bg-success">Nachher</span></td>' +
                '<td>' + esc(area) + '</td>' +
                '<td>' + esc(label) + '</td>' +
                '<td><details><summary>Nachher anzeigen</summary><code>' + esc(String(entry.after || '-')) + '</code></details></td>' +
              '</tr>'
            );
          }
          const warnings = Array.isArray(entry.warnings) ? entry.warnings.filter(Boolean) : [];
          warnings.forEach((item) => {
            rows.push(
              '<tr>' +
                '<td><span class="badge bg-warning text-dark">Hinweis</span></td>' +
                '<td>' + esc(area) + '</td>' +
                '<td>' + esc(label) + '</td>' +
                '<td>' + esc(item) + '</td>' +
              '</tr>'
            );
          });
        });

        if (!rows.length) {
          return '';
        }

        return '<table class="table table-sm table-bordered align-middle">' +
          '<thead class="table-light"><tr><th>Status</th><th>Bereich</th><th>Änderung</th><th>Details</th></tr></thead>' +
          '<tbody>' + rows.join('') + '</tbody>' +
        '</table>';
      }

      function collectAddedSourceColumns(configDiff) {
        const entries = Array.isArray(configDiff) ? configDiff : [];
        return entries
          .filter((entry) => String(entry?.area || '').toLowerCase() === 'source')
          .flatMap((entry) => Array.isArray(entry.added) ? entry.added : [])
          .map((item) => String(item || '').trim())
          .filter(Boolean);
      }

      function highlightSqlAddedColumns(sqlText, addedColumns) {
        let html = esc(sqlText || '');
        const columns = Array.isArray(addedColumns) ? addedColumns : [];
        columns
          .filter(Boolean)
          .sort((a, b) => String(b).length - String(a).length)
          .forEach((column) => {
            const escapedColumn = escapeRegExp(String(column));
            const pattern = new RegExp('(^|[^A-Za-z0-9_])(' + escapedColumn + ')(?=[^A-Za-z0-9_]|$)', 'g');
            html = html.replace(pattern, '$1<span class="ai-sql-added-column">$2</span>');
          });
        return html;
      }

      function escapeRegExp(value) {
        const specialChars = '\\\\.^$*+?()[]{}|';
        return String(value || '')
          .split('')
          .map((char) => specialChars.includes(char) ? '\\\\' + char : char)
          .join('');
      }

      function hasDeltaConfig(sourceDefinition) {
        const raw = String(sourceDefinition || '').trim();
        if (!raw) {
          return false;
        }
        try {
          const parsed = JSON.parse(raw);
          return !!(parsed && typeof parsed === 'object' && parsed.delta && typeof parsed.delta === 'object' && parsed.delta.field);
        } catch {
          return false;
        }
      }

      function formatMetadataBasis(metadataBasis) {
        const basis = metadataBasis && typeof metadataBasis === 'object' ? metadataBasis : {};
        const salesforce = basis.salesforce && typeof basis.salesforce === 'object' ? basis.salesforce : null;
        const sage100 = basis.sage100 && typeof basis.sage100 === 'object' ? basis.sage100 : null;
        const parts = [];

        if (salesforce && salesforce.status === 'success') {
          parts.push(
            'Salesforce-Metadaten: ' +
            esc(formatDateTime(salesforce.refreshedAt) || salesforce.refreshedAt || '-') +
            ' · ' + esc(String(salesforce.objectCount || 0)) + ' Objekte' +
            ' · ' + esc(String(salesforce.fieldCount || 0)) + ' Felder'
          );
        } else {
          parts.push('Salesforce-Metadaten: nicht geladen');
        }

        if (sage100) {
          const tables = Array.isArray(sage100.matchedTables) ? sage100.matchedTables.filter(Boolean) : [];
          parts.push(
            'SAGE100-Doku: ' +
            esc(formatDateTime(sage100.generatedAt) || sage100.generatedAt || '-') +
            ' · ' + esc(String(sage100.tableCount || 0)) + ' Tabellen' +
            (tables.length ? ' · Treffer: ' + esc(tables.slice(0, 4).join(', ')) : '')
          );
        }

        return parts.join('<br>');
      }

      function formatDateTime(value) {
        const raw = String(value || '').trim();
        if (!raw) {
          return '';
        }
        const date = new Date(raw);
        if (Number.isNaN(date.getTime())) {
          return raw;
        }
        return date.toLocaleString('de-DE', {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit'
        });
      }

      function detectDeltaSuggestionFromFields(fields) {
        const candidates = Array.isArray(fields) ? fields : [];
        if (!candidates.length) {
          return null;
        }

        const priorityPatterns = [
          { pattern: /(lastmodifieddate|lastmodified|systemmodstamp|updatedat|modifiedat)/i, strategy: 'datetime' },
          { pattern: /(timestamp|rowversion)/i, strategy: 'timestamp' }
        ];

        for (const rule of priorityPatterns) {
          const match = candidates.find((field) => rule.pattern.test(String(field?.name || '')));
          if (match?.name) {
            return {
              field: String(match.name).trim(),
              strategy: rule.strategy
            };
          }
        }

        return null;
      }

      async function autoFixSqlQuery() {
        const result = window.aiSchedulerState.currentResult;
        const assessment = window.aiSchedulerState.currentSqlAssessment;
        const btn = document.getElementById('ai-sql-autofix-btn');

        if (!result?.schedule || !assessment?.canAutoFix) {
          return;
        }

        const schedule = result.schedule;
        const originalLabel = btn ? btn.textContent : '';
        if (btn) {
          btn.disabled = true;
          btn.textContent = 'Auto-Fix läuft...';
        }

        try {
          if (assessment.reason !== 'select_star' && assessment.reason !== 'missing_where') {
            setAiTestStatus('warning', 'Für diese SQL-Warnung ist aktuell kein Auto-Fix definiert.');
            return;
          }

          const fieldsResult = await aiRequestJson('/api/sources/fields', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              sourceType: schedule.sourceType,
              sourceDefinition: schedule.sourceDefinition,
              objectName: schedule.objectName,
              connectorId: schedule.connectorId
            })
          });

          const fields = Array.isArray(fieldsResult?.fields) ? fieldsResult.fields : [];
          const columnNames = fields.map((field) => String(field?.name || '').trim()).filter(Boolean);

          if (!columnNames.length) {
            setAiTestStatus('warning', 'Auto-Fix konnte keine Quellfelder laden.');
            return;
          }

          const selectClause = 'SELECT ' + columnNames.join(', ');
          const rawSourceDefinition = String(schedule.sourceDefinition || '').trim();
          let currentQueryText = String(assessment.sourceQueryText || '').trim();
          let parsedAsJson = false;
          let parsedObject = null;

          try {
            parsedObject = JSON.parse(rawSourceDefinition);
            parsedAsJson = !!(parsedObject && typeof parsedObject === 'object' && !Array.isArray(parsedObject));
            if (parsedAsJson && typeof parsedObject.queryText === 'string') {
              currentQueryText = String(parsedObject.queryText || '').trim();
            }
          } catch {
            parsedAsJson = false;
          }

          let updatedQuery = currentQueryText;
          if (assessment.reason === 'select_star') {
            if (!/SELECT\\s+\\*/i.test(currentQueryText)) {
              setAiTestStatus('warning', 'Auto-Fix konnte kein SELECT * mehr finden. Bitte neu generieren/testen.');
              return;
            }
            updatedQuery = currentQueryText.replace(/SELECT\\s+\\*/i, selectClause);
          }

          if (assessment.reason === 'missing_where') {
            if (/\bWHERE\b/i.test(currentQueryText)) {
              setAiTestStatus('warning', 'Die Abfrage enthält bereits ein WHERE. Bitte neu generieren/testen.');
              return;
            }
            const baseQuery = currentQueryText.replace(/;\s*$/, '').trim();
            updatedQuery = baseQuery + ' WHERE 1=1';
          }

          if (parsedAsJson && parsedObject) {
            parsedObject.queryText = updatedQuery;
            schedule.sourceDefinition = JSON.stringify(parsedObject, null, 2);
          } else {
            schedule.sourceDefinition = updatedQuery;
          }

          window.aiSchedulerState.currentResult.schedule = schedule;
          displayResult(window.aiSchedulerState.currentResult);
          const successMessage = assessment.reason === 'select_star'
            ? 'SQL Auto-Fix angewendet: SELECT * wurde durch explizite Spalten ersetzt.'
            : 'SQL Auto-Fix angewendet: Standard-WHERE ergänzt (WHERE 1=1). Delta-/Fachfilter bitte ergänzen.';
          setAiTestStatus('success', successMessage);
        } catch (error) {
          setAiTestStatus('error', 'SQL Auto-Fix fehlgeschlagen: ' + (error?.message || 'Unbekannter Fehler'));
        } finally {
          if (btn) {
            btn.disabled = false;
            btn.textContent = originalLabel || 'SQL Auto-Fix anwenden';
          }
        }
      }

      function applyDeltaSuggestion() {
        const result = window.aiSchedulerState.currentResult;
        const suggestion = window.aiSchedulerState.currentDeltaSuggestion;
        if (!result?.schedule || !suggestion?.field || !suggestion?.strategy) {
          return;
        }

        const schedule = result.schedule;
        const rawSourceDefinition = String(schedule.sourceDefinition || '').trim();
        let queryText = rawSourceDefinition;
        let nextDefinition = {};

        try {
          const parsed = JSON.parse(rawSourceDefinition);
          if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
            nextDefinition = { ...parsed };
            queryText = String(parsed.queryText || parsed.query || parsed.soql || rawSourceDefinition).trim();
          }
        } catch {
          nextDefinition = {};
        }

        if (!queryText) {
          setAiTestStatus('warning', 'Delta konnte nicht übernommen werden: keine Abfrage vorhanden.');
          return;
        }

        nextDefinition.queryText = queryText;
        nextDefinition.delta = {
          strategy: suggestion.strategy,
          field: suggestion.field
        };

        schedule.sourceDefinition = JSON.stringify(nextDefinition, null, 2);
        window.aiSchedulerState.currentResult.schedule = schedule;
        displayResult(window.aiSchedulerState.currentResult);
        setAiTestStatus('success', 'Delta-Vorschlag übernommen: ' + suggestion.strategy + ' auf Feld ' + suggestion.field + '.');
      }

      function showLoading(loading) {
        window.aiSchedulerState.isLoading = loading;
        const btn = document.getElementById('ai-generate-btn');
        const spinner = document.getElementById('ai-loading-spinner');
        if (btn) {
          btn.disabled = loading;
          if (loading) {
            spinner.classList.remove('d-none');
            document.getElementById('ai-btn-text').textContent = 'Wird generiert...';
          } else {
            spinner.classList.add('d-none');
            document.getElementById('ai-btn-text').textContent = '⚡ Konfiguration generieren';
          }
        }
      }

      function clearForm() {
        document.getElementById('ai-prompt-input').value = '';
        hideResult();
      }

      function hideResult() {
        document.getElementById('ai-result-container').classList.add('d-none');
        window.aiSchedulerState.currentResult = null;
      }

      async function saveScheduler() {
        const result = window.aiSchedulerState.currentResult;
        if (!result) return;

        try {
          const response = await aiRequestJson('/api/schedules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(result.schedule)
          });

          const expectedSource = normalizePersistedConfig(result.schedule?.sourceDefinition);
          const persistedSource = normalizePersistedConfig(response?.sourceDefinition);
          const expectedMapping = normalizePersistedConfig(result.schedule?.mappingDefinition);
          const persistedMapping = normalizePersistedConfig(response?.mappingDefinition);
          const mismatches = [];
          if (expectedSource && persistedSource && expectedSource !== persistedSource) {
            mismatches.push('SQL/SourceDefinition');
          }
          if (expectedMapping && persistedMapping && expectedMapping !== persistedMapping) {
            mismatches.push('Mapping');
          }

          if (mismatches.length) {
            throw new Error('Speichern wurde von Salesforce bestätigt, aber diese Änderungen wurden nicht übernommen: ' + mismatches.join(', '));
          }

          window.alert(
            result.mode === 'update'
              ? 'Scheduler erfolgreich aktualisiert: ' + (response.name || response.id)
              : 'Scheduler erfolgreich erstellt: ' + (response.name || response.id)
          );
          clearForm();
          
          if (typeof window.refreshSchedules === 'function') {
            await window.refreshSchedules({ includeGraph: true, includeRecordsSummary: true });
          }
        } catch (error) {
          alert('Fehler beim Speichern: ' + (error.message || 'Unbekannter Fehler'));
        }
      }

      function normalizePersistedConfig(value) {
        const raw = String(value || '').trim();
        if (!raw) {
          return '';
        }
        try {
          return JSON.stringify(JSON.parse(raw));
        } catch {
          return raw.replace(/\s+/g, ' ');
        }
      }

      function toMappingDataType(rawType) {
        const normalized = String(rawType || '').trim().toLowerCase();
        if (!normalized) return 'string';
        if (normalized === 'integer' || normalized === 'int') return 'integer';
        if (normalized === 'number' || normalized === 'double' || normalized === 'currency' || normalized === 'percent' || normalized === 'decimal') return 'number';
        if (normalized === 'boolean') return 'boolean';
        if (normalized === 'date' || normalized === 'datetime') return 'datetime';
        return 'string';
      }

      function buildMappingDefinitionFromGeneratedItems(items) {
        const safeItems = Array.isArray(items) ? items : [];
        return safeItems
          .map((item) => {
            const targetField = String(item?.targetField || '').trim();
            const sourceField = String(item?.sourceField || '').trim();
            if (!targetField || !sourceField) {
              return '';
            }
            const targetType = toMappingDataType(item?.targetType || item?.targetFieldType || 'string');
            const transform = String(item?.transformFunction || 'NONE').trim().toUpperCase() || 'NONE';
            return targetField + ';' + targetType + '=' + sourceField + ';' + transform;
          })
          .filter(Boolean)
          .join('\\n');
      }

      function setAiTestStatus(kind, message) {
        const box = document.getElementById('ai-test-status');
        if (!box) {
          return;
        }
        const cssClass = kind === 'success'
          ? 'alert-success'
          : kind === 'error'
            ? 'alert-danger'
            : 'alert-warning';
        box.className = 'alert ' + cssClass + ' mb-3';
        box.textContent = String(message || '');
      }

      function evaluateSqlTrafficLight(sourceType, queryText) {
        const type = String(sourceType || '').trim().toUpperCase();
        const query = String(queryText || '').trim();
        const isSqlLike = type === 'MSSQL_SQL' || type === 'MSSQL' || type === 'SALESFORCE_SOQL';

        if (!isSqlLike) {
          return null;
        }

        if (!query) {
          return {
            level: 'red',
            label: 'ROT',
            message: 'Keine Abfrage erkannt.'
          };
        }

        const upper = query.toUpperCase();
        const startsWithRead = /^\s*(SELECT|WITH)\b/i.test(query);
        const dangerousPattern = /\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|EXEC|MERGE)\b/i;

        if (dangerousPattern.test(upper)) {
          return {
            level: 'red',
            label: 'ROT',
            message: 'Abfrage enthält schreibende/gefährliche SQL-Befehle.'
          };
        }

        if (!startsWithRead) {
          return {
            level: 'red',
            label: 'ROT',
            message: 'Erwartet SELECT/WITH als Start der Abfrage.'
          };
        }

        const hasSelectStar = /SELECT\\s+\\*/i.test(query);
        const hasWhere = /\bWHERE\b/i.test(query);

        if (hasSelectStar || !hasWhere) {
          return {
            level: 'yellow',
            label: 'GELB',
              canAutoFix: hasSelectStar,
              reason: hasSelectStar ? 'select_star' : 'missing_where',
            message: hasSelectStar
              ? 'SELECT * erkannt - besser explizite Spalten wählen.'
              : 'Kein WHERE erkannt - prüfe Datenmenge/Delta-Filter.'
          };
        }

        return {
          level: 'green',
          label: 'GRÜN',
            canAutoFix: false,
            reason: 'ok',
          message: 'Abfrage wirkt syntaktisch plausibel für den Scheduler-Lesezugriff.'
        };
      }

      async function refineScheduler() {
        const result = window.aiSchedulerState.currentResult;
        if (!result || !result.schedule) {
          alert('Bitte zuerst eine Konfiguration generieren.');
          return;
        }

        const refineBtn = document.getElementById('ai-refine-btn');
        const originalLabel = refineBtn ? refineBtn.textContent : '';
        if (refineBtn) {
          refineBtn.disabled = true;
          refineBtn.textContent = 'Teste...';
        }

        try {
          const schedule = result.schedule;

          const validation = await aiRequestJson('/api/schedules/validate-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(schedule)
          });

          const validationIssues = Array.isArray(validation?.issues) ? validation.issues : [];
          const validationErrors = validationIssues.filter((issue) => String(issue?.severity || '').toLowerCase() === 'error');

          if (validationErrors.length > 0) {
            setAiTestStatus('error', 'Konfigurationsfehler: ' + validationErrors.map((issue) => String(issue.message || '')).filter(Boolean).join(' | '));
            return;
          }

          const previewResult = await aiRequestJson('/api/sources/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              sourceType: schedule.sourceType,
              sourceDefinition: schedule.sourceDefinition,
              connectorId: schedule.connectorId,
              limit: 5
            })
          });

          const previewRows = Array.isArray(previewResult?.rows) ? previewResult.rows : [];

          let sourceFields = [];
          const sourceType = String(schedule.sourceType || '').trim().toUpperCase();
          if (sourceType === 'MSSQL_SQL' || sourceType === 'MSSQL' || sourceType === 'SALESFORCE_SOQL') {
            try {
              const sourceFieldsResult = await aiRequestJson('/api/sources/fields', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  sourceType: schedule.sourceType,
                  sourceDefinition: schedule.sourceDefinition,
                  objectName: schedule.objectName,
                  connectorId: schedule.connectorId
                })
              });
              sourceFields = Array.isArray(sourceFieldsResult?.fields) ? sourceFieldsResult.fields : [];
            } catch {
              sourceFields = [];
            }
          }

          const deltaSuggestion = detectDeltaSuggestionFromFields(sourceFields);
          window.aiSchedulerState.currentDeltaSuggestion = hasDeltaConfig(schedule.sourceDefinition) ? null : deltaSuggestion;

          let generatedMappings = [];
          const targetType = String(schedule.targetType || '').trim().toUpperCase();
          if (targetType === 'SALESFORCE' || targetType === 'SALESFORCE_GLOBAL_PICKLIST') {
            const targetFieldsResult = await aiRequestJson('/api/mapping/target-fields', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                targetSystem: schedule.targetSystem,
                targetObject: schedule.objectName,
                connectorId: schedule.connectorId
              })
            });

            const targetFields = Array.isArray(targetFieldsResult?.fields) ? targetFieldsResult.fields : [];

            if (sourceFields.length && targetFields.length) {
              const mappingResult = await aiRequestJson('/api/salesforce/generate-mapping', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  sourceFields: sourceFields.map((field) => ({
                    name: String(field?.name || '').trim(),
                    type: String(field?.type || 'string').trim()
                  })),
                  targetFields: targetFields.map((field) => ({
                    name: String(field?.name || '').trim(),
                    label: String(field?.label || '').trim(),
                    type: String(field?.type || '').trim(),
                    isExternalId: field?.isExternalId === true
                  })),
                  targetObjectApiName: String(schedule.objectName || '').trim() || 'Contact',
                  profile: 'standard'
                })
              });

              generatedMappings = Array.isArray(mappingResult?.items) ? mappingResult.items : [];
              if (generatedMappings.length > 0) {
                schedule.mappingDefinition = buildMappingDefinitionFromGeneratedItems(generatedMappings);
                window.aiSchedulerState.currentResult.schedule = schedule;
              }
            }
          }

          displayResult(window.aiSchedulerState.currentResult);

          const warningCount = validationIssues.filter((issue) => String(issue?.severity || '').toLowerCase() === 'warning').length;
          const summary = 'Abfrage erfolgreich getestet (' + previewRows.length + ' Vorschau-Zeilen). '
            + (generatedMappings.length > 0
              ? 'Mapping aufgebaut: ' + generatedMappings.length + ' Regeln.'
              : 'Kein zusaetzliches Auto-Mapping erzeugt.')
            + (window.aiSchedulerState.currentDeltaSuggestion
              ? ' Delta-Vorschlag: ' + window.aiSchedulerState.currentDeltaSuggestion.field + ' (' + window.aiSchedulerState.currentDeltaSuggestion.strategy + ').'
              : '')
            + (warningCount > 0 ? ' Warnungen: ' + warningCount + '.' : '');
          setAiTestStatus('success', summary);
        } catch (error) {
          setAiTestStatus('error', 'Test fehlgeschlagen: ' + (error?.message || 'Unbekannter Fehler'));
        } finally {
          if (refineBtn) {
            refineBtn.disabled = false;
            refineBtn.textContent = originalLabel || '🔧 Abfrage testen + Mapping aufbauen';
          }
        }
      }

      function esc(str) {
        const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
        return String(str || '').replace(/[&<>"']/g, m => map[m]);
      }

      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', bootAIAssistant);
      } else {
        void bootAIAssistant();
      }
    </script>
  `;
}
