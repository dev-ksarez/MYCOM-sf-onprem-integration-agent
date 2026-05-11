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

                <!-- Issues -->
                <div id="ai-issues-container"></div>

                <!-- Preview -->
                <div class="mb-3">
                  <h6 class="fw-semibold mb-2">Scheduler-Vorschau</h6>
                  <div class="ai-config-preview p-3 bg-light rounded border" id="ai-config-preview"></div>
                </div>

                <!-- Mapping Preview -->
                <div class="mb-3">
                  <h6 class="fw-semibold mb-2">Feld-Zuordnung (Mapping)</h6>
                  <div class="ai-mapping-preview p-3 bg-light rounded border" id="ai-mapping-preview"></div>
                </div>

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
                    🔧 Verfeinern
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
        isLoading: false
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
        "inbound-1": "Alle Accounts aus unserer MSSQL-Datenbank nach Salesforce synchronisieren. Email als eindeutige ID. Täglich um 08:00 Uhr.",
        "inbound-2": "REST-API Kundenaktualisierungen nach Salesforce Contact Object. Stündliche Synchronisation.",
        "outbound-1": "Alle aktiven Salesforce Contacts mit Newsletter-Opt-in nach Brevo/Newsletter-System exportieren. Täglicher Export um 22 Uhr.",
        "outbound-2": "Abgeschlossene Opportunities aus Salesforce als Orders in unser ERP-System synchronisieren."
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
        const issuesContainer = document.getElementById('ai-issues-container');
        const configPreview = document.getElementById('ai-config-preview');
        const mappingPreview = document.getElementById('ai-mapping-preview');
        const confidenceBadge = document.getElementById('ai-confidence-badge');

        // Confidence Badge
        const confidence = (result.confidence * 100).toFixed(0);
        const confidenceClass = result.confidence > 0.75 ? 'bg-success' : result.confidence > 0.5 ? 'bg-warning' : 'bg-danger';
        confidenceBadge.className = 'badge ' + confidenceClass;
        confidenceBadge.textContent = confidence + '%';

        // Reasoning
        reasoningAlert.innerHTML = '✓ ' + esc(result.reasoning);

        // Issues
        if (result.issues && result.issues.length > 0) {
          issuesContainer.innerHTML = result.issues
            .map(issue => '<div class="alert alert-' + (issue.severity === 'error' ? 'danger' : 'warning') + ' py-2 mb-2">' +
              (issue.severity === 'error' ? '❌' : '⚠️') + ' ' + esc(issue.message) + '</div>')
            .join('');
        } else {
          issuesContainer.innerHTML = '';
        }

        // Config Preview
        const schedule = result.schedule;
        configPreview.textContent = 
          'Name: ' + esc(schedule.name) + '\\n' +
          'Quelle: ' + esc(schedule.sourceSystem) + ' (' + esc(schedule.sourceType) + ')\\n' +
          'Ziel: ' + esc(schedule.targetSystem) + ' (' + esc(schedule.targetType) + ')\\n' +
          'Object: ' + esc(schedule.objectName) + ' / ' + esc(schedule.operation) + '\\n' +
          'Richtung: ' + esc(schedule.direction) + '\\n' +
          'Batch-Size: ' + (schedule.batchSize || 100) + '\\n' +
          'Timing: ' + esc(schedule.timingDefinition || '-');

        // Mapping Preview
        mappingPreview.textContent = esc(schedule.mappingDefinition || 'Keine Zuordnung definiert');

        container.classList.remove('d-none');
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

          window.alert('Scheduler erfolgreich erstellt: ' + (response.name || response.id));
          clearForm();
          
          // Refresh parent view wenn vorhanden
          if (window.refreshSchedules) window.refreshSchedules();
        } catch (error) {
          alert('Fehler beim Speichern: ' + (error.message || 'Unbekannter Fehler'));
        }
      }

      function refineScheduler() {
        alert('Verfeinerungs-Dialog kommt bald...');
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
