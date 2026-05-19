export function renderConnectorUiModule(): string {
  return `
      function renderConnectors() {
        const panels = document.getElementById('connectors-panels');
        const summary = document.getElementById('connectors-summary');
        const sqlSelect = document.getElementById('sql-connector-select');

        const mssqlItems = state.connectors.filter((item) => String(item.connectorType).toLowerCase() === 'mssql');
        if (sqlSelect) {
          sqlSelect.innerHTML = mssqlItems.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.name) + '</option>').join('');
          if (!sqlSelect.innerHTML) {
            sqlSelect.innerHTML = '<option value="">Keine MSSQL-Connectoren</option>';
          }
        }

        const totalConnectors = (state.connectors || []).length;
        const activeConnectors = (state.connectors || []).filter((item) => item.active).length;
        const testedConnectors = Object.keys(state.connectorTestResults || {}).length;
        if (summary) {
          summary.textContent = totalConnectors + ' Connectoren, ' + activeConnectors + ' aktiv, ' + testedConnectors + ' mit Teststatus';
        }

        if (!state.connectors.length) {
          panels.innerHTML = '<div class="col-12"><div class="alert alert-light border text-secondary mb-0">' + esc(state.runtimeContextUnavailableMessage || 'Keine Connectoren gefunden.') + '</div></div>';
          return;
        }

        const orderedConnectors = (state.connectors || []).slice().sort((left, right) => {
          if (Boolean(left.active) !== Boolean(right.active)) {
            return left.active ? -1 : 1;
          }
          return String(left.name || '').localeCompare(String(right.name || ''), 'de', { sensitivity: 'base' });
        });

        function buildConnectorFacts(item) {
          const parameters = item && typeof item.parameters === 'object' && !Array.isArray(item.parameters) ? item.parameters : {};
          const filePaths = item && item.filePaths && typeof item.filePaths === 'object' ? item.filePaths : null;
          if (String(item.connectorType || '').toLowerCase() === 'mssql') {
            const schemaName = parameters.schema || parameters.schemaName;
            const tableName = parameters.table || parameters.tableName;
            return [
              'Server: ' + esc(parameters.server || '-'),
              'DB: ' + esc(parameters.database || '-'),
              'Schema/Tabelle: ' + esc([schemaName, tableName].filter(Boolean).join('.') || '-')
            ];
          }
          if (isRestConnectorType(item.connectorType)) {
            return [
              'Base URL: ' + esc(parameters.baseUrl || '-'),
              'Pfad: ' + esc(parameters.resourcePath || parameters.endpoint || '/'),
              'Auth: ' + esc(parameters.authType || 'none')
            ];
          }
          if (isFileConnectorType(item.connectorType)) {
            return [
              'Root: ' + esc(filePaths?.basePath || parameters.basePath || parameters.fileBasePath || 'artifacts/files'),
              'Import: ' + esc(filePaths?.importPath || parameters.importPath || 'inbound'),
              'Export: ' + esc(filePaths?.exportPath || parameters.exportPath || 'outbound'),
              'Archiv: ' + esc(filePaths?.archivePath || parameters.archivePath || 'archive')
            ];
          }
          return [
            'Parameter: ' + esc((item.parameterKeys || []).slice(0, 5).join(', ') || '-'),
            'Target: ' + esc(item.targetSystem || '-'),
            'Richtung: ' + esc(item.direction || '-')
          ];
        }

        function buildConnectorVisual(item) {
          const typeLabel = String(item.connectorType || 'Connector').trim();
          const visualClass = getConnectorGraphClass(item.connectorType, item.name || item.connectorType || '');
          const icon = getConnectorIcon(item.connectorType, item.name || item.connectorType || '');
          return '<div class="connector-type-visual ' + esc(visualClass) + '">' +
            '<div class="connector-type-visual-bg"></div>' +
            '<div class="connector-type-visual-icon" aria-hidden="true">' + esc(icon) + '</div>' +
            '<div class="connector-type-visual-meta">' +
              '<div class="connector-type-visual-label">' + esc(typeLabel) + '</div>' +
              '<div class="connector-type-visual-caption">Typprofil</div>' +
            '</div>' +
          '</div>';
        }

        function buildConnectorTestMarkup(item) {
          const result = state.connectorTestResults ? state.connectorTestResults[item.id] : null;
          if (!result) {
            return '<div class="small text-secondary">Noch kein Test ausgeführt.</div>';
          }
          if (result.pending) {
            return '<div class="alert alert-info py-2 mb-0 small">Connector-Test läuft...</div>';
          }
          const tone = result.ok ? 'success' : 'danger';
          const checks = Array.isArray(result.checks) ? result.checks : [];
          return '<div class="alert alert-' + tone + ' py-2 mb-0 small">' +
            '<div class="fw-semibold mb-1">' + esc(result.message || (result.ok ? 'Test erfolgreich' : 'Test fehlgeschlagen')) + '</div>' +
            (checks.length ? '<ul class="mb-0 ps-3">' + checks.map((check) => '<li><strong>' + esc(check.label) + ':</strong> ' + esc(check.details) + '</li>').join('') + '</ul>' : '') +
          '</div>';
        }

        function buildConnectorNotificationMarkup(item) {
          const parameters = item && typeof item.parameters === 'object' && !Array.isArray(item.parameters) ? item.parameters : {};
          const ownerId = String(parameters.notificationTaskOwnerId || '').trim();
          const ownerUsername = String(parameters.notificationTaskOwnerUsername || '').trim();
          const enabled = parameters.notificationTaskEnabled === true && !!ownerId;
          const errorClasses = Array.isArray(parameters.notificationTaskErrorClasses)
            ? parameters.notificationTaskErrorClasses
            : String(parameters.notificationTaskErrorClasses || '')
                .split(',')
                .map((value) => value.trim())
                .filter(Boolean);

          if (!enabled) {
            return '<div class="small text-secondary">Fehlerbenachrichtigung: nicht aktiv</div>';
          }

          const normalizedClasses = errorClasses.length ? errorClasses : ['CONNECTION', 'AUTH', 'DATA', 'VALIDATION', 'UNKNOWN'];
          return '<div class="alert alert-warning-subtle border py-2 mb-0 small">' +
            '<div class="fw-semibold mb-1">Fehlerbenachrichtigung aktiv</div>' +
            '<div><strong>Empfänger:</strong> ' + esc(ownerUsername || ownerId) + '</div>' +
            '<div><strong>Klassen:</strong> ' + esc(normalizedClasses.join(', ')) + '</div>' +
          '</div>';
        }

        panels.innerHTML = orderedConnectors.map((item) =>
          '<div class="col-12 col-xl-6" data-connector-panel>' +
            '<div class="card h-100 border-0 shadow-sm bg-body-tertiary">' +
              '<div class="card-body d-flex flex-column gap-3">' +
                '<div class="d-flex justify-content-between align-items-start gap-3">' +
                  '<div class="d-flex align-items-start gap-3 flex-grow-1">' +
                    buildConnectorVisual(item) +
                    '<div>' +
                    '<div class="d-flex flex-wrap gap-2 align-items-center mb-1"><strong>' + esc(item.name) + '</strong><span class="badge bg-secondary-subtle text-secondary border">' + esc(item.connectorType) + '</span>' + (item.active ? '<span class="badge bg-success-subtle text-success border">aktiv</span>' : '<span class="badge bg-secondary-subtle text-secondary border">inaktiv</span>') + '</div>' +
                    '<div class="small text-secondary">' + esc(item.direction || '-') + ' • ' + esc(item.targetSystem || 'kein Zielsystem') + '</div>' +
                    '</div>' +
                  '</div>' +
                  '<div class="d-flex flex-wrap gap-1 justify-content-end">' +
                    '<button class="btn btn-sm btn-outline-primary" data-edit-connector="' + esc(item.id) + '">Öffnen</button>' +
                    '<button class="btn btn-sm btn-outline-secondary" data-test-connector="' + esc(item.id) + '">Testen</button>' +
                    '<button class="btn btn-sm btn-outline-danger" data-delete-connector="' + esc(item.id) + '">Löschen</button>' +
                  '</div>' +
                '</div>' +
                '<div class="small d-grid gap-1">' + buildConnectorFacts(item).map((line) => '<div>' + line + '</div>').join('') + '</div>' +
                '<div>' + buildConnectorNotificationMarkup(item) + '</div>' +
                '<div>' + buildConnectorTestMarkup(item) + '</div>' +
              '</div>' +
            '</div>' +
          '</div>'
        ).join('');

        panels.querySelectorAll('button[data-edit-connector]').forEach((button) => {
          button.addEventListener('click', () => openConnectorModal(button.getAttribute('data-edit-connector')));
        });

        panels.querySelectorAll('button[data-test-connector]').forEach((button) => {
          button.addEventListener('click', async () => {
            const connectorId = String(button.getAttribute('data-test-connector') || '').trim();
            const connector = (state.connectors || []).find((item) => String(item.id || '').trim() === connectorId);
            if (!connectorId) {
              return;
            }
            state.connectorTestResults[connectorId] = {
              pending: true,
              ok: true,
              message: 'Connector-Test läuft...',
              connectorId,
              connectorName: connector?.name || connectorId,
              connectorType: connector?.connectorType || '',
              checks: []
            };
            renderConnectors();
            try {
              const result = await requestJson('/api/connectors/' + encodeURIComponent(connectorId) + '/test', { method: 'POST' });
              state.connectorTestResults[connectorId] = result;
            } catch (error) {
              state.connectorTestResults[connectorId] = {
                ok: false,
                connectorId,
                connectorName: connector?.name || connectorId,
                connectorType: connector?.connectorType || '',
                message: error.message || 'Connector-Test fehlgeschlagen',
                testedAt: new Date().toISOString(),
                checks: [{ label: 'Connector-Test', ok: false, details: error.message || 'Connector-Test fehlgeschlagen' }]
              };
            }
            renderConnectors();
          });
        });

        panels.querySelectorAll('button[data-delete-connector]').forEach((button) => {
          button.addEventListener('click', async () => {
            const connectorId = button.getAttribute('data-delete-connector');
            if (!connectorId) {
              return;
            }

            const connector = (state.connectors || []).find((item) => item.id === connectorId);
            const linkedSchedules = (state.schedules || []).filter((item) => item.connectorId === connectorId);
            const confirmed = window.confirm(
              'Connector "' + (connector?.name || connectorId) + '" löschen? Zugeordnete Scheduler werden ebenfalls entfernt (' + linkedSchedules.length + ').'
            );
            if (!confirmed) {
              return;
            }

            await requestJson('/api/connectors/' + encodeURIComponent(connectorId), { method: 'DELETE' });
            delete state.connectorTestResults[connectorId];
            await refresh();
          });
        });

        setTimeout(() => initializeTableFilters(), 100);
      }
`;
}
