// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-migration-planning
// Source lines: 16533–16607
// ──────────────────────────────────────────────────────────────────────

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


