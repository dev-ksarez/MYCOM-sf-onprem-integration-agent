// ──────────────────────────────────────────────────────────────────────
// Module: admin-ui-core
// Source lines: 1311–1712
// ──────────────────────────────────────────────────────────────────────

let logsChart;
let recordsChart;
const salesforceGaugeCharts = {};

// Hex to HSL & HSL to Hex helpers
function hexToRgb(hex) {
  const clean = hex.replace(/^#/, '');
  const bigint = parseInt(clean, 16);
  const r = (bigint >> 16) & 255;
  const g = (bigint >> 8) & 255;
  const b = bigint & 255;
  return { r, g, b };
}

function rgbToHsl(r, g, b) {
  r /= 255;
  g /= 255;
  b /= 255;
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  let h, s, l = (max + min) / 2;

  if (max === min) {
    h = s = 0; // achromatic
  } else {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    switch (max) {
      case r: h = (g - b) / d + (g < b ? 6 : 0); break;
      case g: h = (b - r) / d + 2; break;
      case b: h = (r - g) / d + 4; break;
    }
    h /= 6;
  }
  return {
    h: Math.round(h * 360),
    s: Math.round(s * 100),
    l: Math.round(l * 100)
  };
}

function hslToHex(h, s, l) {
  s /= 100;
  l /= 100;
  const k = n => (n + h / 30) % 12;
  const a = s * Math.min(l, 1 - l);
  const f = n => l - a * Math.max(-1, Math.min(k(n) - 3, 9 - k(n), 1));
  const toHex = x => {
    const val = Math.round(x * 255).toString(16);
    return val.length === 1 ? '0' + val : val;
  };
  return `#${toHex(f(0))}${toHex(f(8))}${toHex(f(4))}`;
}

// Generate the HSL-based palette from a base primary hex color
function generateLogoThemePalette(primaryHex) {
  const { h, s, l } = rgbToHsl(hexToRgb(primaryHex).r, hexToRgb(primaryHex).g, hexToRgb(primaryHex).b);
  
  // Ensure the primary color lightness is in a readable range (35% to 65% for light UI)
  const adjustedL = Math.max(35, Math.min(l, 65));
  const primary = hslToHex(h, s, adjustedL);
  
  // Generate harmonious palette
  const primaryStrong = hslToHex(h, s, Math.max(20, adjustedL - 12));
  
  // Accent is complementary (rotate 180 degrees) or high-contrast alternative
  const accent = hslToHex((h + 180) % 360, Math.max(50, s), Math.max(40, Math.min(adjustedL, 60)));
  
  const surfaceSoft = hslToHex(h, 8, 97);
  const border = hslToHex(h, 12, 90);
  const borderStrong = hslToHex(h, 15, 80);
  
  // Sidebar: Dark slate with a subtle tint of the primary brand color
  const sidebarBg1 = hslToHex(h, Math.max(15, Math.min(s, 25)), 10);
  const sidebarBg2 = hslToHex(h, Math.max(15, Math.min(s, 25)), 16);
  const contentBg = hslToHex(h, 8, 95);

  return {
    primary,
    primaryStrong,
    accent,
    surfaceSoft,
    border,
    borderStrong,
    sidebarBg1,
    sidebarBg2,
    contentBg
  };
}

// Inject style block dynamically
function injectLogoThemeStyle(palette) {
  let styleEl = document.getElementById('dynamic-logo-theme-style');
  if (!styleEl) {
    styleEl = document.createElement('style');
    styleEl.id = 'dynamic-logo-theme-style';
    document.head.appendChild(styleEl);
  }
  
  styleEl.textContent = `
    body.theme-logo {
      --ui-primary: ${palette.primary} !important;
      --ui-primary-strong: ${palette.primaryStrong} !important;
      --ui-accent: ${palette.accent} !important;
      --ui-surface: #ffffff !important;
      --ui-surface-soft: ${palette.surfaceSoft} !important;
      --ui-border: ${palette.border} !important;
      --ui-border-strong: ${palette.borderStrong} !important;
      --ui-text: #172534 !important;
      --ui-text-muted: #4f6175 !important;
      --ui-shadow-sm: 0 2px 10px rgba(17, 37, 63, 0.08) !important;
      --ui-shadow-md: 0 12px 30px rgba(17, 37, 63, 0.12) !important;
      
      --layout-sidebar-bg: ${palette.sidebarBg1} !important;
      --layout-sidebar-bg-2: ${palette.sidebarBg2} !important;
      --layout-sidebar-text: rgba(255, 255, 255, 0.78) !important;
      --layout-sidebar-text-strong: #ffffff !important;
      --layout-content-bg: ${palette.contentBg} !important;
      --layout-card-border: ${palette.border} !important;
      --layout-topbar-bg: #ffffff !important;
      --layout-topbar-border: ${palette.border} !important;
      --layout-topbar-text: #203246 !important;
      --layout-surface: #ffffff !important;
      --layout-surface-soft: ${palette.surfaceSoft} !important;
      --layout-surface-muted: ${palette.border} !important;
      --layout-text: #203246 !important;
      --layout-text-muted: #53677c !important;
      --layout-tab-hover: rgba(255, 255, 255, 0.1) !important;
      --layout-tab-active-bg: linear-gradient(90deg, rgba(255, 255, 255, 0.18) 0%, rgba(255, 255, 255, 0.07) 100%) !important;
      --layout-tab-active-accent: rgba(255, 255, 255, 0.72) !important;
    }
  `;
}

// Extractor logic using HTML5 Canvas
function extractColorsFromLogo(dataUrl) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      try {
        const canvas = document.createElement('canvas');
        canvas.width = 50;
        canvas.height = 50;
        const ctx = canvas.getContext('2d');
        ctx.drawImage(img, 0, 0, 50, 50);
        
        const imgData = ctx.getImageData(0, 0, 50, 50).data;
        const colorCounts = {};
        
        for (let i = 0; i < imgData.length; i += 4) {
          const r = imgData[i];
          const g = imgData[i+1];
          const b = imgData[i+2];
          const a = imgData[i+3];
          
          // Skip transparent or near-transparent pixels
          if (a < 120) continue;
          
          // Skip grayscale / white / black pixels
          const maxVal = Math.max(r, g, b);
          const minVal = Math.min(r, g, b);
          if (maxVal - minVal < 25) {
            // Low saturation: check if it's too white or black
            if (maxVal > 220 || maxVal < 35) continue;
          }
          
          // Group colors roughly to avoid noise (quantization to multiples of 8)
          const qr = Math.round(r / 8) * 8;
          const qg = Math.round(g / 8) * 8;
          const qb = Math.round(b / 8) * 8;
          const key = `${qr},${qg},${qb}`;
          
          colorCounts[key] = (colorCounts[key] || 0) + 1;
        }
        
        // Find most frequent color
        let dominantColor = '0,106,168'; // default fallback blue
        let maxCount = 0;
        
        Object.entries(colorCounts).forEach(([color, count]) => {
          if (count > maxCount) {
            maxCount = count;
            dominantColor = color;
          }
        });
        
        const [r, g, b] = dominantColor.split(',').map(Number);
        
        // Convert to Hex
        const componentToHex = c => {
          const hex = c.toString(16);
          return hex.length === 1 ? '0' + hex : hex;
        };
        const hexColor = `#${componentToHex(r)}${componentToHex(g)}${componentToHex(b)}`;
        resolve(hexColor);
      } catch (err) {
        reject(err);
      }
    };
    img.onerror = () => reject(new Error('Bild konnte nicht geladen werden.'));
    img.src = dataUrl;
  });
}

function applyUiTheme(themeName) {
  const normalized = themeName === 'industrial' || themeName === 'midnight' || themeName === 'logo' ? themeName : 'corporate';
  if (document.body) {
    document.body.classList.remove('theme-corporate', 'theme-industrial', 'theme-midnight', 'theme-logo');
    document.body.classList.add('theme-' + normalized);
  }

  const overviewRunsTable = document.getElementById('overview-runs-table');
  if (overviewRunsTable) {
    overviewRunsTable.classList.toggle('table-dark', normalized === 'midnight');
    if (normalized === 'midnight') {
      overviewRunsTable.style.setProperty('--bs-table-bg', '#132032');
      overviewRunsTable.style.setProperty('--bs-table-color', '#d7e4f5');
      overviewRunsTable.style.setProperty('--bs-table-border-color', '#223146');
      overviewRunsTable.style.setProperty('--bs-table-accent-bg', '#132032');
      overviewRunsTable.style.setProperty('--bs-table-striped-bg', '#1a2a3f');
      overviewRunsTable.style.setProperty('--bs-table-hover-bg', '#1a2a3f');
      overviewRunsTable.style.setProperty('--bs-table-striped-color', '#e6edf7');
      overviewRunsTable.style.setProperty('--bs-table-hover-color', '#e6edf7');
    } else {
      overviewRunsTable.style.removeProperty('--bs-table-bg');
      overviewRunsTable.style.removeProperty('--bs-table-color');
      overviewRunsTable.style.removeProperty('--bs-table-border-color');
      overviewRunsTable.style.removeProperty('--bs-table-accent-bg');
      overviewRunsTable.style.removeProperty('--bs-table-striped-bg');
      overviewRunsTable.style.removeProperty('--bs-table-hover-bg');
      overviewRunsTable.style.removeProperty('--bs-table-striped-color');
      overviewRunsTable.style.removeProperty('--bs-table-hover-color');
    }
  }

  // Handle Logo Theme Injection
  if (normalized === 'logo') {
    const projectId = (typeof state !== 'undefined' && state.headerProjectId) || 'default-project';
    const savedPalette = localStorage.getItem('custom-logo-palette-' + projectId);
    if (savedPalette) {
      try {
        injectLogoThemeStyle(JSON.parse(savedPalette));
      } catch (e) {
        console.error('Failed to parse dynamic logo palette', e);
      }
    } else {
      const styleEl = document.getElementById('dynamic-logo-theme-style');
      if (styleEl) styleEl.remove();
    }
  }

  try {
    localStorage.setItem(UI_THEME_STORAGE_KEY, normalized);
  } catch {
    // ignore storage access issues
  }

  const select = document.getElementById('theme-select');
  if (select && select.value !== normalized) {
    select.value = normalized;
  }
}

function initializeUiTheme() {
  let storedTheme = 'corporate';
  try {
    storedTheme = localStorage.getItem(UI_THEME_STORAGE_KEY) || 'corporate';
  } catch {
    storedTheme = 'corporate';
  }
  applyUiTheme(storedTheme);
}

function createModalController(modalId) {
  const element = document.getElementById(modalId);
  const bootstrapModal =
    window.bootstrap && window.bootstrap.Modal
      ? new window.bootstrap.Modal(element)
      : null;

  const showFallback = () => {
    element.style.setProperty('display', 'block', 'important');
    element.classList.add('show');
    element.removeAttribute('aria-hidden');
    document.body.classList.add('modal-open');
  };

  const hideFallback = () => {
    element.classList.remove('show');
    element.style.display = 'none';
    element.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
  };

  if (!bootstrapModal) {
    hideFallback();
    element.querySelectorAll('[data-bs-dismiss="modal"]').forEach((button) => {
      button.addEventListener('click', hideFallback);
    });
  }

  return {
    show() {
      if (bootstrapModal) {
        try {
          bootstrapModal.show();
        } catch {
          showFallback();
          return;
        }
        if (!element.classList.contains('show')) {
          showFallback();
        }
        return;
      }
      showFallback();
    },
    hide() {
      if (bootstrapModal) {
        try {
          bootstrapModal.hide();
        } catch {
          hideFallback();
          return;
        }
        if (element.classList.contains('show')) {
          hideFallback();
        }
        return;
      }
      hideFallback();
    }
  };
}

const scheduleModal = createModalController('schedule-modal');
const connectorModal = createModalController('connector-modal');
const templatePickerModal = createModalController('template-picker-modal');
const logsModal = createModalController('logs-modal');
const failedRecordsModal = createModalController('failed-records-modal');
const recordsSchedulerModal = createModalController('records-scheduler-modal');
const connectorNotificationErrorClassOptions = ['CONNECTION', 'AUTH', 'DATA', 'VALIDATION', 'UNKNOWN'];

function esc(value) {
  return String(value ?? '-')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function htmlEscape(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function getTemplateAccent(item) {
  const tags = Array.isArray(item?.tags) ? item.tags.map((tag) => String(tag || '').toLowerCase()) : [];
  if (tags.includes('ezb')) {
    return { start: '#0f4c81', end: '#6db1ff', glaze: 'rgba(255,255,255,0.22)' };
  }
  if (tags.includes('brevo')) {
    return { start: '#0f766e', end: '#2dd4bf', glaze: 'rgba(255,255,255,0.18)' };
  }
  if (tags.includes('newsletter')) {
    return { start: '#b45309', end: '#f59e0b', glaze: 'rgba(255,255,255,0.16)' };
  }
  if (item?.kind === 'bundle') {
    return { start: '#4c1d95', end: '#7c3aed', glaze: 'rgba(255,255,255,0.18)' };
  }
  if (item?.kind === 'schedule') {
    return { start: '#155e75', end: '#06b6d4', glaze: 'rgba(255,255,255,0.18)' };
  }
  return { start: '#9a3412', end: '#f97316', glaze: 'rgba(255,255,255,0.16)' };
}

function getTemplateSymbol(item) {
  const tags = Array.isArray(item?.tags) ? item.tags.map((tag) => String(tag || '').toUpperCase()) : [];
  if (tags.includes('EZB')) {
    return 'EZB';
  }
  if (tags.includes('BREVO')) {
    return 'BR';
  }
  if (tags.includes('NEWSLETTER')) {
    return 'NL';
  }
  if (item?.kind === 'bundle') {
    return 'SET';
  }
  const source = String(item?.name || '').trim();
  const initials = source
    .split(/s+/)
    .map((part) => part.replace(/[^A-Za-z0-9]/g, '').slice(0, 1).toUpperCase())
    .filter(Boolean)
    .slice(0, 3)
    .join('');
  return initials || (item?.kind === 'schedule' ? 'JOB' : 'API');
}

function renderInstallerSummary() {
  const summary = state.installerSummary;
  const statusSummary = document.getElementById('installer-status-summary');
  const checksContainer = document.getElementById('installer-checks');
  const pathsContainer = document.getElementById('installer-paths');
  const commandsList = document.getElementById('installer-commands');
  const envTemplate = document.getElementById('installer-env-template');
  const scenarioSelect = document.getElementById('installer-scenario');
  const scenarioDescription = document.getElementById('installer-scenario-description');
  if (!statusSummary || !checksContainer || !pathsContainer || !commandsList || !envTemplate || !scenarioSelect || !scenarioDescription) {
    return;
  }

  if (!summary) {
    statusSummary.textContent = 'Installer-Daten konnten nicht geladen werden.';
    checksContainer.innerHTML = '';
    pathsContainer.innerHTML = '';
    commandsList.innerHTML = '';
    envTemplate.textContent = 'Keine Daten';
    return;
  }

  const scenarios = Array.isArray(summary.scenarios) ? summary.scenarios : [];
  if (!scenarioSelect.options.length) {
    scenarioSelect.innerHTML = scenarios.map((item) => '<option value="' + esc(item.id) + '">' + esc(item.label) + ' - ' + esc(item.networkScope) + '</option>').join('');
    if (summary.defaultScenarioId) {
      scenarioSelect.value = summary.defaultScenarioId;
    }
  }
  const selectedScenario = scenarios.find((item) => item.id === scenarioSelect.value) || scenarios[0];
  if (!selectedScenario) {
    statusSummary.textContent = 'Keine Setup-Szenarien verfügbar.';
    return;
  }

  scenarioDescription.textContent = selectedScenario.description + ' Generierte Dateien: ' + selectedScenario.generatedFilesLabel + '.';
  statusSummary.textContent = summary.authConfigured
    ? 'Das gewählte Setup-Szenario ist vorbereitet. Admin-Login, CSRF- und Origin-Schutz sind aktiv.'
    : 'Das gewählte Setup-Szenario ist vorbereitet, aber Admin-Zugang ist noch nicht vollständig konfiguriert.';

  renderHeaderMenuState();

  checksContainer.innerHTML = (Array.isArray(selectedScenario.checks) ? selectedScenario.checks : []).map((item) => {
    const badgeClass = item.status === 'ready' ? 'text-bg-success' : item.status === 'in-progress' ? 'text-bg-warning' : 'text-bg-danger';
    return '<div class="border rounded p-2 bg-light">' +
      '<div class="d-flex justify-content-between align-items-start gap-2">' +
      '<div><div class="fw-semibold">' + esc(item.label) + '</div><div class="small text-secondary">' + esc(item.detail) + '</div></div>' +
      '<span class="badge ' + badgeClass + '">' + esc(item.status) + '</span>' +
      '</div></div>';
  }).join('');

  pathsContainer.innerHTML = Object.entries(selectedScenario.paths || {}).map(([key, value]) => (
    '<div class="col-md-6"><div class="border rounded p-2 h-100 bg-light">' +
    '<div class="small text-secondary">' + esc(key) + '</div>' +
    '<div class="fw-semibold small">' + esc(String(value || '-')) + '</div>' +
    '</div></div>'
  )).join('');

  commandsList.innerHTML = (Array.isArray(selectedScenario.commands) ? selectedScenario.commands : []).map((command) => (
    '<li class="mb-2"><code>' + esc(command) + '</code></li>'
  )).join('');

  envTemplate.textContent = String(selectedScenario.envTemplate || '');
}

function renderHeaderMenuState() {
  const summary = state.installerSummary;
  const authConfigured = !!summary?.authConfigured;
  const subtitle = document.getElementById('agent-header-menu-subtitle');
  const authHint = document.getElementById('agent-menu-auth-hint');
  const authPanel = document.getElementById('agent-menu-auth-panel');
  const logoutButton = document.getElementById('logout-admin');

  if (subtitle) {
    subtitle.textContent = authConfigured
      ? 'Navigation, Arbeitsbereich und Login-Session'
      : 'Navigation und Arbeitsbereich';
  }

  if (authHint) {
    if (authConfigured) {
      authHint.textContent = 'Admin-Login ist aktiv. Die aktuelle Session kann hier beendet werden.';
      authHint.classList.remove('d-none');
    } else {
      authHint.textContent = 'Für diese Instanz ist aktuell kein UI-Login aktiv.';
      authHint.classList.remove('d-none');
    }
  }

  if (authPanel) {
    authPanel.classList.toggle('d-none', !authConfigured);
  }

  if (logoutButton) {
    logoutButton.disabled = !authConfigured;
    logoutButton.setAttribute('aria-hidden', authConfigured ? 'false' : 'true');
  }
}

function applyInstallerScenarioDefaults() {
  const summary = state.installerSummary;
  const scenarioSelect = document.getElementById('installer-scenario');
  if (!summary || !scenarioSelect) {
    return;
  }

  const scenarios = Array.isArray(summary.scenarios) ? summary.scenarios : [];
  const selectedScenario = scenarios.find((item) => item.id === scenarioSelect.value) || scenarios[0];
  if (!selectedScenario || !selectedScenario.defaults) {
    return;
  }

  const defaults = selectedScenario.defaults;
  const appDirEl = document.getElementById('installer-app-dir');
  const serviceUserEl = document.getElementById('installer-service-user');
  const serviceGroupEl = document.getElementById('installer-service-group');
  const publicHostEl = document.getElementById('installer-public-host');
  const portEl = document.getElementById('installer-port');
  const adminUsernameEl = document.getElementById('installer-admin-username');
  if (appDirEl) appDirEl.value = defaults.appDir || '';
  if (serviceUserEl) serviceUserEl.value = defaults.serviceUser || '';
  if (serviceGroupEl) serviceGroupEl.value = defaults.serviceGroup || '';
  if (publicHostEl) publicHostEl.value = defaults.publicHost || '';
  if (portEl) portEl.value = String(defaults.port || 9010);
  if (adminUsernameEl) adminUsernameEl.value = defaults.adminUsername || 'admin';
}

async function generateInstallerFilesFromUi() {
  const statusEl = document.getElementById('installer-generate-status');
  const outputEl = document.getElementById('installer-generated-files');
  const button = document.getElementById('installer-generate-files');
  const downloadLink = document.getElementById('installer-download-archive');
  if (!statusEl || !outputEl || !button) {
    return;
  }

  button.disabled = true;
  if (downloadLink) {
    downloadLink.classList.add('d-none');
    downloadLink.removeAttribute('href');
  }
  statusEl.textContent = 'Installationsdateien werden erzeugt...';
  try {
    const result = await requestJson('/api/installer/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scenarioId: document.getElementById('installer-scenario')?.value,
        appDir: document.getElementById('installer-app-dir')?.value,
        serviceUser: document.getElementById('installer-service-user')?.value,
        serviceGroup: document.getElementById('installer-service-group')?.value,
        publicHost: document.getElementById('installer-public-host')?.value,
        port: Number(document.getElementById('installer-port')?.value || '9010'),
        adminUsername: document.getElementById('installer-admin-username')?.value
      })
    });
    state.installerGeneratedFiles = Array.isArray(result.files) ? result.files : [];
    statusEl.textContent = 'Dateien erzeugt unter ' + String(result.outputDir || 'artifacts/installer/generated');
    outputEl.textContent = ['Output: ' + String(result.outputDir || ''), '', ...(state.installerGeneratedFiles || []), '', 'Archiv: ' + String(result.archiveFileName || ''), 'Install: ' + String(result.installCommand || '')].join('\n');
    if (downloadLink && result.downloadUrl) {
      downloadLink.setAttribute('href', String(result.downloadUrl));
      downloadLink.classList.remove('d-none');
    }
  } catch (error) {
    statusEl.textContent = error.message || 'Installationsdateien konnten nicht erzeugt werden';
    outputEl.textContent = 'Fehler: ' + (error.message || 'Unbekannter Fehler');
  } finally {
    button.disabled = false;
  }
}

function getTemplateHeroLabel(item) {
  if (item?.kind === 'bundle') {
    return 'Komplettset';
  }
  return item?.kind === 'schedule' ? 'Scheduler' : 'Connector';
}

function getTemplatePreviewTitle(item) {
  if (item?.kind === 'bundle') {
    return 'Sofort einsatzbereit';
  }
  if (item?.kind === 'schedule') {
    return 'Ablauf inklusive Timing';
  }
  return 'Verbindung vorkonfiguriert';
}

function isoToLocalDateTimeInput(value) {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  const localTime = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return localTime.toISOString().slice(0, 16);
}

function localDateTimeInputToIso(value) {
  if (!value) {
    return undefined;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return undefined;
  }

  return date.toISOString();
}

