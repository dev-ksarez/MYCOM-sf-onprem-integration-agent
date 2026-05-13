export interface AppModuleDefinition {
  id: string;
  label: string;
  icon: string;
  tabId?: string;
  modulePath?: string;
  sidebarButtonId?: string;
  menuButtonId?: string;
  hiddenInSidebar?: boolean;
  showInstallUpdateBullet?: boolean;
}

const REGISTERED_APP_MODULES: AppModuleDefinition[] = [
  { id: "overview", label: "Start", icon: "▦", tabId: "tab-overview" },
  { id: "installer", label: "Setup", icon: "⚙", tabId: "tab-installer", sidebarButtonId: "tab-installer-trigger", hiddenInSidebar: true, showInstallUpdateBullet: true },
  { id: "schedulers", label: "Sched.", icon: "◷", tabId: "tab-schedulers" },
  { id: "connectors", label: "Conn.", icon: "◫", tabId: "tab-connectors" },
  { id: "ai-assistant", label: "KI", icon: "⚡", tabId: "tab-ai-assistant" },
  { id: "monitor", label: "Monitor", icon: "◉", tabId: "tab-monitor" },
  { id: "migration", label: "Mig.", icon: "⊞", tabId: "tab-migration" },
  { id: "admin", label: "Admin", icon: "☷", sidebarButtonId: "open-admin-modal-sidebar", menuButtonId: "open-admin-modal-menu" }
];

export function registerAppModule(module: AppModuleDefinition): void {
  const existingIndex = REGISTERED_APP_MODULES.findIndex((item) => item.id === module.id);
  if (existingIndex >= 0) {
    REGISTERED_APP_MODULES[existingIndex] = module;
    return;
  }
  REGISTERED_APP_MODULES.push(module);
}

export function listAppModules(): AppModuleDefinition[] {
  return REGISTERED_APP_MODULES.map((module) => ({ ...module }));
}

export function renderSidebarModuleNavigation(modules: AppModuleDefinition[] = listAppModules()): string {
  return modules.map((module, index) => {
    const itemClasses = module.hiddenInSidebar ? "nav-item d-none" : "nav-item";
    const activeClass = index === 0 && module.tabId ? " active" : "";
    const idAttribute = module.sidebarButtonId ? ` id="${module.sidebarButtonId}"` : "";
    const tabAttributes = module.tabId
      ? ` data-bs-toggle="tab" data-bs-target="#${module.tabId}"`
      : "";
    return `          <li class="${itemClasses}" role="presentation"><button${idAttribute} class="nav-link${activeClass}"${tabAttributes} type="button"><span class="agent-tab-icon">${module.icon}</span>${module.label}</button></li>`;
  }).join("\n");
}

export function renderMenuModuleNavigation(modules: AppModuleDefinition[] = listAppModules()): string {
  return modules.map((module) => {
    const idAttribute = module.menuButtonId ? ` id="${module.menuButtonId}"` : "";
    const tabAttribute = module.tabId ? ` data-menu-tab="#${module.tabId}"` : "";
    const installBullet = module.showInstallUpdateBullet
      ? '<span id="agent-install-update-bullet" class="agent-update-bullet agent-update-bullet-inline d-none" aria-hidden="true"></span>'
      : "";
    return `                  <button type="button"${idAttribute} class="agent-menu-tab"${tabAttribute} data-bs-dismiss="offcanvas"><span class="agent-menu-tab-icon" aria-hidden="true">${module.icon}</span><span>${module.label}</span>${installBullet}</button>`;
  }).join("\n");
}
