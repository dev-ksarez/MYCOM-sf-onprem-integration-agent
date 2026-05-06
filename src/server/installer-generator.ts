import { createWriteStream } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import archiver from "archiver";
import { AdminAuthConfig, getAdminAuthConfig } from "./admin-auth";

export const INSTALLER_OUTPUT_DIR = path.resolve(process.cwd(), "artifacts/installer/generated");

export type InstallerScenarioId = "windows-service-local" | "linux-ubuntu-local" | "linux-public-secure";

interface InstallerScenarioSummary {
  id: InstallerScenarioId;
  label: string;
  networkScope: string;
  description: string;
  generatedFilesLabel: string;
  generatorMode: "windows" | "linux-local" | "linux-public";
  defaults: {
    appDir: string;
    serviceUser: string;
    serviceGroup: string;
    publicHost: string;
    port: number;
    adminUsername: string;
  };
  paths: Record<string, string>;
  commands: string[];
  checks: Array<{ label: string; status: "ready" | "in-progress" | "missing"; detail: string }>;
  envTemplate: string;
}

export interface InstallerGenerationInput {
  scenarioId?: InstallerScenarioId;
  appDir?: string;
  serviceUser?: string;
  serviceGroup?: string;
  publicHost?: string;
  port?: number;
  adminUsername?: string;
}

function buildInstallerEnvTemplate(options: {
  nodeEnv?: string;
  webUiHost: string;
  webUiPort: number;
  logLevel?: string;
  adminUsername: string;
  schedulerIntervalMs?: number;
}): string {
  return [
    `NODE_ENV=${options.nodeEnv || "production"}`,
    "WEB_UI_ENABLED=1",
    `WEB_UI_HOST=${options.webUiHost}`,
    `WEB_UI_PORT=${options.webUiPort}`,
    `LOG_LEVEL=${options.logLevel || "info"}`,
    `ADMIN_UI_USERNAME=${options.adminUsername}`,
    "ADMIN_UI_PASSWORD=<starkes-passwort>",
    `SCHEDULER_INTERVAL_MS=${options.schedulerIntervalMs || 60000}`
  ].join("\n");
}

function getInstallerScenarios(adminAuth: AdminAuthConfig): InstallerScenarioSummary[] {
  const defaultAdminUsername = adminAuth.users[0]?.username || "admin";
  return [
    {
      id: "windows-service-local",
      label: "Windows Server / Dienst",
      networkScope: "Lokales Netz",
      description: "Windows-Server im internen Netz. Der Agent läuft als Windows-Dienst, die Web UI ist nur im LAN oder über VPN vorgesehen.",
      generatedFilesLabel: "Windows-Installationshinweise und .env-Vorlage",
      generatorMode: "windows",
      defaults: {
        appDir: "C:\\apps\\sf-onprem-integration-agent",
        serviceUser: "SfOnpremIntegrationAgent",
        serviceGroup: "SfOnpremIntegrationAgent",
        publicHost: "windows-agent.intern.local",
        port: 9010,
        adminUsername: defaultAdminUsername
      },
      paths: {
        appDir: "C:\\apps\\sf-onprem-integration-agent",
        envFile: "C:\\apps\\sf-onprem-integration-agent\\.env",
        logDir: "C:\\apps\\sf-onprem-integration-agent\\logs",
        dataDir: "C:\\apps\\sf-onprem-integration-agent\\artifacts",
        fileDrop: "C:\\apps\\sf-onprem-integration-agent\\artifacts\\files"
      },
      commands: [
        "npm run build",
        "npm run win:install-service -- -AppRoot \"C:\\apps\\sf-onprem-integration-agent\" -InstallRoles agent,web,updater",
        "powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile agent-host",
        "powershell -File scripts/windows/install-windows-agent.ps1 -InstallProfile web-host",
        "Get-Service SfOnpremIntegrationAgent, SfOnpremIntegrationWeb, SfOnpremIntegrationUpdater"
      ],
      checks: [
        { label: "Bereitstellung", status: "ready", detail: "Runbook und Dienstskripte für Windows sind vorhanden." },
        { label: "Netzwerkgrenze", status: "ready", detail: "Szenario ist für internes LAN oder VPN gedacht, nicht für direkte Internet-Exponierung." },
        { label: "Admin-Login", status: adminAuth.enabled ? "ready" : "missing", detail: adminAuth.enabled ? "ADMIN_UI_USERNAME und ADMIN_UI_PASSWORD gesetzt" : "Admin-Credentials für die Web UI fehlen noch" },
        { label: "Updates", status: "ready", detail: "Windows-Updater und Rollback-Pfad sind dokumentiert." }
      ],
      envTemplate: buildInstallerEnvTemplate({ webUiHost: "0.0.0.0", webUiPort: 9010, adminUsername: defaultAdminUsername })
    },
    {
      id: "linux-ubuntu-local",
      label: "Linux (Ubuntu)",
      networkScope: "Lokales Netz",
      description: "Ubuntu-Server im lokalen Netz. systemd wird genutzt, die Web UI kann intern direkt oder über internen Reverse Proxy bereitgestellt werden.",
      generatedFilesLabel: "Ubuntu-LAN-Dateien für systemd und .env",
      generatorMode: "linux-local",
      defaults: {
        appDir: "/opt/sf-integration-agent",
        serviceUser: "sfagent",
        serviceGroup: "sfagent",
        publicHost: "ubuntu-agent.intern.local",
        port: 9010,
        adminUsername: defaultAdminUsername
      },
      paths: {
        appDir: "/opt/sf-integration-agent",
        envFile: "/etc/sf-integration-agent/agent.env",
        logDir: "/var/log/sf-integration-agent",
        dataDir: "/var/lib/sf-integration-agent",
        fileDrop: "/opt/sf-integration-agent/artifacts/files"
      },
      commands: [
        "sudo bash scripts/linux/install-linux-agent.sh --app-dir /opt/sf-integration-agent --service-user sfagent --service-group sfagent --port 9010 --public-host ubuntu-agent.intern.local --roles agent,web,updater",
        "sudo bash scripts/linux/install-linux-agent.sh --app-dir /opt/sf-integration-agent --service-user sfagent --service-group sfagent --port 9010 --roles agent,updater",
        "sudo bash scripts/linux/install-linux-agent.sh --app-dir /opt/sf-integration-agent --service-user sfagent --service-group sfagent --port 9010 --roles web",
        "sudo systemctl enable --now sf-integration-agent.service sf-integration-web.service sf-integration-updater.service",
        "sudo bash scripts/linux/setup-sftp-user.sh --app-dir /opt/sf-integration-agent --service-user sfagent --sftp-user sfagentdrop",
        "sudo systemctl status sf-integration-agent.service sf-integration-web.service sf-integration-updater.service"
      ],
      checks: [
        { label: "Bereitstellung", status: "ready", detail: "Ubuntu-Setup nutzt systemd und lokale Dateipfade." },
        { label: "Netzwerkgrenze", status: "ready", detail: "Szenario ist für internes LAN oder VPN ausgelegt; HTTPS ist optional über internen Proxy." },
        { label: "Datei-Connectoren", status: "ready", detail: "Optionaler SFTP-Drop für inbound, outbound und archive ist vorgesehen." },
        { label: "Admin-Login", status: adminAuth.enabled ? "ready" : "missing", detail: adminAuth.enabled ? "ADMIN_UI_USERNAME und ADMIN_UI_PASSWORD gesetzt" : "Admin-Credentials für die Web UI fehlen noch" }
      ],
      envTemplate: buildInstallerEnvTemplate({ webUiHost: "0.0.0.0", webUiPort: 9010, adminUsername: defaultAdminUsername })
    },
    {
      id: "linux-public-secure",
      label: "Öffentlicher Linux Server",
      networkScope: "Öffentlich erreichbar",
      description: "Öffentliche Linux-VM mit Reverse Proxy, TLS, Login-Schutz und abgesichertem SFTP-Zugang. Der Node-Prozess bleibt auf localhost gebunden.",
      generatedFilesLabel: "Harte Linux-Internet-Dateien für systemd, nginx und .env",
      generatorMode: "linux-public",
      defaults: {
        appDir: "/opt/sf-integration-agent",
        serviceUser: "sfagent",
        serviceGroup: "sfagent",
        publicHost: "agent.example.com",
        port: 9010,
        adminUsername: defaultAdminUsername
      },
      paths: {
        appDir: "/opt/sf-integration-agent",
        envFile: "/etc/sf-integration-agent/agent.env",
        logDir: "/var/log/sf-integration-agent",
        dataDir: "/var/lib/sf-integration-agent",
        sftpDropRoot: "/var/lib/sf-integration-agent/sftp/<user>/drop"
      },
      commands: [
        "sudo bash scripts/linux/install-linux-agent.sh --app-dir /opt/sf-integration-agent --service-user sfagent --service-group sfagent --port 9010 --public-host agent.example.com --roles agent,web,updater",
        "sudo bash scripts/linux/setup-sftp-user.sh --app-dir /opt/sf-integration-agent --service-user sfagent --sftp-user sfagentdrop",
        "sudo systemctl enable --now sf-integration-agent.service sf-integration-web.service sf-integration-updater.service",
        "sudo nginx -t && sudo systemctl reload nginx"
      ],
      checks: [
        { label: "Admin-Login", status: adminAuth.enabled ? "ready" : "missing", detail: adminAuth.enabled ? "ADMIN_UI_USERNAME und ADMIN_UI_PASSWORD gesetzt" : "Vor öffentlichem Betrieb müssen Admin-Credentials gesetzt werden" },
        { label: "CSRF/Origin Schutz", status: "ready", detail: "Mutierende Requests verlangen X-CSRF-Token und prüfen die Request-Origin." },
        { label: "Reverse Proxy + TLS", status: "ready", detail: "nginx- und systemd-Artefakte liegen im Repo; TLS wird am Proxy terminiert." },
        { label: "Datei-Connector SFTP", status: "ready", detail: "SFTP-Drop für inbound, outbound und archive ist vorgesehen." }
      ],
      envTemplate: buildInstallerEnvTemplate({ webUiHost: "127.0.0.1", webUiPort: 9010, adminUsername: defaultAdminUsername })
    }
  ];
}

function getInstallerScenarioById(id: string | undefined, scenarios: InstallerScenarioSummary[]): InstallerScenarioSummary {
  return scenarios.find((scenario) => scenario.id === id) || scenarios[0];
}

export function getInstallerSummary() {
  const adminAuth = getAdminAuthConfig();
  const scenarios = getInstallerScenarios(adminAuth);

  return {
    mode: adminAuth.enabled ? "secured" : "needs-admin-auth",
    authConfigured: adminAuth.enabled,
    csrfProtectionEnabled: true,
    originProtectionEnabled: true,
    nodeEnv: String(process.env.NODE_ENV || "development"),
    defaultScenarioId: "windows-service-local" as InstallerScenarioId,
    scenarios,
    fileConnectorDefaults: {
      basePath: "artifacts/files",
      importPath: "inbound",
      exportPath: "outbound",
      archivePath: "archive"
    },
    checks: [
      { label: "Windows Szenario", status: "ready", detail: "Windows Server als Dienst im lokalen Netz ist abgedeckt." },
      { label: "Ubuntu Szenario", status: "ready", detail: "Ubuntu im lokalen Netz ist als separater Setup-Pfad abgedeckt." },
      { label: "Öffentlicher Linux Server", status: "ready", detail: "Öffentliche Linux-VM mit Reverse Proxy und TLS ist als eigener Pfad abgedeckt." },
      { label: "Webbasierter Installer", status: "in-progress", detail: "Der Installer kann jetzt zwischen drei Setup-Szenarien umschalten und passende Artefakte erzeugen." }
    ],
    dockerTest: {
      image: "sf-agent-ubuntu-test",
      dockerfile: "Dockerfile.ubuntu-test",
      verifyCommand: "docker build -f Dockerfile.ubuntu-test -t sf-agent-ubuntu-test . && docker run --rm sf-agent-ubuntu-test"
    }
  };
}

async function createInstallerArchive(outputDir: string, archiveBaseName: string): Promise<{ archivePath: string; archiveFileName: string }> {
  const archiveFileName = `${archiveBaseName}.zip`;
  const archivePath = path.join(outputDir, archiveFileName);

  await new Promise<void>((resolve, reject) => {
    const output = createWriteStream(archivePath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", () => resolve());
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    archive.directory(outputDir, false, (entry) => (entry.name === archiveFileName ? false : entry));
    void archive.finalize();
  });

  return { archivePath, archiveFileName };
}

function sanitizeInstallerGenerationInput(input: InstallerGenerationInput | undefined): Required<InstallerGenerationInput> {
  const scenarioId = (String(input?.scenarioId || "windows-service-local").trim() || "windows-service-local") as InstallerScenarioId;
  const scenarios = getInstallerScenarios(getAdminAuthConfig());
  const scenario = getInstallerScenarioById(scenarioId, scenarios);
  const appDir = String(input?.appDir || scenario.defaults.appDir).trim() || scenario.defaults.appDir;
  const serviceUser = String(input?.serviceUser || scenario.defaults.serviceUser).trim() || scenario.defaults.serviceUser;
  const serviceGroup = String(input?.serviceGroup || scenario.defaults.serviceGroup).trim() || scenario.defaults.serviceGroup;
  const publicHost = String(input?.publicHost || scenario.defaults.publicHost).trim() || scenario.defaults.publicHost;
  const adminUsername = String(input?.adminUsername || scenario.defaults.adminUsername).trim() || scenario.defaults.adminUsername;
  const parsedPort = Number(input?.port);
  const port = Number.isFinite(parsedPort) && parsedPort > 0 ? parsedPort : scenario.defaults.port;
  return { scenarioId: scenario.id, appDir, serviceUser, serviceGroup, publicHost, port, adminUsername };
}

export async function generateInstallerFiles(input: InstallerGenerationInput | undefined): Promise<{ outputDir: string; files: string[]; installCommand: string; archiveFileName: string; downloadUrl: string }> {
  const sanitized = sanitizeInstallerGenerationInput(input);
  const scenarios = getInstallerScenarios(getAdminAuthConfig());
  const scenario = getInstallerScenarioById(sanitized.scenarioId, scenarios);
  const outputDir = path.join(INSTALLER_OUTPUT_DIR, new Date().toISOString().replace(/[:.]/g, "-"));
  const envFile = "/etc/sf-integration-agent/agent.env";
  const logDir = "/var/log/sf-integration-agent";
  const envTemplate = buildInstallerEnvTemplate({
    webUiHost: scenario.generatorMode === "linux-public" ? "127.0.0.1" : "0.0.0.0",
    webUiPort: sanitized.port,
    adminUsername: sanitized.adminUsername
  }) + "\n";

  await fs.mkdir(outputDir, { recursive: true });
  let files: string[] = [];
  let installCommand = "";

  if (scenario.generatorMode === "windows") {
    const installNotes = [
      "# Windows Server / Dienst Installer Preview",
      "",
      `Szenario: ${scenario.label}`,
      `Netzwerk: ${scenario.networkScope}`,
      `App Dir: ${sanitized.appDir}`,
      `Web UI Port: ${sanitized.port}`,
      `Admin Username: ${sanitized.adminUsername}`,
      "",
      "Next steps:",
      `1. Copy .env.example to ${path.join(sanitized.appDir, ".env")}`,
      `2. All-in-one: npm run win:install-service -- -AppRoot \"${sanitized.appDir}\" -InstallRoles agent,web,updater`,
      `3. Split hosts: powershell -File scripts/windows/install-windows-agent.ps1 -AppRoot \"${sanitized.appDir}\" -InstallProfile agent-host OR web-host`,
      "4. Verify the services with Get-Service SfOnpremIntegrationAgent, SfOnpremIntegrationWeb, SfOnpremIntegrationUpdater"
    ].join("\n") + "\n";
    const commandNotes = [
      `npm run win:install-service -- -AppRoot \"${sanitized.appDir}\" -InstallRoles agent,web,updater`,
      `powershell -File scripts/windows/install-windows-agent.ps1 -AppRoot \"${sanitized.appDir}\" -InstallProfile agent-host`,
      `powershell -File scripts/windows/install-windows-agent.ps1 -AppRoot \"${sanitized.appDir}\" -InstallProfile web-host`,
      "Get-Service SfOnpremIntegrationAgent, SfOnpremIntegrationWeb, SfOnpremIntegrationUpdater"
    ].join("\n") + "\n";
    files = [
      path.join(outputDir, ".env.example"),
      path.join(outputDir, "WINDOWS-INSTALL-README.txt"),
      path.join(outputDir, "WINDOWS-INSTALL-COMMANDS.txt")
    ];
    await Promise.all([
      fs.writeFile(files[0], envTemplate, "utf-8"),
      fs.writeFile(files[1], installNotes, "utf-8"),
      fs.writeFile(files[2], commandNotes, "utf-8")
    ]);
    installCommand = `npm run win:install-service -- -AppRoot \"${sanitized.appDir}\"`;
  } else {
    const renderServiceTemplate = async (templateName: string): Promise<string> => {
      const template = await fs.readFile(path.resolve(process.cwd(), "scripts/linux", templateName), "utf-8");
      return template
        .replaceAll("__APP_DIR__", sanitized.appDir)
        .replaceAll("__SERVICE_USER__", sanitized.serviceUser)
        .replaceAll("__SERVICE_GROUP__", sanitized.serviceGroup)
        .replaceAll("__ENV_FILE__", envFile)
        .replaceAll("__LOG_DIR__", logDir);
    };
    installCommand = `sudo bash scripts/linux/install-linux-agent.sh --app-dir ${sanitized.appDir} --service-user ${sanitized.serviceUser} --service-group ${sanitized.serviceGroup} --port ${sanitized.port} --public-host ${sanitized.publicHost}`;
    const installNotes = [
      scenario.generatorMode === "linux-public" ? "# Öffentlicher Linux Installer Preview" : "# Ubuntu LAN Installer Preview",
      "",
      `Szenario: ${scenario.label}`,
      `Netzwerk: ${scenario.networkScope}`,
      `App Dir: ${sanitized.appDir}`,
      `Service User: ${sanitized.serviceUser}`,
      `Service Group: ${sanitized.serviceGroup}`,
      `Host: ${sanitized.publicHost}`,
      `Port: ${sanitized.port}`,
      "",
      "Next steps:",
      `1. Copy agent.env.example to ${envFile}`,
      "2. Review the generated systemd files",
      `3. Run: ${installCommand}`,
      `4. Optional SFTP: sudo bash scripts/linux/setup-sftp-user.sh --app-dir ${sanitized.appDir} --service-user ${sanitized.serviceUser} --sftp-user ${sanitized.serviceUser}drop`
    ];

    files = [
      path.join(outputDir, "agent.env.example"),
      path.join(outputDir, "sf-integration-agent.service"),
      path.join(outputDir, "sf-integration-web.service"),
      path.join(outputDir, "sf-integration-updater.service")
    ];
    const writes: Array<Promise<void>> = [
      fs.writeFile(files[0], envTemplate, "utf-8"),
      renderServiceTemplate("sf-integration-agent.service").then((content) => fs.writeFile(files[1], content, "utf-8")),
      renderServiceTemplate("sf-integration-web.service").then((content) => fs.writeFile(files[2], content, "utf-8")),
      renderServiceTemplate("sf-integration-updater.service").then((content) => fs.writeFile(files[3], content, "utf-8"))
    ];

    if (scenario.generatorMode === "linux-public") {
      const nginxTemplate = await fs.readFile(path.resolve(process.cwd(), "scripts/linux/nginx-sf-integration-agent.conf"), "utf-8");
      const renderedNginx = nginxTemplate
        .replaceAll("__PUBLIC_HOST__", sanitized.publicHost)
        .replaceAll("__APP_PORT__", String(sanitized.port));
      files.push(path.join(outputDir, "nginx-sf-integration-agent.conf"));
      writes.push(fs.writeFile(files[4], renderedNginx, "utf-8"));
      installNotes.splice(10, 0, "TLS/Reverse Proxy: nginx-Konfiguration liegt für das öffentliche Szenario bei.");
    }

    files.push(path.join(outputDir, scenario.generatorMode === "linux-public" ? "PUBLIC-LINUX-README.txt" : "UBUNTU-LAN-README.txt"));
    writes.push(fs.writeFile(files[files.length - 1], installNotes.join("\n") + "\n", "utf-8"));
    await Promise.all(writes);
  }

  const archiveBaseName = `installer-${scenario.id}-${path.basename(outputDir)}`;
  const { archiveFileName } = await createInstallerArchive(outputDir, archiveBaseName);

  return {
    outputDir,
    files,
    installCommand,
    archiveFileName,
    downloadUrl: `/api/installer/archive?dir=${encodeURIComponent(path.basename(outputDir))}&file=${encodeURIComponent(archiveFileName)}`
  };
}
