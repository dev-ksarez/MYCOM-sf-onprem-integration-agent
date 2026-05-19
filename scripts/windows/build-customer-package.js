#!/usr/bin/env node

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const os = require("os");
const archiver = require("archiver");

function parseArgs(argv) {
  const args = {
    appRoot: "",
    outputDir: "",
    includeNodeModules: false,
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--include-node-modules") {
      args.includeNodeModules = true;
      continue;
    }

    if (arg === "--app-root") {
      args.appRoot = argv[i + 1] || "";
      i += 1;
      continue;
    }

    if (arg === "--output-dir") {
      args.outputDir = argv[i + 1] || "";
      i += 1;
      continue;
    }
  }

  return args;
}

async function exists(p) {
  try {
    await fsp.access(p);
    return true;
  } catch {
    return false;
  }
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function copyIfExists(src, dst, options = {}) {
  if (await exists(src)) {
    await fsp.cp(src, dst, { recursive: true, force: true, ...options });
  }
}

async function createZip(sourceDir, zipPath) {
  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", resolve);
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    archive.directory(sourceDir, path.basename(sourceDir));
    archive.finalize();
  });
}

async function main() {
  const args = parseArgs(process.argv);
  const appRoot = args.appRoot
    ? path.resolve(args.appRoot)
    : path.resolve(__dirname, "..", "..");

  const packageJsonPath = path.join(appRoot, "package.json");
  const distPath = path.join(appRoot, "dist");

  if (!(await exists(packageJsonPath))) {
    throw new Error(`package.json not found at ${packageJsonPath}`);
  }

  if (!(await exists(distPath))) {
    throw new Error(`dist directory not found at ${distPath}. Run 'npm run build' first.`);
  }

  const pkg = JSON.parse(await fsp.readFile(packageJsonPath, "utf8"));
  const version = String(pkg.version || "").trim();
  if (!version) {
    throw new Error("Version missing in package.json");
  }

  const outputDir = args.outputDir
    ? path.resolve(args.outputDir)
    : path.join(appRoot, "artifacts");
  await ensureDir(outputDir);

  const stagingRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "sf-agent-customer-package-"));
  const packageRoot = path.join(stagingRoot, `sf-onprem-integration-agent-customer-installer-${version}`);
  const stagingAppRoot = path.join(packageRoot, "sf-onprem-integration-agent");
  await ensureDir(packageRoot);
  await ensureDir(stagingAppRoot);

  console.log(`Staging package at: ${packageRoot}`);

  // Entferne große und unnötige Dateien aus dem ZIP-Archiv
  const excludePatterns = [
    path.join(stagingAppRoot, "artifacts", "file-examples"),
    path.join(stagingAppRoot, "artifacts", "dev-sandbox-schedule-examples.json"),
  ];

  for (const excludePath of excludePatterns) {
    if (await exists(excludePath)) {
      console.log(`Entferne: ${excludePath}`);
      await fsp.rm(excludePath, { recursive: true, force: true });
    }
  }

  // Sicherstellen, dass das dist-Verzeichnis explizit in das Staging-Verzeichnis kopiert wird
  const distSource = path.join(appRoot, "dist");
  const distTarget = path.join(stagingAppRoot, "dist");
  console.log(`Kopiere dist-Verzeichnis von ${distSource} nach ${distTarget}`);
  await fsp.cp(distSource, distTarget, { recursive: true });

  await fsp.cp(path.join(appRoot, "scripts"), path.join(stagingAppRoot, "scripts"), {
    recursive: true,
    force: true,
  });
  await copyIfExists(path.join(appRoot, "src", "css"), path.join(stagingAppRoot, "src", "css"));
  await copyIfExists(path.join(appRoot, "src", "public"), path.join(stagingAppRoot, "src", "public"));
  await copyIfExists(
    path.join(appRoot, "artifacts", "migrations.json"),
    path.join(stagingAppRoot, "artifacts", "migrations.json")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "schedule-health.json"),
    path.join(stagingAppRoot, "artifacts", "schedule-health.json")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "schedule-timing.json"),
    path.join(stagingAppRoot, "artifacts", "schedule-timing.json")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "sf-instances.json"),
    path.join(stagingAppRoot, "artifacts", "sf-instances.json")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "admin-users.json"),
    path.join(stagingAppRoot, "artifacts", "admin-users.json")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "templates"),
    path.join(stagingAppRoot, "artifacts", "templates")
  );
  await copyIfExists(
    path.join(appRoot, "artifacts", "file-examples"),
    path.join(stagingAppRoot, "artifacts", "file-examples")
  );
  await copyIfExists(path.join(appRoot, "migrations"), path.join(stagingAppRoot, "migrations"));
  await copyIfExists(path.join(appRoot, "salesforce"), path.join(stagingAppRoot, "salesforce"));
  await fsp.cp(path.join(appRoot, "package.json"), path.join(stagingAppRoot, "package.json"), {
    force: true,
  });
  await copyIfExists(path.join(appRoot, "package-lock.json"), path.join(stagingAppRoot, "package-lock.json"));
  await copyIfExists(path.join(appRoot, ".env.example"), path.join(stagingAppRoot, ".env.example"));
  await copyIfExists(
    path.join(appRoot, "WINDOWS_DEPLOYMENT.md"),
    path.join(stagingAppRoot, "WINDOWS_DEPLOYMENT.md")
  );
  await copyIfExists(
    path.join(appRoot, "OAUTH_ERROR_QUICK_FIX.md"),
    path.join(stagingAppRoot, "OAUTH_ERROR_QUICK_FIX.md")
  );
  await copyIfExists(
    path.join(appRoot, "SALESFORCE_OAUTH_TROUBLESHOOTING.md"),
    path.join(stagingAppRoot, "SALESFORCE_OAUTH_TROUBLESHOOTING.md")
  );
  await copyIfExists(
    path.join(appRoot, "METADATA_DEPLOYMENT_TROUBLESHOOTING.md"),
    path.join(stagingAppRoot, "METADATA_DEPLOYMENT_TROUBLESHOOTING.md")
  );
  await copyIfExists(
    path.join(appRoot, `RELEASE_NOTES_${version}.md`),
    path.join(stagingAppRoot, `RELEASE_NOTES_${version}.md`)
  );
  await copyIfExists(path.join(appRoot, "nssm.exe"), path.join(stagingAppRoot, "nssm.exe"));
  await copyIfExists(
    path.join(appRoot, "scripts", "windows", "install-customer-package.ps1"),
    path.join(packageRoot, "install-customer-package.ps1")
  );
  await copyIfExists(
    path.join(appRoot, "scripts", "windows", "install-customer-package.cmd"),
    path.join(packageRoot, "install-customer-package.cmd")
  );
  await copyIfExists(
    path.join(appRoot, "scripts", "windows", "update-existing-installation.ps1"),
    path.join(packageRoot, "update-existing-installation.ps1")
  );
  await copyIfExists(
    path.join(appRoot, "scripts", "windows", "update-existing-installation.cmd"),
    path.join(packageRoot, "update-existing-installation.cmd")
  );

  if (args.includeNodeModules) {
    const nodeModulesPath = path.join(appRoot, "node_modules");
    if (!(await exists(nodeModulesPath))) {
      throw new Error(`--include-node-modules set, but node_modules missing at ${nodeModulesPath}`);
    }

    console.log("Including node_modules in package...");
    await fsp.cp(nodeModulesPath, path.join(stagingAppRoot, "node_modules"), {
      recursive: true,
      force: true,
    });
  }

  const zipName = `sf-onprem-integration-agent-customer-installer-${version}.zip`;
  const zipPath = path.join(outputDir, zipName);

  if (await exists(zipPath)) {
    await fsp.rm(zipPath, { force: true });
  }

  await createZip(packageRoot, zipPath);
  console.log(`Package created: ${zipPath}`);

  if (!args.includeNodeModules) {
    console.log("Note: node_modules is not included. Customer must run 'npm ci --omit=dev'.");
  }
  console.log("Bundled runtime helper included: nssm.exe");
  console.log("Bootstrap launcher included: install-customer-package.cmd / .ps1");
  console.log("Update launcher included for existing Windows installations: update-existing-installation.cmd / .ps1");

  await fsp.rm(stagingRoot, { recursive: true, force: true });
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
