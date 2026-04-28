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

async function copyIfExists(src, dst) {
  if (await exists(src)) {
    await fsp.cp(src, dst, { recursive: true, force: true });
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
  const stagingAppRoot = path.join(stagingRoot, "sf-onprem-integration-agent");
  await ensureDir(stagingAppRoot);

  console.log(`Staging package at: ${stagingAppRoot}`);

  await fsp.cp(path.join(appRoot, "dist"), path.join(stagingAppRoot, "dist"), {
    recursive: true,
    force: true,
  });
  await fsp.cp(path.join(appRoot, "scripts"), path.join(stagingAppRoot, "scripts"), {
    recursive: true,
    force: true,
  });
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

  await createZip(stagingAppRoot, zipPath);
  console.log(`Package created: ${zipPath}`);

  if (!args.includeNodeModules) {
    console.log("Note: node_modules is not included. Customer must run 'npm ci --omit=dev'.");
  }

  await fsp.rm(stagingRoot, { recursive: true, force: true });
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
