const fs = require("fs");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..", "..");
const specsRoot = path.join(repoRoot, "docs", "specs");
const requiredHeadings = [
  "# ",
  "## Kontext",
  "## Problem",
  "## Zielbild",
  "## Nicht-Ziele",
  "## Akzeptanzkriterien",
  "## Umsetzungsskizze",
  "## Aufgaben",
  "## Verifikation",
  "## Status"
];

function collectSpecFiles(dirPath) {
  if (!fs.existsSync(dirPath)) {
    return [];
  }

  const entries = fs.readdirSync(dirPath, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (entry.name === "_template") {
      continue;
    }

    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectSpecFiles(fullPath));
      continue;
    }

    if (entry.isFile() && entry.name === "spec.md") {
      files.push(fullPath);
    }
  }

  return files;
}

function validateSpec(filePath) {
  const content = fs.readFileSync(filePath, "utf8");
  const missing = requiredHeadings.filter((heading) => !content.includes(heading));
  const hasUncheckedTasks = /- \[ \]/.test(content);
  const hasStatusMarker = /- Status: (draft|ready|in-progress|done)/.test(content);
  const issues = [];

  if (missing.length > 0) {
    issues.push(`fehlende Sektionen: ${missing.join(", ")}`);
  }

  if (!hasStatusMarker) {
    issues.push("Status-Markierung fehlt oder ist ungueltig");
  }

  return {
    filePath,
    issues,
    hasUncheckedTasks
  };
}

function main() {
  const specFiles = collectSpecFiles(specsRoot);
  if (specFiles.length === 0) {
    console.log("Keine Specs gefunden. Lege zuerst mit 'npm run spec:new -- \"Titel\"' eine Spec an.");
    process.exit(0);
  }

  const results = specFiles.map(validateSpec);
  const invalid = results.filter((result) => result.issues.length > 0);

  for (const result of results) {
    const relativePath = path.relative(repoRoot, result.filePath);
    if (result.issues.length === 0) {
      const taskState = result.hasUncheckedTasks ? "offene Aufgaben" : "keine offenen Aufgaben";
      console.log(`OK  ${relativePath} (${taskState})`);
      continue;
    }

    console.log(`FEHLER  ${relativePath}`);
    for (const issue of result.issues) {
      console.log(`  - ${issue}`);
    }
  }

  process.exit(invalid.length === 0 ? 0 : 1);
}

main();