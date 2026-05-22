#!/usr/bin/env node
/**
 * Splits the extracted admin-ui JavaScript into sequential blocks.
 * Each block is a contiguous range of lines from _extracted-full.js.
 */
const fs = require("fs");
const path = require("path");

const clientDir = path.resolve(__dirname, "..", "src", "client");
const fullFile = path.join(clientDir, "_extracted-full.js");
const content = fs.readFileSync(fullFile, "utf8");
const lines = content.split("\n");
const totalLines = lines.length;

console.log(`Source: ${totalLines} lines\n`);

// Find line number (0-indexed) of a top-level declaration by its identifier
// afterLine: only find declarations after this line number (0-indexed)
function findLine(id, afterLine = -1) {
  const patterns = [
    new RegExp(`^      (?:async )?function ${id}\\b`),
    new RegExp(`^      (?:const|let|var) ${id}\\b`),
  ];
  for (let i = afterLine + 1; i < totalLines; i++) {
    for (const p of patterns) {
      if (p.test(lines[i])) return i;
    }
  }
  console.warn(`  WARNING: '${id}' (after L${afterLine + 1}) not found!`);
  return -1;
}

// Module definitions using explicit line numbers where ambiguity exists
// Format: [name, startLine (0-indexed)]
// endLine is automatically the startLine of the next module
const cuts = [
  ["admin-ui-state", 0],
  ["admin-ui-migration-helpers", findLine("getMigLoginUrlForEnvironment")],
  ["admin-ui-core", findLine("logsChart")],
  ["admin-ui-utilities", findLine("renderScheduleConnectorOptions")],
  ["admin-ui-scheduler-mapping", findLine("isSchedulerMssqlUpsertSelection")],
  ["admin-ui-admin", findLine("renderAdminUsers")],
  ["admin-ui-charts", findLine("renderLogChart")],
  ["admin-ui-wizards", findLine("getConnectorWizardTotalSteps")],
  ["admin-ui-monitor", findLine("renderSchedulerConnectorFilterOptions")],
  ["admin-ui-schedule-modal", findLine("openScheduleModal")],
  ["admin-ui-settings", findLine("loadInstances")],
  ["admin-ui-init", findLine("AUTO_REFRESH_INTERVAL_MS")],
  ["admin-ui-scheduler-fields", findLine("loadMappingFields")],
  ["admin-ui-migration-wizard", findLine("migUuidV4")],
  ["admin-ui-migration-ui", findLine("getMigrationInstanceStatusMeta")],
  ["admin-ui-migration-planning", findLine("renderMigDependencies")],
  // These are from the sub-modules appended at the end
  // renderConnectors (L16608) from connector-ui-module.ts
  ["admin-ui-connector", 16607],
  // renderSchedules (L16819) from scheduler-ui-module.ts (second occurrence)
  ["admin-ui-scheduler-render", 16818],
  // Migration runtime modules from migration-ui-module.ts
  ["admin-ui-migration-runtime", findLine("renderMigPreflightWarnings", 17000)],
];

// Build modules with end lines
const modules = [];
for (let i = 0; i < cuts.length; i++) {
  const [name, startLine] = cuts[i];
  const endLine = (i + 1 < cuts.length) ? cuts[i + 1][1] : totalLines;
  modules.push({ name, startLine, endLine });
}

// Write modules
let totalWritten = 0;
for (const mod of modules) {
  const moduleLines = lines.slice(mod.startLine, mod.endLine);

  // Remove common 6-space indentation
  const dedented = moduleLines.map(line => {
    if (line.startsWith("      ")) return line.substring(6);
    if (line.trim() === "") return "";
    return line;
  });

  const header =
    `// ──────────────────────────────────────────────────────────────────────\n` +
    `// Module: ${mod.name}\n` +
    `// Source lines: ${mod.startLine + 1}–${mod.endLine}\n` +
    `// ──────────────────────────────────────────────────────────────────────\n` +
    `\n`;

  const moduleContent = header + dedented.join("\n") + "\n";
  const outPath = path.join(clientDir, mod.name + ".js");
  fs.writeFileSync(outPath, moduleContent, "utf8");
  const lineCount = dedented.length;
  totalWritten += lineCount;
  console.log(`  ✓ ${mod.name.padEnd(30)} ${String(lineCount).padStart(5)} lines  (L${mod.startLine + 1}–L${mod.endLine})`);
}

console.log(`\n  Total: ${totalWritten} lines across ${modules.length} modules`);
console.log(`  Source: ${totalLines} lines`);

// Verify contiguity
let lastEnd = 0;
for (const mod of modules) {
  if (mod.startLine !== lastEnd) {
    console.warn(`  ⚠ Gap: lines ${lastEnd + 1}–${mod.startLine}`);
  }
  lastEnd = mod.endLine;
}
if (lastEnd !== totalLines) {
  console.warn(`  ⚠ Trailing: lines ${lastEnd + 1}–${totalLines}`);
}
