#!/usr/bin/env node
/**
 * Build script for admin-ui client-side JavaScript.
 * Concatenates all modules from src/client/ into dist/public/admin-ui.js
 * in the correct dependency order.
 */
const fs = require("fs");
const path = require("path");

const clientDir = path.resolve(__dirname, "..", "src", "client");
const outDir = path.resolve(__dirname, "..", "dist", "public");
const outFile = path.join(outDir, "admin-ui.js");

// Module loading order — dependencies first, init last
const moduleOrder = [
  "admin-ui-state",
  "admin-ui-migration-helpers",
  "admin-ui-core",
  "admin-ui-utilities",
  "admin-ui-scheduler-mapping",
  "admin-ui-admin",
  "admin-ui-charts",
  "admin-ui-wizards",
  "admin-ui-monitor",
  "admin-ui-schedule-modal",
  "admin-ui-settings",
  "admin-ui-init",
  "admin-ui-scheduler-fields",
  "admin-ui-migration-wizard",
  "admin-ui-migration-ui",
  "admin-ui-migration-planning",
  "admin-ui-connector",
  "admin-ui-scheduler-render",
  "admin-ui-migration-runtime",
];

fs.mkdirSync(outDir, { recursive: true });

const parts = [];
let totalLines = 0;

for (const moduleName of moduleOrder) {
  const filePath = path.join(clientDir, moduleName + ".js");
  if (!fs.existsSync(filePath)) {
    console.error(`ERROR: Module not found: ${filePath}`);
    process.exit(1);
  }
  const content = fs.readFileSync(filePath, "utf8");
  parts.push(content);
  const lineCount = content.split("\n").length;
  totalLines += lineCount;
}

const bundle = parts.join("\n");
fs.writeFileSync(outFile, bundle, "utf8");

const sizeKb = Math.round(Buffer.byteLength(bundle, "utf8") / 1024);
console.log(`admin-ui.js built: ${totalLines} lines, ${sizeKb} KB → ${outFile}`);
