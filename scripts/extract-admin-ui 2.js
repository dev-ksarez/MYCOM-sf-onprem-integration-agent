#!/usr/bin/env node
/**
 * Extracts the full client-side JavaScript from renderAdminUiScript()
 * into a standalone .js file for subsequent modular splitting.
 */
const fs = require("fs");
const path = require("path");

const { renderAdminUiScript } = require("../dist/server/admin-ui-script");

const output = renderAdminUiScript();
const outDir = path.resolve(__dirname, "..", "src", "client");
fs.mkdirSync(outDir, { recursive: true });

const outFile = path.join(outDir, "_extracted-full.js");
fs.writeFileSync(outFile, output, "utf8");

const lines = output.split("\n").length;
const sizeKb = Math.round(Buffer.byteLength(output, "utf8") / 1024);
console.log(`Extracted ${lines} lines (${sizeKb} KB) → ${outFile}`);
