import fs from "node:fs/promises";
import path from "node:path";
import XLSX from "xlsx";
import { ConnectorConfig } from "../clients/salesforce/salesforce-client";

export type FileDataFormat = "csv" | "excel" | "json";

export interface FileTransferDefinition {
  fileName?: string;
  filePath?: string;
  format?: FileDataFormat;
  charset?: string;
  delimiter?: string;
  hasHeader?: boolean;
  writeHeader?: boolean;
  sheetName?: string;
  jsonPath?: string;
}

export interface ParsedFilePayload {
  format: FileDataFormat;
  headers: string[];
  rows: Record<string, unknown>[];
  charset: string;
  filePath: string;
  fileName: string;
}

export interface ParsedUploadPayload {
  format: FileDataFormat;
  headers: string[];
  charset: string;
  delimiter: string;
}

interface FileConnectorRuntimeConfig {
  basePath: string;
  importPath: string;
  exportPath: string;
  archivePath: string;
  defaultCharset: string;
  defaultDelimiter: string;
  archiveOnRead: boolean;
  archiveOnWrite: boolean;
}

function resolveBoolean(value: unknown, defaultValue: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return true;
    }
    if (normalized === "false") {
      return false;
    }
  }

  return defaultValue;
}

function detectFormatByName(fileName: string): FileDataFormat {
  const normalized = String(fileName || "").trim().toLowerCase();
  if (normalized.endsWith(".json")) {
    return "json";
  }
  if (normalized.endsWith(".xlsx") || normalized.endsWith(".xls")) {
    return "excel";
  }
  return "csv";
}

function getByPath(value: unknown, rawPath: string): unknown {
  const pathTokens = String(rawPath || "")
    .split(".")
    .map((token) => token.trim())
    .filter(Boolean);

  let current: unknown = value;
  for (const token of pathTokens) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (Array.isArray(current)) {
      const index = Number(token);
      if (Number.isNaN(index) || index < 0 || index >= current.length) {
        return undefined;
      }
      current = current[index];
      continue;
    }

    if (typeof current === "object") {
      current = (current as Record<string, unknown>)[token];
      continue;
    }

    return undefined;
  }

  return current;
}

function normalizeJsonRows(value: unknown): Record<string, unknown>[] {
  let rawRows: unknown[];
  if (Array.isArray(value)) {
    rawRows = value;
  } else if (value && typeof value === "object") {
    rawRows = [value];
  } else {
    throw new Error("JSON-Datei enthaelt keine gueltigen Datensaetze");
  }

  return rawRows.map((entry) => {
    if (entry && typeof entry === "object" && !Array.isArray(entry)) {
      return entry as Record<string, unknown>;
    }
    return { value: entry };
  });
}

function parseDefinition(rawDefinition: string): FileTransferDefinition {
  const trimmed = String(rawDefinition || "").trim();
  if (!trimmed) {
    throw new Error("Datei-Definition darf nicht leer sein");
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Datei-Definition muss ein JSON-Objekt sein");
    }
    return parsed as FileTransferDefinition;
  } catch {
    return { fileName: trimmed, format: detectFormatByName(trimmed) };
  }
}

function resolveRuntimeConfig(connectorConfig: ConnectorConfig): FileConnectorRuntimeConfig {
  const parameters = connectorConfig.parameters || {};
  const basePath = path.resolve(
    process.cwd(),
    String(parameters.basePath || parameters.fileBasePath || "artifacts/files")
  );

  const importPath = path.resolve(basePath, String(parameters.importPath || "inbound"));
  const exportPath = path.resolve(basePath, String(parameters.exportPath || "outbound"));
  const archivePath = path.resolve(basePath, String(parameters.archivePath || "archive"));

  return {
    basePath,
    importPath,
    exportPath,
    archivePath,
    defaultCharset: String(parameters.defaultCharset || "utf8").trim() || "utf8",
    defaultDelimiter: String(parameters.defaultDelimiter || ";").trim() || ";",
    archiveOnRead: resolveBoolean(parameters.archiveOnRead, true),
    archiveOnWrite: resolveBoolean(parameters.archiveOnWrite, false)
  };
}

function splitCsvLine(line: string, delimiter: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const nextChar = line[index + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === delimiter) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values.map((value) => value.trim());
}

function parseCsvRows(raw: string, delimiter: string): string[][] {
  return String(raw || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0)
    .map((line) => splitCsvLine(line, delimiter));
}

function normalizeHeader(value: unknown, fallbackIndex: number): string {
  const normalized = String(value ?? "").trim();
  return normalized || `column_${fallbackIndex + 1}`;
}

function rowsToObjects(rows: unknown[][], headers: string[]): Record<string, unknown>[] {
  return rows.map((row) => {
    const result: Record<string, unknown> = {};
    headers.forEach((header, index) => {
      result[header] = row[index] ?? "";
    });
    return result;
  });
}

function escapeCsvValue(value: unknown, delimiter: string): string {
  const text = value === null || value === undefined ? "" : String(value);
  if (text.includes('"') || text.includes("\n") || text.includes("\r") || text.includes(delimiter)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

function detectDelimiterFromHeader(headerLine: string): string {
  const candidates = [";", ",", "\t", "|"];
  const scores = candidates.map((candidate) => ({
    delimiter: candidate,
    score: splitCsvLine(headerLine, candidate).length
  }));
  scores.sort((a, b) => b.score - a.score);
  return scores[0]?.delimiter || ";";
}

function buildAbsoluteFilePath(
  definition: FileTransferDefinition,
  runtime: FileConnectorRuntimeConfig,
  mode: "read" | "write"
): { absolutePath: string; fileName: string } {
  const explicitPath = String(definition.filePath || "").trim();
  const fileName = String(definition.fileName || "").trim();

  if (!explicitPath && !fileName) {
    throw new Error("Datei-Definition erfordert fileName oder filePath");
  }

  if (explicitPath) {
    const absolutePath = path.isAbsolute(explicitPath)
      ? explicitPath
      : path.resolve(runtime.basePath, explicitPath);
    return { absolutePath, fileName: path.basename(absolutePath) };
  }

  const root = mode === "read" ? runtime.importPath : runtime.exportPath;
  return {
    absolutePath: path.resolve(root, fileName),
    fileName
  };
}

async function archiveFile(originalPath: string, fileName: string, archivePath: string): Promise<void> {
  await fs.mkdir(archivePath, { recursive: true });
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const archivedName = `${timestamp}-${fileName}`;
  const targetPath = path.resolve(archivePath, archivedName);
  await fs.rename(originalPath, targetPath);
}

export async function parseFileFromConnector(
  connectorConfig: ConnectorConfig,
  rawDefinition: string,
  options?: { archiveOnRead?: boolean }
): Promise<ParsedFilePayload> {
  const definition = parseDefinition(rawDefinition);
  const runtime = resolveRuntimeConfig(connectorConfig);
  const { absolutePath, fileName } = buildAbsoluteFilePath(definition, runtime, "read");
  const format = definition.format || detectFormatByName(fileName);
  const charset = String(definition.charset || runtime.defaultCharset).trim() || "utf8";

  if (format === "json") {
    const raw = await fs.readFile(absolutePath, { encoding: charset as BufferEncoding });
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error("JSON-Datei ist ungueltig");
    }

    const scoped = String(definition.jsonPath || "").trim()
      ? getByPath(parsed, String(definition.jsonPath || "").trim())
      : parsed;
    const payloadRows = normalizeJsonRows(scoped);

    const headers = Array.from(new Set(payloadRows.flatMap((row) => Object.keys(row || {}))));

    const shouldArchive = options?.archiveOnRead === undefined ? runtime.archiveOnRead : options.archiveOnRead;
    if (shouldArchive) {
      await archiveFile(absolutePath, fileName, runtime.archivePath);
    }

    return {
      format,
      headers,
      rows: payloadRows,
      charset,
      filePath: absolutePath,
      fileName
    };
  }

  if (format === "excel") {
    const buffer = await fs.readFile(absolutePath);
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheetName = String(definition.sheetName || workbook.SheetNames[0] || "").trim();
    if (!sheetName || !workbook.Sheets[sheetName]) {
      throw new Error(`Excel-Sheet nicht gefunden: ${sheetName || "(leer)"}`);
    }

    const sheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: false, defval: "" });
    const hasHeader = definition.hasHeader !== false;
    const headerRow = hasHeader ? (rows[0] || []) : [];
    const headers = hasHeader
      ? headerRow.map((value, index) => normalizeHeader(value, index))
      : Array.from({ length: Math.max(...rows.map((row) => row.length), 0) }, (_, index) => `column_${index + 1}`);

    const dataRows = hasHeader ? rows.slice(1) : rows;
    const payloadRows = rowsToObjects(dataRows, headers);

    const shouldArchive = options?.archiveOnRead === undefined ? runtime.archiveOnRead : options.archiveOnRead;
    if (shouldArchive) {
      await archiveFile(absolutePath, fileName, runtime.archivePath);
    }

    return {
      format,
      headers,
      rows: payloadRows,
      charset,
      filePath: absolutePath,
      fileName
    };
  }

  const delimiter = String(definition.delimiter || runtime.defaultDelimiter || ";");
  const raw = await fs.readFile(absolutePath, { encoding: charset as BufferEncoding });
  const rowValues = parseCsvRows(raw, delimiter);
  const hasHeader = definition.hasHeader !== false;
  const headers = hasHeader
    ? (rowValues[0] || []).map((value, index) => normalizeHeader(value, index))
    : Array.from({ length: Math.max(...rowValues.map((row) => row.length), 0) }, (_, index) => `column_${index + 1}`);

  const dataRows = hasHeader ? rowValues.slice(1) : rowValues;
  const payloadRows = rowsToObjects(dataRows, headers);

  const shouldArchive = options?.archiveOnRead === undefined ? runtime.archiveOnRead : options.archiveOnRead;
  if (shouldArchive) {
    await archiveFile(absolutePath, fileName, runtime.archivePath);
  }

  return {
    format,
    headers,
    rows: payloadRows,
    charset,
    filePath: absolutePath,
    fileName
  };
}

export async function writeFileFromConnector(
  connectorConfig: ConnectorConfig,
  rawDefinition: string,
  rows: Record<string, unknown>[]
): Promise<{ format: FileDataFormat; filePath: string; fileName: string; rowCount: number }> {
  const definition = parseDefinition(rawDefinition);
  const runtime = resolveRuntimeConfig(connectorConfig);
  const { absolutePath, fileName } = buildAbsoluteFilePath(definition, runtime, "write");
  const format = definition.format || detectFormatByName(fileName);
  const charset = String(definition.charset || runtime.defaultCharset).trim() || "utf8";

  await fs.mkdir(path.dirname(absolutePath), { recursive: true });

  const headers = Array.from(
    new Set(rows.flatMap((row) => Object.keys(row || {})))
  );

  if (format === "json") {
    const content = JSON.stringify(rows.map((row) => ({ ...(row || {}) })), null, 2);
    await fs.writeFile(absolutePath, content, { encoding: charset as BufferEncoding });
  } else if (format === "excel") {
    const worksheet = XLSX.utils.json_to_sheet(rows, { header: headers.length ? headers : undefined });
    const workbook = XLSX.utils.book_new();
    const sheetName = String(definition.sheetName || "Sheet1").trim() || "Sheet1";
    XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
    XLSX.writeFile(workbook, absolutePath);
  } else {
    const delimiter = String(definition.delimiter || runtime.defaultDelimiter || ";");
    const writeHeader = definition.writeHeader !== false;
    const lines: string[] = [];
    if (writeHeader && headers.length) {
      lines.push(headers.map((header) => escapeCsvValue(header, delimiter)).join(delimiter));
    }

    rows.forEach((row) => {
      const line = headers.map((header) => escapeCsvValue(row?.[header], delimiter)).join(delimiter);
      lines.push(line);
    });

    const content = lines.join("\n");
    await fs.writeFile(absolutePath, content, { encoding: charset as BufferEncoding });
  }

  if (runtime.archiveOnWrite) {
    await archiveFile(absolutePath, fileName, runtime.archivePath);
  }

  return {
    format,
    filePath: absolutePath,
    fileName,
    rowCount: rows.length
  };
}

export function analyzeUploadedFile(fileName: string, content: Buffer): ParsedUploadPayload {
  const format = detectFormatByName(fileName);

  if (format === "json") {
    let parsed: unknown;
    try {
      parsed = JSON.parse(content.toString("utf8").replace(/^\uFEFF/, ""));
    } catch {
      throw new Error("Hochgeladene JSON-Datei ist ungueltig");
    }

    const rows = normalizeJsonRows(parsed).slice(0, 200);
    const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row || {}))));
    return {
      format,
      headers,
      charset: "utf8",
      delimiter: ""
    };
  }

  if (format === "excel") {
    const workbook = XLSX.read(content, { type: "buffer" });
    const firstSheet = workbook.SheetNames[0];
    const rows = XLSX.utils.sheet_to_json<unknown[]>(workbook.Sheets[firstSheet], { header: 1, raw: false, defval: "" });
    const headerRow = (rows[0] || []) as unknown[];
    const headers = headerRow.map((value, index) => normalizeHeader(value, index));
    return {
      format,
      headers,
      charset: "utf8",
      delimiter: ";"
    };
  }

  const decoded = content.toString("utf8").replace(/^\uFEFF/, "");
  const firstLine = decoded.split(/\r?\n/).find((line) => line.trim().length > 0) || "";
  const delimiter = detectDelimiterFromHeader(firstLine);
  const headers = splitCsvLine(firstLine, delimiter).map((value, index) => normalizeHeader(value, index));

  return {
    format,
    headers,
    charset: "utf8",
    delimiter
  };
}
