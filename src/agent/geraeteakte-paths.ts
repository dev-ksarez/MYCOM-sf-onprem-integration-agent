import fs from "node:fs";
import path from "node:path";

export type GeraeteakteDirectoryLayout = "direct" | "annaburg-fg-bucket";

export interface GeraeteakteSerialDirectory {
  serial: string;
  directoryName: string;
  bucketName: string;
  absolutePath: string;
}

export interface GeraeteaktePathConfig {
  basePath: string;
  layout: GeraeteakteDirectoryLayout;
}

let activeGeraeteaktePathConfig: GeraeteaktePathConfig | null = null;

function isEnabled(value: unknown): boolean {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

export function getGeraeteakteDirectoryLayout(): GeraeteakteDirectoryLayout {
  const configured = String(process.env.GERAETEAKTE_DIRECTORY_LAYOUT || process.env.FILE_BROWSE_DIRECTORY_LAYOUT || "")
    .trim()
    .toLowerCase();
  if (configured === "annaburg" || configured === "annaburg-fg-bucket" || configured === "fg-bucket") {
    return "annaburg-fg-bucket";
  }
  if (isEnabled(process.env.GERAETEAKTE_ANNABURG_LAYOUT) || isEnabled(process.env.FILE_BROWSE_ANNABURG_LAYOUT)) {
    return "annaburg-fg-bucket";
  }
  return "direct";
}

export function normalizeGeraeteakteDirectoryLayout(value: unknown): GeraeteakteDirectoryLayout {
  const configured = String(value || "").trim().toLowerCase();
  if (configured === "annaburg" || configured === "annaburg-fg-bucket" || configured === "fg-bucket") {
    return "annaburg-fg-bucket";
  }
  return "direct";
}

export function normalizeGeraeteaktePathText(value: unknown): string {
  return String(value || "").trim().normalize("NFC");
}

function findUnicodeEquivalentEntry(parentPath: string, segment: string): string | null {
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(parentPath, { withFileTypes: true });
  } catch {
    return null;
  }

  const normalizedSegment = segment.normalize("NFC");
  const exact = entries.find((entry) => entry.name.normalize("NFC") === normalizedSegment);
  if (exact) {
    return exact.name;
  }

  const lowerSegment = normalizedSegment.toLocaleLowerCase();
  return entries.find((entry) => entry.name.normalize("NFC").toLocaleLowerCase() === lowerSegment)?.name || null;
}

export function resolveExistingPathWithUnicodeFallback(candidatePath: string): string {
  const normalizedCandidate = normalizeGeraeteaktePathText(candidatePath);
  const absoluteCandidate = path.resolve(normalizedCandidate);
  if (fs.existsSync(absoluteCandidate)) {
    return absoluteCandidate;
  }

  const parsed = path.parse(absoluteCandidate);
  const root = parsed.root || path.sep;
  if (!fs.existsSync(root)) {
    return absoluteCandidate;
  }

  const segments = path.relative(root, absoluteCandidate).split(path.sep).filter(Boolean);
  let current = root;
  for (const segment of segments) {
    const direct = path.join(current, segment);
    if (fs.existsSync(direct)) {
      current = direct;
      continue;
    }

    const equivalentEntry = findUnicodeEquivalentEntry(current, segment);
    if (!equivalentEntry) {
      return absoluteCandidate;
    }
    current = path.join(current, equivalentEntry);
  }

  return current;
}

export function resolveGeraeteaktePathConfig(parameters?: Record<string, unknown>, fallbackBasePath?: string): GeraeteaktePathConfig | null {
  const params = parameters || {};
  const basePath = normalizeGeraeteaktePathText(
    params.basePath ||
    params.fileBasePath ||
    params.rootPath ||
    fallbackBasePath ||
    ""
  ).trim();
  if (!basePath) {
    return null;
  }

  const rawLayout = params.directoryLayout || params.layout || params.folderLayout || params.serialDirectoryLayout;
  const layout = rawLayout
    ? normalizeGeraeteakteDirectoryLayout(rawLayout)
    : getGeraeteakteDirectoryLayout();

  return {
    basePath,
    layout,
  };
}

export function setActiveGeraeteaktePathConfig(config: GeraeteaktePathConfig | null): void {
  activeGeraeteaktePathConfig = config;
}

export function getActiveGeraeteaktePathConfig(): GeraeteaktePathConfig | null {
  return activeGeraeteaktePathConfig;
}

export function isSafeGeraeteaktePathSegment(segment: string): boolean {
  if (!segment || segment.length > 255) return false;
  if (segment === ".") return false;
  if (segment.includes("..") || segment.includes("\0")) return false;
  return !/[<>:"|?*\x00-\x1f]/.test(segment);
}

function normalizeFgSerial(rawSerial: string): string {
  const match = String(rawSerial || "").trim().match(/^([A-Za-z]+)(\d+)$/);
  if (!match) {
    return String(rawSerial || "").trim();
  }

  const prefix = match[1];
  const digits = match[2];
  if (!/^fg$/i.test(prefix) || digits.length >= 6) {
    return `${prefix}${digits}`;
  }

  return `${prefix}${digits.padStart(6, "0")}`;
}

function buildAnnaburgBucketName(directoryName: string): string {
  const match = directoryName.match(/^([A-Za-z]+)(\d{2,})$/);
  if (!match) {
    return directoryName;
  }

  const prefix = match[1];
  const digits = match[2];
  return `${prefix}${digits.slice(0, -2)}xx`;
}

function assertInsideBase(candidatePath: string, basePath: string): string {
  const resolvedCandidate = resolveExistingPathWithUnicodeFallback(candidatePath);
  const resolvedBase = resolveExistingPathWithUnicodeFallback(basePath);
  const relative = path.relative(resolvedBase, resolvedCandidate);
  if (relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative))) {
    return resolvedCandidate;
  }
  throw new Error("Ungueltige Seriennummer (Pfad-Traversal)");
}

export function resolveGeraeteakteSerialDirectory(
  basePath: string,
  rawSerial: string,
  layout: GeraeteakteDirectoryLayout = getGeraeteakteDirectoryLayout()
): GeraeteakteSerialDirectory {
  const serial = normalizeGeraeteaktePathText(rawSerial);
  if (!isSafeGeraeteaktePathSegment(serial)) {
    throw new Error("Ungueltige Seriennummer");
  }

  const directoryName = layout === "annaburg-fg-bucket" ? normalizeFgSerial(serial) : serial;
  if (!isSafeGeraeteaktePathSegment(directoryName)) {
    throw new Error("Ungueltige Seriennummer");
  }

  const bucketName = layout === "annaburg-fg-bucket" ? buildAnnaburgBucketName(directoryName) : "";
  if (bucketName && !isSafeGeraeteaktePathSegment(bucketName)) {
    throw new Error("Ungueltige Seriennummer");
  }

  const absolutePath = layout === "annaburg-fg-bucket"
    ? path.join(basePath, bucketName, directoryName)
    : path.join(basePath, directoryName);

  return {
    serial,
    directoryName,
    bucketName,
    absolutePath: assertInsideBase(absolutePath, basePath),
  };
}

export function listGeraeteakteSerialDirectories(
  basePath: string,
  layout: GeraeteakteDirectoryLayout = getGeraeteakteDirectoryLayout()
): GeraeteakteSerialDirectory[] {
  const root = resolveExistingPathWithUnicodeFallback(basePath);
  if (!fs.existsSync(root)) {
    return [];
  }

  if (layout === "direct") {
    return fs.readdirSync(root, { withFileTypes: true })
      .filter((entry) => entry.isDirectory() && isSafeGeraeteaktePathSegment(entry.name))
      .map((entry) => ({
        serial: entry.name,
        directoryName: entry.name,
        bucketName: "",
        absolutePath: assertInsideBase(path.join(root, entry.name), root),
      }));
  }

  const result: GeraeteakteSerialDirectory[] = [];
  const bucketEntries = fs.readdirSync(root, { withFileTypes: true });
  for (const bucketEntry of bucketEntries) {
    if (!bucketEntry.isDirectory() || !isSafeGeraeteaktePathSegment(bucketEntry.name)) continue;
    const bucketPath = assertInsideBase(path.join(root, bucketEntry.name), root);
    let serialEntries: fs.Dirent[];
    try {
      serialEntries = fs.readdirSync(bucketPath, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const serialEntry of serialEntries) {
      if (!serialEntry.isDirectory() || !isSafeGeraeteaktePathSegment(serialEntry.name)) continue;
      result.push({
        serial: serialEntry.name,
        directoryName: serialEntry.name,
        bucketName: bucketEntry.name,
        absolutePath: assertInsideBase(path.join(bucketPath, serialEntry.name), root),
      });
    }
  }

  return result;
}
