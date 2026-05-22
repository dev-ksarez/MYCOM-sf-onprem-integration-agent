import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type pino from "pino";
import sharp from "sharp";
import { SalesforceClient } from "../clients/salesforce/salesforce-client";

// ─── Thumbnails als data:-URI (für Anzeige direkt aus Salesforce) ────────────

const THUMBNAIL_IMAGE_EXTENSIONS = new Set(["jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif"]);
const THUMBNAIL_WIDTH = 160;
const THUMB_CACHE_MAX = 3000;
const thumbDataUriCache = new Map<string, string | null>();

async function buildThumbnailDataUri(absPath: string, mtimeMs: number, size: number): Promise<string | null> {
  const cacheKey = `${absPath}:${mtimeMs}:${size}`;
  if (thumbDataUriCache.has(cacheKey)) {
    return thumbDataUriCache.get(cacheKey) ?? null;
  }
  let dataUri: string | null = null;
  try {
    const buffer = await sharp(absPath)
      .rotate()
      .resize(THUMBNAIL_WIDTH, THUMBNAIL_WIDTH, { fit: "inside", withoutEnlargement: true })
      .jpeg({ quality: 70 })
      .toBuffer();
    dataUri = `data:image/jpeg;base64,${buffer.toString("base64")}`;
  } catch {
    dataUri = null;
  }
  if (thumbDataUriCache.size >= THUMB_CACHE_MAX) {
    const oldest = thumbDataUriCache.keys().next().value;
    if (oldest) thumbDataUriCache.delete(oldest);
  }
  thumbDataUriCache.set(cacheKey, dataUri);
  return dataUri;
}

/**
 * Geräteakte Index Publisher (Weg 2)
 *
 * Scannt das lokale Basisverzeichnis (FILE_BROWSE_BASE_PATH). Jeder
 * Unterordner entspricht einer Seriennummer, die enthaltenen Dateien
 * werden als Metadaten (NUR Name/Größe/Datum, KEIN Inhalt) nach
 * Salesforce in das Objekt MSD_GeraeteakteFile__c gepusht.
 *
 * Dadurch muss Salesforce den Agent NICHT aus dem Internet erreichen –
 * die Liste kommt aus Salesforce, der Download läuft direkt Browser→Agent
 * im lokalen Netz (signierte URL).
 */

export const GERAETEAKTE_FILE_OBJECT = "MSD_GeraeteakteFile__c";
export const GERAETEAKTE_EXTERNAL_ID_FIELD = "MSD_ExternalKey__c";

export interface ScannedFile {
  externalKey: string;
  seriennummer: string;
  relativePath: string;   // POSIX, z. B. "Abnahme/foto.jpg"
  folderPath: string;     // z. B. "Abnahme" ("" = Wurzel)
  fileName: string;
  extension: string;
  size: number;
  createdAt: string;
  modifiedAt: string;
  mtimeMs: number;
}

const JUNK_FILES = new Set([".DS_Store", "Thumbs.db", "desktop.ini"]);

function isSafeSegment(segment: string): boolean {
  if (!segment || segment.length > 255) return false;
  if (segment === ".") return false;
  if (segment.includes("..") || segment.includes("\0")) return false;
  return !/[<>:"|?*\x00-\x1f]/.test(segment);
}

function isIgnoredFile(name: string): boolean {
  return name.startsWith(".") || JUNK_FILES.has(name);
}

/** Rekursiver Walk innerhalb eines Seriennummer-Ordners. */
function walkSerialDir(serialDir: string, relPrefix: string, out: { rel: string; abs: string }[]): void {
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(serialDir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const entry of entries) {
    if (!isSafeSegment(entry.name)) continue;
    const abs = path.join(serialDir, entry.name);
    const rel = relPrefix ? `${relPrefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      walkSerialDir(abs, rel, out);
    } else if (entry.isFile() && !isIgnoredFile(entry.name)) {
      out.push({ rel, abs });
    }
  }
}

/** Liest alle Seriennummer-Ordner und deren Dateien (rekursiv) unterhalb von basePath. */
export function scanFileIndex(basePath: string): ScannedFile[] {
  const root = path.resolve(basePath);
  if (!fs.existsSync(root)) {
    return [];
  }

  const result: ScannedFile[] = [];
  const serialDirs = fs.readdirSync(root, { withFileTypes: true });

  for (const serialEntry of serialDirs) {
    if (!serialEntry.isDirectory()) continue;
    const seriennummer = serialEntry.name;
    if (!isSafeSegment(seriennummer)) continue;

    const serialDir = path.join(root, seriennummer);
    const found: { rel: string; abs: string }[] = [];
    walkSerialDir(serialDir, "", found);

    for (const { rel, abs } of found) {
      try {
        const stat = fs.statSync(abs);
        const fileName = path.posix.basename(rel);
        const folderPath = rel.includes("/") ? rel.slice(0, rel.lastIndexOf("/")) : "";
        const externalKey = crypto
          .createHash("sha256")
          .update(`${seriennummer}::${rel}`)
          .digest("hex");
        result.push({
          externalKey,
          seriennummer,
          relativePath: rel,
          folderPath,
          fileName,
          extension: path.extname(fileName).replace(".", "").toLowerCase(),
          size: stat.size,
          createdAt: stat.birthtime.toISOString(),
          modifiedAt: stat.mtime.toISOString(),
          mtimeMs: stat.mtimeMs,
        });
      } catch {
        /* nicht lesbare Datei überspringen */
      }
    }
  }

  return result;
}

export interface PublishResult {
  scanned: number;
  upserted: number;
  pruned: number;
}

/**
 * Synchronisiert den Datei-Index nach Salesforce:
 *  1. Scan des Basisverzeichnisses
 *  2. Bulk-Upsert aller aktuellen Dateien (MSD_IndexedAt__c = runStart)
 *  3. Löschen verwaister Einträge (MSD_IndexedAt__c < runStart)
 */
export async function publishFileIndex(
  client: SalesforceClient,
  basePath: string,
  logger: pino.Logger
): Promise<PublishResult> {
  const runStart = new Date().toISOString();
  const files = scanFileIndex(basePath);

  // 1+2) Upsert (für Bilder Thumbnail als data:-URI erzeugen)
  let upserted = 0;
  if (files.length > 0) {
    const valuesList = await Promise.all(files.map(async (file) => {
      let thumbnail: string | null = null;
      if (THUMBNAIL_IMAGE_EXTENSIONS.has(file.extension)) {
        const absPath = path.join(basePath, file.seriennummer, ...file.relativePath.split("/"));
        thumbnail = await buildThumbnailDataUri(absPath, file.mtimeMs, file.size);
      }
      return {
        [GERAETEAKTE_EXTERNAL_ID_FIELD]: file.externalKey,
        MSD_Seriennummer__c: file.seriennummer,
        MSD_RelativePath__c: file.relativePath,
        MSD_FolderPath__c: file.folderPath,
        MSD_FileName__c: file.fileName,
        MSD_Extension__c: file.extension,
        MSD_Size__c: file.size,
        MSD_FileCreatedAt__c: file.createdAt,
        MSD_FileModifiedAt__c: file.modifiedAt,
        MSD_IndexedAt__c: runStart,
        MSD_Thumbnail__c: thumbnail,
        Name: file.fileName.slice(0, 80),
      };
    }));

    const results = await client.upsertGenericRecords({
      objectApiName: GERAETEAKTE_FILE_OBJECT,
      externalIdField: GERAETEAKTE_EXTERNAL_ID_FIELD,
      valuesList,
    });
    upserted = results.filter((r) => r.success).length;
  }

  // 3) Pruning: alle Einträge, die in diesem Lauf NICHT angefasst wurden
  const stale = await client.queryGeneric(`
    SELECT Id FROM ${GERAETEAKTE_FILE_OBJECT}
    WHERE MSD_IndexedAt__c < ${runStart}
    LIMIT 10000
  `);
  const staleIds = stale.map((row) => String(row.Id || "").trim()).filter(Boolean);
  const pruned = staleIds.length > 0
    ? await client.deleteGenericRecords(GERAETEAKTE_FILE_OBJECT, staleIds)
    : 0;

  logger.info(
    { scanned: files.length, upserted, pruned },
    "Geräteakte Datei-Index nach Salesforce synchronisiert"
  );

  return { scanned: files.length, upserted, pruned };
}
