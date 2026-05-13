#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const sqlite3 = require("sqlite3");

const sourceFile = path.resolve(process.env.SF_PROJECTS_FILE || path.resolve(process.cwd(), "artifacts/projects.json"));
const sqliteFile = path.resolve(process.env.PROJECTS_SQLITE_FILE || path.resolve(process.cwd(), "data/projects.sqlite"));

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) {
        reject(error);
        return;
      }
      resolve({ changes: this.changes, lastID: this.lastID });
    });
  });
}

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function normalizeProjects(input) {
  const now = new Date().toISOString();
  const list = Array.isArray(input) ? input : [];

  const normalized = list
    .filter((item) => item && typeof item === "object" && !Array.isArray(item))
    .map((item) => {
      const id = String(item.id || "").trim();
      const name = String(item.name || "").trim();
      if (!id || !name) {
        return null;
      }

      const description = typeof item.description === "string" ? item.description.trim() : "";
      const createdAt = typeof item.createdAt === "string" && item.createdAt.trim() ? item.createdAt : now;
      const updatedAt = typeof item.updatedAt === "string" && item.updatedAt.trim() ? item.updatedAt : now;

      return {
        id,
        name,
        description,
        archived: item.archived === true ? 1 : 0,
        productionWriteProtection: item.productionWriteProtection === false ? 0 : 1,
        createdAt,
        updatedAt
      };
    })
    .filter(Boolean);

  if (!normalized.some((item) => item.id === "default-project")) {
    normalized.unshift({
      id: "default-project",
      name: "Default-Projekt",
      description: "",
      archived: 0,
      productionWriteProtection: 1,
      createdAt: now,
      updatedAt: now
    });
  }

  return normalized;
}

async function main() {
  const sourceDir = path.dirname(sourceFile);
  const sqliteDir = path.dirname(sqliteFile);
  fs.mkdirSync(sourceDir, { recursive: true });
  fs.mkdirSync(sqliteDir, { recursive: true });

  let sourceProjects = [];
  if (fs.existsSync(sourceFile)) {
    const raw = fs.readFileSync(sourceFile, "utf8").trim();
    if (raw) {
      sourceProjects = JSON.parse(raw);
    }
  }

  const projects = normalizeProjects(sourceProjects);
  const db = new sqlite3.Database(sqliteFile, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);

  try {
    await run(
      db,
      `CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        archived INTEGER NOT NULL DEFAULT 0,
        production_write_protection INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )`
    );

    await run(db, "BEGIN");
    for (const project of projects) {
      await run(
        db,
        `INSERT INTO projects (id, name, description, archived, production_write_protection, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            description = excluded.description,
            archived = excluded.archived,
            production_write_protection = excluded.production_write_protection,
            updated_at = excluded.updated_at`,
        [
          project.id,
          project.name,
          project.description,
          project.archived,
          project.productionWriteProtection,
          project.createdAt,
          project.updatedAt
        ]
      );
    }
    await run(db, "COMMIT");

    process.stdout.write(
      `OK: ${projects.length} Projekte nach ${sqliteFile} migriert (Quelle: ${sourceFile})\n`
    );
  } catch (error) {
    try {
      await run(db, "ROLLBACK");
    } catch {
      // ignore rollback errors
    }
    throw error;
  } finally {
    await close(db);
  }
}

main().catch((error) => {
  process.stderr.write(`Fehler bei Projektmigration nach SQLite: ${error?.message || error}\n`);
  process.exitCode = 1;
});
