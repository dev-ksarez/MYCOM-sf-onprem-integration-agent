

import fs from "fs";
import path from "path";
import sqlite3 from "sqlite3";

export interface SqliteConfig {
  filePath?: string;
}

export class SqliteDatabase {
  private readonly database: sqlite3.Database;

  public constructor(config?: SqliteConfig) {
    const defaultPath = path.resolve(process.cwd(), ".data", "mock-agent.sqlite");
    const filePath = path.resolve(config?.filePath || defaultPath);

    const directoryPath = path.dirname(filePath);
    fs.mkdirSync(directoryPath, { recursive: true });

    this.database = new sqlite3.Database(
      filePath,
      sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
    );
  }

  public async initialize(): Promise<void> {
    await this.run(`
      CREATE TABLE IF NOT EXISTS mock_accounts (
        external_key TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        name TEXT NOT NULL,
        account_number TEXT,
        billing_street TEXT,
        billing_postal_code TEXT,
        billing_city TEXT,
        billing_country TEXT,
        phone TEXT,
        website TEXT,
        last_modified TEXT NOT NULL,
        source_system TEXT NOT NULL,
        target_system TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);

    await this.run(`
      CREATE TABLE IF NOT EXISTS mock_operation_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        correlation_id TEXT NOT NULL,
        external_key TEXT NOT NULL,
        operation TEXT NOT NULL,
        status_code TEXT NOT NULL,
        message TEXT,
        created_at TEXT NOT NULL
      )
    `);

    await this.run(`
      CREATE TABLE IF NOT EXISTS mock_error_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_key TEXT NOT NULL,
        error_type TEXT NOT NULL,
        message TEXT NOT NULL,
        retryable INTEGER NOT NULL,
        is_active INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);
  }

  public async run(sql: string, params: unknown[] = []): Promise<{ lastID: number; changes: number }> {
    return new Promise((resolve, reject) => {
      this.database.run(sql, params, function onRun(error) {
        if (error) {
          reject(error);
          return;
        }

        resolve({
          lastID: this.lastID,
          changes: this.changes
        });
      });
    });
  }

  public async get<T>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      this.database.get(sql, params, (error, row) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(row as T | undefined);
      });
    });
  }

  public async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.database.all(sql, params, (error, rows) => {
        if (error) {
          reject(error);
          return;
        }

        resolve((rows as T[]) || []);
      });
    });
  }

  public async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.database.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });
  }
}