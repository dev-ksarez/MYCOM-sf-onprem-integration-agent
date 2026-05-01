import path from "node:path";
import { SqliteDatabase } from "./sqlite";

export interface MigrationStageObjectMeta {
  migrationId: string;
  objectId: string;
  filePath?: string;
  sourceFileName: string;
  fileFormat: "csv" | "excel" | "json";
  fileCharset: string;
  fileDelimiter: string;
  fileTextQualifier: string;
  recordCount: number;
  columns: string[];
  uploadedAt: string;
}

export interface MigrationStageRow {
  rowIndex: number;
  payload: Record<string, unknown>;
  status: string;
  errorMessage?: string;
}

export interface MigrationStageStatusSummary {
  total: number;
  byStatus: Record<string, number>;
}

interface StageObjectMetaRow {
  migration_id: string;
  object_id: string;
  file_path?: string;
  source_file_name: string;
  file_format: "csv" | "excel" | "json";
  file_charset: string;
  file_delimiter: string;
  file_text_qualifier: string;
  record_count: number;
  columns_json: string;
  uploaded_at: string;
}

interface StageRowRecord {
  row_index: number;
  payload_json: string;
  status_code: string;
  error_message?: string;
}

export class MigrationStagingSqlite {
  private readonly database: SqliteDatabase;
  private initialized = false;
  private readonly databaseFilePath: string;

  public constructor(filePath?: string) {
    this.databaseFilePath = path.resolve(filePath || path.resolve(process.cwd(), ".data", "migration-staging.sqlite"));
    this.database = new SqliteDatabase({ filePath: this.databaseFilePath });
  }

  public getFilePath(): string {
    return this.databaseFilePath;
  }

  public async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    await this.database.run(`
      CREATE TABLE IF NOT EXISTS migration_stage_objects (
        migration_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        file_path TEXT,
        source_file_name TEXT NOT NULL,
        file_format TEXT NOT NULL,
        file_charset TEXT NOT NULL,
        file_delimiter TEXT NOT NULL,
        file_text_qualifier TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        columns_json TEXT NOT NULL,
        uploaded_at TEXT NOT NULL,
        PRIMARY KEY (migration_id, object_id)
      )
    `);

    await this.database.run(`
      CREATE TABLE IF NOT EXISTS migration_stage_rows (
        migration_id TEXT NOT NULL,
        object_id TEXT NOT NULL,
        row_index INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        status_code TEXT NOT NULL,
        error_message TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (migration_id, object_id, row_index)
      )
    `);

    this.initialized = true;
  }

  public async replaceObjectRows(meta: MigrationStageObjectMeta, rows: Record<string, unknown>[]): Promise<void> {
    await this.initialize();
    const now = new Date().toISOString();

    await this.database.run("BEGIN TRANSACTION");
    try {
      await this.database.run(
        `DELETE FROM migration_stage_rows WHERE migration_id = ? AND object_id = ?`,
        [meta.migrationId, meta.objectId]
      );
      await this.database.run(
        `DELETE FROM migration_stage_objects WHERE migration_id = ? AND object_id = ?`,
        [meta.migrationId, meta.objectId]
      );

      await this.database.run(
        `
          INSERT INTO migration_stage_objects (
            migration_id,
            object_id,
            file_path,
            source_file_name,
            file_format,
            file_charset,
            file_delimiter,
            file_text_qualifier,
            record_count,
            columns_json,
            uploaded_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          meta.migrationId,
          meta.objectId,
          meta.filePath || null,
          meta.sourceFileName,
          meta.fileFormat,
          meta.fileCharset,
          meta.fileDelimiter,
          meta.fileTextQualifier,
          meta.recordCount,
          JSON.stringify(meta.columns || []),
          meta.uploadedAt || now
        ]
      );

      for (let index = 0; index < rows.length; index += 1) {
        await this.database.run(
          `
            INSERT INTO migration_stage_rows (
              migration_id,
              object_id,
              row_index,
              payload_json,
              status_code,
              error_message,
              created_at,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `,
          [
            meta.migrationId,
            meta.objectId,
            index + 1,
            JSON.stringify(rows[index] || {}),
            "pending",
            null,
            now,
            now
          ]
        );
      }

      await this.database.run("COMMIT");
    } catch (error) {
      await this.database.run("ROLLBACK");
      throw error;
    }
  }

  public async getObjectMeta(migrationId: string, objectId: string): Promise<MigrationStageObjectMeta | undefined> {
    await this.initialize();
    const row = await this.database.get<StageObjectMetaRow>(
      `
        SELECT
          migration_id,
          object_id,
          file_path,
          source_file_name,
          file_format,
          file_charset,
          file_delimiter,
          file_text_qualifier,
          record_count,
          columns_json,
          uploaded_at
        FROM migration_stage_objects
        WHERE migration_id = ? AND object_id = ?
      `,
      [migrationId, objectId]
    );

    if (!row) {
      return undefined;
    }

    return {
      migrationId: row.migration_id,
      objectId: row.object_id,
      filePath: row.file_path,
      sourceFileName: row.source_file_name,
      fileFormat: row.file_format,
      fileCharset: row.file_charset,
      fileDelimiter: row.file_delimiter,
      fileTextQualifier: row.file_text_qualifier,
      recordCount: Number(row.record_count || 0),
      columns: this.parseJsonArray(row.columns_json),
      uploadedAt: row.uploaded_at
    };
  }

  public async listObjectRows(
    migrationId: string,
    objectId: string,
    options?: { limit?: number; offset?: number }
  ): Promise<MigrationStageRow[]> {
    await this.initialize();
    const limitClause = typeof options?.limit === "number" && options.limit > 0 ? ` LIMIT ${Math.floor(options.limit)}` : "";
    const offsetClause = typeof options?.offset === "number" && options.offset > 0 ? ` OFFSET ${Math.floor(options.offset)}` : "";
    const rows = await this.database.all<StageRowRecord>(
      `
        SELECT row_index, payload_json, status_code, error_message
        FROM migration_stage_rows
        WHERE migration_id = ? AND object_id = ?
        ORDER BY row_index ASC${limitClause}${offsetClause}
      `,
      [migrationId, objectId]
    );

    return rows.map((row) => ({
      rowIndex: Number(row.row_index || 0),
      payload: this.parseJsonObject(row.payload_json),
      status: String(row.status_code || "pending"),
      errorMessage: row.error_message || undefined
    }));
  }

  public async getObjectStatusSummary(migrationId: string, objectId: string): Promise<MigrationStageStatusSummary> {
    await this.initialize();
    const rows = await this.database.all<Array<{ status_code?: string; count_value?: number }> extends infer _ ? { status_code?: string; count_value?: number } : never>(
      `
        SELECT status_code, COUNT(*) AS count_value
        FROM migration_stage_rows
        WHERE migration_id = ? AND object_id = ?
        GROUP BY status_code
      `,
      [migrationId, objectId]
    );

    const byStatus: Record<string, number> = {};
    let total = 0;
    for (const row of rows) {
      const status = String(row.status_code || "pending");
      const count = Number(row.count_value || 0);
      byStatus[status] = count;
      total += count;
    }

    return { total, byStatus };
  }

  public async updateRowStatuses(
    migrationId: string,
    objectId: string,
    updates: Array<{ rowIndex: number; status: string; errorMessage?: string }>
  ): Promise<void> {
    await this.initialize();
    const now = new Date().toISOString();

    await this.database.run("BEGIN TRANSACTION");
    try {
      for (const update of updates) {
        if (!update || typeof update.rowIndex !== "number") {
          continue;
        }

        await this.database.run(
          `
            UPDATE migration_stage_rows
            SET status_code = ?, error_message = ?, updated_at = ?
            WHERE migration_id = ? AND object_id = ? AND row_index = ?
          `,
          [
            update.status,
            update.errorMessage || null,
            now,
            migrationId,
            objectId,
            update.rowIndex
          ]
        );
      }

      await this.database.run("COMMIT");
    } catch (error) {
      await this.database.run("ROLLBACK");
      throw error;
    }
  }

  private parseJsonArray(input: string): string[] {
    try {
      const parsed = JSON.parse(input);
      return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
    } catch {
      return [];
    }
  }

  private parseJsonObject(input: string): Record<string, unknown> {
    try {
      const parsed = JSON.parse(input);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? (parsed as Record<string, unknown>)
        : {};
    } catch {
      return {};
    }
  }
}