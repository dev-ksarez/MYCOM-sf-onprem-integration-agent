import { MssqlDatabase } from "../../infrastructure/db/mssql";
import { CanonicalAccount } from "../../types/canonical-account";
import { MappedRecord } from "../../types/mapped-record";
import { JobContext } from "../../types/job-context";

export interface MssqlAccountRow {
  external_key: string;
  source_id: string;
  name: string;
  account_number?: string;
  billing_street?: string;
  billing_postal_code?: string;
  billing_city?: string;
  billing_country?: string;
  phone?: string;
  website?: string;
  last_modified: string;
  source_system: string;
  target_system: string;
  created_at: string;
  updated_at: string;
}

function validateIdentifier(value: string, label: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid MSSQL identifier for ${label}: ${value}`);
  }

  return value;
}

function quoteIdentifier(value: string): string {
  return `[${value}]`;
}

function normalizeParameterValue(value: unknown): unknown {
  if (value === undefined) {
    return null;
  }

  return value;
}

export class MssqlRepository {
  private readonly database: MssqlDatabase;
  private readonly schemaName: string;
  private readonly tableName: string;

  public constructor(database: MssqlDatabase, schemaName: string, tableName: string) {
    this.database = database;
    this.schemaName = validateIdentifier(schemaName, "schemaName");
    this.tableName = validateIdentifier(tableName, "tableName");
  }

  public async ensureSchema(): Promise<void> {
    const qualifiedTableName = `[${this.schemaName}].[${this.tableName}]`;

    await this.database.query(`
      IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name = '${this.tableName}'
          AND s.name = '${this.schemaName}'
      )
      BEGIN
        CREATE TABLE ${qualifiedTableName} (
          external_key NVARCHAR(255) NOT NULL PRIMARY KEY,
          source_id NVARCHAR(255) NOT NULL,
          name NVARCHAR(255) NOT NULL,
          account_number NVARCHAR(255) NULL,
          billing_street NVARCHAR(255) NULL,
          billing_postal_code NVARCHAR(50) NULL,
          billing_city NVARCHAR(255) NULL,
          billing_country NVARCHAR(255) NULL,
          phone NVARCHAR(255) NULL,
          website NVARCHAR(500) NULL,
          last_modified DATETIME2 NOT NULL,
          source_system NVARCHAR(100) NOT NULL,
          target_system NVARCHAR(100) NOT NULL,
          created_at DATETIME2 NOT NULL,
          updated_at DATETIME2 NOT NULL
        )
      END
    `);
  }

  public async findAccountByExternalKey(externalKey: string): Promise<MssqlAccountRow | undefined> {
    const qualifiedTableName = `[${this.schemaName}].[${this.tableName}]`;

    const result = await this.database.execute<MssqlAccountRow>(
      `
        SELECT
          external_key,
          source_id,
          name,
          account_number,
          billing_street,
          billing_postal_code,
          billing_city,
          billing_country,
          phone,
          website,
          last_modified,
          source_system,
          target_system,
          created_at,
          updated_at
        FROM ${qualifiedTableName}
        WHERE external_key = @externalKey
      `,
      { externalKey }
    );

    return result.recordset[0];
  }

  public async findMappedRecordByUpsertKey(
    upsertKey: string,
    upsertValue: unknown
  ): Promise<Record<string, unknown> | undefined> {
    const validatedUpsertKey = validateIdentifier(upsertKey, "upsertKey");
    const qualifiedTableName = `${quoteIdentifier(this.schemaName)}.${quoteIdentifier(this.tableName)}`;

    const result = await this.database.execute<Record<string, unknown>>(
      `
        SELECT *
        FROM ${qualifiedTableName}
        WHERE ${quoteIdentifier(validatedUpsertKey)} = @upsertValue
      `,
      { upsertValue: normalizeParameterValue(upsertValue) }
    );

    return result.recordset[0];
  }

  public async upsertAccount(account: CanonicalAccount): Promise<"INSERTED" | "UPDATED"> {
    const existing = await this.findAccountByExternalKey(account.externalKey);
    const qualifiedTableName = `[${this.schemaName}].[${this.tableName}]`;
    const now = new Date().toISOString();

    const parameters = {
      externalKey: account.externalKey,
      sourceId: account.sourceId,
      name: account.name,
      accountNumber: account.accountNumber || null,
      billingStreet: account.billingStreet || null,
      billingPostalCode: account.billingPostalCode || null,
      billingCity: account.billingCity || null,
      billingCountry: account.billingCountry || null,
      phone: account.phone || null,
      website: account.website || null,
      lastModified: account.lastModified,
      sourceSystem: account.sourceSystem,
      targetSystem: account.targetSystem,
      now
    };

    if (existing) {
      await this.database.execute(
        `
          UPDATE ${qualifiedTableName}
          SET
            source_id = @sourceId,
            name = @name,
            account_number = @accountNumber,
            billing_street = @billingStreet,
            billing_postal_code = @billingPostalCode,
            billing_city = @billingCity,
            billing_country = @billingCountry,
            phone = @phone,
            website = @website,
            last_modified = @lastModified,
            source_system = @sourceSystem,
            target_system = @targetSystem,
            updated_at = @now
          WHERE external_key = @externalKey
        `,
        parameters
      );

      return "UPDATED";
    }

    await this.database.execute(
      `
        INSERT INTO ${qualifiedTableName} (
          external_key,
          source_id,
          name,
          account_number,
          billing_street,
          billing_postal_code,
          billing_city,
          billing_country,
          phone,
          website,
          last_modified,
          source_system,
          target_system,
          created_at,
          updated_at
        )
        VALUES (
          @externalKey,
          @sourceId,
          @name,
          @accountNumber,
          @billingStreet,
          @billingPostalCode,
          @billingCity,
          @billingCountry,
          @phone,
          @website,
          @lastModified,
          @sourceSystem,
          @targetSystem,
          @now,
          @now
        )
      `,
      parameters
    );

    return "INSERTED";
  }

  public async upsertMappedRecord(
    record: MappedRecord,
    upsertKey: string
  ): Promise<"INSERTED" | "UPDATED"> {
    const validatedUpsertKey = validateIdentifier(upsertKey, "upsertKey");
    const qualifiedTableName = `${quoteIdentifier(this.schemaName)}.${quoteIdentifier(this.tableName)}`;
    const inputValues = record.values;
    const upsertValue = inputValues[validatedUpsertKey];

    if (upsertValue === undefined || upsertValue === null || upsertValue === "") {
      throw new Error(`Mapped record is missing required upsert key: ${validatedUpsertKey}`);
    }

    const columnEntries = Object.entries(inputValues)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => ({
        columnName: validateIdentifier(key, `mapped field ${key}`),
        value: normalizeParameterValue(value)
      }));

    if (columnEntries.length === 0) {
      throw new Error("Mapped record does not contain any writable fields");
    }

    const existing = await this.findMappedRecordByUpsertKey(validatedUpsertKey, upsertValue);

    const parameters = Object.fromEntries(
      columnEntries.map(({ columnName, value }) => [columnName, value])
    );

    if (existing) {
      const updateEntries = columnEntries.filter(({ columnName }) => columnName !== validatedUpsertKey);

      if (updateEntries.length === 0) {
        return "UPDATED";
      }

      const updateAssignments = updateEntries
        .map(({ columnName }) => `${quoteIdentifier(columnName)} = @${columnName}`)
        .join(",\n            ");

      await this.database.execute(
        `
          UPDATE ${qualifiedTableName}
          SET
            ${updateAssignments}
          WHERE ${quoteIdentifier(validatedUpsertKey)} = @${validatedUpsertKey}
        `,
        parameters
      );

      return "UPDATED";
    }

    const insertColumns = columnEntries.map(({ columnName }) => quoteIdentifier(columnName)).join(", ");
    const insertValues = columnEntries.map(({ columnName }) => `@${columnName}`).join(", ");

    await this.database.execute(
      `
        INSERT INTO ${qualifiedTableName} (
          ${insertColumns}
        )
        VALUES (
          ${insertValues}
        )
      `,
      parameters
    );

    return "INSERTED";
  }

  public async writeOperationLog(
    _context: JobContext,
    _externalKey: string,
    _operation: string,
    _statusCode: string,
    _message?: string
  ): Promise<void> {
    return Promise.resolve();
  }
}