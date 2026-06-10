import { OracleDatabase } from "../../infrastructure/db/oracle";
import { CanonicalAccount } from "../../types/canonical-account";
import { JobContext } from "../../types/job-context";
import { MappedRecord } from "../../types/mapped-record";

export interface OracleAccountRow {
  EXTERNAL_KEY: string;
  SOURCE_ID: string;
  NAME: string;
  ACCOUNT_NUMBER?: string;
  BILLING_STREET?: string;
  BILLING_POSTAL_CODE?: string;
  BILLING_CITY?: string;
  BILLING_COUNTRY?: string;
  PHONE?: string;
  WEBSITE?: string;
  LAST_MODIFIED: string;
  SOURCE_SYSTEM: string;
  TARGET_SYSTEM: string;
  CREATED_AT: string;
  UPDATED_AT: string;
}

function validateIdentifier(value: string, label: string): string {
  const normalized = String(value || "").trim().toUpperCase();
  if (!/^[A-Z][A-Z0-9_#$]*$/.test(normalized)) {
    throw new Error(`Invalid Oracle identifier for ${label}: ${value}`);
  }

  return normalized;
}

function quoteIdentifier(value: string): string {
  return `"${value}"`;
}

function normalizeParameterValue(value: unknown): unknown {
  return value === undefined ? null : value;
}

function normalizeFieldKey(value: string): string {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function hasMappedValue(value: unknown): boolean {
  return value !== undefined && value !== null && value !== "";
}

function normalizeMappedInputValues(
  inputValues: Record<string, unknown>,
  requiredKey: string
): Record<string, unknown> {
  const normalizedRequiredKey = normalizeFieldKey(requiredKey);
  if (!normalizedRequiredKey) {
    return inputValues;
  }

  const normalizedValues: Record<string, unknown> = {};
  let exactValue: unknown;
  let exactKeyPresent = false;
  let fallbackValue: unknown;
  let foundEquivalentKey = false;

  for (const [key, value] of Object.entries(inputValues)) {
    const normalizedKey = validateIdentifier(key, `mapped field ${key}`);
    if (normalizedKey === requiredKey) {
      exactValue = value;
      exactKeyPresent = true;
      continue;
    }

    if (normalizeFieldKey(normalizedKey) === normalizedRequiredKey) {
      foundEquivalentKey = true;
      if (!hasMappedValue(fallbackValue) && hasMappedValue(value)) {
        fallbackValue = value;
      }
      continue;
    }

    normalizedValues[normalizedKey] = value;
  }

  if (!exactKeyPresent && !foundEquivalentKey) {
    return normalizedValues;
  }

  normalizedValues[requiredKey] = hasMappedValue(exactValue) ? exactValue : fallbackValue ?? exactValue;
  return normalizedValues;
}

export class OracleRepository {
  private readonly database: OracleDatabase;
  private readonly schemaName: string;
  private readonly tableName: string;

  public constructor(database: OracleDatabase, schemaName: string, tableName: string) {
    this.database = database;
    this.schemaName = validateIdentifier(schemaName, "schemaName");
    this.tableName = validateIdentifier(tableName, "tableName");
  }

  private qualifiedTableName(): string {
    return `${quoteIdentifier(this.schemaName)}.${quoteIdentifier(this.tableName)}`;
  }

  public async ensureSchema(): Promise<void> {
    const exists = await this.database.query<{ TABLE_NAME: string }>(
      `
        SELECT TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER = :schemaName
          AND TABLE_NAME = :tableName
      `,
      {
        schemaName: this.schemaName,
        tableName: this.tableName
      }
    );

    if (exists.rows.length > 0) {
      return;
    }

    await this.database.execute(`
      CREATE TABLE ${this.qualifiedTableName()} (
        "EXTERNAL_KEY" VARCHAR2(255) NOT NULL PRIMARY KEY,
        "SOURCE_ID" VARCHAR2(255) NOT NULL,
        "NAME" VARCHAR2(255) NOT NULL,
        "ACCOUNT_NUMBER" VARCHAR2(255) NULL,
        "BILLING_STREET" VARCHAR2(255) NULL,
        "BILLING_POSTAL_CODE" VARCHAR2(50) NULL,
        "BILLING_CITY" VARCHAR2(255) NULL,
        "BILLING_COUNTRY" VARCHAR2(255) NULL,
        "PHONE" VARCHAR2(255) NULL,
        "WEBSITE" VARCHAR2(500) NULL,
        "LAST_MODIFIED" TIMESTAMP NOT NULL,
        "SOURCE_SYSTEM" VARCHAR2(100) NOT NULL,
        "TARGET_SYSTEM" VARCHAR2(100) NOT NULL,
        "CREATED_AT" TIMESTAMP NOT NULL,
        "UPDATED_AT" TIMESTAMP NOT NULL
      )
    `);
  }

  public async findAccountByExternalKey(externalKey: string): Promise<OracleAccountRow | undefined> {
    const result = await this.database.query<OracleAccountRow>(
      `
        SELECT *
        FROM ${this.qualifiedTableName()}
        WHERE "EXTERNAL_KEY" = :externalKey
      `,
      { externalKey }
    );

    return result.rows[0];
  }

  public async findMappedRecordByUpsertKey(
    upsertKey: string,
    upsertValue: unknown
  ): Promise<Record<string, unknown> | undefined> {
    const validatedUpsertKey = validateIdentifier(upsertKey, "upsertKey");
    const result = await this.database.query<Record<string, unknown>>(
      `
        SELECT *
        FROM ${this.qualifiedTableName()}
        WHERE ${quoteIdentifier(validatedUpsertKey)} = :upsertValue
      `,
      { upsertValue: normalizeParameterValue(upsertValue) }
    );

    return result.rows[0];
  }

  public async upsertAccount(account: CanonicalAccount): Promise<"INSERTED" | "UPDATED"> {
    const existing = await this.findAccountByExternalKey(account.externalKey);
    const now = new Date();
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
      lastModified: new Date(account.lastModified),
      sourceSystem: account.sourceSystem,
      targetSystem: account.targetSystem,
      now
    };

    if (existing) {
      await this.database.execute(
        `
          UPDATE ${this.qualifiedTableName()}
          SET
            "SOURCE_ID" = :sourceId,
            "NAME" = :name,
            "ACCOUNT_NUMBER" = :accountNumber,
            "BILLING_STREET" = :billingStreet,
            "BILLING_POSTAL_CODE" = :billingPostalCode,
            "BILLING_CITY" = :billingCity,
            "BILLING_COUNTRY" = :billingCountry,
            "PHONE" = :phone,
            "WEBSITE" = :website,
            "LAST_MODIFIED" = :lastModified,
            "SOURCE_SYSTEM" = :sourceSystem,
            "TARGET_SYSTEM" = :targetSystem,
            "UPDATED_AT" = :now
          WHERE "EXTERNAL_KEY" = :externalKey
        `,
        parameters
      );
      return "UPDATED";
    }

    await this.database.execute(
      `
        INSERT INTO ${this.qualifiedTableName()} (
          "EXTERNAL_KEY", "SOURCE_ID", "NAME", "ACCOUNT_NUMBER", "BILLING_STREET",
          "BILLING_POSTAL_CODE", "BILLING_CITY", "BILLING_COUNTRY", "PHONE", "WEBSITE",
          "LAST_MODIFIED", "SOURCE_SYSTEM", "TARGET_SYSTEM", "CREATED_AT", "UPDATED_AT"
        )
        VALUES (
          :externalKey, :sourceId, :name, :accountNumber, :billingStreet,
          :billingPostalCode, :billingCity, :billingCountry, :phone, :website,
          :lastModified, :sourceSystem, :targetSystem, :now, :now
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
    const inputValues = normalizeMappedInputValues(record.values, validatedUpsertKey);
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
    const parameters = Object.fromEntries(columnEntries.map(({ columnName, value }) => [columnName, value]));

    if (existing) {
      const updateEntries = columnEntries.filter(({ columnName }) => columnName !== validatedUpsertKey);
      if (updateEntries.length === 0) {
        return "UPDATED";
      }

      const updateAssignments = updateEntries
        .map(({ columnName }) => `${quoteIdentifier(columnName)} = :${columnName}`)
        .join(",\n            ");

      await this.database.execute(
        `
          UPDATE ${this.qualifiedTableName()}
          SET
            ${updateAssignments}
          WHERE ${quoteIdentifier(validatedUpsertKey)} = :${validatedUpsertKey}
        `,
        parameters
      );

      return "UPDATED";
    }

    const insertColumns = columnEntries.map(({ columnName }) => quoteIdentifier(columnName)).join(", ");
    const insertValues = columnEntries.map(({ columnName }) => `:${columnName}`).join(", ");

    await this.database.execute(
      `
        INSERT INTO ${this.qualifiedTableName()} (
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
