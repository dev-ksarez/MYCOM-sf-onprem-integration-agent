import { CanonicalAccount } from "../../types/canonical-account";
import { JobContext } from "../../types/job-context";
import { SqliteDatabase } from "../../infrastructure/db/sqlite";

export interface MockAccountRow {
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

export interface MockErrorRuleRow {
  id: number;
  external_key: string;
  error_type: string;
  message: string;
  retryable: number;
  is_active: number;
  created_at: string;
  updated_at: string;
}

export class MockRepository {
  private readonly database: SqliteDatabase;

  public constructor(database: SqliteDatabase) {
    this.database = database;
  }

  public async initialize(): Promise<void> {
    await this.database.initialize();
  }

  public async findAccountByExternalKey(externalKey: string): Promise<MockAccountRow | undefined> {
    return this.database.get<MockAccountRow>(
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
        FROM mock_accounts
        WHERE external_key = ?
      `,
      [externalKey]
    );
  }

  public async upsertAccount(account: CanonicalAccount): Promise<"INSERTED" | "UPDATED"> {
    const existing = await this.findAccountByExternalKey(account.externalKey);
    const now = new Date().toISOString();

    if (existing) {
      await this.database.run(
        `
          UPDATE mock_accounts
          SET
            source_id = ?,
            name = ?,
            account_number = ?,
            billing_street = ?,
            billing_postal_code = ?,
            billing_city = ?,
            billing_country = ?,
            phone = ?,
            website = ?,
            last_modified = ?,
            source_system = ?,
            target_system = ?,
            updated_at = ?
          WHERE external_key = ?
        `,
        [
          account.sourceId,
          account.name,
          account.accountNumber || null,
          account.billingStreet || null,
          account.billingPostalCode || null,
          account.billingCity || null,
          account.billingCountry || null,
          account.phone || null,
          account.website || null,
          account.lastModified,
          account.sourceSystem,
          account.targetSystem,
          now,
          account.externalKey
        ]
      );

      return "UPDATED";
    }

    await this.database.run(
      `
        INSERT INTO mock_accounts (
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        account.externalKey,
        account.sourceId,
        account.name,
        account.accountNumber || null,
        account.billingStreet || null,
        account.billingPostalCode || null,
        account.billingCity || null,
        account.billingCountry || null,
        account.phone || null,
        account.website || null,
        account.lastModified,
        account.sourceSystem,
        account.targetSystem,
        now,
        now
      ]
    );

    return "INSERTED";
  }

  public async findActiveErrorRule(externalKey: string): Promise<MockErrorRuleRow | undefined> {
    return this.database.get<MockErrorRuleRow>(
      `
        SELECT
          id,
          external_key,
          error_type,
          message,
          retryable,
          is_active,
          created_at,
          updated_at
        FROM mock_error_rules
        WHERE external_key = ?
          AND is_active = 1
        ORDER BY id ASC
        LIMIT 1
      `,
      [externalKey]
    );
  }

  public async writeOperationLog(
    context: JobContext,
    externalKey: string,
    operation: string,
    statusCode: string,
    message?: string
  ): Promise<void> {
    await this.database.run(
      `
        INSERT INTO mock_operation_log (
          run_id,
          correlation_id,
          external_key,
          operation,
          status_code,
          message,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      [
        context.runId,
        context.correlationId,
        externalKey,
        operation,
        statusCode,
        message || null,
        new Date().toISOString()
      ]
    );
  }
}