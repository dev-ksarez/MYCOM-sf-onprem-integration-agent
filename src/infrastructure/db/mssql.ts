

import sql from "mssql";

export interface MssqlConnectionConfig {
  server: string;
  port?: number;
  database: string;
  user: string;
  password: string;
  encrypt?: boolean;
  trustServerCertificate?: boolean;
  connectionTimeout?: number;
  requestTimeout?: number;
  poolMax?: number;
  poolMin?: number;
  poolIdleTimeoutMillis?: number;
}

export class MssqlDatabase {
  private readonly config: MssqlConnectionConfig;
  private pool?: sql.ConnectionPool;

  public constructor(config: MssqlConnectionConfig) {
    this.config = config;
  }

  public async connect(): Promise<void> {
    if (this.pool?.connected) {
      return;
    }

    const connectionConfig: sql.config = {
      server: this.config.server,
      port: this.config.port || 1433,
      database: this.config.database,
      user: this.config.user,
      password: this.config.password,
      options: {
        encrypt: this.config.encrypt ?? false,
        trustServerCertificate: this.config.trustServerCertificate ?? true
      },
      connectionTimeout: this.config.connectionTimeout ?? 30000,
      requestTimeout: this.config.requestTimeout ?? 30000,
      pool: {
        max: this.config.poolMax ?? 10,
        min: this.config.poolMin ?? 0,
        idleTimeoutMillis: this.config.poolIdleTimeoutMillis ?? 30000
      }
    };

    this.pool = await new sql.ConnectionPool(connectionConfig).connect();
  }

  public async testConnection(): Promise<boolean> {
    await this.connect();
    await this.query("SELECT 1 AS connection_ok");
    return true;
  }

  public async query<T = unknown>(queryText: string): Promise<sql.IResult<T>> {
    await this.connect();

    if (!this.pool) {
      throw new Error("MSSQL connection pool not initialized");
    }

    return this.pool.request().query<T>(queryText);
  }

  public async execute<T = unknown>(
    queryText: string,
    parameters: Record<string, unknown>
  ): Promise<sql.IResult<T>> {
    await this.connect();

    if (!this.pool) {
      throw new Error("MSSQL connection pool not initialized");
    }

    const request = this.pool.request();

    for (const [key, value] of Object.entries(parameters)) {
      request.input(key, value as sql.ISqlTypeFactoryWithNoParams | unknown);
    }

    return request.query<T>(queryText);
  }

  public async close(): Promise<void> {
    if (!this.pool) {
      return;
    }

    await this.pool.close();
    this.pool = undefined;
  }
}