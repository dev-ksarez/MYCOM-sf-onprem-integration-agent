import oracledb from "oracledb";

export interface OracleConnectionConfig {
  connectString: string;
  user: string;
  password: string;
  poolMax?: number;
  poolMin?: number;
  poolIncrement?: number;
  poolTimeout?: number;
}

export interface OracleQueryResult<T = unknown> {
  rows: T[];
  rowsAffected?: number;
}

export class OracleDatabase {
  private readonly config: OracleConnectionConfig;
  private pool?: oracledb.Pool;

  public constructor(config: OracleConnectionConfig) {
    this.config = config;
  }

  public async connect(): Promise<void> {
    if (this.pool) {
      return;
    }

    this.pool = await oracledb.createPool({
      connectString: this.config.connectString,
      user: this.config.user,
      password: this.config.password,
      poolMax: this.config.poolMax ?? 10,
      poolMin: this.config.poolMin ?? 0,
      poolIncrement: this.config.poolIncrement ?? 1,
      poolTimeout: this.config.poolTimeout ?? 60
    });
  }

  public async testConnection(): Promise<boolean> {
    await this.query("SELECT 1 AS CONNECTION_OK FROM DUAL");
    return true;
  }

  public async query<T = unknown>(
    queryText: string,
    parameters: Record<string, unknown> = {}
  ): Promise<OracleQueryResult<T>> {
    await this.connect();

    if (!this.pool) {
      throw new Error("Oracle connection pool not initialized");
    }

    const connection = await this.pool.getConnection();
    try {
      const result = await connection.execute<T>(
        queryText,
        parameters as oracledb.BindParameters,
        {
          autoCommit: false,
          outFormat: oracledb.OUT_FORMAT_OBJECT
        }
      );

      return {
        rows: (result.rows || []) as T[],
        rowsAffected: result.rowsAffected
      };
    } finally {
      await connection.close();
    }
  }

  public async execute<T = unknown>(
    queryText: string,
    parameters: Record<string, unknown> = {}
  ): Promise<OracleQueryResult<T>> {
    await this.connect();

    if (!this.pool) {
      throw new Error("Oracle connection pool not initialized");
    }

    const connection = await this.pool.getConnection();
    try {
      const result = await connection.execute<T>(
        queryText,
        parameters as oracledb.BindParameters,
        {
          autoCommit: true,
          outFormat: oracledb.OUT_FORMAT_OBJECT
        }
      );

      return {
        rows: (result.rows || []) as T[],
        rowsAffected: result.rowsAffected
      };
    } finally {
      await connection.close();
    }
  }

  public async close(): Promise<void> {
    if (!this.pool) {
      return;
    }

    await this.pool.close(0);
    this.pool = undefined;
  }
}
