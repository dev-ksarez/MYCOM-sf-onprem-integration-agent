export interface DatabaseColumnMetadata {
  name: string;
  label?: string;
  type: string;
  nullable?: boolean;
  primaryKey?: boolean;
  ordinal?: number;
  length?: number;
  precision?: number;
  scale?: number;
}

export interface DatabaseTableMetadata {
  name: string;
  schema?: string;
  label?: string;
  type?: string;
  columns: DatabaseColumnMetadata[];
}

export interface DatabaseMetadata {
  connectorId: string;
  connectorName: string;
  connectorType: string;
  databaseName?: string;
  refreshedAt: string;
  tables: DatabaseTableMetadata[];
}
