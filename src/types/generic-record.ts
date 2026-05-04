import { SourceRecordCheckpoint } from "../utils/query-source-definition";

export interface GenericRecord {
  values: Record<string, unknown>;
  checkpoint?: SourceRecordCheckpoint;
}
