import { GenericRecord } from "./generic-record";
import { TransferContext } from "./transfer-context";

export interface SourceAdapter {
  readRecords(context: TransferContext): Promise<GenericRecord[]>;
}
