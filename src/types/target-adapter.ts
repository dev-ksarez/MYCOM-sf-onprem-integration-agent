

import { ConnectorResult } from "./connector-result";
import { GenericRecord } from "./generic-record";
import { TransferContext } from "./transfer-context";

export interface TargetAdapter {
  writeRecords(records: GenericRecord[], context: TransferContext): Promise<ConnectorResult[]>;
}