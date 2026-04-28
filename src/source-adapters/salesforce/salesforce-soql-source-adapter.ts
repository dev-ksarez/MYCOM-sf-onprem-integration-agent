

import { SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";

export class SalesforceSoqlSourceAdapter implements SourceAdapter {
  private readonly salesforceClient: SalesforceClient;
  private readonly soql: string;

  public constructor(salesforceClient: SalesforceClient, soql: string) {
    this.salesforceClient = salesforceClient;
    this.soql = soql;
  }

  public async readRecords(_context: TransferContext): Promise<GenericRecord[]> {
    const queryResult = await this.salesforceClient.queryGeneric(this.soql);

    return queryResult.map((record) => ({
      values: record as Record<string, unknown>
    }));
  }
}