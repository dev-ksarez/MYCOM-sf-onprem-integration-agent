

import { SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { GenericRecord } from "../../types/generic-record";
import { SourceAdapter } from "../../types/source-adapter";
import { TransferContext } from "../../types/transfer-context";
import {
  DeltaConfig,
  getRecordIdentifier,
  getRecordValueByField,
  normalizeCheckpointValue,
  parseQuerySourceDefinition
} from "../../utils/query-source-definition";

function formatSoqlDateTime(value: string): string {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function escapeSoqlString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function buildDeltaOrderedSoql(queryText: string, delta: DeltaConfig, checkpointValue?: string, recordId?: string): string {
  const baseQuery = queryText.replace(/;\s*$/, "").trim();
  const limitMatch = baseQuery.match(/\s+LIMIT\s+\d+\s*$/i);
  const limitClause = limitMatch ? limitMatch[0].trim() : "";
  const withoutLimit = limitMatch ? baseQuery.slice(0, limitMatch.index).trimEnd() : baseQuery;
  const orderMatch = withoutLimit.match(/\s+ORDER\s+BY\s+[\s\S]*$/i);
  const withoutOrder = orderMatch ? withoutLimit.slice(0, orderMatch.index).trimEnd() : withoutLimit;
  let filteredQuery = withoutOrder;

  if (checkpointValue) {
    const connector = /\bWHERE\b/i.test(withoutOrder) ? " AND " : " WHERE ";
    let condition: string;
    if (delta.strategy === "datetime") {
      const formattedCheckpoint = formatSoqlDateTime(checkpointValue);
      condition = recordId
        ? `(${delta.field} > ${formattedCheckpoint} OR (${delta.field} = ${formattedCheckpoint} AND Id > '${escapeSoqlString(recordId)}'))`
        : `${delta.field} > ${formattedCheckpoint}`;
    } else {
      condition = `${delta.field} > '${escapeSoqlString(checkpointValue)}'`;
    }
    filteredQuery = `${withoutOrder}${connector}${condition}`;
  }

  const orderClause = delta.strategy === "datetime"
    ? `ORDER BY ${delta.field} ASC, Id ASC`
    : `ORDER BY ${delta.field} ASC`;
  return `${filteredQuery} ${orderClause}${limitClause ? ` ${limitClause}` : ""}`.trim();
}

export class SalesforceSoqlSourceAdapter implements SourceAdapter {
  private readonly salesforceClient: SalesforceClient;
  private readonly definition: ReturnType<typeof parseQuerySourceDefinition>;

  public constructor(salesforceClient: SalesforceClient, soql: string) {
    this.salesforceClient = salesforceClient;
    this.definition = parseQuerySourceDefinition(soql);
  }

  public async readRecords(context: TransferContext): Promise<GenericRecord[]> {
    const delta = this.definition.delta;
    const checkpointCursor = delta?.strategy === "datetime"
      ? context.checkpoint?.value
      : context.checkpoint?.recordId || context.checkpoint?.value;
    const checkpointRecordId = delta?.strategy === "datetime" ? context.checkpoint?.recordId : undefined;
    const queryText = delta
      ? buildDeltaOrderedSoql(this.definition.queryText, delta, checkpointCursor, checkpointRecordId)
      : this.definition.queryText;
    const queryResult = await this.salesforceClient.queryGeneric(queryText);

    return queryResult.map((record) => ({
      values: record as Record<string, unknown>,
      checkpoint: delta
        ? (() => {
            const rawRecord = record as Record<string, unknown>;
            const checkpointValue = normalizeCheckpointValue(getRecordValueByField(rawRecord, delta.field));
            if (!checkpointValue) {
              return undefined;
            }
            return {
              value: checkpointValue,
              recordId: getRecordIdentifier(rawRecord)
            };
          })()
        : undefined
    }));
  }
}