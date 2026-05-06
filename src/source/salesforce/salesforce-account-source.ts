import { SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { MappingDefinitionEngine } from "../../core/mapping-dsl/mapping-definition-engine";
import { MappingDefinitionParser } from "../../core/mapping-dsl/mapping-definition-parser";
import { AccountMappingEngine } from "../../core/mapping-engine/account-mapping-engine";
import { CanonicalAccount } from "../../types/canonical-account";
import { SalesforceMappingSource } from "./salesforce-mapping-source";

export class SalesforceAccountSource {
  private readonly salesforceClient: SalesforceClient;
  private readonly mappingSource: SalesforceMappingSource;
  private readonly mappingEngine: AccountMappingEngine;
  private readonly mappingDefinitionParser: MappingDefinitionParser;
  private readonly mappingDefinitionEngine: MappingDefinitionEngine;

  public constructor(salesforceClient: SalesforceClient) {
    this.salesforceClient = salesforceClient;
    this.mappingSource = new SalesforceMappingSource(salesforceClient);
    this.mappingEngine = new AccountMappingEngine();
    this.mappingDefinitionParser = new MappingDefinitionParser();
    this.mappingDefinitionEngine = new MappingDefinitionEngine();
  }

  public async getAccounts(
    targetSystem: string,
    lastCheckpoint?: string,
    lastRecordId?: string,
    mappingDefinition?: string
  ): Promise<CanonicalAccount[]> {
    const records = await this.salesforceClient.queryAccounts(lastCheckpoint, lastRecordId);

    if (mappingDefinition?.trim()) {
      const parsedDefinition = this.mappingDefinitionParser.parse(mappingDefinition);

      return await Promise.all(
        records.map(async (record) => {
          const mapped = await this.mappingDefinitionEngine.mapRecord(
            record as unknown as Record<string, unknown>,
            parsedDefinition.lines
          );

          return {
            sourceSystem: "salesforce",
            targetSystem,
            ...mapped.values
          } as CanonicalAccount;
        })
      );
    }

    const loadedMapping = await this.mappingSource.loadAccountMapping(targetSystem, "Upsert");

    return records.map((record) =>
      this.mappingEngine.mapRecord(record, loadedMapping.fieldMappings, targetSystem)
    );
  }
}