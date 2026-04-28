

import {
  FieldMappingData,
  ObjectMappingData,
  SalesforceClient
} from "../../clients/salesforce/salesforce-client";

export interface LoadedAccountMapping {
  objectMapping: ObjectMappingData;
  fieldMappings: FieldMappingData[];
}

export class SalesforceMappingSource {
  private readonly salesforceClient: SalesforceClient;

  public constructor(salesforceClient: SalesforceClient) {
    this.salesforceClient = salesforceClient;
  }

  public async loadAccountMapping(
    targetSystem: string,
    operation: string
  ): Promise<LoadedAccountMapping> {
    const objectMappings = await this.salesforceClient.queryObjectMappings(
      "Account",
      targetSystem,
      operation
    );

    if (objectMappings.length === 0) {
      throw new Error(
        `No active object mapping found for sourceObject=Account, targetSystem=${targetSystem}, operation=${operation}`
      );
    }

    if (objectMappings.length > 1) {
      throw new Error(
        `Multiple active object mappings found for sourceObject=Account, targetSystem=${targetSystem}, operation=${operation}`
      );
    }

    const objectMapping = objectMappings[0];
    const fieldMappings = await this.salesforceClient.queryFieldMappings(objectMapping.developerName);

    if (fieldMappings.length === 0) {
      throw new Error(`No active field mappings found for object mapping ${objectMapping.developerName}`);
    }

    return {
      objectMapping,
      fieldMappings
    };
  }
}