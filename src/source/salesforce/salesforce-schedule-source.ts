import { SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { IntegrationSchedule } from "../../types/integration-schedule";

export class SalesforceScheduleSource {
  private readonly salesforceClient: SalesforceClient;

  public constructor(salesforceClient: SalesforceClient) {
    this.salesforceClient = salesforceClient;
  }

  public async getActiveSchedules(): Promise<IntegrationSchedule[]> {
    const records = await this.salesforceClient.querySchedules();

    return records.map((record) => ({
      id: record.Id,
      name: record.Name,
      active: record.Active__c,
      sourceSystem: record.SourceSystem__c || "",
      targetSystem: record.TargetSystem__c || "",
      objectName: record.ObjectName__c || "",
      operation: record.Operation__c || "",
      connectorId: record.MSD_Connector__c,
      mappingDefinition: record.MSD_MappingDefinition__c,
      direction: record.MSD_Direction__c,
      sourceType: record.MSD_SourceType__c,
      targetType: record.MSD_TargetType__c,
      sourceDefinition: record.MSD_SourceDefinition__c,
      targetDefinition: record.MSD_TargetDefinition__c,
      batchSize: record.BatchSize__c || 100,
      nextRunAt: record.NextRunAt__c,
      lastRunAt: record.LastRunAt__c
    }));
  }
}