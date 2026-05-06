import { SalesforceClient } from "../../clients/salesforce/salesforce-client";
import { IntegrationSchedule } from "../../types/integration-schedule";

function extractHierarchySettings(targetDefinition?: string): {
  parentScheduleId?: string;
  inheritTimingFromParent?: boolean;
} {
  const raw = String(targetDefinition || "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const candidate = parsed as Record<string, unknown>;
    const parentScheduleId =
      typeof candidate.parentScheduleId === "string" && candidate.parentScheduleId.trim()
        ? candidate.parentScheduleId.trim()
        : undefined;
    const inheritTimingFromParent = candidate.inheritTimingFromParent === true;

    return {
      parentScheduleId,
      inheritTimingFromParent
    };
  } catch {
    return {};
  }
}

function extractTimingDefinition(targetDefinition?: string): string | undefined {
  const raw = String(targetDefinition || "").trim();
  if (!raw) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return undefined;
    }

    const timingDefinition = (parsed as Record<string, unknown>).timingDefinition;
    return typeof timingDefinition === "string" && timingDefinition.trim() ? timingDefinition.trim() : undefined;
  } catch {
    return undefined;
  }
}

export class SalesforceScheduleSource {
  private readonly salesforceClient: SalesforceClient;

  public constructor(salesforceClient: SalesforceClient) {
    this.salesforceClient = salesforceClient;
  }

  public async getActiveSchedules(): Promise<IntegrationSchedule[]> {
    const records = await this.salesforceClient.querySchedules();

    return records.map((record) => ({
      ...extractHierarchySettings(record.MSD_TargetDefinition__c),
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
      lastRunAt: record.LastRunAt__c,
      timingDefinition: extractTimingDefinition(record.MSD_TargetDefinition__c)
    }));
  }
}