import {
  ConnectorListItem,
  InstanceMetadataContext,
  Sage100DocumentationContext,
  ScheduleListItem
} from "./admin-data-service";

export interface AISchedulerKnowledgeInput {
  connectors: ConnectorListItem[];
  schedules: ScheduleListItem[];
  metadataContext?: InstanceMetadataContext;
  sage100DocumentationContext?: Sage100DocumentationContext;
}

function limitString(value: unknown, maxLength: number): string {
  const text = String(value || "").trim();
  return text.length > maxLength ? `${text.substring(0, maxLength)}...` : text;
}

function sanitizeConnector(connector: ConnectorListItem): Record<string, unknown> {
  return {
    id: connector.id,
    name: connector.name,
    connectorType: connector.connectorType,
    targetSystem: connector.targetSystem,
    direction: connector.direction,
    active: connector.active
  };
}

function summarizeSchedule(schedule: ScheduleListItem): Record<string, unknown> {
  return {
    id: schedule.id,
    name: schedule.name,
    sourceSystem: schedule.sourceSystem,
    targetSystem: schedule.targetSystem,
    objectName: schedule.objectName,
    operation: schedule.operation,
    direction: schedule.direction,
    sourceType: schedule.sourceType,
    targetType: schedule.targetType,
    connectorId: schedule.connectorId,
    mappingDefinition: limitString(schedule.mappingDefinition, 1200),
    sourceDefinition: limitString(schedule.sourceDefinition, 1200),
    targetDefinition: limitString(schedule.targetDefinition, 1200)
  };
}

export function buildSchedulerAIKnowledge(input: AISchedulerKnowledgeInput): string {
  const metadata = input.metadataContext;
  const metadataObjects = (metadata?.objects || []).slice(0, 20).map((object) => {
    const fields = (metadata?.fieldsByObject?.[object.objectName] || []).slice(0, 80).map((field) => ({
      name: field.name,
      label: field.label,
      type: field.type,
      required: field.required,
      externalId: field.externalId,
      createable: field.createable,
      updateable: field.updateable,
      referenceTo: field.referenceTo
    }));
    return {
      objectName: object.objectName,
      label: object.label,
      queryable: object.queryable,
      fields
    };
  });

  const sage = input.sage100DocumentationContext;
  const knowledge = {
    connectoren: input.connectors.slice(0, 30).map(sanitizeConnector),
    bestehendeScheduler: input.schedules.slice(0, 20).map(summarizeSchedule),
    salesforceMetadaten: metadata ? {
      status: metadata.snapshot?.status,
      refreshedAt: metadata.snapshot?.refreshedAt,
      objectCount: metadata.snapshot?.objectCount,
      fieldCount: metadata.snapshot?.fieldCount,
      objects: metadataObjects
    } : null,
    sage100Dokumentation: sage ? {
      generatedAt: sage.generatedAt,
      sourceFile: sage.sourceFile,
      tableCount: sage.tableCount,
      matchedTables: (sage.matchedTables || []).slice(0, 12)
    } : null,
    regeln: [
      "Salesforce-Zielfelder muessen in den Metadaten existieren, wenn Metadaten vorhanden sind.",
      "Connector-IDs duerfen nur aus der Connector-Liste verwendet werden.",
      "Secrets, Passwoerter und Tokens duerfen nie in SourceDefinition, TargetDefinition oder MappingDefinition geschrieben werden.",
      "Bestehende erfolgreiche Mapping-Konventionen duerfen als Beispiele genutzt werden.",
      "Bei unklaren Feldern Warnungen ausgeben und die regelbasierte Basis-Konfiguration nicht verschlechtern."
    ]
  };

  return JSON.stringify(knowledge, null, 2);
}
