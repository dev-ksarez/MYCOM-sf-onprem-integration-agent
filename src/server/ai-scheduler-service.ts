import { ConnectorListItem } from "./admin-data-service";
import { ScheduleMutationInput } from "./admin-data-service";

/**
 * AI-basierte Scheduler-Generation aus Benutzer-Prompts
 * Hybrid-Ansatz: Rule-based + Optional Ollama für Verfeinerung
 */

export interface AISchedulerRequest {
  userPrompt: string;
  connectorId?: string;
  targetSystem?: string;
  objectName?: string;
  existingConnectors: ConnectorListItem[];
}

export interface AIGenerationResult {
  schedule: ScheduleMutationInput;
  connector?: {
    name: string;
    connectorType: string;
    targetSystem?: string;
    parameters?: Record<string, unknown>;
  };
  confidence: number;
  reasoning: string;
  issues: Array<{ severity: "warning" | "error"; message: string }>;
  requiresUserValidation: boolean;
}

export class AISchedulerService {
  /**
   * Haupt-Einstiegspunkt: Generiere Scheduler aus Benutzer-Prompt
   */
  async generateScheduler(request: AISchedulerRequest): Promise<AIGenerationResult> {
    try {
      // Phase 1: Keyword-Analyse
      const analysis = this.analyzePrompt(request.userPrompt);

      // Phase 2: Connector-Matching
      const selectedConnector =
        request.connectorId && this.findConnectorById(request.connectorId, request.existingConnectors)
          ? this.findConnectorById(request.connectorId, request.existingConnectors)!
          : this.matchBestConnector(analysis, request.existingConnectors);

      if (!selectedConnector) {
        return {
          schedule: this.createEmptySchedule(),
          confidence: 0,
          reasoning: "Kein passender Connector gefunden. Benutzerdefinierte Connector-Erstellung erforderlich.",
          issues: [
            {
              severity: "error",
              message: `Kein Connector gefunden, der zu "${analysis.sourceSystem || "unbekannt"}" passt.`
            }
          ],
          requiresUserValidation: true
        };
      }

      // Phase 3: Basis-Scheduler generieren
      const baseSchedule = this.generateBaseSchedule(analysis, selectedConnector, request);
      const confidence = this.calculateConfidence(analysis, selectedConnector);

      // Phase 4: Validation & Issues
      const issues = this.validateScheduleConfiguration(baseSchedule, analysis);
      const requiresValidation = confidence < 0.75 || issues.some((i) => i.severity === "warning");

      return {
        schedule: baseSchedule,
        confidence,
        reasoning: this.buildReasoningText(analysis, selectedConnector, confidence),
        issues,
        requiresUserValidation: requiresValidation
      };
    } catch (error) {
      return {
        schedule: this.createEmptySchedule(),
        confidence: 0,
        reasoning: error instanceof Error ? error.message : "Unbekannter Fehler bei der Generierung",
        issues: [{ severity: "error", message: String(error) }],
        requiresUserValidation: true
      };
    }
  }

  /**
   * Analysiere den Benutzer-Prompt und extrahiere Schlüsselinformationen
   */
  private analyzePrompt(prompt: string): {
    sourceSystem?: string;
    targetSystem?: string;
    objectName?: string;
    operation?: string;
    direction?: "Inbound" | "Outbound";
    timing?: { type: string; value: string };
    confidence: number;
    rawKeywords: string[];
  } {
    const lower = prompt.toLowerCase();
    const keywords = this.extractKeywords(prompt);

    // Source-System erkennen
    const sourceSystem = this.detectSourceSystem(lower, keywords);

    // Target-System erkennen
    const targetSystem = this.detectTargetSystem(lower, keywords);

    // Object-Name erkennen
    const objectName = this.detectObjectName(lower, keywords);

    // Operation erkennen
    const operation = this.detectOperation(lower, keywords);

    // Direction bestimmen
    const direction = this.detectDirection(lower, sourceSystem, targetSystem);

    // Timing erkennen
    const timing = this.extractTiming(prompt);

    // Gesamtvertrauen berechnen
    const confidence = [sourceSystem, targetSystem, objectName, operation, timing]
      .filter((x) => Boolean(x))
      .length / 5;

    return {
      sourceSystem,
      targetSystem,
      objectName,
      operation,
      direction,
      timing,
      confidence,
      rawKeywords: keywords
    };
  }

  /**
   * Extrahiere Wörter und Phrasen aus dem Prompt
   */
  private extractKeywords(prompt: string): string[] {
    const words = prompt.match(/\b\w+\b/gi) || [];
    return [...new Set(words.map((w) => w.toLowerCase()))];
  }

  /**
   * Erkenne das Source-System
   */
  private detectSourceSystem(lower: string, keywords: string[]): string | undefined {
    const sourceKeywords: Record<string, string[]> = {
      MSSQL: ["mssql", "sql", "sqlserver", "database", "oracle", "postgres"],
      REST_API: ["rest", "api", "http", "endpoint", "webhook"],
      Salesforce: ["salesforce", "sfdc", "sf", "crm"],
      FILE: ["file", "csv", "xlsx", "export", "import", "datei"],
      "Sage/SAGE": ["sage", "sage100", "sage50", "erp"]
    };

    for (const [system, patterns] of Object.entries(sourceKeywords)) {
      if (patterns.some((p) => lower.includes(p) || keywords.includes(p))) {
        return system;
      }
    }

    return undefined;
  }

  /**
   * Erkenne das Target-System
   */
  private detectTargetSystem(lower: string, keywords: string[]): string | undefined {
    // "nach", "to", "→" zeigen Target an
    const directionMarkers = lower.match(/(nach|to|→|->|\|)/gi) || [];
    if (directionMarkers.length === 0) {
      return undefined;
    }

    const targetKeywords: Record<string, string[]> = {
      Salesforce: ["salesforce", "sfdc", "sf", "crm"],
      MSSQL: ["mssql", "sql", "database", "oracle"],
      REST_API: ["rest", "api", "http"],
      FILE: ["file", "csv", "xlsx"],
      Brevo: ["brevo", "newsletter", "email"]
    };

    // Suche nach Target-Keyword nach dem Richtungs-Marker
    const lastMarkerIndex = lower.lastIndexOf(directionMarkers[0] || "");
    const afterMarker = lower.substring(Math.max(0, lastMarkerIndex + 1)).toLowerCase();

    for (const [system, patterns] of Object.entries(targetKeywords)) {
      if (patterns.some((p) => afterMarker.includes(p))) {
        return system;
      }
    }

    return undefined;
  }

  /**
   * Erkenne das Object-Name aus dem Prompt
   */
  private detectObjectName(lower: string, keywords: string[]): string | undefined {
    const commonObjects: Record<string, string[]> = {
      Account: ["account", "konto", "accounts"],
      Contact: ["contact", "kontakt", "kontakte", "person"],
      Lead: ["lead", "leads", "interessent"],
      Opportunity: ["opportunity", "deal", "chance"],
      Order: ["order", "bestellung", "auftrag"],
      "Custom Object": ["custom", "objekt"]
    };

    for (const [objectName, patterns] of Object.entries(commonObjects)) {
      if (keywords.some((k) => patterns.includes(k))) {
        return objectName;
      }
    }

    return undefined;
  }

  /**
   * Erkenne die Operation (Insert/Update/Upsert/Delete)
   */
  private detectOperation(lower: string, keywords: string[]): string | undefined {
    if (lower.includes("löschen") || keywords.includes("delete")) return "Delete";
    if (lower.includes("einfügen") || keywords.includes("insert")) return "Insert";
    if (lower.includes("aktualisier") || keywords.includes("update")) return "Update";
    // Upsert ist sicherer Default für die meisten Use-Cases
    return "Upsert";
  }

  /**
   * Bestimme die Richtung (Inbound/Outbound)
   */
  private detectDirection(
    lower: string,
    sourceSystem?: string,
    targetSystem?: string
  ): "Inbound" | "Outbound" {
    // Wenn von Salesforce weg → Outbound
    if (sourceSystem?.toLowerCase().includes("salesforce")) return "Outbound";
    // Wenn zu Salesforce hin → Inbound
    if (targetSystem?.toLowerCase().includes("salesforce")) return "Inbound";
    // Default
    return "Inbound";
  }

  /**
   * Extrahiere Timing-Information aus dem Prompt
   */
  private extractTiming(prompt: string): { type: string; value: string } | undefined {
    // Täglich um 22 Uhr
    const dailyMatch = prompt.match(/täglich.*?(\d{1,2}):?(\d{0,2})/i);
    if (dailyMatch) {
      const hour = dailyMatch[1].padStart(2, "0");
      return { type: "daily", value: `${hour}:00` };
    }

    // Stündlich
    if (prompt.match(/stündlich|hourly|jede stunde/i)) {
      return { type: "hourly", value: "60" };
    }

    // Wöchentlich
    if (prompt.match(/wöchentlich|weekly|jede woche/i)) {
      return { type: "weekly", value: "7days" };
    }

    // Monatlich
    if (prompt.match(/monatlich|monthly|jeden monat/i)) {
      return { type: "monthly", value: "30days" };
    }

    return undefined;
  }

  /**
   * Finde Connector nach ID
   */
  private findConnectorById(id: string, connectors: ConnectorListItem[]): ConnectorListItem | undefined {
    return connectors.find((c) => c.id === id);
  }

  /**
   * Finde besten passenden Connector
   */
  private matchBestConnector(
    analysis: ReturnType<typeof this.analyzePrompt>,
    connectors: ConnectorListItem[]
  ): ConnectorListItem | undefined {
    const scored = connectors.map((connector) => ({
      connector,
      score: this.calculateConnectorMatchScore(connector, analysis)
    }));

    scored.sort((a, b) => b.score - a.score);
    return scored[0]?.score > 0 ? scored[0].connector : undefined;
  }

  /**
   * Berechne Match-Score zwischen Connector und Anforderung
   */
  private calculateConnectorMatchScore(
    connector: ConnectorListItem,
    analysis: ReturnType<typeof this.analyzePrompt>
  ): number {
    let score = 0;

    // Connector-Typ Match
    if (
      analysis.sourceSystem &&
      connector.connectorType?.toLowerCase().includes(analysis.sourceSystem.toLowerCase().split("/")[0])
    ) {
      score += 0.4;
    }

    // Target-System Match
    if (analysis.targetSystem && connector.targetSystem?.toLowerCase().includes(analysis.targetSystem.toLowerCase())) {
      score += 0.3;
    }

    // Aktiv/Getestet
    if (connector.active) score += 0.2;

    // Hat Secret (authentifiziert)
    if (connector.hasSecret) score += 0.1;

    return score;
  }

  /**
   * Generiere Basis-Scheduler
   */
  private generateBaseSchedule(
    analysis: ReturnType<typeof this.analyzePrompt>,
    connector: ConnectorListItem,
    request: AISchedulerRequest
  ): ScheduleMutationInput {
    const timing = this.generateTimingDefinition(analysis.timing);

    return {
      name: this.generateSchedulerName(analysis, connector),
      active: true,
      sourceSystem: analysis.sourceSystem || connector.targetSystem || "External",
      targetSystem: analysis.targetSystem || "Salesforce",
      objectName: analysis.objectName || request.objectName || "Contact",
      operation: analysis.operation || "Upsert",
      direction: analysis.direction || "Inbound",
      sourceType: connector.connectorType || "REST_API",
      targetType: analysis.targetSystem?.toUpperCase() === "SALESFORCE" ? "SALESFORCE" : "REST_API",
      batchSize: this.inferBatchSize(connector),
      connectorId: connector.id,
      timingDefinition: timing,
      sourceDefinition: this.generateSourceDefinition(analysis, connector),
      targetDefinition: this.generateTargetDefinition(analysis),
      mappingDefinition: this.generateMappingDefinition(analysis)
    };
  }

  /**
   * Generiere aussagekräftigen Namen
   */
  private generateSchedulerName(analysis: ReturnType<typeof this.analyzePrompt>, connector: ConnectorListItem): string {
    const parts = [
      analysis.sourceSystem || connector.name,
      "→",
      analysis.targetSystem || "Salesforce",
      analysis.objectName ? `(${analysis.objectName})` : ""
    ];
    return parts.filter(Boolean).join(" ").slice(0, 80);
  }

  /**
   * Generiere Timing-Definition
   */
  private generateTimingDefinition(timing?: { type: string; value: string }): string {
    if (!timing) {
      // Default: täglich 09:00
      return JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: "09:00" });
    }

    const baseConfig = { days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: "09:00" };

    switch (timing.type) {
      case "daily":
        return JSON.stringify({ ...baseConfig, startTime: timing.value });
      case "hourly":
        return JSON.stringify({ days: [1, 2, 3, 4, 5, 6, 7], intervalMinutes: 60 });
      case "weekly":
        return JSON.stringify({ days: [1], intervalMinutes: 60, startTime: "09:00" });
      case "monthly":
        return JSON.stringify({ days: [1], intervalMinutes: 60, startTime: "09:00" });
      default:
        return JSON.stringify(baseConfig);
    }
  }

  /**
   * Inferiere geeignete Batch-Size basierend auf Connector-Typ
   */
  private inferBatchSize(connector: ConnectorListItem): number {
    if (connector.connectorType === "REST_API") return 50; // REST APIs bevorzugen kleinere Batches
    if (connector.connectorType?.includes("MSSQL")) return 100; // Datenbanken können mehr verarbeiten
    if (connector.connectorType?.includes("FILE")) return 500; // Datei-Connectoren sind robust
    return 100; // Default
  }

  /**
   * Generiere Source-Definition (JSON)
   */
  private generateSourceDefinition(
    analysis: ReturnType<typeof this.analyzePrompt>,
    connector: ConnectorListItem
  ): string {
    const baseDefinition: Record<string, unknown> = {};

    if (connector.connectorType === "REST_API") {
      Object.assign(baseDefinition, {
        endpoint: "/v1/data",
        method: "GET",
        responseType: "json",
        pagination: { type: "offset", pageSize: 100 }
      });
    } else if (connector.connectorType?.includes("MSSQL")) {
      Object.assign(baseDefinition, {
        table: analysis.objectName || "Contacts",
        query: `SELECT * FROM ${analysis.objectName || "Contacts"} WHERE LastModifiedDate > @lastSync`,
        parameters: { lastSync: "@lastSync" }
      });
    } else if (connector.connectorType?.includes("FILE")) {
      Object.assign(baseDefinition, {
        filePath: "./import",
        format: "csv",
        encoding: "utf-8"
      });
    }

    return JSON.stringify(baseDefinition, null, 2);
  }

  /**
   * Generiere Target-Definition (JSON)
   */
  private generateTargetDefinition(analysis: ReturnType<typeof this.analyzePrompt>): string {
    if (analysis.targetSystem?.toLowerCase().includes("salesforce")) {
      return JSON.stringify(
        {
          objectApiName: analysis.objectName || "Contact",
          externalIdField: "Id",
          deployOptions: {
            purgeOnDelete: false,
            rollbackOnError: true
          }
        },
        null,
        2
      );
    }

    // Default für andere Systeme
    return JSON.stringify(
      {
        endpoint: "/v1/records",
        method: "POST",
        upsertKey: "Id"
      },
      null,
      2
    );
  }

  /**
   * Generiere Mapping-Definition
   * Format: targetField;dataType=sourceField;TRANSFORMS
   */
  private generateMappingDefinition(analysis: ReturnType<typeof this.analyzePrompt>): string {
    const objectName = analysis.objectName || "Contact";

    // Standard-Mappings basierend auf Object-Type
    const standardMappings: Record<string, string[]> = {
      Account: [
        "Name;string=Name;NONE",
        "Phone;string=Phone;NONE",
        "Website;string=Website;NONE",
        "BillingStreet;string=Street;NONE",
        "BillingCity;string=City;NONE",
        "BillingPostalCode;string=ZipCode;NONE",
        "BillingCountry;string=Country;NONE"
      ],
      Contact: [
        "Email;string=Email;NONE",
        "FirstName;string=FirstName;NONE",
        "LastName;string=LastName;NONE",
        "Phone;string=Phone;NONE",
        "MobilePhone;string=Mobile;NONE",
        "Title;string=JobTitle;NONE"
      ],
      Lead: [
        "Email;string=Email;NONE",
        "FirstName;string=FirstName;NONE",
        "LastName;string=LastName;NONE",
        "Phone;string=Phone;NONE",
        "Company;string=Company;NONE"
      ],
      Order: ["Name;string=OrderNumber;NONE", "Amount;number=Total;NONE", "Status;string=Status;NONE"],
      Opportunity: ["Name;string=OpportunityName;NONE", "Amount;number=Value;NONE", "StageName;string=Stage;NONE"]
    };

    return (standardMappings[objectName] || standardMappings["Contact"]).join("\n");
  }

  /**
   * Berechne Gesamt-Konfidenz
   */
  private calculateConfidence(
    analysis: ReturnType<typeof this.analyzePrompt>,
    selectedConnector: ConnectorListItem
  ): number {
    const analysisConfidence = analysis.confidence * 0.5;
    const connectorConfidence = selectedConnector.active ? 0.5 : 0.25;
    return Math.min(1, analysisConfidence + connectorConfidence);
  }

  /**
   * Validiere die generierte Scheduler-Konfiguration
   */
  private validateScheduleConfiguration(
    schedule: ScheduleMutationInput,
    analysis: ReturnType<typeof this.analyzePrompt>
  ): Array<{ severity: "warning" | "error"; message: string }> {
    const issues: Array<{ severity: "warning" | "error"; message: string }> = [];

    if (!schedule.connectorId) {
      issues.push({ severity: "error", message: "Kein Connector ausgewählt" });
    }

    if (!schedule.objectName || schedule.objectName === "Contact") {
      issues.push({ severity: "warning", message: "Object-Name konnte nicht sicher identifiziert werden" });
    }

    if (analysis.confidence < 0.5) {
      issues.push({
        severity: "warning",
        message: "Konfidenz der Anforderungs-Analyse ist niedrig - bitte überprüfen Sie manuell"
      });
    }

    if (!analysis.timing) {
      issues.push({
        severity: "warning",
        message: "Timing-Information nicht erkannt - nutze Standard (täglich 09:00)"
      });
    }

    return issues;
  }

  /**
   * Erstelle leere Schedule als Fallback
   */
  private createEmptySchedule(): ScheduleMutationInput {
    return {
      name: "Neue Scheduler",
      active: true,
      sourceSystem: "External",
      targetSystem: "Salesforce",
      objectName: "Contact",
      operation: "Upsert",
      direction: "Inbound",
      sourceType: "REST_API",
      targetType: "SALESFORCE",
      batchSize: 100,
      timingDefinition: JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 60, startTime: "09:00" })
    };
  }

  /**
   * Baue verständliche Erklärung
   */
  private buildReasoningText(
    analysis: ReturnType<typeof this.analyzePrompt>,
    connector: ConnectorListItem,
    confidence: number
  ): string {
    const parts: string[] = [];

    if (analysis.sourceSystem) {
      parts.push(`Quelle: ${analysis.sourceSystem}`);
    }
    if (analysis.targetSystem) {
      parts.push(`Ziel: ${analysis.targetSystem}`);
    }
    if (analysis.objectName) {
      parts.push(`Object: ${analysis.objectName}`);
    }
    if (analysis.operation) {
      parts.push(`Operation: ${analysis.operation}`);
    }
    if (connector) {
      parts.push(`Connector: ${connector.name}`);
    }

    parts.push(`Konfidenz: ${(confidence * 100).toFixed(0)}%`);

    return parts.join(" • ");
  }
}
