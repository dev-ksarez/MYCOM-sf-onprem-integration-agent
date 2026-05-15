import {
  ConnectorListItem,
  InstanceMetadataContext,
  PersistedMetadataField,
  Sage100DocumentationContext
} from "./admin-data-service";
import { ScheduleMutationInput } from "./admin-data-service";

/**
 * AI-basierte Scheduler-Generation aus Benutzer-Prompts
 * Hybrid-Ansatz: Rule-based + Optional Ollama für Verfeinerung
 */

interface MappingFieldCandidate {
  name: string;
  label?: string;
  type?: string;
  required?: boolean;
  externalId?: boolean;
  description?: string;
  source: "salesforce" | "sage100";
}

interface AISchedulerTiming {
  type: string;
  value: string;
  intervalMinutes?: number;
  days?: number[];
  startTime?: string;
}

export interface AISchedulerRequest {
  userPrompt: string;
  connectorId?: string;
  targetSystem?: string;
  objectName?: string;
  existingConnectors: ConnectorListItem[];
  metadataContext?: InstanceMetadataContext;
  sage100DocumentationContext?: Sage100DocumentationContext;
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
  metadataBasis?: {
    salesforce?: {
      status: "success" | "error" | "running" | "missing";
      refreshedAt?: string;
      objectCount?: number;
      fieldCount?: number;
      instanceId?: string;
      projectId?: string;
    };
    sage100?: {
      generatedAt?: string;
      sourceFile?: string;
      tableCount?: number;
      matchedTables?: string[];
    };
  };
}

export class AISchedulerService {
  /**
   * Haupt-Einstiegspunkt: Generiere Scheduler aus Benutzer-Prompt
   */
  async generateScheduler(request: AISchedulerRequest): Promise<AIGenerationResult> {
    try {
      // Phase 1: Keyword-Analyse
      const analysis = this.analyzePrompt(request.userPrompt, request.metadataContext, request.sage100DocumentationContext);

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
        requiresUserValidation: requiresValidation,
        metadataBasis: this.buildMetadataBasis(request)
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

  private buildMetadataBasis(request: AISchedulerRequest): AIGenerationResult["metadataBasis"] {
    const snapshot = request.metadataContext?.snapshot;
    const sageContext = request.sage100DocumentationContext;
    return {
      salesforce: snapshot
        ? {
            status: snapshot.status,
            refreshedAt: snapshot.refreshedAt,
            objectCount: snapshot.objectCount,
            fieldCount: snapshot.fieldCount,
            instanceId: snapshot.instanceId,
            projectId: snapshot.projectId
          }
        : { status: "missing" },
      sage100: sageContext
        ? {
            generatedAt: sageContext.generatedAt,
            sourceFile: sageContext.sourceFile,
            tableCount: sageContext.tableCount,
            matchedTables: sageContext.matchedTables.map((table) => table.name)
          }
        : undefined
    };
  }

  /**
   * Analysiere den Benutzer-Prompt und extrahiere Schlüsselinformationen
   */
  private analyzePrompt(
    prompt: string,
    metadataContext?: InstanceMetadataContext,
    sage100DocumentationContext?: Sage100DocumentationContext
  ): {
    sourceSystem?: string;
    targetSystem?: string;
    sourceObjectName?: string;
    targetObjectName?: string;
    objectName?: string;
    metadataContext?: InstanceMetadataContext;
    sage100DocumentationContext?: Sage100DocumentationContext;
    operation?: string;
    sqlQuery?: string;
    upsertTargetField?: string;
    upsertSourceField?: string;
    direction?: "Inbound" | "Outbound";
    timing?: AISchedulerTiming;
    confidence: number;
    rawKeywords: string[];
  } {
    const lower = prompt.toLowerCase();
    const keywords = this.extractKeywords(prompt);

    // Source-System erkennen
    const sourceSystem = this.detectSourceSystem(lower, keywords);

    // Target-System erkennen
    const targetSystem = this.detectTargetSystem(lower, keywords);

    // Object-Namen erkennen: Bei "Opportunity aus Salesforce als Order ins ERP"
    // sind Quelle und Ziel bewusst unterschiedliche Objekte.
    const sourceObjectName = this.detectSourceObjectName(lower) || this.detectMetadataSourceObjectName(lower, metadataContext);
    const targetObjectName = this.detectTargetObjectName(lower) || this.detectMetadataTargetObjectName(lower, metadataContext);
    const objectName = targetObjectName || sourceObjectName || this.detectObjectName(lower, keywords);

    // Operation erkennen
    const operation = this.detectOperation(lower, keywords);

    // SQL-Abfrage erkennen (z.B. SQL: SELECT ... oder ```sql ... ```)
    const sqlQuery = this.extractSqlQuery(prompt);

    // Upsert-Feld (Target/Source) erkennen, z.B. "Upsert Feld external_id zu ERP_adressnummer"
    const upsertKey = this.extractUpsertKey(prompt);

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
      sourceObjectName,
      targetObjectName,
      objectName,
      metadataContext,
      sage100DocumentationContext,
      operation,
      sqlQuery,
      upsertTargetField: upsertKey.targetField,
      upsertSourceField: upsertKey.sourceField,
      direction,
      timing,
      confidence,
      rawKeywords: keywords
    };
  }

  /**
   * Extrahiert Upsert-Schlüssel aus natürlicher Sprache
   */
  private extractUpsertKey(prompt: string): { targetField?: string; sourceField?: string } {
    const raw = String(prompt || "");

    const pairMatch = raw.match(/upsert\s*feld(?:\s+id)?\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:zu|=|auf|from)\s*([A-Za-z_][A-Za-z0-9_]*)/i);
    if (pairMatch) {
      return {
        targetField: pairMatch[1],
        sourceField: pairMatch[2]
      };
    }

    const explicitExternalId = raw.match(/(?:external\s*id|eindeutige\s*id|upsert\s*feld)\s*[:=]?\s*([A-Za-z_][A-Za-z0-9_]*)/i);
    if (explicitExternalId) {
      const field = explicitExternalId[1];
      return { targetField: field, sourceField: field };
    }

    return {};
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

    const sourceMatches = [...lower.matchAll(/\b(?:aus|von|from)\b\s+([^.;\n]+)/gi)];
    const afterSourceMarker = sourceMatches.length > 0
      ? String(sourceMatches[sourceMatches.length - 1][1] || "")
          .split(/\b(?:nach|to|in|ins|zu|into|→|->)\b/i)[0]
          .toLowerCase()
      : "";

    for (const [system, patterns] of Object.entries(sourceKeywords)) {
      if (afterSourceMarker && patterns.some((p) => afterSourceMarker.includes(p))) {
        return system;
      }
    }

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
    const targetKeywords: Record<string, string[]> = {
      Salesforce: ["salesforce", "sfdc", "sf", "crm"],
      MSSQL: ["mssql", "sql", "database", "oracle"],
      REST_API: ["rest", "api", "http"],
      FILE: ["file", "csv", "xlsx"],
      ERP: ["erp", "erp-system", "sage", "sage100", "sage50", "warenwirtschaft"],
      Brevo: ["brevo", "newsletter", "email"]
    };

    const directionalMatches = [...lower.matchAll(/\b(?:nach|to|in|ins|zu|into|→|->|\|)\b\s+([^.;\n]+)/gi)];
    const afterMarker = directionalMatches.length > 0
      ? String(directionalMatches[directionalMatches.length - 1][1] || "").toLowerCase()
      : "";

    for (const [system, patterns] of Object.entries(targetKeywords)) {
      if (afterMarker && patterns.some((p) => afterMarker.includes(p))) {
        return system;
      }
    }

    if (lower.includes("erp") && !lower.match(/\b(?:aus|von|from)\s+[^.;\n]*erp\b/i)) {
      return "ERP";
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
      Opportunity: ["opportunity", "opportunities", "deal", "deals", "chance", "chancen"],
      Order: ["order", "orders", "bestellung", "bestellungen", "auftrag", "aufträge", "auftraege"],
      "Custom Object": ["custom", "objekt"]
    };

    for (const [objectName, patterns] of Object.entries(commonObjects)) {
      if (keywords.some((k) => patterns.includes(k))) {
        return objectName;
      }
    }

    return undefined;
  }

  private detectSourceObjectName(lower: string): string | undefined {
    const beforeSourceMatch = lower.match(/\b(accounts?|kontakte?|contacts?|leads?|opportunit(?:y|ies)|orders?|bestellungen?|auftr[aä]ge|auftraege)\b[^.;\n]{0,80}\b(?:aus|von|from)\s+(?:salesforce|sfdc|sf|crm|erp|mssql|sql|datenbank)\b/i);
    if (beforeSourceMatch?.[1]) {
      return this.normalizeObjectToken(beforeSourceMatch[1]);
    }

    return undefined;
  }

  private detectTargetObjectName(lower: string): string | undefined {
    const asTargetMatch = lower.match(/\b(?:als|as)\s+(accounts?|kontakte?|contacts?|leads?|opportunit(?:y|ies)|orders?|bestellungen?|auftr[aä]ge|auftraege)\b/i);
    if (asTargetMatch?.[1]) {
      return this.normalizeObjectToken(asTargetMatch[1]);
    }

    return undefined;
  }

  private detectMetadataSourceObjectName(lower: string, metadataContext?: InstanceMetadataContext): string | undefined {
    const sourcePhrase = lower.match(/\b([a-z0-9_äöüß\s-]{2,80})\s+(?:aus|von|from)\s+(?:salesforce|sfdc|sf|crm)\b/i)?.[1];
    return this.matchMetadataObjectName(sourcePhrase || lower, metadataContext);
  }

  private detectMetadataTargetObjectName(lower: string, metadataContext?: InstanceMetadataContext): string | undefined {
    const targetPhrase = lower.match(/\b(?:als|as|nach|in|ins|zu|to|into)\s+([a-z0-9_äöüß\s-]{2,80})/i)?.[1];
    return this.matchMetadataObjectName(targetPhrase || "", metadataContext);
  }

  private matchMetadataObjectName(text: string, metadataContext?: InstanceMetadataContext): string | undefined {
    const normalizedText = this.normalizeMatchText(text);
    if (!normalizedText || !metadataContext?.objects?.length) {
      return undefined;
    }

    const scored = metadataContext.objects
      .map((object) => {
        const objectName = String(object.objectName || "").trim();
        const label = String(object.label || "").trim();
        const candidates = [
          objectName,
          label,
          objectName.replace(/__c$/i, ""),
          label.replace(/\s+/g, "")
        ]
          .map((candidate) => this.normalizeMatchText(candidate))
          .filter(Boolean);

        let score = 0;
        for (const candidate of candidates) {
          if (normalizedText === candidate || normalizedText === this.toSingularToken(candidate)) {
            score = Math.max(score, 1);
          } else if (normalizedText.includes(candidate) || normalizedText.includes(this.toPluralToken(candidate))) {
            score = Math.max(score, 0.8);
          }
        }

        return { objectName, score };
      })
      .filter((entry) => entry.objectName && entry.score > 0)
      .sort((a, b) => b.score - a.score);

    return scored[0]?.objectName;
  }

  private normalizeMatchText(value: string): string {
    return String(value || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9_]+/g, " ")
      .trim();
  }

  private toSingularToken(value: string): string {
    return value.replace(/ies\b/, "y").replace(/s\b/, "");
  }

  private toPluralToken(value: string): string {
    if (value.endsWith("y")) {
      return `${value.slice(0, -1)}ies`;
    }
    return value.endsWith("s") ? value : `${value}s`;
  }

  private normalizeObjectToken(token: string): string | undefined {
    const normalized = String(token || "").trim().toLowerCase();
    if (!normalized) {
      return undefined;
    }

    if (/^accounts?$|^konto$/.test(normalized)) return "Account";
    if (/^contacts?$|^kontakte?$|^person$/.test(normalized)) return "Contact";
    if (/^leads?$|^interessent$/.test(normalized)) return "Lead";
    if (/^opportunit(y|ies)$|^deals?$|^chancen?$/.test(normalized)) return "Opportunity";
    if (/^orders?$|^bestellungen?$|^auftr[aä]ge$|^auftraege$/.test(normalized)) return "Order";
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
   * Extrahiert eine explizite SQL-Abfrage aus dem Prompt.
   * Unterstuetzt SQL-Codeblocks, "SQL:"-Praefix und freie SELECT/WITH-Abfragen.
   */
  private extractSqlQuery(prompt: string): string | undefined {
    const raw = String(prompt || "").trim();
    if (!raw) {
      return undefined;
    }

    const sqlBlockMatch = raw.match(/```sql\s*([\s\S]*?)```/i);
    if (sqlBlockMatch?.[1]) {
      const fromBlock = sqlBlockMatch[1].trim();
      if (/^(select|with)\b/i.test(fromBlock)) {
        return fromBlock;
      }
    }

    const sqlPrefixMatch = raw.match(/(?:^|\n)\s*sql\s*:\s*([\s\S]+)/i);
    if (sqlPrefixMatch?.[1]) {
      const candidate = sqlPrefixMatch[1].trim();
      const selectMatch = candidate.match(/\b(select|with)\b[\s\S]*/i);
      if (selectMatch?.[0]) {
        return this.trimSqlCandidate(selectMatch[0]);
      }
    }

    const inlineSelectMatch = raw.match(/\b(select|with)\b[\s\S]*?(?:;|$)/i);
    if (inlineSelectMatch?.[0]) {
      const candidate = this.trimSqlCandidate(inlineSelectMatch[0]);
      if (/^(select|with)\b/i.test(candidate)) {
        return candidate;
      }
    }

    return undefined;
  }

  private trimSqlCandidate(candidateRaw: string): string {
    const candidate = String(candidateRaw || "").trim();
    if (!candidate) {
      return "";
    }

    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inBracketIdentifier = false;
    let inLineComment = false;
    let inBlockComment = false;

    for (let i = 0; i < candidate.length; i += 1) {
      const ch = candidate[i];
      const next = candidate[i + 1] || "";
      const remaining = candidate.slice(i + 1).trimStart();

      if (inLineComment) {
        if (ch === "\n") {
          inLineComment = false;
        }
        continue;
      }

      if (inBlockComment) {
        if (ch === "*" && next === "/") {
          inBlockComment = false;
          i += 1;
        }
        continue;
      }

      if (!inSingleQuote && !inDoubleQuote && !inBracketIdentifier) {
        if (ch === "-" && next === "-") {
          inLineComment = true;
          i += 1;
          continue;
        }
        if (ch === "/" && next === "*") {
          inBlockComment = true;
          i += 1;
          continue;
        }
      }

      if (!inDoubleQuote && !inBracketIdentifier && ch === "'") {
        inSingleQuote = !inSingleQuote;
        continue;
      }

      if (!inSingleQuote && !inBracketIdentifier && ch === '"') {
        inDoubleQuote = !inDoubleQuote;
        continue;
      }

      if (!inSingleQuote && !inDoubleQuote && ch === "[") {
        inBracketIdentifier = true;
        continue;
      }

      if (inBracketIdentifier && ch === "]") {
        inBracketIdentifier = false;
        continue;
      }

      if (!inSingleQuote && !inDoubleQuote && !inBracketIdentifier) {
        if (ch === ";") {
          return candidate.slice(0, i + 1).trim();
        }

        if (ch === "\n" && this.isNaturalLanguageTail(remaining)) {
          return candidate.slice(0, i).trim();
        }

        if (/\s/.test(ch)) {
          const tailMarkerMatch = remaining.match(/^(und|bitte|danach|anschließend|then|please)\b/i);
          if (tailMarkerMatch) {
            const afterMarker = remaining.slice(tailMarkerMatch[0].length).trimStart();
            if (this.isNaturalLanguageTail(afterMarker)) {
              return candidate.slice(0, i).trim();
            }
          }
        }
      }
    }

    return candidate;
  }

  private isNaturalLanguageTail(text: string): boolean {
    const normalized = String(text || "").trim();
    if (!normalized) {
      return false;
    }

    if (/^(select|with|from|where|group\s+by|order\s+by|having|limit|offset|join|left|right|inner|outer|union|and|or)\b/i.test(normalized)) {
      return false;
    }

    return /^(bitte|und\b|dann\b|danach\b|anschließend\b|then\b|please\b|nutze\b|verwende\b|mappe\b|schreibe\b|erstelle\b|setze\b|für\b|nach\b|to\b|in\b)/i.test(normalized);
  }

  /**
   * Bestimme die Richtung (Inbound/Outbound)
   */
  private detectDirection(
    lower: string,
    sourceSystem?: string,
    targetSystem?: string
  ): "Inbound" | "Outbound" {
    // Wenn von Salesforce weg -> Outbound
    if (sourceSystem?.toLowerCase().includes("salesforce")) return "Outbound";
    // Wenn zu Salesforce hin -> Inbound
    if (targetSystem?.toLowerCase().includes("salesforce")) return "Inbound";
    // Default
    return "Inbound";
  }

  /**
   * Extrahiere Timing-Information aus dem Prompt
   */
  private extractTiming(prompt: string): AISchedulerTiming | undefined {
    const normalized = String(prompt || "").trim();
    if (!normalized) {
      return undefined;
    }

    const days = this.extractTimingDays(normalized);
    const startTime = this.extractStartTime(normalized);
    const intervalMinutes = this.extractIntervalMinutes(normalized);
    if (intervalMinutes) {
      return {
        type: "interval",
        value: String(intervalMinutes),
        intervalMinutes,
        days: days || [1, 2, 3, 4, 5, 6, 0],
        startTime: startTime || "00:00"
      };
    }

    if (/\b(täglich|taeglich|jeden\s+tag|jeden\s+werktag|daily|every\s+day)\b/i.test(normalized)) {
      return {
        type: "daily",
        value: startTime || "09:00",
        days: days || [1, 2, 3, 4, 5, 6, 0],
        startTime: startTime || "09:00"
      };
    }

    // Stündlich
    if (prompt.match(/stündlich|hourly|jede stunde/i)) {
      return { type: "interval", value: "60", intervalMinutes: 60, days: days || [1, 2, 3, 4, 5, 6, 0], startTime: startTime || "00:00" };
    }

    // Wöchentlich
    if (prompt.match(/wöchentlich|weekly|jede woche/i)) {
      return { type: "weekly", value: "7days", days: days || [1], startTime: startTime || "09:00" };
    }

    // Monatlich
    if (prompt.match(/monatlich|monthly|jeden monat/i)) {
      return { type: "monthly", value: "30days", days: days || [1], startTime: startTime || "09:00" };
    }

    return undefined;
  }

  private extractIntervalMinutes(prompt: string): number | undefined {
    const intervalMatch = prompt.match(/\b(?:alle|jede[nrs]?|every)\s+(\d{1,4})\s*(?:min(?:ute)?n?|minutes?|m)\b/i);
    if (intervalMatch?.[1]) {
      const minutes = Number(intervalMatch[1]);
      if (Number.isInteger(minutes) && minutes > 0 && minutes <= 1440) {
        return minutes;
      }
    }

    const hourlyIntervalMatch = prompt.match(/\b(?:alle|jede[nrs]?|every)\s+(\d{1,2})\s*(?:h|std\.?|stunden?|hours?)\b/i);
    if (hourlyIntervalMatch?.[1]) {
      const hours = Number(hourlyIntervalMatch[1]);
      if (Number.isInteger(hours) && hours > 0 && hours <= 24) {
        return hours * 60;
      }
    }

    return undefined;
  }

  private extractStartTime(prompt: string): string | undefined {
    const colonMatch = prompt.match(/\b(?:um|ab|start(?:et)?\s+um|from)?\s*(\d{1,2}):(\d{2})\s*(?:uhr)?\b/i);
    if (colonMatch?.[1] && colonMatch?.[2]) {
      return this.normalizeStartTime(colonMatch[1], colonMatch[2]);
    }

    const hourMatch = prompt.match(/\b(?:um|ab|start(?:et)?\s+um)\s*(\d{1,2})\s*uhr\b/i)
      || prompt.match(/\b(\d{1,2})\s*uhr\b/i);
    if (hourMatch?.[1]) {
      return this.normalizeStartTime(hourMatch[1], "00");
    }

    return undefined;
  }

  private normalizeStartTime(hourRaw: string, minuteRaw: string): string | undefined {
    const hour = Number(hourRaw);
    const minute = Number(minuteRaw || "0");
    if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
      return undefined;
    }
    return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
  }

  private extractTimingDays(prompt: string): number[] | undefined {
    if (/\b(jeden\s+tag|täglich|taeglich|daily|every\s+day)\b/i.test(prompt)) {
      return [1, 2, 3, 4, 5, 6, 0];
    }

    if (/\b(werktags|arbeitstag|arbeitstage|mo\s*-\s*fr|montag\s*-\s*freitag|business\s+days|weekdays)\b/i.test(prompt)) {
      return [1, 2, 3, 4, 5];
    }

    const dayMap: Array<[RegExp, number]> = [
      [/\b(montag|mondays?)\b/i, 1],
      [/\b(dienstag|dienstags|tuesdays?)\b/i, 2],
      [/\b(mittwoch|mittwochs|wednesdays?)\b/i, 3],
      [/\b(donnerstag|donnerstags|thursdays?)\b/i, 4],
      [/\b(freitag|freitags|fridays?)\b/i, 5],
      [/\b(samstag|samstags|sonnabend|saturdays?)\b/i, 6],
      [/\b(sonntag|sonntags|sundays?)\b/i, 0]
    ];
    const matchedDays = dayMap
      .filter(([pattern]) => pattern.test(prompt))
      .map(([, day]) => day);

    return matchedDays.length ? [...new Set(matchedDays)] : undefined;
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

    const connectorType = String(connector.connectorType || "").toLowerCase();
    const targetSystem = String(connector.targetSystem || "").toLowerCase();
    const wantsSalesforceSource = String(analysis.sourceSystem || "").toLowerCase().includes("salesforce");
    const wantsErpTarget = this.isErpLikeSystem(analysis.targetSystem);

    if (wantsSalesforceSource && wantsErpTarget && (connectorType.includes("mssql") || targetSystem.includes("erp") || targetSystem.includes("sage"))) {
      score += 0.6;
    } else if (
      analysis.sourceSystem &&
      connectorType.includes(analysis.sourceSystem.toLowerCase().split("/")[0])
    ) {
      score += 0.4;
    }

    // Target-System Match
    if (analysis.targetSystem && targetSystem.includes(analysis.targetSystem.toLowerCase())) {
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
      sourceType: this.resolveSourceType(analysis, connector),
      targetType: this.resolveTargetType(analysis),
      batchSize: this.inferBatchSize(connector),
      connectorId: connector.id,
      timingDefinition: timing,
      sourceDefinition: this.generateSourceDefinition(analysis, connector),
      targetDefinition: this.generateTargetDefinition(analysis),
      mappingDefinition: this.generateMappingDefinition(analysis)
    };
  }

  private resolveSourceTypeFromConnector(connectorType?: string): string {
    const normalized = String(connectorType || "").trim().toUpperCase();
    if (!normalized) {
      return "REST_API";
    }
    if (normalized.includes("MSSQL")) {
      return "MSSQL_SQL";
    }
    return normalized;
  }

  private resolveSourceType(
    analysis: ReturnType<typeof this.analyzePrompt>,
    connector: ConnectorListItem
  ): string {
    if (String(analysis.sourceSystem || "").toLowerCase().includes("salesforce")) {
      return "SALESFORCE_SOQL";
    }

    return this.resolveSourceTypeFromConnector(connector.connectorType);
  }

  private resolveTargetType(analysis: ReturnType<typeof this.analyzePrompt>): string {
    if (String(analysis.targetSystem || "").toLowerCase().includes("salesforce")) {
      return "SALESFORCE";
    }

    if (this.isErpLikeSystem(analysis.targetSystem)) {
      return "MSSQL";
    }

    return "REST_API";
  }

  private isErpLikeSystem(system?: string): boolean {
    const normalized = String(system || "").trim().toLowerCase();
    return normalized === "erp" || normalized.includes("sage") || normalized.includes("mssql");
  }

  /**
   * Generiere aussagekräftigen Namen
   */
  private generateSchedulerName(analysis: ReturnType<typeof this.analyzePrompt>, connector: ConnectorListItem): string {
    const objectPart = analysis.sourceObjectName && analysis.targetObjectName && analysis.sourceObjectName !== analysis.targetObjectName
      ? `(${analysis.sourceObjectName} → ${analysis.targetObjectName})`
      : analysis.objectName
        ? `(${analysis.objectName})`
        : "";
    const parts = [
      analysis.sourceSystem || connector.name,
      "→",
      analysis.targetSystem || "Salesforce",
      objectPart
    ];
    return parts.filter(Boolean).join(" ").slice(0, 80);
  }

  /**
   * Generiere Timing-Definition
   */
  private generateTimingDefinition(timing?: AISchedulerTiming): string {
    if (!timing) {
      // Default: täglich 09:00
      return JSON.stringify({ days: [1, 2, 3, 4, 5], intervalMinutes: 1440, startTime: "09:00" });
    }

    const baseConfig = { days: [1, 2, 3, 4, 5], intervalMinutes: 1440, startTime: "09:00" };

    switch (timing.type) {
      case "interval":
        return JSON.stringify({
          days: timing.days || [1, 2, 3, 4, 5, 6, 0],
          intervalMinutes: timing.intervalMinutes || Number(timing.value) || 60,
          startTime: timing.startTime || "00:00"
        });
      case "daily":
        return JSON.stringify({
          ...baseConfig,
          days: timing.days || [1, 2, 3, 4, 5, 6, 0],
          startTime: timing.startTime || timing.value
        });
      case "hourly":
        return JSON.stringify({ days: [1, 2, 3, 4, 5, 6, 0], intervalMinutes: 60 });
      case "weekly":
        return JSON.stringify({ days: timing.days || [1], intervalMinutes: 1440, startTime: timing.startTime || "09:00" });
      case "monthly":
        return JSON.stringify({ days: timing.days || [1], intervalMinutes: 1440, startTime: timing.startTime || "09:00" });
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

    if (String(analysis.sourceSystem || "").toLowerCase().includes("salesforce")) {
      const sourceObjectName = analysis.sourceObjectName || (analysis.targetObjectName ? undefined : analysis.objectName) || "Opportunity";
      Object.assign(baseDefinition, {
        queryText: this.generateSalesforceSoql(analysis, sourceObjectName)
      });
    } else if (connector.connectorType === "REST_API") {
      Object.assign(baseDefinition, {
        endpoint: "/v1/data",
        method: "GET",
        responseType: "json",
        pagination: { type: "offset", pageSize: 100 }
      });
    } else if (connector.connectorType?.toUpperCase().includes("MSSQL")) {
      if (analysis.sqlQuery) {
        Object.assign(baseDefinition, {
          queryText: analysis.sqlQuery
        });
      } else if (this.isErpLikeSystem(analysis.sourceSystem) || String(analysis.sourceSystem || "").toLowerCase().includes("sage")) {
        Object.assign(baseDefinition, {
          queryText: this.generateSage100Sql(analysis)
        });
      } else {
        const tableName = this.resolveDefaultMssqlTableName(analysis.objectName);
        Object.assign(baseDefinition, {
          queryText: `SELECT * FROM ${tableName}`
        });
      }
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
    const defaultUpsertField = this.inferDefaultUpsertField(analysis.objectName || "Contact");
    const upsertField = analysis.upsertTargetField || defaultUpsertField;

    if (analysis.targetSystem?.toLowerCase().includes("salesforce")) {
      return JSON.stringify(
        {
          objectApiName: analysis.objectName || "Contact",
          externalIdField: upsertField,
          deployOptions: {
            purgeOnDelete: false,
            rollbackOnError: true
          }
        },
        null,
        2
      );
    }

    if (this.isErpLikeSystem(analysis.targetSystem)) {
      const sageTable = this.findBestSage100Table(analysis);
      return JSON.stringify(
        {
          tableName: sageTable?.name,
          upsertKey: upsertField,
          documentation: sageTable
            ? {
                source: "SAGE100 Datenbankdokumentation",
                pages: sageTable.pages,
                primaryKey: sageTable.primaryKey,
                fields: sageTable.fields.slice(0, 20).map((field) => ({
                  name: field.name,
                  type: field.type,
                  required: field.required === true
                }))
              }
            : undefined
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
        upsertKey: upsertField
      },
      null,
      2
    );
  }

  /**
   * Liefert sinnvollen Default für Upsert-Key je Object
   */
  private inferDefaultUpsertField(objectName: string): string {
    const normalized = String(objectName || "").toLowerCase();
    if (normalized === "contact" || normalized === "lead") {
      return "Email";
    }
    if (normalized === "account") {
      return "Name";
    }
    if (normalized === "order") {
      return "OrderNumber";
    }
    return "Name";
  }

  private findBestSage100Table(analysis: ReturnType<typeof this.analyzePrompt>) {
    return analysis.sage100DocumentationContext?.matchedTables?.[0];
  }

  private generateSage100Sql(analysis: ReturnType<typeof this.analyzePrompt>): string {
    const table = this.findBestSage100Table(analysis);
    if (!table?.name) {
      const tableName = this.resolveDefaultSage100TableName(analysis.objectName);
      return this.buildSage100Select(tableName, this.getDefaultSage100Columns(analysis.objectName));
    }

    const tableName = String(table.name || "").trim();
    const columns = tableName.toLowerCase() === "khkadressen"
      ? this.getDefaultSage100Columns("Account")
      : this.getPreferredSage100Columns(analysis, table.fields.map((field) => field.name));
    const safeColumns = columns.length ? columns : table.fields.slice(0, 12).map((field) => field.name).filter(Boolean);
    return this.buildSage100Select(tableName, safeColumns);
  }

  private buildSage100Select(tableName: string, columns: string[]): string {
    const safeTableName = String(tableName || "").trim() || "KHKAdressen";
    const safeColumns = columns.map((column) => String(column || "").trim()).filter(Boolean);
    const base = `SELECT ${safeColumns.length ? safeColumns.join(", ") : "*"} FROM dbo.${safeTableName}`;
    if (safeTableName.toLowerCase() === "khkadressen") {
      return `${base} WHERE Aktiv = -1`;
    }
    return base;
  }

  private resolveDefaultSage100TableName(objectName?: string): string {
    const normalized = String(objectName || "").trim().toLowerCase();
    if (normalized === "account") return "KHKAdressen";
    if (normalized === "contact") return "KHKAnsprechpartner";
    if (normalized === "product2" || normalized === "product") return "KHKArtikel";
    if (normalized === "order") return "KHKVKBelege";
    if (normalized === "orderitem") return "KHKVKBelegePositionen";
    return "KHKAdressen";
  }

  private getDefaultSage100Columns(objectName?: string): string[] {
    const normalized = String(objectName || "").trim().toLowerCase();
    if (normalized === "account") {
      return ["Adresse", "Matchcode", "Name1", "Name2", "LieferStrasse", "LieferPLZ", "LieferOrt", "LieferLand", "Telefon", "EMail"];
    }
    if (normalized === "contact") {
      return ["Nummer", "Adresse", "Vorname", "Nachname", "Ansprechpartner", "Position"];
    }
    if (normalized === "product2" || normalized === "product") {
      return ["Artikelnummer", "Bezeichnung1", "Bezeichnung2", "Aktiv"];
    }
    if (normalized === "order") {
      return ["BelID", "Belegnummer", "Adresse", "Belegdatum", "Nettobetrag", "Status"];
    }
    return ["Adresse", "Matchcode", "Name1"];
  }

  private getPreferredSage100Columns(
    analysis: ReturnType<typeof this.analyzePrompt>,
    availableColumnNames: string[]
  ): string[] {
    const available = new Map(availableColumnNames.map((name) => [name.toLowerCase(), name]));
    return this.getDefaultSage100Columns(analysis.objectName)
      .map((name) => available.get(name.toLowerCase()))
      .filter((name): name is string => Boolean(name));
  }

  private generateSalesforceSoql(
    analysis: ReturnType<typeof this.analyzePrompt>,
    sourceObjectName: string
  ): string {
    if (sourceObjectName === "Opportunity") {
      const closedFilter = /\babgeschlossen|closed|gewonnen|won\b/i.test(analysis.rawKeywords.join(" "));
      const selectedFields = this.selectAvailableSalesforceFields(analysis, "Opportunity", [
        "Id",
        "Name",
        "Amount",
        "CloseDate",
        "StageName",
        "AccountId",
        "LastModifiedDate"
      ]);
      const hasIsClosed = this.hasPersistedField(analysis, "Opportunity", "IsClosed");
      return closedFilter
        ? `SELECT ${selectedFields.join(", ")} FROM Opportunity${hasIsClosed ? " WHERE IsClosed = true" : " WHERE StageName = 'Closed Won'"}`
        : `SELECT ${selectedFields.join(", ")} FROM Opportunity`;
    }

    if (sourceObjectName === "Order") {
      const selectedFields = this.selectAvailableSalesforceFields(analysis, "Order", [
        "Id",
        "OrderNumber",
        "Status",
        "TotalAmount",
        "EffectiveDate",
        "AccountId",
        "LastModifiedDate"
      ]);
      return `SELECT ${selectedFields.join(", ")} FROM Order`;
    }

    const selectedFields = this.selectAvailableSalesforceFields(analysis, sourceObjectName, ["Id", "Name", "LastModifiedDate"]);
    return `SELECT ${selectedFields.join(", ")} FROM ${sourceObjectName}`;
  }

  private getPersistedFields(
    analysis: ReturnType<typeof this.analyzePrompt>,
    objectName: string
  ): PersistedMetadataField[] {
    const fieldsByObject = analysis.metadataContext?.fieldsByObject || {};
    const direct = fieldsByObject[objectName];
    if (Array.isArray(direct)) {
      return direct;
    }

    const normalizedObjectName = objectName.toLowerCase();
    const matchingKey = Object.keys(fieldsByObject).find((key) => key.toLowerCase() === normalizedObjectName);
    return matchingKey ? fieldsByObject[matchingKey] || [] : [];
  }

  private hasPersistedField(
    analysis: ReturnType<typeof this.analyzePrompt>,
    objectName: string,
    fieldName: string
  ): boolean {
    const normalizedFieldName = fieldName.toLowerCase();
    return this.getPersistedFields(analysis, objectName)
      .some((field) => field.name.toLowerCase() === normalizedFieldName);
  }

  private selectAvailableSalesforceFields(
    analysis: ReturnType<typeof this.analyzePrompt>,
    objectName: string,
    preferredFields: string[]
  ): string[] {
    const persistedFields = this.getPersistedFields(analysis, objectName);
    if (!persistedFields.length) {
      return preferredFields;
    }

    const available = new Set(persistedFields.map((field) => field.name.toLowerCase()));
    const selected = preferredFields.filter((field) => available.has(field.toLowerCase()));
    if (selected.length > 0) {
      return selected;
    }

    return persistedFields.slice(0, 12).map((field) => field.name).filter(Boolean);
  }

  private resolveDefaultMssqlTableName(objectName?: string): string {
    const normalized = String(objectName || "").trim();
    if (!normalized) {
      return "[Contacts]";
    }

    // Nur sichere Zeichen zulassen und in [] quoten.
    const safe = normalized.replace(/[^a-zA-Z0-9_]/g, "");
    if (!safe) {
      return "[Contacts]";
    }
    return `[${safe}]`;
  }

  /**
   * Generiere Mapping-Definition
   * Format: targetField;dataType=sourceField;TRANSFORMS
   */
  private generateMappingDefinition(analysis: ReturnType<typeof this.analyzePrompt>): string {
    const objectName = analysis.objectName || "Contact";
    const metadataAwareMappings = this.generateMetadataAwareMappingLines(analysis);
    if (metadataAwareMappings.length > 0) {
      return metadataAwareMappings.join("\n");
    }

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

    const mappingLines = analysis.sourceObjectName === "Opportunity" && analysis.targetObjectName === "Order"
      ? [
          "OrderNumber;string=Id;NONE",
          "Name;string=Name;NONE",
          "TotalAmount;number=Amount;NONE",
          "Status;string=StageName;NONE",
          "EffectiveDate;datetime=CloseDate;NONE",
          "AccountId;string=AccountId;NONE"
        ]
      : [...(standardMappings[objectName] || standardMappings["Contact"])];

    const upsertTargetField = analysis.upsertTargetField || this.inferDefaultUpsertField(objectName);
    const upsertSourceField = analysis.upsertSourceField || upsertTargetField;

    const hasTargetField = mappingLines.some((line) => line.split(";")[0] === upsertTargetField);
    if (!hasTargetField) {
      mappingLines.unshift(`${upsertTargetField};string=${upsertSourceField};NONE`);
    }

    return mappingLines.join("\n");
  }

  private generateMetadataAwareMappingLines(analysis: ReturnType<typeof this.analyzePrompt>): string[] {
    const curatedSageMappings = this.generateCuratedSage100MappingLines(analysis);
    if (curatedSageMappings.length > 0) {
      return curatedSageMappings;
    }

    const targetFields = this.resolveMappingTargetFields(analysis);
    const sourceFields = this.resolveMappingSourceFields(analysis);
    if (!targetFields.length || !sourceFields.length) {
      return [];
    }

    const sourceByName = new Map(sourceFields.map((field) => [field.name.toLowerCase(), field]));
    const selectedMappings: Array<{ target: MappingFieldCandidate; source: MappingFieldCandidate; score: number }> = [];
    const usedSourceNames = new Set<string>();

    for (const target of this.prioritizeTargetFields(targetFields)) {
      const match = this.findBestFieldMatch(target, sourceFields, usedSourceNames);
      if (!match || match.score < 28) {
        continue;
      }

      selectedMappings.push({ target, source: match.field, score: match.score });
      usedSourceNames.add(match.field.name.toLowerCase());
    }

    const upsertTargetField = analysis.upsertTargetField || this.inferDefaultUpsertField(this.resolveMappingTargetObjectName(analysis) || analysis.objectName || "Contact");
    const upsertSourceField = analysis.upsertSourceField || upsertTargetField;
    const explicitUpsertSource = sourceByName.get(upsertSourceField.toLowerCase());
    const explicitUpsertTarget = targetFields.find((field) => field.name.toLowerCase() === upsertTargetField.toLowerCase());
    if (
      explicitUpsertSource &&
      explicitUpsertTarget &&
      !selectedMappings.some((entry) => entry.target.name.toLowerCase() === explicitUpsertTarget.name.toLowerCase())
    ) {
      selectedMappings.unshift({ target: explicitUpsertTarget, source: explicitUpsertSource, score: 120 });
    }

    return selectedMappings
      .sort((a, b) => {
        const requiredDiff = Number(Boolean(b.target.required || b.target.externalId)) - Number(Boolean(a.target.required || a.target.externalId));
        if (requiredDiff !== 0) return requiredDiff;
        return b.score - a.score;
      })
      .slice(0, 30)
      .map((entry) => `${entry.target.name};${this.mapMappingDataType(entry.target.type)}=${entry.source.name};NONE`);
  }

  private generateCuratedSage100MappingLines(analysis: ReturnType<typeof this.analyzePrompt>): string[] {
    if (!(this.isErpLikeSystem(analysis.sourceSystem) || String(analysis.sourceSystem || "").toLowerCase().includes("sage"))) {
      return [];
    }

    const targetObject = String(this.resolveMappingTargetObjectName(analysis) || analysis.objectName || "").trim().toLowerCase();
    const tableName = String(this.findBestSage100Table(analysis)?.name || "").trim().toLowerCase();
    if (targetObject !== "account" || tableName !== "khkadressen") {
      return [];
    }

    const targetFields = new Map(this.resolveMappingTargetFields(analysis).map((field) => [field.name.toLowerCase(), field]));
    const sourceFields = new Set(this.resolveMappingSourceFields(analysis).map((field) => field.name.toLowerCase()));
    for (const knownField of this.getDefaultSage100Columns("Account")) {
      sourceFields.add(knownField.toLowerCase());
    }
    const candidates = [
      ["Name", "string", "Name1"],
      ["AccountNumber", "string", "Adresse"],
      ["ERP_Account_Number__c", "string", "Adresse"],
      ["Phone", "string", "Telefon"],
      ["General_Email__c", "string", "EMail"],
      ["ShippingStreet", "string", "LieferStrasse"],
      ["ShippingPostalCode", "string", "LieferPLZ"],
      ["ShippingCity", "string", "LieferOrt"],
      ["ShippingCountry", "string", "LieferLand"]
    ];

    return candidates
      .filter(([target, , source]) => targetFields.has(target.toLowerCase()) && sourceFields.has(source.toLowerCase()))
      .map(([target, type, source]) => `${target};${type}=${source};TRIM`);
  }

  private resolveMappingSourceFields(analysis: ReturnType<typeof this.analyzePrompt>): MappingFieldCandidate[] {
    if (String(analysis.sourceSystem || "").toLowerCase().includes("salesforce")) {
      const objectName = analysis.sourceObjectName || analysis.objectName || "Contact";
      return this.toSalesforceMappingFields(this.getPersistedFields(analysis, objectName));
    }

    if (this.isErpLikeSystem(analysis.sourceSystem) || String(analysis.sourceSystem || "").toLowerCase().includes("sage")) {
      return this.toSage100MappingFields(this.findBestSage100Table(analysis)?.fields || []);
    }

    return [];
  }

  private resolveMappingTargetFields(analysis: ReturnType<typeof this.analyzePrompt>): MappingFieldCandidate[] {
    if (String(analysis.targetSystem || "").toLowerCase().includes("salesforce")) {
      const objectName = this.resolveMappingTargetObjectName(analysis) || "Contact";
      return this.toSalesforceMappingFields(this.getPersistedFields(analysis, objectName), true);
    }

    if (this.isErpLikeSystem(analysis.targetSystem) || String(analysis.targetSystem || "").toLowerCase().includes("sage")) {
      return this.toSage100MappingFields(this.findBestSage100Table(analysis)?.fields || []);
    }

    return [];
  }

  private resolveMappingTargetObjectName(analysis: ReturnType<typeof this.analyzePrompt>): string | undefined {
    return analysis.targetObjectName || analysis.objectName;
  }

  private toSalesforceMappingFields(fields: PersistedMetadataField[], target = false): MappingFieldCandidate[] {
    const blockedTargetFields = new Set([
      "id",
      "isdeleted",
      "createdbyid",
      "createddate",
      "lastmodifiedbyid",
      "lastmodifieddate",
      "systemmodstamp",
      "lastvieweddate",
      "lastreferenceddate"
    ]);

    return fields
      .filter((field) => {
        const name = String(field.name || "").trim();
        if (!name) return false;
        if (target && blockedTargetFields.has(name.toLowerCase())) return false;
        if (target && field.createable === false && field.updateable === false && field.externalId !== true) return false;
        return true;
      })
      .map((field) => ({
        name: field.name,
        label: field.label,
        type: field.type,
        required: field.required,
        externalId: field.externalId,
        source: "salesforce" as const
      }));
  }

  private toSage100MappingFields(fields: Array<{ name: string; type?: string; required?: boolean; description?: string }>): MappingFieldCandidate[] {
    return fields
      .map((field) => ({
        name: String(field.name || "").trim(),
        label: field.description,
        type: field.type,
        required: field.required,
        description: field.description,
        source: "sage100" as const
      }))
      .filter((field) => field.name);
  }

  private prioritizeTargetFields(fields: MappingFieldCandidate[]): MappingFieldCandidate[] {
    const commonPriority = new Map(
      [
        "email",
        "name",
        "firstname",
        "lastname",
        "company",
        "phone",
        "mobilephone",
        "website",
        "ordernumber",
        "totalamount",
        "amount",
        "status",
        "effectivedate",
        "closedate",
        "stagename",
        "accountid",
        "billingstreet",
        "billingcity",
        "billingpostalcode",
        "billingcountry"
      ].map((name, index) => [name, index])
    );

    return [...fields].sort((a, b) => {
      const requiredDiff = Number(Boolean(b.required || b.externalId)) - Number(Boolean(a.required || a.externalId));
      if (requiredDiff !== 0) return requiredDiff;
      const priorityA = commonPriority.get(a.name.toLowerCase()) ?? 999;
      const priorityB = commonPriority.get(b.name.toLowerCase()) ?? 999;
      if (priorityA !== priorityB) return priorityA - priorityB;
      return a.name.localeCompare(b.name);
    });
  }

  private findBestFieldMatch(
    target: MappingFieldCandidate,
    sourceFields: MappingFieldCandidate[],
    usedSourceNames: Set<string>
  ): { field: MappingFieldCandidate; score: number } | undefined {
    return sourceFields
      .filter((field) => !usedSourceNames.has(field.name.toLowerCase()))
      .map((field) => ({
        field,
        score: this.scoreFieldMatch(target, field)
      }))
      .sort((a, b) => b.score - a.score)[0];
  }

  private scoreFieldMatch(target: MappingFieldCandidate, source: MappingFieldCandidate): number {
    const targetTokens = this.getFieldMatchTokens(target);
    const sourceTokens = this.getFieldMatchTokens(source);
    const targetName = this.normalizeFieldName(target.name);
    const sourceName = this.normalizeFieldName(source.name);
    const targetLabel = this.normalizeFieldName(target.label || "");
    const sourceLabel = this.normalizeFieldName(source.label || source.description || "");

    let score = 0;
    if (targetName && sourceName && targetName === sourceName) score += 100;
    if (targetLabel && sourceLabel && targetLabel === sourceLabel) score += 80;
    if (targetName && sourceLabel && sourceLabel.includes(targetName)) score += 45;
    if (sourceName && targetLabel && targetLabel.includes(sourceName)) score += 45;
    if (targetName && sourceName && (targetName.includes(sourceName) || sourceName.includes(targetName))) score += 35;

    for (const token of targetTokens) {
      if (sourceTokens.has(token)) {
        score += 18;
      }
    }

    const typeScore = this.scoreTypeCompatibility(target.type, source.type);
    score += typeScore;

    return score;
  }

  private getFieldMatchTokens(field: MappingFieldCandidate): Set<string> {
    const raw = [field.name, field.label || "", field.description || ""].join(" ");
    const tokens = this.normalizeFieldName(raw)
      .split(" ")
      .flatMap((token) => this.expandFieldToken(token))
      .filter((token) => token.length >= 2);
    return new Set(tokens);
  }

  private normalizeFieldName(value: string): string {
    return String(value || "")
      .replace(/__c$/i, "")
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\b(khk|mandant|mandanten|msd|erp|sf|salesforce)\b/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  private expandFieldToken(token: string): string[] {
    const aliases: Record<string, string[]> = {
      adresse: ["adresse", "address", "account", "kunde", "customer"],
      adressnummer: ["adressnummer", "addressnumber", "kundennummer", "customerid", "accountnumber", "number"],
      auftrag: ["auftrag", "order", "bestellung", "beleg"],
      auftragsnummer: ["auftragsnummer", "ordernumber", "belegnummer", "nummer"],
      beleg: ["beleg", "order", "auftrag", "document"],
      belegdatum: ["belegdatum", "date", "datum", "effectivedate"],
      belegnummer: ["belegnummer", "ordernumber", "auftragsnummer", "number"],
      betrag: ["betrag", "amount", "total", "summe"],
      bruttobetrag: ["bruttobetrag", "grossamount", "totalamount", "amount"],
      close: ["close", "date", "datum", "belegdatum"],
      closedate: ["closedate", "date", "datum", "belegdatum"],
      nettobetrag: ["nettobetrag", "netamount", "amount", "totalamount"],
      datum: ["datum", "date"],
      effective: ["effective", "date", "datum", "belegdatum"],
      effectivedate: ["effectivedate", "date", "datum", "belegdatum"],
      email: ["email", "mail", "e-mail"],
      firma: ["firma", "company", "account", "name"],
      gesamtbetrag: ["gesamtbetrag", "totalamount", "amount", "total"],
      id: ["id", "identifier", "nummer", "number"],
      kunde: ["kunde", "customer", "account", "adresse"],
      kundennummer: ["kundennummer", "customerid", "accountnumber", "adressnummer", "number"],
      land: ["land", "country"],
      mail: ["mail", "email", "e-mail"],
      name: ["name", "bezeichnung", "firma"],
      nummer: ["nummer", "number", "id"],
      ort: ["ort", "city"],
      plz: ["plz", "postalcode", "zip"],
      preis: ["preis", "price", "amount"],
      status: ["status", "stage", "zustand"],
      strasse: ["strasse", "street"],
      telefon: ["telefon", "phone", "tel"],
      vorname: ["vorname", "firstname", "first"],
      nachname: ["nachname", "lastname", "last"],
      waehrung: ["waehrung", "currency", "currencyisocode"]
    };
    return aliases[token] || [token];
  }

  private scoreTypeCompatibility(targetType?: string, sourceType?: string): number {
    const target = this.mapMappingDataType(targetType);
    const source = this.mapMappingDataType(sourceType);
    if (!target || !source) return 0;
    if ((target === "date" && source === "datetime") || (target === "datetime" && source === "date")) return 6;
    return target === source ? 10 : -6;
  }

  private mapMappingDataType(type?: string): string {
    const normalized = String(type || "").toLowerCase();
    if (/bool|bit/.test(normalized)) return "boolean";
    if (/date|time/.test(normalized)) return /datetime|timestamp/.test(normalized) ? "datetime" : "date";
    if (/double|currency|percent|decimal|number|int|float|money|numeric/.test(normalized)) return "number";
    return "string";
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

    if (!schedule.objectName || (schedule.objectName === "Contact" && !analysis.sourceObjectName && !analysis.targetObjectName)) {
      issues.push({ severity: "warning", message: "Object-Name konnte nicht sicher identifiziert werden" });
    }

    if (analysis.confidence < 0.5) {
      issues.push({
        severity: "warning",
        message: "Konfidenz der Anforderungs-Analyse ist niedrig - bitte überprüfen Sie manuell"
      });
    }

    if (!analysis.metadataContext?.snapshot || analysis.metadataContext.snapshot.status !== "success") {
      issues.push({
        severity: "warning",
        message: "Keine persistierten Instanz-Metadaten gefunden - Vorschlag basiert teilweise auf Heuristiken"
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
    const sageTable = this.findBestSage100Table(analysis);
    if (sageTable) {
      parts.push(`SAGE100-Tabelle: ${sageTable.name}`);
    }

    parts.push(`Konfidenz: ${(confidence * 100).toFixed(0)}%`);

    return parts.join(" • ");
  }
}
