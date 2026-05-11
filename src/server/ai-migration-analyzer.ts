/**
 * KI-Migration-Analyzer - Intelligente Datenquellen-Analyse mit Datenschutz
 *
 * Features:
 * - Automatische Datenquellen-Struktur-Analyse
 * - Sensitive Felder erkennen (PII)
 * - Datenschutz-Compliance (DSGVO, etc.)
 * - Privacy-bewusste Mapping-Generierung
 * - Anonymisierungs-Empfehlungen
 * - Migrationskonfiguration mit Datenschutz
 */

export interface MigrationSourceData {
  sourceName: string;
  sourceType: "MSSQL_SQL" | "REST_API" | "FILE_CSV" | "FILE_XLSX" | "SALESFORCE" | "OTHER";
  sampleData?: Record<string, unknown>[];
  fieldDefinitions?: Array<{
    name: string;
    type: string;
    nullable?: boolean;
    sampleValues?: unknown[];
  }>;
  estimatedRecords?: number;
  description?: string;
}

export interface SensitiveField {
  fieldName: string;
  category: "personal_name" | "email" | "phone" | "address" | "ssn_tax" | "financial" | "health" | "biometric" | "other";
  risk: "high" | "medium" | "low";
  reason: string;
  suggestedAction: "mask" | "anonymize" | "exclude" | "encrypt" | "hash";
}

export interface MigrationAnalysisResult {
  sourceName: string;
  sourceType: string;
  totalFields: number;
  sensitiveFields: SensitiveField[];
  dataQualityScore: number; // 0-1
  complianceIssues: string[];
  recommendations: string[];
  suggestedMappings: Array<{
    sourceField: string;
    targetField?: string;
    dataType: string;
    isSensitive: boolean;
    privacyAction?: string;
  }>;
  estimatedRecords?: number;
  confidence: number; // 0-1
}

export class AIMigrationAnalyzer {
  /**
   * Analysiert Datenquelle mit Fokus auf Datenschutz & Mapping
   */
  async analyzeMigrationSource(sourceData: MigrationSourceData): Promise<MigrationAnalysisResult> {
    const totalFields = sourceData.fieldDefinitions?.length || 0;
    const sensitiveFields = this.detectSensitiveFields(sourceData.fieldDefinitions || [], sourceData.sampleData || []);
    const complianceIssues = this.checkComplianceIssues(sensitiveFields, sourceData.sourceType);
    const recommendations = this.generateRecommendations(sensitiveFields, complianceIssues, sourceData.sourceType);
    const suggestedMappings = this.generatePrivacyAwareMappings(sourceData.fieldDefinitions || [], sensitiveFields);
    const dataQualityScore = this.calculateDataQuality(sourceData);
    const confidence = this.calculateConfidence(sourceData);

    return {
      sourceName: sourceData.sourceName,
      sourceType: sourceData.sourceType,
      totalFields,
      sensitiveFields,
      dataQualityScore,
      complianceIssues,
      recommendations,
      suggestedMappings,
      estimatedRecords: sourceData.estimatedRecords,
      confidence
    };
  }

  /**
   * Erkennt sensitive Felder (PII & weitere Kategorien)
   */
  private detectSensitiveFields(fieldDefs: MigrationSourceData["fieldDefinitions"], sampleData: Record<string, unknown>[]): SensitiveField[] {
    const sensitiveFields: SensitiveField[] = [];

    // Muster für verschiedene Feldtypen
    const patterns = {
      personal_name: {
        regex: /\b(firstName|lastName|fullName|name|foreName|surname|given_name|family_name|vorname|nachname|name)\b/i,
        reason: "Persönlicher Name (DSGVO - direkt identifizierbar)"
      },
      email: {
        regex: /\b(email|emailAddress|email_address|mail|e-mail|contact_email|email_address_primary)\b/i,
        reason: "E-Mail-Adresse (DSGVO - direkt identifizierbar)"
      },
      phone: {
        regex: /\b(phone|phoneNumber|telephone|tel|mobile|cell|phone_number|mobile_number|telefon|mobilnummer)\b/i,
        reason: "Telefonnummer (DSGVO - direkt identifizierbar)"
      },
      address: {
        regex: /\b(address|street|city|state|zip|postal|country|street_address|billing_address|home_address|adresse|stadt|plz|strasse)\b/i,
        reason: "Adresse (DSGVO - direkt identifizierbar)"
      },
      ssn_tax: {
        regex: /\b(ssn|social_security|tax_id|ein|ueid|federal_id|steuerid|personalausweis)\b/i,
        reason: "Steuernummer/Ausweisnummer (DSGVO - höchst sensitiv)"
      },
      financial: {
        regex: /\b(bankAccount|iban|bic|credit_card|card_number|account_number|routing_number|kontonummer)\b/i,
        reason: "Finanzielle Daten (PCI-DSS, DSGVO)"
      },
      health: {
        regex: /\b(health|medical|disease|diagnosis|treatment|medication|prescription|health_condition|diabetes|hypertension)\b/i,
        reason: "Gesundheitsdaten (DSGVO Art. 9 - besondere Kategorie)"
      },
      biometric: {
        regex: /\b(fingerprint|dna|iris|retina|face|biometric|biometrie)\b/i,
        reason: "Biometrische Daten (DSGVO Art. 9 - höchst sensitiv)"
      }
    };

    // Analysiere jedes Feld
    for (const fieldDef of fieldDefs || []) {
      let detectedCategory: SensitiveField["category"] | null = null;

      // Pattern-basierte Erkennung
      for (const [category, { regex }] of Object.entries(patterns)) {
        if (regex.test(fieldDef.name)) {
          detectedCategory = category as SensitiveField["category"];
          break;
        }
      }

      // Content-basierte Erkennung (sample data analysieren)
      if (!detectedCategory && fieldDef.sampleValues && fieldDef.sampleValues.length > 0) {
        detectedCategory = this.detectCategoryFromContent(fieldDef.sampleValues);
      }

      if (detectedCategory) {
        const riskLevel = this.calculateRisk(detectedCategory);
        const action = this.suggestPrivacyAction(detectedCategory);

        sensitiveFields.push({
          fieldName: fieldDef.name,
          category: detectedCategory,
          risk: riskLevel,
          reason: patterns[detectedCategory as keyof typeof patterns]?.reason || `Sensitives Feld: ${detectedCategory}`,
          suggestedAction: action
        });
      }
    }

    return sensitiveFields;
  }

  /**
   * Erkennt Feldtyp aus Sample-Daten
   */
  private detectCategoryFromContent(sampleValues: unknown[]): SensitiveField["category"] | null {
    for (const value of sampleValues) {
      if (!value) continue;
      const str = String(value).toLowerCase();

      // Email
      if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(str)) {
        return "email";
      }

      // Telefon (US/DE Format)
      if (/^[\+]?[(]?[0-9]{1,4}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,9}$/.test(str.replace(/\s/g, ""))) {
        return "phone";
      }

      // IBAN
      if (/^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$/.test(str)) {
        return "financial";
      }

      // SSN-ähnlich (999-99-9999)
      if (/^\d{3}-?\d{2}-?\d{4}$/.test(str)) {
        return "ssn_tax";
      }
    }

    return null;
  }

  /**
   * Berechnet Risiko-Level für Feldkategorie
   */
  private calculateRisk(category: SensitiveField["category"]): SensitiveField["risk"] {
    const risks: Record<SensitiveField["category"], SensitiveField["risk"]> = {
      personal_name: "high",
      email: "high",
      phone: "high",
      address: "high",
      ssn_tax: "high",
      financial: "high",
      health: "high",
      biometric: "high",
      other: "medium"
    };
    return risks[category] || "medium";
  }

  /**
   * Schlägt Privacy-Action vor
   */
  private suggestPrivacyAction(category: SensitiveField["category"]): SensitiveField["suggestedAction"] {
    const actions: Record<SensitiveField["category"], SensitiveField["suggestedAction"]> = {
      personal_name: "anonymize",
      email: "mask",
      phone: "mask",
      address: "anonymize",
      ssn_tax: "exclude", // Sollte nicht migriert werden
      financial: "encrypt",
      health: "exclude", // Sehr sensitiv
      biometric: "exclude", // Sehr sensitiv
      other: "mask"
    };
    return actions[category] || "mask";
  }

  /**
   * Prüft DSGVO-Compliance-Probleme
   */
  private checkComplianceIssues(sensitiveFields: SensitiveField[], sourceType: string): string[] {
    const issues: string[] = [];

    // Zähle nach Risiko
    const highRiskCount = sensitiveFields.filter((f) => f.risk === "high").length;
    const shouldExcludeCount = sensitiveFields.filter((f) => f.suggestedAction === "exclude").length;

    if (highRiskCount > 5) {
      issues.push(`⚠️ Viele sensitive Felder (${highRiskCount}) - Datenschutz-Audit empfohlen`);
    }

    if (sensitiveFields.some((f) => f.category === "health")) {
      issues.push("🔒 Gesundheitsdaten entdeckt - DSGVO Art. 9 (besondere Kategorie)");
    }

    if (sensitiveFields.some((f) => f.category === "biometric")) {
      issues.push("🔐 Biometrische Daten entdeckt - Höchste Schutzstufe erforderlich");
    }

    if (sensitiveFields.some((f) => f.category === "ssn_tax")) {
      issues.push("❌ Steuernummern/Ausweisnummern - DSGVO kritisch, Migration nicht empfohlen");
    }

    if (sourceType === "SALESFORCE") {
      issues.push("💼 Salesforce-Daten: Überprüfe Datenverarbeitungsvertrag (DPA)");
    }

    return issues;
  }

  /**
   * Generiert Datenschutz-Empfehlungen
   */
  private generateRecommendations(sensitiveFields: SensitiveField[], complianceIssues: string[], sourceType: string): string[] {
    const recommendations: string[] = [];

    // Alle exclude-Felder
    const excludeFields = sensitiveFields.filter((f) => f.suggestedAction === "exclude");
    if (excludeFields.length > 0) {
      recommendations.push(`❌ Ausschließen (${excludeFields.length}): ${excludeFields.map((f) => f.fieldName).join(", ")}`);
    }

    // Anonymisierung
    const anonymizeFields = sensitiveFields.filter((f) => f.suggestedAction === "anonymize");
    if (anonymizeFields.length > 0) {
      recommendations.push(`🔄 Anonymisieren (${anonymizeFields.length}): ${anonymizeFields.map((f) => f.fieldName).join(", ")}`);
    }

    // Verschlüsselung
    const encryptFields = sensitiveFields.filter((f) => f.suggestedAction === "encrypt");
    if (encryptFields.length > 0) {
      recommendations.push(`🔐 Verschlüsseln (${encryptFields.length}): ${encryptFields.map((f) => f.fieldName).join(", ")}`);
    }

    // Maskierung
    const maskFields = sensitiveFields.filter((f) => f.suggestedAction === "mask");
    if (maskFields.length > 0) {
      recommendations.push(`🔍 Maskieren (${maskFields.length}): ${maskFields.map((f) => f.fieldName).join(", ")} - z.B. ***@***.de`);
    }

    // Logging
    recommendations.push(`📋 Audit-Logging aktivieren für Migration mit sensiblen Daten`);
    recommendations.push(`✅ Datenschutzbeauftragte benachrichtigen vor Migration`);
    recommendations.push(`🔔 Betroffene benachrichtigen (Privacy Notice aktualisieren)`);

    return recommendations;
  }

  /**
   * Generiert Privacy-bewusste Mappings
   */
  private generatePrivacyAwareMappings(
    fieldDefs: MigrationSourceData["fieldDefinitions"],
    sensitiveFields: SensitiveField[]
  ): MigrationAnalysisResult["suggestedMappings"] {
    const sensitiveMap = new Map(sensitiveFields.map((f) => [f.fieldName, f]));

    return (fieldDefs || []).map((field) => {
      const sensitive = sensitiveMap.get(field.name);
      const isSensitive = !!sensitive;

      return {
        sourceField: field.name,
        targetField: this.suggestTargetField(field.name, sensitive),
        dataType: this.normalizeDataType(field.type),
        isSensitive,
        privacyAction: sensitive?.suggestedAction
      };
    });
  }

  /**
   * Schlägt Ziel-Feldname vor
   */
  private suggestTargetField(sourceField: string, sensitive?: SensitiveField): string | undefined {
    if (!sensitive) {
      return sourceField; // 1:1 Mapping für nicht-sensitive Felder
    }

    // Für sensitive Felder: Suffix hinzufügen
    const suffixes: Record<SensitiveField["suggestedAction"], string> = {
      exclude: undefined as any,
      anonymize: "_anon",
      mask: "_masked",
      encrypt: "_encrypted",
      hash: "_hashed"
    };

    const suffix = suffixes[sensitive.suggestedAction];
    return suffix ? `${sourceField}${suffix}` : undefined;
  }

  /**
   * Normalisiert Datentypen
   */
  private normalizeDataType(sqlType: string): string {
    const normalized = sqlType.toLowerCase();

    if (normalized.includes("varchar") || normalized.includes("text") || normalized.includes("char")) {
      return "text";
    }
    if (normalized.includes("int") || normalized.includes("bigint")) {
      return "integer";
    }
    if (normalized.includes("decimal") || normalized.includes("float") || normalized.includes("numeric")) {
      return "decimal";
    }
    if (normalized.includes("date") || normalized.includes("timestamp")) {
      return "datetime";
    }
    if (normalized.includes("bool") || normalized.includes("bit")) {
      return "boolean";
    }

    return normalized;
  }

  /**
   * Berechnet Datenqualitäts-Score
   */
  private calculateDataQuality(sourceData: MigrationSourceData): number {
    let score = 1.0;

    // Felder ohne Typ: -0.1
    const noTypeCount = (sourceData.fieldDefinitions || []).filter((f) => !f.type).length;
    score -= noTypeCount * 0.1;

    // Viele NULLs: -0.05 pro Feld
    const nullableCount = (sourceData.fieldDefinitions || []).filter((f) => f.nullable).length;
    score -= Math.min(nullableCount * 0.05, 0.3);

    // Große Datenmengen: -0.1
    if ((sourceData.estimatedRecords || 0) > 10000000) {
      score -= 0.1;
    }

    return Math.max(0, score);
  }

  /**
   * Berechnet Konfidenz der Analyse
   */
  private calculateConfidence(sourceData: MigrationSourceData): number {
    let confidence = 0.5;

    // Mit Sample-Daten: +0.2
    if (sourceData.sampleData && sourceData.sampleData.length > 0) {
      confidence += 0.2;
    }

    // Mit Field-Definitionen: +0.2
    if (sourceData.fieldDefinitions && sourceData.fieldDefinitions.length > 0) {
      confidence += 0.2;
    }

    // Mit Beschreibung: +0.1
    if (sourceData.description && sourceData.description.length > 20) {
      confidence += 0.1;
    }

    return Math.min(confidence, 1.0);
  }

  /**
   * Generiert HTML für Analyse-Report (für UI)
   */
  generateAnalysisHtml(analysis: MigrationAnalysisResult): string {
    const complianceColor = analysis.complianceIssues.length > 0 ? "danger" : "success";
    const qualityColor = analysis.dataQualityScore > 0.7 ? "success" : analysis.dataQualityScore > 0.4 ? "warning" : "danger";

    return `
      <div class="card soft-card">
        <div class="card-header bg-light d-flex justify-content-between align-items-center">
          <strong>KI-Migrations-Analyse</strong>
          <div class="d-flex gap-2">
            <span class="badge bg-${qualityColor}">Datenqualität: ${Math.round(analysis.dataQualityScore * 100)}%</span>
            <span class="badge bg-${complianceColor}">Compliance: ${analysis.complianceIssues.length > 0 ? "⚠️" : "✓"}</span>
          </div>
        </div>
        <div class="card-body">
          <div class="row g-3">
            <div class="col-12">
              <strong>Quelle:</strong> ${this.escapeHtml(analysis.sourceName)} (${analysis.sourceType})
              <br/>
              <small class="text-secondary">Felder: ${analysis.totalFields} | Sensitive: ${analysis.sensitiveFields.length} | Konfidenz: ${Math.round(analysis.confidence * 100)}%</small>
            </div>

            ${
              analysis.sensitiveFields.length > 0
                ? `
              <div class="col-12">
                <strong>🔒 Sensitive Felder (${analysis.sensitiveFields.length}):</strong>
                <ul class="small mb-0">
                  ${analysis.sensitiveFields
                    .map((f) => {
                      const colors: Record<SensitiveField["risk"], string> = {
                        high: "danger",
                        medium: "warning",
                        low: "info"
                      };
                      const color = colors[f.risk];
                      return `<li><code>${this.escapeHtml(f.fieldName)}</code> <span class="badge bg-${color}">${f.category}</span> - Aktion: <strong>${f.suggestedAction}</strong></li>`;
                    })
                    .join("")}
                </ul>
              </div>
            `
                : ""
            }

            ${
              analysis.complianceIssues.length > 0
                ? `
              <div class="col-12">
                <div class="alert alert-warning mb-0">
                  <strong>⚠️ Compliance-Hinweise:</strong>
                  <ul class="small mb-0">
                    ${analysis.complianceIssues.map((issue) => `<li>${this.escapeHtml(issue)}</li>`).join("")}
                  </ul>
                </div>
              </div>
            `
                : ""
            }

            <div class="col-12">
              <strong>Datenschutz-Empfehlungen:</strong>
              <ul class="small mb-0">
                ${analysis.recommendations.map((rec) => `<li>${this.escapeHtml(rec)}</li>`).join("")}
              </ul>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  private escapeHtml(text: string): string {
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
}
