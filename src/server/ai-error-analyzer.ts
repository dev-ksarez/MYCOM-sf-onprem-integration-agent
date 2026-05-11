/**
 * AI-Error-Analyzer - Intelligente Fehleranalyse für Scheduler-Runs
 *
 * Nutzt das KI-System zur automatischen Analyse fehlgeschlagener Runs:
 * - Fehlerursachen-Erkennung
 * - Automatische Handlungsempfehlungen
 * - Konfigurationsprobleme identifizieren
 * - Connector-/Mapping-Fehler diagnostizieren
 */

export interface RunErrorData {
  runId: string;
  scheduleName: string;
  sourceSystem: string;
  targetSystem: string;
  errorLog: string;
  errorCode?: string;
  recordsProcessed?: number;
  failedRecords?: number;
  timestamp: Date;
}

export interface ErrorAnalysisResult {
  runId: string;
  errorCategory:
    | "connector_unavailable"
    | "authentication_failed"
    | "mapping_error"
    | "data_validation"
    | "network_issue"
    | "timeout"
    | "quota_exceeded"
    | "unknown";
  severity: "critical" | "error" | "warning";
  rootCause: string;
  recommendations: string[];
  suggestedFix?: string;
  affectedFields?: string[];
  confidence: number; // 0-1
  rawAnalysis?: string;
}

export class AIErrorAnalyzer {
  /**
   * Analysiert Fehler-Logs von fehlgeschlagenen Runs
   */
  async analyzeRunError(errorData: RunErrorData): Promise<ErrorAnalysisResult> {
    const { errorLog, errorCode, sourceSystem, targetSystem, failedRecords, recordsProcessed } = errorData;

    // Fehler-Pattern erkennen
    const category = this.detectErrorCategory(errorLog, errorCode);
    const severity = this.calculateSeverity(category, failedRecords, recordsProcessed);
    const rootCause = this.extractRootCause(errorLog, category);
    const recommendations = this.generateRecommendations(category, rootCause, sourceSystem, targetSystem);
    const affectedFields = this.extractAffectedFields(errorLog);
    const confidence = this.calculateConfidence(category, rootCause);

    return {
      runId: errorData.runId,
      errorCategory: category,
      severity,
      rootCause,
      recommendations,
      suggestedFix: this.generateSuggestedFix(category, sourceSystem, targetSystem),
      affectedFields,
      confidence
    };
  }

  /**
   * Erkennt Fehler-Kategorie aus Log und Error-Code
   */
  private detectErrorCategory(
    errorLog: string,
    errorCode?: string
  ): ErrorAnalysisResult["errorCategory"] {
    const logLower = errorLog.toLowerCase();

    // Auth-Fehler
    if (logLower.includes("unauthorized") || logLower.includes("forbidden") || logLower.includes("401") || logLower.includes("403") || errorCode?.startsWith("AUTH_")) {
      return "authentication_failed";
    }

    // Connector-Fehler
    if (logLower.includes("connection refused") || logLower.includes("connection timeout") || logLower.includes("host unreachable") || errorCode?.startsWith("CONN_")) {
      return "connector_unavailable";
    }

    // Mapping-Fehler
    if (
      logLower.includes("mapping") ||
      logLower.includes("field") ||
      logLower.includes("invalid field") ||
      logLower.includes("no matching field") ||
      errorCode?.startsWith("MAP_")
    ) {
      return "mapping_error";
    }

    // Daten-Validierung
    if (logLower.includes("validation") || logLower.includes("invalid value") || logLower.includes("type mismatch") || errorCode?.startsWith("VAL_")) {
      return "data_validation";
    }

    // Netzwerk-Fehler
    if (
      logLower.includes("network") ||
      logLower.includes("socket") ||
      logLower.includes("econnrefused") ||
      logLower.includes("enotfound") ||
      errorCode?.startsWith("NET_")
    ) {
      return "network_issue";
    }

    // Timeout
    if (logLower.includes("timeout") || logLower.includes("timed out") || errorCode?.startsWith("TIMEOUT_")) {
      return "timeout";
    }

    // Quota überschritten
    if (logLower.includes("quota") || logLower.includes("rate limit") || logLower.includes("throttled") || errorCode?.startsWith("QUOTA_")) {
      return "quota_exceeded";
    }

    return "unknown";
  }

  /**
   * Berechnet Schweregrad basierend auf Fehlertyp und Auswirkung
   */
  private calculateSeverity(
    category: ErrorAnalysisResult["errorCategory"],
    failedRecords?: number,
    recordsProcessed?: number
  ): ErrorAnalysisResult["severity"] {
    // Strukturelle Fehler sind kritisch
    if (["connector_unavailable", "authentication_failed"].includes(category)) {
      return "critical";
    }

    // Wenn >50% der Records fehlgeschlagen sind
    if (recordsProcessed && failedRecords && failedRecords > recordsProcessed * 0.5) {
      return "critical";
    }

    // Mapping/Validation sind Fehler
    if (["mapping_error", "data_validation"].includes(category)) {
      return "error";
    }

    // Rest ist Warnung
    return "warning";
  }

  /**
   * Extrahiert Root-Cause aus Error-Log
   */
  private extractRootCause(errorLog: string, category: ErrorAnalysisResult["errorCategory"]): string {
    // Erste Fehlerzeile extrahieren
    const firstErrorLine = errorLog.split("\n").find((line) => line.includes("Error") || line.includes("error"));

    if (firstErrorLine) {
      return firstErrorLine.replace(/^.*Error:?\s*/, "").substring(0, 150);
    }

    // Fallback basierend auf Kategorie
    const categoryHints: Record<string, string> = {
      authentication_failed: "Authentifizierung fehlgeschlagen - überprüfe API-Keys/Credentials",
      connector_unavailable: "Connector nicht erreichbar - überprüfe Netzwerk & Konfiguration",
      mapping_error: "Feld-Mapping Problem - Ziel-Felder passen nicht zu Source",
      data_validation: "Daten-Validierungsfehler - Datentypen oder Format stimmen nicht",
      network_issue: "Netzwerkfehler - Verbindungsproblem",
      timeout: "Timeout - Request dauerte zu lange",
      quota_exceeded: "Rate-Limit erreicht - zu viele Requests",
      unknown: "Unbekannter Fehler - siehe Logs für Details"
    };

    return categoryHints[category] || "Fehlerursache unbekannt";
  }

  /**
   * Generiert Handlungsempfehlungen basierend auf Fehler
   */
  private generateRecommendations(
    category: ErrorAnalysisResult["errorCategory"],
    rootCause: string,
    sourceSystem: string,
    targetSystem: string
  ): string[] {
    const recommendations: string[] = [];

    switch (category) {
      case "authentication_failed":
        recommendations.push(`Überprüfe ${sourceSystem}-Zugangsdaten (API-Key, Username, Passwort)`);
        recommendations.push(`Stelle sicher, dass der ${sourceSystem}-Benutzer notwendige Berechtigungen hat`);
        recommendations.push(`Überprüfe, ob Zugangsdaten abgelaufen sind`);
        break;

      case "connector_unavailable":
        recommendations.push(`Überprüfe, ob der ${sourceSystem}-Server erreichbar ist`);
        recommendations.push(`Überprüfe Netzwerkverbindung & Firewall-Regeln`);
        recommendations.push(`Versuche Connector-Verbindung zu testen`);
        break;

      case "mapping_error":
        recommendations.push(`Überprüfe Feld-Mapping: Existieren alle Ziel-Felder im ${targetSystem}?`);
        recommendations.push(`Prüfe, ob Custom Fields korrekt gemappt sind`);
        recommendations.push(`Verifiziere Source-Feld-Namen aus ${sourceSystem}`);
        break;

      case "data_validation":
        recommendations.push(`Überprüfe Datentypen der gemappten Felder`);
        recommendations.push(`Prüfe Feldlängen & erforderliche Felder`);
        recommendations.push(`Validiere Datenformat (Email, Phone, Datum, etc.)`);
        break;

      case "network_issue":
        recommendations.push(`Überprüfe Netzwerkverbindung`);
        recommendations.push(`Stelle sicher, dass Firewall ${sourceSystem} erlaubt`);
        recommendations.push(`Versuche mit VPN/Proxy zu verbinden (falls nötig)`);
        break;

      case "timeout":
        recommendations.push(`Erhöhe Timeout-Einstellung in der Scheduler-Konfiguration`);
        recommendations.push(`Überprüfe ${sourceSystem}-Performance/Last`);
        recommendations.push(`Teile Sync in kleinere Batches auf`);
        break;

      case "quota_exceeded":
        recommendations.push(`Warte auf Rate-Limit Zurücksetzen (normalerweise nach 1h)`);
        recommendations.push(`Erhöhe Batch-Größe (macht 1 Request statt vielen)`);
        recommendations.push(`Berücksichtige ${targetSystem}-API-Limits in Timing`);
        break;

      default:
        recommendations.push(`Überprüfe die detaillierten Logs`);
        recommendations.push(`Versuche manuellen Test des Connectors`);
        recommendations.push(`Kontaktiere Support mit Fehler-Details`);
    }

    return recommendations;
  }

  /**
   * Extrahiert betroffene Felder aus Error-Log
   */
  private extractAffectedFields(errorLog: string): string[] {
    const fields: string[] = [];

    // Suche nach Field-Fehler-Patterns
    const fieldPatterns = [/field[:\s]+['"]?(\w+)['"]?/gi, /column[:\s]+['"]?(\w+)['"]?/gi, /attribute[:\s]+['"]?(\w+)['"]?/gi];

    for (const pattern of fieldPatterns) {
      let match;
      while ((match = pattern.exec(errorLog)) !== null) {
        const field = match[1];
        if (field && field.length < 50 && !fields.includes(field)) {
          fields.push(field);
        }
      }
    }

    return fields.slice(0, 5); // Max 5 Felder
  }

  /**
   * Berechnet Konfidenz der Analyse (0-1)
   */
  private calculateConfidence(category: ErrorAnalysisResult["errorCategory"], rootCause: string): number {
    // Unbekannte Fehler = niedrige Konfidenz
    if (category === "unknown") {
      return 0.4;
    }

    // Spezifische Root-Causes = hohe Konfidenz
    if (rootCause.length > 30 && !rootCause.includes("unbekannt")) {
      return 0.85;
    }

    // Standard-Empfehlungen = mittlere Konfidenz
    return 0.7;
  }

  /**
   * Generiert vorgeschlagene Behebung
   */
  private generateSuggestedFix(
    category: ErrorAnalysisResult["errorCategory"],
    sourceSystem: string,
    targetSystem: string
  ): string | undefined {
    switch (category) {
      case "authentication_failed":
        return `Überprüfe und erneuere die Zugangsdaten für ${sourceSystem} im Connector-Editor`;

      case "connector_unavailable":
        return `Überprüfe unter "Connectoren" > "${sourceSystem}", ob die Verbindung funktioniert`;

      case "mapping_error":
        return `Öffne Scheduler und überprüfe die Feld-Mappings unter dem "Mapping"-Tab`;

      case "data_validation":
        return `Überprüfe die Source-Daten in ${sourceSystem} auf ungültige/unvollständige Werte`;

      case "timeout":
        return `Erhöhe unter Scheduler > Timing die Timeout-Einstellung oder reduziere Batch-Size`;

      case "quota_exceeded":
        return `Reduziere die Sync-Häufigkeit oder erhöhe die Batch-Size`;

      default:
        return undefined;
    }
  }

  /**
   * Generiert HTML für Error-Analysis UI (für Monitor-Integration)
   */
  generateAnalysisHtml(analysis: ErrorAnalysisResult): string {
    const categoryColors: Record<ErrorAnalysisResult["errorCategory"], string> = {
      authentication_failed: "danger",
      connector_unavailable: "danger",
      mapping_error: "warning",
      data_validation: "warning",
      network_issue: "danger",
      timeout: "warning",
      quota_exceeded: "info",
      unknown: "secondary"
    };

    const color = categoryColors[analysis.errorCategory];
    const confidencePercent = Math.round(analysis.confidence * 100);

    return `
      <div class="card soft-card border-${color}">
        <div class="card-header bg-light d-flex justify-content-between align-items-center">
          <div>
            <strong>KI-Fehleranalyse</strong>
            <span class="badge bg-${color} ms-2">${analysis.errorCategory}</span>
          </div>
          <span class="badge bg-secondary">${confidencePercent}% Konfidenz</span>
        </div>
        <div class="card-body">
          <div class="mb-3">
            <small class="text-secondary">Severity:</small>
            <span class="badge bg-${analysis.severity === "critical" ? "danger" : analysis.severity === "error" ? "warning" : "info"}">${analysis.severity.toUpperCase()}</span>
          </div>

          <div class="mb-3">
            <small class="text-secondary d-block mb-1"><strong>Root-Cause:</strong></small>
            <p class="small mb-0">${this.escapeHtml(analysis.rootCause)}</p>
          </div>

          ${
            analysis.affectedFields && analysis.affectedFields.length > 0
              ? `
            <div class="mb-3">
              <small class="text-secondary d-block mb-1"><strong>Betroffene Felder:</strong></small>
              <div class="small">${analysis.affectedFields.map((f) => `<code>${this.escapeHtml(f)}</code>`).join(", ")}</div>
            </div>
          `
              : ""
          }

          <div class="mb-3">
            <small class="text-secondary d-block mb-2"><strong>Handlungsempfehlungen:</strong></small>
            <ul class="small mb-0">
              ${analysis.recommendations.map((rec) => `<li>${this.escapeHtml(rec)}</li>`).join("")}
            </ul>
          </div>

          ${
            analysis.suggestedFix
              ? `
            <div class="alert alert-info mb-0 small">
              <strong>Schnelle Lösung:</strong> ${this.escapeHtml(analysis.suggestedFix)}
            </div>
          `
              : ""
          }
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
