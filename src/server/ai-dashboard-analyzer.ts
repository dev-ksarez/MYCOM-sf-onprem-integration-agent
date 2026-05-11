export interface AIDashboardAnalysisInput {
  serviceStatus?: string;
  schedulerStatus?: string;
  runsTotal?: number;
  runsFailed?: number;
  errorRate?: number;
  averageRunDurationMs?: number | null;
  cpuLoadPercent?: number | null;
  dataGrowthAbsolute?: number;
  dataGrowthPercent?: number | null;
  sqliteErrors?: number;
}

export interface AIDashboardAnalysisResult {
  score: number;
  status: "Gesund" | "Stabil" | "Beobachten" | "Kritisch";
  summary: string;
  recommendations: string[];
  insights: {
    runtime: string;
    errors: string;
    dataGrowth: string;
  };
}

export class AIDashboardAnalyzer {
  analyze(input: AIDashboardAnalysisInput): AIDashboardAnalysisResult {
    const runsTotal = Math.max(0, Number(input.runsTotal || 0));
    const runsFailed = Math.max(0, Number(input.runsFailed || 0));
    const errorRate = Number.isFinite(Number(input.errorRate))
      ? Math.max(0, Math.min(100, Number(input.errorRate)))
      : (runsTotal > 0 ? (runsFailed / runsTotal) * 100 : 0);
    const avgRuntimeMs = Number.isFinite(Number(input.averageRunDurationMs)) ? Number(input.averageRunDurationMs) : null;
    const cpuLoad = Number.isFinite(Number(input.cpuLoadPercent)) ? Number(input.cpuLoadPercent) : null;
    const dataGrowthAbs = Number(input.dataGrowthAbsolute || 0);
    const dataGrowthPct = Number.isFinite(Number(input.dataGrowthPercent)) ? Number(input.dataGrowthPercent) : null;
    const sqliteErrors = Math.max(0, Number(input.sqliteErrors || 0));

    let score = 100;
    const recommendations: string[] = [];

    if (String(input.serviceStatus || "").toLowerCase() !== "ok") {
      score -= 30;
      recommendations.push("Service-Health prüfen (Systemressourcen, Logs, Verfügbarkeit).");
    }

    if (String(input.schedulerStatus || "").toLowerCase() !== "running") {
      score -= 15;
      recommendations.push("Scheduler-Zustand prüfen und ggf. neu starten.");
    }

    score -= Math.min(35, Math.round(errorRate * 0.8));
    if (errorRate >= 10) {
      recommendations.push("Fehlerquote ist erhöht: Top-Fehler im Monitoring priorisiert beheben.");
    }

    if (avgRuntimeMs !== null) {
      if (avgRuntimeMs > 5 * 60_000) {
        score -= 20;
        recommendations.push("Laufzeit optimieren: Batch-Size, Filter und Quellabfragen prüfen.");
      } else if (avgRuntimeMs > 2 * 60_000) {
        score -= 10;
        recommendations.push("Mittlere Laufzeit erhöht: Langläufer analysieren.");
      }
    }

    if (cpuLoad !== null && cpuLoad > 85) {
      score -= 10;
      recommendations.push("CPU-Last hoch: Parallelität reduzieren oder Host-Ressourcen erhöhen.");
    }

    if (sqliteErrors > 0) {
      score -= Math.min(15, sqliteErrors);
      recommendations.push("SQLite-Staging-Fehler vorhanden: fehlerhafte Objekte und Mapping prüfen.");
    }

    if (dataGrowthPct !== null && dataGrowthPct > 50) {
      score -= 8;
      recommendations.push("Starker Datenwuchs erkannt: Retention, Archivierung und Incremental-Logik prüfen.");
    }

    score = Math.max(0, Math.min(100, Math.round(score)));

    const status: AIDashboardAnalysisResult["status"] = score >= 85
      ? "Gesund"
      : score >= 65
        ? "Stabil"
        : score >= 45
          ? "Beobachten"
          : "Kritisch";

    const runtimeText = avgRuntimeMs === null
      ? "Keine Laufzeitdaten"
      : `Ø Laufzeit ${this.formatDuration(avgRuntimeMs)}`;
    const errorsText = runsTotal > 0
      ? `${runsFailed}/${runsTotal} fehlgeschlagen (${Math.round(errorRate)}%)`
      : "Keine Runs im Zeitraum";
    const growthText = dataGrowthPct === null
      ? `${dataGrowthAbs >= 0 ? "+" : ""}${dataGrowthAbs} Datensätze`
      : `${dataGrowthAbs >= 0 ? "+" : ""}${dataGrowthAbs} Datensätze (${dataGrowthPct >= 0 ? "+" : ""}${dataGrowthPct.toFixed(1)}%)`;

    const summaryParts: string[] = [];
    summaryParts.push(`KI-Bewertung: ${status} (${score}/100).`);
    summaryParts.push(runtimeText + ".");
    summaryParts.push("Fehlerbild: " + errorsText + ".");
    summaryParts.push("Datenwuchs: " + growthText + ".");

    if (!recommendations.length) {
      recommendations.push("Aktueller Zustand ist stabil. Weiterhin Trends im Blick behalten.");
    }

    return {
      score,
      status,
      summary: summaryParts.join(" "),
      recommendations: recommendations.slice(0, 4),
      insights: {
        runtime: runtimeText,
        errors: errorsText,
        dataGrowth: growthText
      }
    };
  }

  private formatDuration(durationMs: number): string {
    const totalSeconds = Math.max(0, Math.round(durationMs / 1000));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return minutes + "m " + String(seconds).padStart(2, "0") + "s";
  }
}
