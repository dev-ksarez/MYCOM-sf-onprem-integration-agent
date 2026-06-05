import { ScheduleMutationInput } from "./admin-data-service";

export type AIProviderType = "rule-based" | "ollama" | "openai-compatible";

export interface AIProviderConfig {
  enabled: boolean;
  provider: AIProviderType;
  model: string;
  baseUrl?: string;
  apiKeyEnv?: string;
  temperature?: number;
  timeoutMs?: number;
  useForScheduler: boolean;
}

export interface AIProviderPublicConfig extends Omit<AIProviderConfig, "apiKeyEnv"> {
  apiKeyEnv?: string;
  hasApiKeyEnv: boolean;
}

export interface SchedulerLLMRefinementInput {
  userPrompt: string;
  context: string;
  baseSchedule: ScheduleMutationInput;
  baseReasoning: string;
}

export interface SchedulerLLMRefinementResult {
  schedule?: Partial<ScheduleMutationInput>;
  reasoning?: string;
  issues?: Array<{ severity: "warning" | "error"; message: string }>;
  confidence?: number;
}

export function normalizeAIProviderConfig(input?: Partial<AIProviderConfig>): AIProviderConfig {
  const provider = input?.provider === "ollama" || input?.provider === "openai-compatible"
    ? input.provider
    : "rule-based";
  const fallbackModel = provider === "ollama" ? "llama3.1" : provider === "openai-compatible" ? "gpt-4.1-mini" : "";
  const baseUrl = String(input?.baseUrl || (provider === "ollama" ? "http://localhost:11434" : "")).trim();
  const temperature = Number.isFinite(Number(input?.temperature)) ? Math.max(0, Math.min(2, Number(input?.temperature))) : 0.2;
  const timeoutMs = Number.isFinite(Number(input?.timeoutMs)) ? Math.max(5_000, Math.min(120_000, Number(input?.timeoutMs))) : 30_000;

  return {
    enabled: input?.enabled === true,
    provider,
    model: String(input?.model || fallbackModel).trim(),
    baseUrl: baseUrl || undefined,
    apiKeyEnv: String(input?.apiKeyEnv || "").trim() || undefined,
    temperature,
    timeoutMs,
    useForScheduler: input?.useForScheduler !== false
  };
}

export function toPublicAIProviderConfig(config: AIProviderConfig): AIProviderPublicConfig {
  return {
    ...config,
    apiKeyEnv: config.apiKeyEnv,
    hasApiKeyEnv: Boolean(config.apiKeyEnv)
  };
}

export class AIProviderClient {
  constructor(private readonly config: AIProviderConfig) {}

  public isEnabledForScheduler(): boolean {
    return this.config.enabled
      && this.config.useForScheduler
      && this.config.provider !== "rule-based"
      && Boolean(this.config.model);
  }

  public async refineScheduler(input: SchedulerLLMRefinementInput): Promise<SchedulerLLMRefinementResult | null> {
    if (!this.isEnabledForScheduler()) {
      return null;
    }

    const messages = [
      {
        role: "system",
        content: [
          "Du bist ein Konfigurationsassistent fuer einen Salesforce On-Prem Integration Agent.",
          "Nutze ausschliesslich den bereitgestellten Kontext zu Connectoren, Schedulern, Salesforce-Metadaten und SAGE100/MSSQL-Dokumentation.",
          "Erfinde keine Secrets, keine Connector-IDs und keine Salesforce-Felder.",
          "Gib nur JSON zurueck, ohne Markdown.",
          "Schema: {\"schedule\":{...Teilfelder...},\"reasoning\":\"...\",\"confidence\":0.0,\"issues\":[{\"severity\":\"warning|error\",\"message\":\"...\"}]}",
          "schedule darf nur Felder enthalten, die in der Basis-Konfiguration vorkommen oder uebliche Scheduler-Felder sind.",
          "Wenn Felder unklar sind, setze issues statt zu raten."
        ].join(" ")
      },
      {
        role: "user",
        content: JSON.stringify({
          userPrompt: input.userPrompt,
          context: input.context,
          baseReasoning: input.baseReasoning,
          baseSchedule: input.baseSchedule
        })
      }
    ];

    const raw = this.config.provider === "ollama"
      ? await this.callOllama(messages)
      : await this.callOpenAICompatible(messages);

    return this.parseJsonResult(raw);
  }

  private async callOllama(messages: Array<{ role: string; content: string }>): Promise<string> {
    const baseUrl = String(this.config.baseUrl || "http://localhost:11434").replace(/\/+$/, "");
    const response = await this.fetchWithTimeout(`${baseUrl}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: this.config.model,
        messages,
        stream: false,
        options: { temperature: this.config.temperature }
      })
    });
    const data = await response.json() as { message?: { content?: string }; response?: string };
    return String(data.message?.content || data.response || "").trim();
  }

  private async callOpenAICompatible(messages: Array<{ role: string; content: string }>): Promise<string> {
    const baseUrl = String(this.config.baseUrl || "https://api.openai.com/v1").replace(/\/+$/, "");
    const apiKey = this.config.apiKeyEnv ? String(process.env[this.config.apiKeyEnv] || "").trim() : "";
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (apiKey) {
      headers.Authorization = `Bearer ${apiKey}`;
    }

    const response = await this.fetchWithTimeout(`${baseUrl}/chat/completions`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: this.config.model,
        messages,
        temperature: this.config.temperature,
        response_format: { type: "json_object" }
      })
    });
    const data = await response.json() as { choices?: Array<{ message?: { content?: string } }> };
    return String(data.choices?.[0]?.message?.content || "").trim();
  }

  private async fetchWithTimeout(url: string, init: RequestInit): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.config.timeoutMs || 30_000);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(`KI-Provider antwortet mit HTTP ${response.status}: ${text.substring(0, 300)}`);
      }
      return response;
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new Error(`KI-Provider Timeout nach ${this.config.timeoutMs || 30_000} ms: ${url}`);
      }
      const cause = error instanceof Error && error.cause instanceof Error ? ` (${error.cause.message})` : "";
      throw new Error(`KI-Provider nicht erreichbar: ${url}${cause}`);
    } finally {
      clearTimeout(timeout);
    }
  }

  private parseJsonResult(raw: string): SchedulerLLMRefinementResult {
    const trimmed = String(raw || "").trim();
    if (!trimmed) {
      throw new Error("KI-Provider hat keine Antwort geliefert");
    }

    const jsonText = trimmed
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/```$/i, "")
      .trim();
    const parsed = JSON.parse(jsonText) as SchedulerLLMRefinementResult;
    return parsed && typeof parsed === "object" ? parsed : {};
  }
}
