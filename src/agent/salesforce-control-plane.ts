import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import pino from "pino";
import { SalesforceClient } from "../clients/salesforce/salesforce-client";
import { SalesforceConfig } from "../infrastructure/config/salesforce-config";
import { HealthSnapshot } from "../server/health-snapshot";
import { triggerDashboardUpdate } from "../server/dashboard-update-service";
import { publishFileIndex } from "./geraeteakte-index-publisher";

interface SalesforceInstanceEnvConfig {
  id: string;
  name?: string;
  loginUrl: string;
  projectId?: string;
  role?: "test" | "production";
  clientId?: string;
  clientSecret?: string;
  clientIdEnv?: string;
  clientSecretEnv?: string;
  queryLimit?: number;
}

interface ResolvedControlInstance {
  id: string;
  name: string;
  projectId: string;
  role: "test" | "production";
  config: SalesforceConfig;
}

interface FieldMap {
  [canonicalName: string]: string | undefined;
}

interface AgentCommandRecord {
  Id: string;
  CreatedDate?: string;
  MSD_CommandId__c?: string;
  MSD_CommandType__c?: string;
  MSD_CommandPayload__c?: string;
  MSD_ResponseInstruction__c?: string;
  MSD_Status__c?: string;
  MSD_ExpiresAt__c?: string;
  MSD_Signature__c?: string;
}

interface RuntimeCommand {
  salesforceId: string;
  commandId: string;
  type: "restart-agent" | "request-update" | "upload-error-log";
  payload: Record<string, unknown>;
  issuedAt: string;
  expiresAt?: string;
  signature?: string;
}

export interface SalesforceControlPlaneOptions {
  logger: pino.Logger;
  agentId: string;
  getHealthSnapshot: () => HealthSnapshot;
  healthIntervalMs: number;
  commandPollIntervalMs: number;
  enabled: boolean;
  geraeteakteIndexEnabled?: boolean;
  geraeteakteIndexIntervalMs?: number;
  geraeteakteBasePath?: string;
}

export interface SalesforceControlPlaneRuntime {
  start(): void;
  stop(): void;
}

const DEFAULT_INSTANCES_FILE = path.resolve(process.cwd(), "artifacts/sf-instances.json");
const COMMAND_STATUS_PENDING = "Pending";
const COMMAND_STATUS_ACCEPTED = "Accepted";
const COMMAND_STATUS_DONE = "Done";
const COMMAND_STATUS_FAILED = "Failed";
const COMMAND_STATUS_IGNORED = "Ignored";
const SUPPORTED_COMMAND_TYPES = new Set(["restart-agent", "request-update", "upload-error-log"]);

function readConfiguredInstances(): SalesforceInstanceEnvConfig[] {
  const filePath = process.env.SF_INSTANCES_FILE || DEFAULT_INSTANCES_FILE;
  try {
    if (!fs.existsSync(filePath)) {
      return [];
    }
    const raw = fs.readFileSync(filePath, "utf8").trim();
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .filter((item): item is SalesforceInstanceEnvConfig => Boolean(item) && typeof item === "object")
      .map((item) => ({
        ...item,
        projectId: String(item.projectId || "").trim() || "default-project",
        role: item.role === "production" ? "production" : "test"
      }));
  } catch {
    return [];
  }
}

function resolveInstance(item: SalesforceInstanceEnvConfig): ResolvedControlInstance | null {
  const id = String(item.id || "").trim();
  const loginUrl = String(item.loginUrl || "").trim();
  const clientId = String(item.clientId || (item.clientIdEnv ? process.env[item.clientIdEnv] : "") || "").trim();
  const clientSecret = String(item.clientSecret || (item.clientSecretEnv ? process.env[item.clientSecretEnv] : "") || "").trim();
  if (!id || !loginUrl || !clientId || !clientSecret) {
    return null;
  }
  return {
    id,
    name: String(item.name || id).trim() || id,
    projectId: String(item.projectId || "").trim() || "default-project",
    role: item.role === "production" ? "production" : "test",
    config: {
      loginUrl,
      authType: "client_credentials",
      clientId,
      clientSecret,
      queryLimit: Number.isFinite(Number(item.queryLimit)) && Number(item.queryLimit) > 0
        ? Math.floor(Number(item.queryLimit))
        : 100
    }
  };
}

function selectControlInstance(): ResolvedControlInstance | null {
  const configuredId = String(process.env.AGENT_CONTROL_INSTANCE_ID || process.env.AGENT_SALESFORCE_INSTANCE_ID || "").trim();
  const instances = readConfiguredInstances()
    .map(resolveInstance)
    .filter((item): item is ResolvedControlInstance => Boolean(item));
  if (!instances.length) {
    return null;
  }
  if (configuredId) {
    return instances.find((item) => item.id === configuredId) || null;
  }
  return instances.find((item) => item.role === "production") || instances[0];
}

function escapeSoqlLiteral(value: unknown): string {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function normalizeApiName(value: string): string {
  return String(value || "").trim().toLowerCase();
}

function resolveField(fields: Set<string>, canonicalName: string, legacyNames: string[] = []): string | undefined {
  if (fields.has(normalizeApiName(canonicalName))) {
    return canonicalName;
  }
  return legacyNames.find((name) => fields.has(normalizeApiName(name)));
}

async function buildFieldMap(
  client: SalesforceClient,
  objectApiName: string,
  definitions: Record<string, string[]>
): Promise<FieldMap> {
  const fields = await client.describeObjectFields(objectApiName);
  const known = new Set(fields.map((field) => normalizeApiName(field.name)).filter(Boolean));
  return Object.fromEntries(
    Object.entries(definitions).map(([canonical, legacyNames]) => [canonical, resolveField(known, canonical, legacyNames)])
  );
}

async function resolveObjectApiName(
  client: SalesforceClient,
  canonicalName: string,
  legacyNames: string[]
): Promise<string> {
  const metadata = await client.listObjectMetadata();
  const knownObjects = new Map(metadata.map((entry) => [normalizeApiName(entry.name), entry.name]));
  const canonical = knownObjects.get(normalizeApiName(canonicalName));
  if (canonical) {
    return canonical;
  }
  for (const legacyName of legacyNames) {
    const legacy = knownObjects.get(normalizeApiName(legacyName));
    if (legacy) {
      return legacy;
    }
  }
  return canonicalName;
}

function requireFields(map: FieldMap, names: string[], objectApiName: string): void {
  const missing = names.filter((name) => !map[name]);
  if (missing.length) {
    throw new Error(`${objectApiName} fehlt erforderliche Felder: ${missing.join(", ")}`);
  }
}

function jsonStringifyCompact(value: unknown): string {
  return JSON.stringify(value, (_key, nestedValue) => nestedValue === undefined ? null : nestedValue);
}

function parsePayload(rawValue: unknown): Record<string, unknown> {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function hmacBase64(secret: string, value: string): string {
  return crypto.createHmac("sha256", secret).update(value).digest("base64");
}

function isValidSignature(command: RuntimeCommand, agentId: string): boolean {
  const secret = String(process.env.AGENT_COMMAND_SHARED_SECRET || "").trim();
  if (!secret) {
    return true;
  }
  const signature = String(command.signature || "").trim();
  if (!signature) {
    return false;
  }

  const candidates = [
    `${command.commandId}|${agentId}|${command.type}|${command.issuedAt}`,
    `${command.commandId}|${agentId}|${command.type}`,
    `${command.commandId}|${command.type}|${jsonStringifyCompact(command.payload)}`
  ].map((payload) => hmacBase64(secret, payload));

  return candidates.some((expected) => {
    const receivedBuffer = Buffer.from(signature);
    const expectedBuffer = Buffer.from(expected);
    return receivedBuffer.length === expectedBuffer.length && crypto.timingSafeEqual(receivedBuffer, expectedBuffer);
  });
}

function commandTargetsThisAgent(command: RuntimeCommand, instance: ResolvedControlInstance, agentId: string): boolean {
  const targetAgentId = String(command.payload.agentId || command.payload.targetAgentId || "").trim();
  const targetProjectId = String(command.payload.projectId || "").trim();
  const targetInstanceId = String(command.payload.instanceId || "").trim();
  if (targetAgentId && targetAgentId !== agentId) {
    return false;
  }
  if (targetProjectId && targetProjectId !== instance.projectId) {
    return false;
  }
  if (targetInstanceId && targetInstanceId !== instance.id) {
    return false;
  }
  return true;
}

function buildAckPayload(input: {
  command: RuntimeCommand;
  agentId: string;
  instance: ResolvedControlInstance;
  status: string;
  message?: string;
  errorCode?: string;
  errorMessage?: string;
  artifactRef?: string;
}): string {
  return jsonStringifyCompact({
    commandId: input.command.commandId,
    agentId: input.agentId,
    projectId: input.instance.projectId,
    instanceId: input.instance.id,
    status: input.status,
    executedAt: new Date().toISOString(),
    result: {
      message: input.message,
      errorCode: input.errorCode,
      errorMessage: input.errorMessage,
      artifactRef: input.artifactRef
    }
  });
}

export function createSalesforceControlPlaneRuntime(options: SalesforceControlPlaneOptions): SalesforceControlPlaneRuntime {
  let healthTimer: NodeJS.Timeout | undefined;
  let commandTimer: NodeJS.Timeout | undefined;
  let geraeteakteTimer: NodeJS.Timeout | undefined;
  let runningHealth = false;
  let runningCommands = false;
  let runningGeraeteakte = false;
  const processedCommandIds = new Set<string>();

  const getClient = async (): Promise<{ client: SalesforceClient; instance: ResolvedControlInstance } | null> => {
    const instance = selectControlInstance();
    if (!instance) {
      return null;
    }
    const client = new SalesforceClient(instance.config);
    await client.login();
    return { client, instance };
  };

  const writeCommandStatus = async (
    client: SalesforceClient,
    objectApiName: string,
    fieldMap: FieldMap,
    command: RuntimeCommand,
    status: string,
    responseInstruction: string
  ): Promise<void> => {
    await client.updateGenericRecord(objectApiName, {
      Id: command.salesforceId,
      [fieldMap.MSD_Status__c || "MSD_Status__c"]: status,
      [fieldMap.MSD_ResponseInstruction__c || "MSD_ResponseInstruction__c"]: responseInstruction
    });
  };

  const publishHealth = async (): Promise<void> => {
    if (runningHealth) {
      return;
    }
    runningHealth = true;
    try {
      const resolved = await getClient();
      if (!resolved) {
        options.logger.debug("Salesforce control plane skipped: no configured instance");
        return;
      }
      const { client, instance } = resolved;
      const objectApiName = await resolveObjectApiName(
        client,
        "MSD_AgentHealth__c",
        ["MSD_AgentPulse__c", "MSD_Heartbeat__c"]
      );
      const fieldMap = await buildFieldMap(client, objectApiName, {
        MSD_InstanceId__c: ["InstanceId__c", "MSD_SourceInstance__c"],
        MSD_ProjectId__c: ["ProjectId__c"],
        MSD_AgentVersion__c: ["Version__c", "AgentVersion__c"],
        MSD_RuntimeStatus__c: ["Status__c", "MSD_Status__c"],
        MSD_LastSeenAt__c: ["LastSeenAt__c", "MSD_LastHeartbeat__c"],
        MSD_HealthPayload__c: ["MSD_HealthJson__c", "HealthPayload__c", "Payload__c"]
      });
      requireFields(fieldMap, ["MSD_InstanceId__c", "MSD_ProjectId__c", "MSD_RuntimeStatus__c", "MSD_LastSeenAt__c", "MSD_HealthPayload__c"], objectApiName);

      const snapshot = options.getHealthSnapshot();
      const now = new Date().toISOString();
      const status = snapshot.service === "ok" && snapshot.scheduler !== "error" ? "ok" : "warning";
      const payload = jsonStringifyCompact({
        agentId: options.agentId,
        projectId: instance.projectId,
        instanceId: instance.id,
        instanceName: instance.name,
        role: instance.role,
        nodeVersion: process.versions.node,
        appVersion: process.env.npm_package_version,
        snapshot
      }).slice(0, 32768);

      const lookup = await client.queryGeneric(`
        SELECT Id
        FROM ${objectApiName}
        WHERE ${fieldMap.MSD_InstanceId__c} = '${escapeSoqlLiteral(instance.id)}'
          AND ${fieldMap.MSD_ProjectId__c} = '${escapeSoqlLiteral(instance.projectId)}'
        ORDER BY LastModifiedDate DESC
        LIMIT 1
      `);
      const values = {
        [fieldMap.MSD_InstanceId__c || "MSD_InstanceId__c"]: instance.id,
        [fieldMap.MSD_ProjectId__c || "MSD_ProjectId__c"]: instance.projectId,
        [fieldMap.MSD_AgentVersion__c || "MSD_AgentVersion__c"]: process.env.npm_package_version || "unknown",
        [fieldMap.MSD_RuntimeStatus__c || "MSD_RuntimeStatus__c"]: status,
        [fieldMap.MSD_LastSeenAt__c || "MSD_LastSeenAt__c"]: now,
        [fieldMap.MSD_HealthPayload__c || "MSD_HealthPayload__c"]: payload
      };
      const existingId = String(lookup[0]?.Id || "").trim();
      if (existingId) {
        await client.updateGenericRecord(objectApiName, { Id: existingId, ...values });
      } else {
        await client.createGenericRecord(objectApiName, values);
      }
      options.logger.debug({ instanceId: instance.id, projectId: instance.projectId }, "Salesforce health pulse published");
    } catch (error) {
      options.logger.warn({ err: error }, "Salesforce health pulse failed");
    } finally {
      runningHealth = false;
    }
  };

  const executeCommand = async (
    client: SalesforceClient,
    objectApiName: string,
    fieldMap: FieldMap,
    command: RuntimeCommand,
    instance: ResolvedControlInstance
  ): Promise<void> => {
    if (processedCommandIds.has(command.commandId)) {
      return;
    }
    processedCommandIds.add(command.commandId);

    if (!SUPPORTED_COMMAND_TYPES.has(command.type)) {
      await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_IGNORED, buildAckPayload({
        command,
        agentId: options.agentId,
        instance,
        status: COMMAND_STATUS_IGNORED,
        errorCode: "unsupported_command_type",
        errorMessage: `Unsupported command type: ${command.type}`
      }));
      return;
    }

    if (!commandTargetsThisAgent(command, instance, options.agentId)) {
      await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_IGNORED, buildAckPayload({
        command,
        agentId: options.agentId,
        instance,
        status: COMMAND_STATUS_IGNORED,
        errorCode: "command_not_targeted",
        errorMessage: "Command target does not match this agent."
      }));
      return;
    }

    if (!isValidSignature(command, options.agentId)) {
      await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_IGNORED, buildAckPayload({
        command,
        agentId: options.agentId,
        instance,
        status: COMMAND_STATUS_IGNORED,
        errorCode: "invalid_signature",
        errorMessage: "Command signature is missing or invalid."
      }));
      return;
    }

    await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_ACCEPTED, buildAckPayload({
      command,
      agentId: options.agentId,
      instance,
      status: COMMAND_STATUS_ACCEPTED,
      message: "Command accepted."
    }));

    try {
      if (command.type === "request-update") {
        const result = await triggerDashboardUpdate();
        await writeCommandStatus(client, objectApiName, fieldMap, command, result.ok ? COMMAND_STATUS_DONE : COMMAND_STATUS_FAILED, buildAckPayload({
          command,
          agentId: options.agentId,
          instance,
          status: result.ok ? COMMAND_STATUS_DONE : COMMAND_STATUS_FAILED,
          message: result.message,
          errorCode: result.ok ? undefined : "update_request_failed",
          errorMessage: result.ok ? undefined : result.message
        }));
        return;
      }

      if (command.type === "upload-error-log") {
        const artifactRef = path.resolve(process.cwd(), "logs");
        await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_DONE, buildAckPayload({
          command,
          agentId: options.agentId,
          instance,
          status: COMMAND_STATUS_DONE,
          message: "Log artifact reference prepared.",
          artifactRef
        }));
        return;
      }

      if (command.type === "restart-agent") {
        const restartEnabled = /^(1|true|yes|on)$/i.test(String(process.env.AGENT_REMOTE_COMMAND_RESTART_ENABLED || ""));
        await writeCommandStatus(client, objectApiName, fieldMap, command, restartEnabled ? COMMAND_STATUS_DONE : COMMAND_STATUS_IGNORED, buildAckPayload({
          command,
          agentId: options.agentId,
          instance,
          status: restartEnabled ? COMMAND_STATUS_DONE : COMMAND_STATUS_IGNORED,
          message: restartEnabled
            ? "Agent restart requested. Process exits after ACK."
            : "Restart command ignored because AGENT_REMOTE_COMMAND_RESTART_ENABLED is not enabled.",
          errorCode: restartEnabled ? undefined : "restart_disabled"
        }));
        if (restartEnabled) {
          setTimeout(() => process.exit(0), 500);
        }
      }
    } catch (error) {
      await writeCommandStatus(client, objectApiName, fieldMap, command, COMMAND_STATUS_FAILED, buildAckPayload({
        command,
        agentId: options.agentId,
        instance,
        status: COMMAND_STATUS_FAILED,
        errorCode: "execution_failed",
        errorMessage: error instanceof Error ? error.message : String(error)
      }));
    }
  };

  const publishGeraeteakteIndex = async (): Promise<void> => {
    if (runningGeraeteakte) {
      return;
    }
    const basePath = String(options.geraeteakteBasePath || "").trim();
    if (!basePath) {
      options.logger.debug("Geräteakte index push skipped: FILE_BROWSE_BASE_PATH not configured");
      return;
    }
    runningGeraeteakte = true;
    try {
      const resolved = await getClient();
      if (!resolved) {
        options.logger.debug("Geräteakte index push skipped: no configured instance");
        return;
      }
      await publishFileIndex(resolved.client, basePath, options.logger);
    } catch (error) {
      options.logger.warn({ err: error }, "Geräteakte Datei-Index-Push fehlgeschlagen");
    } finally {
      runningGeraeteakte = false;
    }
  };

  const pollCommands = async (): Promise<void> => {
    if (runningCommands) {
      return;
    }
    runningCommands = true;
    try {
      const resolved = await getClient();
      if (!resolved) {
        return;
      }
      const { client, instance } = resolved;
      const objectApiName = await resolveObjectApiName(
        client,
        "MSD_AgentCommand__c",
        ["MSD_RemoteCommand__c", "MSD_AgentInstruction__c"]
      );
      const fieldMap = await buildFieldMap(client, objectApiName, {
        MSD_CommandId__c: ["CommandId__c"],
        MSD_CommandType__c: ["CommandType__c", "Type__c"],
        MSD_CommandPayload__c: ["MSD_PayloadJson__c", "Payload__c"],
        MSD_ResponseInstruction__c: ["ResponseInstruction__c", "Instruction__c"],
        MSD_Status__c: ["Status__c"],
        MSD_ExpiresAt__c: ["ExpiresAt__c"],
        MSD_Signature__c: ["Signature__c"]
      });
      requireFields(fieldMap, ["MSD_CommandId__c", "MSD_CommandType__c", "MSD_Status__c", "MSD_ResponseInstruction__c"], objectApiName);

      const now = new Date().toISOString();
      const expiresFilter = fieldMap.MSD_ExpiresAt__c
        ? `AND (${fieldMap.MSD_ExpiresAt__c} = NULL OR ${fieldMap.MSD_ExpiresAt__c} >= ${now})`
        : "";
      const records = await client.queryGeneric(`
        SELECT Id, CreatedDate,
          ${fieldMap.MSD_CommandId__c},
          ${fieldMap.MSD_CommandType__c},
          ${fieldMap.MSD_Status__c},
          ${fieldMap.MSD_ResponseInstruction__c}
          ${fieldMap.MSD_CommandPayload__c ? `, ${fieldMap.MSD_CommandPayload__c}` : ""}
          ${fieldMap.MSD_ExpiresAt__c ? `, ${fieldMap.MSD_ExpiresAt__c}` : ""}
          ${fieldMap.MSD_Signature__c ? `, ${fieldMap.MSD_Signature__c}` : ""}
        FROM ${objectApiName}
        WHERE ${fieldMap.MSD_Status__c} IN ('${COMMAND_STATUS_PENDING}', 'pending', 'PENDING')
          ${expiresFilter}
        ORDER BY CreatedDate ASC
        LIMIT 25
      `) as unknown as AgentCommandRecord[];

      for (const record of records) {
        const type = String(record[fieldMap.MSD_CommandType__c as keyof AgentCommandRecord] || "").trim() as RuntimeCommand["type"];
        const command: RuntimeCommand = {
          salesforceId: String(record.Id || "").trim(),
          commandId: String(record[fieldMap.MSD_CommandId__c as keyof AgentCommandRecord] || record.Id || "").trim(),
          type,
          payload: parsePayload(fieldMap.MSD_CommandPayload__c ? record[fieldMap.MSD_CommandPayload__c as keyof AgentCommandRecord] : undefined),
          issuedAt: String(record.CreatedDate || "").trim(),
          expiresAt: fieldMap.MSD_ExpiresAt__c ? String(record[fieldMap.MSD_ExpiresAt__c as keyof AgentCommandRecord] || "").trim() : undefined,
          signature: fieldMap.MSD_Signature__c ? String(record[fieldMap.MSD_Signature__c as keyof AgentCommandRecord] || "").trim() : undefined
        };
        if (!command.salesforceId || !command.commandId) {
          continue;
        }
        await executeCommand(client, objectApiName, fieldMap, command, instance);
      }
    } catch (error) {
      options.logger.warn({ err: error }, "Salesforce remote command polling failed");
    } finally {
      runningCommands = false;
    }
  };

  return {
    start(): void {
      if (!options.enabled) {
        options.logger.info("Salesforce control plane disabled");
        return;
      }
      void publishHealth();
      void pollCommands();
      healthTimer = setInterval(() => void publishHealth(), options.healthIntervalMs);
      commandTimer = setInterval(() => void pollCommands(), options.commandPollIntervalMs);

      if (options.geraeteakteIndexEnabled && options.geraeteakteBasePath) {
        const interval = options.geraeteakteIndexIntervalMs && options.geraeteakteIndexIntervalMs > 0
          ? options.geraeteakteIndexIntervalMs
          : 300_000;
        void publishGeraeteakteIndex();
        geraeteakteTimer = setInterval(() => void publishGeraeteakteIndex(), interval);
        options.logger.info({ geraeteakteIndexIntervalMs: interval }, "Geräteakte index push enabled");
      }

      options.logger.info(
        { healthIntervalMs: options.healthIntervalMs, commandPollIntervalMs: options.commandPollIntervalMs },
        "Salesforce control plane started"
      );
    },
    stop(): void {
      if (healthTimer) {
        clearInterval(healthTimer);
      }
      if (commandTimer) {
        clearInterval(commandTimer);
      }
      if (geraeteakteTimer) {
        clearInterval(geraeteakteTimer);
      }
      healthTimer = undefined;
      commandTimer = undefined;
      geraeteakteTimer = undefined;
    }
  };
}
