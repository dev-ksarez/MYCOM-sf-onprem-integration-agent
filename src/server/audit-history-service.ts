import fs from "node:fs/promises";
import path from "node:path";

const AUDIT_HISTORY_FILE = path.resolve(process.cwd(), "artifacts/audit-history.json");

export interface AuditActor {
  userId?: string;
  username?: string;
}

export async function appendAuditHistory(entry: {
  action: string;
  entityType: string;
  entityId?: string;
  entityName?: string;
  actor?: AuditActor | null;
  status?: "success" | "error";
  message?: string;
}): Promise<void> {
  const now = new Date().toISOString();
  let items: unknown[] = [];
  try {
    items = JSON.parse(await fs.readFile(AUDIT_HISTORY_FILE, "utf8")) as unknown[];
    if (!Array.isArray(items)) {
      items = [];
    }
  } catch {
    items = [];
  }

  const auditEntry = {
    id: `audit-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    at: now,
    actor: entry.actor || null,
    action: entry.action,
    entityType: entry.entityType,
    entityId: entry.entityId,
    entityName: entry.entityName,
    status: entry.status || "success",
    message: entry.message
  };
  const nextItems = [auditEntry, ...items].slice(0, 500);
  await fs.mkdir(path.dirname(AUDIT_HISTORY_FILE), { recursive: true });
  await fs.writeFile(AUDIT_HISTORY_FILE, JSON.stringify(nextItems, null, 2), "utf8");
}

export async function listAuditHistory(limit = 100, filter?: { entityType?: string; entityId?: string }): Promise<unknown[]> {
  try {
    const items = JSON.parse(await fs.readFile(AUDIT_HISTORY_FILE, "utf8")) as unknown[];
    if (!Array.isArray(items)) {
      return [];
    }
    const entityType = String(filter?.entityType || "").trim();
    const entityId = String(filter?.entityId || "").trim();
    const filtered = items.filter((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) {
        return false;
      }
      const candidate = item as Record<string, unknown>;
      if (entityType && String(candidate.entityType || "") !== entityType) {
        return false;
      }
      if (entityId && String(candidate.entityId || "") !== entityId) {
        return false;
      }
      return true;
    });
    return filtered.slice(0, Math.max(1, Math.min(500, limit)));
  } catch {
    return [];
  }
}
