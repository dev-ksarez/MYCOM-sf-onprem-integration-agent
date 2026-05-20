import fs from "node:fs/promises";
import os from "node:os";

export interface HealthSnapshot {
  service: "ok" | "degraded";
  scheduler: "running" | "idle" | "error";
  startedAt: string;
  uptimeSeconds: number;
  cpuLoadPercent?: number;
  operatingSystem?: string;
  memoryUsedBytes?: number;
  memoryTotalBytes?: number;
  diskUsedBytes?: number;
  diskTotalBytes?: number;
  agentVersion?: string;
  lastRunStartedAt?: string;
  lastRunFinishedAt?: string;
  lastRunStatus?: "success" | "error";
  lastRunError?: string;
  schedulesFound?: number;
  dueSchedules?: number;
  processedSchedules?: number;
  logRetentionDays?: number;
}

interface CpuSample {
  idle: number;
  total: number;
}

function getCpuLoadPercent(): number | undefined {
  const cpus = os.cpus();
  if (!cpus.length) {
    return undefined;
  }

  const currentSample = cpus.reduce<CpuSample>(
    (sample, cpu) => {
      const cpuTimes = cpu.times;
      const total = cpuTimes.user + cpuTimes.nice + cpuTimes.sys + cpuTimes.idle + cpuTimes.irq;
      sample.idle += cpuTimes.idle;
      sample.total += total;
      return sample;
    },
    { idle: 0, total: 0 }
  );

  const previousSample = getCpuLoadPercent.previousSample;
  getCpuLoadPercent.previousSample = currentSample;
  if (!previousSample) {
    return undefined;
  }

  const idleDelta = currentSample.idle - previousSample.idle;
  const totalDelta = currentSample.total - previousSample.total;
  if (!Number.isFinite(idleDelta) || !Number.isFinite(totalDelta) || totalDelta <= 0) {
    return undefined;
  }

  const loadPercent = (1 - idleDelta / totalDelta) * 100;
  return Math.max(0, Math.min(100, Math.round(loadPercent)));
}

getCpuLoadPercent.previousSample = undefined as CpuSample | undefined;

function getOperatingSystemLabel(): string | undefined {
  if (process.platform === "win32") {
    return `Windows ${os.release()}`;
  }

  if (process.platform === "linux") {
    return `Linux ${os.release()}`;
  }

  if (process.platform === "darwin") {
    return `macOS ${os.release()}`;
  }

  return undefined;
}

async function getDiskUsage(): Promise<{ usedBytes?: number; totalBytes?: number }> {
  if (process.platform !== "win32" && process.platform !== "linux" && process.platform !== "darwin") {
    return {};
  }

  try {
    const stats = await fs.statfs(process.cwd());
    const blockSize = Number(stats.bsize);
    const totalBlocks = Number(stats.blocks);
    const freeBlocks = Number(stats.bavail || stats.bfree);
    if (!Number.isFinite(blockSize) || !Number.isFinite(totalBlocks) || !Number.isFinite(freeBlocks)) {
      return {};
    }

    const totalBytes = Math.max(0, Math.round(totalBlocks * blockSize));
    const freeBytes = Math.max(0, Math.round(freeBlocks * blockSize));
    return {
      totalBytes,
      usedBytes: Math.max(0, totalBytes - freeBytes)
    };
  } catch {
    return {};
  }
}

export async function buildSystemHealthSnapshot(baseSnapshot: HealthSnapshot): Promise<HealthSnapshot> {
  const totalMemoryBytes = os.totalmem();
  const freeMemoryBytes = os.freemem();
  const diskUsage = await getDiskUsage();

  return {
    ...baseSnapshot,
    cpuLoadPercent: getCpuLoadPercent(),
    operatingSystem: getOperatingSystemLabel(),
    memoryTotalBytes: Number.isFinite(totalMemoryBytes) ? totalMemoryBytes : undefined,
    memoryUsedBytes:
      Number.isFinite(totalMemoryBytes) && Number.isFinite(freeMemoryBytes)
        ? Math.max(0, totalMemoryBytes - freeMemoryBytes)
        : undefined,
    diskTotalBytes: diskUsage.totalBytes,
    diskUsedBytes: diskUsage.usedBytes
  };
}
