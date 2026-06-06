import { GPUStats, ProcessGPUUsage, ProcessTPUUsage, TPUStats } from "../type/node";

export type UnifiedProcessAcceleratorUsage = {
  pid: number;
  memoryUsage: number;
};

export type UnifiedAcceleratorStat = {
  uuid?: string;
  name: string;
  index: number;
  type: "GPU" | "TPU";
  utilization?: number;
  memoryUsed: number;
  memoryTotal: number;
  processesPids?: UnifiedProcessAcceleratorUsage[];
  rawGpu?: GPUStats;
  rawTpu?: TPUStats;
};

export const normalizeAccelerators = (
  gpus?: GPUStats[],
  tpus?: TPUStats[],
): UnifiedAcceleratorStat[] => {
  const normalized: UnifiedAcceleratorStat[] = [];
  if (gpus) {
    gpus.forEach((gpu) => {
      normalized.push({
        uuid: gpu.uuid,
        name: gpu.name,
        index: gpu.index,
        type: "GPU",
        utilization: gpu.utilizationGpu,
        memoryUsed: gpu.memoryUsed,
        memoryTotal: gpu.memoryTotal,
        processesPids: gpu.processesPids?.map((p: ProcessGPUUsage) => ({
          pid: p.pid,
          memoryUsage: p.gpuMemoryUsage,
        })),
        rawGpu: gpu,
      });
    });
  }
  if (tpus) {
    tpus.forEach((tpu) => {
      if (tpu.memoryTotal <= 0) {
        // Sometimes neighboring chips are reported with placeholders like no
        // memory capacity; these should be omitted on the dashboard.
        return
      }
      normalized.push({
        name: tpu.name,
        index: tpu.index,
        type: "TPU",
        utilization: tpu.tensorcoreUtilization,
        // Convert Bytes to MiB to match GPUStats
        memoryUsed: tpu.memoryUsed / (1024 * 1024),
        memoryTotal: tpu.memoryTotal / (1024 * 1024),
        processesPids: tpu.processesPids?.map((p: ProcessTPUUsage) => ({
          pid: p.pid,
          // ProcessTPUUsage memory is also in bytes, convert if present
          memoryUsage: p.tpuMemoryUsage / (1024 * 1024),
        })),
        rawTpu: tpu,
      });
    });
  }
  return normalized;
};
