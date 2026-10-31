import {
  GPUStats,
  NPUStats,
  ProcessGPUUsage,
  ProcessNPUUsage,
  TPUStats,
} from "../type/node";

export type UnifiedProcessAcceleratorUsage = {
  pid: number;
  memoryUsage: number;
};

export type UnifiedAcceleratorStat = {
  uuid?: string;
  name: string;
  index: number;
  type: "GPU" | "NPU" | "TPU";
  utilization?: number;
  memoryUsed: number;
  memoryTotal: number;
  processesPids?: UnifiedProcessAcceleratorUsage[];
  rawGpu?: GPUStats;
  rawNpu?: NPUStats;
  rawTpu?: TPUStats;
};

export const hasAnyAccelerator = (
  gpus?: GPUStats[],
  tpus?: TPUStats[],
  npus?: NPUStats[],
): boolean => Boolean(gpus?.length || tpus?.length || npus?.length);

export const normalizeAccelerators = (
  gpus?: GPUStats[],
  tpus?: TPUStats[],
  npus?: NPUStats[],
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
  if (npus) {
    npus.forEach((npu) => {
      normalized.push({
        name: npu.name,
        index: npu.index,
        type: "NPU",
        utilization: npu.utilizationAicore,
        memoryUsed: (npu.memoryUsed ?? Number.NaN) / (1024 * 1024),
        memoryTotal: (npu.memoryTotal ?? Number.NaN) / (1024 * 1024),
        processesPids: npu.processesPids?.map((p: ProcessNPUUsage) => ({
          pid: p.pid,
          memoryUsage: p.npuMemoryUsage / (1024 * 1024),
        })),
        rawNpu: npu,
      });
    });
  }
  if (tpus) {
    tpus.forEach((tpu) => {
      normalized.push({
        name: tpu.name,
        index: tpu.index,
        type: "TPU",
        utilization: tpu.tensorcoreUtilization,
        // Convert Bytes to MiB to match GPUStats.
        // Note these metrics are missing on some TPU types.
        memoryUsed: tpu.memoryUsed / (1024 * 1024),
        memoryTotal: tpu.memoryTotal / (1024 * 1024),
        rawTpu: tpu,
      });
    });
  }
  return normalized;
};
