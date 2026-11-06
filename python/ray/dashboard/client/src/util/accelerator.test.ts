import { GPUStats, NPUStats, TPUStats } from "../type/node";
import { hasAnyAccelerator, normalizeAccelerators } from "./accelerator";

describe("hasAnyAccelerator", () => {
  it("detects an NPU-only node", () => {
    const npus: NPUStats[] = [
      {
        index: 0,
        cardId: 2,
        deviceId: 0,
        name: "Ascend910",
      },
    ];

    expect(hasAnyAccelerator(undefined, undefined, npus)).toBe(true);
  });

  it("returns false when all accelerator lists are empty", () => {
    expect(hasAnyAccelerator([], [], [])).toBe(false);
  });
});

describe("normalizeAccelerators", () => {
  it("handles undefined inputs", () => {
    expect(normalizeAccelerators(undefined, undefined)).toEqual([]);
  });

  it("normalizes gpus correctly", () => {
    const gpus: GPUStats[] = [
      {
        uuid: "gpu-1",
        name: "A100",
        index: 0,
        utilizationGpu: 85,
        memoryUsed: 1024,
        memoryTotal: 4096,
        processesPids: [{ pid: 123, gpuMemoryUsage: 512 }],
      },
    ];

    const result = normalizeAccelerators(gpus, undefined);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      uuid: "gpu-1",
      name: "A100",
      index: 0,
      type: "GPU",
      utilization: 85,
      memoryUsed: 1024,
      memoryTotal: 4096,
      processesPids: [{ pid: 123, memoryUsage: 512 }],
      rawGpu: gpus[0],
    });
  });

  it("normalizes tpus correctly and converts bytes to MiB", () => {
    const tpus: TPUStats[] = [
      {
        name: "tpu-1",
        index: 0,
        tpuType: "v5e",
        tpuTopology: "2x2",
        tensorcoreUtilization: 90,
        memoryUsed: 1024 * 1024 * 1024, // 1024 MiB in bytes
        memoryTotal: 4096 * 1024 * 1024, // 4096 MiB in bytes
        processesPids: [{ pid: 456, tpuMemoryUsage: 512 * 1024 * 1024 }],
      },
    ];

    const result = normalizeAccelerators(undefined, tpus);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      name: "tpu-1",
      index: 0,
      type: "TPU",
      utilization: 90,
      memoryUsed: 1024, // Converted to MiB
      memoryTotal: 4096, // Converted to MiB
      rawTpu: tpus[0],
    });
  });

  it("normalizes Ascend NPUs and converts bytes to MiB", () => {
    const npus: NPUStats[] = [
      {
        index: 0,
        cardId: 2,
        deviceId: 0,
        physicalId: 4,
        name: "Ascend910",
        utilizationAicore: 97,
        memoryUsed: 4270 * 1024 * 1024,
        memoryTotal: 65536 * 1024 * 1024,
        healthStatus: 0,
        powerMw: 940700,
        temperatureC: 71,
        processesPids: [{ pid: 123, npuMemoryUsage: 2048 * 1024 * 1024 }],
      },
    ];

    const result = normalizeAccelerators(undefined, undefined, npus);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      name: "Ascend910",
      index: 0,
      type: "NPU",
      utilization: 97,
      memoryUsed: 4270,
      memoryTotal: 65536,
      processesPids: [{ pid: 123, memoryUsage: 2048 }],
      rawNpu: npus[0],
    });
  });

  it("does not filter out tpus with <= 0 memoryTotal", () => {
    const tpus: TPUStats[] = [
      {
        name: "tpu-1",
        index: 0,
        tpuType: "v5e",
        tpuTopology: "2x2",
        tensorcoreUtilization: 0,
        memoryUsed: 0,
        memoryTotal: 0,
      },
    ];

    const result = normalizeAccelerators(undefined, tpus);
    expect(result).toHaveLength(1);
  });

  it("handles missing memory metrics for tpus", () => {
    const tpus: any[] = [
      {
        name: "tpu-1",
        index: 0,
        tpuType: "v5e",
        tpuTopology: "2x2",
        tensorcoreUtilization: 90,
        // memoryUsed and memoryTotal missing
      },
    ];

    const result = normalizeAccelerators(undefined, tpus as TPUStats[]);
    expect(result).toHaveLength(1);
    expect(result[0].memoryUsed).toBeNaN();
    expect(result[0].memoryTotal).toBeNaN();
  });

  it("combines both gpus and tpus", () => {
    const gpus: GPUStats[] = [
      {
        uuid: "gpu-1",
        name: "A100",
        index: 0,
        memoryUsed: 1024,
        memoryTotal: 4096,
      },
    ];
    const tpus: TPUStats[] = [
      {
        name: "tpu-1",
        index: 1,
        tpuType: "v5e",
        tpuTopology: "2x2",
        memoryUsed: 1024 * 1024 * 1024,
        memoryTotal: 4096 * 1024 * 1024,
      },
    ];

    const result = normalizeAccelerators(gpus, tpus);
    expect(result).toHaveLength(2);
    expect(result[0].type).toBe("GPU");
    expect(result[1].type).toBe("TPU");
  });
});
