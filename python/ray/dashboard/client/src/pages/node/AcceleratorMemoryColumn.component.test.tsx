import { render, screen } from "@testing-library/react";
import React from "react";
import { GPUStats, NodeDetail } from "../../type/node";
import {
  NodeAcceleratorMemory,
  WorkerAcceleratorMemory,
} from "./AcceleratorMemoryColumn";

// A GPU with no separate memory pool (e.g. a unified-memory part such as GB10):
// NVML returns NVML_ERROR_NOT_SUPPORTED for the device memory query, so both
// device-level figures are null, while the per-process APIs still report real
// `usedGpuMemory` for any process on the device.
const UNIFIED_MEMORY_GPU: GPUStats = {
  uuid: "gpu-1",
  name: "NVIDIA GB10",
  index: 0,
  utilizationGpu: 30,
  memoryUsed: null,
  memoryTotal: null,
  processesPids: [{ pid: 1234, gpuMemoryUsage: 245 }],
};

describe("NodeAcceleratorMemory", () => {
  it("renders N/A when the device reports no memory pool", () => {
    const node = { gpus: [UNIFIED_MEMORY_GPU] } as NodeDetail;

    render(<NodeAcceleratorMemory node={node} />);

    expect(screen.getByText("N/A")).toBeInTheDocument();
  });

  it("renders a used/total ratio when memory is reported", () => {
    const node = {
      gpus: [{ ...UNIFIED_MEMORY_GPU, memoryUsed: 1024, memoryTotal: 4096 }],
    } as NodeDetail;

    render(<NodeAcceleratorMemory node={node} />);

    expect(screen.getByText("1.00GB/4.00GB")).toBeInTheDocument();
    expect(screen.queryByText("N/A")).not.toBeInTheDocument();
  });
});

describe("WorkerAcceleratorMemory", () => {
  it("renders absolute process usage when the device total is unknown", () => {
    // The worker row gets a real per-process figure even though the device
    // total is null, so it must show the measurement rather than N/A. There is
    // no total to divide by, so no ratio is rendered.
    render(
      <WorkerAcceleratorMemory workerPID={1234} gpus={[UNIFIED_MEMORY_GPU]} />,
    );

    expect(screen.getByText("245.00MB")).toBeInTheDocument();
    expect(screen.queryByText("N/A")).not.toBeInTheDocument();
  });

  it("renders N/A when the worker has no process on the device", () => {
    render(
      <WorkerAcceleratorMemory workerPID={9999} gpus={[UNIFIED_MEMORY_GPU]} />,
    );

    expect(screen.getByText("N/A")).toBeInTheDocument();
  });
});
