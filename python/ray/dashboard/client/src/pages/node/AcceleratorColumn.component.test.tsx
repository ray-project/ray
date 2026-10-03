import { render, screen } from "@testing-library/react";
import React from "react";
import { GPUStats } from "../../type/node";
import { UnifiedAcceleratorStat } from "../../util/accelerator";
import { NodeAcceleratorEntry } from "./AcceleratorColumn";

describe("NodeAcceleratorEntry", () => {
  it("renders N/A without crashing when utilization is null", () => {
    const accelerator: UnifiedAcceleratorStat = {
      name: "Tesla T4",
      index: 0,
      type: "GPU",
      utilization: null as any,
      memoryUsed: 0,
      memoryTotal: 16000,
    };

    render(<NodeAcceleratorEntry slot={0} accelerator={accelerator} />);

    expect(screen.getByText("N/A")).toBeInTheDocument();
  });

  it("renders without crashing when powerMw and temperatureC are null", () => {
    // GpuUtilizationInfo types both as Optional[int]; the reporter serializes
    // unset optionals as null rather than omitting them.
    const rawGpu = {
      uuid: "GPU-0",
      index: 0,
      name: "Tesla T4",
      utilizationGpu: 50,
      memoryUsed: 0,
      memoryTotal: 16000,
      powerMw: null,
      temperatureC: null,
    } as unknown as GPUStats;
    const accelerator: UnifiedAcceleratorStat = {
      name: "Tesla T4",
      index: 0,
      type: "GPU",
      utilization: 50,
      memoryUsed: 0,
      memoryTotal: 16000,
      rawGpu,
    };

    render(<NodeAcceleratorEntry slot={0} accelerator={accelerator} />);

    expect(screen.getByText("50.0%")).toBeInTheDocument();
  });
});
