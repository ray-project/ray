import { render, screen } from "@testing-library/react";
import React from "react";
import { TPUStats } from "../../type/node";
import { NodeAcceleratorMemory } from "./AcceleratorMemoryColumn";

describe("NodeAcceleratorMemory", () => {
  it("renders without crashing when a TPU reports no hbmUtilization", () => {
    // TpuUtilizationInfo types hbmUtilization as optional, and the reporter
    // serializes unset optionals as null rather than omitting them.
    const tpu = {
      index: 0,
      name: "tpu-0",
      tpuType: "v6e",
      tpuTopology: "2x2",
      tensorcoreUtilization: null,
      hbmUtilization: null,
      memoryUsed: 0,
      memoryTotal: 0,
    } as unknown as TPUStats;

    render(<NodeAcceleratorMemory node={{ gpus: [], tpus: [tpu] } as any} />);

    expect(screen.getByText(/\[0\]:/)).toBeInTheDocument();
  });

  it("renders N/A when the node has no accelerators", () => {
    render(<NodeAcceleratorMemory node={{ gpus: [], tpus: [] } as any} />);

    expect(screen.getByText("N/A")).toBeInTheDocument();
  });
});
