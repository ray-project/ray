import { render, screen } from "@testing-library/react";
import React from "react";
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
});
