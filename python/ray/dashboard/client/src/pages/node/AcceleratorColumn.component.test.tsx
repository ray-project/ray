import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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

  it("renders Ascend NPU details in the tooltip", async () => {
    const user = userEvent.setup();
    const accelerator: UnifiedAcceleratorStat = {
      name: "Ascend910",
      index: 0,
      type: "NPU",
      utilization: 42,
      memoryUsed: 4096,
      memoryTotal: 65536,
      rawNpu: {
        index: 0,
        cardId: 2,
        deviceId: 0,
        physicalId: 4,
        name: "Ascend910",
        utilizationAicore: 42,
        healthStatus: 0,
        temperatureC: 71,
        powerMw: 940700,
      },
    };

    render(<NodeAcceleratorEntry slot={0} accelerator={accelerator} />);
    await user.hover(screen.getByText("42.0%"));

    expect(await screen.findByText("Name: Ascend910")).toBeInTheDocument();
    expect(screen.getByText("Card/Device: 2/0")).toBeInTheDocument();
    expect(screen.getByText("Physical ID: 4")).toBeInTheDocument();
    expect(screen.getByText("Health: Healthy")).toBeInTheDocument();
    expect(screen.getByText("Temperature: 71°C")).toBeInTheDocument();
    expect(screen.getByText("Power Draw: 940.7 W")).toBeInTheDocument();
  });

  it("renders the normalized missing NPU health status", async () => {
    const user = userEvent.setup();
    const accelerator: UnifiedAcceleratorStat = {
      name: "Ascend910",
      index: 0,
      type: "NPU",
      utilization: 0,
      memoryUsed: 0,
      memoryTotal: 65536,
      rawNpu: {
        index: 0,
        cardId: 2,
        deviceId: 0,
        name: "Ascend910",
        utilizationAicore: 0,
        healthStatus: 4,
      },
    };

    render(<NodeAcceleratorEntry slot={0} accelerator={accelerator} />);
    await user.hover(screen.getByText("0.0%"));

    expect(await screen.findByText("Health: Missing")).toBeInTheDocument();
  });
});
