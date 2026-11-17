import { render, screen } from "@testing-library/react";
import React from "react";
import { NodeDetail, NPUStats } from "../../type/node";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import {
  NodeAcceleratorMemory,
  WorkerAcceleratorMemory,
} from "./AcceleratorMemoryColumn";

const NPU_WITHOUT_HBM: NPUStats = {
  index: 0,
  cardId: 0,
  deviceId: 0,
  name: "Ascend NPU",
  processesPids: [
    {
      pid: 123,
      npuMemoryUsage: 32 * 1024 * 1024,
    },
  ],
};

describe("AcceleratorMemoryColumn", () => {
  it("renders N/A instead of an invalid bar when node HBM data is missing", () => {
    const { container } = render(
      <NodeAcceleratorMemory
        node={{ npus: [NPU_WITHOUT_HBM] } as NodeDetail}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    expect(screen.getByText("N/A")).toBeInTheDocument();
    expect(screen.queryByText("/")).not.toBeInTheDocument();
    expect(container.innerHTML).not.toContain("NaN");
  });

  it("renders process memory without a ratio bar when total HBM is missing", () => {
    const { container } = render(
      <WorkerAcceleratorMemory workerPID={123} npus={[NPU_WITHOUT_HBM]} />,
      { wrapper: TEST_APP_WRAPPER },
    );

    expect(screen.getByText("32.00MB")).toBeInTheDocument();
    expect(screen.queryByText("N/A")).not.toBeInTheDocument();
    expect(screen.queryByText("/")).not.toBeInTheDocument();
    expect(container.innerHTML).not.toContain("NaN");
  });
});
