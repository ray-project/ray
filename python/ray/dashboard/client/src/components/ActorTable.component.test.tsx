import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { ActorDetail } from "../type/actor";
import { TEST_APP_WRAPPER } from "../util/test-utils";
import ActorTable from "./ActorTable";

const MOCK_ACTORS: { [actorId: string]: ActorDetail } = {
  ACTOR_1: {
    actorId: "ACTOR_1",
    jobId: "01000000",
    address: {
      nodeId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e1",
      ipAddress: "172.31.11.178",
      port: 10003,
      workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6d",
    },
    state: "ALIVE",
    numRestarts: "0",
    name: "",
    pid: 25321,
    startTime: 1679010689148,
    endTime: 0,
    actorClass: "Counter",
    exitDetail: "-",
    requiredResources: {},
    placementGroupId: "123",
    reprName: ",",
    workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6d",
    numPendingTasks: 0,
    taskQueueLength: 0,
    numExecutedTasks: 0,
    numInPlasma: 0,
    numLocalObjects: 0,
    numObjectRefsInScope: 0,
    gpus: [],
    processStats: {
      cmdline: [],
      cpuPercent: 0,
      cpuTimes: {
        user: 0,
        system: 0,
        childrenUser: 0,
        childrenUystem: 0,
        iowait: 0,
      },
      createTime: 0,
      memoryInfo: {
        rss: 0,
        vms: 0,
        pfaults: 0,
        pageins: 0,
      },
      pid: 25321,
    },
    labelSelector: {
      "test-label-key": "test-label-value",
    },
  },
  ACTOR_2: {
    actorId: "ACTOR_2",
    jobId: "01000000",
    address: {
      nodeId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e1",
      ipAddress: "172.31.11.178",
      port: 10003,
      workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6d",
    },
    state: "DEAD",
    numRestarts: "0",
    name: "",
    pid: 25322,
    startTime: 1679010689150,
    endTime: 0,
    actorClass: "Counter",
    exitDetail: "-",
    requiredResources: {},
    placementGroupId: "123",
    reprName: ",",
    workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6d",
    numPendingTasks: 0,
    taskQueueLength: 0,
    numExecutedTasks: 0,
    numInPlasma: 0,
    numLocalObjects: 0,
    numObjectRefsInScope: 0,
    gpus: [],
    processStats: {
      cmdline: [],
      cpuPercent: 0,
      cpuTimes: {
        user: 0,
        system: 0,
        childrenUser: 0,
        childrenUystem: 0,
        iowait: 0,
      },
      createTime: 0,
      memoryInfo: {
        rss: 0,
        vms: 0,
        pfaults: 0,
        pageins: 0,
      },
      pid: 25322,
    },
    labelSelector: {},
  },
};

// These tests are slow because they involve a lot of interactivity.
// Clicking various buttons and waiting for the table to update.
// So we increase the timeout to 40 seconds.
jest.setTimeout(40000);

describe("ActorTable", () => {
  it("renders a table of actors filtered by node ID", async () => {
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        address: {
          nodeId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e2",
          ipAddress: "172.31.11.178",
          port: 10003,
          workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6e",
        },
      },
    };

    const user = userEvent.setup();
    render(<ActorTable actors={RUNNING_ACTORS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    const nodeIdFilter = screen.getByTestId("nodeIdFilter");
    const input = within(nodeIdFilter).getByRole("combobox");

    // Filter by node ID of ACTOR_2
    await user.type(
      input,
      "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e2",
    );
    await screen.findAllByText("Actor ID");

    expect(screen.queryByText("ACTOR_1")).not.toBeInTheDocument();
    expect(screen.queryByText("ACTOR_2")).toBeInTheDocument();
  });

  it("renders a table of actors sorted by state", () => {
    const { getByRole } = render(
      <ActorTable actors={MOCK_ACTORS} showAcceleratorColumns={false} />,
      {
        wrapper: TEST_APP_WRAPPER,
      },
    );

    const actor1Row = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2Row = getByRole("row", {
      name: /ACTOR_2/,
    });

    expect(within(actor1Row).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2Row).getByText("ACTOR_2")).toBeInTheDocument();

    expect(actor1Row.compareDocumentPosition(actor2Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear after actor1Row

    // Accelerator columns hidden when showAcceleratorColumns is false.
    expect(screen.queryByText("Accelerator")).not.toBeInTheDocument();
    expect(screen.queryByText("Accelerator Memory")).not.toBeInTheDocument();
  });

  it("renders a table of actors sorted by startTime desc when states are the same", () => {
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        state: "ALIVE",
      },
    };

    const { getByRole } = render(<ActorTable actors={RUNNING_ACTORS} />, {
      wrapper: TEST_APP_WRAPPER,
    });
    const actor1Row = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2Row = getByRole("row", {
      name: /ACTOR_2/,
    });

    expect(within(actor1Row).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2Row).getByText("ACTOR_2")).toBeInTheDocument();
    expect(
      screen.queryByText('{ "test-label-key": "test-label-value" }'),
    ).toBeInTheDocument();

    expect(actor2Row.compareDocumentPosition(actor1Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear after actor2Row
  });

  it("renders a table of actors sorted by startTime desc when states are the same, actor1 appears first", () => {
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        state: "ALIVE",
        startTime: 1679010689146,
      },
    };

    const { getByRole } = render(<ActorTable actors={RUNNING_ACTORS} />, {
      wrapper: TEST_APP_WRAPPER,
    });
    const actor1Row = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2Row = getByRole("row", {
      name: /ACTOR_2/,
    });

    expect(within(actor1Row).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2Row).getByText("ACTOR_2")).toBeInTheDocument();

    expect(actor1Row.compareDocumentPosition(actor2Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear before actor2Row
  });

  it("renders a table of actors with same state sorted by resource utilization", async () => {
    /*
    When sorted by
      - CPU: Actor 2 CPU > Actor 1 CPU --> Actor 2 row before Actor 1 row
      - Used memory: Actor 1 memory > Actor 2 memory --> Actor 1 row before Actor 2 row
      - Uptime: Actor 2 uptime < Actor 1 uptime --> Actor 2 row before Actor 1 row
      - Accelerator Utilization: Actor 1 GPU > Actor 2 GPU --> Actor 1 row before Actor 2 row
      - Accelerator Memory Usage: Actor 2 GRAM > Actor 1 GRAM --> Actor 2 row before Actor 1 row
    */
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_1: {
        ...MOCK_ACTORS.ACTOR_1,
        state: "ALIVE",
        startTime: 0,
        processStats: {
          cmdline: [],
          cpuPercent: 0,
          cpuTimes: {
            user: 0,
            system: 0,
            childrenUser: 0,
            childrenUystem: 0,
            iowait: 0,
          },
          createTime: 0,
          memoryInfo: {
            rss: 10,
            vms: 0,
            pfaults: 0,
            pageins: 0,
          },
          pid: 25321,
        },
        gpus: [
          {
            uuid: "mock_gpu_uuid1",
            index: 0,
            name: "mock_gpu_name1",
            utilizationGpu: 50,
            memoryUsed: 0,
            memoryTotal: 20,
            processesPids: [{ pid: 25321, gpuMemoryUsage: 0 }],
          },
        ],
      },
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        state: "ALIVE",
        startTime: 1,
        processStats: {
          cmdline: [],
          cpuPercent: 20,
          cpuTimes: {
            user: 0,
            system: 0,
            childrenUser: 0,
            childrenUystem: 0,
            iowait: 0,
          },
          createTime: 0,
          memoryInfo: {
            rss: 0,
            vms: 0,
            pfaults: 0,
            pageins: 0,
          },
          pid: 25322,
        },
        gpus: [
          {
            uuid: "mock_gpu_uuid2",
            index: 0,
            name: "mock_gpu_name2",
            utilizationGpu: 0,
            memoryUsed: 10,
            memoryTotal: 20,
            processesPids: [{ pid: 25322, gpuMemoryUsage: 10 }],
          },
        ],
      },
    };

    const user = userEvent.setup();
    render(<ActorTable actors={RUNNING_ACTORS} />, {
      wrapper: TEST_APP_WRAPPER,
    });

    // Sort by CPU utilization
    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(screen.getByRole("option", { name: /CPU/ }));
    const actor1CPURow = screen.getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2CPURow = screen.getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1CPURow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2CPURow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2CPURow.compareDocumentPosition(actor1CPURow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row

    // Sort by used memory
    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(screen.getByRole("option", { name: /Used Memory/ }));
    const actor1MemRow = screen.getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2MemRow = screen.getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1MemRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2MemRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor1MemRow.compareDocumentPosition(actor2MemRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear before actor2Row

    // Sort by uptime
    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(screen.getByRole("option", { name: /Uptime/ }));
    const actor1UptimeRow = screen.getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2UptimeRow = screen.getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1UptimeRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2UptimeRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2UptimeRow.compareDocumentPosition(actor1UptimeRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row

    // Sort by accelerator utilization
    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(
      screen.getByRole("option", { name: /Accelerator Utilization/ }),
    );
    const actor1GPURow = screen.getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2GPURow = screen.getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1GPURow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2GPURow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor1GPURow.compareDocumentPosition(actor2GPURow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear before actor2Row

    // Sort by accelerator memory usage
    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(
      screen.getByRole("option", { name: /Accelerator Memory Usage/ }),
    );
    const actor1GRAMRow = screen.getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2GRAMRow = screen.getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1GRAMRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2GRAMRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2GRAMRow.compareDocumentPosition(actor1GRAMRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row

    // Generic accelerator columns are visible when actors have accelerators.
    expect(screen.getByText("Accelerator")).toBeInTheDocument();
    expect(screen.getByText("Accelerator Memory")).toBeInTheDocument();
  });

  it("renders and sorts Ascend NPU utilization and memory", async () => {
    const NPU_ACTORS: { [actorId: string]: ActorDetail } = {
      ACTOR_1: {
        ...MOCK_ACTORS.ACTOR_1,
        state: "ALIVE",
        npus: [
          {
            index: 0,
            cardId: 0,
            deviceId: 0,
            name: "Ascend910",
            utilizationAicore: 20,
            memoryUsed: 32 * 1024 * 1024,
            memoryTotal: 64 * 1024 * 1024,
            processesPids: [{ pid: 25321, npuMemoryUsage: 30 * 1024 * 1024 }],
          },
        ],
      },
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        state: "ALIVE",
        npus: [
          {
            index: 1,
            cardId: 0,
            deviceId: 1,
            name: "Ascend910",
            utilizationAicore: 80,
            memoryUsed: 16 * 1024 * 1024,
            memoryTotal: 64 * 1024 * 1024,
            processesPids: [{ pid: 25322, npuMemoryUsage: 10 * 1024 * 1024 }],
          },
        ],
      },
    };

    const user = userEvent.setup();
    render(<ActorTable actors={NPU_ACTORS} showAcceleratorColumns />, {
      wrapper: TEST_APP_WRAPPER,
    });

    expect(screen.getByText("20.0%")).toBeInTheDocument();
    expect(screen.getByText("80.0%")).toBeInTheDocument();

    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(
      screen.getByRole("option", { name: /Accelerator Utilization/ }),
    );
    const actor1UtilizationRow = screen.getByRole("row", { name: /ACTOR_1/ });
    const actor2UtilizationRow = screen.getByRole("row", { name: /ACTOR_2/ });
    expect(
      actor2UtilizationRow.compareDocumentPosition(actor1UtilizationRow),
    ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);

    await user.click(screen.getByRole("combobox", { name: /Sort By/ }));
    await user.click(
      screen.getByRole("option", { name: /Accelerator Memory Usage/ }),
    );
    const actor1MemoryRow = screen.getByRole("row", { name: /ACTOR_1/ });
    const actor2MemoryRow = screen.getByRole("row", { name: /ACTOR_2/ });
    expect(actor1MemoryRow.compareDocumentPosition(actor2MemoryRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
  });
});
