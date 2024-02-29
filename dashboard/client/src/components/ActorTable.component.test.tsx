import { fireEvent, render, screen, within } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { ActorDetail } from "../type/actor";
import ActorTable from "./ActorTable";
const MOCK_ACTORS: { [actorId: string]: ActorDetail } = {
  ACTOR_1: {
    actorId: "ACTOR_1",
    jobId: "01000000",
    address: {
      rayletId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e1",
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
  },
  ACTOR_2: {
    actorId: "ACTOR_2",
    jobId: "01000000",
    address: {
      rayletId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e1",
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
  },
};
describe("ActorTable", () => {
  it("renders a table of actors filtered by node ID", async () => {
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        address: {
          rayletId: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e2",
          ipAddress: "172.31.11.178",
          port: 10003,
          workerId: "b8b276a03612644098ed7a929c3b0e50f5bde894eb0d8cab288fbb6e",
        },
      },
    };

    const { getByTestId } = render(
      <MemoryRouter>
        <ActorTable actors={RUNNING_ACTORS} />
      </MemoryRouter>,
    );

    const nodeIdFilter = getByTestId("nodeIdFilter");
    const input = within(nodeIdFilter).getByRole("textbox");
    // Filter by node ID of ACTOR_2
    fireEvent.change(input, {
      target: {
        value: "426854e68e4225b3941deaf03c8dcfcb1daacc69a92711d370dbb0e2",
      },
    });
    await screen.findByText("Actor ID");

    expect(screen.queryByText("ACTOR_1")).not.toBeInTheDocument();
    expect(screen.queryByText("ACTOR_2")).toBeInTheDocument();
  });

  it("renders a table of actors sorted by state", () => {
    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={MOCK_ACTORS} />
      </MemoryRouter>,
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
  });

  it("renders a table of actors sorted by startTime desc when states are the same", () => {
    const RUNNING_ACTORS = {
      ...MOCK_ACTORS,
      ACTOR_2: {
        ...MOCK_ACTORS.ACTOR_2,
        state: "ALIVE",
      },
    };

    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={RUNNING_ACTORS} />
      </MemoryRouter>,
    );
    const actor1Row = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2Row = getByRole("row", {
      name: /ACTOR_2/,
    });

    expect(within(actor1Row).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2Row).getByText("ACTOR_2")).toBeInTheDocument();

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

    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={RUNNING_ACTORS} />
      </MemoryRouter>,
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
    ); // actor1Row appear before actor2Row
  });

  it("renders a table of actors with same state sorted by resource utilization", () => {
    /*
    When sorted by
      - CPU: Actor 2 CPU > Actor 1 CPU --> Actor 2 row before Actor 1 row
      - Used memory: Actor 1 memory > Actor 2 memory --> Actor 1 row before Actor 2 row
      - Uptime: Actor 2 uptime < Actor 1 uptime --> Actor 2 row before Actor 1 row
      - GPU Utilization: Actor 1 GPU > Actor 2 GPU --> Actor 1 row before Actor 2 row
      - GRAM Utilization: Actor 2 GRAM > Actor 1 GRAM --> Actor 2 row before Actor 1 row
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
            processes: [{ pid: 25321, gpuMemoryUsage: 0 }],
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
            processes: [{ pid: 25322, gpuMemoryUsage: 10 }],
          },
        ],
      },
    };

    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={RUNNING_ACTORS} />
      </MemoryRouter>,
    );
    const sortByFilter = screen.getByTestId("sortByFilter");
    const input = within(sortByFilter).getByRole("textbox", { hidden: true });
    // Sort by CPU utilization
    fireEvent.change(input, {
      target: {
        value: "processStats.cpuPercent",
      },
    });
    const actor1CPURow = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2CPURow = getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1CPURow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2CPURow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2CPURow.compareDocumentPosition(actor1CPURow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row

    // Sort by used memory
    fireEvent.change(input, {
      target: {
        value: "processStats.memoryInfo.rss",
      },
    });
    const actor1MemRow = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2MemRow = getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1MemRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2MemRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor1MemRow.compareDocumentPosition(actor2MemRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear before actor2Row

    // Sort by uptime
    fireEvent.change(input, {
      target: {
        value: "fake_uptime_attr",
      },
    });
    const actor1UptimeRow = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2UptimeRow = getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1UptimeRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2UptimeRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2UptimeRow.compareDocumentPosition(actor1UptimeRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row

    // Sort by GPU utilization
    fireEvent.change(input, {
      target: {
        value: "fake_gpu_attr",
      },
    });
    const actor1GPURow = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2GPURow = getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1GPURow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2GPURow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor1GPURow.compareDocumentPosition(actor2GPURow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor1Row appear before actor2Row

    // Sort by GRAM usage
    fireEvent.change(input, {
      target: {
        value: "fake_gram_attr",
      },
    });
    const actor1GRAMRow = getByRole("row", {
      name: /ACTOR_1/,
    });
    const actor2GRAMRow = getByRole("row", {
      name: /ACTOR_2/,
    });
    expect(within(actor1GRAMRow).getByText("ACTOR_1")).toBeInTheDocument();
    expect(within(actor2GRAMRow).getByText("ACTOR_2")).toBeInTheDocument();
    expect(actor2GRAMRow.compareDocumentPosition(actor1GRAMRow)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row
  });
});
