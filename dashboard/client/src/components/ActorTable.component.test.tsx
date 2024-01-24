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

  it("renders a table of actors sorted by startTime asc when states are the same", () => {
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

    expect(actor1Row.compareDocumentPosition(actor2Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear after actor1Row
  });

  it("renders a table of actors sorted by startTime asc when states are the same, actor2 appears first", () => {
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

    expect(actor2Row.compareDocumentPosition(actor1Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear before actor1Row
  });
});
