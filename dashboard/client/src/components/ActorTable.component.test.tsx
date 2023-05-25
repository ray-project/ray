import { render, within } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { Actor } from "../type/actor";
import ActorTable from "./ActorTable";
const MOCK_ACTORS: { [actorId: string]: Actor } = {
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
  },
};
describe("ActorTable", () => {
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
