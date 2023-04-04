import { render } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { Actor } from "../type/actor";
import ActorTable from "./ActorTable";
describe("ActorTable", () => {
  const actors: { [actorId: string]: Actor } = {
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
    },
  };
  it("renders a table of actors sorted by state", () => {
    const actors1 = JSON.parse(JSON.stringify(actors));
    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={actors1} />
      </MemoryRouter>,
    );
    // Check that the table is rendered
    const table = getByRole("table");

    // Check that the actors are sorted by state and startTime
    const rows = table.querySelectorAll("tbody tr");
    const actor1Row = rows[0];
    const actor2Row = rows[1];
    expect(actor1Row.textContent).toContain("ACTOR_1");
    expect(actor2Row.textContent).toContain("ACTOR_2");
    expect(actor1Row.compareDocumentPosition(actor2Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear after actor1Row
  });

  it("renders a table of actors sorted by startTime increasingly when states are the same", () => {
    const actors2 = JSON.parse(JSON.stringify(actors));
    actors2["ACTOR_2"].state = "ALIVE";

    const { getByRole } = render(
      <MemoryRouter>
        <ActorTable actors={actors2} />
      </MemoryRouter>,
    );

    // Check that the table is rendered
    const table = getByRole("table");
    // Check that the actors are sorted by state and startTime
    const rows = table.querySelectorAll("tbody tr");
    const actor1Row = rows[0];
    const actor2Row = rows[1];
    expect(actor1Row.textContent).toContain("ACTOR_1");
    expect(actor2Row.textContent).toContain("ACTOR_2");
    expect(actor1Row.compareDocumentPosition(actor2Row)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    ); // actor2Row appear after actor1Row
  });
});
