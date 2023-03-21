import { render } from "@testing-library/react";
import React from "react";
import { Actor } from "../type/actor";
import ActorTable from "./ActorTable";
describe("ActorTable", () => {
  const ACTOR_1 = "1a77333eac321119fae2f60601000000";
  const ACTOR_2 = "2a77333eac321119fae2f60601000000";
  const ACTOR_3 = "3a77333eac321119fae2f60601000000";
  const actors: { [actorId: string]: Actor } = {
    //actor1
    "1a77333eac321119fae2f60601000000": {
      actorId: "1a77333eac321119fae2f60601000000",
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
    //actor2
    "2a77333eac321119fae2f60601000000": {
      actorId: "2a77333eac321119fae2f60601000000",
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
    //actor3
    "3a77333eac321119fae2f60601000000": {
      actorId: "3a77333eac321119fae2f60601000000",
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
      pid: 25322,
      startTime: 1679010689151,
      endTime: 0,
      actorClass: "Counter",
      exitDetail: "-",
      requiredResources: {},
      placementGroupId: "123",
    },
  };
  it("renders a table of actors sorted by state and startTime", () => {
    const { getByRole } = render(<ActorTable actors={actors} />);

    // Check that the table is rendered
    const table = getByRole("table");

    // Check that the actors are sorted by state and startTime
    const rows = table.querySelectorAll("tbody tr");
    const actor1Row = rows[0];
    const actor2Row = rows[1];
    expect(actor1Row).toHaveTextContent("1a77333eac321119fae2f60601000000"); // actor1
    expect(actor2Row).toHaveTextContent("2a77333eac321119fae2f60601000000"); // actor2
    expect(actor2Row.compareDocumentPosition(actor1Row)).toBe(2); // 2 = Node.DOCUMENT_POSITION_FOLLOWING
  });
  it("renders a table of actors sorted by state and startTime", () => {
    const { getByRole } = render(<ActorTable actors={actors} />);

    // Check that the table is rendered
    const table = getByRole("table");

    // Check that the actors are sorted by state and startTime
    const rows = table.querySelectorAll("tbody tr");
    const actor1Row = rows[0];
    const actor2Row = rows[1];
    expect(actor1Row).toHaveTextContent("1a77333eac321119fae2f60601000000"); // actor1
    expect(actor2Row).toHaveTextContent("2a77333eac321119fae2f60601000000"); // actor2
    expect(actor2Row.compareDocumentPosition(actor1Row)).toBe(2); // 2 = Node.DOCUMENT_POSITION_FOLLOWING
  });
});
