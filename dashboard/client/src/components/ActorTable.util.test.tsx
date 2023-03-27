import { Actor, ActorEnum } from "../type/actor";
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import ActorTable, { sortActors } from "./ActorTable";

// check the state and startTime as the sort criteria
describe("sortActors", () => {
  const actor1: Actor = {
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
    reprName: "repr1",
  };
  const actor2: Actor = {
    actorId: "2a77333eac321119fae2f60601000000",
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
    startTime: 1679010689150,
    endTime: 0,
    actorClass: "Counter",
    exitDetail: "-",
    requiredResources: {},
    placementGroupId: "123",
    reprName: "repr2",
  };
  const actorList: Actor[] = [actor1, actor2];

  it.each([
    [
      "sorts actors by state when states are different",
      "DEAD",
      "ALIVE",
      [actor2, actor1],
    ],
    [
      "sorts actors by startTime when states are equal and start times are different",
      "ALIVE",
      "ALIVE",
      [actor1, actor2],
    ],
  ])("%s", (_, state1, state2, expectedOrder) => {
    actor1.state = state1 as ActorEnum;
    actor2.state = state2 as ActorEnum;
    expect(sortActors(actorList)).toEqual(expectedOrder);
  });
});
