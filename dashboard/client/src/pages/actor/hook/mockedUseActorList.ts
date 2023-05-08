import { Actor, ActorEnum } from "../../../type/actor";

const mockedActors: { [actorId: string]: Actor } = {
  "1": {
    actorId: "1",
    state: ActorEnum.ALIVE,
    ipAddress: "127.0.0.1",
    port: 1234,
    pid: 5678,
    createTime: new Date(),
    updateTime: new Date(),
  },
  "2": {
    actorId: "2",
    state: ActorEnum.DEAD,
    ipAddress: "127.0.0.1",
    port: 1235,
    pid: 5679,
    createTime: new Date(),
    updateTime: new Date(),
  },
  "3": {
    actorId: "3",
    state: ActorEnum.DEPENDENCIES_UNREADY,
    ipAddress: "127.0.0.1",
    port: 1236,
    pid: 5680,
    createTime: new Date(),
    updateTime: new Date(),
  },
  "4": {
    actorId: "4",
    state: ActorEnum.PENDING_CREATION,
    ipAddress: "127.0.0.1",
    port: 1237,
    pid: 5681,
    createTime: new Date(),
    updateTime: new Date(),
  },
  "5": {
    actorId: "5",
    state: ActorEnum.RESTARTING,
    ipAddress: "127.0.0.1",
    port: 1238,
    pid: 5682,
    createTime: new Date(),
    updateTime: new Date(),
  } as any,
};

export const useActorList = (): { [actorId: string]: Actor } => {
  return mockedActors;
};
