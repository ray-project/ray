export enum ActorEnum {
  ALIVE = "ALIVE",
  PENDING = "PENDING",
  RECONSTRUCTING = "RECONSTRUCTING",
  DEAD = "DEAD",
}

export type Address = {
  rayletId: string;
  ipAddress: string;
  port: number;
  workerId: string;
};

export type Actor = {
  actorId: string;
  jobId: string;
  state: ActorEnum | string; // PENDING, ALIVE, RECONSTRUCTING, DEAD
  pid: number | null;
  address: Address;
  name: string;
  numRestarts: string;
  actorClass: string;
  startTime: number | null;
  endTime: number | null;
  requiredResources: {
    [key: string]: number;
  };
  exitDetail: string;
};
