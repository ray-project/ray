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

export type TaskSpec = {
  actorCreationTaskSpec: {
    actorId: string;
    dynamicWorkerOptions: string[];
    extensionData: string;
    isAsyncio: boolean;
    isDetached: boolean;
    maxActorRestarts: boolean;
    maxConcurrency: number;
    name: string;
  };
  args: {
    data: string;
    metadata: string;
    nestedInlinedIds: string[];
    objectIds: string[];
  }[];
  callerAddress: {
    ipAddress: string;
    port: number;
    rayletId: string;
    workerId: string;
  };
  callerId: string;
  functionDescriptor: {
    javaFunctionDescriptor: {
      className: string;
      functionName: string;
      signature: string;
    };
    pythonFunctionDescriptor: {
      className: string;
      functionName: string;
      signature: string;
    };
  };
  jobId: string;
  language: string;
  maxRetries: number;
  numReturns: string;
  parentCounter: string;
  parentTaskId: string;
  requiredPlacementResources: {
    [key: string]: number;
  };
  requiredResources: {
    [key: string]: number;
  };
  sourceActorId: string;
  taskId: string;
  type: string;
};

export type Actor = {
  actorId: string;
  jobId: string;
  state: ActorEnum | string; // PENDING, ALIVE, RECONSTRUCTING, DEAD
  nodeId: string;
  pid: number;
  address: Address;
  name: string;
  numRestarts: string;
  taskSpec: TaskSpec;
};
