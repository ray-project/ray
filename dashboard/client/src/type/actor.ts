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
  children: { [key: string]: Actor };
  taskSpec: TaskSpec;
  ipAddress: string;
  isDirectCall: boolean;
  jobId: string;
  numExecutedTasks: number;
  numLocalObjects: number;
  numObjectIdsInScope: number;
  state: ActorEnum | string; // PENDING, ALIVE, RECONSTRUCTING, DEAD
  taskQueueLength: number;
  usedObjectStoreMemory: number;
  usedResources: { [key: string]: string | number };
  timestamp: number;
  actorTitle: string;
  averageTaskExecutionSpeed: number;
  nodeId: string;
  pid: number;
  ownerAddress: Address;
  address: Address;
  maxReconstructions: string;
  remainingReconstructions: string;
  isDetached: false;
  name: string;
  numRestarts: string;
};
