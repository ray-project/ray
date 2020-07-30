import { getv2 } from "./common/requestUtils";

export type HostnamesResponse = APIResponse<HostnamesResponseData>;
export type NodeSummaryResponse = APIResponse<NodeSummaryResponseData>;
export type NodeDetailsResponse = APIResponse<NodeDetailsResponseData>;

export type ResourceSlot = {
  slot: number;
  allocation: number;
};

export type ResourceAllocations = {
  resourceSlots: ResourceSlot[];
};

export type GPUProcessStats = {
  // Sub stat of GPU stats, this type represents the GPU
  // utilization of a single process of a single GPU.
  username: string;
  command: string;
  gpuMemoryUsage: number;
  pid: number;
};

export type GPUStats = {
  // This represents stats fetched from a node about a single GPU
  uuid: string;
  name: string;
  temperatureGpu: number;
  fanSpeed: number;
  utilizationGpu: number;
  powerDraw: number;
  enforcedPowerLimit: number;
  memoryUsed: number;
  memoryTotal: number;
  processes: GPUProcessStats[];
};

export const getNodeSummaries = () =>
  getv2<NodeSummaryResponseData>("/api/v2/nodes", {});

export const getHostnames = () =>
  getv2<HostnamesResponseData>("/api/v2/hostnames", {});

export const getNodeDetails = (hostname: string) =>
  getv2<NodeDetailsResponseData>(`/api/v2/nodes/${hostname}`, {});

export const getAllNodeDetails = (hostnames: string[]) => {
  return Promise.all(hostnames.map((hostname) => getNodeDetails(hostname)));
};

export type NodeSummaryResponseData = {
  summaries: NodeSummary[];
};

export type NodeDetailsResponseData = {
  details: NodeDetails;
};

type RayletAddressInformation = {
  rayletId: string;
  ipAddress: string;
  port: number;
  workerId: string;
};

export enum ActorState {
  Pending = "PENDING",
  Alive = "ALIVE",
  Dead = "DEAD",
  Creating = "CREATING",
  Restarting = "RESTARTING",
  Invalid = "INVALID",
}

export type NodeSummary = BaseNodeInfo;

export type NodeDetails = {
  workers: Worker[];
} & BaseNodeInfo;

type BaseNodeInfo = {
  now: number;
  hostname: string;
  ip: string;
  cpu: number;
  cpus: number[];
  gpus: GPUStats[]; // GPU stats fetched from node, 1 entry per GPU
  mem: number[];
  bootTime: number;
  loadAvg: number[][]; // todo figure out what this is
  disk: {
    [dir: string]: {
      total: number;
      used: number;
      free: number;
      percent: number;
    };
  };
  net: number[];
  logCount: number;
  errorCount: number;
  actors: { [actorId: string]: Actor };
  raylet: {
    numWorkers: number;
    pid: number;
  };
};

export type Actor = {
  actorId: string;
  parentId: string;
  actorTitle: string;
  actorCreationDummyObjectId: string;
  jobId: string;
  address: RayletAddressInformation;
  ownerAddress: RayletAddressInformation;
  timestamp: number;
  workerId: string;
  pid: number;
  functionDescriptor: string;
  state: ActorState;
  maxRestarts: number;
  remainingRestarts: number;
  isDetached: boolean;
};

export type Worker = {
  pid: number;
  createTime: number;
  memoryInfo: {
    rss: number;
    vms: number;
    shared: number;
    text: number;
    lib: number;
    data: number;
    dirty: Number;
  };
  cmdline: string[];
  cpuTimes: {
    user: number;
    system: number;
    childrenUser: number;
    childrenSystem: number;
    iowait: number;
  };
  cpuPercent: number;
  logCount: number;
  errorCount: number;
  coreWorkerStats: CoreWorkerStats[];
};

export type CoreWorkerStats = {
  ipAddress: string;
  port: number;
  usedResources: { [key: string]: ResourceAllocations };
  numExecutedTasks: number;
  workerId: string;
  // We need the below but Ant's API does not yet support it.
};

type HostnamesResponseData = {
  hostnames: string[];
};

type APIResponse<T> = {
  result: boolean;
  msg: string;
  data?: T;
};
