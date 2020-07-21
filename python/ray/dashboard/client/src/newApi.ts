import { get } from "./common/requestUtils";

type HostnamesResponse = APIResponse<HostnamesResponseData>;
type NodeSummaryResponse = APIResponse<NodeSummaryResponseData>;
type NodeDetailsResponse = APIResponse<NodeDetailsResponseData>;

export type GPUProcessStats = {
  // Sub stat of GPU stats, this type represents the GPU
  // utilization of a single process of a single GPU.
  username: string;
  command: string;
  gpu_memory_usage: number;
  pid: number;
};

export type GPUStats = {
  // This represents stats fetched from a node about a single GPU
  uuid: string;
  name: string;
  temperature_gpu: number;
  fan_speed: number;
  utilization_gpu: number;
  power_draw: number;
  enforced_power_limit: number;
  memory_used: number;
  memory_total: number;
  processes: GPUProcessStats[];
};

export const getNodeSummaries = () =>
  get<NodeSummaryResponse>("/api/v2/hosts", { view: "summary" });

export const getHostnames = () =>
  get<HostnamesResponse>("/api/v2/hosts", { view: "hostnamelist" });

export const getNodeDetails = (hostname: string) =>
  get<NodeDetailsResponse>(`/api/v2/hosts/${hostname}`, {});

type NodeSummaryResponseData = {
  summaries: NodeSummary[];
};

type NodeDetailsResponseData = {
  details: NodeDetails;
};

type RayletAddressInformation = {
  rayletId: string;
  ipAddress: string;
  port: number;
  workerId: string;
};
type ActorState = "ALIVE" | string; // todo flesh out once ant provides other values

type NodeSummary = BaseNodeInfo;

type NodeDetails = {
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
      user: number;
      free: number;
      percent: number;
    };
  };
  net: number[];
  logCounts: number;
  errorCounts: number;
  actors: { [actorId: string]: Actor };
  raylet: {
    numWorkers: number;
    pid: number;
  };
};

type Actor = {
  actorId: string;
  parentId: string;
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

type Worker = {
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
  cmdLine: string[];
  cpuTimes: {
    user: number;
    system: number;
    childrenUser: number;
    childrenSystem: number;
    iowait: number;
  };
  coreWorkerStats: CoreWorkerStats[];
};

type CoreWorkerStats = {
  ipAddress: string;
  port: number;
  usedResources: { [resource: string]: number };
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
  data: T;
};
