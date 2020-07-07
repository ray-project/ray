
import { get } from "./common/requestUtils";

type HostnamesResponse = APIResponse<HostnamesResponseData>;
type NodeSummaryResponse = APIResponse<NodeSummaryResponseData>;
type NodeDetailsResponse = APIResponse<NodeDetailsResponseData>;

const getNodeSummaries = () =>
  get<NodeSummaryResponse>("/v2/hosts", { view: "summary" });
  
const getHostnames = () =>
  get<HostnamesResponse>("/v2/hosts", { view: "hostnamelist" });

const getNodeDetails = (hostname: string) =>
  get<NodeDetailsResponse>(`/v2/hosts/${hostname}`, {});

type NodeSummaryResponseData = {
  summaries: NodeSummary[];
};

type NodeDetailsResponseData = {
  details: NodeDetails
};

type RayletAddressInformation = {
  rayletId: string,
  ipAddress: string,
  port: number,
  workerId: string,
};
type ActorState = "ALIVE" | string; // todo flesh out once ant provides other values

type NodeSummary = BaseNodeInfo;

type NodeDetails = {
  workers: Worker[];
} & BaseNodeInfo;

type BaseNodeInfo = {
  now: number,
  hostname: string,
  ip: string,
  cpu: number,
  cpus: number[],
  mem: number[],
  bootTime: number,
  loadAvg: number[][], // todo figure out what this is
  disk: {
    [dir: string]: {
      total: number,
      user: number,
      free: number,
      percent: number,
    }
  },
  net: number[],
  logCounts: number,
  errorCounts: number,
  actors: { [actorId: string]: Actor },
  raylet: {
    numWorkers: number,
    pid: number,
  }
}

type Actor = {
  actorId: string,
  parentId: string,
  actorCreationDummyObjectId: string,
  jobId: string,
  address: RayletAddressInformation,
  ownerAddress: RayletAddressInformation,
  timestamp: number,
  workerId: string,
  pid: number,
  functionDescriptor: string,
  state: ActorState,
  maxRestarts: number,
  remainingRestarts: number,
  isDetached: boolean,
};

type Worker = {
  pid: number,
  createTime: number,
  memoryInfo: {
    rss: number,
    vms: number,
    shared: number,
    text: number,
    lib: number,
    data: number,
    dirty: Number,
  },
  cmdLine: string[],
  cpuTimes: {
    user: number,
    system: number,
    childrenUser: number,
    childrenSystem: number,
    iowait: number,
  },
  coreWorkerStats: CoreWorkerStats[],
}

type CoreWorkerStats = {
  ipAddress: string,
  port: number,
  usedResources: { [resource: string]: number },
  numExecutedTasks: number,
  workerId: string,
  // We need the below but Ant's API does not yet support it.
}

type HostnamesResponseData = {
  hostnames: string[];
};


type APIResponse<T> = {
  result: boolean,
  msg: string,
  data: T,
}