const base =
  process.env.NODE_ENV === "development"
    ? "http://localhost:8265"
    : window.location.origin;

type APIResponse<T> = {
  result: boolean;
  msg: string;
  data?: T;
};
// TODO(mitchellstern): Add JSON schema validation for the responses.
const get = async <T>(path: string, params: { [key: string]: any }) => {
  const url = new URL(path, base);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  const json: APIResponse<T> = await response.json();

  const { result, msg, data } = json;

  if (!result) {
    throw Error(msg);
  }

  return data as T;
};

const post = async <T>(path: string, params: { [key: string]: any }) => {
  const requestOptions = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  };

  const url = new URL(path, base);

  const response = await fetch(url.toString(), requestOptions);
  const json: APIResponse<T> = await response.json();

  const { result, msg, data } = json;

  if (!result) {
    throw Error(msg);
  }

  return data as T;
};

export type RayConfigResponse = {
  minWorkers: number;
  maxWorkers: number;
  initialWorkers: number;
  autoscalingMode: string;
  idleTimeoutMinutes: number;
  headType: string;
  workerType: string;
};

export const getRayConfig = () => get<RayConfigResponse>("/api/ray_config", {});

export type Worker = {
  pid: number;
  workerId: string;
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
  language: string;
  jobId: string;
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

export type NodeSummary = BaseNodeInfo;

export type NodeDetails = {
  workers: Worker[];
  raylet: RayletData;
} & BaseNodeInfo;

export type RayletData = {
  // Merger of GCSNodeStats and GetNodeStatsReply
  // GetNodeStatsReply fields.
  // Note workers are in an array in NodeDetails
  viewData: { [viewName: string]: ViewData };
  numWorkers: number;

  // GCSNodeStats fields
  nodeId: number;
  nodeManagerAddress: string;
  rayletSocketName: string;
  objectStoreSocketName: string;
  nodeManagerPort: number;
  objectManagerPort: number;
  state: "ALIVE" | "DEAD";
  nodeManagerHostname: string;
  metricsExportPort: number;
};

export type ViewData = {
  viewName: string;
  measures: Measure[];
};

export type Measure = {
  tags: string; // e.g.  "Tag1:Value1,Tag2:Value2,Tag3:Value3"
  intValue?: number;
  doubleValue?: number;
  distributionMin?: number;
  distributionMean?: number;
  distributionMax?: number;
  distributionCount?: number;
  distributionBucketBoundaries?: number[];
  distributionBucketCounts?: number[]
};

type BaseNodeInfo = {
  now: number;
  hostname: string;
  ip: string;
  bootTime: number; // System boot time expressed in seconds since epoch
  cpu: number; // System-wide CPU utilization expressed as a percentage
  cpus: [number, number]; // Number of logical CPUs and physical CPUs
  gpus: Array<GPUStats>; // GPU stats fetched from node, 1 entry per GPU
  mem: [number, number, number]; // Total, available, and used percentage of memory
  disk: {
    [dir: string]: {
      total: number;
      free: number;
      used: number;
      percent: number;
    };
  };
  loadAvg: [[number, number, number], [number, number, number]];
  net: [number, number]; // Sent and received network traffic in bytes / second
  logCount: number;
  errorCount: number;
};

export type NodeInfoResponse = {
  clients: NodeDetails[];
};

export const getNodeInfo = () => get<NodeInfoResponse>("/all_node_details", {});

export type ResourceSlot = {
  slot: number;
  allocation: number;
};

export type ResourceAllocations = {
  resourceSlots: ResourceSlot[];
};

export enum ActorState {
  // These two are virtual states that we air because there is
  // an existing task to create an actor
  Infeasible = "INFEASIBLE", // Actor task is waiting on resources (e.g. RAM, CPUs or GPUs) that the cluster does not have
  PendingResources = "PENDING_RESOURCES", // Actor task is waiting on resources the cluster has but are in-use
  // The rest below are "official" GCS actor states
  DependenciesUnready = "PENDING", // Actor is pending on an argument to be ready
  PendingCreation = "CREATING", // Actor creation is running
  Alive = "ALIVE", // Actor is alive and handling tasks
  Restarting = "RESTARTING", // Actor died and is being restarted
  Dead = "DEAD", // Actor died and is not being restarted
}

export type ActorInfo = FullActorInfo | ActorTaskInfo;

export type FullActorInfo = {
  actorId: string;
  actorTitle: string;
  averageTaskExecutionSpeed: number;
  children?: ActorInfo[];
  ipAddress: string;
  jobId: string;
  nodeId: string;
  numExecutedTasks: number;
  numLocalObjects: number;
  numObjectRefsInScope: number;
  pid: number;
  port: number;
  state:
    | ActorState.Alive
    | ActorState.Restarting
    | ActorState.Dead
    | ActorState.DependenciesUnready
    | ActorState.PendingCreation;
  taskQueueLength: number;
  timestamp: number;
  usedObjectStoreMemory: number;
  usedResources: { [key: string]: ResourceAllocations };
  currentTaskDesc?: string;
  numPendingTasks?: number;
  webuiDisplay?: Record<string, string>;
};

export type ActorTaskInfo = {
  actorId?: string;
  actorTitle?: string;
  requiredResources?: { [key: string]: number };
  state: ActorState.Infeasible | ActorState.PendingResources;
};

// eslint-disable-next-line
export function isFullActorInfo(
  actorInfo: ActorInfo,
): actorInfo is FullActorInfo {
  // Lint disabled because arrow functions don't play well with type guards.
  // This function is used to determine what kind of information we have about
  // a given actor in a response based on its state.
  return (
    actorInfo.state !== ActorState.Infeasible &&
    actorInfo.state !== ActorState.PendingResources
  );
}

export type ActorGroupSummary = {
  stateToCount: { [state in ActorState]: number };
  avgLifetime: number;
  maxLifetime: number;
  numExecutedTasks: number;
};

export type ActorGroup = {
  entries: ActorInfo[];
  summary: ActorGroupSummary;
};

export type ActorsResponse = {
  groups: { [key: string]: ActorGroup };
};

export type ErrorsResponse = {
  [pid: string]: Array<{
    message: string;
    timestamp: number;
    type: string;
  }>;
};

export const getErrors = (hostname: string, pid: number | null) =>
  get<ErrorsResponse>("/api/errors", {
    hostname,
    pid: pid === null ? "" : pid,
  });

export type LogsResponse = {
  [pid: string]: string[];
};

export const getLogs = (hostname: string, pid: number | null) =>
  get<LogsResponse>("/api/logs", {
    hostname,
    pid: pid === null ? "" : pid,
  });

export type LaunchProfilingResponse = string;

export const launchProfiling = (
  nodeId: string,
  pid: number,
  duration: number,
) =>
  get<LaunchProfilingResponse>("/api/launch_profiling", {
    node_id: nodeId,
    pid: pid,
    duration: duration,
  });

export type CheckProfilingStatusResponse =
  | { status: "pending" }
  | { status: "finished" }
  | { status: "error"; error: string };

export const checkProfilingStatus = (profilingId: string) =>
  get<CheckProfilingStatusResponse>("/api/check_profiling_status", {
    profiling_id: profilingId,
  });

export const getProfilingResultURL = (profilingId: string) =>
  `${base}/speedscope/index.html#profileURL=${encodeURIComponent(
    `${base}/api/get_profiling_info?profiling_id=${profilingId}`,
  )}`;

export const launchKillActor = (
  actorId: string,
  actorIpAddress: string,
  actorPort: number,
) =>
  get<object>("/api/kill_actor", {
    // make sure object is okay
    actor_id: actorId,
    ip_address: actorIpAddress,
    port: actorPort,
  });

export type TuneTrial = {
  date: string;
  episodesTotal: string;
  experimentId: string;
  experimentTag: string;
  hostname: string;
  iterationsSinceRestore: number;
  logdir: string;
  nodeIp: string;
  pid: number;
  timeSinceRestore: number;
  timeThisIterS: number;
  timeTotalS: number;
  timestamp: number;
  timestepsSinceRestore: number;
  timestepsTotal: number;
  trainingIteration: number;
  startTime: string;
  status: string;
  trialId: string | number;
  jobId: string;
  params: { [key: string]: string | number };
  metrics: { [key: string]: string | number };
  error: string;
};

export type TuneError = {
  text: string;
  jobId: string;
  trialId: string;
};

export type TuneJobResponse = {
  trialRecords: { [key: string]: TuneTrial };
  errors: { [key: string]: TuneError };
  tensorboard: {
    tensorboardCurrent: boolean;
    tensorboardEnabled: boolean;
  };
};

export const getTuneInfo = () => get<TuneJobResponse>("/tune/info", {});

export type TuneAvailabilityResponse = {
  available: boolean;
  trialsAvailable: boolean;
};

export const getTuneAvailability = () =>
  get<TuneAvailabilityResponse>("/tune/availability", {});

export type TuneSetExperimentReponse = {
  experiment: string;
};

export const setTuneExperiment = (experiment: string) =>
  post<TuneSetExperimentReponse>("/tune/set_experiment", {
    experiment: experiment,
  });

export const enableTuneTensorBoard = () =>
  post<{}>("/tune/enable_tensorboard", {});

export type MemoryTableSummary = {
  totalActorHandles: number;
  totalCapturedInObjects: number;
  totalLocalRefCount: number;
  // The measurement is B.
  totalObjectSize: number;
  totalPinnedInMemory: number;
  totalUsedByPendingTask: number;
};

export type MemoryTableEntry = {
  nodeIpAddress: string;
  pid: number;
  type: string;
  objectRef: string;
  objectSize: number;
  referenceType: string;
  callSite: string;
};

export type MemoryTableGroups = {
  [groupKey: string]: MemoryTableGroup;
};

export type MemoryTableGroup = {
  entries: MemoryTableEntry[];
  summary: MemoryTableSummary;
};

export type MemoryTableResponse = {
  group: MemoryTableGroups;
  summary: MemoryTableSummary;
};

// This doesn't return anything.
export type StopMemoryTableResponse = {};

export type MemoryGroupByKey = "node" | "stack_trace" | "";

export const getMemoryTable = async (groupByKey: MemoryGroupByKey) => {
  return get<MemoryTableResponse>("/memory/memory_table", {
    groupBy: groupByKey,
  });
};

export const setMemoryTableCollection = (value: boolean) =>
  post<{}>("/memory/set_fetch", {"shouldFetch": value});
