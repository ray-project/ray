const base =
  process.env.NODE_ENV === "development"
    ? "http://localhost:8265"
    : window.location.origin;

// TODO(mitchellstern): Add JSON schema validation for the responses.
const get = async <T>(path: string, params: { [key: string]: any }) => {
  const url = new URL(path, base);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url.toString());
  const json = await response.json();

  const { result, error } = json;

  if (error !== null) {
    throw Error(error);
  }

  return result as T;
};

const post = async <T>(path: string, params: { [key: string]: any }) => {
  const requestOptions = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  };

  const url = new URL(path, base);

  const response = await fetch(url.toString(), requestOptions);
  const json = await response.json();

  const { result, error } = json;

  if (error !== null) {
    throw Error(error);
  }

  return result as T;
};

export type RayConfigResponse = {
  min_workers: number;
  max_workers: number;
  initial_workers: number;
  autoscaling_mode: string;
  idle_timeout_minutes: number;
  head_type: string;
  worker_type: string;
};

export const getRayConfig = () => get<RayConfigResponse>("/api/ray_config", {});

export type NodeInfoResponse = {
  clients: Array<{
    now: number;
    hostname: string;
    ip: string;
    boot_time: number; // System boot time expressed in seconds since epoch
    cpu: number; // System-wide CPU utilization expressed as a percentage
    cpus: [number, number]; // Number of logical CPUs and physical CPUs
    mem: [number, number, number]; // Total, available, and used percentage of memory
    disk: {
      [path: string]: {
        total: number;
        free: number;
        used: number;
        percent: number;
      };
    };
    load_avg: [[number, number, number], [number, number, number]];
    net: [number, number]; // Sent and received network traffic in bytes / second
    workers: Array<{
      pid: number;
      create_time: number;
      cmdline: string[];
      cpu_percent: number;
      cpu_times: {
        system: number;
        children_system: number;
        user: number;
        children_user: number;
      };
      memory_info: {
        pageins: number;
        pfaults: number;
        vms: number;
        rss: number;
      };
    }>;
  }>;
  log_counts: {
    [ip: string]: {
      [pid: string]: number;
    };
  };
  error_counts: {
    [ip: string]: {
      [pid: string]: number;
    };
  };
};

export const getNodeInfo = () => get<NodeInfoResponse>("/api/node_info", {});

export type RayletInfoResponse = {
  nodes: {
    [ip: string]: {
      extraInfo?: string;
      workersStats: {
        pid: number;
        isDriver?: boolean;
      }[];
    };
  };
  actors: {
    [actorId: string]:
      | {
          actorId: string;
          actorTitle: string;
          averageTaskExecutionSpeed: number;
          children: RayletInfoResponse["actors"];
          // currentTaskFuncDesc: string[];
          ipAddress: string;
          isDirectCall: boolean;
          jobId: string;
          nodeId: string;
          numExecutedTasks: number;
          numLocalObjects: number;
          numObjectIdsInScope: number;
          pid: number;
          port: number;
          state: 0 | 1 | 2;
          taskQueueLength: number;
          timestamp: number;
          usedObjectStoreMemory: number;
          usedResources: { [key: string]: number };
          currentTaskDesc?: string;
          numPendingTasks?: number;
          webuiDisplay?: Record<string, string>;
        }
      | {
          actorId: string;
          actorTitle: string;
          requiredResources: { [key: string]: number };
          state: -1;
          invalidStateType?: "infeasibleActor" | "pendingActor";
        };
  };
};

export const getRayletInfo = () =>
  get<RayletInfoResponse>("/api/raylet_info", {});

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
  episodes_total: string;
  experiment_id: string;
  experiment_tag: string;
  hostname: string;
  iterations_since_restore: number;
  logdir: string;
  node_ip: string;
  pid: number;
  time_since_restore: number;
  time_this_iter_s: number;
  time_total_s: number;
  timestamp: number;
  timesteps_since_restore: number;
  timesteps_total: number;
  training_iteration: number;
  start_time: string;
  status: string;
  trial_id: string | number;
  job_id: string;
  params: { [key: string]: string | number };
  metrics: { [key: string]: string | number };
  error: string;
};

export type TuneError = {
  text: string;
  job_id: string;
  trial_id: string;
};

export type TuneJobResponse = {
  trial_records: { [key: string]: TuneTrial };
  errors: { [key: string]: TuneError };
  tensorboard: {
    tensorboard_current: boolean;
    tensorboard_enabled: boolean;
  };
};

export const getTuneInfo = () => get<TuneJobResponse>("/api/tune_info", {});

export type TuneAvailabilityResponse = {
  available: boolean;
  trials_available: boolean;
};

export const getTuneAvailability = () =>
  get<TuneAvailabilityResponse>("/api/tune_availability", {});

export type TuneSetExperimentReponse = {
  experiment: string;
};

export const setTuneExperiment = (experiment: string) =>
  post<TuneSetExperimentReponse>("/api/set_tune_experiment", {
    experiment: experiment,
  });

export const enableTuneTensorBoard = () =>
  post<{}>("/api/enable_tune_tensorboard", {});
