const base =
  process.env.NODE_ENV === "development"
    ? "http://localhost:8080"
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

export interface RayConfigResponse {
  min_workers: number;
  max_workers: number;
  initial_workers: number;
  autoscaling_mode: string;
  idle_timeout_minutes: number;
  head_type: string;
  worker_type: string;
}

export const getRayConfig = () => get<RayConfigResponse>("/api/ray_config", {});

export interface NodeInfoResponse {
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
      memory_full_info: null; // Currently unused as it requires superuser permission on some systems
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
}

export const getNodeInfo = () => get<NodeInfoResponse>("/api/node_info", {});

export interface ErrorsResponse {
  [pid: string]: Array<{
    message: string;
    timestamp: number;
    type: string;
  }>;
}

export const getErrors = (hostname: string, pid: string | undefined) =>
  get<ErrorsResponse>("/api/errors", { hostname, pid: pid || "" });

export interface LogsResponse {
  [pid: string]: string[];
}

export const getLogs = (hostname: string, pid: string | undefined) =>
  get<LogsResponse>("/api/logs", { hostname, pid: pid || "" });
