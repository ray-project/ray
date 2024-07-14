export type SubmitListRsp = SubmitInfo[];

export enum SubmitStatus {
  PENDING = "PENDING",
  RUNNING = "RUNNING",
  STOPPED = "STOPPED",
  SUCCEEDED = "SUCCEEDED",
  FAILED = "FAILED",
}

export type SubmitInfo = {
  submission_id: string;
  type: string;
  status: SubmitStatus;
  entrypoint: string;
  entrypoint_num_cpus: number;
  entrypoint_num_gpus: number;
  entrypoint_memory: number;
  entrypoint_resources: { [key: string]: string } | null;
  message: string | null;
  error_type: string | null;
  start_time: number | null;
  end_time: number | null;
  metadata: { [key: string]: string } | null;
  runtime_env: { [key: string]: string } | null;
  driver_info: DriverInfo | null;
  driver_agent_http_address: string | null;
  driver_node_id: string | null;
};

export type DriverInfo = {
  id: string;
  node_ip_address: string;
  node_id: string;
  pid: string;
};
