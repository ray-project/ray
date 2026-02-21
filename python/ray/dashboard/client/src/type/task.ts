export enum TypeTaskStatus {
  // Must be in sync with custom_types.py::TaskStatus
  NIL = "NIL",
  PENDING_ARGS_AVAIL = "PENDING_ARGS_AVAIL",
  PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT",
  PENDING_OBJ_STORE_MEM_AVAIL = "PENDING_OBJ_STORE_MEM_AVAIL",
  PENDING_ARGS_FETCH = "PENDING_ARGS_FETCH",
  SUBMITTED_TO_WORKER = "SUBMITTED_TO_WORKER",
  PENDING_ACTOR_TASK_ARGS_FETCH = "PENDING_ACTOR_TASK_ARGS_FETCH",
  PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY = "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY",
  RUNNING = "RUNNING",
  RUNNING_IN_RAY_GET = "RUNNING_IN_RAY_GET",
  RUNNING_IN_RAY_WAIT = "RUNNING_IN_RAY_WAIT",
  FINISHED = "FINISHED",
  FAILED = "FAILED",
}

export enum TypeTaskType {
  // Must be in sync with custom_types.py::TaskType
  NORMAL_TASK = "NORMAL_TASK",
  ACTOR_CREATION_TASK = "ACTOR_CREATION_TASK",
  ACTOR_TASK = "ACTOR_TASK",
  DRIVER_TASK = "DRIVER_TASK",
}

export type Task = {
  task_id: string;
  name: string;
  attempt_number: number;
  state: TypeTaskStatus;
  job_id: string;
  node_id: string;
  actor_id: string | null;
  placement_group_id: string | null;
  type: TypeTaskType;
  func_or_class_name: string;
  language: string;
  required_resources: { [key: string]: number };
  runtime_env_info: string;
  events: { [key: string]: string }[];
  start_time_ms: number | null;
  end_time_ms: number | null;
  worker_id: string | null;
  profiling_data: ProfilingData;
  error_type: string | null;
  error_message: string | null;
  task_log_info: { [key: string]: string | null | number };
  call_site: string | null;
  label_selector: { [key: string]: string } | null;
};

export type ProfilingData = {
  node_ip_address?: string;
  events: {
    event_name: string;
    extra_data?: {
      type?: string;
      traceback?: string;
    };
  }[];
};
