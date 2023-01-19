export enum TypeTaskStatus {
  // Must be in sync with custom_types.py::TaskStatus
  NIL = "NIL",
  PENDING_ARGS_AVAIL = "PENDING_ARGS_AVAIL",
  PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT",
  PENDING_OBJ_STORE_MEM_AVAIL = "PENDING_OBJ_STORE_MEM_AVAIL",
  PENDING_ARGS_FETCH = "PENDING_ARGS_FETCH",
  SUBMITTED_TO_WORKER = "SUBMITTED_TO_WORKER",
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
  state: TypeTaskStatus;
  job_id: string;
  node_id: string;
  actor_id: string;
  type: TypeTaskType;
  func_or_class_name: string;
  language: string;
  required_resources: { [key: string]: number };
  runtime_env_info: string;
  events: { [key: string]: string }[];
  start_time_ms: int | null;
  end_time_ms: int | null;
};
