export enum ServeApplicationStatus {
  // Keep in sync with ApplicationStatus in python/ray/serve/_private/common.py
  NOT_STARTED = "NOT_STARTED",
  DEPLOYING = "DEPLOYING",
  RUNNING = "RUNNING",
  DEPLOY_FAILED = "DEPLOY_FAILED",
  DELETING = "DELETING",
}

export type ServeApplication = {
  name: string;
  route_prefix: string;
  docs_path: string | null;
  status: ServeApplicationStatus;
  message: string;
  last_deployed_time_s: number;
  deployed_app_config: Record<string, any>;
  deployments: {
    [name: string]: ServeDeployment;
  };
};

export enum ServeDeploymentStatus {
  // Keep in sync with DeploymentStatus in python/ray/serve/_private/common.py
  UPDATING = "UPDATING",
  HEALTHY = "HEALTHY",
  UNHEALTHY = "UNHEALTHY",
}

export type ServeDeployment = {
  name: string;
  status: ServeDeploymentStatus;
  message: string;
  deployment_config: ServeDeploymentConfig;
  replicas: ServeReplica[];
};

export type ServeDeploymentConfig = {
  name: string;
  num_replicas: number;
  max_concurrent_queries: number;
  user_config: Record<string, any> | null;
  autoscaling_config: ServeAutoscalingConfig | null;
  graceful_shutdown_wait_loop_s: number;
  graceful_shutdown_timeout_s: number;
  health_check_period_s: number;
  health_check_timeout_s: number;
  ray_actor_options: Record<string, any>;
  is_driver_deployment: boolean;
};

export type ServeAutoscalingConfig = {
  min_replicas: number;
  initial_replicas: number | null;
  max_replicas: number;
  target_num_ongoing_requests_per_replica: number;
};

export enum ServeReplicaState {
  // Keep in sync with ReplicaState in python/ray/serve/_private/common.py
  STARTING = "STARTING",
  UPDATING = "UPDATING",
  RECOVERING = "RECOVERING",
  RUNNING = "RUNNING",
  STOPPING = "STOPPING",
}

export type ServeReplica = {
  replica_id: string;
  state: ServeReplicaState;
  pid: string | null;
  actor_name: string;
  actor_id: string | null;
  node_id: string | null;
  node_ip: string | null;
  start_time_s: number;
};

// Keep in sync with DeploymentMode in python/ray/serve/config.py
export enum ServeDeploymentMode {
  NoServer = "NoServer",
  HeadOnly = "HeadOnly",
  EveryNode = "EveryNode",
  FixedNumber = "FixedNumber",
}

export type ServeApplicationsRsp = {
  http_options: {
    host: string;
    port: number;
  };
  proxy_location: ServeDeploymentMode;
  applications: {
    [name: string]: ServeApplication;
  };
};
