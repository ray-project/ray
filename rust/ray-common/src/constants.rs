// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Constants matching `src/ray/common/constants.h`.

/// Length of Ray full-length UniqueIDs in bytes.
pub const UNIQUE_ID_SIZE: usize = 28;

/// Object index bit width.
pub const OBJECT_ID_INDEX_SIZE: usize = 32;

/// Precision of fractional resource quantity.
pub const RESOURCE_UNIT_SCALING: i32 = 10000;

/// Ray version string.
pub const RAY_VERSION: &str = "3.0.0.dev0";

/// Default task event enabled flag.
pub const DEFAULT_TASK_EVENT_ENABLED: bool = true;

/// Streaming generator return sentinel.
pub const STREAMING_GENERATOR_RETURN: i32 = -2;

/// Raylet exit code on plasma store socket error.
pub const RAYLET_STORE_ERROR_EXIT_CODE: i32 = 100;

/// Prefix for object table keys in Redis.
pub const OBJECT_TABLE_PREFIX: &str = "ObjectTable";

/// Key for cluster ID in Redis.
pub const CLUSTER_ID_KEY: &str = "ray_cluster_id";

/// gRPC auth token header key.
pub const AUTH_TOKEN_KEY: &str = "authorization";

/// Bearer token prefix.
pub const BEARER_PREFIX: &str = "Bearer ";

/// Public DNS server used for local IP detection.
pub const PUBLIC_DNS_SERVER_IP: &str = "8.8.8.8";
pub const PUBLIC_DNS_SERVER_PORT: u16 = 53;

/// Environment variable keys.
pub const ENV_VAR_KEY_JOB_ID: &str = "RAY_JOB_ID";
pub const ENV_VAR_KEY_RAYLET_PID: &str = "RAY_RAYLET_PID";

/// MessagePack offset for cross-language serialization.
pub const MESSAGE_PACK_OFFSET: usize = 9;

/// Setup worker filename.
pub const SETUP_WORKER_FILENAME: &str = "setup_worker.py";

/// Named ports.
pub const RUNTIME_ENV_AGENT_PORT_NAME: &str = "runtime_env_agent_port";
pub const METRICS_AGENT_PORT_NAME: &str = "metrics_agent_port";
pub const GCS_SERVER_PORT_NAME: &str = "gcs_server_port";

/// Implicit resource prefix for node resources.
pub const IMPLICIT_RESOURCE_PREFIX: &str = "node:__internal_implicit_resource_";

/// GCS PID key.
pub const GCS_PID_KEY: &str = "gcs_pid";

/// Internal namespace prefix.
pub const RAY_INTERNAL_NAMESPACE_PREFIX: &str = "_ray_internal_";

/// Library path environment variable (platform-dependent).
#[cfg(target_os = "macos")]
pub const LIBRARY_PATH_ENV_NAME: &str = "DYLD_LIBRARY_PATH";
#[cfg(target_os = "linux")]
pub const LIBRARY_PATH_ENV_NAME: &str = "LD_LIBRARY_PATH";
#[cfg(target_os = "windows")]
pub const LIBRARY_PATH_ENV_NAME: &str = "PATH";

/// Label key prefix.
pub const RAY_LABEL_KEY_PREFIX: &str = "ray.io/";
