// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <climits>

/// Default value for enable_task_events within core.
constexpr bool kDefaultTaskEventEnabled = true;

/// The precision of fractional resource quantity.
constexpr int kResourceUnitScaling = 10000;

constexpr char kWorkerSetupHookKeyName[] = "FunctionsToRun";

constexpr int kStreamingGeneratorReturn = -2;

/// Length of Ray full-length IDs in bytes.
constexpr size_t kUniqueIDSize = 28;

/// An ObjectID's bytes are split into the task ID itself and the index of the
/// object's creation. This is the maximum width of the object index in bits.
constexpr int kObjectIdIndexSize = 32;
static_assert(kObjectIdIndexSize % CHAR_BIT == 0,
              "ObjectID prefix not a multiple of bytes");

/// Raylet exit code on plasma store socket error.
constexpr int kRayletStoreErrorExitCode = 100;

/// Prefix for the object table keys in redis.
constexpr char kObjectTablePrefix[] = "ObjectTable";

constexpr char kClusterIdKey[] = "ray_cluster_id";

constexpr char kWorkerDynamicOptionPlaceholder[] =
    "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER";

constexpr char kNodeManagerPortPlaceholder[] = "RAY_NODE_MANAGER_PORT_PLACEHOLDER";

/// Public DNS address which is is used to connect and get local IP.
constexpr char kPublicDNSServerIp[] = "8.8.8.8";
constexpr int kPublicDNSServerPort = 53;

constexpr char kEnvVarKeyJobId[] = "RAY_JOB_ID";
constexpr char kEnvVarKeyRayletPid[] = "RAY_RAYLET_PID";

constexpr char kEnvVarKeyGrpcThreadCount[] = "RAY_num_grpc_internal_threads";

/// for cross-langueage serialization
constexpr int kMessagePackOffset = 9;

/// Filename of "shim process" that sets up Python worker environment.
/// Should be kept in sync with SETUP_WORKER_FILENAME in ray_constants.py
constexpr char kSetupWorkerFilename[] = "setup_worker.py";

/// The version of Ray
constexpr char kRayVersion[] = "3.0.0.dev0";

/*****************************/
/* ENV labels for autoscaler */
/*****************************/
constexpr char kGcsAutoscalerStateNamespace[] = "__autoscaler";
constexpr char kGcsAutoscalerV2EnabledKey[] = "__autoscaler_v2_enabled";
constexpr char kGcsAutoscalerClusterConfigKey[] = "__autoscaler_cluster_config";

/// Name for cloud instance id env
constexpr char kNodeCloudInstanceIdEnv[] = "RAY_CLOUD_INSTANCE_ID";

/// ENV keys for Ray node labels
constexpr char kNodeTypeNameEnv[] = "RAY_NODE_TYPE_NAME";
constexpr char kNodeMarketTypeEnv[] = "RAY_NODE_MARKET_TYPE";
constexpr char kNodeRegionEnv[] = "RAY_NODE_REGION";
constexpr char kNodeZoneEnv[] = "RAY_NODE_ZONE";

constexpr char kNodeCloudInstanceTypeNameEnv[] = "RAY_CLOUD_INSTANCE_TYPE_NAME";

/**********************************/
/* ENV labels for autoscaler ends */
/**********************************/

/// Key for the placement group's bundle placement constraint.
/// Used by FormatPlacementGroupLabelName()
constexpr char kPlacementGroupConstraintKeyPrefix[] = "_PG_";

#if defined(__APPLE__)
constexpr char kLibraryPathEnvName[] = "DYLD_LIBRARY_PATH";
#elif defined(_WIN32)
constexpr char kLibraryPathEnvName[] = "PATH";
#else
constexpr char kLibraryPathEnvName[] = "LD_LIBRARY_PATH";
#endif

/// Default node label keys populated by the Raylet
#define RAY_LABEL_KEY_PREFIX "ray.io/"

// The unique ID assigned to this node by the Raylet.
constexpr char kLabelKeyNodeID[] = RAY_LABEL_KEY_PREFIX "node-id";

// The accelerator type associated with the Ray node (e.g., "A100").
constexpr char kLabelKeyNodeAcceleratorType[] = RAY_LABEL_KEY_PREFIX "accelerator-type";

// The market type of the cloud instance this Ray node runs on (e.g., "on-demand" or
// "spot").
constexpr char kLabelKeyNodeMarketType[] = RAY_LABEL_KEY_PREFIX "market-type";

// The region of the cloud instance this Ray node runs on (e.g., "us-central2").
constexpr char kLabelKeyNodeRegion[] = RAY_LABEL_KEY_PREFIX "availability-region";

// The zone of the cloud instance this Ray node runs on (e.g., "us-central2-b").
constexpr char kLabelKeyNodeZone[] = RAY_LABEL_KEY_PREFIX "availability-zone";

// The name of the head or worker group this Ray node is a part of.
constexpr char kLabelKeyNodeGroup[] = RAY_LABEL_KEY_PREFIX "node-group";

/// TPU specific default labels. Used for multi-host TPU workload scheduling.

// The physical chip topology of the TPU accelerator of this Ray node.
constexpr char kLabelKeyTpuTopology[] = RAY_LABEL_KEY_PREFIX "tpu-topology";

// A unique identifier within the RayCluster for the TPU slice this Ray
// node is scheduled on.
constexpr char kLabelKeyTpuSliceName[] = RAY_LABEL_KEY_PREFIX "tpu-slice-name";

// A unique integer ID for a Ray node with TPU resources within the TPU slice
// it's scheduled on. Valid values are 0 to N-1 where N is the number of TPU hosts.
constexpr char kLabelKeyTpuWorkerId[] = RAY_LABEL_KEY_PREFIX "tpu-worker-id";

// A string representing the current TPU pod type, e.g. v6e-32.
constexpr char kLabelKeyTpuPodType[] = RAY_LABEL_KEY_PREFIX "tpu-pod-type";

#undef RAY_LABEL_KEY_PREFIX

/// All nodes implicitly have resources with this prefix and the quantity is 1.
/// NOTE: DON'T CHANGE THIS since autoscaler depends on it.
/// Ideally we want to define the constant in autoscaler.proto so it
/// can be shared but protobuf doesn't support defining string constants.
/// https://docs.google.com/document/d/151T4VnknX_5EtPy6E-LbpL-r1T4ZSO0UBvSgWdSjx4Q/edit#heading=h.2ews5m5fmz
constexpr char kImplicitResourcePrefix[] = "node:__internal_implicit_resource_";

/// PID of GCS process to record metrics.
constexpr char kGcsPidKey[] = "gcs_pid";
