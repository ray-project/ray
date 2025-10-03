// Copyright  The Ray Authors.
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
#include "gmock/gmock.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/autoscaler_state_accessor.h"
#include "ray/gcs_rpc_client/accessors/error_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/internal_kv_accessor.h"
#include "ray/gcs_rpc_client/accessors/job_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/node_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/node_resource_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/placement_group_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/publisher_accessor.h"
#include "ray/gcs_rpc_client/accessors/runtime_env_accessor.h"
#include "ray/gcs_rpc_client/accessors/task_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/worker_info_accessor.h"

namespace ray {
namespace gcs {

class MockActorInfoAccessor : public ActorInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncGet,
              (const ActorID &actor_id,
               const OptionalItemCallback<rpc::ActorTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAllByFilter,
              (const std::optional<ActorID> &actor_id,
               const std::optional<JobID> &job_id,
               const std::optional<std::string> &actor_state_name,
               const MultiItemCallback<rpc::ActorTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncGetByName,
              (const std::string &name,
               const std::string &ray_namespace,
               const OptionalItemCallback<rpc::ActorTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncRegisterActor,
              (const TaskSpecification &task_spec,
               const StatusCallback &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(Status,
              SyncRegisterActor,
              (const TaskSpecification &task_spec),
              (override));
  MOCK_METHOD(void,
              AsyncKillActor,
              (const ActorID &actor_id,
               bool force_kill,
               bool no_restart,
               const StatusCallback &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncCreateActor,
              (const TaskSpecification &task_spec,
               const rpc::ClientCallback<rpc::CreateActorReply> &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncSubscribe,
              (const ActorID &actor_id,
               (const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncUnsubscribe, (const ActorID &actor_id), (override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
  MOCK_METHOD(bool, IsActorUnsubscribed, (const ActorID &actor_id), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockJobInfoAccessor : public JobInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncAdd,
              (const std::shared_ptr<rpc::JobTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncMarkFinished,
              (const JobID &job_id, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status,
              AsyncSubscribeAll,
              ((const SubscribeCallback<JobID, rpc::JobTableData> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const std::optional<std::string> &job_or_submission_id,
               bool skip_submission_job_info_field,
               bool skip_is_running_tasks_field,
               const MultiItemCallback<rpc::JobTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
  MOCK_METHOD(void, AsyncGetNextJobID, (const ItemCallback<JobID> &callback), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockNodeInfoAccessor : public NodeInfoAccessor {
 public:
  MOCK_METHOD(Status,
              RegisterSelf,
              (const rpc::GcsNodeInfo &local_node_info, const StatusCallback &callback),
              (override));
  MOCK_METHOD(const NodeID &, GetSelfId, (), (const, override));
  MOCK_METHOD(const rpc::GcsNodeInfo &, GetSelfInfo, (), (const, override));
  MOCK_METHOD(void,
              AsyncRegister,
              (const rpc::GcsNodeInfo &node_info, const StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncCheckSelfAlive,
              (const std::function<void(Status, bool)> &callback, int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncCheckAlive,
              (const std::vector<NodeID> &node_ids,
               int64_t timeout_ms,
               const MultiItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const MultiItemCallback<rpc::GcsNodeInfo> &callback,
               int64_t timeout_ms,
               const std::vector<NodeID> &node_ids),
              (override));
  MOCK_METHOD(void,
              AsyncSubscribeToNodeChange,
              (std::function<void(NodeID, const rpc::GcsNodeInfo &)> subscribe,
               StatusCallback done),
              (override));
  MOCK_METHOD(const rpc::GcsNodeInfo *,
              Get,
              (const NodeID &node_id, bool filter_dead_nodes),
              (const, override));
  MOCK_METHOD((const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &),
              GetAll,
              (),
              (const, override));
  MOCK_METHOD(Status,
              CheckAlive,
              (const std::vector<NodeID> &node_ids,
               int64_t timeout_ms,
               std::vector<bool> &nodes_alive),
              (override));
  MOCK_METHOD(bool, IsNodeDead, (const NodeID &node_id), (const, override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockNodeResourceInfoAccessor : public NodeResourceInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncGetAllAvailableResources,
              (const MultiItemCallback<rpc::AvailableResources> &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
  MOCK_METHOD(void,
              AsyncGetAllResourceUsage,
              (const ItemCallback<rpc::ResourceUsageBatchData> &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockErrorInfoAccessor : public ErrorInfoAccessor {
 public:
  MOCK_METHOD(void, AsyncReportJobError, (rpc::ErrorTableData data), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockTaskInfoAccessor : public TaskInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncAddTaskEventData,
              (std::unique_ptr<rpc::TaskEventData> data_ptr, StatusCallback callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockWorkerInfoAccessor : public WorkerInfoAccessor {
 public:
  MOCK_METHOD(Status,
              AsyncSubscribeToWorkerFailures,
              (const ItemCallback<rpc::WorkerDeltaData> &subscribe,
               const StatusCallback &done),
              (override));
  MOCK_METHOD(void,
              AsyncReportWorkerFailure,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGet,
              (const WorkerID &worker_id,
               const OptionalItemCallback<rpc::WorkerTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const MultiItemCallback<rpc::WorkerTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncAdd,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockPlacementGroupInfoAccessor : public PlacementGroupInfoAccessor {
 public:
  MOCK_METHOD(Status,
              SyncCreatePlacementGroup,
              (const PlacementGroupSpecification &placement_group_spec),
              (override));
  MOCK_METHOD(void,
              AsyncGet,
              (const PlacementGroupID &placement_group_id,
               const OptionalItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetByName,
              (const std::string &placement_group_name,
               const std::string &ray_namespace,
               const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const MultiItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(Status,
              SyncRemovePlacementGroup,
              (const PlacementGroupID &placement_group_id),
              (override));
  MOCK_METHOD(Status,
              SyncWaitUntilReady,
              (const PlacementGroupID &placement_group_id, int64_t timeout_seconds),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockInternalKVAccessor : public InternalKVAccessor {
 public:
  MOCK_METHOD(void,
              AsyncInternalKVKeys,
              (const std::string &ns,
               const std::string &prefix,
               const int64_t timeout_ms,
               const OptionalItemCallback<std::vector<std::string>> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVGet,
              (const std::string &ns,
               const std::string &key,
               const int64_t timeout_ms,
               const OptionalItemCallback<std::string> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVPut,
              (const std::string &ns,
               const std::string &key,
               const std::string &value,
               bool overwrite,
               const int64_t timeout_ms,
               const OptionalItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVExists,
              (const std::string &ns,
               const std::string &key,
               const int64_t timeout_ms,
               const OptionalItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVDel,
              (const std::string &ns,
               const std::string &key,
               bool del_by_prefix,
               const int64_t timeout_ms,
               const OptionalItemCallback<int> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetInternalConfig,
              (const OptionalItemCallback<std::string> &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockRuntimeEnvAccessor : public RuntimeEnvAccessor {
 public:
  MOCK_METHOD(Status,
              PinRuntimeEnvUri,
              (const std::string &uri, int expiration_s, int64_t timeout_ms),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockAutoscalerStateAccessor : public AutoscalerStateAccessor {
 public:
  MOCK_METHOD(Status,
              RequestClusterResourceConstraint,
              (int64_t timeout_ms,
               (const std::vector<std::unordered_map<std::string, double>> &bundles),
               (const std::vector<int64_t> &count_array)),
              (override));
  MOCK_METHOD(Status,
              GetClusterResourceState,
              (int64_t timeout_ms, std::string &serialized_reply),
              (override));
  MOCK_METHOD(Status,
              GetClusterStatus,
              (int64_t timeout_ms, std::string &serialized_reply),
              (override));
  MOCK_METHOD(Status,
              ReportAutoscalingState,
              (int64_t timeout_ms, const std::string &serialized_state),
              (override));
  MOCK_METHOD(Status,
              DrainNode,
              (const std::string &node_id,
               int32_t reason,
               const std::string &reason_message,
               int64_t deadline_timestamp_ms,
               int64_t timeout_ms,
               bool &is_accepted,
               std::string &rejection_reason_message),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockPublisherAccessor : public PublisherAccessor {
 public:
  MOCK_METHOD(Status,
              PublishError,
              (std::string key_id, rpc::ErrorTableData data, int64_t timeout_ms),
              (override));
  MOCK_METHOD(Status,
              PublishLogs,
              (std::string key_id, rpc::LogBatch data, int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncPublishNodeResourceUsage,
              (std::string key_id,
               std::string node_resource_usage_json,
               const StatusCallback &done),
              (override));
};

}  // namespace gcs
}  // namespace ray
