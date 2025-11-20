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
#include "ray/gcs_rpc_client/accessor.h"

namespace ray {
namespace gcs {

class MockActorInfoAccessor : public ActorInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncGet,
              (const ActorID &actor_id,
               const rpc::OptionalItemCallback<rpc::ActorTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAllByFilter,
              (const std::optional<ActorID> &actor_id,
               const std::optional<JobID> &job_id,
               const std::optional<std::string> &actor_state_name,
               const rpc::MultiItemCallback<rpc::ActorTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncGetByName,
              (const std::string &name,
               const std::string &ray_namespace,
               const rpc::OptionalItemCallback<rpc::ActorTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncRegisterActor,
              (const TaskSpecification &task_spec,
               const rpc::StatusCallback &callback,
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
               const rpc::StatusCallback &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncCreateActor,
              (const TaskSpecification &task_spec,
               const rpc::ClientCallback<rpc::CreateActorReply> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncSubscribe,
              (const ActorID &actor_id,
               (const rpc::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe),
               const rpc::StatusCallback &done),
              (override));
  MOCK_METHOD(void, AsyncUnsubscribe, (const ActorID &actor_id), (override));
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
               const rpc::StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncMarkFinished,
              (const JobID &job_id, const rpc::StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncSubscribeAll,
              ((const rpc::SubscribeCallback<JobID, rpc::JobTableData> &subscribe),
               const rpc::StatusCallback &done),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const std::optional<std::string> &job_or_submission_id,
               bool skip_submission_job_info_field,
               bool skip_is_running_tasks_field,
               const rpc::MultiItemCallback<rpc::JobTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (), (override));
  MOCK_METHOD(void,
              AsyncGetNextJobID,
              (const rpc::ItemCallback<JobID> &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockNodeInfoAccessor : public NodeInfoAccessor {
 public:
  MOCK_METHOD(void,
              RegisterSelf,
              (rpc::GcsNodeInfo && local_node_info, const rpc::StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncRegister,
              (const rpc::GcsNodeInfo &node_info, const rpc::StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncCheckAlive,
              (const std::vector<NodeID> &node_ids,
               int64_t timeout_ms,
               const rpc::MultiItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const rpc::MultiItemCallback<rpc::GcsNodeInfo> &callback,
               int64_t timeout_ms,
               const std::vector<NodeID> &node_ids),
              (override));
  MOCK_METHOD(void,
              AsyncGetAllNodeAddressAndLiveness,
              (const rpc::MultiItemCallback<rpc::GcsNodeAddressAndLiveness> &callback,
               int64_t timeout_ms,
               const std::vector<NodeID> &node_ids),
              (override));
  MOCK_METHOD(void,
              AsyncSubscribeToNodeChange,
              (std::function<void(NodeID, const rpc::GcsNodeInfo &)> subscribe,
               rpc::StatusCallback done),
              (override));
  MOCK_METHOD(
      void,
      AsyncSubscribeToNodeAddressAndLivenessChange,
      (std::function<void(NodeID, const rpc::GcsNodeAddressAndLiveness &)> subscribe,
       rpc::StatusCallback done),
      (override));
  MOCK_METHOD(const rpc::GcsNodeInfo *,
              Get,
              (const NodeID &node_id, bool filter_dead_nodes),
              (const, override));
  MOCK_METHOD(const rpc::GcsNodeAddressAndLiveness *,
              GetNodeAddressAndLiveness,
              (const NodeID &node_id, bool filter_dead_nodes),
              (const, override));
  MOCK_METHOD((const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &),
              GetAll,
              (),
              (const, override));
  MOCK_METHOD((const absl::flat_hash_map<NodeID, rpc::GcsNodeAddressAndLiveness> &),
              GetAllNodeAddressAndLiveness,
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
              (const rpc::MultiItemCallback<rpc::AvailableResources> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAllResourceUsage,
              (const rpc::ItemCallback<rpc::ResourceUsageBatchData> &callback),
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
              (std::unique_ptr<rpc::TaskEventData> data_ptr,
               rpc::StatusCallback callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockWorkerInfoAccessor : public WorkerInfoAccessor {
 public:
  MOCK_METHOD(void,
              AsyncSubscribeToWorkerFailures,
              (const rpc::ItemCallback<rpc::WorkerDeltaData> &subscribe,
               const rpc::StatusCallback &done),
              (override));
  MOCK_METHOD(void,
              AsyncReportWorkerFailure,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const rpc::StatusCallback &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGet,
              (const WorkerID &worker_id,
               const rpc::OptionalItemCallback<rpc::WorkerTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const rpc::MultiItemCallback<rpc::WorkerTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncAdd,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const rpc::StatusCallback &callback),
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
               const rpc::OptionalItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetByName,
              (const std::string &placement_group_name,
               const std::string &ray_namespace,
               const rpc::OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
               int64_t timeout_ms),
              (override));
  MOCK_METHOD(void,
              AsyncGetAll,
              (const rpc::MultiItemCallback<rpc::PlacementGroupTableData> &callback),
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
               const rpc::OptionalItemCallback<std::vector<std::string>> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVGet,
              (const std::string &ns,
               const std::string &key,
               const int64_t timeout_ms,
               const rpc::OptionalItemCallback<std::string> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVPut,
              (const std::string &ns,
               const std::string &key,
               const std::string &value,
               bool overwrite,
               const int64_t timeout_ms,
               const rpc::OptionalItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVExists,
              (const std::string &ns,
               const std::string &key,
               const int64_t timeout_ms,
               const rpc::OptionalItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncInternalKVDel,
              (const std::string &ns,
               const std::string &key,
               bool del_by_prefix,
               const int64_t timeout_ms,
               const rpc::OptionalItemCallback<int> &callback),
              (override));
  MOCK_METHOD(void,
              AsyncGetInternalConfig,
              (const rpc::OptionalItemCallback<std::string> &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
