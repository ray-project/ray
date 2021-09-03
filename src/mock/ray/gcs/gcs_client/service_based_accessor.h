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
namespace ray {
namespace gcs {

class MockServiceBasedJobInfoAccessor : public ServiceBasedJobInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncAdd,
              (const std::shared_ptr<rpc::JobTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncMarkFinished,
              (const JobID &job_id, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncSubscribeAll,
              ((const SubscribeCallback<JobID, rpc::JobTableData> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncGetAll, (const MultiItemCallback<rpc::JobTableData> &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
  MOCK_METHOD(Status, AsyncGetNextJobID, (const ItemCallback<JobID> &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedActorInfoAccessor : public ServiceBasedActorInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncGet,
              (const ActorID &actor_id,
               const OptionalItemCallback<rpc::ActorTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAll,
              (const MultiItemCallback<rpc::ActorTableData> &callback), (override));
  MOCK_METHOD(Status, AsyncGetByName,
              (const std::string &name, const std::string &ray_namespace,
               const OptionalItemCallback<rpc::ActorTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncListNamedActors,
              (bool all_namespaces, const std::string &ray_namespace,
               const ItemCallback<std::vector<rpc::NamedActorInfo>> &callback),
              (override));
  MOCK_METHOD(Status, AsyncRegisterActor,
              (const TaskSpecification &task_spec, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncCreateActor,
              (const TaskSpecification &task_spec, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncKillActor,
              (const ActorID &actor_id, bool force_kill, bool no_restart,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncSubscribeAll,
              ((const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncSubscribe,
              (const ActorID &actor_id,
               (const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncUnsubscribe, (const ActorID &actor_id), (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
  MOCK_METHOD(bool, IsActorUnsubscribed, (const ActorID &actor_id), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedNodeInfoAccessor : public ServiceBasedNodeInfoAccessor {
 public:
  MOCK_METHOD(Status, RegisterSelf,
              (const rpc::GcsNodeInfo &local_node_info, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, UnregisterSelf, (), (override));
  MOCK_METHOD(const NodeID &, GetSelfId, (), (const, override));
  MOCK_METHOD(const rpc::GcsNodeInfo &, GetSelfInfo, (), (const, override));
  MOCK_METHOD(Status, AsyncRegister,
              (const rpc::GcsNodeInfo &node_info, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncUnregister,
              (const NodeID &node_id, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncGetAll, (const MultiItemCallback<rpc::GcsNodeInfo> &callback),
              (override));
  MOCK_METHOD(Status, AsyncSubscribeToNodeChange,
              ((const SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(boost::optional<rpc::GcsNodeInfo>, Get,
              (const NodeID &node_id, bool filter_dead_nodes), (const, override));
  MOCK_METHOD((const std::unordered_map<NodeID, rpc::GcsNodeInfo> &), GetAll, (),
              (const, override));
  MOCK_METHOD(bool, IsRemoved, (const NodeID &node_id), (const, override));
  MOCK_METHOD(Status, AsyncReportHeartbeat,
              (const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
  MOCK_METHOD(Status, AsyncGetInternalConfig,
              (const OptionalItemCallback<std::string> &callback), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedNodeResourceInfoAccessor
    : public ServiceBasedNodeResourceInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncGetResources,
              (const NodeID &node_id, const OptionalItemCallback<ResourceMap> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAllAvailableResources,
              (const MultiItemCallback<rpc::AvailableResources> &callback), (override));
  MOCK_METHOD(Status, AsyncUpdateResources,
              (const NodeID &node_id, const ResourceMap &resources,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncDeleteResources,
              (const NodeID &node_id, const std::vector<std::string> &resource_names,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncSubscribeToResources,
              (const ItemCallback<rpc::NodeResourceChange> &subscribe,
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncReportResourceUsage,
              (const std::shared_ptr<rpc::ResourcesData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncReReportResourceUsage, (), (override));
  MOCK_METHOD(Status, AsyncGetAllResourceUsage,
              (const ItemCallback<rpc::ResourceUsageBatchData> &callback), (override));
  MOCK_METHOD(Status, AsyncSubscribeBatchedResourceUsage,
              (const ItemCallback<rpc::ResourceUsageBatchData> &subscribe,
               const StatusCallback &done),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedTaskInfoAccessor : public ServiceBasedTaskInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncAdd,
              (const std::shared_ptr<rpc::TaskTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncGet,
              (const TaskID &task_id,
               const OptionalItemCallback<rpc::TaskTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncAddTaskLease,
              (const std::shared_ptr<rpc::TaskLeaseData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetTaskLease,
              (const TaskID &task_id,
               const OptionalItemCallback<rpc::TaskLeaseData> &callback),
              (override));
  MOCK_METHOD(
      Status, AsyncSubscribeTaskLease,
      (const TaskID &task_id,
       (const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe),
       const StatusCallback &done),
      (override));
  MOCK_METHOD(Status, AsyncUnsubscribeTaskLease, (const TaskID &task_id), (override));
  MOCK_METHOD(Status, AttemptTaskReconstruction,
              (const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
  MOCK_METHOD(bool, IsTaskLeaseUnsubscribed, (const TaskID &task_id), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedObjectInfoAccessor : public ServiceBasedObjectInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncGetLocations,
              (const ObjectID &object_id,
               const OptionalItemCallback<rpc::ObjectLocationInfo> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAll,
              (const MultiItemCallback<rpc::ObjectLocationInfo> &callback), (override));
  MOCK_METHOD(Status, AsyncAddLocation,
              (const ObjectID &object_id, const NodeID &node_id, size_t object_size,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncAddSpilledUrl,
              (const ObjectID &object_id, const std::string &spilled_url,
               const NodeID &node_id, size_t object_size, const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncRemoveLocation,
              (const ObjectID &object_id, const NodeID &node_id,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncSubscribeToLocations,
              (const ObjectID &object_id,
               (const SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
                    &subscribe),
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncUnsubscribeToLocations, (const ObjectID &object_id),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
  MOCK_METHOD(bool, IsObjectUnsubscribed, (const ObjectID &object_id), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedStatsInfoAccessor : public ServiceBasedStatsInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncAddProfileData,
              (const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAll,
              (const MultiItemCallback<rpc::ProfileTableData> &callback), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedErrorInfoAccessor : public ServiceBasedErrorInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncReportJobError,
              (const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedWorkerInfoAccessor : public ServiceBasedWorkerInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncSubscribeToWorkerFailures,
              (const ItemCallback<rpc::WorkerDeltaData> &subscribe,
               const StatusCallback &done),
              (override));
  MOCK_METHOD(Status, AsyncReportWorkerFailure,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncGet,
              (const WorkerID &worker_id,
               const OptionalItemCallback<rpc::WorkerTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAll,
              (const MultiItemCallback<rpc::WorkerTableData> &callback), (override));
  MOCK_METHOD(Status, AsyncAdd,
              (const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(void, AsyncResubscribe, (bool is_pubsub_server_restarted), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedPlacementGroupInfoAccessor
    : public ServiceBasedPlacementGroupInfoAccessor {
 public:
  MOCK_METHOD(Status, AsyncCreatePlacementGroup,
              (const PlacementGroupSpecification &placement_group_spec,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncRemovePlacementGroup,
              (const PlacementGroupID &placement_group_id,
               const StatusCallback &callback),
              (override));
  MOCK_METHOD(Status, AsyncGet,
              (const PlacementGroupID &placement_group_id,
               const OptionalItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetByName,
              (const std::string &name, const std::string &ray_namespace,
               const OptionalItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncGetAll,
              (const MultiItemCallback<rpc::PlacementGroupTableData> &callback),
              (override));
  MOCK_METHOD(Status, AsyncWaitUntilReady,
              (const PlacementGroupID &placement_group_id,
               const StatusCallback &callback),
              (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockServiceBasedInternalKVAccessor : public ServiceBasedInternalKVAccessor {
 public:
  MOCK_METHOD(Status, AsyncInternalKVKeys,
              (const std::string &prefix,
               const OptionalItemCallback<std::vector<std::string>> &callback),
              (override));
  MOCK_METHOD(Status, AsyncInternalKVGet,
              (const std::string &key, const OptionalItemCallback<std::string> &callback),
              (override));
  MOCK_METHOD(Status, AsyncInternalKVPut,
              (const std::string &key, const std::string &value, bool overwrite,
               const OptionalItemCallback<int> &callback),
              (override));
  MOCK_METHOD(Status, AsyncInternalKVExists,
              (const std::string &key, const OptionalItemCallback<bool> &callback),
              (override));
  MOCK_METHOD(Status, AsyncInternalKVDel,
              (const std::string &key, const StatusCallback &callback), (override));
};

}  // namespace gcs
}  // namespace ray
