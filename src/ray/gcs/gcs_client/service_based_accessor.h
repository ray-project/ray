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

#include "ray/common/task/task_spec.h"
#include "ray/gcs/accessor.h"
#include "ray/util/sequencer.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

using SubscribeOperation = std::function<Status(const StatusCallback &done)>;

using FetchDataOperation = std::function<void(const StatusCallback &done)>;

class ServiceBasedGcsClient;

/// \class ServiceBasedJobInfoAccessor
/// ServiceBasedJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit ServiceBasedJobInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedJobInfoAccessor() = default;

  Status AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::JobTableData> &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

 private:
  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_all_data_operation_;

  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_operation_;

  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedActorInfoAccessor
/// ServiceBasedActorInfoAccessor is an implementation of `ActorInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit ServiceBasedActorInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedActorInfoAccessor() = default;

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncGetByName(
      const std::string &name, const std::string &ray_namespace,
      const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            const StatusCallback &callback) override;

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override;

  Status AsyncKillActor(const ActorID &actor_id, bool force_kill, bool no_restart,
                        const StatusCallback &callback) override;

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

  bool IsActorUnsubscribed(const ActorID &actor_id) override;

 private:
  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_all_operation_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_all_data_operation_;

  // Mutex to protect the subscribe_operations_ field and fetch_data_operations_ field.
  absl::Mutex mutex_;

  /// Save the subscribe operation of actors.
  std::unordered_map<ActorID, SubscribeOperation> subscribe_operations_
      GUARDED_BY(mutex_);

  /// Save the fetch data operation of actors.
  std::unordered_map<ActorID, FetchDataOperation> fetch_data_operations_
      GUARDED_BY(mutex_);

  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedNodeInfoAccessor
/// ServiceBasedNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit ServiceBasedNodeInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedNodeInfoAccessor() = default;

  Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                      const StatusCallback &callback) override;

  Status UnregisterSelf() override;

  const NodeID &GetSelfId() const override;

  const rpc::GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                       const StatusCallback &callback) override;

  Status AsyncUnregister(const NodeID &node_id, const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback) override;

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done) override;

  boost::optional<rpc::GcsNodeInfo> Get(const NodeID &node_id,
                                        bool filter_dead_nodes = false) const override;

  const std::unordered_map<NodeID, rpc::GcsNodeInfo> &GetAll() const override;

  bool IsRemoved(const NodeID &node_id) const override;

  Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

  Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::string> &callback) override;

 private:
  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_node_operation_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_node_data_operation_;

  void HandleNotification(const rpc::GcsNodeInfo &node_info);

  ServiceBasedGcsClient *client_impl_;

  using NodeChangeCallback =
      std::function<void(const NodeID &id, const rpc::GcsNodeInfo &node_info)>;

  rpc::GcsNodeInfo local_node_info_;
  NodeID local_node_id_;

  /// The callback to call when a new node is added or a node is removed.
  NodeChangeCallback node_change_callback_{nullptr};

  /// A cache for information about all nodes.
  std::unordered_map<NodeID, rpc::GcsNodeInfo> node_cache_;
  /// The set of removed nodes.
  std::unordered_set<NodeID> removed_nodes_;
};

/// \class ServiceBasedNodeResourceInfoAccessor
/// ServiceBasedNodeResourceInfoAccessor is an implementation of
/// `NodeResourceInfoAccessor` that uses GCS Service as the backend.
class ServiceBasedNodeResourceInfoAccessor : public NodeResourceInfoAccessor {
 public:
  explicit ServiceBasedNodeResourceInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedNodeResourceInfoAccessor() = default;

  Status AsyncGetResources(const NodeID &node_id,
                           const OptionalItemCallback<ResourceMap> &callback) override;

  Status AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback) override;

  Status AsyncUpdateResources(const NodeID &node_id, const ResourceMap &resources,
                              const StatusCallback &callback) override;

  Status AsyncDeleteResources(const NodeID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeToResources(const ItemCallback<rpc::NodeResourceChange> &subscribe,
                                   const StatusCallback &done) override;

  Status AsyncReportResourceUsage(const std::shared_ptr<rpc::ResourcesData> &data_ptr,
                                  const StatusCallback &callback) override;

  void AsyncReReportResourceUsage() override;

  /// Fill resource fields with cached resources. Used by light resource usage report.
  void FillResourceUsageRequest(rpc::ReportResourceUsageRequest &resource_usage);

  Status AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback) override;

  Status AsyncSubscribeBatchedResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &subscribe,
      const StatusCallback &done) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

 private:
  // Mutex to protect the cached_resource_usage_ field.
  absl::Mutex mutex_;

  /// Save the resource usage data, so we can resend it again when GCS server restarts
  /// from a failure.
  rpc::ReportResourceUsageRequest cached_resource_usage_ GUARDED_BY(mutex_);

  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_resource_operation_;
  SubscribeOperation subscribe_batch_resource_usage_operation_;

  ServiceBasedGcsClient *client_impl_;

  Sequencer<NodeID> sequencer_;
};

/// \class ServiceBasedTaskInfoAccessor
/// ServiceBasedTaskInfoAccessor is an implementation of `TaskInfoAccessor`
/// that uses GCS service as the backend.
class ServiceBasedTaskInfoAccessor : public TaskInfoAccessor {
 public:
  explicit ServiceBasedTaskInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedTaskInfoAccessor() = default;

  Status AsyncAdd(const std::shared_ptr<rpc::TaskTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<rpc::TaskTableData> &callback) override;

  Status AsyncAddTaskLease(const std::shared_ptr<rpc::TaskLeaseData> &data_ptr,
                           const StatusCallback &callback) override;

  Status AsyncGetTaskLease(
      const TaskID &task_id,
      const OptionalItemCallback<rpc::TaskLeaseData> &callback) override;

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id) override;

  Status AttemptTaskReconstruction(
      const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

  bool IsTaskLeaseUnsubscribed(const TaskID &task_id) override;

 private:
  /// Save the subscribe operations, so we can call them again when PubSub
  /// server restarts from a failure.
  std::unordered_map<TaskID, SubscribeOperation> subscribe_task_lease_operations_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  std::unordered_map<TaskID, FetchDataOperation> fetch_task_lease_data_operations_;

  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedObjectInfoAccessor
/// ServiceBasedObjectInfoAccessor is an implementation of `ObjectInfoAccessor`
/// that uses GCS service as the backend.
class ServiceBasedObjectInfoAccessor : public ObjectInfoAccessor {
 public:
  explicit ServiceBasedObjectInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedObjectInfoAccessor() = default;

  Status AsyncGetLocations(
      const ObjectID &object_id,
      const OptionalItemCallback<rpc::ObjectLocationInfo> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ObjectLocationInfo> &callback) override;

  Status AsyncAddLocation(const ObjectID &object_id, const NodeID &node_id,
                          size_t object_size, const StatusCallback &callback) override;

  Status AsyncAddSpilledUrl(const ObjectID &object_id, const std::string &spilled_url,
                            const NodeID &node_id, size_t object_size,
                            const StatusCallback &callback) override;

  Status AsyncRemoveLocation(const ObjectID &object_id, const NodeID &node_id,
                             const StatusCallback &callback) override;

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
          &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

  bool IsObjectUnsubscribed(const ObjectID &object_id) override;

 private:
  // Mutex to protect the subscribe_object_operations_ field and
  // fetch_object_data_operations_ field.
  absl::Mutex mutex_;

  /// Save the subscribe operations, so we can call them again when PubSub
  /// server restarts from a failure.
  std::unordered_map<ObjectID, SubscribeOperation> subscribe_object_operations_
      GUARDED_BY(mutex_);

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  std::unordered_map<ObjectID, FetchDataOperation> fetch_object_data_operations_
      GUARDED_BY(mutex_);

  ServiceBasedGcsClient *client_impl_;

  Sequencer<ObjectID> sequencer_;
};

/// \class ServiceBasedStatsInfoAccessor
/// ServiceBasedStatsInfoAccessor is an implementation of `StatsInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedStatsInfoAccessor : public StatsInfoAccessor {
 public:
  explicit ServiceBasedStatsInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedStatsInfoAccessor() = default;

  Status AsyncAddProfileData(const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
                             const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ProfileTableData> &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedErrorInfoAccessor
/// ServiceBasedErrorInfoAccessor is an implementation of `ErrorInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedErrorInfoAccessor : public ErrorInfoAccessor {
 public:
  explicit ServiceBasedErrorInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedErrorInfoAccessor() = default;

  Status AsyncReportJobError(const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
                             const StatusCallback &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedWorkerInfoAccessor
/// ServiceBasedWorkerInfoAccessor is an implementation of `WorkerInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedWorkerInfoAccessor : public WorkerInfoAccessor {
 public:
  explicit ServiceBasedWorkerInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedWorkerInfoAccessor() = default;

  Status AsyncSubscribeToWorkerFailures(
      const ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportWorkerFailure(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                                  const StatusCallback &callback) override;

  Status AsyncGet(const WorkerID &worker_id,
                  const OptionalItemCallback<rpc::WorkerTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::WorkerTableData> &callback) override;

  Status AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                  const StatusCallback &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

 private:
  /// Save the subscribe operation in this function, so we can call it again when GCS
  /// restarts from a failure.
  SubscribeOperation subscribe_operation_;

  ServiceBasedGcsClient *client_impl_;
};

/// \class ServiceBasedPlacementGroupInfoAccessor
/// ServiceBasedPlacementGroupInfoAccessor is an implementation of
/// `PlacementGroupInfoAccessor` that uses GCS Service as the backend.

class ServiceBasedPlacementGroupInfoAccessor : public PlacementGroupInfoAccessor {
  // TODO(AlisaWu):fill the ServiceAccessor.
 public:
  explicit ServiceBasedPlacementGroupInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedPlacementGroupInfoAccessor() = default;

  Status AsyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec,
      const StatusCallback &callback) override;

  Status AsyncRemovePlacementGroup(const PlacementGroupID &placement_group_id,
                                   const StatusCallback &callback) override;

  Status AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) override;

  Status AsyncGetByName(
      const std::string &name,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) override;

  Status AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback) override;

  Status AsyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                             const StatusCallback &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_;
};

class ServiceBasedInternalKVAccessor : public InternalKVAccessor {
 public:
  explicit ServiceBasedInternalKVAccessor(ServiceBasedGcsClient *client_impl);
  ~ServiceBasedInternalKVAccessor() override = default;

  Status AsyncInternalKVKeys(
      const std::string &prefix,
      const OptionalItemCallback<std::vector<std::string>> &callback) override;
  Status AsyncInternalKVGet(const std::string &key,
                            const OptionalItemCallback<std::string> &callback) override;
  Status AsyncInternalKVPut(const std::string &key, const std::string &value,
                            bool overwrite,
                            const OptionalItemCallback<int> &callback) override;
  Status AsyncInternalKVExists(const std::string &key,
                               const OptionalItemCallback<bool> &callback) override;
  Status AsyncInternalKVDel(const std::string &key,
                            const StatusCallback &callback) override;

 private:
  ServiceBasedGcsClient *client_impl_;
};

}  // namespace gcs
}  // namespace ray
