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
#include "ray/gcs/subscription_executor.h"
#include "ray/util/sequencer.h"

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

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<JobID, JobTableData> &subscribe,
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

  Status GetAll(std::vector<ActorTableData> *actor_table_data_list) override;

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncGetByName(
      const std::string &name,
      const OptionalItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            const StatusCallback &callback) override;

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override;

  Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id) override;

  Status AsyncAddCheckpoint(const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback) override;

  Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointData> &callback) override;

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

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

  Sequencer<ActorID> sequencer_;
};

/// \class ServiceBasedNodeInfoAccessor
/// ServiceBasedNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses GCS Service as the backend.
class ServiceBasedNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit ServiceBasedNodeInfoAccessor(ServiceBasedGcsClient *client_impl);

  virtual ~ServiceBasedNodeInfoAccessor() = default;

  Status RegisterSelf(const GcsNodeInfo &local_node_info) override;

  Status UnregisterSelf() override;

  const ClientID &GetSelfId() const override;

  const GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                       const StatusCallback &callback) override;

  Status AsyncUnregister(const ClientID &node_id,
                         const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) override;

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
      const StatusCallback &done) override;

  boost::optional<GcsNodeInfo> Get(const ClientID &node_id) const override;

  const std::unordered_map<ClientID, GcsNodeInfo> &GetAll() const override;

  bool IsRemoved(const ClientID &node_id) const override;

  Status AsyncGetResources(const ClientID &node_id,
                           const OptionalItemCallback<ResourceMap> &callback) override;

  Status AsyncUpdateResources(const ClientID &node_id, const ResourceMap &resources,
                              const StatusCallback &callback) override;

  Status AsyncDeleteResources(const ClientID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeToResources(const ItemCallback<rpc::NodeResourceChange> &subscribe,
                                   const StatusCallback &done) override;

  Status AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeHeartbeat(
      const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback) override;

  Status AsyncSubscribeBatchHeartbeat(
      const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
      const StatusCallback &done) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

  Status AsyncSetInternalConfig(
      std::unordered_map<std::string, std::string> &config) override;

  Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback)
      override;

 private:
  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_node_operation_;
  SubscribeOperation subscribe_resource_operation_;
  SubscribeOperation subscribe_batch_heartbeat_operation_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_node_data_operation_;

  void HandleNotification(const GcsNodeInfo &node_info);

  ServiceBasedGcsClient *client_impl_;

  using NodeChangeCallback =
      std::function<void(const ClientID &id, const GcsNodeInfo &node_info)>;

  GcsNodeInfo local_node_info_;
  ClientID local_node_id_;

  Sequencer<ClientID> sequencer_;

  /// The callback to call when a new node is added or a node is removed.
  NodeChangeCallback node_change_callback_{nullptr};

  /// A cache for information about all nodes.
  std::unordered_map<ClientID, GcsNodeInfo> node_cache_;
  /// The set of removed nodes.
  std::unordered_set<ClientID> removed_nodes_;
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

  Status AsyncDelete(const std::vector<TaskID> &task_ids,
                     const StatusCallback &callback) override;

  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const TaskID &task_id) override;

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

 private:
  /// Save the subscribe operations, so we can call them again when PubSub
  /// server restarts from a failure.
  std::unordered_map<TaskID, SubscribeOperation> subscribe_task_operations_;
  std::unordered_map<TaskID, SubscribeOperation> subscribe_task_lease_operations_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  std::unordered_map<TaskID, FetchDataOperation> fetch_task_data_operations_;
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
      const MultiItemCallback<rpc::ObjectTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ObjectLocationInfo> &callback) override;

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback) override;

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback) override;

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override;

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
      const SubscribeCallback<WorkerID, rpc::WorkerTableData> &subscribe,
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
      const PlacementGroupSpecification &placement_group_spec) override;

 private:
  ServiceBasedGcsClient *client_impl_;
};

}  // namespace gcs
}  // namespace ray
