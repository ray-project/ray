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

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/subscription_executor.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class RedisGcsClient;

/// \class RedisLogBasedActorInfoAccessor
/// `RedisLogBasedActorInfoAccessor` is an implementation of `ActorInfoAccessor`
/// that uses Redis as the backend storage.
class RedisLogBasedActorInfoAccessor : public ActorInfoAccessor {
 public:
  explicit RedisLogBasedActorInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisLogBasedActorInfoAccessor() {}

  Status GetAll(std::vector<ActorTableData> *actor_table_data_list) override;

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override {
    return Status::NotImplemented(
        "RedisLogBasedActorInfoAccessor does not support AsyncGetAll.");
  }

  Status AsyncGetByName(const std::string &name,
                        const OptionalItemCallback<ActorTableData> &callback) override {
    return Status::NotImplemented(
        "RedisLogBasedActorInfoAccessor does not support named detached actors.");
  }

  Status AsyncRegisterActor(const TaskSpecification &task_spec,
                            const StatusCallback &callback) override;

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  bool IsActorUnsubscribed(const ActorID &actor_id) override { return false; }

 protected:
  virtual std::vector<ActorID> GetAllActorID() const;
  virtual Status Get(const ActorID &actor_id, ActorTableData *actor_table_data) const;

  RedisGcsClient *client_impl_{nullptr};
  // Use a random NodeID for actor subscription. Because:
  // If we use NodeID::Nil, GCS will still send all actors' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local NodeID, so we use
  // random NodeID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  NodeID subscribe_id_{NodeID::FromRandom()};

 private:
  typedef SubscriptionExecutor<ActorID, ActorTableData, LogBasedActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor log_based_actor_sub_executor_;
};

/// \class RedisActorInfoAccessor
/// `RedisActorInfoAccessor` is an implementation of `ActorInfoAccessor`
/// that uses Redis as the backend storage.
class RedisActorInfoAccessor : public RedisLogBasedActorInfoAccessor {
 public:
  explicit RedisActorInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisActorInfoAccessor() {}

  Status AsyncGet(const ActorID &actor_id,
                  const OptionalItemCallback<ActorTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) override;

  Status AsyncGetByName(const std::string &name,
                        const OptionalItemCallback<ActorTableData> &callback) override {
    return Status::NotImplemented(
        "RedisActorInfoAccessor does not support named detached actors.");
  }

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id) override;

 protected:
  std::vector<ActorID> GetAllActorID() const override;
  Status Get(const ActorID &actor_id, ActorTableData *actor_table_data) const override;

 private:
  typedef SubscriptionExecutor<ActorID, ActorTableData, ActorTable>
      ActorSubscriptionExecutor;
  ActorSubscriptionExecutor actor_sub_executor_;
};

/// \class RedisJobInfoAccessor
/// RedisJobInfoAccessor is an implementation of `JobInfoAccessor`
/// that uses Redis as the backend storage.
class RedisJobInfoAccessor : public JobInfoAccessor {
 public:
  explicit RedisJobInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisJobInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<JobTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<JobID, JobTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::JobTableData> &callback) override {
    return Status::NotImplemented("AsyncGetAll not implemented");
  }

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  /// Append job information to GCS asynchronously.
  ///
  /// \param data_ptr The job information that will be appended to GCS.
  /// \param callback Callback that will be called after append done.
  /// \return Status
  Status DoAsyncAppend(const std::shared_ptr<JobTableData> &data_ptr,
                       const StatusCallback &callback);

  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<JobID, JobTableData, JobTable> JobSubscriptionExecutor;
  JobSubscriptionExecutor job_sub_executor_;
};

/// \class RedisTaskInfoAccessor
/// `RedisTaskInfoAccessor` is an implementation of `TaskInfoAccessor`
/// that uses Redis as the backend storage.
class RedisTaskInfoAccessor : public TaskInfoAccessor {
 public:
  explicit RedisTaskInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisTaskInfoAccessor() {}

  Status AsyncAdd(const std::shared_ptr<TaskTableData> &data_ptr,
                  const StatusCallback &callback) override;

  Status AsyncGet(const TaskID &task_id,
                  const OptionalItemCallback<TaskTableData> &callback) override;

  Status AsyncDelete(const std::vector<TaskID> &task_ids,
                     const StatusCallback &callback) override;

  Status AsyncSubscribe(const TaskID &task_id,
                        const SubscribeCallback<TaskID, TaskTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const TaskID &task_id) override;

  Status AsyncAddTaskLease(const std::shared_ptr<TaskLeaseData> &data_ptr,
                           const StatusCallback &callback) override;

  Status AsyncGetTaskLease(const TaskID &task_id,
                           const OptionalItemCallback<TaskLeaseData> &callback) override;

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id) override;

  Status AttemptTaskReconstruction(
      const std::shared_ptr<TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  bool IsTaskUnsubscribed(const TaskID &task_id) override { return false; }

  bool IsTaskLeaseUnsubscribed(const TaskID &task_id) override { return false; }

 private:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random NodeID for task subscription. Because:
  // If we use NodeID::Nil, GCS will still send all tasks' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local NodeID, so we use
  // random NodeID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  NodeID subscribe_id_{NodeID::FromRandom()};

  typedef SubscriptionExecutor<TaskID, TaskTableData, raylet::TaskTable>
      TaskSubscriptionExecutor;
  TaskSubscriptionExecutor task_sub_executor_;

  typedef SubscriptionExecutor<TaskID, boost::optional<TaskLeaseData>, TaskLeaseTable>
      TaskLeaseSubscriptionExecutor;
  TaskLeaseSubscriptionExecutor task_lease_sub_executor_;
};

/// \class RedisObjectInfoAccessor
/// RedisObjectInfoAccessor is an implementation of `ObjectInfoAccessor`
/// that uses Redis as the backend storage.
class RedisObjectInfoAccessor : public ObjectInfoAccessor {
 public:
  explicit RedisObjectInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisObjectInfoAccessor() {}

  Status AsyncGetLocations(
      const ObjectID &object_id,
      const OptionalItemCallback<rpc::ObjectLocationInfo> &callback) override;

  Status AsyncGetAll(
      const MultiItemCallback<rpc::ObjectLocationInfo> &callback) override {
    return Status::NotImplemented("AsyncGetAll not implemented");
  }

  Status AsyncAddLocation(const ObjectID &object_id, const NodeID &node_id,
                          const StatusCallback &callback) override;

  Status AsyncAddSpilledUrl(const ObjectID &object_id, const std::string &spilled_url,
                            const StatusCallback &callback) override {
    return Status::NotImplemented("AsyncAddSpilledUrl not implemented");
  }

  Status AsyncRemoveLocation(const ObjectID &object_id, const NodeID &node_id,
                             const StatusCallback &callback) override;

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
          &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  bool IsObjectUnsubscribed(const ObjectID &object_id) override { return false; }

 private:
  RedisGcsClient *client_impl_{nullptr};

  // Use a random NodeID for object subscription. Because:
  // If we use NodeID::Nil, GCS will still send all objects' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local NodeID, so we use
  // random NodeID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  NodeID subscribe_id_{NodeID::FromRandom()};

  typedef SubscriptionExecutor<ObjectID, ObjectChangeNotification, ObjectTable>
      ObjectSubscriptionExecutor;
  ObjectSubscriptionExecutor object_sub_executor_;
};

/// \class RedisNodeInfoAccessor
/// RedisNodeInfoAccessor is an implementation of `NodeInfoAccessor`
/// that uses Redis as the backend storage.
class RedisNodeInfoAccessor : public NodeInfoAccessor {
 public:
  explicit RedisNodeInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisNodeInfoAccessor() {}

  Status RegisterSelf(const GcsNodeInfo &local_node_info,
                      const StatusCallback &callback) override;

  Status UnregisterSelf() override;

  const NodeID &GetSelfId() const override;

  const GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncRegister(const GcsNodeInfo &node_info,
                       const StatusCallback &callback) override;

  Status AsyncUnregister(const NodeID &node_id, const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) override;

  Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<NodeID, GcsNodeInfo> &subscribe,
      const StatusCallback &done) override;

  boost::optional<GcsNodeInfo> Get(const NodeID &node_id,
                                   bool filter_dead_nodes = true) const override;

  const std::unordered_map<NodeID, GcsNodeInfo> &GetAll() const override;

  bool IsRemoved(const NodeID &node_id) const override;

  Status AsyncReportHeartbeat(const std::shared_ptr<HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override;

  Status AsyncReportResourceUsage(const std::shared_ptr<rpc::ResourcesData> &data_ptr,
                                  const StatusCallback &callback) override;

  void AsyncReReportResourceUsage() override;

  Status AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback) override {
    return Status::NotImplemented("AsyncGetAllResourceUsage not implemented");
  }

  Status AsyncSubscribeBatchedResourceUsage(
      const ItemCallback<ResourceUsageBatchData> &subscribe,
      const StatusCallback &done) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

  Status AsyncSetInternalConfig(
      std::unordered_map<std::string, std::string> &config) override {
    return Status::NotImplemented("SetInternaConfig not implemented.");
  }

  Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback)
      override {
    return Status::NotImplemented("GetInternalConfig not implemented.");
  }

 private:
  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<NodeID, ResourceUsageBatchData, ResourceUsageBatchTable>
      HeartbeatBatchSubscriptionExecutor;
  HeartbeatBatchSubscriptionExecutor resource_usage_batch_sub_executor_;
};

/// \class RedisNodeResourceInfoAccessor
/// RedisNodeResourceInfoAccessor is an implementation of `NodeResourceInfoAccessor`
/// that uses Redis as the backend storage.
class RedisNodeResourceInfoAccessor : public NodeResourceInfoAccessor {
 public:
  explicit RedisNodeResourceInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisNodeResourceInfoAccessor() {}

  Status AsyncGetResources(const NodeID &node_id,
                           const OptionalItemCallback<ResourceMap> &callback) override;

  Status AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback) override {
    return Status::NotImplemented("AsyncGetAllAvailableResources not implemented");
  }

  Status AsyncUpdateResources(const NodeID &node_id, const ResourceMap &resources,
                              const StatusCallback &callback) override;

  Status AsyncDeleteResources(const NodeID &node_id,
                              const std::vector<std::string> &resource_names,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeToResources(const ItemCallback<rpc::NodeResourceChange> &subscribe,
                                   const StatusCallback &done) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<NodeID, ResourceChangeNotification, DynamicResourceTable>
      DynamicResourceSubscriptionExecutor;
  DynamicResourceSubscriptionExecutor resource_sub_executor_;
};

/// \class RedisErrorInfoAccessor
/// RedisErrorInfoAccessor is an implementation of `ErrorInfoAccessor`
/// that uses Redis as the backend storage.
class RedisErrorInfoAccessor : public ErrorInfoAccessor {
 public:
  explicit RedisErrorInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisErrorInfoAccessor() = default;

  Status AsyncReportJobError(const std::shared_ptr<ErrorTableData> &data_ptr,
                             const StatusCallback &callback) override;
};

/// \class RedisStatsInfoAccessor
/// RedisStatsInfoAccessor is an implementation of `StatsInfoAccessor`
/// that uses Redis as the backend storage.
class RedisStatsInfoAccessor : public StatsInfoAccessor {
 public:
  explicit RedisStatsInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisStatsInfoAccessor() = default;

  Status AsyncAddProfileData(const std::shared_ptr<ProfileTableData> &data_ptr,
                             const StatusCallback &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::ProfileTableData> &callback) override {
    return Status::NotImplemented("AsyncGetAll not implemented");
  }

 private:
  RedisGcsClient *client_impl_{nullptr};
};

/// \class RedisWorkerInfoAccessor
/// RedisWorkerInfoAccessor is an implementation of `WorkerInfoAccessor`
/// that uses Redis as the backend storage.
class RedisWorkerInfoAccessor : public WorkerInfoAccessor {
 public:
  explicit RedisWorkerInfoAccessor(RedisGcsClient *client_impl);

  virtual ~RedisWorkerInfoAccessor() = default;

  Status AsyncSubscribeToWorkerFailures(
      const SubscribeCallback<WorkerID, WorkerTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportWorkerFailure(const std::shared_ptr<WorkerTableData> &data_ptr,
                                  const StatusCallback &callback) override;

  Status AsyncGet(const WorkerID &worker_id,
                  const OptionalItemCallback<rpc::WorkerTableData> &callback) override;

  Status AsyncGetAll(const MultiItemCallback<rpc::WorkerTableData> &callback) override;

  Status AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                  const StatusCallback &callback) override;

  void AsyncResubscribe(bool is_pubsub_server_restarted) override {}

 private:
  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<WorkerID, WorkerTableData, WorkerTable>
      WorkerFailureSubscriptionExecutor;
  WorkerFailureSubscriptionExecutor worker_failure_sub_executor_;
};

class RedisPlacementGroupInfoAccessor : public PlacementGroupInfoAccessor {
 public:
  virtual ~RedisPlacementGroupInfoAccessor() = default;

  Status AsyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec) override;

  Status AsyncRemovePlacementGroup(const PlacementGroupID &placement_group_id,
                                   const StatusCallback &callback) override;

  Status AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) override;

  Status AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback) override;

  Status AsyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                             const StatusCallback &callback) override;
};

}  // namespace gcs

}  // namespace ray
