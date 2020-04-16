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

#ifndef RAY_GCS_REDIS_ACCESSOR_H
#define RAY_GCS_REDIS_ACCESSOR_H

#include <ray/common/task/task_spec.h>
#include "ray/common/id.h"
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

  Status AsyncCreateActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback) override;

  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) override;

  Status AsyncAddCheckpoint(const std::shared_ptr<ActorCheckpointData> &data_ptr,
                            const StatusCallback &callback) override;

  Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
      const OptionalItemCallback<ActorCheckpointData> &callback) override;

  Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<ActorCheckpointIdData> &callback) override;

 protected:
  virtual std::vector<ActorID> GetAllActorID() const;
  virtual Status Get(const ActorID &actor_id, ActorTableData *actor_table_data) const;

 private:
  /// Add checkpoint id to GCS asynchronously.
  ///
  /// \param actor_id The ID of actor that the checkpoint belongs to.
  /// \param checkpoint_id The ID of checkpoint that will be added to GCS.
  /// \return Status
  Status AsyncAddCheckpointID(const ActorID &actor_id,
                              const ActorCheckpointID &checkpoint_id,
                              const StatusCallback &callback);

 protected:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random ClientID for actor subscription. Because:
  // If we use ClientID::Nil, GCS will still send all actors' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID subscribe_id_{ClientID::FromRandom()};

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

  Status AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                       const StatusCallback &callback) override;

  Status AsyncUpdate(const ActorID &actor_id,
                     const std::shared_ptr<ActorTableData> &data_ptr,
                     const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                           const StatusCallback &done) override;

  Status AsyncSubscribe(const ActorID &actor_id,
                        const SubscribeCallback<ActorID, ActorTableData> &subscribe,
                        const StatusCallback &done) override;

  Status AsyncUnsubscribe(const ActorID &actor_id, const StatusCallback &done) override;

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

  Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, JobTableData> &subscribe,
      const StatusCallback &done) override;

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

  Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done) override;

  Status AsyncAddTaskLease(const std::shared_ptr<TaskLeaseData> &data_ptr,
                           const StatusCallback &callback) override;

  Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<TaskLeaseData>> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeTaskLease(const TaskID &task_id,
                                   const StatusCallback &done) override;

  Status AttemptTaskReconstruction(
      const std::shared_ptr<TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) override;

 private:
  RedisGcsClient *client_impl_{nullptr};
  // Use a random ClientID for task subscription. Because:
  // If we use ClientID::Nil, GCS will still send all tasks' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID subscribe_id_{ClientID::FromRandom()};

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

  Status AsyncGetLocations(const ObjectID &object_id,
                           const MultiItemCallback<ObjectTableData> &callback) override;

  Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                          const StatusCallback &callback) override;

  Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                             const StatusCallback &callback) override;

  Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) override;

  Status AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                     const StatusCallback &done) override;

 private:
  RedisGcsClient *client_impl_{nullptr};

  // Use a random ClientID for object subscription. Because:
  // If we use ClientID::Nil, GCS will still send all objects' updates to this GCS Client.
  // Even we can filter out irrelevant updates, but there will be extra overhead.
  // And because the new GCS Client will no longer hold the local ClientID, so we use
  // random ClientID instead.
  // TODO(micafan): Remove this random id, once GCS becomes a service.
  ClientID subscribe_id_{ClientID::FromRandom()};

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

  Status RegisterSelf(const GcsNodeInfo &local_node_info) override;

  Status UnregisterSelf() override;

  const ClientID &GetSelfId() const override;

  const GcsNodeInfo &GetSelfInfo() const override;

  Status AsyncRegister(const GcsNodeInfo &node_info,
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

  Status AsyncSubscribeToResources(
      const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportHeartbeat(const std::shared_ptr<HeartbeatTableData> &data_ptr,
                              const StatusCallback &callback) override;

  Status AsyncSubscribeHeartbeat(
      const SubscribeCallback<ClientID, HeartbeatTableData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback) override;

  Status AsyncSubscribeBatchHeartbeat(
      const ItemCallback<HeartbeatBatchTableData> &subscribe,
      const StatusCallback &done) override;

 private:
  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<ClientID, ResourceChangeNotification, DynamicResourceTable>
      DynamicResourceSubscriptionExecutor;
  DynamicResourceSubscriptionExecutor resource_sub_executor_;

  typedef SubscriptionExecutor<ClientID, HeartbeatTableData, HeartbeatTable>
      HeartbeatSubscriptionExecutor;
  HeartbeatSubscriptionExecutor heartbeat_sub_executor_;

  typedef SubscriptionExecutor<ClientID, HeartbeatBatchTableData, HeartbeatBatchTable>
      HeartbeatBatchSubscriptionExecutor;
  HeartbeatBatchSubscriptionExecutor heartbeat_batch_sub_executor_;
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

 private:
  RedisGcsClient *client_impl_{nullptr};
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
      const SubscribeCallback<WorkerID, WorkerFailureData> &subscribe,
      const StatusCallback &done) override;

  Status AsyncReportWorkerFailure(const std::shared_ptr<WorkerFailureData> &data_ptr,
                                  const StatusCallback &callback) override;

  Status AsyncRegisterWorker(
      rpc::WorkerType worker_type, const WorkerID &worker_id,
      const std::unordered_map<std::string, std::string> &worker_info,
      const StatusCallback &callback) override;

 private:
  RedisGcsClient *client_impl_{nullptr};

  typedef SubscriptionExecutor<WorkerID, WorkerFailureData, WorkerFailureTable>
      WorkerFailureSubscriptionExecutor;
  WorkerFailureSubscriptionExecutor worker_failure_sub_executor_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_REDIS_ACCESSOR_H
