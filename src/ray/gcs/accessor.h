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

#ifndef RAY_GCS_ACCESSOR_H
#define RAY_GCS_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class ActorInfoAccessor
/// `ActorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// actor information in the GCS.
class ActorInfoAccessor {
 public:
  virtual ~ActorInfoAccessor() = default;

  /// Get all actor specification from GCS synchronously.
  ///
  /// \param actor_table_data_list The container to hold the actor specification.
  /// \return Status
  virtual Status GetAll(std::vector<rpc::ActorTableData> *actor_table_data_list) = 0;

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const ActorID &actor_id,
                          const OptionalItemCallback<rpc::ActorTableData> &callback) = 0;

  /// Create an actor to GCS asynchronously.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \return Status
  virtual Status AsyncCreateActor(const TaskSpecification &task_spec,
                                  const StatusCallback &callback) = 0;

  /// Register an actor to GCS asynchronously.
  ///
  /// \param data_ptr The actor that will be registered to the GCS.
  /// \param callback Callback that will be called after actor has been registered
  /// to the GCS.
  /// \return Status
  virtual Status AsyncRegister(const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                               const StatusCallback &callback) = 0;

  /// Update dynamic states of actor in GCS asynchronously.
  ///
  /// \param actor_id ID of the actor to update.
  /// \param data_ptr Data of the actor to update.
  /// \param callback Callback that will be called after update finishes.
  /// \return Status
  /// TODO(micafan) Don't expose the whole `ActorTableData` and only allow
  /// updating dynamic states.
  virtual Status AsyncUpdate(const ActorID &actor_id,
                             const std::shared_ptr<rpc::ActorTableData> &data_ptr,
                             const StatusCallback &callback) = 0;

  /// Subscribe to any register or update operations of actors.
  ///
  /// \param subscribe Callback that will be called each time when an actor is registered
  /// or updated.
  /// \param done Callback that will be called when subscription is complete and we
  /// are ready to receive notification.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Subscribe to any update operations of an actor.
  ///
  /// \param actor_id The ID of actor to be subscribed to.
  /// \param subscribe Callback that will be called each time when the actor is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to an actor.
  ///
  /// \param actor_id The ID of the actor to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribe(const ActorID &actor_id,
                                  const StatusCallback &done) = 0;

  /// Add actor checkpoint data to GCS asynchronously.
  ///
  /// \param data_ptr The checkpoint data that will be added to GCS.
  /// \param callback The callback that will be called after add finishes.
  /// \return Status
  /// TODO(micafan) When the GCS backend is redis,
  /// the checkpoint of the same actor needs to be updated serially,
  /// otherwise the checkpoint may be overwritten. This issue will be resolved if
  /// necessary.
  virtual Status AsyncAddCheckpoint(
      const std::shared_ptr<rpc::ActorCheckpointData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Get actor checkpoint data from GCS asynchronously.
  ///
  /// \param checkpoint_id The ID of checkpoint to lookup in GCS.
  /// \param actor_id The ID of actor that this checkpoint belongs to.
  /// \param callback The callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetCheckpoint(
      const ActorCheckpointID &checkpoint_id, const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointData> &callback) = 0;

  /// Get actor checkpoint id data from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to lookup in GCS.
  /// \param callback The callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetCheckpointID(
      const ActorID &actor_id,
      const OptionalItemCallback<rpc::ActorCheckpointIdData> &callback) = 0;

 protected:
  ActorInfoAccessor() = default;
};

/// \class JobInfoAccessor
/// `JobInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// job information in the GCS.
class JobInfoAccessor {
 public:
  virtual ~JobInfoAccessor() = default;

  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finished.
  /// \return Status
  virtual Status AsyncMarkFinished(const JobID &job_id,
                                   const StatusCallback &callback) = 0;

  /// Subscribe to finished jobs.
  ///
  /// \param subscribe Callback that will be called each time when a job finishes.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToFinishedJobs(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done) = 0;

 protected:
  JobInfoAccessor() = default;
};

/// \class TaskInfoAccessor
/// `TaskInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// task information in the GCS.
class TaskInfoAccessor {
 public:
  virtual ~TaskInfoAccessor() {}

  /// Add a task to GCS asynchronously.
  ///
  /// \param data_ptr The task that will be added to GCS.
  /// \param callback Callback that will be called after task has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::TaskTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Get task information from GCS asynchronously.
  ///
  /// \param task_id The ID of the task to look up in GCS.
  /// \param callback Callback that is called after lookup finished.
  /// \return Status
  virtual Status AsyncGet(const TaskID &task_id,
                          const OptionalItemCallback<rpc::TaskTableData> &callback) = 0;

  /// Delete tasks from GCS asynchronously.
  ///
  /// \param task_ids The vector of IDs to delete from GCS.
  /// \param callback Callback that is called after delete finished.
  /// \return Status
  // TODO(micafan) Will support callback of batch deletion in the future.
  // Currently this callback will never be called.
  virtual Status AsyncDelete(const std::vector<TaskID> &task_ids,
                             const StatusCallback &callback) = 0;

  /// Subscribe asynchronously to the event that the given task is added in GCS.
  ///
  /// \param task_id The ID of the task to be subscribed to.
  /// \param subscribe Callback that will be called each time when the task is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to a task asynchronously.
  ///
  /// \param task_id The ID of the task to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribe(const TaskID &task_id, const StatusCallback &done) = 0;

  /// Add a task lease to GCS asynchronously.
  ///
  /// \param data_ptr The task lease that will be added to GCS.
  /// \param callback Callback that will be called after task lease has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAddTaskLease(const std::shared_ptr<rpc::TaskLeaseData> &data_ptr,
                                   const StatusCallback &callback) = 0;

  /// Subscribe asynchronously to the event that the given task lease is added in GCS.
  ///
  /// \param task_id The ID of the task to be subscribed to.
  /// \param subscribe Callback that will be called each time when the task lease is
  /// updated or the task lease is empty currently.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeTaskLease(
      const TaskID &task_id,
      const SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to a task lease asynchronously.
  ///
  /// \param task_id The ID of the task to be unsubscribed to.
  /// \param done Callback that will be called when unsubscribe is complete.
  /// \return Status
  virtual Status AsyncUnsubscribeTaskLease(const TaskID &task_id,
                                           const StatusCallback &done) = 0;

  /// Attempt task reconstruction to GCS asynchronously.
  ///
  /// \param data_ptr The task reconstruction that will be added to GCS.
  /// \param callback Callback that will be called after task reconstruction
  /// has been added to GCS.
  /// \return Status
  virtual Status AttemptTaskReconstruction(
      const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) = 0;

 protected:
  TaskInfoAccessor() = default;
};

/// `ObjectInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// object information in the GCS.
class ObjectInfoAccessor {
 public:
  virtual ~ObjectInfoAccessor() {}

  /// Get object's locations from GCS asynchronously.
  ///
  /// \param object_id The ID of object to lookup in GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetLocations(
      const ObjectID &object_id,
      const MultiItemCallback<rpc::ObjectTableData> &callback) = 0;

  /// Add location of object to GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be added to GCS.
  /// \param node_id The location that will be added to GCS.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  virtual Status AsyncAddLocation(const ObjectID &object_id, const ClientID &node_id,
                                  const StatusCallback &callback) = 0;

  /// Remove location of object from GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be removed from GCS.
  /// \param node_id The location that will be removed from GCS.
  /// \param callback Callback that will be called after the delete finished.
  /// \return Status
  virtual Status AsyncRemoveLocation(const ObjectID &object_id, const ClientID &node_id,
                                     const StatusCallback &callback) = 0;

  /// Subscribe to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be subscribed to.
  /// \param subscribe Callback that will be called each time when the object's
  /// location is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToLocations(
      const ObjectID &object_id,
      const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be unsubscribed to.
  /// \param done Callback that will be called when unsubscription is complete.
  /// \return Status
  virtual Status AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                             const StatusCallback &done) = 0;

 protected:
  ObjectInfoAccessor() = default;
};

/// \class NodeInfoAccessor
/// `NodeInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// node information in the GCS.
class NodeInfoAccessor {
 public:
  virtual ~NodeInfoAccessor() = default;

  /// Register local node to GCS synchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \return Status
  virtual Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info) = 0;

  /// Cancel registration of local node to GCS synchronously.
  ///
  /// \return Status
  virtual Status UnregisterSelf() = 0;

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return ClientID
  virtual const ClientID &GetSelfId() const = 0;

  /// Get information of local node which was registered by 'RegisterSelf'.
  ///
  /// \return GcsNodeInfo
  virtual const rpc::GcsNodeInfo &GetSelfInfo() const = 0;

  /// Register a node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  /// \return Status
  virtual Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                               const StatusCallback &callback) = 0;

  /// Cancel registration of a node to GCS asynchronously.
  ///
  /// \param node_id The ID of node that to be unregistered.
  /// \param callback Callback that will be called when unregistration is complete.
  /// \return Status
  virtual Status AsyncUnregister(const ClientID &node_id,
                                 const StatusCallback &callback) = 0;

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback) = 0;

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done) = 0;

  /// Get node information from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \return The item returned by GCS. If the item to read doesn't exist,
  /// this optional object is empty.
  virtual boost::optional<rpc::GcsNodeInfo> Get(const ClientID &node_id) const = 0;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \return All nodes in cache.
  virtual const std::unordered_map<ClientID, rpc::GcsNodeInfo> &GetAll() const = 0;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  virtual bool IsRemoved(const ClientID &node_id) const = 0;

  // TODO(micafan) Define ResourceMap in GCS proto.
  typedef std::unordered_map<std::string, std::shared_ptr<rpc::ResourceTableData>>
      ResourceMap;

  /// Get node's resources from GCS asynchronously.
  ///
  /// \param node_id The ID of node to lookup dynamic resources.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetResources(const ClientID &node_id,
                                   const OptionalItemCallback<ResourceMap> &callback) = 0;

  /// Update resources of node in GCS asynchronously.
  ///
  /// \param node_id The ID of node to update dynamic resources.
  /// \param resources The dynamic resources of node to be updated.
  /// \param callback Callback that will be called after update finishes.
  virtual Status AsyncUpdateResources(const ClientID &node_id,
                                      const ResourceMap &resources,
                                      const StatusCallback &callback) = 0;

  /// Delete resources of a node from GCS asynchronously.
  ///
  /// \param node_id The ID of node to delete resources from GCS.
  /// \param resource_names The names of resource to be deleted.
  /// \param callback Callback that will be called after delete finishes.
  virtual Status AsyncDeleteResources(const ClientID &node_id,
                                      const std::vector<std::string> &resource_names,
                                      const StatusCallback &callback) = 0;

  /// Subscribe to node resource changes.
  ///
  /// \param subscribe Callback that will be called when any resource is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToResources(
      const SubscribeCallback<ClientID, ResourceChangeNotification> &subscribe,
      const StatusCallback &done) = 0;

  /// Report heartbeat of a node to GCS asynchronously.
  ///
  /// \param data_ptr The heartbeat that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  // TODO(micafan) NodeStateAccessor will call this method to report heartbeat.
  virtual Status AsyncReportHeartbeat(
      const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Subscribe to the heartbeat of each node from GCS.
  ///
  /// \param subscribe Callback that will be called each time when heartbeat is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeHeartbeat(
      const SubscribeCallback<ClientID, rpc::HeartbeatTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Report state of all nodes to GCS asynchronously.
  ///
  /// \param data_ptr The heartbeats that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  virtual Status AsyncReportBatchHeartbeat(
      const std::shared_ptr<rpc::HeartbeatBatchTableData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Subscribe batched state of all nodes from GCS.
  ///
  /// \param subscribe Callback that will be called each time when batch heartbeat is
  /// updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeBatchHeartbeat(
      const ItemCallback<rpc::HeartbeatBatchTableData> &subscribe,
      const StatusCallback &done) = 0;

 protected:
  NodeInfoAccessor() = default;
};

/// \class ErrorInfoAccessor
/// `ErrorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// error information in the GCS.
class ErrorInfoAccessor {
 public:
  virtual ~ErrorInfoAccessor() = default;

  /// Report a job error to GCS asynchronously.
  /// The error message will be pushed to the driver of a specific if it is
  /// a job internal error, or broadcast to all drivers if it is a system error.
  ///
  /// TODO(rkn): We need to make sure that the errors are unique because
  /// duplicate messages currently cause failures (the GCS doesn't allow it). A
  /// natural way to do this is to have finer-grained time stamps.
  ///
  /// \param data_ptr The error message that will be reported to GCS.
  /// \param callback Callback that will be called when report is complete.
  /// \return Status
  virtual Status AsyncReportJobError(const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
                                     const StatusCallback &callback) = 0;

 protected:
  ErrorInfoAccessor() = default;
};

/// \class StatsInfoAccessor
/// `StatsInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// stats in the GCS.
class StatsInfoAccessor {
 public:
  virtual ~StatsInfoAccessor() = default;

  /// Add profile data to GCS asynchronously.
  ///
  /// \param data_ptr The profile data that will be added to GCS.
  /// \param callback Callback that will be called when add is complete.
  /// \return Status
  virtual Status AsyncAddProfileData(
      const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
      const StatusCallback &callback) = 0;

 protected:
  StatsInfoAccessor() = default;
};

/// \class WorkerInfoAccessor
/// `WorkerInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// worker information in the GCS.
class WorkerInfoAccessor {
 public:
  virtual ~WorkerInfoAccessor() = default;

  /// Subscribe to all unexpected failure of workers from GCS asynchronously.
  /// Note that this does not include workers that failed due to node failure.
  ///
  /// \param subscribe Callback that will be called each time when a worker failed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToWorkerFailures(
      const SubscribeCallback<WorkerID, rpc::WorkerFailureData> &subscribe,
      const StatusCallback &done) = 0;

  /// Report a worker failure to GCS asynchronously.
  ///
  /// \param data_ptr The worker failure information that will be reported to GCS.
  /// \param callback Callback that will be called when report is complate.
  /// \param Status
  virtual Status AsyncReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerFailureData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Register a worker to GCS asynchronously.
  ///
  /// \param worker_type The type of the worker.
  /// \param worker_id The ID of the worker.
  /// \param worker_info The information of the worker.
  /// \return Status.
  virtual Status AsyncRegisterWorker(
      rpc::WorkerType worker_type, const WorkerID &worker_id,
      const std::unordered_map<std::string, std::string> &worker_info,
      const StatusCallback &callback) = 0;

 protected:
  WorkerInfoAccessor() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACCESSOR_H
