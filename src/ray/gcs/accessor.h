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
#include "ray/common/placement_group.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class ActorInfoAccessor
/// `ActorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// actor information in the GCS.
class ActorInfoAccessor {
 public:
  virtual ~ActorInfoAccessor() = default;

  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const ActorID &actor_id,
                          const OptionalItemCallback<rpc::ActorTableData> &callback) = 0;

  /// Get all actor specification from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback) = 0;

  /// Get actor specification for a named actor from GCS asynchronously.
  ///
  /// \param name The name of the detached actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetByName(
      const std::string &name, const std::string &ray_namespace,
      const OptionalItemCallback<rpc::ActorTableData> &callback) = 0;

  /// Register actor to GCS asynchronously.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \return Status
  virtual Status AsyncRegisterActor(const TaskSpecification &task_spec,
                                    const StatusCallback &callback) = 0;

  /// Kill actor via GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to destroy.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  /// \param callback Callback that will be called after the actor is destroyed.
  /// \return Status
  virtual Status AsyncKillActor(const ActorID &actor_id, bool force_kill, bool no_restart,
                                const StatusCallback &callback) = 0;

  /// Asynchronously request GCS to create the actor.
  ///
  /// This should be called after the worker has resolved the actor dependencies.
  /// TODO(...): Currently this request will only reply after the actor is created.
  /// We should change it to reply immediately after GCS has persisted the actor
  /// dependencies in storage.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \return Status
  virtual Status AsyncCreateActor(const TaskSpecification &task_spec,
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
  /// \return Status
  virtual Status AsyncUnsubscribe(const ActorID &actor_id) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

  /// Check if the specified actor is unsubscribed.
  ///
  /// \param actor_id The ID of the actor.
  /// \return Whether the specified actor is unsubscribed.
  virtual bool IsActorUnsubscribed(const ActorID &actor_id) = 0;

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

  /// Subscribe to job updates.
  ///
  /// \param subscribe Callback that will be called each time when a job updates.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Get all job info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::JobTableData> &callback) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

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

  /// Add a task lease to GCS asynchronously.
  ///
  /// \param data_ptr The task lease that will be added to GCS.
  /// \param callback Callback that will be called after task lease has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAddTaskLease(const std::shared_ptr<rpc::TaskLeaseData> &data_ptr,
                                   const StatusCallback &callback) = 0;

  /// Get task lease information from GCS asynchronously.
  ///
  /// \param task_id The ID of the task to look up in GCS.
  /// \param callback Callback that is called after lookup finished.
  /// \return Status
  virtual Status AsyncGetTaskLease(
      const TaskID &task_id,
      const OptionalItemCallback<rpc::TaskLeaseData> &callback) = 0;

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
  /// \return Status
  virtual Status AsyncUnsubscribeTaskLease(const TaskID &task_id) = 0;

  /// Attempt task reconstruction to GCS asynchronously.
  ///
  /// \param data_ptr The task reconstruction that will be added to GCS.
  /// \param callback Callback that will be called after task reconstruction
  /// has been added to GCS.
  /// \return Status
  virtual Status AttemptTaskReconstruction(
      const std::shared_ptr<rpc::TaskReconstructionData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

  /// Check if the specified task lease is unsubscribed.
  ///
  /// \param task_id The ID of the task.
  /// \return Whether the specified task lease is unsubscribed.
  virtual bool IsTaskLeaseUnsubscribed(const TaskID &task_id) = 0;

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
      const OptionalItemCallback<rpc::ObjectLocationInfo> &callback) = 0;

  /// Get all object's locations from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(
      const MultiItemCallback<rpc::ObjectLocationInfo> &callback) = 0;

  /// Add location of object to GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be added to GCS.
  /// \param node_id The location that will be added to GCS.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  virtual Status AsyncAddLocation(const ObjectID &object_id, const NodeID &node_id,
                                  size_t object_size, const StatusCallback &callback) = 0;

  /// Add spilled location of object to GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be added to GCS.
  /// \param spilled_url The URL where the object has been spilled.
  /// \param spilled_node_id The NodeID where the object has been spilled.
  /// \param callback Callback that will be called after object has been added to GCS.
  /// \return Status
  virtual Status AsyncAddSpilledUrl(const ObjectID &object_id,
                                    const std::string &spilled_url,
                                    const NodeID &spilled_node_id, size_t object_size,
                                    const StatusCallback &callback) = 0;

  /// Remove location of object from GCS asynchronously.
  ///
  /// \param object_id The ID of object which location will be removed from GCS.
  /// \param node_id The location that will be removed from GCS.
  /// \param callback Callback that will be called after the delete finished.
  /// \return Status
  virtual Status AsyncRemoveLocation(const ObjectID &object_id, const NodeID &node_id,
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
      const SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
          &subscribe,
      const StatusCallback &done) = 0;

  /// Cancel subscription to any update of an object's location.
  ///
  /// \param object_id The ID of the object to be unsubscribed to.
  /// \return Status
  virtual Status AsyncUnsubscribeToLocations(const ObjectID &object_id) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

  /// Check if the specified object is unsubscribed.
  ///
  /// \param object_id The ID of the object.
  /// \return Whether the specified object is unsubscribed.
  virtual bool IsObjectUnsubscribed(const ObjectID &object_id) = 0;

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

  /// Register local node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  /// \return Status
  virtual Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                              const StatusCallback &callback) = 0;

  /// Cancel registration of local node to GCS synchronously.
  ///
  /// \return Status
  virtual Status UnregisterSelf() = 0;

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return NodeID
  virtual const NodeID &GetSelfId() const = 0;

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
  virtual Status AsyncUnregister(const NodeID &node_id,
                                 const StatusCallback &callback) = 0;

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback) = 0;

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed. The callback needs to be idempotent because it will also
  /// be called for existing nodes.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done) = 0;

  /// Get node information from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \param filter_dead_nodes Whether or not if this method will filter dead nodes.
  /// \return The item returned by GCS. If the item to read doesn't exist or the node is
  /// dead, this optional object is empty.
  virtual boost::optional<rpc::GcsNodeInfo> Get(const NodeID &node_id,
                                                bool filter_dead_nodes = true) const = 0;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \return All nodes in cache.
  virtual const std::unordered_map<NodeID, rpc::GcsNodeInfo> &GetAll() const = 0;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  virtual bool IsRemoved(const NodeID &node_id) const = 0;

  /// Report heartbeat of a node to GCS asynchronously.
  ///
  /// \param data_ptr The heartbeat that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  // TODO(micafan) NodeStateAccessor will call this method to report heartbeat.
  virtual Status AsyncReportHeartbeat(
      const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

  /// Get the internal config string from GCS.
  ///
  /// \param callback Processes a map of config options
  /// \return Status
  virtual Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::string> &callback) = 0;

 protected:
  NodeInfoAccessor() = default;
};

/// \class NodeResourceInfoAccessor
/// `NodeResourceInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// node resource information in the GCS.
class NodeResourceInfoAccessor {
 public:
  virtual ~NodeResourceInfoAccessor() = default;

  // TODO(micafan) Define ResourceMap in GCS proto.
  typedef std::unordered_map<std::string, std::shared_ptr<rpc::ResourceTableData>>
      ResourceMap;

  /// Get node's resources from GCS asynchronously.
  ///
  /// \param node_id The ID of node to lookup dynamic resources.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetResources(const NodeID &node_id,
                                   const OptionalItemCallback<ResourceMap> &callback) = 0;

  /// Get available resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback) = 0;

  /// Update resources of node in GCS asynchronously.
  ///
  /// \param node_id The ID of node to update dynamic resources.
  /// \param resources The dynamic resources of node to be updated.
  /// \param callback Callback that will be called after update finishes.
  virtual Status AsyncUpdateResources(const NodeID &node_id, const ResourceMap &resources,
                                      const StatusCallback &callback) = 0;

  /// Delete resources of a node from GCS asynchronously.
  ///
  /// \param node_id The ID of node to delete resources from GCS.
  /// \param resource_names The names of resource to be deleted.
  /// \param callback Callback that will be called after delete finishes.
  virtual Status AsyncDeleteResources(const NodeID &node_id,
                                      const std::vector<std::string> &resource_names,
                                      const StatusCallback &callback) = 0;

  /// Subscribe to node resource changes.
  ///
  /// \param subscribe Callback that will be called when any resource is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToResources(
      const ItemCallback<rpc::NodeResourceChange> &subscribe,
      const StatusCallback &done) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

  /// Report resource usage of a node to GCS asynchronously.
  ///
  /// \param data_ptr The data that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  virtual Status AsyncReportResourceUsage(
      const std::shared_ptr<rpc::ResourcesData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Resend resource usage when GCS restarts from a failure.
  virtual void AsyncReReportResourceUsage() = 0;

  /// Return resources in last report. Used by light heartbeat.
  const std::shared_ptr<SchedulingResources> &GetLastResourceUsage() {
    return last_resource_usage_;
  }

  /// Get newest resource usage of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback) = 0;

  /// Subscribe batched state of all nodes from GCS.
  ///
  /// \param subscribe Callback that will be called each time when batch resource usage is
  /// updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeBatchedResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &subscribe,
      const StatusCallback &done) = 0;

 protected:
  NodeResourceInfoAccessor() = default;

  /// Cache which stores resource usage in last report used to check if they are changed.
  /// Used by light resource usage report.
  std::shared_ptr<SchedulingResources> last_resource_usage_ =
      std::make_shared<SchedulingResources>();
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

  /// Get all profile info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(
      const MultiItemCallback<rpc::ProfileTableData> &callback) = 0;

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
  /// Note that this does not include workers that failed due to node failure
  /// and only fileds in WorkerDeltaData would be published.
  ///
  /// \param subscribe Callback that will be called each time when a worker failed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToWorkerFailures(
      const ItemCallback<rpc::WorkerDeltaData> &subscribe,
      const StatusCallback &done) = 0;

  /// Report a worker failure to GCS asynchronously.
  ///
  /// \param data_ptr The worker failure information that will be reported to GCS.
  /// \param callback Callback that will be called when report is complate.
  /// \param Status
  virtual Status AsyncReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
      const StatusCallback &callback) = 0;

  /// Get worker specification from GCS asynchronously.
  ///
  /// \param worker_id The ID of worker to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const WorkerID &worker_id,
                          const OptionalItemCallback<rpc::WorkerTableData> &callback) = 0;

  /// Get all worker info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::WorkerTableData> &callback) = 0;

  /// Add worker information to GCS asynchronously.
  ///
  /// \param data_ptr The worker that will be add to GCS.
  /// \param callback Callback that will be called after worker information has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                          const StatusCallback &callback) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  ///
  /// \param is_pubsub_server_restarted Whether pubsub server is restarted.
  virtual void AsyncResubscribe(bool is_pubsub_server_restarted) = 0;

 protected:
  WorkerInfoAccessor() = default;
};

class PlacementGroupInfoAccessor {
 public:
  virtual ~PlacementGroupInfoAccessor() = default;

  /// Create a placement group to GCS asynchronously.
  ///
  /// \param placement_group_spec The specification for the placement group creation task.
  /// \param callback Callback that will be called after the placement group info is
  /// written to GCS.
  /// \return Status.
  virtual Status AsyncCreatePlacementGroup(
      const PlacementGroupSpecification &placement_group_spec,
      const StatusCallback &callback) = 0;

  /// Get a placement group data from GCS asynchronously by id.
  ///
  /// \param placement_group_id The id of a placement group to obtain from GCS.
  /// \return Status.
  virtual Status AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) = 0;

  /// Get a placement group data from GCS asynchronously by name.
  ///
  /// \param placement_group_name The name of a placement group to obtain from GCS.
  /// \return Status.
  virtual Status AsyncGetByName(
      const std::string &placement_group_name,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) = 0;

  /// Get all placement group info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback) = 0;

  /// Remove a placement group to GCS asynchronously.
  ///
  /// \param placement_group_id The id for the placement group to remove.
  /// \param callback Callback that will be called after the placement group is
  /// removed from GCS.
  /// \return Status
  virtual Status AsyncRemovePlacementGroup(const PlacementGroupID &placement_group_id,
                                           const StatusCallback &callback) = 0;

  /// Wait for a placement group until ready asynchronously.
  ///
  /// \param placement_group_id The id for the placement group to wait for until ready.
  /// \param callback Callback that will be called after the placement group is created.
  /// \return Status
  virtual Status AsyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                                     const StatusCallback &callback) = 0;

 protected:
  PlacementGroupInfoAccessor() = default;
};

class InternalKVAccessor {
 public:
  virtual ~InternalKVAccessor() = default;

  /// Asynchronously list keys with prefix stored in internal kv
  ///
  /// \param prefix The prefix to scan.
  /// \param callback Callback that will be called after scanning.
  /// \return Status
  virtual Status AsyncInternalKVKeys(
      const std::string &prefix,
      const OptionalItemCallback<std::vector<std::string>> &callback) = 0;

  /// Asynchronously get the value for a given key.
  ///
  /// \param key The key to lookup.
  /// \param callback Callback that will be called after get the value.
  virtual Status AsyncInternalKVGet(
      const std::string &key, const OptionalItemCallback<std::string> &callback) = 0;

  /// Asynchronously set the value for a given key.
  ///
  /// \param key The key in <key, value> pair
  /// \param value The value associated with the key
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVPut(const std::string &key, const std::string &value,
                                    bool overwrite,
                                    const OptionalItemCallback<int> &callback) = 0;

  /// Asynchronously check the existence of a given key
  ///
  /// \param key The key to check
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVExists(const std::string &key,
                                       const OptionalItemCallback<bool> &callback) = 0;

  /// Asynchronously delete a key
  ///
  /// \param key The key to delete
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVDel(const std::string &key,
                                    const StatusCallback &callback) = 0;

  // These are sync functions of the async above

  /// List keys with prefix stored in internal kv
  ///
  /// \param prefix The prefix to scan.
  /// \param value It's an output parameter. It'll be set to the keys with `prefix`
  /// \return Status
  Status Keys(const std::string &prefix, std::vector<std::string> &value);

  /// Set the <key, value> in the store
  ///
  /// \param key The key of the pair
  /// \param value The value of the pair
  /// \param overwrite If it's true, it'll overwrite existing <key, value> if it
  ///     exists.
  /// \param added It's an output parameter. It'll be set to be true if
  ///     any row is added.
  /// \return Status
  Status Put(const std::string &key, const std::string &value, bool overwrite,
             bool &added);

  /// Retrive the value associated with a key
  ///
  /// \param key The key to lookup
  /// \param value It's an output parameter. It'll be set to the value of the key
  /// \return Status
  Status Get(const std::string &key, std::string &value);

  /// Delete the key
  ///
  /// \param key The key to delete
  /// \return Status
  Status Del(const std::string &key);

  /// Check existence of a key in the store
  ///
  /// \param key The key to check
  /// \param exist It's an output parameter. It'll be true if the key exists in the
  ///    system. Otherwise, it'll be set to be false.
  /// \return Status
  Status Exists(const std::string &key, bool &exist);

 protected:
  InternalKVAccessor() = default;
};

}  // namespace gcs

}  // namespace ray
