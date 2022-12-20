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

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/placement_group.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/rpc/client_call.h"
#include "ray/util/sequencer.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

using SubscribeOperation = std::function<Status(const StatusCallback &done)>;
using FetchDataOperation = std::function<void(const StatusCallback &done)>;

class GcsClient;

/// \class ActorInfoAccessor
/// `ActorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// actor information in the GCS.
class ActorInfoAccessor {
 public:
  ActorInfoAccessor() = default;
  explicit ActorInfoAccessor(GcsClient *client_impl);
  virtual ~ActorInfoAccessor() = default;
  /// Get actor specification from GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const ActorID &actor_id,
                          const OptionalItemCallback<rpc::ActorTableData> &callback);

  /// Get all actor specification from the GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::ActorTableData> &callback);

  /// Get actor specification for a named actor from the GCS asynchronously.
  ///
  /// \param name The name of the detached actor to look up in the GCS.
  /// \param ray_namespace The namespace to filter to.
  /// \param callback Callback that will be called after lookup finishes.
  /// \param timeout_ms RPC timeout in milliseconds. -1 means the default.
  /// \return Status
  virtual Status AsyncGetByName(const std::string &name,
                                const std::string &ray_namespace,
                                const OptionalItemCallback<rpc::ActorTableData> &callback,
                                int64_t timeout_ms = -1);

  /// Get actor specification for a named actor from the GCS synchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param name The name of the detached actor to look up in the GCS.
  /// \param ray_namespace The namespace to filter to.
  /// \return Status. TimedOut status if RPC is timed out.
  /// NotFound if the name doesn't exist.
  virtual Status SyncGetByName(const std::string &name,
                               const std::string &ray_namespace,
                               rpc::ActorTableData &actor_table_data,
                               rpc::TaskSpec &task_spec);

  /// List all named actors from the GCS asynchronously.
  ///
  /// \param all_namespaces Whether or not to include actors from all Ray namespaces.
  /// \param ray_namespace The namespace to filter to if all_namespaces is false.
  /// \param callback Callback that will be called after lookup finishes.
  /// \param timeout_ms The RPC timeout in milliseconds. -1 means the default.
  /// \return Status
  virtual Status AsyncListNamedActors(
      bool all_namespaces,
      const std::string &ray_namespace,
      const OptionalItemCallback<std::vector<rpc::NamedActorInfo>> &callback,
      int64_t timeout_ms = -1);

  /// List all named actors from the GCS synchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param all_namespaces Whether or not to include actors from all Ray namespaces.
  /// \param ray_namespace The namespace to filter to if all_namespaces is false.
  /// \param[out] actors The pair of list of named actors. Each pair includes the
  /// namespace and name of the actor. \return Status. TimeOut if RPC times out.
  virtual Status SyncListNamedActors(
      bool all_namespaces,
      const std::string &ray_namespace,
      std::vector<std::pair<std::string, std::string>> &actors);

  /// Register actor to GCS asynchronously.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \param callback Callback that will be called after the actor info is written to GCS.
  /// \param timeout_ms RPC timeout ms. -1 means there's no timeout.
  /// \return Status
  virtual Status AsyncRegisterActor(const TaskSpecification &task_spec,
                                    const StatusCallback &callback,
                                    int64_t timeout_ms = -1);

  /// Register actor to GCS synchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param task_spec The specification for the actor creation task.
  /// \return Status. Timedout if actor is not registered by the global
  /// GCS timeout.
  virtual Status SyncRegisterActor(const ray::TaskSpecification &task_spec);

  /// Kill actor via GCS asynchronously.
  ///
  /// \param actor_id The ID of actor to destroy.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  /// \param callback Callback that will be called after the actor is destroyed.
  /// \return Status
  virtual Status AsyncKillActor(const ActorID &actor_id,
                                bool force_kill,
                                bool no_restart,
                                const StatusCallback &callback);

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
  virtual Status AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback);

  /// Subscribe to any update operations of an actor.
  ///
  /// \param actor_id The ID of actor to be subscribed to.
  /// \param subscribe Callback that will be called each time when the actor is updated.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribe(
      const ActorID &actor_id,
      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
      const StatusCallback &done);

  /// Cancel subscription to an actor.
  ///
  /// \param actor_id The ID of the actor to be unsubscribed to.
  /// \return Status
  virtual Status AsyncUnsubscribe(const ActorID &actor_id);

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe();

  /// Check if the specified actor is unsubscribed.
  ///
  /// \param actor_id The ID of the actor.
  /// \return Whether the specified actor is unsubscribed.
  virtual bool IsActorUnsubscribed(const ActorID &actor_id);

 private:
  // Mutex to protect the resubscribe_operations_ field and fetch_data_operations_ field.
  absl::Mutex mutex_;

  /// Resubscribe operations for actors.
  absl::flat_hash_map<ActorID, SubscribeOperation> resubscribe_operations_
      GUARDED_BY(mutex_);

  /// Save the fetch data operation of actors.
  absl::flat_hash_map<ActorID, FetchDataOperation> fetch_data_operations_
      GUARDED_BY(mutex_);

  GcsClient *client_impl_;
};

/// \class JobInfoAccessor
/// `JobInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// job information in the GCS.
class JobInfoAccessor {
 public:
  JobInfoAccessor() = default;
  explicit JobInfoAccessor(GcsClient *client_impl);
  virtual ~JobInfoAccessor() = default;
  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                          const StatusCallback &callback);

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finished.
  /// \return Status
  virtual Status AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback);

  /// Subscribe to job updates.
  ///
  /// \param subscribe Callback that will be called each time when a job updates.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done);

  /// Get all job info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::JobTableData> &callback);

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe();

  /// Increment and get next job id. This is not idempotent.
  ///
  /// \param done Callback that will be called when request successfully.
  /// \return Status
  virtual Status AsyncGetNextJobID(const ItemCallback<JobID> &callback);

 private:
  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_all_data_operation_;

  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_operation_;

  GcsClient *client_impl_;
};

/// \class NodeInfoAccessor
/// `NodeInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// node information in the GCS.
class NodeInfoAccessor {
 public:
  NodeInfoAccessor() = default;
  explicit NodeInfoAccessor(GcsClient *client_impl);
  virtual ~NodeInfoAccessor() = default;
  /// Register local node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  /// \return Status
  virtual Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                              const StatusCallback &callback);

  /// Drain (remove the information of the node from the cluster) the local node from GCS
  /// synchronously.
  ///
  /// Once the RPC is replied, it is guaranteed that GCS drains the information of the
  /// local node, and all the nodes in the cluster will "eventually" be informed that the
  /// node is drained. \return Status
  virtual Status DrainSelf();

  /// Get id of local node which was registered by 'RegisterSelf'.
  ///
  /// \return NodeID
  virtual const NodeID &GetSelfId() const;

  /// Get information of local node which was registered by 'RegisterSelf'.
  ///
  /// \return GcsNodeInfo
  virtual const rpc::GcsNodeInfo &GetSelfInfo() const;

  /// Register a node to GCS asynchronously.
  ///
  /// \param node_info The information of node to register to GCS.
  /// \param callback Callback that will be called when registration is complete.
  /// \return Status
  virtual Status AsyncRegister(const rpc::GcsNodeInfo &node_info,
                               const StatusCallback &callback);

  /// Drain (remove the information of the node from the cluster) the local node from GCS
  /// asynchronously.
  ///
  /// Check gcs_service.proto NodeInfoGcsService.DrainNode for the API spec.
  ///
  /// \param node_id The ID of node that to be unregistered.
  /// \param callback Callback that will be called when unregistration is complete.
  /// \return Status
  virtual Status AsyncDrainNode(const NodeID &node_id, const StatusCallback &callback);

  /// Get information of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback);

  /// Subscribe to node addition and removal events from GCS and cache those information.
  ///
  /// \param subscribe Callback that will be called if a node is
  /// added or a node is removed. The callback needs to be idempotent because it will also
  /// be called for existing nodes.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToNodeChange(
      const SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe,
      const StatusCallback &done);

  /// Get node information from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The ID of node to look up in local cache.
  /// \param filter_dead_nodes Whether or not if this method will filter dead nodes.
  /// \return The item returned by GCS. If the item to read doesn't exist or the node is
  virtual  /// dead, this optional object is empty.
      const rpc::GcsNodeInfo *
      Get(const NodeID &node_id, bool filter_dead_nodes = true) const;

  /// Get information of all nodes from local cache.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \return All nodes in cache.
  virtual const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &GetAll() const;

  /// Search the local cache to find out if the given node is removed.
  /// Non-thread safe.
  /// Note, the local cache is only available if `AsyncSubscribeToNodeChange`
  /// is called before.
  ///
  /// \param node_id The id of the node to check.
  /// \return Whether the node is removed.
  virtual bool IsRemoved(const NodeID &node_id) const;

  /// Report heartbeat of a node to GCS asynchronously.
  ///
  /// \param data_ptr The heartbeat that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  virtual  // TODO(micafan) NodeStateAccessor will call this method to report heartbeat.
      Status
      AsyncReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> &data_ptr,
                           const StatusCallback &callback);

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe();

  /// Get the internal config string from GCS.
  ///
  /// \param callback Processes a map of config options
  /// \return Status
  virtual Status AsyncGetInternalConfig(
      const OptionalItemCallback<std::string> &callback);

  /// Add a node to accessor cache.
  virtual void HandleNotification(const rpc::GcsNodeInfo &node_info);

 private:
  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_node_operation_;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_node_data_operation_;

  GcsClient *client_impl_;

  using NodeChangeCallback =
      std::function<void(const NodeID &id, const rpc::GcsNodeInfo &node_info)>;

  rpc::GcsNodeInfo local_node_info_;
  NodeID local_node_id_;

  /// The callback to call when a new node is added or a node is removed.
  NodeChangeCallback node_change_callback_{nullptr};

  /// A cache for information about all nodes.
  absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> node_cache_;
  /// The set of removed nodes.
  std::unordered_set<NodeID> removed_nodes_;
};

/// \class NodeResourceInfoAccessor
/// `NodeResourceInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// node resource information in the GCS.
class NodeResourceInfoAccessor {
 public:
  NodeResourceInfoAccessor() = default;
  explicit NodeResourceInfoAccessor(GcsClient *client_impl);
  virtual ~NodeResourceInfoAccessor() = default;
  // TODO(micafan) Define ResourceMap in GCS proto.
  typedef absl::flat_hash_map<std::string, std::shared_ptr<rpc::ResourceTableData>>
      ResourceMap;

  /// Get node's resources from GCS asynchronously.
  ///
  /// \param node_id The ID of node to lookup dynamic resources.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetResources(const NodeID &node_id,
                                   const OptionalItemCallback<ResourceMap> &callback);

  /// Get available resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback);

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe();

  /// Report resource usage of a node to GCS asynchronously.
  ///
  /// \param data_ptr The data that will be reported to GCS.
  /// \param callback Callback that will be called after report finishes.
  /// \return Status
  virtual Status AsyncReportResourceUsage(
      const std::shared_ptr<rpc::ResourcesData> &data_ptr,
      const StatusCallback &callback);

  /// Resend resource usage when GCS restarts from a failure.
  virtual void AsyncReReportResourceUsage();

  /// Return resources in last report. Used by light heartbeat.
  virtual const std::shared_ptr<NodeResources> &GetLastResourceUsage() {
    return last_resource_usage_;
  }

  /// Get newest resource usage of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback);

  /// Fill resource fields with cached resources. Used by light resource usage report.
  virtual void FillResourceUsageRequest(rpc::ReportResourceUsageRequest &resource_usage);

 protected:
  /// Cache which stores resource usage in last report used to check if they are changed.
  /// Used by light resource usage report.
  std::shared_ptr<NodeResources> last_resource_usage_ = std::make_shared<NodeResources>();

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

  GcsClient *client_impl_;

  Sequencer<NodeID> sequencer_;
};

/// \class ErrorInfoAccessor
/// `ErrorInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// error information in the GCS.
class ErrorInfoAccessor {
 public:
  ErrorInfoAccessor() = default;
  explicit ErrorInfoAccessor(GcsClient *client_impl);
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
                                     const StatusCallback &callback);

 private:
  GcsClient *client_impl_;
};

/// \class TaskInfoAccessor
/// `TaskInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// task info in the GCS.
class TaskInfoAccessor {
 public:
  TaskInfoAccessor() = default;
  explicit TaskInfoAccessor(GcsClient *client_impl) : client_impl_(client_impl) {}
  virtual ~TaskInfoAccessor() = default;
  /// Add task event data to GCS asynchronously.
  ///
  /// \param data_ptr The task states event data that will be added to GCS.
  /// \param callback Callback that will be called when add is complete.
  /// \return Status
  virtual Status AsyncAddTaskEventData(std::unique_ptr<rpc::TaskEventData> data_ptr,
                                       StatusCallback callback);

  /// Get all info/events of all tasks stored in GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGetTaskEvents(const MultiItemCallback<rpc::TaskEvents> &callback);

 private:
  GcsClient *client_impl_;
};

/// \class StatsInfoAccessor
/// `StatsInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// stats in the GCS.
class StatsInfoAccessor {
 public:
  StatsInfoAccessor() = default;
  explicit StatsInfoAccessor(GcsClient *client_impl);
  virtual ~StatsInfoAccessor() = default;
  /// Add profile data to GCS asynchronously.
  ///
  /// \param data_ptr The profile data that will be added to GCS.
  /// \param callback Callback that will be called when add is complete.
  /// \return Status
  virtual Status AsyncAddProfileData(
      const std::shared_ptr<rpc::ProfileTableData> &data_ptr,
      const StatusCallback &callback);

  /// Get all profile info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::ProfileTableData> &callback);

 private:
  GcsClient *client_impl_;
};

/// \class WorkerInfoAccessor
/// `WorkerInfoAccessor` is a sub-interface of `GcsClient`.
/// This class includes all the methods that are related to accessing
/// worker information in the GCS.
class WorkerInfoAccessor {
 public:
  WorkerInfoAccessor() = default;
  explicit WorkerInfoAccessor(GcsClient *client_impl);
  virtual ~WorkerInfoAccessor() = default;
  /// Subscribe to all unexpected failure of workers from GCS asynchronously.
  /// Note that this does not include workers that failed due to node failure
  /// and only fileds in WorkerDeltaData would be published.
  ///
  /// \param subscribe Callback that will be called each time when a worker failed.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeToWorkerFailures(
      const ItemCallback<rpc::WorkerDeltaData> &subscribe, const StatusCallback &done);

  /// Report a worker failure to GCS asynchronously.
  ///
  /// \param data_ptr The worker failure information that will be reported to GCS.
  /// \param callback Callback that will be called when report is complate.
  /// \param Status
  virtual Status AsyncReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
      const StatusCallback &callback);

  /// Get worker specification from GCS asynchronously.
  ///
  /// \param worker_id The ID of worker to look up in the GCS.
  /// \param callback Callback that will be called after lookup finishes.
  /// \return Status
  virtual Status AsyncGet(const WorkerID &worker_id,
                          const OptionalItemCallback<rpc::WorkerTableData> &callback);

  /// Get all worker info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(const MultiItemCallback<rpc::WorkerTableData> &callback);

  /// Add worker information to GCS asynchronously.
  ///
  /// \param data_ptr The worker that will be add to GCS.
  /// \param callback Callback that will be called after worker information has been added
  /// to GCS.
  /// \return Status
  virtual Status AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                          const StatusCallback &callback);

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe();

 private:
  /// Save the subscribe operation in this function, so we can call it again when GCS
  /// restarts from a failure.
  SubscribeOperation subscribe_operation_;

  GcsClient *client_impl_;
};

class PlacementGroupInfoAccessor {
 public:
  PlacementGroupInfoAccessor() = default;
  explicit PlacementGroupInfoAccessor(GcsClient *client_impl);
  virtual ~PlacementGroupInfoAccessor() = default;

  /// Create a placement group to GCS synchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param placement_group_spec The specification for the placement group creation task.
  /// \return Status. The status of the RPC. TimedOut if the RPC times out. Invalid if the
  /// same name placement group is registered. NotFound if the placement group is removed.
  virtual Status SyncCreatePlacementGroup(
      const ray::PlacementGroupSpecification &placement_group_spec);

  /// Get a placement group data from GCS asynchronously by id.
  ///
  /// \param placement_group_id The id of a placement group to obtain from GCS.
  /// \return Status.
  virtual Status AsyncGet(
      const PlacementGroupID &placement_group_id,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback);

  /// Get a placement group data from GCS asynchronously by name.
  ///
  /// \param placement_group_name The name of a placement group to obtain from GCS.
  /// \param ray_namespace The ray namespace.
  /// \param callback The callback that's called when the RPC is replied.
  /// \param timeout_ms The RPC timeout in milliseconds. -1 means the default.
  /// \return Status.
  virtual Status AsyncGetByName(
      const std::string &placement_group_name,
      const std::string &ray_namespace,
      const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
      int64_t timeout_ms = -1);

  /// Get all placement group info from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finished.
  /// \return Status
  virtual Status AsyncGetAll(
      const MultiItemCallback<rpc::PlacementGroupTableData> &callback);

  /// Remove a placement group to GCS synchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param placement_group_id The id for the placement group to remove.
  /// \return Status
  virtual Status SyncRemovePlacementGroup(const PlacementGroupID &placement_group_id);

  /// Wait for a placement group until ready asynchronously.
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param placement_group_id The id for the placement group to wait for until ready.
  /// \param timeout_seconds The timeout in seconds.
  /// \return Status. TimedOut if the RPC times out. NotFound if the placement has already
  /// removed.
  virtual Status SyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                                    int64_t timeout_seconds);

 private:
  GcsClient *client_impl_;
};

class InternalKVAccessor {
 public:
  InternalKVAccessor() = default;
  explicit InternalKVAccessor(GcsClient *client_impl);
  virtual ~InternalKVAccessor() = default;
  /// Asynchronously list keys with prefix stored in internal kv
  ///
  /// \param ns The namespace to scan.
  /// \param prefix The prefix to scan.
  /// \param callback Callback that will be called after scanning.
  /// \return Status
  virtual Status AsyncInternalKVKeys(
      const std::string &ns,
      const std::string &prefix,
      const OptionalItemCallback<std::vector<std::string>> &callback);

  /// Asynchronously get the value for a given key.
  ///
  /// \param ns The namespace to lookup.
  /// \param key The key to lookup.
  /// \param callback Callback that will be called after get the value.
  virtual Status AsyncInternalKVGet(const std::string &ns,
                                    const std::string &key,
                                    const OptionalItemCallback<std::string> &callback);

  /// Asynchronously set the value for a given key.
  ///
  /// \param ns The namespace to put the key.
  /// \param key The key in <key, value> pair
  /// \param value The value associated with the key
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVPut(const std::string &ns,
                                    const std::string &key,
                                    const std::string &value,
                                    bool overwrite,
                                    const OptionalItemCallback<int> &callback);

  /// Asynchronously check the existence of a given key
  ///
  /// \param ns The namespace to check.
  /// \param key The key to check.
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVExists(const std::string &ns,
                                       const std::string &key,
                                       const OptionalItemCallback<bool> &callback);

  /// Asynchronously delete a key
  ///
  /// \param ns The namespace to delete from.
  /// \param key The key to delete.
  /// \param del_by_prefix If set to be true, delete all keys with prefix as `key`.
  /// \param callback Callback that will be called after the operation.
  /// \return Status
  virtual Status AsyncInternalKVDel(const std::string &ns,
                                    const std::string &key,
                                    bool del_by_prefix,
                                    const StatusCallback &callback);

  // These are sync functions of the async above

  /// List keys with prefix stored in internal kv
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param ns The namespace to scan.
  /// \param prefix The prefix to scan.
  /// \param value It's an output parameter. It'll be set to the keys with `prefix`
  /// \return Status
  virtual Status Keys(const std::string &ns,
                      const std::string &prefix,
                      std::vector<std::string> &value);

  /// Set the <key, value> in the store
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param ns The namespace to put the key.
  /// \param key The key of the pair
  /// \param value The value of the pair
  /// \param overwrite If it's true, it'll overwrite existing <key, value> if it
  ///     exists.
  /// \param added It's an output parameter. It'll be set to be true if
  ///     any row is added.
  /// \return Status
  virtual Status Put(const std::string &ns,
                     const std::string &key,
                     const std::string &value,
                     bool overwrite,
                     bool &added);

  /// Retrive the value associated with a key
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param ns The namespace to lookup.
  /// \param key The key to lookup
  /// \param value It's an output parameter. It'll be set to the value of the key
  /// \return Status
  virtual Status Get(const std::string &ns, const std::string &key, std::string &value);

  /// Delete the key
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param ns The namespace to delete from.
  /// \param key The key to delete
  /// \param del_by_prefix If set to be true, delete all keys with prefix as `key`.
  /// \return Status
  virtual Status Del(const std::string &ns, const std::string &key, bool del_by_prefix);

  /// Check existence of a key in the store
  ///
  /// The RPC will timeout after the default GCS RPC timeout is exceeded.
  ///
  /// \param ns The namespace to check.
  /// \param key The key to check
  /// \param exist It's an output parameter. It'll be true if the key exists in the
  ///    system. Otherwise, it'll be set to be false.
  /// \return Status
  virtual Status Exists(const std::string &ns, const std::string &key, bool &exist);

 private:
  GcsClient *client_impl_;
};

}  // namespace gcs

}  // namespace ray
