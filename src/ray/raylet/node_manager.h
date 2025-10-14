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

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease.h"
#include "ray/common/memory_monitor.h"
#include "ray/common/ray_object.h"
#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/common/scheduling/resource_set.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/experimental_mutable_object_provider.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/flatbuffers/node_manager_generated.h"
#include "ray/object_manager/object_directory.h"
#include "ray/object_manager/object_manager.h"
#include "ray/object_manager/plasma/client.h"
#include "ray/pubsub/subscriber.h"
#include "ray/raylet/agent_manager.h"
#include "ray/raylet/lease_dependency_manager.h"
#include "ray/raylet/local_lease_manager.h"
#include "ray/raylet/local_object_manager_interface.h"
#include "ray/raylet/placement_group_resource_manager.h"
#include "ray/raylet/runtime_env_agent_client.h"
#include "ray/raylet/scheduling/cluster_lease_manager_interface.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/wait_manager.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/raylet/worker_pool.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/rpc/node_manager/node_manager_server.h"
#include "ray/rpc/rpc_callback_types.h"
#include "ray/util/throttler.h"

namespace ray::raylet {

using rpc::ErrorType;
using rpc::GcsNodeInfo;
using rpc::JobTableData;
using rpc::ResourceUsageBatchData;

// TODO(#54703): Put this type in a separate target.
using AddProcessToCgroupHook = std::function<void(const std::string &)>;

struct NodeManagerConfig {
  /// The node's resource configuration.
  ResourceSet resource_config;
  /// The IP address this node manager is running on.
  std::string node_manager_address;
  /// The port to use for listening to incoming connections. If this is 0 then
  /// the node manager will choose its own port.
  int node_manager_port;
  /// The port to connect the runtime env agent. Note the address is equal to the
  /// node manager address.
  int runtime_env_agent_port;
  /// The lowest port number that workers started will bind on.
  /// If this is set to 0, workers will bind on random ports.
  int min_worker_port;
  /// The highest port number that workers started will bind on.
  /// If this is not set to 0, min_worker_port must also not be set to 0.
  int max_worker_port;
  /// An explicit list of open ports that workers started will bind
  /// on. This takes precedence over min_worker_port and max_worker_port.
  std::vector<int> worker_ports;
  /// The soft limit of the number of workers.
  int64_t num_workers_soft_limit;
  /// Number of initial Python workers to start when raylet starts.
  int num_prestart_python_workers;
  /// The maximum number of workers that can be started concurrently by a
  /// worker pool.
  int maximum_startup_concurrency;
  /// The commands used to start the worker process, grouped by language.
  WorkerCommandMap worker_commands;
  /// The native library path which includes the core libraries.
  std::string native_library_path;
  /// The command used to start the dashboard agent. Must not be empty.
  std::string dashboard_agent_command;
  /// The command used to start the runtime env agent. Must not be empty.
  std::string runtime_env_agent_command;
  /// The time between reports resources in milliseconds.
  uint64_t report_resources_period_ms;
  /// The store socket name.
  std::string store_socket_name;
  /// The path of this ray log dir.
  std::string log_dir;
  /// The path of this ray session dir.
  std::string session_dir;
  /// The path of this ray resource dir.
  std::string resource_dir;
  /// If true make Ray debugger available externally.
  int ray_debugger_external;
  /// The raylet config list of this node.
  std::string raylet_config;
  // The time between record metrics in milliseconds, or 0 to disable.
  uint64_t record_metrics_period_ms;
  // The number if max io workers.
  int max_io_workers;
  // The key-value labels of this node.
  absl::flat_hash_map<std::string, std::string> labels;
};

class NodeManager : public rpc::NodeManagerServiceHandler,
                    public syncer::ReporterInterface,
                    public syncer::ReceiverInterface {
 public:
  /// Create a node manager.
  ///
  /// \param config Configuration of node manager, e.g. initial resources, ports, etc.
  /// \param object_manager_config Configuration of object manager, e.g. initial memory
  /// allocation.
  NodeManager(
      instrumented_io_context &io_service,
      const NodeID &self_node_id,
      std::string self_node_name,
      const NodeManagerConfig &config,
      gcs::GcsClient &gcs_client,
      rpc::ClientCallManager &client_call_manager,
      rpc::CoreWorkerClientPool &worker_rpc_pool,
      rpc::RayletClientPool &raylet_client_pool,
      pubsub::SubscriberInterface &core_worker_subscriber,
      ClusterResourceScheduler &cluster_resource_scheduler,
      LocalLeaseManagerInterface &local_lease_manager,
      ClusterLeaseManagerInterface &cluster_lease_manager,
      IObjectDirectory &object_directory,
      ObjectManagerInterface &object_manager,
      LocalObjectManagerInterface &local_object_manager,
      LeaseDependencyManager &lease_dependency_manager,
      WorkerPoolInterface &worker_pool,
      absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers,
      std::shared_ptr<plasma::PlasmaClientInterface> store_client,
      std::unique_ptr<core::experimental::MutableObjectProviderInterface>
          mutable_object_provider,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
      AddProcessToCgroupHook add_process_to_system_cgroup_hook,
      std::unique_ptr<CgroupManagerInterface> cgroup_manager);

  /// Handle an unexpected error that occurred on a client connection.
  /// The client will be disconnected and no more messages will be processed.
  ///
  /// \param client The client whose connection the error occurred on.
  /// \param error The error details.
  void HandleClientConnectionError(const std::shared_ptr<ClientConnection> &client,
                                   const boost::system::error_code &error);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message_data A pointer to the message data.
  void ProcessClientMessage(const std::shared_ptr<ClientConnection> &client,
                            int64_t message_type,
                            const uint8_t *message_data);

  /// Subscribe to the relevant GCS tables and set up handlers.
  void RegisterGcs();

  /// Get initial node manager configuration.
  const NodeManagerConfig &GetInitialConfig() const;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics();

  /// Report worker OOM kill stats
  void ReportWorkerOOMKillStats();

  /// Get the port of the node manager rpc server.
  int GetServerPort() const { return node_manager_server_.GetPort(); }

  void ConsumeSyncMessage(std::shared_ptr<const syncer::RaySyncMessage> message) override;

  std::optional<syncer::RaySyncMessage> CreateSyncMessage(
      int64_t after_version, syncer::MessageType message_type) const override;

  int GetObjectManagerPort() const { return object_manager_.GetServerPort(); }

  LocalObjectManagerInterface &GetLocalObjectManager() { return local_object_manager_; }

  /// Trigger global GC across the cluster to free up references to actors or
  /// object ids.
  void TriggerGlobalGC();

  /// Mark the specified objects as failed with the given error type.
  ///
  /// \param error_type The type of the error that caused this task to fail.
  /// \param object_ids The object ids to store error messages into.
  /// \param job_id The optional job to push errors to if the writes fail.
  void MarkObjectsAsFailed(const ErrorType &error_type,
                           const std::vector<rpc::ObjectReference> &object_ids,
                           const JobID &job_id);

  /// Stop this node manager.
  void Stop();

  /// Query all of local core worker states.
  ///
  /// \param on_replied A callback that's called when each of query RPC is replied.
  /// \param send_reply_callback A reply callback that will be called when all
  /// RPCs are replied.
  /// \param include_memory_info If true, it requires every object ref information
  /// from all workers.
  /// \param include_task_info If true, it requires every task metadata information
  /// from all workers.
  /// \param limit A maximum number of task/object entries to return from each core
  /// worker.
  /// \param on_all_replied A callback that's called when every worker replies.
  void QueryAllWorkerStates(
      const std::function<void(const ray::Status &status,
                               const rpc::GetCoreWorkerStatsReply &r)> &on_replied,
      rpc::SendReplyCallback &send_reply_callback,
      bool include_memory_info,
      bool include_task_info,
      int64_t limit,
      const std::function<void()> &on_all_replied);

  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  ///
  /// \param object_info The info about the object that is locally available.
  void HandleObjectLocal(const ObjectInfo &object_info);

  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that has been evicted locally.
  void HandleObjectMissing(const ObjectID &object_id);

  /// Handle a `WorkerLease` request.
  void HandleRequestWorkerLease(rpc::RequestWorkerLeaseRequest request,
                                rpc::RequestWorkerLeaseReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  /// Get pointers to objects stored in plasma. They will be
  /// released once the returned references go out of scope.
  ///
  /// \param[in] object_ids The objects to get.
  /// \param[out] results The pointers to objects stored in
  /// plasma.
  /// \return Whether the request was successful.
  bool GetObjectsFromPlasma(const std::vector<ObjectID> &object_ids,
                            std::vector<std::unique_ptr<RayObject>> *results);

  /// Get the local drain request.
  std::optional<rpc::DrainRayletRequest> GetLocalDrainRequest() const {
    return cluster_resource_scheduler_.GetLocalResourceManager().GetLocalDrainRequest();
  }

  /// gRPC Handlers
  /// Handle a `PinObjectIDs` request.
  void HandlePinObjectIDs(rpc::PinObjectIDsRequest request,
                          rpc::PinObjectIDsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ResizeLocalResourceInstances` request.
  void HandleResizeLocalResourceInstances(
      rpc::ResizeLocalResourceInstancesRequest request,
      rpc::ResizeLocalResourceInstancesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleReturnWorkerLease(rpc::ReturnWorkerLeaseRequest request,
                               rpc::ReturnWorkerLeaseReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleCancelWorkerLease(rpc::CancelWorkerLeaseRequest request,
                               rpc::CancelWorkerLeaseReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

 private:
  FRIEND_TEST(NodeManagerStaticTest, TestHandleReportWorkerBacklog);

  // Removes the worker from node_manager's leased_workers_ map.
  // Warning: this does NOT release the worker's resources, or put the leased worker
  // back to the worker pool, or destroy the worker. The caller must handle the worker's
  // resources well.
  void ReleaseWorker(const LeaseID &lease_id) {
    RAY_CHECK(leased_workers_.contains(lease_id));
    leased_workers_.erase(lease_id);
    SetIdleIfLeaseEmpty();
  }

  void SetIdleIfLeaseEmpty() {
    if (leased_workers_.empty()) {
      cluster_resource_scheduler_.GetLocalResourceManager().SetIdleFootprint(
          WorkFootprint::NODE_WORKERS);
    }
  }

  /// If the primary objects' usage is over the threshold
  /// specified in RayConfig, spill objects up to the max
  /// throughput.
  void SpillIfOverPrimaryObjectsThreshold();

  /// Methods for handling nodes.

  /// Handle an unexpected failure notification from GCS pubsub.
  ///
  /// \param data The data of the worker that died.
  void HandleUnexpectedWorkerFailure(const WorkerID &worker_id);

  /// Handler for the addition of a new node.
  ///
  /// \param data Data associated with the new node.
  void NodeAdded(const GcsNodeInfo &data);

  /// Handler for the removal of a GCS node.
  /// \param node_id Id of the removed node.
  void NodeRemoved(const NodeID &node_id);

  /// Handler for the addition or updation of a resource in the GCS
  /// \param node_id ID of the node that created or updated resources.
  /// \param createUpdatedResources Created or updated resources.
  /// \return Whether the update is applied.
  bool ResourceCreateUpdated(const NodeID &node_id,
                             const ResourceRequest &createUpdatedResources);

  /// Handler for the deletion of a resource in the GCS
  /// \param node_id ID of the node that deleted resources.
  /// \param resource_names Names of deleted resources.
  /// \return Whether the deletion is applied.
  bool ResourceDeleted(const NodeID &node_id,
                       const std::vector<std::string> &resource_names);

  /// Evaluates the local infeasible queue to check if any tasks can be scheduled.
  /// This is called whenever there's an update to the resources on the local node.
  void TryLocalInfeasibleTaskScheduling();

  /// Write out debug state to a file.
  void DumpDebugState() const;

  /// Flush objects that are out of scope in the application. This will attempt
  /// to eagerly evict all plasma copies of the object from the cluster.
  void FlushObjectsToFree();

  /// Handler for a resource usage notification from the GCS.
  ///
  /// \param id The ID of the node manager that sent the resource usage.
  /// \param resource_view_sync_message The resource usage data.
  /// \return Whether the node resource usage is updated.
  bool UpdateResourceUsage(
      const NodeID &id,
      const syncer::ResourceViewSyncMessage &resource_view_sync_message);

  /// Cleanup any lease resources and state for a worker that was granted a lease.
  ///
  /// \param worker The worker that was granted the lease.
  /// \return Whether the worker should be returned to the idle pool. This is
  /// only false for actor creation calls, which should never be returned to idle.
  bool CleanupLease(const std::shared_ptr<WorkerInterface> &worker);

  /// Convert a worker to an actor since it's finished an actor creation task.
  /// \param worker The worker that was granted the actor creation lease.
  /// \param lease The lease of the actor creation task.
  void ConvertWorkerToActor(const std::shared_ptr<WorkerInterface> &worker,
                            const RayLease &lease);

  /// Start a get or wait request for the requested objects.
  ///
  /// \param client The client that is requesting the objects.
  /// \param object_refs The objects that are requested.
  /// \param is_get_request If this is a get request, else it's a wait request.
  void AsyncGetOrWait(const std::shared_ptr<ClientConnection> &client,
                      const std::vector<rpc::ObjectReference> &object_refs,
                      bool is_get_request);

  /// Cancel all ongoing get requests from the client.
  ///
  /// This does *not* cancel ongoing wait requests.
  ///
  /// \param client The client whose get requests will be canceled.
  void CancelGetRequest(const std::shared_ptr<ClientConnection> &client);

  /// Handle a task that is blocked. Note that this callback may
  /// arrive after the worker lease has been returned to the node manager.
  ///
  /// \param worker Shared ptr to the worker, or nullptr if lost.
  void HandleNotifyWorkerBlocked(const std::shared_ptr<WorkerInterface> &worker);

  /// Handle a task that is unblocked. Note that this callback may
  /// arrive after the worker lease has been returned to the node manager.
  /// However, it is guaranteed to arrive after DirectCallTaskBlocked.
  ///
  /// \param worker Shared ptr to the worker, or nullptr if lost.
  void HandleNotifyWorkerUnblocked(const std::shared_ptr<WorkerInterface> &worker);

  /// Destroy a worker.
  /// We will disconnect the worker connection first and then kill the worker.
  ///
  /// \param worker The worker to destroy.
  /// \param disconnect_type The reason why this worker process is disconnected.
  /// \param disconnect_detail The detailed reason for a given exit.
  /// \param force true to destroy immediately, false to give time for the worker to
  /// clean up and exit gracefully.
  void DestroyWorker(std::shared_ptr<WorkerInterface> worker,
                     rpc::WorkerExitType disconnect_type,
                     const std::string &disconnect_detail,
                     bool force = false);

  /// Handles the event that a job is started.
  ///
  /// \param job_id ID of the started job.
  /// \param job_data Data associated with the started job.

  void HandleJobStarted(const JobID &job_id, const JobTableData &job_data);

  /// Handles the event that a job is finished.
  ///
  /// \param job_id ID of the finished job.
  /// \param job_data Data associated with the finished job.
  void HandleJobFinished(const JobID &job_id, const JobTableData &job_data);

  /// Process client message of RegisterClientRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessRegisterClientRequestMessage(
      const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data);

  Status ProcessRegisterClientRequestMessageImpl(
      const std::shared_ptr<ClientConnection> &client,
      const ray::protocol::RegisterClientRequest *message);

  // Register a new worker into worker pool.
  Status RegisterForNewWorker(std::shared_ptr<WorkerInterface> worker,
                              pid_t pid,
                              const StartupToken &worker_startup_token,
                              std::function<void(Status, int)> send_reply_callback = {});
  // Register a new driver into worker pool.
  Status RegisterForNewDriver(std::shared_ptr<WorkerInterface> worker,
                              pid_t pid,
                              const JobID &job_id,
                              const ray::protocol::RegisterClientRequest *message,
                              std::function<void(Status, int)> send_reply_callback = {});

  /// Process client message of AnnounceWorkerPort
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessAnnounceWorkerPortMessage(const std::shared_ptr<ClientConnection> &client,
                                        const uint8_t *message_data);
  void ProcessAnnounceWorkerPortMessageImpl(
      const std::shared_ptr<ClientConnection> &client,
      const ray::protocol::AnnounceWorkerPort *message);

  // Send status of port announcement to client side.
  void SendPortAnnouncementResponse(const std::shared_ptr<ClientConnection> &client,
                                    Status status);

  /// Handle the case that a worker is available.
  ///
  /// \param worker The pointer to the worker
  void HandleWorkerAvailable(const std::shared_ptr<WorkerInterface> &worker);

  /// Handle a client that has disconnected. This can be called multiple times
  /// on the same client because this is triggered both when a client
  /// disconnects and when the node manager fails to write a message to the
  /// client.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessDisconnectClientMessage(const std::shared_ptr<ClientConnection> &client,
                                      const uint8_t *message_data);

  /// Handle client request AsyncGetObjects.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void HandleAsyncGetObjectsRequest(const std::shared_ptr<ClientConnection> &client,
                                    const uint8_t *message_data);

  /// Process client message of WaitRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessWaitRequestMessage(const std::shared_ptr<ClientConnection> &client,
                                 const uint8_t *message_data);

  /// Process client message of WaitForActorCallArgsRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessWaitForActorCallArgsRequestMessage(
      const std::shared_ptr<ClientConnection> &client, const uint8_t *message_data);

  /// Process client message of PushErrorRequest
  ///
  /// \param message_data A pointer to the message data.
  void ProcessPushErrorRequestMessage(const uint8_t *message_data);

  /// Process worker subscribing to a given plasma object become available. This handler
  /// makes sure that the plasma object is local and calls core worker's PlasmaObjectReady
  /// gRPC endpoint.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessSubscribePlasmaReady(const std::shared_ptr<ClientConnection> &client,
                                   const uint8_t *message_data);

  /// Handle a `GetResourceLoad` request.
  void HandleGetResourceLoad(rpc::GetResourceLoadRequest request,
                             rpc::GetResourceLoadReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `CancelLeasesWithResourceShapes` request.
  void HandleCancelLeasesWithResourceShapes(
      rpc::CancelLeasesWithResourceShapesRequest request,
      rpc::CancelLeasesWithResourceShapesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `PrepareBundleResources` request.
  void HandlePrepareBundleResources(rpc::PrepareBundleResourcesRequest request,
                                    rpc::PrepareBundleResourcesReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `CommitBundleResources` request.
  void HandleCommitBundleResources(rpc::CommitBundleResourcesRequest request,
                                   rpc::CommitBundleResourcesReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ResourcesReturn` request.
  void HandleCancelResourceReserve(rpc::CancelResourceReserveRequest request,
                                   rpc::CancelResourceReserveReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandlePrestartWorkers(rpc::PrestartWorkersRequest request,
                             rpc::PrestartWorkersReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ReportWorkerBacklog` request.
  void HandleReportWorkerBacklog(rpc::ReportWorkerBacklogRequest request,
                                 rpc::ReportWorkerBacklogReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// This is created for unit test purpose so that we don't need to create
  /// a node manager in order to test HandleReportWorkerBacklog.
  static void HandleReportWorkerBacklog(rpc::ReportWorkerBacklogRequest request,
                                        rpc::ReportWorkerBacklogReply *reply,
                                        rpc::SendReplyCallback send_reply_callback,
                                        WorkerPoolInterface &worker_pool,
                                        LocalLeaseManagerInterface &local_lease_manager);

  /// Handle a `ReleaseUnusedActorWorkers` request.
  // On GCS restart, there's a pruning effort. GCS sends raylet a list of actor workers it
  // still wants (that it keeps tracks of); and the raylet destroys all other actor
  // workers.
  void HandleReleaseUnusedActorWorkers(
      rpc::ReleaseUnusedActorWorkersRequest request,
      rpc::ReleaseUnusedActorWorkersReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ShutdownRaylet` request.
  void HandleShutdownRaylet(rpc::ShutdownRayletRequest request,
                            rpc::ShutdownRayletReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `DrainRaylet` request.
  void HandleDrainRaylet(rpc::DrainRayletRequest request,
                         rpc::DrainRayletReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleIsLocalWorkerDead(rpc::IsLocalWorkerDeadRequest request,
                               rpc::IsLocalWorkerDeadReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `NodeStats` request.
  void HandleGetNodeStats(rpc::GetNodeStatsRequest request,
                          rpc::GetNodeStatsReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `GlobalGC` request.
  void HandleGlobalGC(rpc::GlobalGCRequest request,
                      rpc::GlobalGCReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `FormatGlobalMemoryInfo`` request.
  void HandleFormatGlobalMemoryInfo(rpc::FormatGlobalMemoryInfoRequest request,
                                    rpc::FormatGlobalMemoryInfoReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `ReleaseUnusedBundles` request.
  void HandleReleaseUnusedBundles(rpc::ReleaseUnusedBundlesRequest request,
                                  rpc::ReleaseUnusedBundlesReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `GetSystemConfig` request.
  void HandleGetSystemConfig(rpc::GetSystemConfigRequest request,
                             rpc::GetSystemConfigReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `GetWorkerFailureCause` request.
  void HandleGetWorkerFailureCause(rpc::GetWorkerFailureCauseRequest request,
                                   rpc::GetWorkerFailureCauseReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleRegisterMutableObject(rpc::RegisterMutableObjectRequest request,
                                   rpc::RegisterMutableObjectReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandlePushMutableObject(rpc::PushMutableObjectRequest request,
                               rpc::PushMutableObjectReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `GetObjectsInfo` request.
  void HandleGetObjectsInfo(rpc::GetObjectsInfoRequest request,
                            rpc::GetObjectsInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a `NotifyGCSRestart` request
  void HandleNotifyGCSRestart(rpc::NotifyGCSRestartRequest request,
                              rpc::NotifyGCSRestartReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Checks the local socket connection for all registered workers and drivers.
  /// If any of them have disconnected unexpectedly (i.e., we receive a SIGHUP),
  /// we disconnect and kill the worker process.
  ///
  /// This is an optimization to avoid processing all messages sent by the worker
  /// before detecing an EOF on the socket.
  void CheckForUnexpectedWorkerDisconnects();

  /// Trigger local GC on each worker of this raylet.
  void DoLocalGC(bool triggered_by_global_gc = false);

  /// Push an error to the driver if this node is full of actors and so we are
  /// unable to schedule new tasks or actors at all.
  void WarnResourceDeadlock();

  /// Dispatch tasks to available workers.
  void DispatchScheduledTasksToWorkers();

  /// Whether a task is an actor creation task.
  bool IsActorCreationTask(const TaskID &task_id);

  /// Return back all the bundle resource.
  ///
  /// \param bundle_spec: Specification of bundle whose resources will be returned.
  /// \return Whether the resource is returned successfully.
  bool ReturnBundleResources(const BundleSpecification &bundle_spec);

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  void FillResourceUsage(rpc::ResourcesData &data);

  /// Disconnect a client.
  ///
  /// \param client The client that sent the message.
  /// \param graceful Indicates if this was a graceful disconnect initiated by the
  ///        worker or a non-graceful disconnect initiated by the raylet. On graceful
  ///        disconnect, a DisconnectClientReply will be sent to the worker prior to
  ///        closing the connection.
  /// \param disconnect_type The reason to disconnect the specified client.
  /// \param disconnect_detail Disconnection information in details.
  /// \param client_error_message Extra error messages about this disconnection
  void DisconnectClient(const std::shared_ptr<ClientConnection> &client,
                        bool graceful,
                        rpc::WorkerExitType disconnect_type,
                        const std::string &disconnect_detail,
                        const rpc::RayException *creation_task_exception = nullptr);

  bool TryLocalGC();

  /// Creates the callback used in the memory monitor.
  MemoryUsageRefreshCallback CreateMemoryUsageRefreshCallback();

  /// Creates the detail message for the worker that is killed due to memory running low.
  const std::string CreateOomKillMessageDetails(
      const std::shared_ptr<WorkerInterface> &worker,
      const NodeID &node_id,
      const MemorySnapshot &system_memory,
      float usage_threshold) const;

  /// Creates the suggestion message for the worker that is killed due to memory running
  /// low.
  const std::string CreateOomKillMessageSuggestions(
      const std::shared_ptr<WorkerInterface> &worker, bool should_retry = true) const;

  /// Stores the failure reason for the task. The entry will be cleaned up by a periodic
  /// function post TTL.
  void SetWorkerFailureReason(const LeaseID &lease_id,
                              const rpc::RayErrorInfo &failure_reason,
                              bool should_retry);

  /// Checks the expiry time of the worker failures and garbage collect them.
  void GCWorkerFailureReason();

  /// Creates a AgentManager that creates and manages a dashboard agent.
  std::unique_ptr<AgentManager> CreateDashboardAgentManager(
      const NodeID &self_node_id, const NodeManagerConfig &config);

  /// Creates a AgentManager that creates and manages a runtime env agent.
  std::unique_ptr<AgentManager> CreateRuntimeEnvAgentManager(
      const NodeID &self_node_id, const NodeManagerConfig &config);

  /// ID of this node.
  NodeID self_node_id_;
  /// The user-given identifier or name of this node.
  std::string self_node_name_;
  instrumented_io_context &io_service_;
  /// A client connection to the GCS.
  gcs::GcsClient &gcs_client_;
  /// The function to shutdown raylet gracefully.
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;
  /// A pool of workers.
  WorkerPoolInterface &worker_pool_;
  /// The `ClientCallManager` object that is shared by all `NodeManagerClient`s
  /// as well as all `CoreWorkerClient`s.
  rpc::ClientCallManager &client_call_manager_;
  /// Pool of RPC client connections to core workers.
  rpc::CoreWorkerClientPool &worker_rpc_pool_;
  // Pool of RPC client connections to raylets.
  rpc::RayletClientPool &raylet_client_pool_;
  /// The raylet client to initiate the pubsub to core workers (owners).
  /// It is used to subscribe objects to evict.
  pubsub::SubscriberInterface &core_worker_subscriber_;
  /// The object table. This is shared between the object manager and node
  /// manager.
  IObjectDirectory &object_directory_;
  /// Manages client requests for object transfers and availability.
  ObjectManagerInterface &object_manager_;
  /// A Plasma object store client. This is used for creating new objects in
  /// the object store (e.g., for actor tasks that can't be run because the
  /// actor died) and to pin objects that are in scope in the cluster.
  std::shared_ptr<plasma::PlasmaClientInterface> store_client_;
  /// Mutable object provider for compiled graphs.
  std::unique_ptr<core::experimental::MutableObjectProviderInterface>
      mutable_object_provider_;
  /// The runner to run function periodically.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  /// The period used for the resources report timer.
  uint64_t report_resources_period_ms_;
  /// Incremented each time we encounter a potential resource deadlock condition.
  /// This is reset to zero when the condition is cleared.
  int resource_deadlock_warned_ = 0;
  /// Whether we have recorded any metrics yet.
  bool recorded_metrics_ = false;
  /// Initial node manager configuration.
  const NodeManagerConfig initial_config_;

  /// A manager to resolve objects needed by queued tasks and workers that
  /// called `ray.get` or `ray.wait`.
  LeaseDependencyManager &lease_dependency_manager_;

  /// A manager for wait requests.
  WaitManager wait_manager_;

  /// A manager for the dashboard agent.
  /// Note: using a pointer because the agent must know node manager's port to start, this
  /// means the AgentManager have to start after node_manager_server_ starts.
  std::unique_ptr<AgentManager> dashboard_agent_manager_;

  /// A manager for the runtime env agent.
  /// Ditto for the pointer argument.
  std::unique_ptr<AgentManager> runtime_env_agent_manager_;

  /// The RPC server.
  rpc::GrpcServer node_manager_server_;

  /// Manages all local objects that are pinned (primary
  /// copies), freed, and/or spilled.
  LocalObjectManagerInterface &local_object_manager_;

  /// Map from node ids to addresses of the remote node managers.
  absl::flat_hash_map<NodeID, std::pair<std::string, int32_t>>
      remote_node_manager_addresses_;

  /// Map of leased workers to their lease ids.
  absl::flat_hash_map<LeaseID, std::shared_ptr<WorkerInterface>> &leased_workers_;

  /// Optional extra information about why the worker failed.
  absl::flat_hash_map<LeaseID, ray::TaskFailureEntry> worker_failure_reasons_;

  /// Whether to trigger global GC in the next resource usage report. This will broadcast
  /// a global GC message to all raylets except for this one.
  bool should_global_gc_ = false;

  /// Whether to trigger local GC in the next resource usage report. This will trigger gc
  /// on all local workers of this raylet.
  bool should_local_gc_ = false;

  /// When plasma storage usage is high, we'll run gc to reduce it.
  double high_plasma_storage_usage_ = 1.0;

  /// the timestampe local gc run
  uint64_t local_gc_run_time_ns_;

  /// Throttler for local gc
  Throttler local_gc_throttler_;

  /// Throttler for global gc
  Throttler global_gc_throttler_;

  /// Target being evicted or null if no target
  std::shared_ptr<WorkerInterface> high_memory_eviction_target_;

  /// Seconds to initialize a local gc
  const uint64_t local_gc_interval_ns_;

  /// These classes make up the new scheduler. ClusterResourceScheduler is
  /// responsible for maintaining a view of the cluster state w.r.t resource
  /// usage. ClusterLeaseManager is responsible for queuing, spilling back, and
  /// dispatching tasks.
  ClusterResourceScheduler &cluster_resource_scheduler_;
  LocalLeaseManagerInterface &local_lease_manager_;
  ClusterLeaseManagerInterface &cluster_lease_manager_;

  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;

  // TODO(swang): Evict entries from these caches.
  /// Cache for the WorkerTable in the GCS.
  absl::flat_hash_set<WorkerID> failed_workers_cache_;
  /// Cache for the NodeTable in the GCS.
  absl::flat_hash_set<NodeID> failed_nodes_cache_;

  /// Concurrency for the following map
  mutable absl::Mutex plasma_object_notification_lock_;

  /// Keeps track of workers waiting for objects
  absl::flat_hash_map<ObjectID, absl::flat_hash_set<std::shared_ptr<WorkerInterface>>>
      async_plasma_objects_notification_
          ABSL_GUARDED_BY(plasma_object_notification_lock_);

  /// Fields that are used to report metrics.
  /// The period between debug state dumps.
  uint64_t record_metrics_period_ms_;

  /// Last time metrics are recorded.
  uint64_t last_metrics_recorded_at_ms_;

  /// The number of workers killed due to memory above threshold since last report.
  uint64_t number_workers_killed_by_oom_ = 0;

  /// The number of workers killed not by memory above threshold since last report.
  uint64_t number_workers_killed_ = 0;

  /// Managers all bundle-related operations.
  std::unique_ptr<PlacementGroupResourceManager> placement_group_resource_manager_;

  /// Next resource broadcast seq no. Non-incrementing sequence numbers
  /// indicate network issues (dropped/duplicated/ooo packets, etc).
  int64_t next_resource_seq_no_;

  /// Whether or not if the shutdown raylet request has been initiated and in progress.
  bool is_shutting_down_ = false;

  /// Ray syncer for synchronization
  syncer::RaySyncer ray_syncer_;

  /// The Policy for selecting the worker to kill when the node runs out of memory.
  std::shared_ptr<WorkerKillingPolicy> worker_killing_policy_;

  /// Monitors and reports node memory usage and whether it is above threshold.
  std::unique_ptr<MemoryMonitor> memory_monitor_;

  /// Used to move the dashboard and runtime_env agents into the system cgroup.
  AddProcessToCgroupHook add_process_to_system_cgroup_hook_;

  // Controls the lifecycle of the CgroupManager.
  std::unique_ptr<CgroupManagerInterface> cgroup_manager_;
};

}  // namespace ray::raylet
