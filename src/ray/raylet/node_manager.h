#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

#include <boost/asio/steady_timer.hpp>

// clang-format off
#include "ray/raylet/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/common/client_connection.h"
#include "ray/gcs/format/util.h"
#include "ray/raylet/actor_registration.h"
#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/scheduling_policy.h"
#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/scheduling_resources.h"
#include "ray/raylet/reconstruction_policy.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/worker_pool.h"
// clang-format on

namespace ray {

namespace raylet {

struct NodeManagerConfig {
  /// The node's resource configuration.
  ResourceSet resource_config;
  /// The port to use for listening to incoming connections. If this is 0 then
  /// the node manager will choose its own port.
  int node_manager_port;
  /// The initial number of workers to create.
  int num_initial_workers;
  /// The number of workers per process.
  int num_workers_per_process;
  /// The maximum number of workers that can be started concurrently by a
  /// worker pool.
  int maximum_startup_concurrency;
  /// The commands used to start the worker process, grouped by language.
  std::unordered_map<Language, std::vector<std::string>> worker_commands;
  /// The time between heartbeats in milliseconds.
  uint64_t heartbeat_period_ms;
  /// the maximum lineage size.
  uint64_t max_lineage_size;
  /// The store socket name.
  std::string store_socket_name;
};

class NodeManager {
 public:
  /// Create a node manager.
  ///
  /// \param resource_config The initial set of node resources.
  /// \param object_manager A reference to the local object manager.
  NodeManager(boost::asio::io_service &io_service, const NodeManagerConfig &config,
              ObjectManager &object_manager,
              std::shared_ptr<gcs::AsyncGcsClient> gcs_client);

  /// Process a new client connection.
  ///
  /// \param client The client to process.
  /// \return Void.
  void ProcessNewClient(LocalClientConnection &client);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessClientMessage(const std::shared_ptr<LocalClientConnection> &client,
                            int64_t message_type, const uint8_t *message_data);

  /// Handle a new node manager connection.
  ///
  /// \param node_manager_client The connection to the remote node manager.
  /// \return Void.
  void ProcessNewNodeManager(TcpClientConnection &node_manager_client);

  /// Handle a message from a remote node manager.
  ///
  /// \param node_manager_client The connection to the remote node manager.
  /// \param message_type The type of the message.
  /// \param message The message contents.
  /// \return Void.
  void ProcessNodeManagerMessage(TcpClientConnection &node_manager_client,
                                 int64_t message_type, const uint8_t *message);

  /// Subscribe to the relevant GCS tables and set up handlers.
  ///
  /// \return Status indicating whether this was done successfully or not.
  ray::Status RegisterGcs();

 private:
  /// Methods for handling clients.

  /// Handler for the addition of a new GCS client.
  ///
  /// \param data Data associated with the new client.
  /// \return Void.
  void ClientAdded(const ClientTableDataT &data);
  /// Handler for the removal of a GCS client.
  /// \param client_data Data associated with the removed client.
  /// \return Void.
  void ClientRemoved(const ClientTableDataT &client_data);

  /// Send heartbeats to the GCS.
  void Heartbeat();

  /// Get profiling information from the object manager and push it to the GCS.
  ///
  /// \return Void.
  void GetObjectManagerProfileInfo();

  /// Handler for a heartbeat notification from the GCS.
  ///
  /// \param client The GCS client.
  /// \param id The ID of the node manager that sent the heartbeat.
  /// \param data The heartbeat data including load information.
  /// \return Void.
  void HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &id,
                      const HeartbeatTableDataT &data);

  /// Methods for task scheduling.

  /// Enqueue a placeable task to wait on object dependencies or be ready for
  /// dispatch.
  ///
  /// \param task The task in question.
  /// \return Void.
  void EnqueuePlaceableTask(const Task &task);
  /// This will treat the task as if it had been executed and failed. This is
  /// done by looping over the task return IDs and for each ID storing an object
  /// that represents a failure in the object store. When clients retrieve these
  /// objects, they will raise application-level exceptions.
  ///
  /// \param spec The specification of the task.
  /// \return Void.
  void TreatTaskAsFailed(const TaskSpecification &spec);
  /// Handle specified task's submission to the local node manager.
  ///
  /// \param task The task being submitted.
  /// \param uncommitted_lineage The uncommitted lineage of the task.
  /// \param forwarded True if the task has been forwarded from a different
  /// node manager and false if it was submitted by a local worker.
  /// \return Void.
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                  bool forwarded = false);
  /// Assign a task. The task is assumed to not be queued in local_queues_.
  ///
  /// \param task The task in question.
  /// \return Void.
  void AssignTask(Task &task);
  /// Handle a worker finishing its assigned task.
  ///
  /// \param The worker that fiished the task.
  /// \return Void.
  void FinishAssignedTask(Worker &worker);
  /// Make a placement decision for placeable tasks given the resource_map
  /// provided. This will perform task state transitions and task forwarding.
  ///
  /// \param resource_map A mapping from node manager ID to an estimate of the
  /// resources available to that node manager. Scheduling decisions will only
  /// consider the local node manager and the node managers in the keys of the
  /// resource_map argument.
  /// \return Void.
  void ScheduleTasks(std::unordered_map<ClientID, SchedulingResources> &resource_map);
  /// Handle a task whose return value(s) must be reconstructed.
  ///
  /// \param task_id The relevant task ID.
  /// \return Void.
  void HandleTaskReconstruction(const TaskID &task_id);
  /// Resubmit a task for execution. This is a task that was previously already
  /// submitted to a raylet but which must now be re-executed.
  ///
  /// \param task The task being resubmitted.
  /// \return Void.
  void ResubmitTask(const Task &task);
  /// Attempt to forward a task to a remote different node manager. If this
  /// fails, the task will be resubmit locally.
  ///
  /// \param task The task in question.
  /// \param node_manager_id The ID of the remote node manager.
  /// \return Void.
  void ForwardTaskOrResubmit(const Task &task, const ClientID &node_manager_id);
  /// Forward a task to another node to execute. The task is assumed to not be
  /// queued in local_queues_.
  ///
  /// \param task The task to forward.
  /// \param node_id The ID of the node to forward the task to.
  /// \param on_error Callback on run on non-ok status.
  void ForwardTask(const Task &task, const ClientID &node_id,
                   const std::function<void(const ray::Status &)> &on_error);

  /// Dispatch locally scheduled tasks. This attempts the transition from "scheduled" to
  /// "running" task state.
  void DispatchTasks();

  /// Handle a task that is blocked. This could be a task assigned to a worker,
  /// an out-of-band task (e.g., a thread created by the application), or a
  /// driver task. This can be triggered when a client starts a get call or a
  /// wait call.
  ///
  /// \param client The client that is executing the blocked task.
  /// \param required_object_ids The IDs that the client is blocked waiting for.
  /// \param current_task_id The task that is blocked.
  /// \return Void.
  void HandleTaskBlocked(const std::shared_ptr<LocalClientConnection> &client,
                         const std::vector<ObjectID> &required_object_ids,
                         const TaskID &current_task_id);

  /// Handle a task that is unblocked. This could be a task assigned to a
  /// worker, an out-of-band task (e.g., a thread created by the application),
  /// or a driver task. This can be triggered when a client finishes a get call
  /// or a wait call. The given task must be blocked, via a previous call to
  /// HandleTaskBlocked.
  ///
  /// \param client The client that is executing the unblocked task.
  /// \param current_task_id The task that is unblocked.
  /// \return Void.
  void HandleTaskUnblocked(const std::shared_ptr<LocalClientConnection> &client,
                           const TaskID &current_task_id);

  /// Kill a worker.
  ///
  /// \param worker The worker to kill.
  /// \return Void.
  void KillWorker(std::shared_ptr<Worker> worker);

  /// Methods for actor scheduling.
  /// Handler for the creation of an actor, possibly on a remote node.
  ///
  /// \param actor_id The actor ID of the actor that was created.
  /// \param data Data associated with the actor creation event.
  /// \return Void.
  void HandleActorCreation(const ActorID &actor_id,
                           const std::vector<ActorTableDataT> &data);

  /// When an actor dies, loop over all of the queued tasks for that actor and
  /// treat them as failed.
  ///
  /// \param actor_id The actor that died.
  /// \return Void.
  void CleanUpTasksForDeadActor(const ActorID &actor_id);

  /// When a driver dies, loop over all of the queued tasks for that driver and
  /// treat them as failed.
  ///
  /// \param driver_id The driver that died.
  /// \return Void.
  void CleanUpTasksForDeadDriver(const DriverID &driver_id);

  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that is locally available.
  /// \return Void.
  void HandleObjectLocal(const ObjectID &object_id);
  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that has been evicted locally.
  /// \return Void.
  void HandleObjectMissing(const ObjectID &object_id);

  /// Handles updates to driver table.
  ///
  /// \param id An unused value. TODO(rkn): Should this be removed?
  /// \param driver_data Data associated with a driver table event.
  /// \return Void.
  void HandleDriverTableUpdate(const ClientID &id,
                               const std::vector<DriverTableDataT> &driver_data);

  /// Check if certain invariants associated with the task dependency manager
  /// and the local queues are satisfied. This is only used for debugging
  /// purposes.
  ///
  /// \return True if the invariants are satisfied and false otherwise.
  bool CheckDependencyManagerInvariant() const;

  /// Process client message of RegisterClientRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessRegisterClientRequestMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of GetTask
  ///
  /// \param client The client that sent the message.
  /// \return Void.
  void ProcessGetTaskMessage(const std::shared_ptr<LocalClientConnection> &client);

  /// Handle a client that has disconnected. This can be called multiple times
  /// on the same client because this is triggered both when a client
  /// disconnects and when the node manager fails to write a message to the
  /// client.
  ///
  /// \param client The client that sent the message.
  /// \param push_warning Propogate error message if true.
  /// \return Void.
  void ProcessDisconnectClientMessage(
      const std::shared_ptr<LocalClientConnection> &client, bool push_warning = true);

  /// Process client message of SubmitTask
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessSubmitTaskMessage(const uint8_t *message_data);

  /// Process client message of FetchOrReconstruct
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessFetchOrReconstructMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of WaitRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessWaitRequestMessage(const std::shared_ptr<LocalClientConnection> &client,
                                 const uint8_t *message_data);

  /// Process client message of PushErrorRequest
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessPushErrorRequestMessage(const uint8_t *message_data);

  boost::asio::io_service &io_service_;
  ObjectManager &object_manager_;
  /// A Plasma object store client. This is used exclusively for creating new
  /// objects in the object store (e.g., for actor tasks that can't be run
  /// because the actor died).
  plasma::PlasmaClient store_client_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  /// The timer used to send heartbeats.
  boost::asio::steady_timer heartbeat_timer_;
  /// The period used for the heartbeat timer.
  std::chrono::milliseconds heartbeat_period_;
  /// The timer used to get profiling information from the object manager and
  /// push it to the GCS.
  boost::asio::steady_timer object_manager_profile_timer_;
  /// The time that the last heartbeat was sent at. Used to make sure we are
  /// keeping up with heartbeats.
  uint64_t last_heartbeat_at_ms_;
  /// The resources local to this node.
  const SchedulingResources local_resources_;
  /// The resources (and specific resource IDs) that are currently available.
  ResourceIdSet local_available_resources_;
  std::unordered_map<ClientID, SchedulingResources> cluster_resource_map_;
  /// A pool of workers.
  WorkerPool worker_pool_;
  /// A set of queues to maintain tasks.
  SchedulingQueue local_queues_;
  /// The scheduling policy in effect for this local scheduler.
  SchedulingPolicy scheduling_policy_;
  /// The reconstruction policy for deciding when to re-execute a task.
  ReconstructionPolicy reconstruction_policy_;
  /// A manager to make waiting tasks's missing object dependencies available.
  TaskDependencyManager task_dependency_manager_;
  /// The lineage cache for the GCS object and task tables.
  LineageCache lineage_cache_;
  std::vector<ClientID> remote_clients_;
  std::unordered_map<ClientID, std::shared_ptr<TcpServerConnection>>
      remote_server_connections_;
  /// A mapping from actor ID to registration information about that actor
  /// (including which node manager owns it).
  std::unordered_map<ActorID, ActorRegistration> actor_registry_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
