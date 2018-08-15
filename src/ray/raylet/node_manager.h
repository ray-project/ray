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
  ResourceSet resource_config;
  int num_initial_workers;
  int num_workers_per_process;
  /// The commands used to start the worker process, grouped by language.
  std::unordered_map<Language, std::vector<std::string>> worker_commands;
  uint64_t heartbeat_period_ms;
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
  void ProcessNewClient(LocalClientConnection &client);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message A pointer to the message data.
  void ProcessClientMessage(const std::shared_ptr<LocalClientConnection> &client,
                            int64_t message_type, const uint8_t *message);

  void ProcessNewNodeManager(TcpClientConnection &node_manager_client);

  void ProcessNodeManagerMessage(TcpClientConnection &node_manager_client,
                                 int64_t message_type, const uint8_t *message);

  ray::Status RegisterGcs();

 private:
  /// Methods for handling clients.
  /// Handler for the addition of a new GCS client.
  void ClientAdded(const ClientTableDataT &data);
  /// Handler for the removal of a GCS client.
  void ClientRemoved(const ClientTableDataT &client_data);
  /// Send heartbeats to the GCS.
  void Heartbeat();
  /// Handler for a heartbeat notification from the GCS.
  void HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &id,
                      const HeartbeatTableDataT &data);

  /// Methods for task scheduling.
  /// Enqueue a placeable task to wait on object dependencies or be ready for dispatch.
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
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                  bool forwarded = false);
  /// Assign a task. The task is assumed to not be queued in local_queues_.
  void AssignTask(Task &task);
  /// Handle a worker finishing its assigned task.
  void FinishAssignedTask(Worker &worker);
  /// Perform a placement decision on placeable tasks.
  void ScheduleTasks();
  /// Handle a task whose return value(s) must be reconstructed.
  void HandleTaskReconstruction(const TaskID &task_id);
  /// Resubmit a task for execution. This is a task that was previously already
  /// submitted to a raylet but which must now be re-executed.
  void ResubmitTask(const Task &task);
  /// Attempt to forward a task to a remote different node manager. If this
  /// fails, the task will be resubmit locally.
  ///
  /// \param task The task in question.
  /// \param node_manager_id The ID of the remote node manager.
  void ForwardTaskOrResubmit(const Task &task, const ClientID &node_manager_id);
  /// Forward a task to another node to execute. The task is assumed to not be
  /// queued in local_queues_.
  ray::Status ForwardTask(const Task &task, const ClientID &node_id);
  /// Dispatch locally scheduled tasks. This attempts the transition from "scheduled" to
  /// "running" task state.
  void DispatchTasks();
  /// Handle a worker becoming blocked in a `ray.get`.
  void HandleWorkerBlocked(std::shared_ptr<Worker> worker);
  /// Handle a worker exiting a `ray.get`.
  void HandleWorkerUnblocked(std::shared_ptr<Worker> worker);

  /// Methods for actor scheduling.
  /// Handler for the creation of an actor, possibly on a remote node.
  void HandleActorCreation(const ActorID &actor_id,
                           const std::vector<ActorTableDataT> &data);

  /// TODO(rkn): This should probably be removed when we improve the
  /// SchedulingQueue API. This is a helper function for
  /// CleanUpTasksForDeadActor.
  ///
  /// This essentially loops over all of the tasks in the provided list and
  /// finds The IDs of the tasks that belong to the given actor.
  ///
  /// \param actor_id The actor to get the tasks for.
  /// \param tasks A list of tasks to extract from.
  /// \param tasks_to_remove The task IDs of the extracted tasks are inserted in
  /// this vector.
  void GetActorTasksFromList(const ActorID &actor_id, const std::list<Task> &tasks,
                             std::unordered_set<TaskID> &tasks_to_remove);

  /// When an actor dies, loop over all of the queued tasks for that actor and
  /// treat them as failed.
  ///
  /// \param actor_id The actor that died.
  /// \return Void.
  void CleanUpTasksForDeadActor(const ActorID &actor_id);

  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  void HandleObjectLocal(const ObjectID &object_id);
  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  void HandleObjectMissing(const ObjectID &object_id);

  /// Handles updates to driver table.
  void HandleDriverTableUpdate(const ClientID &id,
                               const std::vector<DriverTableDataT> &driver_data);

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
  std::unordered_map<ClientID, TcpServerConnection> remote_server_connections_;
  /// A mapping from actor ID to registration information about that actor
  /// (including which node manager owns it).
  std::unordered_map<ActorID, ActorRegistration> actor_registry_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
