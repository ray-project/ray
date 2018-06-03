#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

// clang-format off
#include "ray/raylet/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/common/client_connection.h"
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
  std::vector<std::string> worker_command;
  uint64_t heartbeat_period_ms;
  uint64_t max_lineage_size;
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
  /// Send heartbeats to the GCS.
  void Heartbeat();
  /// Handler for a heartbeat notification from the GCS.
  void HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &id,
                      const HeartbeatTableDataT &data);

  /// Methods for task scheduling.
  // Queue a task for local execution.
  void QueueTask(const Task &task);
  /// Submit a task to this node.
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage);
  /// Assign a task. The task is assumed to not be queued in local_queues_.
  void AssignTask(Task &task);
  /// Handle a worker finishing its assigned task.
  void FinishAssignedTask(Worker &worker);
  /// Schedule tasks.
  void ScheduleTasks();
  /// Resubmit a task whose return value needs to be reconstructed.
  void ResubmitTask(const TaskID &task_id);
  /// Forward a task to another node to execute. The task is assumed to not be
  /// queued in local_queues_.
  ray::Status ForwardTask(const Task &task, const ClientID &node_id);
  /// Dispatch locally scheduled tasks. This attempts the transition from "scheduled" to
  /// "running" task state.
  void DispatchTasks();

  /// Methods for actor scheduling.
  /// Handler for the creation of an actor, possibly on a remote node.
  void HandleActorCreation(const ActorID &actor_id,
                           const std::vector<ActorTableDataT> &data);

  /// Methods for managing object dependencies.
  /// Handle a dependency required by a queued task that is missing locally.
  /// The dependency is (1) on a remote node, (2) pending creation on a remote
  /// node, or (3) missing from all nodes and requires reconstruction.
  void HandleRemoteDependencyRequired(const ObjectID &dependency_id);
  /// Handle a dependency that was previously required by a queued task that is
  /// no longer required.
  void HandleRemoteDependencyCanceled(const ObjectID &dependency_id);
  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  void HandleObjectLocal(const ObjectID &object_id);
  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  void HandleObjectMissing(const ObjectID &object_id);

  boost::asio::io_service &io_service_;
  ObjectManager &object_manager_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  boost::asio::deadline_timer heartbeat_timer_;
  uint64_t heartbeat_period_ms_;
  /// The resources local to this node.
  const SchedulingResources local_resources_;
  // TODO(atumanov): Add resource information from other nodes.
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
  std::unordered_map<ActorID, ActorRegistration> actor_registry_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
