#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

// clang-format off
#include "ray/raylet/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/common/client_connection.h"
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
  std::vector<const char *> worker_command;
  uint64_t heartbeat_period_ms;
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
  void ProcessNewClient(std::shared_ptr<LocalClientConnection> client);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message A pointer to the message data.
  void ProcessClientMessage(std::shared_ptr<LocalClientConnection> client,
                            int64_t message_type, const uint8_t *message);

  void ProcessNewNodeManager(std::shared_ptr<TcpClientConnection> node_manager_client);

  void ProcessNodeManagerMessage(std::shared_ptr<TcpClientConnection> node_manager_client,
                                 int64_t message_type, const uint8_t *message);

  void ClientAdded(gcs::AsyncGcsClient *client, const UniqueID &id,
                   const ClientTableDataT &data);

  void HeartbeatAdded(gcs::AsyncGcsClient *client, const ClientID &id,
                      const HeartbeatTableDataT &data);

 private:
  /// Submit a task to this node.
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage);
  /// Assign a task.
  void AssignTask(const Task &task);
  /// Finish a task.
  void FinishTask(const TaskID &task_id);
  /// Schedule tasks.
  void ScheduleTasks();
  /// Handle a task whose local dependencies were missing and are now available.
  void HandleWaitingTaskReady(const TaskID &task_id);
  /// Resubmit a task whose return value needs to be reconstructed.
  void ResubmitTask(const TaskID &task_id);
  ray::Status ForwardTask(Task &task, const ClientID &node_id);
  /// Send heartbeats to the GCS.
  void Heartbeat();

  boost::asio::io_service &io_service_;
  boost::asio::deadline_timer heartbeat_timer_;
  uint64_t heartbeat_period_ms_;
  /// The resources local to this node.
  const SchedulingResources local_resources_;
  // TODO(atumanov): Add resource information from other nodes.
  std::unordered_map<ClientID, SchedulingResources, UniqueIDHasher> cluster_resource_map_;
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
  /// A client connection to the GCS.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  std::vector<ClientID> remote_clients_;
  std::unordered_map<ClientID, TcpServerConnection, UniqueIDHasher>
      remote_server_connections_;
  ObjectManager &object_manager_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
