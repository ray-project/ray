#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

// clang-format off
#include "ray/common/client_connection.h"
#include "ray/raylet/scheduling_policy.h"
#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/scheduling_resources.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/reconstruction_policy.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/worker_pool.h"
// clang-format on

namespace ray {

class NodeManager : public ClientManager<boost::asio::local::stream_protocol> {
 public:
  /// Create a node manager.
  ///
  /// \param socket_name The pathname of the Unix domain socket to listen at
  ///        for local connections.
  /// \param resource_config The initial set of node resources.
  /// \param object_manager A reference to the local object manager.
  NodeManager(const std::string &socket_name, const ResourceSet &resource_config,
              ObjectManager &object_manager);

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

 private:
  /// Submit a task to this node.
  void SubmitTask(const Task &task);
  /// Assign a task.
  void AssignTask(const Task &task);
  /// Finish a task.
  void FinishTask(const TaskID &task_id);
  /// Schedule tasks.
  void ScheduleTasks();
  /// Handle a task whose local dependencies were missing and are now
  /// available.
  void HandleWaitingTaskReady(const TaskID &task_id);
  /// Resubmit a task whose return value needs to be reconstructed.
  void ResubmitTask(const TaskID &task_id);

  /// The resources local to this node.
  SchedulingResources local_resources_;
  // TODO(atumanov): Add resource information from other nodes.
  // std::unordered_map<ClientID, SchedulingResources&, UniqueIDHasher>
  // cluster_resource_map_;
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
};

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
