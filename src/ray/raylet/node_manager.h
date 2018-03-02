#ifndef LOCAL_SCHEDULER_H
#define LOCAL_SCHEDULER_H

#include "client_connection.h"
#include "scheduling_resources.h"
#include "scheduling_queue.h"
#include "scheduling_policy.h"
#include "ray/object_manager/object_manager.h"
#include "reconstruction_policy.h"
#include "task_dependency_manager.h"
#include "worker_pool.h"

namespace ray {

class NodeManager : public ClientManager<boost::asio::local::stream_protocol> {
 public:
  /// Create a node manager.
  ///
  /// @param socket_name The pathname of the Unix domain socket to listen at
  ///        for local connections.
  /// @param resource_config The initial set of node resources.
  /// @param object_manager A reference to the local object manager.
  NodeManager(
      const std::string &socket_name,
      const ResourceSet &resource_config,
      ObjectManager &object_manager);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// @param client The client that sent the message.
  /// @param message_type The message type (e.g., a flatbuffer enum).
  /// @param message A pointer to the message data.
  void ProcessClientMessage(
      std::shared_ptr<LocalClientConnection> client,
      int64_t message_type,
      const uint8_t *message);

  /// Handle a task whose local dependencies were missing and are now
  /// available.
  ///
  /// @param task_id The ID of the waiting task.
  void HandleWaitingTaskReady(const TaskID &task_id);

 private:
  /// Submit a task to this node.
  void submitTask(const Task& task);
  /// Assign a task.
  void assignTask(const Task& task);
  /// Finish a task.
  void finishTask(const TaskID& task_id);
  /// Schedule tasks.
  void scheduleTasks();

  /// The resources local to this node.
  SchedulingResources local_resources_;
  // TODO(alexey): Add resource information from other nodes.
  //std::unordered_map<DBClientID, SchedulingResources&, UniqueIDHasher> cluster_resource_map_;
  /// A pool of workers.
  WorkerPool worker_pool_;
  /// A set of queues to maintain tasks.
  SchedulingQueue local_queues_;
  /// The scheduling policy in effect for this local scheduler.
  SchedulingPolicy sched_policy_;
  /// The reconstruction policy for deciding when to re-execute a task.
  ReconstructionPolicy reconstruction_policy_;
  /// A manager to make waiting tasks's missing object dependencies available.
  TaskDependencyManager task_dependency_manager_;
};


} // end namespace ray

#endif  // LOCAL_SCHEDULER_H
