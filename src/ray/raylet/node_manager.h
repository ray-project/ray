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
  NodeManager(
      const std::string &socket_name,
      const ResourceSet &resource_config,
      ObjectManager &object_manager);
  /// Process a message from a client, then listen for more messages if the
  /// client is still alive.
  void ProcessClientMessage(std::shared_ptr<LocalClientConnection> client, int64_t message_type, const uint8_t *message);
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
  // A set of workers, in a WorkerPool()
  WorkerPool worker_pool_;
  // A set of queues that maintain tasks enqueued in pending, ready, running
  // states.
  SchedulingQueue local_queues_;
  // Scheduling policy in effect for this local scheduler.
  SchedulingPolicy sched_policy_;
  ReconstructionPolicy reconstruction_policy_;
  TaskDependencyManager task_dependency_manager_;
};


} // end namespace ray

#endif  // LOCAL_SCHEDULER_H
