#ifndef LOCAL_SCHEDULER_H
#define LOCAL_SCHEDULER_H

#include "client_connection.h"
#include "LsResources.h"
#include "LsQueue.h"
#include "LsPolicy.h"
#include "ray/om/object_manager.h"
#include "reconstruction_policy.h"
#include "task_dependency_manager.h"
#include "WorkerPool.h"

using namespace std;
namespace ray {

// TODO(swang): Move this to a header file shared by LocalScheduler and ObjectManager.
class ClientManager {
 public:
  /// Process a message from a client, then listen for more messages if the
  /// client is still alive.
  virtual void ProcessClientMessage(
      shared_ptr<ClientConnection> client,
      int64_t message_type,
      const uint8_t *message) = 0;
};


class LocalScheduler : public ClientManager {
 public:
  LocalScheduler(
      const std::string &socket_name,
      const ResourceSet &resource_config,
      ObjectManager &object_manager);
  /// Process a message from a client, then listen for more messages if the
  /// client is still alive.
  void ProcessClientMessage(shared_ptr<ClientConnection> client, int64_t message_type, const uint8_t *message);
 private:
  /// Submit a task to this node.
  void submitTask(const Task& task);
  /// Assign a task.
  void assignTask(const Task& task);
  /// Finish a task.
  void finishTask(const TaskID& task_id);

  /// The resources local to this node.
  LsResources local_resources_;
  // TODO(alexey): Add resource information from other nodes.
  //std::unordered_map<DBClientID, LsResources&, UniqueIDHasher> cluster_resource_map_;
  // A set of workers, in a WorkerPool()
  WorkerPool worker_pool_;
  // A set of queues that maintain tasks enqueued in pending, ready, running
  // states.
  LsQueue local_queues_;
  // Scheduling policy in effect for this local scheduler.
  LsPolicy sched_policy_;
  ReconstructionPolicy reconstruction_policy_;
  TaskDependencyManager task_dependency_manager_;
};


} // end namespace ray

#endif  // LOCAL_SCHEDULER_H
