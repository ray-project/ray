#ifndef LS_QUEUE_H
#define LS_QUEUE_H
#include <list>
#include <vector>
#include <unordered_map>

namespace ray {

/// LSQueue: encapsulates task queues. Responsible for task queue transitions.
///
class LSQueue {
 public:
  /// public constructor of the LSQueue class
  LSQueue() {}

  /// LSQueue destructor
  virtual ~LSQueue() {}

  /// Return a list of tasks in the waiting state.
  const std::list<Task>& waiting_tasks();
  /// Return a list of tasks in the ready state.
  const std::list<Task>& ready_tasks();
  /// Return a list of methods in the ready state.
  const std::list<Task>& ready_methods();
  /// Return a list of tasks in the running state.
  const std::list<Task>& running_tasks();
  std::vector<Task> RemoveTasks(std::vector<std::list<Task>::iterator> tasks);
  void QueueWaitingTasks(std::vector<Task> tasks);
  void QueueReadyTasks(std::vector<Task> tasks);
  void QueueRunningTasks(std::vector<Task> tasks);
  void RegisterActor(ActorID actor_id, ActorInformation &actor_information);

 private:
  std::list<Task> waiting_tasks_;
  std::list<Task> ready_tasks_;
  std::list<Task> running_tasks_;
  std::unordered_map<ActorID, ActorInformation> actor_registry_;
}; // end class LSQueue
} // end namespace ray

#endif
