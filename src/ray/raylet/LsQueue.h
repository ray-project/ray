#ifndef LS_QUEUE_H
#define LS_QUEUE_H

#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "Task.h"
#include "actor.h"

namespace ray {

/// LSQueue: encapsulates task queues. Responsible for task queue transitions.
///
class LsQueue {
 public:
  /// public constructor of the LSQueue class
  LsQueue() {}

  /// LSQueue destructor
  virtual ~LsQueue() {}

  /// Return a list of tasks in the waiting state.
  const std::list<Task>& waiting_tasks() const;
  /// Return a list of tasks in the ready state.
  const std::list<Task>& ready_tasks() const;
  /// Return a list of methods in the ready state.
  const std::list<Task>& ready_methods() const;
  /// Return a list of tasks in the scheduled state.
  const std::list<Task>& scheduled_tasks() const;
  /// Return a list of tasks in the running state.
  const std::list<Task>& running_tasks() const;
  std::vector<Task> RemoveTasks(std::unordered_set<TaskID, UniqueIDHasher> tasks);
  void QueueWaitingTasks(const std::vector<Task> &tasks);
  void QueueReadyTasks(const std::vector<Task> &tasks);
  void QueueScheduledTasks(const std::vector<Task> &tasks);
  void QueueRunningTasks(const std::vector<Task> &tasks);
  bool RegisterActor(ActorID actor_id,
                     const ActorInformation &actor_information);

 private:
  // Tasks that are waiting for an object dependency to appear locally.
  std::list<Task> waiting_tasks_;
  // Tasks whose object dependencies are locally available, but that are
  // waiting for resources to run.
  std::list<Task> ready_tasks_;
  // Tasks that have resources and are scheduled to run, but that are waiting
  // for a worker.
  std::list<Task> scheduled_tasks_;
  // Tasks that have acquired resources and are running on a worker.
  std::list<Task> running_tasks_;
  std::unordered_map<ActorID, ActorInformation, UniqueIDHasher> actor_registry_;
}; // end class LSQueue
} // end namespace ray

#endif
