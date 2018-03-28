#ifndef RAY_RAYLET_SCHEDULING_QUEUE_H
#define RAY_RAYLET_SCHEDULING_QUEUE_H

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/raylet/actor.h"
#include "ray/raylet/task.h"

namespace ray {

/// \class SchedulingQueue
///
/// Encapsulates task queues. Each queue represents a scheduling state for a
/// task. The scheduling state is one of (1) waiting: for object dependencies
/// to become available, (2) ready: object dependencies are available and the
/// task is ready to be scheduled, (3) scheduled: the task has been scheduled
/// but is waiting for a worker, or (4) running: the task has been scheduled
/// and is running on a worker.
class SchedulingQueue {
 public:
  /// Create a scheduling queue.
  SchedulingQueue() {}

  /// SchedulingQueue destructor.
  virtual ~SchedulingQueue() {}

  /// Get the queue of tasks in the waiting state.
  ///
  /// \return A const reference to the queue of tasks that are waiting for
  /// object dependencies to become available.
  const std::list<Task> &GetWaitingTasks() const;

  /// Get the queue of tasks in the ready state.
  ///
  /// \return A const reference to the queue of tasks that have all
  /// dependencies local and that are waiting to be scheduled.
  const std::list<Task> &GetReadyTasks() const;

  /// Get the queue of actor methods in the ready state.
  ///
  /// \return A const reference to the queue of actor methods that have all
  /// dependencies local and that are waiting to be scheduled.
  const std::list<Task> &GetReadyMethods() const;

  /// Get the queue of tasks in the scheduled state.
  ///
  /// \return A const reference to the queue of tasks that have been scheduled
  /// to execute but that are waiting for a worker.
  const std::list<Task> &GetScheduledTasks() const;

  /// Get the queue of tasks in the running state.
  ///
  /// \return A const reference to the queue of tasks that are currently
  /// executing on a worker.
  const std::list<Task> &GetRunningTasks() const;

  /// Remove tasks from the task queue.
  ///
  /// \param tasks The set of task IDs to remove from the queue. The
  ///        corresponding tasks must be contained in the queue.
  /// \return A vector of the tasks that were removed.
  std::vector<Task> RemoveTasks(std::unordered_set<TaskID, UniqueIDHasher> tasks);

  /// Queue tasks in the waiting state.
  ///
  /// \param tasks The tasks to queue.
  void QueueWaitingTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the ready state.
  ///
  /// \param tasks The tasks to queue.
  void QueueReadyTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the scheduled state.
  ///
  /// \param tasks The tasks to queue.
  void QueueScheduledTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the running state.
  ///
  /// \param tasks The tasks to queue.
  void QueueRunningTasks(const std::vector<Task> &tasks);

  /// Register an actor.
  ///
  /// \param actor_id The ID of the actor to register.
  /// \param actor_information Information about the actor.
  bool RegisterActor(ActorID actor_id, const ActorInformation &actor_information);

 private:
  /// Tasks that are waiting for an object dependency to appear locally.
  std::list<Task> waiting_tasks_;
  /// Tasks whose object dependencies are locally available, but that are
  /// waiting to be scheduled.
  std::list<Task> ready_tasks_;
  /// Tasks that have been scheduled to run, but that are waiting for a worker.
  std::list<Task> scheduled_tasks_;
  /// Tasks that are running on a worker.
  std::list<Task> running_tasks_;
  /// The registry of known actors.
  std::unordered_map<ActorID, ActorInformation, UniqueIDHasher> actor_registry_;
};
}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_QUEUE_H
