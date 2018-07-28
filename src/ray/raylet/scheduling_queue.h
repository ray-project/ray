#ifndef RAY_RAYLET_SCHEDULING_QUEUE_H
#define RAY_RAYLET_SCHEDULING_QUEUE_H

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/raylet/task.h"

namespace ray {

namespace raylet {

enum class TaskState { INIT, PLACEABLE, WAITING, READY, RUNNING, BLOCKED, DRIVER };

/// \class SchedulingQueue
///
/// Encapsulates task queues.
// (See design_docs/task_states.rst for the state transition diagram.)
class SchedulingQueue {
 public:
  /// Create a scheduling queue.
  SchedulingQueue() {}

  /// SchedulingQueue destructor.
  virtual ~SchedulingQueue() {}

  /// Get the queue of tasks that are destined for actors that have not yet
  /// been created.
  ///
  /// \return A const reference to the queue of tasks that are destined for
  /// actors that have not yet been created.
  const std::list<Task> &GetWaitForActorCreationMethods() const;

  /// Get the queue of tasks in the waiting state.
  ///
  /// \return A const reference to the queue of tasks that are waiting for
  /// object dependencies to become available.
  const std::list<Task> &GetWaitingTasks() const;

  /// Get the queue of tasks in the placeable state.
  ///
  /// \return A const reference to the queue of tasks that have all
  /// dependencies local and that are waiting to be scheduled.
  const std::list<Task> &GetPlaceableTasks() const;

  /// Get the queue of tasks in the ready state.
  ///
  /// \return A const reference to the queue of tasks ready
  /// to execute but that are waiting for a worker.
  const std::list<Task> &GetReadyTasks() const;

  /// Get the queue of tasks in the running state.
  ///
  /// \return A const reference to the queue of tasks that are currently
  /// executing on a worker.
  const std::list<Task> &GetRunningTasks() const;

  /// Get the tasks in the blocked state.
  ///
  /// \return A const reference to the queue of tasks that have been dispatched
  /// to a worker but are blocked on a data dependency discovered to be missing
  /// at runtime.
  const std::list<Task> &GetBlockedTasks() const;

  /// Get the set of driver task IDs.
  ///
  /// \return A const reference to the set of driver task IDs. These are empty
  /// tasks used to represent drivers.
  const std::unordered_set<TaskID> &GetDriverTaskIds() const;

  /// Remove tasks from the task queue.
  ///
  /// \param tasks The set of task IDs to remove from the queue. The
  /// corresponding tasks must be contained in the queue. The IDs of removed
  /// tasks will be erased from the set.
  /// \return A vector of the tasks that were removed.
  std::vector<Task> RemoveTasks(std::unordered_set<TaskID> &tasks);

  /// Remove a task from the task queue.
  ///
  /// \param task_id The task ID to remove from the queue. The corresponding
  /// task must be contained in the queue.
  /// \return The task that was removed.
  Task RemoveTask(const TaskID &task_id);

  /// Remove a driver task ID. This is an empty task used to represent a driver.
  ///
  /// \param The driver task ID to remove.
  void RemoveDriverTaskId(const TaskID &task_id);

  /// Queue tasks that are destined for actors that have not yet been created.
  ///
  /// \param tasks The tasks to queue.
  void QueueWaitForActorCreationMethods(const std::vector<Task> &tasks);

  /// Queue tasks in the waiting state. These are tasks that cannot yet be
  /// dispatched since they are blocked on a missing data dependency.
  ///
  /// \param tasks The tasks to queue.
  void QueueWaitingTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the placeable state.
  ///
  /// \param tasks The tasks to queue.
  void QueuePlaceableTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the ready state.
  ///
  /// \param tasks The tasks to queue.
  void QueueReadyTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the running state.
  ///
  /// \param tasks The tasks to queue.
  void QueueRunningTasks(const std::vector<Task> &tasks);

  /// Queue tasks in the blocked state. These are tasks that have been
  /// dispatched to a worker but are blocked on a data dependency that was
  /// discovered to be missing at runtime.
  ///
  /// \param tasks The tasks to queue.
  void QueueBlockedTasks(const std::vector<Task> &tasks);

  /// Add a driver task ID. This is an empty task used to represent a driver.
  ///
  /// \param The driver task ID to add.
  void AddDriverTaskId(const TaskID &task_id);

  /// \brief Move the specified tasks from the source state to the destination
  /// state.
  ///
  /// \param tasks The set of task IDs to move. The IDs of successfully moved
  /// tasks will be erased from the set.
  /// \param src_state Source state, which corresponds to one of the internal
  /// task queues.
  /// \param dst_state Destination state, corresponding to one of the internal
  /// task queues.
  void MoveTasks(std::unordered_set<TaskID> &tasks, TaskState src_state,
                 TaskState dst_state);

  /// \brief Filter out task IDs based on their scheduling state.
  ///
  /// \param task_ids The set of task IDs to filter. All tasks that have the
  /// given filter_state will be removed from this set.
  /// \param filter_state The task state to filter out.
  void FilterState(std::unordered_set<TaskID> &task_ids, TaskState filter_state) const;

 private:
  /// Tasks that are destined for actors that have not yet been created.
  std::list<Task> wait_for_actor_creation_methods_;
  /// Tasks that are waiting for an object dependency to appear locally.
  std::list<Task> waiting_tasks_;
  /// Tasks whose object dependencies are locally available, but that are
  /// waiting to be scheduled.
  std::list<Task> placeable_tasks_;
  /// Tasks ready for dispatch, but that are waiting for a worker.
  std::list<Task> ready_tasks_;
  /// Tasks that are running on a worker.
  std::list<Task> running_tasks_;
  /// Tasks that were dispatched to a worker but are blocked on a data
  /// dependency that was missing at runtime.
  std::list<Task> blocked_tasks_;
  /// The set of currently running driver tasks. These are empty tasks that are
  /// started by a driver process on initialization.
  std::unordered_set<TaskID> driver_task_ids_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_QUEUE_H
