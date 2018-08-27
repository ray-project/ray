#ifndef RAY_RAYLET_SCHEDULING_QUEUE_H
#define RAY_RAYLET_SCHEDULING_QUEUE_H

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/raylet/task.h"

namespace ray {

namespace raylet {

enum class TaskState {
  INIT,
  PLACEABLE,
  WAITING,
  READY,
  RUNNING,
  BLOCKED,
  DRIVER,
  INFEASIBLE
};

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

  /// \brief Check if the queue contains a specific task id.
  ///
  /// \param task_id The task ID for the task.
  /// \return Whether the task_id exists in the queue.
  bool HasTask(const TaskID &task_id) const;

  /// Get the queue of tasks that are destined for actors that have not yet
  /// been created.
  ///
  /// \return A const reference to the queue of tasks that are destined for
  /// actors that have not yet been created.
  const std::list<Task> &GetMethodsWaitingForActorCreation() const;

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

  /// Get the queue of tasks in the infeasible state.
  ///
  /// \return A const reference to the queue of tasks whose resource
  /// requirements are not satisfied by any node in the cluster.
  const std::list<Task> &GetInfeasibleTasks() const;

  /// \brief Return an aggregate resource set for all tasks exerting load on this raylet.
  ///
  /// \return A resource set with aggregate resource information about resource load on
  /// this raylet.
  ResourceSet GetResourceLoad() const;

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
  void QueueMethodsWaitingForActorCreation(const std::vector<Task> &tasks);

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

  /// \brief Return all resource demand associated with the ready queue.
  ///
  /// \return Aggregate resource demand from ready tasks.
  ResourceSet GetReadyQueueResources() const;

  /// Return a human-readable string indicating the number of tasks in each
  /// queue.
  ///
  /// \return A string that can be used to display the contents of the queues
  /// for debugging purposes.
  const std::string ToString() const;

  class TaskQueue {
   public:
    /// Creating a task queue.
    TaskQueue() {}

    /// Destructor for task queue.
    ~TaskQueue();

    /// \brief Append a task to queue.
    ///
    /// \param task_id The task ID for the task to append.
    /// \param task The task to append to the queue.
    /// \return Whether the append operation succeeds.
    bool AppendTask(const TaskID &task_id, const Task &task);

    /// \brief Remove a task from queue.
    ///
    /// \param task_id The task ID for the task to remove from the queue.
    /// \return Whether the removal succeeds.
    bool RemoveTask(const TaskID &task_id);

    /// \brief Remove a task from queue.
    ///
    /// \param task_id The task ID for the task to remove from the queue.
    /// \param removed_tasks If the task specified by task_id is successfully
    //  removed from the queue, the task data is appended to the vector.
    /// \return Whether the removal succeeds.
    bool RemoveTask(const TaskID &task_id, std::vector<Task> &removed_tasks);

    /// \brief Check if the queue contains a specific task id.
    ///
    /// \param task_id The task ID for the task.
    /// \return Whether the task_id exists in this queue.
    bool HasTask(const TaskID &task_id) const;

    /// \brief Remove the task list of the queue.
    /// \return A list of tasks contained in this queue.
    const std::list<Task> &GetTasks() const;

   private:
    // A list of tasks.
    std::list<Task> task_list_;
    // A hash to speed up looking up a task.
    std::unordered_map<TaskID, std::list<Task>::iterator> task_map_;
  };

 private:
  /// Tasks that are destined for actors that have not yet been created.
  TaskQueue methods_waiting_for_actor_creation_;
  /// Tasks that are waiting for an object dependency to appear locally.
  TaskQueue waiting_tasks_;
  /// Tasks whose object dependencies are locally available, but that are
  /// waiting to be scheduled.
  TaskQueue placeable_tasks_;
  /// Tasks ready for dispatch, but that are waiting for a worker.
  TaskQueue ready_tasks_;
  /// Tasks that are running on a worker.
  TaskQueue running_tasks_;
  /// Tasks that were dispatched to a worker but are blocked on a data
  /// dependency that was missing at runtime.
  TaskQueue blocked_tasks_;
  /// Tasks that require resources that are not available on any of the nodes
  /// in the cluster.
  TaskQueue infeasible_tasks_;
  /// The set of currently running driver tasks. These are empty tasks that are
  /// started by a driver process on initialization.
  std::unordered_set<TaskID> driver_task_ids_;

  /// \brief Return all resource demand associated with the specified task queue.
  ///
  /// \param task_queue The task queue for which aggregate resource demand is calculated.
  /// \return Aggregate resource demand.
  ResourceSet GetQueueResources(const TaskQueue &task_queue) const;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_QUEUE_H
