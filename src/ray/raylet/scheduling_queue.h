// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <array>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/common/task/task.h"
#include "ray/util/logging.h"
#include "ray/util/ordered_set.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace raylet {

enum class TaskState {
  // The task may be placed on a node.
  PLACEABLE,
  // The task has been placed on a node and is waiting for some object
  // dependencies to become local.
  WAITING,
  // The task has been placed on a node, all dependencies are satisfied, and is
  // waiting for resources to run.
  READY,
  // The task is running on a worker. The task may also be blocked in a ray.get
  // or ray.wait call, in which case it also has state BLOCKED.
  RUNNING,
  // The task has resources that cannot be satisfied by any node, as far as we
  // know.
  INFEASIBLE,
  // The task is an actor method and is waiting to learn where the actor was
  // created.
  WAITING_FOR_ACTOR_CREATION,
  // Swap queue for tasks that are in between states. This can happen when a
  // task is removed from one queue, and an async callback is responsible for
  // re-queuing the task. For example, a READY task that has just been assigned
  // to a worker will get moved to the SWAP queue while waiting for a response
  // from the worker. If the worker accepts the task, the task will be added to
  // the RUNNING queue, else it will be returned to READY.
  SWAP,
  // The number of task queues. All states that precede this enum must have an
  // associated TaskQueue in SchedulingQueue. All states that succeed
  // this enum do not have an associated TaskQueue, since the tasks
  // in those states may not have any associated task data.
  kNumTaskQueues,
  // The task is running but blocked in a ray.get or ray.wait call. Tasks that
  // were explicitly assigned by us may be both BLOCKED and RUNNING, while
  // tasks that were created out-of-band (e.g., the application created
  // multiple threads) are only BLOCKED.
  BLOCKED,
  // The task is a driver task.
  DRIVER,
};

class TaskQueue {
 public:
  /// TaskQueue destructor.
  virtual ~TaskQueue() {}

  /// \brief Append a task to queue.
  ///
  /// \param task_id The task ID for the task to append.
  /// \param task The task to append to the queue.
  /// \return Whether the append operation succeeds.
  virtual bool AppendTask(const TaskID &task_id, const Task &task);

  /// \brief Remove a task from queue.
  ///
  /// \param task_id The task ID for the task to remove from the queue.
  /// \param removed_tasks If the task specified by task_id is successfully
  ///  removed from the queue, the task data is appended to the vector. Can
  ///  be a nullptr, in which case nothing is appended.
  /// \return Whether the removal succeeds.
  virtual bool RemoveTask(const TaskID &task_id,
                          std::vector<Task> *removed_tasks = nullptr);

  /// \brief Check if the queue contains a specific task id.
  ///
  /// \param task_id The task ID for the task.
  /// \return Whether the task_id exists in this queue.
  bool HasTask(const TaskID &task_id) const;

  /// \brief Return the task list of the queue.
  ///
  /// \return A list of tasks contained in this queue.
  const std::list<Task> &GetTasks() const;

  /// Get a task from the queue. The caller must ensure that the task is in
  /// the queue.
  ///
  /// \return The task.
  const Task &GetTask(const TaskID &task_id) const;

  /// \brief Return all resource demand associated with the ready queue.
  ///
  /// \return Aggregate resource demand from ready tasks.
  const ResourceSet &GetTotalResourceLoad() const;

  /// \brief Get the resources required by the tasks in the queue.
  ///
  /// \return A map from resource shape key to the number of tasks queued that
  /// require that shape.
  const std::unordered_map<SchedulingClass, uint64_t> &GetResourceLoadByShape() const;

 protected:
  /// A list of tasks.
  std::list<Task> task_list_;
  /// A hash to speed up looking up a task.
  std::unordered_map<TaskID, std::list<Task>::iterator> task_map_;
  /// Aggregate resources of all the tasks in this queue.
  ResourceSet total_resource_load_;
  /// Required resources for all the tasks in this queue. This is a
  /// map from resource shape key to number of tasks queued that require that
  /// shape.
  std::unordered_map<SchedulingClass, uint64_t> resource_load_by_shape_;
};

class ReadyQueue : public TaskQueue {
 public:
  ReadyQueue(){};

  ReadyQueue(const ReadyQueue &other) = delete;

  /// ReadyQueue destructor.
  virtual ~ReadyQueue() {}

  /// \brief Append a task to queue.
  ///
  /// \param task_id The task ID for the task to append.
  /// \param task The task to append to the queue.
  /// \return Whether the append operation succeeds.
  bool AppendTask(const TaskID &task_id, const Task &task) override;

  /// \brief Remove a task from queue.
  ///
  /// \param task_id The task ID for the task to remove from the queue.
  /// \return Whether the removal succeeds.
  bool RemoveTask(const TaskID &task_id, std::vector<Task> *removed_tasks) override;

  /// \brief Get a mapping from resource shape to tasks.
  ///
  /// \return Mapping from resource set to task IDs with these resource requirements.
  const std::unordered_map<SchedulingClass, ordered_set<TaskID>> &GetTasksByClass() const;

 private:
  /// Index from task description to tasks queued of that type.
  std::unordered_map<SchedulingClass, ordered_set<TaskID>> tasks_by_class_;
};

/// \class SchedulingQueue
///
/// Encapsulates task queues.
// (See design_docs/task_states.rst for the state transition diagram.)
class SchedulingQueue {
 public:
  /// Create a scheduling queue.
  SchedulingQueue() : ready_queue_(std::make_shared<ReadyQueue>()) {
    for (const auto &task_state : {
             TaskState::PLACEABLE,
             TaskState::WAITING,
             TaskState::READY,
             TaskState::RUNNING,
             TaskState::INFEASIBLE,
             TaskState::WAITING_FOR_ACTOR_CREATION,
             TaskState::SWAP,
         }) {
      if (task_state == TaskState::READY) {
        task_queues_[static_cast<int>(task_state)] = ready_queue_;
      } else {
        task_queues_[static_cast<int>(task_state)] = std::make_shared<TaskQueue>();
      }
    }
  }

  /// SchedulingQueue destructor.
  virtual ~SchedulingQueue() {}

  /// \brief Check if the queue contains a specific task id.
  ///
  /// \param task_id The task ID for the task.
  /// \return Whether the task_id exists in the queue.
  bool HasTask(const TaskID &task_id) const;

  /// \brief Get all tasks in the given state.
  ///
  /// \param task_state The requested task state. This must correspond to one
  /// of the task queues (has value < TaskState::kNumTaskQueues).
  const std::list<Task> &GetTasks(TaskState task_state) const;

  /// Get a reference to the queue of ready tasks.
  ///
  /// \return A reference to the queue of ready tasks.
  const std::unordered_map<SchedulingClass, ordered_set<TaskID>> &GetReadyTasksByClass()
      const;

  /// Get a task from the queue of a given state. The caller must ensure that
  /// the task has the given state.
  ///
  /// \param task_id The task to get.
  /// \param task_state The state that the requested task should be in.
  /// \return The task.
  const Task &GetTaskOfState(const TaskID &task_id, TaskState task_state) const;

  /// \brief Return an aggregate resource set for all tasks exerting load on this raylet.
  ///
  /// \return A resource set with aggregate resource information about resource load on
  /// this raylet.
  ResourceSet GetTotalResourceLoad() const;

  /// \brief Return a summary of the requests in the ready and infeasible
  /// queues.
  ///
  /// \return A message summarizing the number of requests, sorted by shape, in
  /// the ready and infeasible queues.
  rpc::ResourceLoad GetResourceLoadByShape(int64_t max_shapes = -1) const;

  /// Get the tasks in the blocked state.
  ///
  /// \return A const reference to the tasks that are are blocked on a data
  /// dependency discovered to be missing at runtime. These include RUNNING
  /// tasks that were explicitly assigned to a worker by us, as well as tasks
  /// that were created out-of-band (e.g., the application created
  // multiple threads) are only BLOCKED.
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;

  /// Get the set of driver task IDs.
  ///
  /// \return A const reference to the set of driver task IDs. These are empty
  /// tasks used to represent drivers.
  const std::unordered_set<TaskID> &GetDriverTaskIds() const;

  /// Remove tasks from the task queue.
  ///
  /// \param task_ids The set of task IDs to remove from the queue. The
  /// corresponding tasks must be contained in the queue. The IDs of removed
  /// tasks will be erased from the set.
  /// \return A vector of the tasks that were removed.
  std::vector<Task> RemoveTasks(std::unordered_set<TaskID> &task_ids);

  /// Remove a task from the task queue.
  ///
  /// \param task_id The task ID to remove from the queue. The corresponding
  /// task must be contained in the queue.
  /// \param task The removed task will be written here, if any.
  /// \param task_state If this is not nullptr, then the state of the removed
  /// task will be written here.
  /// \return true if the task was removed, false if it is not in the queue.
  bool RemoveTask(const TaskID &task_id, Task *removed_task,
                  TaskState *removed_task_state = nullptr);

  /// Remove a driver task ID. This is an empty task used to represent a driver.
  ///
  /// \param The driver task ID to remove.
  void RemoveDriverTaskId(const TaskID &task_id);

  /// Add tasks to the given queue.
  ///
  /// \param tasks The tasks to queue.
  /// \param task_state The state of the tasks to queue. The requested task
  /// state must correspond to one of the task queues (has value <
  /// TaskState::kNumTaskQueues).
  void QueueTasks(const std::vector<Task> &tasks, TaskState task_state);

  /// Add a task ID in the blocked state. These are tasks that have been
  /// dispatched to a worker but are blocked on a data dependency that was
  /// discovered to be missing at runtime.
  ///
  /// \param task_id The task to mark as blocked.
  void AddBlockedTaskId(const TaskID &task_id);

  /// Remove a task ID in the blocked state. These are tasks that have been
  /// dispatched to a worker but were blocked on a data dependency that was
  /// discovered to be missing at runtime.
  ///
  /// \param task_id The task to mark as unblocked.
  void RemoveBlockedTaskId(const TaskID &task_id);

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

  /// \brief Get all the task IDs for a job.
  ///
  /// \param job_id All the tasks that have the given job_id are returned.
  /// \return All the tasks that have the given job ID.
  std::unordered_set<TaskID> GetTaskIdsForJob(const JobID &job_id) const;

  /// \brief Get all the task IDs for an actor.
  ///
  /// \param actor_id All the tasks that have the given actor_id are returned.
  /// \return All the tasks that have the given actor ID.
  std::unordered_set<TaskID> GetTaskIdsForActor(const ActorID &actor_id) const;

  /// Returns the number of running tasks in this class.
  ///
  /// \return int.
  int NumRunning(const SchedulingClass &cls) const;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Record metrics.
  void RecordMetrics() const;

 private:
  /// Get the task queue in the given state. The requested task state must
  /// correspond to one of the task queues (has value <
  /// TaskState::kNumTaskQueues).
  const std::shared_ptr<TaskQueue> &GetTaskQueue(TaskState task_state) const;

  /// A helper function to remove tasks from a given queue. The requested task
  /// state must correspond to one of the task queues (has value <
  /// TaskState::kNumTaskQueues).
  void RemoveTasksFromQueue(ray::raylet::TaskState task_state,
                            std::unordered_set<ray::TaskID> &task_ids,
                            std::vector<ray::Task> *removed_tasks);

  /// A helper function to filter out tasks of a given state from the set of
  /// task IDs. The requested task state must correspond to one of the task
  /// queues (has value < TaskState::kNumTaskQueues).
  void FilterStateFromQueue(std::unordered_set<ray::TaskID> &task_ids,
                            TaskState task_state) const;

  // A pointer to the ready queue.
  const std::shared_ptr<ReadyQueue> ready_queue_;
  /// Track the breakdown of tasks by class in the RUNNING queue.
  std::unordered_map<SchedulingClass, int32_t> num_running_tasks_;
  // A pointer to the task queues. These contain all tasks that have a task
  // state < TaskState::kNumTaskQueues.
  std::array<std::shared_ptr<TaskQueue>, static_cast<int>(TaskState::kNumTaskQueues)>
      task_queues_;
  /// Tasks that were dispatched to a worker but are blocked on a data
  /// dependency that was missing at runtime.
  std::unordered_set<TaskID> blocked_task_ids_;
  /// The set of currently running driver tasks. These are empty tasks that are
  /// started by a driver process on initialization.
  std::unordered_set<TaskID> driver_task_ids_;
};

}  // namespace raylet

}  // namespace ray
