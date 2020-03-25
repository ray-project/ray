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

#include "scheduling_queue.h"

#include <sstream>

#include "ray/common/status.h"
#include "ray/stats/stats.h"

namespace {

static constexpr const char *task_state_strings[] = {
    "placeable", "waiting",    "ready",
    "running",   "infeasible", "waiting for actor creation",
    "swap"};
static_assert(sizeof(task_state_strings) / sizeof(const char *) ==
                  static_cast<int>(ray::raylet::TaskState::kNumTaskQueues),
              "Must specify a TaskState name for every task queue");

inline const char *GetTaskStateString(ray::raylet::TaskState task_state) {
  return task_state_strings[static_cast<int>(task_state)];
}

// Helper function to get tasks for a job from a given state.
template <typename TaskQueue>
inline void GetTasksForJobFromQueue(const TaskQueue &queue, const ray::JobID &job_id,
                                    std::unordered_set<ray::TaskID> &task_ids) {
  const auto &tasks = queue.GetTasks();
  for (const auto &task : tasks) {
    auto const &spec = task.GetTaskSpecification();
    if (job_id == spec.JobId()) {
      task_ids.insert(spec.TaskId());
    }
  }
}

// Helper function to get tasks for an actor from a given state.
template <typename TaskQueue>
inline void GetActorTasksFromQueue(const TaskQueue &queue, const ray::ActorID &actor_id,
                                   std::unordered_set<ray::TaskID> &task_ids) {
  const auto &tasks = queue.GetTasks();
  for (const auto &task : tasks) {
    auto const &spec = task.GetTaskSpecification();
    if (spec.IsActorTask() && actor_id == spec.ActorId()) {
      task_ids.insert(spec.TaskId());
    }
  }
}

}  // namespace

namespace ray {

namespace raylet {

bool TaskQueue::AppendTask(const TaskID &task_id, const Task &task) {
  RAY_CHECK(task_map_.find(task_id) == task_map_.end());
  auto list_iterator = task_list_.insert(task_list_.end(), task);
  task_map_[task_id] = list_iterator;
  // Resource bookkeeping
  current_resource_load_.AddResources(task.GetTaskSpecification().GetRequiredResources());
  return true;
}

bool TaskQueue::RemoveTask(const TaskID &task_id, std::vector<Task> *removed_tasks) {
  auto task_found_iterator = task_map_.find(task_id);
  if (task_found_iterator == task_map_.end()) {
    return false;
  }

  auto list_iterator = task_found_iterator->second;
  // Resource bookkeeping
  current_resource_load_.SubtractResourcesStrict(
      list_iterator->GetTaskSpecification().GetRequiredResources());
  if (removed_tasks) {
    removed_tasks->push_back(std::move(*list_iterator));
  }
  task_map_.erase(task_found_iterator);
  task_list_.erase(list_iterator);
  return true;
}

bool TaskQueue::HasTask(const TaskID &task_id) const {
  return task_map_.find(task_id) != task_map_.end();
}

const std::list<Task> &TaskQueue::GetTasks() const { return task_list_; }

const Task &TaskQueue::GetTask(const TaskID &task_id) const {
  auto it = task_map_.find(task_id);
  RAY_CHECK(it != task_map_.end());
  return *it->second;
}

const ResourceSet &TaskQueue::GetCurrentResourceLoad() const {
  return current_resource_load_;
}

bool ReadyQueue::AppendTask(const TaskID &task_id, const Task &task) {
  const auto &scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  tasks_by_class_[scheduling_class].push_back(task_id);
  return TaskQueue::AppendTask(task_id, task);
}

bool ReadyQueue::RemoveTask(const TaskID &task_id, std::vector<Task> *removed_tasks) {
  if (task_map_.find(task_id) != task_map_.end()) {
    const auto &scheduling_class =
        task_map_[task_id]->GetTaskSpecification().GetSchedulingClass();
    tasks_by_class_[scheduling_class].erase(task_id);
  }
  return TaskQueue::RemoveTask(task_id, removed_tasks);
}

const std::unordered_map<SchedulingClass, ordered_set<TaskID>>
    &ReadyQueue::GetTasksByClass() const {
  return tasks_by_class_;
}

const std::list<Task> &SchedulingQueue::GetTasks(TaskState task_state) const {
  const auto &queue = GetTaskQueue(task_state);
  return queue->GetTasks();
}

const std::unordered_map<SchedulingClass, ordered_set<TaskID>>
    &SchedulingQueue::GetReadyTasksByClass() const {
  return ready_queue_->GetTasksByClass();
}

const Task &SchedulingQueue::GetTaskOfState(const TaskID &task_id,
                                            TaskState task_state) const {
  const auto &queue = GetTaskQueue(task_state);
  return queue->GetTask(task_id);
}

ResourceSet SchedulingQueue::GetResourceLoad() const {
  auto load = ready_queue_->GetCurrentResourceLoad();
  // Also take into account infeasible tasks so they show up for autoscaling.
  load.AddResources(
      task_queues_[static_cast<int>(TaskState::INFEASIBLE)]->GetCurrentResourceLoad());
  return load;
}

const std::unordered_set<TaskID> &SchedulingQueue::GetBlockedTaskIds() const {
  return blocked_task_ids_;
}

void SchedulingQueue::FilterStateFromQueue(std::unordered_set<ray::TaskID> &task_ids,
                                           TaskState task_state) const {
  auto &queue = GetTaskQueue(task_state);
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    if (queue->HasTask(*it)) {
      it = task_ids.erase(it);
    } else {
      it++;
    }
  }
}

void SchedulingQueue::FilterState(std::unordered_set<TaskID> &task_ids,
                                  TaskState filter_state) const {
  switch (filter_state) {
  case TaskState::PLACEABLE:
    FilterStateFromQueue(task_ids, TaskState::PLACEABLE);
    break;
  case TaskState::WAITING_FOR_ACTOR_CREATION:
    FilterStateFromQueue(task_ids, TaskState::WAITING_FOR_ACTOR_CREATION);
    break;
  case TaskState::WAITING:
    FilterStateFromQueue(task_ids, TaskState::WAITING);
    break;
  case TaskState::READY:
    FilterStateFromQueue(task_ids, TaskState::READY);
    break;
  case TaskState::RUNNING:
    FilterStateFromQueue(task_ids, TaskState::RUNNING);
    break;
  case TaskState::INFEASIBLE:
    FilterStateFromQueue(task_ids, TaskState::INFEASIBLE);
    break;
  case TaskState::SWAP:
    FilterStateFromQueue(task_ids, TaskState::SWAP);
    break;
  case TaskState::BLOCKED: {
    const auto blocked_ids = GetBlockedTaskIds();
    for (auto it = task_ids.begin(); it != task_ids.end();) {
      if (blocked_ids.count(*it) == 1) {
        it = task_ids.erase(it);
      } else {
        it++;
      }
    }
  } break;
  case TaskState::DRIVER: {
    const auto driver_task_ids = GetDriverTaskIds();
    for (auto it = task_ids.begin(); it != task_ids.end();) {
      if (driver_task_ids.count(*it) == 1) {
        it = task_ids.erase(it);
      } else {
        it++;
      }
    }
  } break;
  default:
    RAY_LOG(FATAL) << "Attempting to filter tasks on unrecognized state "
                   << static_cast<std::underlying_type<TaskState>::type>(filter_state);
  }
}

const std::shared_ptr<TaskQueue> &SchedulingQueue::GetTaskQueue(
    TaskState task_state) const {
  RAY_CHECK(task_state < TaskState::kNumTaskQueues)
      << static_cast<int>(task_state) << "Task state " << static_cast<int>(task_state)
      << " does not correspond to a task queue";
  return task_queues_[static_cast<int>(task_state)];
}

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void SchedulingQueue::RemoveTasksFromQueue(ray::raylet::TaskState task_state,
                                           std::unordered_set<ray::TaskID> &task_ids,
                                           std::vector<ray::Task> *removed_tasks) {
  auto &queue = GetTaskQueue(task_state);
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    const auto &task_id = *it;
    if (queue->RemoveTask(task_id, removed_tasks)) {
      RAY_LOG(DEBUG) << "Removed task " << task_id << " from "
                     << GetTaskStateString(task_state) << " queue";
      if (task_state == TaskState::RUNNING) {
        num_running_tasks_
            [removed_tasks->back().GetTaskSpecification().GetSchedulingClass()] -= 1;
      }
      it = task_ids.erase(it);
    } else {
      it++;
    }
  }
}

std::vector<Task> SchedulingQueue::RemoveTasks(std::unordered_set<TaskID> &task_ids) {
  // List of removed tasks to be returned.
  std::vector<Task> removed_tasks;
  // Try to find the tasks to remove from the queues.
  for (const auto &task_state : {
           TaskState::PLACEABLE,
           TaskState::WAITING,
           TaskState::READY,
           TaskState::RUNNING,
           TaskState::INFEASIBLE,
           TaskState::WAITING_FOR_ACTOR_CREATION,
           TaskState::SWAP,
       }) {
    RemoveTasksFromQueue(task_state, task_ids, &removed_tasks);
  }

  RAY_CHECK(task_ids.size() == 0);
  return removed_tasks;
}

bool SchedulingQueue::RemoveTask(const TaskID &task_id, Task *removed_task,
                                 TaskState *removed_task_state) {
  std::vector<Task> removed_tasks;
  std::unordered_set<TaskID> task_id_set = {task_id};
  // Try to find the task to remove in the queues.
  for (const auto &task_state : {
           TaskState::PLACEABLE,
           TaskState::WAITING,
           TaskState::READY,
           TaskState::RUNNING,
           TaskState::INFEASIBLE,
           TaskState::WAITING_FOR_ACTOR_CREATION,
           TaskState::SWAP,
       }) {
    RemoveTasksFromQueue(task_state, task_id_set, &removed_tasks);
    if (task_id_set.empty()) {
      // The task was removed from the current queue.
      if (removed_task_state != nullptr) {
        // If the state of the removed task was requested, then set it with the
        // current queue's state.
        *removed_task_state = task_state;
      }
      break;
    }
  }

  // Make sure we got the removed task.
  if (removed_tasks.size() == 1) {
    *removed_task = removed_tasks.front();
    RAY_CHECK(removed_task->GetTaskSpecification().TaskId() == task_id);
    return true;
  }
  RAY_LOG(DEBUG) << "Task " << task_id
                 << " that is to be removed could not be found any more."
                 << " Probably its driver was removed.";
  return false;
}

void SchedulingQueue::MoveTasks(std::unordered_set<TaskID> &task_ids, TaskState src_state,
                                TaskState dst_state) {
  std::vector<Task> removed_tasks;

  // Remove the tasks from the specified source queue.
  switch (src_state) {
  case TaskState::PLACEABLE:
    RemoveTasksFromQueue(TaskState::PLACEABLE, task_ids, &removed_tasks);
    break;
  case TaskState::WAITING:
    RemoveTasksFromQueue(TaskState::WAITING, task_ids, &removed_tasks);
    break;
  case TaskState::READY:
    RemoveTasksFromQueue(TaskState::READY, task_ids, &removed_tasks);
    break;
  case TaskState::RUNNING:
    RemoveTasksFromQueue(TaskState::RUNNING, task_ids, &removed_tasks);
    break;
  case TaskState::INFEASIBLE:
    RemoveTasksFromQueue(TaskState::INFEASIBLE, task_ids, &removed_tasks);
    break;
  case TaskState::SWAP:
    RemoveTasksFromQueue(TaskState::SWAP, task_ids, &removed_tasks);
    break;
  default:
    RAY_LOG(FATAL) << "Attempting to move tasks from unrecognized state "
                   << static_cast<std::underlying_type<TaskState>::type>(src_state);
  }

  // Make sure that all tasks were able to be moved.
  RAY_CHECK(task_ids.empty());

  // Add the tasks to the specified destination queue.
  switch (dst_state) {
  case TaskState::PLACEABLE:
    QueueTasks(removed_tasks, TaskState::PLACEABLE);
    break;
  case TaskState::WAITING:
    QueueTasks(removed_tasks, TaskState::WAITING);
    break;
  case TaskState::READY:
    QueueTasks(removed_tasks, TaskState::READY);
    break;
  case TaskState::RUNNING:
    QueueTasks(removed_tasks, TaskState::RUNNING);
    break;
  case TaskState::INFEASIBLE:
    QueueTasks(removed_tasks, TaskState::INFEASIBLE);
    break;
  case TaskState::SWAP:
    QueueTasks(removed_tasks, TaskState::SWAP);
    break;
  default:
    RAY_LOG(FATAL) << "Attempting to move tasks to unrecognized state "
                   << static_cast<std::underlying_type<TaskState>::type>(dst_state);
  }
}

void SchedulingQueue::QueueTasks(const std::vector<Task> &tasks, TaskState task_state) {
  auto &queue = GetTaskQueue(task_state);
  for (const auto &task : tasks) {
    RAY_LOG(DEBUG) << "Added task " << task.GetTaskSpecification().TaskId() << " to "
                   << GetTaskStateString(task_state) << " queue";
    if (task_state == TaskState::RUNNING) {
      num_running_tasks_[task.GetTaskSpecification().GetSchedulingClass()] += 1;
    }
    queue->AppendTask(task.GetTaskSpecification().TaskId(), task);
  }
}

bool SchedulingQueue::HasTask(const TaskID &task_id) const {
  for (const auto &task_queue : task_queues_) {
    if (task_queue->HasTask(task_id)) {
      return true;
    }
  }
  return false;
}

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForJob(const JobID &job_id) const {
  std::unordered_set<TaskID> task_ids;
  for (const auto &task_queue : task_queues_) {
    GetTasksForJobFromQueue(*task_queue, job_id, task_ids);
  }
  return task_ids;
}

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForActor(
    const ActorID &actor_id) const {
  std::unordered_set<TaskID> task_ids;
  int swap = static_cast<int>(TaskState::SWAP);
  int i = 0;
  for (const auto &task_queue : task_queues_) {
    // This is a hack to make sure that we don't remove tasks from the SWAP
    // queue, since these are always guaranteed to be removed and eventually
    // resubmitted if necessary by the node manager.
    if (i != swap) {
      GetActorTasksFromQueue(*task_queue, actor_id, task_ids);
    }
    i++;
  }
  return task_ids;
}

void SchedulingQueue::AddBlockedTaskId(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Added blocked task " << task_id;
  auto inserted = blocked_task_ids_.insert(task_id);
  RAY_CHECK(inserted.second);
}

void SchedulingQueue::RemoveBlockedTaskId(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Removed blocked task " << task_id;
  auto erased = blocked_task_ids_.erase(task_id);
  RAY_CHECK(erased == 1);
}

void SchedulingQueue::AddDriverTaskId(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Added driver task " << task_id;
  auto inserted = driver_task_ids_.insert(task_id);
  RAY_CHECK(inserted.second);
}

void SchedulingQueue::RemoveDriverTaskId(const TaskID &task_id) {
  RAY_LOG(DEBUG) << "Removed driver task " << task_id;
  auto erased = driver_task_ids_.erase(task_id);
  RAY_CHECK(erased == 1);
}

const std::unordered_set<TaskID> &SchedulingQueue::GetDriverTaskIds() const {
  return driver_task_ids_;
}

int SchedulingQueue::NumRunning(const SchedulingClass &cls) const {
  auto it = num_running_tasks_.find(cls);
  if (it == num_running_tasks_.end()) {
    return 0;
  } else {
    return it->second;
  }
}

std::string SchedulingQueue::DebugString() const {
  std::stringstream result;
  result << "SchedulingQueue:";
  for (size_t i = 0; i < static_cast<int>(ray::raylet::TaskState::kNumTaskQueues); i++) {
    TaskState task_state = static_cast<TaskState>(i);
    result << "\n- num " << GetTaskStateString(task_state)
           << " tasks: " << GetTaskQueue(task_state)->GetTasks().size();
  }
  result << "\n- num tasks blocked: " << blocked_task_ids_.size();
  result << "\nScheduledTaskCounts:";
  size_t total = 0;
  for (const auto &pair : num_running_tasks_) {
    result << "\n- ";
    auto desc = TaskSpecification::GetSchedulingClassDescriptor(pair.first);
    result << desc.second->ToString();
    result << desc.first.ToString();
    result << ": " << pair.second;
    total += pair.second;
  }
  RAY_CHECK(total == GetTaskQueue(TaskState::RUNNING)->GetTasks().size())
      << total << " vs " << GetTaskQueue(TaskState::RUNNING)->GetTasks().size();
  return result.str();
}

void SchedulingQueue::RecordMetrics() const {
  for (size_t i = 0; i < static_cast<int>(ray::raylet::TaskState::kNumTaskQueues); i++) {
    TaskState task_state = static_cast<TaskState>(i);
    stats::SchedulingQueueStats().Record(
        static_cast<double>(GetTaskQueue(task_state)->GetTasks().size()),
        {{stats::ValueTypeKey,
          std::string("num_") + GetTaskStateString(task_state) + "_tasks"}});
  }
}

}  // namespace raylet

}  // namespace ray
