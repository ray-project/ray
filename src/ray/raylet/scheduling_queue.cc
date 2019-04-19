#include "scheduling_queue.h"

#include <sstream>

#include "ray/stats/stats.h"
#include "ray/status.h"

namespace {

static constexpr const char *task_state_strings[] = {
    "placeable", "waiting",    "ready",
    "running",   "infeasible", "waiting_for_actor_creation"};
static_assert(sizeof(task_state_strings) / sizeof(const char *) ==
                  static_cast<int>(ray::raylet::TaskState::kNumTaskQueues),
              "Must specify a TaskState name for every task queue");

inline const char *GetTaskStateString(ray::raylet::TaskState task_state) {
  return task_state_strings[static_cast<int>(task_state)];
}

// Helper function to get tasks for a driver from a given state.
template <typename TaskQueue>
inline void GetDriverTasksFromQueue(const TaskQueue &queue,
                                    const ray::DriverID &driver_id,
                                    std::unordered_set<ray::TaskID> &task_ids) {
  const auto &tasks = queue.GetTasks();
  for (const auto &task : tasks) {
    auto const &spec = task.GetTaskSpecification();
    if (driver_id == spec.DriverId()) {
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
    if (actor_id == spec.ActorId()) {
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
  const auto &resources = task.GetTaskSpecification().GetRequiredResources();
  tasks_with_resources_[resources].push_back(task_id);
  return TaskQueue::AppendTask(task_id, task);
}

bool ReadyQueue::RemoveTask(const TaskID &task_id, std::vector<Task> *removed_tasks) {
  if (task_map_.find(task_id) != task_map_.end()) {
    const auto &resources =
        task_map_[task_id]->GetTaskSpecification().GetRequiredResources();
    tasks_with_resources_[resources].erase(task_id);
  }
  return TaskQueue::RemoveTask(task_id, removed_tasks);
}

const std::unordered_map<ResourceSet, ordered_set<TaskID>>
    &ReadyQueue::GetTasksWithResources() const {
  return tasks_with_resources_;
}

const std::list<Task> &SchedulingQueue::GetTasks(TaskState task_state) const {
  const auto &queue = GetTaskQueue(task_state);
  return queue->GetTasks();
}

const std::unordered_map<ResourceSet, ordered_set<TaskID>>
    &SchedulingQueue::GetReadyTasksWithResources() const {
  return ready_queue_->GetTasksWithResources();
}

const Task &SchedulingQueue::GetTaskOfState(const TaskID &task_id,
                                            TaskState task_state) const {
  const auto &queue = GetTaskQueue(task_state);
  return queue->GetTask(task_id);
}

ResourceSet SchedulingQueue::GetResourceLoad() const {
  // TODO(atumanov): consider other types of tasks as part of load.
  return ready_queue_->GetCurrentResourceLoad();
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
    const auto driver_ids = GetDriverTaskIds();
    for (auto it = task_ids.begin(); it != task_ids.end();) {
      if (driver_ids.count(*it) == 1) {
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
void SchedulingQueue::RemoveTasksFromQueue(
    ray::raylet::TaskState task_state, std::unordered_set<ray::TaskID> &task_ids,
    std::vector<ray::raylet::Task> *removed_tasks) {
  auto &queue = GetTaskQueue(task_state);
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    const auto &task_id = *it;
    if (queue->RemoveTask(task_id, removed_tasks)) {
      RAY_LOG(DEBUG) << "Removed task " << task_id << " from "
                     << GetTaskStateString(task_state) << " queue";
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
           TaskState::PLACEABLE, TaskState::WAITING, TaskState::READY, TaskState::RUNNING,
           TaskState::INFEASIBLE, TaskState::WAITING_FOR_ACTOR_CREATION,
       }) {
    RemoveTasksFromQueue(task_state, task_ids, &removed_tasks);
  }

  RAY_CHECK(task_ids.size() == 0);
  return removed_tasks;
}

Task SchedulingQueue::RemoveTask(const TaskID &task_id, TaskState *removed_task_state) {
  std::vector<Task> removed_tasks;
  std::unordered_set<TaskID> task_id_set = {task_id};
  // Try to find the task to remove in the queues.
  for (const auto &task_state : {
           TaskState::PLACEABLE, TaskState::WAITING, TaskState::READY, TaskState::RUNNING,
           TaskState::INFEASIBLE, TaskState::WAITING_FOR_ACTOR_CREATION,
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
  RAY_CHECK(removed_tasks.size() == 1);
  const auto &task = removed_tasks.front();
  RAY_CHECK(task.GetTaskSpecification().TaskId() == task_id);
  return task;
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

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForDriver(
    const DriverID &driver_id) const {
  std::unordered_set<TaskID> task_ids;
  for (const auto &task_queue : task_queues_) {
    GetDriverTasksFromQueue(*task_queue, driver_id, task_ids);
  }
  return task_ids;
}

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForActor(
    const ActorID &actor_id) const {
  std::unordered_set<TaskID> task_ids;
  for (const auto &task_queue : task_queues_) {
    GetActorTasksFromQueue(*task_queue, actor_id, task_ids);
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

void SchedulingQueue::AddDriverTaskId(const TaskID &driver_id) {
  RAY_LOG(DEBUG) << "Added driver task " << driver_id;
  auto inserted = driver_task_ids_.insert(driver_id);
  RAY_CHECK(inserted.second);
}

void SchedulingQueue::RemoveDriverTaskId(const TaskID &driver_id) {
  RAY_LOG(DEBUG) << "Removed driver task " << driver_id;
  auto erased = driver_task_ids_.erase(driver_id);
  RAY_CHECK(erased == 1);
}

const std::unordered_set<TaskID> &SchedulingQueue::GetDriverTaskIds() const {
  return driver_task_ids_;
}

std::string SchedulingQueue::DebugString() const {
  std::stringstream result;
  result << "SchedulingQueue:";
  for (const auto &task_state : {
           TaskState::PLACEABLE, TaskState::WAITING, TaskState::READY, TaskState::RUNNING,
           TaskState::INFEASIBLE, TaskState::WAITING_FOR_ACTOR_CREATION,
       }) {
    result << "\n- num " << GetTaskStateString(task_state)
           << " tasks: " << GetTaskQueue(task_state)->GetTasks().size();
  }
  result << "\n- num tasks blocked: " << blocked_task_ids_.size();
  return result.str();
}

void SchedulingQueue::RecordMetrics() const {
  for (const auto &task_state : {
           TaskState::PLACEABLE, TaskState::WAITING, TaskState::READY, TaskState::RUNNING,
           TaskState::INFEASIBLE, TaskState::WAITING_FOR_ACTOR_CREATION,
       }) {
    stats::SchedulingQueueStats().Record(
        static_cast<double>(GetTaskQueue(task_state)->GetTasks().size()),
        {{stats::ValueTypeKey,
          std::string("num_") + GetTaskStateString(task_state) + "_tasks"}});
  }
}

}  // namespace raylet

}  // namespace ray
