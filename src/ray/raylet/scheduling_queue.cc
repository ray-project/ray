#include "scheduling_queue.h"

#include "ray/status.h"

namespace {

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void RemoveTasksFromQueue(std::list<ray::raylet::Task> &queue,
                          std::unordered_set<ray::TaskID> &task_ids,
                          std::vector<ray::raylet::Task> &removed_tasks) {
  for (auto it = queue.begin(); it != queue.end();) {
    auto task_id = task_ids.find(it->GetTaskSpecification().TaskId());
    if (task_id != task_ids.end()) {
      task_ids.erase(task_id);
      removed_tasks.push_back(std::move(*it));
      it = queue.erase(it);
    } else {
      it++;
    }
  }
}

// Helper function to queue the given tasks to the given queue.
inline void QueueTasks(std::list<ray::raylet::Task> &queue,
                       const std::vector<ray::raylet::Task> &tasks) {
  queue.insert(queue.end(), tasks.begin(), tasks.end());
}

// Helper function to filter out tasks of a given state.
inline void FilterStateFromQueue(const std::list<ray::raylet::Task> &queue,
                                 std::unordered_set<ray::TaskID> &task_ids,
                                 ray::raylet::TaskState filter_state) {
  for (auto it = queue.begin(); it != queue.end(); it++) {
    auto task_id = task_ids.find(it->GetTaskSpecification().TaskId());
    if (task_id != task_ids.end()) {
      task_ids.erase(task_id);
    }
  }
}

}  // namespace

namespace ray {

namespace raylet {

const std::list<Task> &SchedulingQueue::GetMethodsWaitingForActorCreation() const {
  return this->methods_waiting_for_actor_creation_;
}

const std::list<Task> &SchedulingQueue::GetWaitingTasks() const {
  return this->waiting_tasks_;
}

const std::list<Task> &SchedulingQueue::GetPlaceableTasks() const {
  return this->placeable_tasks_;
}

const std::list<Task> &SchedulingQueue::GetReadyTasks() const {
  return this->ready_tasks_;
}

const std::list<Task> &SchedulingQueue::GetRunningTasks() const {
  return this->running_tasks_;
}

const std::list<Task> &SchedulingQueue::GetBlockedTasks() const {
  return this->blocked_tasks_;
}

void SchedulingQueue::FilterState(std::unordered_set<TaskID> &task_ids,
                                  TaskState filter_state) const {
  switch (filter_state) {
  case TaskState::PLACEABLE:
    FilterStateFromQueue(placeable_tasks_, task_ids, filter_state);
    break;
  case TaskState::WAITING:
    FilterStateFromQueue(waiting_tasks_, task_ids, filter_state);
    break;
  case TaskState::READY:
    FilterStateFromQueue(ready_tasks_, task_ids, filter_state);
    break;
  case TaskState::RUNNING:
    FilterStateFromQueue(running_tasks_, task_ids, filter_state);
    break;
  case TaskState::BLOCKED:
    FilterStateFromQueue(blocked_tasks_, task_ids, filter_state);
    break;
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

std::vector<Task> SchedulingQueue::RemoveTasks(std::unordered_set<TaskID> &task_ids) {
  // List of removed tasks to be returned.
  std::vector<Task> removed_tasks;

  // Try to find the tasks to remove from the queues.
  RemoveTasksFromQueue(methods_waiting_for_actor_creation_, task_ids, removed_tasks);
  RemoveTasksFromQueue(waiting_tasks_, task_ids, removed_tasks);
  RemoveTasksFromQueue(placeable_tasks_, task_ids, removed_tasks);
  RemoveTasksFromQueue(ready_tasks_, task_ids, removed_tasks);
  RemoveTasksFromQueue(running_tasks_, task_ids, removed_tasks);
  RemoveTasksFromQueue(blocked_tasks_, task_ids, removed_tasks);

  RAY_CHECK(task_ids.size() == 0);
  return removed_tasks;
}

Task SchedulingQueue::RemoveTask(const TaskID &task_id) {
  std::unordered_set<TaskID> task_id_set = {task_id};
  auto task = RemoveTasks(task_id_set).front();
  RAY_CHECK(task.GetTaskSpecification().TaskId() == task_id);
  return task;
}

void SchedulingQueue::MoveTasks(std::unordered_set<TaskID> &task_ids, TaskState src_state,
                                TaskState dst_state) {
  // TODO(atumanov): check the states first to ensure the move is transactional.
  std::vector<Task> removed_tasks;
  // Remove the tasks from the specified source queue.
  switch (src_state) {
  case TaskState::PLACEABLE:
    RemoveTasksFromQueue(placeable_tasks_, task_ids, removed_tasks);
    break;
  case TaskState::WAITING:
    RemoveTasksFromQueue(waiting_tasks_, task_ids, removed_tasks);
    break;
  case TaskState::READY:
    RemoveTasksFromQueue(ready_tasks_, task_ids, removed_tasks);
    break;
  case TaskState::RUNNING:
    RemoveTasksFromQueue(running_tasks_, task_ids, removed_tasks);
    break;
  case TaskState::BLOCKED:
    RemoveTasksFromQueue(blocked_tasks_, task_ids, removed_tasks);
    break;
  default:
    RAY_LOG(FATAL) << "Attempting to move tasks from unrecognized state "
                   << static_cast<std::underlying_type<TaskState>::type>(src_state);
  }
  // Add the tasks to the specified destination queue.
  switch (dst_state) {
  case TaskState::PLACEABLE:
    QueueTasks(placeable_tasks_, removed_tasks);
    break;
  case TaskState::WAITING:
    QueueTasks(waiting_tasks_, removed_tasks);
    break;
  case TaskState::READY:
    QueueTasks(ready_tasks_, removed_tasks);
    break;
  case TaskState::RUNNING:
    QueueTasks(running_tasks_, removed_tasks);
    break;
  case TaskState::BLOCKED:
    QueueTasks(blocked_tasks_, removed_tasks);
    break;
  default:
    RAY_LOG(FATAL) << "Attempting to move tasks to unrecognized state "
                   << static_cast<std::underlying_type<TaskState>::type>(dst_state);
  }
}

void SchedulingQueue::QueueMethodsWaitingForActorCreation(
    const std::vector<Task> &tasks) {
  QueueTasks(methods_waiting_for_actor_creation_, tasks);
}

void SchedulingQueue::QueueWaitingTasks(const std::vector<Task> &tasks) {
  QueueTasks(waiting_tasks_, tasks);
}

void SchedulingQueue::QueuePlaceableTasks(const std::vector<Task> &tasks) {
  QueueTasks(placeable_tasks_, tasks);
}

void SchedulingQueue::QueueReadyTasks(const std::vector<Task> &tasks) {
  QueueTasks(ready_tasks_, tasks);
}

void SchedulingQueue::QueueRunningTasks(const std::vector<Task> &tasks) {
  QueueTasks(running_tasks_, tasks);
}

void SchedulingQueue::QueueBlockedTasks(const std::vector<Task> &tasks) {
  QueueTasks(blocked_tasks_, tasks);
}

void SchedulingQueue::AddDriverTaskId(const TaskID &driver_id) {
  auto inserted = driver_task_ids_.insert(driver_id);
  RAY_CHECK(inserted.second);
}

void SchedulingQueue::RemoveDriverTaskId(const TaskID &driver_id) {
  auto erased = driver_task_ids_.erase(driver_id);
  RAY_CHECK(erased == 1);
}

const std::unordered_set<TaskID> &SchedulingQueue::GetDriverTaskIds() const {
  return driver_task_ids_;
}

}  // namespace raylet

}  // namespace ray
