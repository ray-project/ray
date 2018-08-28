#include "scheduling_queue.h"

#include "ray/status.h"

namespace {

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void RemoveTasksFromQueue(ray::raylet::SchedulingQueue::TaskQueue &queue,
                          std::unordered_set<ray::TaskID> &task_ids,
                          std::vector<ray::raylet::Task> &removed_tasks) {
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    if (queue.RemoveTask(*it, removed_tasks)) {
      it = task_ids.erase(it);
    } else {
      it++;
    }
  }
}

// Helper function to queue the given tasks to the given queue.
inline void QueueTasks(ray::raylet::SchedulingQueue::TaskQueue &queue,
                       const std::vector<ray::raylet::Task> &tasks) {
  for (const auto &task : tasks) {
    queue.AppendTask(task.GetTaskSpecification().TaskId(), task);
  }
}

// Helper function to filter out tasks of a given state.
inline void FilterStateFromQueue(const ray::raylet::SchedulingQueue::TaskQueue &queue,
                                 std::unordered_set<ray::TaskID> &task_ids,
                                 ray::raylet::TaskState filter_state) {
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    if (queue.HasTask(*it)) {
      it = task_ids.erase(it);
    } else {
      it++;
    }
  }
}

}  // namespace

namespace ray {

namespace raylet {

SchedulingQueue::TaskQueue::~TaskQueue() {
  task_map_.clear();
  task_list_.clear();
}

bool SchedulingQueue::TaskQueue::AppendTask(const TaskID &task_id, const Task &task) {
  RAY_CHECK(task_map_.find(task_id) == task_map_.end());
  auto list_iterator = task_list_.insert(task_list_.end(), task);
  task_map_[task_id] = list_iterator;
  return true;
}

bool SchedulingQueue::TaskQueue::RemoveTask(const TaskID &task_id) {
  auto task_found_iterator = task_map_.find(task_id);
  if (task_found_iterator == task_map_.end()) {
    return false;
  }

  auto list_iterator = task_found_iterator->second;
  task_map_.erase(task_found_iterator);
  task_list_.erase(list_iterator);
  return true;
}

bool SchedulingQueue::TaskQueue::RemoveTask(const TaskID &task_id,
                                            std::vector<Task> &removed_tasks) {
  auto task_found_iterator = task_map_.find(task_id);
  if (task_found_iterator == task_map_.end()) {
    return false;
  }

  auto list_iterator = task_found_iterator->second;
  removed_tasks.push_back(std::move(*list_iterator));
  task_map_.erase(task_found_iterator);
  task_list_.erase(list_iterator);
  return true;
}

bool SchedulingQueue::TaskQueue::HasTask(const TaskID &task_id) const {
  return task_map_.find(task_id) != task_map_.end();
}

const std::list<Task> &SchedulingQueue::TaskQueue::GetTasks() const { return task_list_; }

const std::list<Task> &SchedulingQueue::GetMethodsWaitingForActorCreation() const {
  return this->methods_waiting_for_actor_creation_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetWaitingTasks() const {
  return this->waiting_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetPlaceableTasks() const {
  return this->placeable_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetReadyTasks() const {
  return this->ready_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetInfeasibleTasks() const {
  return this->infeasible_tasks_.GetTasks();
}

ResourceSet SchedulingQueue::GetQueueResources(const TaskQueue &task_queue) const {
  // Iterate over all tasks of the specified queue and aggregate total resource
  // demand in a resource set.
  ResourceSet queue_resources;
  for (const auto &task : task_queue.GetTasks()) {
    queue_resources.AddResources(task.GetTaskSpecification().GetRequiredResources());
  }
  return queue_resources;
}

ResourceSet SchedulingQueue::GetReadyQueueResources() const {
  return GetQueueResources(ready_tasks_);
}

ResourceSet SchedulingQueue::GetResourceLoad() const {
  ResourceSet load_resource_set;
  load_resource_set.AddResources(GetReadyQueueResources());
  // TODO(atumanov): consider other types of tasks as part of load.
  return load_resource_set;
}

const std::list<Task> &SchedulingQueue::GetRunningTasks() const {
  return this->running_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetBlockedTasks() const {
  return this->blocked_tasks_.GetTasks();
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
  case TaskState::INFEASIBLE:
    FilterStateFromQueue(infeasible_tasks_, task_ids, filter_state);
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
  RemoveTasksFromQueue(infeasible_tasks_, task_ids, removed_tasks);

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
  case TaskState::INFEASIBLE:
    RemoveTasksFromQueue(infeasible_tasks_, task_ids, removed_tasks);
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
  case TaskState::INFEASIBLE:
    QueueTasks(infeasible_tasks_, removed_tasks);
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

bool SchedulingQueue::HasTask(const TaskID &task_id) const {
  return (methods_waiting_for_actor_creation_.HasTask(task_id) ||
          waiting_tasks_.HasTask(task_id) || placeable_tasks_.HasTask(task_id) ||
          ready_tasks_.HasTask(task_id) || running_tasks_.HasTask(task_id) ||
          blocked_tasks_.HasTask(task_id) || infeasible_tasks_.HasTask(task_id));
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

const std::string SchedulingQueue::ToString() const {
  std::string result;

  result += "placeable_tasks_ size is " +
            std::to_string(placeable_tasks_.GetTasks().size()) + "\n";
  result +=
      "waiting_tasks_ size is " + std::to_string(waiting_tasks_.GetTasks().size()) + "\n";
  result +=
      "ready_tasks_ size is " + std::to_string(ready_tasks_.GetTasks().size()) + "\n";
  result +=
      "running_tasks_ size is " + std::to_string(running_tasks_.GetTasks().size()) + "\n";
  result +=
      "blocked_tasks_ size is " + std::to_string(blocked_tasks_.GetTasks().size()) + "\n";
  result += "infeasible_tasks_ size is " +
            std::to_string(infeasible_tasks_.GetTasks().size()) + "\n";
  result += "methods_waiting_for_actor_creation_ size is " +
            std::to_string(methods_waiting_for_actor_creation_.GetTasks().size()) + "\n";
  return result;
}

}  // namespace raylet

}  // namespace ray
