#include "scheduling_queue.h"

#include <sstream>

#include "ray/status.h"

namespace {

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
template <typename TaskQueue>
void RemoveTasksFromQueue(ray::raylet::TaskState task_state, TaskQueue &queue,
                          std::unordered_set<ray::TaskID> &task_ids,
                          std::vector<ray::raylet::Task> *removed_tasks,
                          std::vector<ray::raylet::TaskState> *task_states = nullptr) {
  for (auto it = task_ids.begin(); it != task_ids.end();) {
    if (queue.RemoveTask(*it, removed_tasks)) {
      it = task_ids.erase(it);
      if (task_states != nullptr) {
        task_states->push_back(task_state);
      }
    } else {
      it++;
    }
  }
}

// Helper function to queue the given tasks to the given queue.
template <typename TaskQueue>
inline void QueueTasks(TaskQueue &queue, const std::vector<ray::raylet::Task> &tasks) {
  for (const auto &task : tasks) {
    queue.AppendTask(task.GetTaskSpecification().TaskId(), task);
  }
}

// Helper function to filter out tasks of a given state.
template <typename TaskQueue>
inline void FilterStateFromQueue(const TaskQueue &queue,
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

const ResourceSet &TaskQueue::GetCurrentResourceLoad() const {
  return current_resource_load_;
}

bool ReadyQueue::AppendTask(const TaskID &task_id, const Task &task) {
  const auto &resources = task.GetTaskSpecification().GetRequiredResources();
  RAY_CHECK(tasks_with_resources_[resources].insert(task_id).second == true);
  return TaskQueue::AppendTask(task_id, task);
}

bool ReadyQueue::RemoveTask(const TaskID &task_id, std::vector<Task> *removed_tasks) {
  if (task_map_.find(task_id) != task_map_.end()) {
    const auto &resources = task_map_[task_id]->GetTaskSpecification().GetRequiredResources();
    RAY_CHECK(tasks_with_resources_[resources].erase(task_id) == 1);
  }
  return TaskQueue::RemoveTask(task_id, removed_tasks);
}

const std::list<Task> &SchedulingQueue::GetMethodsWaitingForActorCreation() const {
  return methods_waiting_for_actor_creation_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetWaitingTasks() const {
  return waiting_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetPlaceableTasks() const {
  return placeable_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetReadyTasks() const {
  return ready_tasks_.GetTasks();
}

const std::list<Task> &SchedulingQueue::GetInfeasibleTasks() const {
  return infeasible_tasks_.GetTasks();
}

ResourceSet SchedulingQueue::GetReadyQueueResources() const {
  return ready_tasks_.GetCurrentResourceLoad();
}

ResourceSet SchedulingQueue::GetResourceLoad() const {
  // TODO(atumanov): consider other types of tasks as part of load.
  return ready_tasks_.GetCurrentResourceLoad();
}

const std::list<Task> &SchedulingQueue::GetRunningTasks() const {
  return running_tasks_.GetTasks();
}

const std::unordered_set<TaskID> &SchedulingQueue::GetBlockedTaskIds() const {
  return blocked_task_ids_;
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

std::vector<Task> SchedulingQueue::RemoveTasks(std::unordered_set<TaskID> &task_ids,
                                               std::vector<TaskState> *task_states) {
  // List of removed tasks to be returned.
  std::vector<Task> removed_tasks;

  // Try to find the tasks to remove from the queues.

  RemoveTasksFromQueue(TaskState::WAITING_FOR_ACTOR, methods_waiting_for_actor_creation_,
                       task_ids, &removed_tasks, task_states);
  RemoveTasksFromQueue(TaskState::WAITING, waiting_tasks_, task_ids, &removed_tasks,
                       task_states);
  RemoveTasksFromQueue(TaskState::PLACEABLE, placeable_tasks_, task_ids, &removed_tasks,
                       task_states);
  RemoveTasksFromQueue(TaskState::READY, ready_tasks_, task_ids, &removed_tasks,
                       task_states);
  RemoveTasksFromQueue(TaskState::RUNNING, running_tasks_, task_ids, &removed_tasks,
                       task_states);
  RemoveTasksFromQueue(TaskState::INFEASIBLE, infeasible_tasks_, task_ids, &removed_tasks,
                       task_states);

  RAY_CHECK(task_ids.size() == 0);
  if (task_states != nullptr) {
    RAY_CHECK(removed_tasks.size() == task_states->size());
  }
  return removed_tasks;
}

Task SchedulingQueue::RemoveTask(const TaskID &task_id, TaskState *task_state) {
  std::unordered_set<TaskID> task_id_set = {task_id};
  std::vector<TaskState> task_state_vector;
  auto const task = RemoveTasks(task_id_set, &task_state_vector).front();

  RAY_CHECK(task_state_vector.size() == 1);
  if (task_state != nullptr) {
    *task_state = task_state_vector[0];
  }

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
    RemoveTasksFromQueue(TaskState::PLACEABLE, placeable_tasks_, task_ids, &removed_tasks);
    break;
  case TaskState::WAITING:
    RemoveTasksFromQueue(TaskState::WAITING, waiting_tasks_, task_ids, &removed_tasks);
    break;
  case TaskState::READY:
    RemoveTasksFromQueue(TaskState::READY, ready_tasks_, task_ids, &removed_tasks);
    break;
  case TaskState::RUNNING:
    RemoveTasksFromQueue(TaskState::RUNNING, running_tasks_, task_ids, &removed_tasks);
    break;
  case TaskState::INFEASIBLE:
    RemoveTasksFromQueue(TaskState::INFEASIBLE, infeasible_tasks_, task_ids,
                         &removed_tasks);
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
          infeasible_tasks_.HasTask(task_id));
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

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForDriver(
    const DriverID &driver_id) const {
  std::unordered_set<TaskID> task_ids;

  GetDriverTasksFromQueue(methods_waiting_for_actor_creation_, driver_id, task_ids);
  GetDriverTasksFromQueue(waiting_tasks_, driver_id, task_ids);
  GetDriverTasksFromQueue(placeable_tasks_, driver_id, task_ids);
  GetDriverTasksFromQueue(ready_tasks_, driver_id, task_ids);
  GetDriverTasksFromQueue(running_tasks_, driver_id, task_ids);
  GetDriverTasksFromQueue(infeasible_tasks_, driver_id, task_ids);

  return task_ids;
}

std::unordered_set<TaskID> SchedulingQueue::GetTaskIdsForActor(
    const ActorID &actor_id) const {
  std::unordered_set<TaskID> task_ids;

  GetActorTasksFromQueue(methods_waiting_for_actor_creation_, actor_id, task_ids);
  GetActorTasksFromQueue(waiting_tasks_, actor_id, task_ids);
  GetActorTasksFromQueue(placeable_tasks_, actor_id, task_ids);
  GetActorTasksFromQueue(ready_tasks_, actor_id, task_ids);
  GetActorTasksFromQueue(running_tasks_, actor_id, task_ids);
  GetActorTasksFromQueue(infeasible_tasks_, actor_id, task_ids);

  return task_ids;
}

void SchedulingQueue::AddBlockedTaskId(const TaskID &task_id) {
  auto inserted = blocked_task_ids_.insert(task_id);
  RAY_CHECK(inserted.second);
}

void SchedulingQueue::RemoveBlockedTaskId(const TaskID &task_id) {
  auto erased = blocked_task_ids_.erase(task_id);
  RAY_CHECK(erased == 1);
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

std::string SchedulingQueue::DebugString() const {
  std::stringstream result;
  result << "SchedulingQueue:";
  result << "\n- num placeable tasks: " << placeable_tasks_.GetTasks().size();
  result << "\n- num waiting tasks: " << waiting_tasks_.GetTasks().size();
  result << "\n- num ready tasks: " << ready_tasks_.GetTasks().size();
  result << "\n- num running tasks: " << running_tasks_.GetTasks().size();
  result << "\n- num infeasible tasks: " << infeasible_tasks_.GetTasks().size();
  result << "\n- num methods waiting for actor creation: "
         << methods_waiting_for_actor_creation_.GetTasks().size();
  return result.str();
}

}  // namespace raylet

}  // namespace ray
