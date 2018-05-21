#include "scheduling_queue.h"

#include "ray/status.h"

namespace ray {

namespace raylet {

const std::list<Task> &SchedulingQueue::GetUncreatedActorMethods() const {
  return this->uncreated_actor_methods_;
}

const std::list<Task> &SchedulingQueue::GetWaitingTasks() const {
  return this->waiting_tasks_;
}

const std::list<Task> &SchedulingQueue::GetReadyTasks() const {
  return this->ready_tasks_;
}

const std::list<Task> &SchedulingQueue::GetScheduledTasks() const {
  return this->scheduled_tasks_;
}

const std::list<Task> &SchedulingQueue::GetRunningTasks() const {
  return this->running_tasks_;
}

const std::list<Task> &SchedulingQueue::GetBlockedTasks() const {
  return this->blocked_tasks_;
}

const std::list<Task> &SchedulingQueue::GetReadyMethods() const {
  throw std::runtime_error("Method not implemented");
}

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void removeTasksFromQueue(std::list<Task> &queue, std::unordered_set<TaskID> &task_ids,
                          std::vector<Task> &removed_tasks) {
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
inline void queueTasks(std::list<Task> &queue, const std::vector<Task> &tasks) {
  queue.insert(queue.end(), tasks.begin(), tasks.end());
}

std::vector<Task> SchedulingQueue::RemoveTasks(std::unordered_set<TaskID> task_ids) {
  // List of removed tasks to be returned.
  std::vector<Task> removed_tasks;

  // Try to find the tasks to remove from the waiting tasks.
  removeTasksFromQueue(uncreated_actor_methods_, task_ids, removed_tasks);
  removeTasksFromQueue(waiting_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(ready_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(scheduled_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(running_tasks_, task_ids, removed_tasks);
  removeTasksFromQueue(blocked_tasks_, task_ids, removed_tasks);
  // TODO(swang): Remove from running methods.

  RAY_CHECK(task_ids.size() == 0);
  return removed_tasks;
}

void SchedulingQueue::QueueUncreatedActorMethods(const std::vector<Task> &tasks) {
  queueTasks(uncreated_actor_methods_, tasks);
}

void SchedulingQueue::QueueWaitingTasks(const std::vector<Task> &tasks) {
  queueTasks(waiting_tasks_, tasks);
}

void SchedulingQueue::QueueReadyTasks(const std::vector<Task> &tasks) {
  queueTasks(ready_tasks_, tasks);
}

void SchedulingQueue::QueueScheduledTasks(const std::vector<Task> &tasks) {
  queueTasks(scheduled_tasks_, tasks);
}

void SchedulingQueue::QueueRunningTasks(const std::vector<Task> &tasks) {
  queueTasks(running_tasks_, tasks);
}

void SchedulingQueue::QueueBlockedTasks(const std::vector<Task> &tasks) {
  queueTasks(blocked_tasks_, tasks);
}

}  // namespace raylet

}  // namespace ray
