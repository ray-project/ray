#ifndef LS_QUEUE_CC
#define LS_QUEUE_CC

#include "LsQueue.h"

#include <list>

#include "common.h"
#include "ray/id.h"

namespace ray {

const std::list<Task> &LsQueue::waiting_tasks() const {
  return this->waiting_tasks_;
}

const std::list<Task> &LsQueue::ready_tasks() const {
  return this->ready_tasks_;
}

const std::list<Task> &LsQueue::running_tasks() const {
  return this->running_tasks_;
}

const std::list<Task>& LsQueue::ready_methods() const {
  throw std::runtime_error("Method not implemented");
}

// Helper function to remove tasks in the given set of task_ids from a
// queue, and append them to the given vector removed_tasks.
void removeTasksFromQueue(
    std::unordered_set<TaskID, UniqueIDHasher> task_ids,
    std::vector<Task> &removed_tasks,
    std::list<Task> &queue) {
  for (auto it = queue.begin();
       it != queue.end(); ) {
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
void queueTasks(
    std::list<Task> &queue,
    const std::vector<Task> &tasks) {
  for (auto &task : tasks) {
    queue.push_back(task);
  }
}

std::vector<Task> LsQueue::RemoveTasks(std::unordered_set<TaskID, UniqueIDHasher> task_ids) {
  std::vector<Task> removed_tasks;

  // Try to find the tasks to remove from the waiting tasks.
  removeTasksFromQueue(task_ids, removed_tasks, waiting_tasks_);
  removeTasksFromQueue(task_ids, removed_tasks, ready_tasks_);
  removeTasksFromQueue(task_ids, removed_tasks, running_tasks_);
  // TODO(swang): Remove from running methods.

  CHECK(task_ids.size() == 0);
  return removed_tasks;
}

void LsQueue::QueueWaitingTasks(const std::vector<Task> &tasks) {
  queueTasks(waiting_tasks_, tasks);
}

void LsQueue::QueueReadyTasks(const std::vector<Task> &tasks) {
  queueTasks(ready_tasks_, tasks);
}

void LsQueue::QueueRunningTasks(const std::vector<Task> &tasks) {
  queueTasks(running_tasks_, tasks);
}

// RegisterActor is responsible for recording provided actor_information
// in the actor registry.
bool LsQueue::RegisterActor(ActorID actor_id,
                            const ActorInformation &actor_information) {
  actor_registry_[actor_id] = actor_information;
  return true;
}

} // end namespace ray

#endif
