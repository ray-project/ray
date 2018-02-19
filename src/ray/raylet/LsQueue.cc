#ifndef LS_QUEUE_CC
#define LS_QUEUE_CC

#include "LsQueue.h"
#include "ray/id.h"
#include <list>

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

std::vector<Task> LsQueue::RemoveTasks(std::vector<std::list<Task>::iterator> tasks) {
  throw std::runtime_error("Method not implemented");
}
void LsQueue::QueueWaitingTasks(const std::vector<Task> &tasks) {
  throw std::runtime_error("Method not implemented");
}
void LsQueue::QueueReadyTasks(const std::vector<Task> &tasks) {
  throw std::runtime_error("Method not implemented");
}
void LsQueue::QueueRunningTasks(const std::vector<Task> &tasks) {
  throw std::runtime_error("Method not implemented");
}
void LsQueue::RegisterActor(ActorID actor_id, ActorInformation &actor_information) {
  throw std::runtime_error("Method not implemented");
}

} // end namespace ray

#endif
