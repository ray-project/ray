#ifndef LS_QUEUE_CC
#define LS_QUEUE_CC

#include "LsQueue.h"

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



} // end namespace ray

#endif
