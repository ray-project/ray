#include "ray/raylet/scheduling/fair_scheduling_queue.h"

namespace ray {
namespace raylet {

using namespace internal;

FairSchedulingQueue::FairSchedulingQueue()
    : comparator_(active_tasks_), work_queue_(comparator_) {}

void FairSchedulingQueue::Push(const Work &work) {
  const auto scheduling_class =
      std::get<0>(work).GetTaskSpecification().GetSchedulingClass();
  work_queue_[scheduling_class].push_back(work);
}

void FairSchedulingQueue::PushAll(const SchedulingClass scheduling_class,
                                  std::deque<Work> works) {
  // TODO (Alex): We may want to do an O(1) move here if this gets too
  // inefficient.
  for (const auto &work : works) {
    Push(work);
  }
}

void FairSchedulingQueue::MarkRunning(const Task &task) {
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  AdjustPriority(scheduling_class, 1);
}

void FairSchedulingQueue::MarkFinished(const Task &task) {
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  AdjustPriority(scheduling_class, -1);
}

void FairSchedulingQueue::AdjustPriority(const SchedulingClass scheduling_class, int64_t diff) {
  auto it = work_queue_.find(scheduling_class);
  if (it != work_queue_.end()) {
    auto queue = std::move(it->second);
    work_queue_.erase(scheduling_class);
    active_tasks_[scheduling_class] += diff;
    work_queue_.emplace(scheduling_class, queue);
  } else {
    active_tasks_[scheduling_class] += diff;
    if (active_tasks_[scheduling_class] == 0) {
      active_tasks_.erase(scheduling_class);
    }
  }
}

std::string FairSchedulingQueue::DebugString() const {
  std::stringstream buffer;
  buffer << "============== Running==============\n";
  for (const auto &pair : active_tasks_) {
    buffer << "\t" << pair.first << ":\t" << pair.second << "\n";
  }
  buffer << "--------------Queue order--------------\n";
  for (const auto &pair : work_queue_) {
    buffer << "\t" << pair.first << ":\t" << pair.second.size() << "\n";
  }
  buffer << "====================================";
  return buffer.str();
}

WorkQueueIterator FairSchedulingQueue::begin() { return work_queue_.begin(); }

ConstWorkQueueIterator FairSchedulingQueue::begin() const { return work_queue_.begin(); }

WorkQueueIterator FairSchedulingQueue::end() { return work_queue_.end(); }

ConstWorkQueueIterator FairSchedulingQueue::end() const { return work_queue_.end(); }

WorkQueueIterator FairSchedulingQueue::find(const SchedulingClass scheduling_class) {
  return work_queue_.find(scheduling_class);
}

ConstWorkQueueIterator FairSchedulingQueue::find(
    const SchedulingClass scheduling_class) const {
  return work_queue_.find(scheduling_class);
}

WorkQueueIterator FairSchedulingQueue::erase(WorkQueueIterator &it) {
  return work_queue_.erase(it);
}

size_t FairSchedulingQueue::size() const { return work_queue_.size(); }

namespace internal {

SchedulingPriorityComparator::SchedulingPriorityComparator(
    std::unordered_map<SchedulingClass, uint64_t> &priorities)
    : priorities_(priorities) {}

bool SchedulingPriorityComparator::operator()(const SchedulingClass &lhs,
                                              const SchedulingClass &rhs) const {
  int64_t lhs_priority = 0;
  int64_t rhs_priority = 0;

  auto it = priorities_.find(lhs);
  if (it != priorities_.end()) {
    lhs_priority = it->second;
  }

  it = priorities_.find(rhs);
  if (it != priorities_.end()) {
    rhs_priority = it->second;
  }

  return lhs_priority < rhs_priority;
}
}  // namespace internal

}  // namespace raylet
}  // namespace ray
