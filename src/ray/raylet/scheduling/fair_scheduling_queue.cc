#include "ray/raylet/scheduling/fair_scheduling_queue.h"

namespace ray {
namespace raylet {

using namespace internal;

FairSchedulingQueue::FairSchedulingQueue()
    : comparator_(active_tasks_), work_queue_(comparator_) {}

void FairSchedulingQueue::Push(const Work &work) {
  const auto scheduling_class = std::get<0>(work).GetTaskSpecification().GetSchedulingClass();
  work_queue_[scheduling_class].push_back(work);
}

void FairSchedulingQueue::Set(const SchedulingClass scheduling_class, std::deque<Work> works) {
  // TODO (Alex): We may want to do an O(1) move here if this gets too
  // inefficient.

  work_queue_.emplace(scheduling_class, works);
}

void FairSchedulingQueue::MarkRunning(const Task &task) {
  // NOTE: It's unsafe to change `active_works_[scheduling_class]` while
  // `scheduling_class` is in `work_queue_` since `works_queue_`'s comparator
  // relies on `active_works_` and thus may become inconsistent.
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  auto queue = std::move(work_queue_[scheduling_class]);
  work_queue_.erase(scheduling_class);
  active_tasks_[scheduling_class]++;
  work_queue_.emplace(scheduling_class, queue);
}

void FairSchedulingQueue::MarkFinished(const Task &task) {
  // NOTE: It's unsafe to change `active_works_[scheduling_class]` while
  // `scheduling_class` is in `work_queue_` since `works_queue_`'s comparator
  // relies on `active_works_` and thus may become inconsistent.
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  auto queue = std::move(work_queue_[scheduling_class]);
  work_queue_.erase(scheduling_class);
  active_tasks_[scheduling_class]--;
  work_queue_.emplace(scheduling_class, queue);
}

WorkQueueIterator FairSchedulingQueue::begin() {
  return work_queue_.begin();
}

ConstWorkQueueIterator FairSchedulingQueue::begin() const {
  return work_queue_.begin();
}

WorkQueueIterator FairSchedulingQueue::end() {
  return work_queue_.end();
}

ConstWorkQueueIterator FairSchedulingQueue::end() const {
  return work_queue_.end();
}

WorkQueueIterator FairSchedulingQueue::find(const SchedulingClass scheduling_class) {
  return work_queue_.find(scheduling_class);
}

ConstWorkQueueIterator FairSchedulingQueue::find(const SchedulingClass scheduling_class) const {
  return work_queue_.find(scheduling_class);
}

WorkQueueIterator FairSchedulingQueue::erase(WorkQueueIterator &it) {
  return work_queue_.erase(it);
}

namespace internal {

SchedulingPriorityComparator::SchedulingPriorityComparator(
    std::unordered_map<SchedulingClass, uint64_t> &priorities)
    : priorities_(priorities) {}

bool SchedulingPriorityComparator::operator()(const SchedulingClass &lhs,
                                              const SchedulingClass &rhs) const {
  return priorities_[lhs] < priorities_[rhs];
}
}  // namespace internal

}  // namespace raylet
}  // namespace ray
