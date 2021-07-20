#include "ray/raylet/scheduling/fair_scheduling_queue.h"

namespace ray {
namespace raylet {

using namespace internal;

FairSchedulingQueue::FairSchedulingQueue()
    : comparator_(active_tasks_), task_queue_(comparator_) {}

void FairSchedulingQueue::Push(const Task &task) {
  task_queue_[task.GetTaskSpecification().GetSchedulingClass()].push_back(task);
}

void FairSchedulingQueue::MarkRunning(const Task &task) {
  // NOTE: It's unsafe to change `active_tasks_[scheduling_class]` while
  // `scheduling_class` is in `task_queue_` since `tasks_queue_`'s comparator
  // relies on `active_tasks_` and thus may become inconsistent.
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  auto queue = std::move(task_queue_[scheduling_class]);
  task_queue_.erase(scheduling_class);
  active_tasks_[scheduling_class]++;
  task_queue_.emplace(scheduling_class, queue);
}

void FairSchedulingQueue::MarkFinished(const Task &task) {
  // NOTE: It's unsafe to change `active_tasks_[scheduling_class]` while
  // `scheduling_class` is in `task_queue_` since `tasks_queue_`'s comparator
  // relies on `active_tasks_` and thus may become inconsistent.
  const auto scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  auto queue = std::move(task_queue_[scheduling_class]);
  task_queue_.erase(scheduling_class);
  active_tasks_[scheduling_class]++;
  task_queue_.emplace(scheduling_class, queue);
}

FairSchedulingQueue::iterator FairSchedulingQueue::begin() const {
  return FairSchedulingQueue::iterator(task_queue_.begin(), task_queue_.end());
}

FairSchedulingQueue::iterator FairSchedulingQueue::end() const {
  return FairSchedulingQueue::iterator(task_queue_.end(), task_queue_.end());
}

FairSchedulingQueue::iterator FairSchedulingQueue::erase(
    FairSchedulingQueue::iterator &iter) {
  auto retval = iter;
  auto &deque = retval.queues_iterator_->second;
  retval.cur_deque_iter_ = deque.erase(iter.cur_deque_iter_);
  if (retval.cur_deque_iter_ == deque.end()) {
    retval.queues_iterator_ = task_queue_.erase(retval.queues_iterator_);
  }
  return retval;
}

FairSchedulingQueue::iterator::iterator(
    internal::TaskQueueType::iterator &queues_iterator,
    internal::TaskQueueType::iterator &end)
    : queues_iterator_(queues_iterator), end_(end) {
  UpdateCurDequeIter();
}

FairSchedulingQueue::iterator FairSchedulingQueue::iterator::operator++() {
  ++cur_deque_iter_;
  if (cur_deque_iter_ == queues_iterator_->second.end()) {
    ++queues_iterator_;
    UpdateCurDequeIter();
  }
  return *this;
}

FairSchedulingQueue::iterator FairSchedulingQueue::iterator::operator++(int) {
  FairSchedulingQueue::iterator retval = *this;
  ++(*this);
  return retval;
}

bool FairSchedulingQueue::iterator::operator==(
    FairSchedulingQueue::iterator &other) const {
  if (queues_iterator_ == other.queues_iterator_) {
    return queues_iterator_ == end_ || cur_deque_iter_ == other.cur_deque_iter_;
  }
  return false;
}

bool FairSchedulingQueue::iterator::operator!=(
    FairSchedulingQueue::iterator &other) const {
  return !((*this) == other);
}

Task &FairSchedulingQueue::iterator::operator*() const { return *cur_deque_iter_; }

inline void FairSchedulingQueue::iterator::UpdateCurDequeIter() {
  if (queues_iterator_ != end_) {
    cur_deque_iter_ = queues_iterator_->second.begin();
  }
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
