#pragma once

/* #include <tuple> */
#include "ray/common/task/task.h"
#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace raylet {

/// Work represents all the information needed to make a scheduling decision.
/// This includes the task, the information we need to communicate to
/// dispatch/spillback and the callback to trigger it.
using Work = std::tuple<Task, rpc::RequestWorkerLeaseReply *, std::function<void(void)>>;

namespace internal {

struct SchedulingPriorityComparator : public std::less<SchedulingClass> {
 public:
  explicit SchedulingPriorityComparator(
      std::unordered_map<SchedulingClass, uint64_t> &priorities);

  bool operator()(const SchedulingClass &lhs, const SchedulingClass &rhs) const;

 private:
  /// A reference to FairSchedulingQueue::active_works_
  std::unordered_map<SchedulingClass, uint64_t> &priorities_;
};

using WorkQueueType =
    std::map<SchedulingClass, std::deque<Work>, internal::SchedulingPriorityComparator>;
}  // namespace internal

/* using WorkQueueIterator = std::iterator<std::forward_iterator_tag, const
 * std::pair<const SchedulingClass, std::deque<Work>>>; */
using WorkQueueIterator = internal::WorkQueueType::iterator;
using ConstWorkQueueIterator = internal::WorkQueueType::const_iterator;

class FairSchedulingQueue {
 public:
  explicit FairSchedulingQueue();

  /// Push a new work onto the queue.
  ///
  /// \param work Work to be pushed.
  void Push(const Work &work);

  /// Set the queue for a scheduling class.
  ///
  /// \param scheduling_class The scheduling class of the queue.
  /// \param work All the work for the scheduling class.
  void PushAll(const SchedulingClass scheduling_class, const std::deque<Work> work_queue);

  /// Mark a task as running, which may or may not influence the next element
  /// to be pushed/popped. This may invalidate existing iterators.
  ///
  /// \param The task that has begun to run.
  void MarkRunning(const Task &task);

  /// Mark a task as finished, which may or may not influence the next element
  /// to be pushed/popped. This may invalidate existing iterators.
  ///
  /// \param The task that has finished.
  void MarkFinished(const Task &task);

  /// Debug string.
  std::string DebugString() const;

  /// NOTE: We only expose these methods instead of the entire map so that
  /// FairSchedulingQueue controls its modification since the comparator is
  /// special.
  /// Standard STL behavior.
  WorkQueueIterator begin();
  ConstWorkQueueIterator begin() const;
  WorkQueueIterator end();
  ConstWorkQueueIterator end() const;
  WorkQueueIterator find(const SchedulingClass scheduling_class);
  ConstWorkQueueIterator find(const SchedulingClass scheduling_class) const;
  WorkQueueIterator erase(WorkQueueIterator &it);
  size_t size() const;

 private:
  /// The number of active tasks for each scheduling class, which is opposite
  /// of the priority of the task (low active count means we should schedule
  /// more of these).
  std::unordered_map<SchedulingClass, uint64_t> active_tasks_;

  /// A comparator that orders work_queue by priority.
  internal::SchedulingPriorityComparator comparator_;
  /// The set of work queues ordered by priority.
  internal::WorkQueueType work_queue_;

  friend class FairSchedulingQueueTest;
};

}  // namespace raylet
}  // namespace ray
