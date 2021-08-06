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

using WorkQueueIterator = internal::WorkQueueType::iterator;
using ConstWorkQueueIterator = internal::WorkQueueType::const_iterator;

/// A fair scheduling queue. Fairness is defined in terms of the number of
/// running tasks of a given scheduling class. For example, assume we push 3
/// tasks with scheduling classes: {1, 2, 2}. Then we mark a task of class 1 as
/// running, then to promote fairness, we should schedule task from class 2.
/// Therfore *queue.begin() will return an std::pair<SchedulingClass,
/// std::deque<Work>> of scheduling class 2 first.
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
  void PushAll(const SchedulingClass scheduling_class, std::deque<Work> work_queue);

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
  /*
  ****************** How the internals work **************************
  Essentially, the behavior we want, is a priority queue, but with 2 caveats:
    1. The priority of a scheduling class can change when a task is
       started/finished in that class.
    2. We want to be able iterate over all the priorities (not just peek/pop the
       top element).

  std::priority_queue doesn't meet requirement (2), so we use an _ordered_ map
  instead. Since a comparator always needs to be consistent, and (1) means a
  scheduling class's priority can change, whenever a task is started/finished we
  need to make sure the map's internal datastructures are consistent so we:
    1. Remove the scheduling class (the internals are naturally consistent here).
    2. Change the priority (since we just removed the scheduling class, we know
       it's not in the map, therefore the map is still consistent).
    3. Reinsert scheduling class with a new priority.
  */

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
