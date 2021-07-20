#pragma once

#include "ray/raylet/scheduling/scheduling_queue.h"

namespace ray {
namespace raylet {

namespace internal {

  using TaskQueueType = std::map<SchedulingClass, std::deque<Task>, internal::SchedulingPriorityComparator>;

struct SchedulingPriorityComparator : public std::less<SchedulingClass> {
 public:
  explicit SchedulingPriorityComparator(
      std::unordered_map<SchedulingClass, uint64_t> &priorities);

  bool operator()(const SchedulingClass &lhs, const SchedulingClass &rhs) const;

 private:
  /// A reference to FairSchedulingQueue::active_tasks_
  std::unordered_map<SchedulingClass, uint64_t> &priorities_;
};

}  // namespace internal

class FairSchedulingQueue {
 public:
  class iterator : public std::iterator<std::forward_iterator_tag, Task> {
   public:
    explicit iterator(
                      TaskQueueType::iterator &queues_iterator,
        TaskQueueType::iterator &end);

    iterator operator++();

    iterator operator++(int);

    bool operator==(iterator &other) const;

    bool operator!=(iterator &other) const;

    Task &operator*() const;

   private:
    /// The current iteration point in `task_queue_`.
    TaskQueueType::iterator queues_iterator_;

    /// The current iteration point in `task_queue_[scheduling_class]`.
    std::deque<Task>::iterator cur_deque_iter_;

    /// `task_queue_.end()` needed to handle the edge case of
    /// `FairSchedulingQueue::end()`, which shouldn't be dereferenced.
    TaskQueueType::iterator end_;

    void UpdateCurDequeIter();

    friend class FairSchedulingQueue;
  };

  explicit FairSchedulingQueue();

  /// Push a new task onto the queue.
  ///
  /// \param task Task to be pushed.
  void Push(const Task &task);

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

  /// These follow the definition for a standard STL container.
  iterator begin();
  iterator end();

  /// NOTE: Erase is an O(log n) operation.
  iterator erase(iterator &iter);

 private:
  /// The number of active tasks for each scheduling class, which is opposite
  /// of the priority of the task (low active count means we should schedule
  /// more of these).
  std::unordered_map<SchedulingClass, uint64_t> active_tasks_;

  /// A comparator that orders task_queue by priority.
  internal::SchedulingPriorityComparator comparator_;
  /// The set of task queues ordered by priority.
      TaskQueueType task_queue_;
};

}  // namespace raylet
}  // namespace ray
