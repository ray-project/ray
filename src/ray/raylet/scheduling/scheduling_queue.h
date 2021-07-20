#pragma once

#include "ray/common/task/task.h"

namespace ray {
namespace raylet {

using SchedulingQueueIterator = std::iterator<std::forward_iterator_tag, Task>;

/// A generic interface for a scheduling queue.
class SchedulingQueueInteface {
 public:
  /// Push a new task onto the queue.
  ///
  /// \param task Task to be pushed.
  virtual void Push(const Task &task) = 0;

  /// Mark a task as running, which may or may not influence the next element
  /// to be pushed/popped. This may invalidate existing iterators.
  ///
  /// \param The task that has begun to run.
  virtual void MarkRunning(const Task &task) = 0;

  /// Mark a task as finished, which may or may not influence the next element
  /// to be pushed/popped. This may invalidate existing iterators.
  ///
  /// \param The task that has finished.
  virtual void MarkFinished(const Task &task) = 0;

  /// These follow the definition for a standard STL container.
  virtual SchedulingQueueIterator begin() const = 0;
  virtual SchedulingQueueIterator end() const = 0;
  virtual SchedulingQueueIterator erase(SchedulingQueueIterator &iterator) = 0;
};

class scheduling_queue_iterator : public std::iterator<std::forward_iterator_tag, Task> {
 public:
  virtual iterator &operator++();
  virtual iterator &operator++(int);
  virtual bool operator==(iterator other) const;
  virtual bool operator!=(iterator other) const;
  virtual reference operator*() const;
};

}  // namespace raylet
}  // namespace ray
