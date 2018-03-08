#ifndef RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
#define RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H

#include <vector>

#include "ray/id.h"

namespace ray {

/// \class TaskExecutionSpecification
///
/// The task execution specification encapsulates all mutable information about
/// the task. These fields may change at execution time, converse to the
/// TaskSpecification that is determined at submission time.
class TaskExecutionSpecification {
 public:
  /// Create a task execution specification.
  ///
  /// \param execution_dependencies The task's dependencies, determined at
  /// execution time.
  TaskExecutionSpecification(const std::vector<ObjectID> &&execution_dependencies);

  /// Create a task execution specification.
  ///
  /// \param execution_dependencies The task's dependencies, determined at
  ///        execution time.
  /// \param spillback_count The number of times this task was spilled back by
  ///        local schedulers.
  TaskExecutionSpecification(const std::vector<ObjectID> &&execution_dependencies,
                             int spillback_count);

  /// Get the task's execution dependencies.
  ///
  /// \return A vector of object IDs representing this task's execution
  ///         dependencies.
  const std::vector<ObjectID> &ExecutionDependencies() const;

  /// Set the task's execution dependencies.
  ///
  /// \param dependencies The value to set the execution dependencies to.
  void SetExecutionDependencies(const std::vector<ObjectID> &dependencies);

  /// Get the task's spillback count, which tracks the number of times
  /// this task was spilled back from local to the global scheduler.
  ///
  /// \return The spillback count for this task.
  int SpillbackCount() const;

  /// Increment the spillback count for this task.
  void IncrementSpillbackCount();

  /// Get the task's last timestamp.
  ///
  /// \return The timestamp when this task was last received for scheduling.
  int64_t LastTimeStamp() const;

  /// Set the task's last timestamp to the specified value.
  ///
  /// \param new_timestamp The new timestamp in millisecond to set the task's
  ///        time stamp to. Tracks the last time this task entered a local
  ///        scheduler.
  void SetLastTimeStamp(int64_t new_timestamp);

 private:
  /// A list of object IDs representing the dependencies of this task that may
  /// change at execution time.
  std::vector<ObjectID> execution_dependencies_;
  /// The last time this task was received for scheduling.
  int64_t last_timestamp_;
  /// The number of times this task was spilled back by local schedulers.
  int spillback_count_;
};

}  // namespace ray
#endif  // RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
