#ifndef TASK_EXECUTION_SPECIFICATION_H
#define TASK_EXECUTION_SPECIFICATION_H

#include <vector>

#include "ray/id.h"

namespace ray {
/// TaskExecutionSpecification encapsulates all information about the task that's
/// ephemeral/dynamic. It is complementary to the TaskSpecification.
class TaskExecutionSpecification {
 public:
  TaskExecutionSpecification();
  TaskExecutionSpecification(
      const std::vector<ObjectID> &&execution_dependencies);
  TaskExecutionSpecification(
      const std::vector<ObjectID> &&execution_dependencies,
      int spillback_count);
  TaskExecutionSpecification(const TaskExecutionSpecification &execution_spec);

  /// Get the task's execution dependencies.
  ///
  /// @return A vector of object IDs representing this task's execution
  ///         dependencies.
  const std::vector<ObjectID> &ExecutionDependencies() const;

  /// Set the task's execution dependencies.
  ///
  /// @param dependencies The value to set the execution dependencies to.
  /// @return Void.
  void SetExecutionDependencies(const std::vector<ObjectID> &dependencies);

  /// Get the task's spillback count, which tracks the number of times
  /// this task was spilled back from local to the global scheduler.
  ///
  /// @return The spillback count for this task.
  int SpillbackCount() const;

  /// Increment the spillback count for this task.
  ///
  /// @return Void.
  void IncrementSpillbackCount();

  /// Get the task's last timestamp.
  ///
  /// @return The timestamp when this task was last received for scheduling.
  int64_t LastTimeStamp() const;

  /// Set the task's last timestamp to the specified value.
  ///
  /// @param new_timestamp The new timestamp in millisecond to set the task's
  ///        time stamp to. Tracks the last time this task entered a local
  ///        scheduler.
  /// @return Void.
  void SetLastTimeStamp(int64_t new_timestamp);

 private:
  /** A list of object IDs representing this task's dependencies at execution
   *  time. */
  std::vector<ObjectID> execution_dependencies_;
  /** Last time this task was received for scheduling. */
  int64_t last_timestamp_;
  /** Number of times this task was spilled back by local schedulers. */
  int spillback_count_;
};

} // end namespace ray
#endif
