#ifndef TASK_EXECUTION_SPECIFICATION_H
#define TASK_EXECUTION_SPECIFICATION_H

namespace ray {
/// TaskExecutionSpec encapsulates all information about the task that's
/// ephemeral/dynamic. It is complementary to the TaskSpecification.
class TaskExecutionSpec {
 public:
  TaskExecutionSpec(const std::vector<ObjectID> &execution_dependencies);

  TaskExecutionSpec(const std::vector<ObjectID> &execution_dependencies,
                    int spillback_count);
  TaskExecutionSpec(TaskExecutionSpec *execution_spec);

  /// Get the task's execution dependencies.
  ///
  /// @return A vector of object IDs representing this task's execution
  ///         dependencies.
  std::vector<ObjectID> ExecutionDependencies() const;

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

  /// Get the number of dependencies. This comprises the immutable task
  /// arguments and the mutable execution dependencies.
  ///
  /// @return The number of dependencies.
  /// TODO(atumanov): probably move this to the Task class, which composes
  /// TaskSpecification and TaskExecutionSpec.
  int64_t NumDependencies() const;

  /// Get the number of object IDs at the given dependency index.
  ///
  /// @param dependency_index The dependency index whose object IDs to count.
  /// @return The number of object IDs at the given dependency_index.
  int DependencyIdCount(int64_t dependency_index) const;

  /// Get the object ID of a given dependency index.
  ///
  /// @param dependency_index The index at which we should look up the object
  ///        ID.
  /// @param id_index The index of the object ID.
  ObjectID DependencyId(int64_t dependency_index, int64_t id_index) const;

  /// Compute whether the task is dependent on an object ID.
  ///
  /// @param object_id The object ID that the task may be dependent on.
  /// @return bool This returns true if the task is dependent on the given
  ///         object ID and false otherwise.
  bool DependsOn(ObjectID object_id) const;

  /// Returns whether the given dependency index is a static dependency (an
  /// argument of the immutable task).
  ///
  /// @param dependency_index The requested dependency index.
  /// @return bool This returns true if the requested dependency index is
  ///         immutable (an argument of the task).
  bool IsStaticDependency(int64_t dependency_index) const;

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
