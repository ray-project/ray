#ifndef RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
#define RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H

#include <vector>

#include "ray/id.h"
#include "ray/raylet/format/node_manager_generated.h"

namespace ray {

namespace raylet {

/// \class TaskExecutionSpecification
///
/// The task execution specification encapsulates all mutable information about
/// the task. These fields may change at execution time, converse to the
/// TaskSpecification that is determined at submission time.
class TaskExecutionSpecification {
 public:
  /// Create a task execution specification.
  ///
  /// \param dependencies The task's dependencies, determined at execution
  /// time.
  TaskExecutionSpecification(const std::vector<ObjectID> &&dependencies);

  /// Create a task execution specification.
  ///
  /// \param dependencies The task's dependencies, determined at execution
  /// time.
  /// \param num_forwards The number of times this task has been forwarded by a
  /// node manager.
  TaskExecutionSpecification(const std::vector<ObjectID> &&dependencies,
                             int num_forwards);

  /// Create a task execution specification from a serialized flatbuffer.
  ///
  /// \param spec_flatbuffer The serialized specification.
  TaskExecutionSpecification(
      const protocol::TaskExecutionSpecification &spec_flatbuffer) {
    spec_flatbuffer.UnPackTo(&execution_spec_);
  }

  /// Serialize a task execution specification to a flatbuffer.
  ///
  /// \param fbb The flatbuffer builder.
  /// \return An offset to the serialized task execution specification.
  flatbuffers::Offset<protocol::TaskExecutionSpecification> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

  /// Get the task's execution dependencies.
  ///
  /// \return A vector of object IDs representing this task's execution
  /// dependencies.
  std::vector<ObjectID> ExecutionDependencies() const;

  /// Set the task's execution dependencies.
  ///
  /// \param dependencies The value to set the execution dependencies to.
  void SetExecutionDependencies(const std::vector<ObjectID> &dependencies);

  /// Get the number of times this task has been forwarded.
  ///
  /// \return The number of times this task has been forwarded.
  int NumForwards() const;

  /// Increment the number of times this task has been forwarded.
  void IncrementNumForwards();

  /// Get the task's last timestamp.
  ///
  /// \return The timestamp when this task was last received for scheduling.
  int64_t LastTimestamp() const;

  /// Set the task's last timestamp to the specified value.
  ///
  /// \param new_timestamp The new timestamp in millisecond to set the task's
  /// time stamp to. Tracks the last time this task entered a local scheduler.
  void SetLastTimestamp(int64_t new_timestamp);

 private:
  protocol::TaskExecutionSpecificationT execution_spec_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
