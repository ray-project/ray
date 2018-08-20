#ifndef RAY_RAYLET_TASK_H
#define RAY_RAYLET_TASK_H

#include <inttypes.h>

#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

/// \class Task
///
/// A Task represents a Ray task and a specification of its execution (e.g.,
/// resource demands). The task's specification contains both immutable fields,
/// determined at submission time, and mutable fields, determined at execution
/// time.
class Task {
 public:
  /// Create a task.
  ///
  /// \param execution_spec The execution specification for the task. These are
  /// the mutable fields in the task specification that may change at task
  /// execution time.
  /// \param task_spec The immutable specification for the task. These fields
  /// are determined at task submission time.
  Task(const TaskExecutionSpecification &execution_spec,
       const TaskSpecification &task_spec)
      : task_execution_spec_(execution_spec), task_spec_(task_spec) {
    ComputeDependencies();
  }

  /// Create a task from a serialized flatbuffer.
  ///
  /// \param task_flatbuffer The serialized task.
  Task(const protocol::Task &task_flatbuffer)
      : Task(*task_flatbuffer.task_execution_spec(),
             *task_flatbuffer.task_specification()) {}

  /// Create a task from a flatbuffer object.
  ///
  /// \param task_data The task flatbuffer object.
  Task(const protocol::TaskT &task_data)
      : Task(*task_data.task_execution_spec, task_data.task_specification) {}

  /// Destroy the task.
  virtual ~Task() {}

  /// Serialize a task to a flatbuffer.
  ///
  /// \param fbb The flatbuffer builder.
  /// \return An offset to the serialized task.
  flatbuffers::Offset<protocol::Task> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

  /// Get the mutable specification for the task. This specification may be
  /// updated at runtime.
  ///
  /// \return The mutable specification for the task.
  const TaskExecutionSpecification &GetTaskExecutionSpec() const;

  /// Get the immutable specification for the task.
  ///
  /// \return The immutable specification for the task.
  const TaskSpecification &GetTaskSpecification() const;

  /// Set the task's execution dependencies.
  ///
  /// \param dependencies The value to set the execution dependencies to.
  void SetExecutionDependencies(const std::vector<ObjectID> &dependencies);

  /// Increment the number of times this task has been forwarded.
  void IncrementNumForwards();

  /// Get the task's object dependencies. This comprises the immutable task
  /// arguments and the mutable execution dependencies.
  ///
  /// \return The object dependencies.
  const std::vector<ObjectID> &GetDependencies() const;

  /// Update the dynamic/mutable information for this task.
  /// \param task Task structure with updated dynamic information.
  void CopyTaskExecutionSpec(const Task &task);

 private:
  void ComputeDependencies();

  /// Task execution specification, consisting of all dynamic/mutable
  /// information about this task determined at execution time..
  TaskExecutionSpecification task_execution_spec_;
  /// Task specification object, consisting of immutable information about this
  /// task determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  TaskSpecification task_spec_;
  /// A cached copy of the task's object dependencies, including arguments from
  /// the TaskSpecification and execution dependencies from the
  /// TaskExecutionSpecification.
  std::vector<ObjectID> dependencies_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_H
