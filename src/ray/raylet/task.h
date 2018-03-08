#ifndef RAY_RAYLET_TASK_H
#define RAY_RAYLET_TASK_H

#include <inttypes.h>

#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

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
      : task_execution_spec_(execution_spec), task_spec_(task_spec) {}

  /// Destroy the task.
  virtual ~Task() {}

  /// Get the execution specification for the task.
  ///
  /// \return The mutable specification for the task.
  const TaskExecutionSpecification &GetTaskExecutionSpec() const;

  /// Get the immutable specification for the task.
  ///
  /// \return The immutable specification for the task.
  const TaskSpecification &GetTaskSpecification() const;

  /// Get the task's object dependencies. This comprises the immutable task
  /// arguments and the mutable execution dependencies.
  ///
  /// \return The object dependencies.
  /// TODO(atumanov): consider returning a constant reference.
  const std::vector<ObjectID> GetDependencies() const;

  /// Compute whether the task is dependent on an object ID.
  ///
  /// \param object_id The object ID that the task may be dependent on.
  /// \return Returns true if the task is dependent on the given object ID and
  /// false otherwise.
  bool DependsOn(const ObjectID &object_id) const;

 private:
  /// Task execution specification, consisting of all dynamic/mutable
  /// information about this task determined at execution time..
  TaskExecutionSpecification task_execution_spec_;
  /// Task specification object, consisting of immutable information about this
  /// task determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  TaskSpecification task_spec_;
};

}  // namespace ray

#endif  // RAY_RAYLET_TASK_H