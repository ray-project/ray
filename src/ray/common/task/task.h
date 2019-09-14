#ifndef RAY_COMMON_TASK_TASK_H
#define RAY_COMMON_TASK_TASK_H

#include <inttypes.h>

#include "ray/common/task/task_common.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"

namespace ray {

/// \class Task
///
/// A Task represents a Ray task and a specification of its execution (e.g.,
/// resource demands). The task's specification contains both immutable fields,
/// determined at submission time, and mutable fields, determined at execution
/// time.
class Task {
 public:
  /// Construct an empty task. This should only be used to pass a task
  /// as an out parameter to a function or method.
  Task() {}

  /// Construct a `Task` object from a protobuf message.
  ///
  /// \param message The protobuf message.
  explicit Task(const rpc::Task &message)
      : task_spec_(message.task_spec()),
        task_execution_spec_(message.task_execution_spec()) {
    ComputeDependencies();
  }

  /// Construct a vector Task
  Task(TaskExecutionSpecification task_execution_spec,
       std::vector<TaskSpecification> specs)
      : task_spec_(specs[0]),
        task_execution_spec_(std::move(task_execution_spec)),
        task_spec_vector_(std::move(specs)) {
    ComputeDependencies();
  }

  /// Construct a `Task` object from a `TaskSpecification` and a
  /// `TaskExecutionSpecification`.
  Task(TaskSpecification task_spec, TaskExecutionSpecification task_execution_spec)
      : task_spec_(std::move(task_spec)),
        task_execution_spec_(std::move(task_execution_spec)) {
    ComputeDependencies();
  }

  /// Get the mutable specification for the task. This specification may be
  /// updated at runtime.
  ///
  /// \return The mutable specification for the task.
  const TaskExecutionSpecification &GetTaskExecutionSpec() const;

  /// Get the immutable specification for the task.
  ///
  /// \return The immutable specification for the task.
  const TaskSpecification &GetTaskSpecification() const;

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

  std::string DebugString() const;

  bool IsVectorTask() const { return !task_spec_vector_.empty(); }

  const std::vector<TaskSpecification>& GetTaskSpecificationVector() const {
    RAY_CHECK(IsVectorTask());
    return task_spec_vector_;
  }

 private:
  void ComputeDependencies();

  /// Task specification object, consisting of immutable information about this
  /// task determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  TaskSpecification task_spec_;
  /// Task execution specification, consisting of all dynamic/mutable
  /// information about this task determined at execution time.
  TaskExecutionSpecification task_execution_spec_;
  /// If this is a vector task, list of all task specs including the first one.
  /// In this case task_spec_ == task_spec_vector_[0];
  std::vector<TaskSpecification> task_spec_vector_;
  /// A cached copy of the task's object dependencies, including arguments from
  /// the TaskSpecification and execution dependencies from the
  /// TaskExecutionSpecification.
  std::vector<ObjectID> dependencies_;
};

}  // namespace ray

#endif  // RAY_COMMON_TASK_TASK_H
