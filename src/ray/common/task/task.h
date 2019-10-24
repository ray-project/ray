#ifndef RAY_COMMON_TASK_TASK_H
#define RAY_COMMON_TASK_TASK_H

#include <inttypes.h>
#include <memory>

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

  /// Construct a vector Task. All task specs should have the same object
  /// dependencies and resource requirements as the first.
  Task(TaskExecutionSpecification task_execution_spec,
       std::vector<TaskSpecification> specs)
      : task_spec_(specs[0]),
        task_execution_spec_(std::move(task_execution_spec)),
        task_spec_vector_(new std::vector<TaskSpecification>(specs)) {
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

  /// Whether this task has at least one coscheduled tasks.
  bool IsVectorTask() const {
    return task_spec_vector_ != nullptr && task_spec_vector_->size() > 1;
  }

  /// Return the list of coscheduled tasks, including the base task spec.
  const std::vector<TaskSpecification> &GetTaskSpecificationVector() const {
    RAY_CHECK(task_spec_vector_ != nullptr);
    return *task_spec_vector_;
  }

  /// Remove the given task ids from the task vector.
  std::vector<TaskSpecification> RemoveTaskSpecsFromVector(std::vector<TaskID> task_ids) {
    RAY_CHECK(task_spec_vector_ != nullptr);
    std::vector<TaskSpecification> out;
    auto it = task_spec_vector_->begin();
    while (it != task_spec_vector_->end()) {
      if (std::count(task_ids.begin(), task_ids.end(), it->TaskId()) > 0) {
        out.push_back(*it);
        task_spec_vector_->erase(it);
      } else {
        ++it;
      }
    }
    RAY_CHECK(out.size() == task_ids.size()) << out.size() << " vs " << task_ids.size();
    return out;
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
  std::shared_ptr<std::vector<TaskSpecification>> task_spec_vector_;
  /// A cached copy of the task's object dependencies, including arguments from
  /// the TaskSpecification and execution dependencies from the
  /// TaskExecutionSpecification.
  std::vector<ObjectID> dependencies_;
};

}  // namespace ray

#endif  // RAY_COMMON_TASK_TASK_H
