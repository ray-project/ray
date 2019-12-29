#ifndef RAY_COMMON_TASK_TASK_H
#define RAY_COMMON_TASK_TASK_H

#include <inttypes.h>

#include "ray/common/task/task_common.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"

namespace ray {

typedef std::function<void(const std::shared_ptr<void>, const std::string &, int,
                           const WorkerID &, const ResourceIdSet &)>
    DispatchTaskCallback;
/// Arguments are the raylet ID to spill back to, the raylet's
/// address and the raylet's port.
typedef std::function<void(const ClientID &, const std::string &, int)>
    SpillbackTaskCallback;

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

  /// Construct a `Task` object from a `TaskSpecification` and a
  /// `TaskExecutionSpecification`.
  Task(TaskSpecification task_spec, TaskExecutionSpecification task_execution_spec)
      : task_spec_(std::move(task_spec)),
        task_execution_spec_(std::move(task_execution_spec)) {
    ComputeDependencies();
  }

  /// Override dispatch behaviour.
  void OnDispatchInstead(const DispatchTaskCallback &callback) {
    on_dispatch_ = callback;
  }

  /// Override spillback behaviour.
  void OnSpillbackInstead(const SpillbackTaskCallback &callback) {
    on_spillback_ = callback;
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

  /// Returns the override dispatch task callback, or nullptr.
  const DispatchTaskCallback &OnDispatch() const { return on_dispatch_; }

  /// Returns the override spillback task callback, or nullptr.
  const SpillbackTaskCallback &OnSpillback() const { return on_spillback_; }

  std::string DebugString() const;

 private:
  void ComputeDependencies();

  /// Task specification object, consisting of immutable information about this
  /// task determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  TaskSpecification task_spec_;
  /// Task execution specification, consisting of all dynamic/mutable
  /// information about this task determined at execution time.
  TaskExecutionSpecification task_execution_spec_;
  /// A cached copy of the task's object dependencies, including arguments from
  /// the TaskSpecification and execution dependencies from the
  /// TaskExecutionSpecification.
  std::vector<ObjectID> dependencies_;

  /// For direct task calls, overrides the dispatch behaviour to send an RPC
  /// back to the submitting worker.
  mutable DispatchTaskCallback on_dispatch_ = nullptr;
  /// For direct task calls, overrides the spillback behaviour to send an RPC
  /// back to the submitting worker.
  mutable SpillbackTaskCallback on_spillback_ = nullptr;
};

}  // namespace ray

#endif  // RAY_COMMON_TASK_TASK_H
