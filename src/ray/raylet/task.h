#ifndef RAY_RAYLET_TASK_H
#define RAY_RAYLET_TASK_H

#include <inttypes.h>

#include "ray/protobuf/common.pb.h"
#include "ray/raylet/format/node_manager_generated.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"
#include "ray/rpc/message_wrapper.h"

namespace ray {

namespace raylet {

/// \class Task
///
/// A Task represents a Ray task and a specification of its execution (e.g.,
/// resource demands). The task's specification contains both immutable fields,
/// determined at submission time, and mutable fields, determined at execution
/// time.
class Task : public rpc::MessageWrapper<rpc::Task> {
 public:
  explicit Task(rpc::Task &message)
      : MessageWrapper(message),
        task_spec_(message_->task_spec()),
        task_execution_spec_(*message_->mutable_task_execution_spec()) {}

  explicit Task(std::unique_ptr<rpc::Task> message)
      : MessageWrapper(std::move(message)),
        task_spec_(message_->task_spec()),
        task_execution_spec_(*message_->mutable_task_execution_spec()) {}

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

 private:
  /// Task specification object, consisting of immutable information about this
  /// task determined at submission time. Includes resource demand, object
  /// dependencies, etc.
  TaskSpecification task_spec_;
  /// Task execution specification, consisting of all dynamic/mutable
  /// information about this task determined at execution time..
  TaskExecutionSpecification task_execution_spec_;
};

std::string SerializeTaskAsString(const std::vector<ObjectID> *dependencies,
                                  const TaskSpecification *task_spec);

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_H
