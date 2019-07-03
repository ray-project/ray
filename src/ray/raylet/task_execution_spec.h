#ifndef RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
#define RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H

#include <vector>

#include "ray/common/id.h"
#include "ray/protobuf/common.pb.h"
#include "ray/rpc/message_wrapper.h"
#include "ray/rpc/util.h"

namespace ray {

namespace raylet {

using rpc::MessageWrapper;

/// \class TaskExecutionSpecification
///
/// The task execution specification encapsulates all mutable information about
/// the task. These fields may change at execution time, converse to the
/// TaskSpecification that is determined at submission time.
class TaskExecutionSpecification : public MessageWrapper<rpc::TaskExecutionSpec> {
 public:
  explicit TaskExecutionSpecification(rpc::TaskExecutionSpec message)
      : MessageWrapper(std::move(message)) {}

  explicit TaskExecutionSpecification(const std::string serialized_binary)
      : MessageWrapper(serialized_binary) {}

  /// Get the task's execution dependencies.
  ///
  /// \return A vector of object IDs representing this task's execution
  /// dependencies.
  const std::vector<ObjectID> ExecutionDependencies() const;

  /// Get the number of times this task has been forwarded.
  ///
  /// \return The number of times this task has been forwarded.
  size_t NumForwards() const;

  /// Increment the number of times this task has been forwarded.
  void IncrementNumForwards();
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
