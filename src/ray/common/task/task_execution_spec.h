#ifndef RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
#define RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H

#include <vector>

#include "ray/common/id.h"
#include "ray/common/task/task_common.h"
#include "ray/common/grpc_util.h"

namespace ray {

/// Wrapper class of protobuf `TaskExecutionSpec`, see `common.proto` for details.
class TaskExecutionSpecification : public MessageWrapper<rpc::TaskExecutionSpec> {
 public:
  /// Construct from a protobuf message object.
  /// The input message will be **copied** into this object.
  ///
  /// \param message The protobuf message.
  explicit TaskExecutionSpecification(rpc::TaskExecutionSpec message)
      : MessageWrapper(std::move(message)) {}

  /// Construct from protobuf-serialized binary.
  ///
  /// \param serialized_binary Protobuf-serialized binary.
  explicit TaskExecutionSpecification(const std::string &serialized_binary)
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

}  // namespace ray

#endif  // RAY_RAYLET_TASK_EXECUTION_SPECIFICATION_H
