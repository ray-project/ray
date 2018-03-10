#include "ray/raylet/task_execution_spec.h"

namespace ray {

TaskExecutionSpecification::TaskExecutionSpecification(
    const std::vector<ObjectID> &&execution_dependencies)
    : execution_dependencies_(std::move(execution_dependencies)),
      last_timestamp_(0),
      spillback_count_(0) {}

TaskExecutionSpecification::TaskExecutionSpecification(
    const std::vector<ObjectID> &&execution_dependencies, int spillback_count)
    : execution_dependencies_(std::move(execution_dependencies)),
      last_timestamp_(0),
      spillback_count_(spillback_count) {}

const std::vector<ObjectID> &TaskExecutionSpecification::ExecutionDependencies() const {
  return execution_dependencies_;
}

void TaskExecutionSpecification::SetExecutionDependencies(
    const std::vector<ObjectID> &dependencies) {
  execution_dependencies_ = dependencies;
}

int TaskExecutionSpecification::SpillbackCount() const { return spillback_count_; }

void TaskExecutionSpecification::IncrementSpillbackCount() { ++spillback_count_; }

int64_t TaskExecutionSpecification::LastTimeStamp() const { return last_timestamp_; }

void TaskExecutionSpecification::SetLastTimeStamp(int64_t new_timestamp) {
  last_timestamp_ = new_timestamp;
}

}  // namespace ray
