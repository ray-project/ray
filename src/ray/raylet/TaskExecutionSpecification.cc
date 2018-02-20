#ifndef TASK_EXECUTION_SPECIFICATION_CC
#define TASK_EXECUTION_SPECIFICATION_CC

#include <vector>

#include "TaskExecutionSpecification.h"

namespace ray {

TaskExecutionSpecification::TaskExecutionSpecification():
  execution_dependencies_(std::vector<ObjectID>()),
  last_timestamp_(0),
  spillback_count_(0) {}

TaskExecutionSpecification::TaskExecutionSpecification(const std::vector<ObjectID> &&execution_dependencies) :
  execution_dependencies_(std::move(execution_dependencies)),
  last_timestamp_(0),
  spillback_count_(0) {}

TaskExecutionSpecification::TaskExecutionSpecification(const std::vector<ObjectID> &&execution_dependencies,
                  int spillback_count) :
  execution_dependencies_(std::move(execution_dependencies)),
  last_timestamp_(0),
  spillback_count_(spillback_count) {}

TaskExecutionSpecification::TaskExecutionSpecification(const TaskExecutionSpecification &execution_spec) :
  execution_dependencies_(execution_spec.ExecutionDependencies()),
  last_timestamp_(execution_spec.LastTimeStamp()),
  spillback_count_(execution_spec.SpillbackCount()) {}

const std::vector<ObjectID> &TaskExecutionSpecification::ExecutionDependencies() const {
  return execution_dependencies_;
}

void TaskExecutionSpecification::SetExecutionDependencies(
    const std::vector<ObjectID> &dependencies) {
  execution_dependencies_ = dependencies;
}

int TaskExecutionSpecification::SpillbackCount() const {
  return spillback_count_;
}

void TaskExecutionSpecification::IncrementSpillbackCount() {
  ++spillback_count_;
}

int64_t TaskExecutionSpecification::LastTimeStamp() const {
  return last_timestamp_;
}

void TaskExecutionSpecification::SetLastTimeStamp(int64_t new_timestamp) {
  last_timestamp_ = new_timestamp;
}

} // end namespace ray

#endif
