#include "ray/raylet/task_execution_spec.h"

namespace ray {

namespace raylet {

const std::vector<ObjectID> &TaskExecutionSpecification::ExecutionDependencies() const {
  return dependencies_;
}

size_t TaskExecutionSpecification::NumForwards() const {
  return message_->num_forwards();
}

void TaskExecutionSpecification::IncrementNumForwards() {
  message_->set_num_forwards(message_->num_forwards() + 1);
}

}  // namespace raylet

}  // namespace ray
