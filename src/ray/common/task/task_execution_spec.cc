#include <sstream>

#include "ray/common/task/task_execution_spec.h"

namespace ray {

size_t TaskExecutionSpecification::NumForwards() const {
  return message_->num_forwards();
}

void TaskExecutionSpecification::IncrementNumForwards() {
  message_->set_num_forwards(message_->num_forwards() + 1);
}

std::string TaskExecutionSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "num_forwards=" << message_->num_forwards();
  return stream.str();
}

TaskExecutionSpecification::TaskExecutionSpecification() {}
TaskExecutionSpecification::TaskExecutionSpecification(rpc::TaskExecutionSpec message)
    : MessageWrapper(std::move(message)) {}
TaskExecutionSpecification::TaskExecutionSpecification(
    const std::string &serialized_binary)
    : MessageWrapper(serialized_binary) {}

}  // namespace ray
