#include <sstream>

#include "ray/common/task/task_execution_spec.h"

namespace ray {

const std::vector<ObjectID> TaskExecutionSpecification::ExecutionDependencies() const {
  return IdVectorFromProtobuf<ObjectID>(message_.dependencies());
}

size_t TaskExecutionSpecification::NumForwards() const { return message_.num_forwards(); }

void TaskExecutionSpecification::IncrementNumForwards() {
  message_.set_num_forwards(message_.num_forwards() + 1);
}

std::string TaskExecutionSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "num_dependencies=" << message_.dependencies_size()
         << ", num_forwards=" << message_.num_forwards();
  return stream.str();
}

}  // namespace ray
