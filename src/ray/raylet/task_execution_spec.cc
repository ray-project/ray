#include "ray/raylet/task_execution_spec.h"

namespace ray {

namespace raylet {

TaskExecutionSpecification::TaskExecutionSpecification(
    const std::vector<ObjectID> &&dependencies) {
  SetExecutionDependencies(dependencies);
}

TaskExecutionSpecification::TaskExecutionSpecification(
    const std::vector<ObjectID> &&dependencies, int num_forwards) {
  // TaskExecutionSpecification(std::move(dependencies));
  SetExecutionDependencies(dependencies);
  execution_spec_.num_forwards = num_forwards;
}

flatbuffers::Offset<protocol::TaskExecutionSpecification>
TaskExecutionSpecification::ToFlatbuffer(flatbuffers::FlatBufferBuilder &fbb) const {
  fbb.ForceDefaults(true);
  return protocol::TaskExecutionSpecification::Pack(fbb, &execution_spec_);
}

std::vector<ObjectID> TaskExecutionSpecification::ExecutionDependencies() const {
  std::vector<ObjectID> dependencies;
  for (const auto &dependency : execution_spec_.dependencies) {
    dependencies.push_back(ObjectID::from_binary(dependency));
  }
  return dependencies;
}

void TaskExecutionSpecification::SetExecutionDependencies(
    const std::vector<ObjectID> &dependencies) {
  execution_spec_.dependencies.clear();
  for (const auto &dependency : dependencies) {
    execution_spec_.dependencies.push_back(dependency.binary());
  }
}

int TaskExecutionSpecification::NumForwards() const {
  return execution_spec_.num_forwards;
}

void TaskExecutionSpecification::IncrementNumForwards() {
  execution_spec_.num_forwards += 1;
}

int64_t TaskExecutionSpecification::LastTimestamp() const {
  return execution_spec_.last_timestamp;
}

void TaskExecutionSpecification::SetLastTimestamp(int64_t new_timestamp) {
  execution_spec_.last_timestamp = new_timestamp;
}

}  // namespace raylet

}  // namespace ray
