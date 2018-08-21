#include "task.h"

namespace ray {

namespace raylet {

flatbuffers::Offset<protocol::Task> Task::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  auto task = CreateTask(fbb, task_spec_.ToFlatbuffer(fbb),
                         task_execution_spec_.ToFlatbuffer(fbb));
  return task;
}

const TaskExecutionSpecification &Task::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const { return task_spec_; }

void Task::SetExecutionDependencies(const std::vector<ObjectID> &dependencies) {
  task_execution_spec_.SetExecutionDependencies(dependencies);
  ComputeDependencies();
}

void Task::IncrementNumForwards() { task_execution_spec_.IncrementNumForwards(); }

const std::vector<ObjectID> &Task::GetDependencies() const { return dependencies_; }

void Task::ComputeDependencies() {
  dependencies_.clear();
  for (int i = 0; i < task_spec_.NumArgs(); ++i) {
    int count = task_spec_.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies_.push_back(task_spec_.ArgId(i, j));
    }
  }
  // TODO(atumanov): why not just return a const reference to ExecutionDependencies() and
  // avoid a copy.
  auto execution_dependencies = task_execution_spec_.ExecutionDependencies();
  dependencies_.insert(dependencies_.end(), execution_dependencies.begin(),
                       execution_dependencies.end());
}

void Task::CopyTaskExecutionSpec(const Task &task) {
  task_execution_spec_ = task.GetTaskExecutionSpec();
  ComputeDependencies();
}

}  // namespace raylet

}  // namespace ray
