#include "task.h"

namespace ray {

namespace raylet {

flatbuffers::Offset<protocol::Task> Task::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  auto task = CreateTask(fbb, task_spec_.ToFlatbuffer(fbb),
                         task_execution_spec_.ToFlatbuffer(fbb));
  return task;
}

TaskExecutionSpecification &Task::GetTaskExecutionSpec() { return task_execution_spec_; }

const TaskExecutionSpecification &Task::GetTaskExecutionSpecReadonly() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const { return task_spec_; }

const std::vector<ObjectID> Task::GetDependencies() const {
  std::vector<ObjectID> dependencies;
  for (int i = 0; i < task_spec_.NumArgs(); ++i) {
    int count = task_spec_.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(task_spec_.ArgId(i, j));
    }
  }
  // TODO(atumanov): why not just return a const reference to ExecutionDependencies() and
  // avoid a copy.
  auto execution_dependencies = task_execution_spec_.ExecutionDependencies();
  dependencies.insert(dependencies.end(), execution_dependencies.begin(),
                      execution_dependencies.end());
  return dependencies;
}

bool Task::DependsOn(const ObjectID &object_id) const {
  // Iterate through the task arguments to see if it contains object_id.
  int64_t num_args = task_spec_.NumArgs();
  for (int i = 0; i < num_args; ++i) {
    int count = task_spec_.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      ObjectID arg_id = task_spec_.ArgId(i, j);
      if (arg_id == object_id) {
        return true;
      }
    }
  }
  // Iterate through the execution dependencies to see if it contains object_id.
  for (const auto &dependency_id : task_execution_spec_.ExecutionDependencies()) {
    if (dependency_id == object_id) {
      return true;
    }
  }
  // The requested object ID was not a task argument or an execution dependency.
  // This task is not dependent on it.
  return false;
}

void Task::Update(const Task &task) {
  task_execution_spec_ = task.GetTaskExecutionSpecReadonly();
}

}  // namespace raylet

}  // namespace ray
