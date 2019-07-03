#include "task.h"

namespace ray {

namespace raylet {

const TaskExecutionSpecification &Task::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const { return task_spec_; }

void Task::IncrementNumForwards() { task_execution_spec_.IncrementNumForwards(); }

const std::vector<ObjectID> &Task::GetDependencies() const {
  return task_execution_spec_.ExecutionDependencies();
}

void Task::CopyTaskExecutionSpec(const Task &task) {
  task_execution_spec_ = task.task_execution_spec_;
}

}  // namespace raylet

}  // namespace ray
