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
  message_->mutable_task_execution_spec()->CopyFrom(
      task.GetTaskExecutionSpec().GetMessage());
  task_execution_spec_.Reset(*message_->mutable_task_execution_spec());
}

}  // namespace raylet

}  // namespace ray
