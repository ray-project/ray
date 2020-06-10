#include <sstream>

#include "task.h"

namespace ray {

const TaskExecutionSpecification &Task::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const { return task_spec_; }

void Task::IncrementNumForwards() { task_execution_spec_.IncrementNumForwards(); }

const std::vector<ObjectID> &Task::GetDependencies() const { return dependencies_; }

void Task::ComputeDependencies() { dependencies_ = task_spec_.GetDependencies(); }

void Task::CopyTaskExecutionSpec(const Task &task) {
  task_execution_spec_ = task.task_execution_spec_;
}

std::string Task::DebugString() const {
  std::ostringstream stream;
  stream << "task_spec={" << task_spec_.DebugString() << "}, task_execution_spec={"
         << task_execution_spec_.DebugString() << "}";
  return stream.str();
}
void Task::SetOnDispatch(const DispatchTaskCallback &on_dispatch) {
  on_dispatch_ = on_dispatch;
}
void Task::SetOnSpillback(const SpillbackTaskCallback &on_spillback) {
  on_spillback_ = on_spillback;
}
void Task::SetOnCancellation(const CancelTaskCallback &on_cancellation) {
  on_cancellation_ = on_cancellation;
}

}  // namespace ray
