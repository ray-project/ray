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

void Task::OnDispatchInstead(const DispatchTaskCallback &callback) {
  on_dispatch_ = callback;
}

void Task::OnSpillbackInstead(const SpillbackTaskCallback &callback) {
  on_spillback_ = callback;
}

const DispatchTaskCallback &Task::OnDispatch() const { return on_dispatch_; }

const SpillbackTaskCallback &Task::OnSpillback() const { return on_spillback_; }

Task::Task() {}

Task::Task(const rpc::Task &message)
    : task_spec_(message.task_spec()),
      task_execution_spec_(message.task_execution_spec()) {
  ComputeDependencies();
}

Task::Task(TaskSpecification task_spec, TaskExecutionSpecification task_execution_spec)
    : task_spec_(std::move(task_spec)),
      task_execution_spec_(std::move(task_execution_spec)) {
  ComputeDependencies();
}

}  // namespace ray
