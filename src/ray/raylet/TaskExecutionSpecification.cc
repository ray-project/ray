#ifndef TASK_EXECUTION_SPECIFICATION_CC
#define TASK_EXECUTION_SPECIFICATION_CC

#include <vector>

#include "TaskExecutionSpecification.h"

namespace ray {
std::vector<ObjectID> TaskExecutionSpec::ExecutionDependencies() const {
  return execution_dependencies_;
}

void TaskExecutionSpec::SetExecutionDependencies(
    const std::vector<ObjectID> &dependencies) {
  execution_dependencies_ = dependencies;
}

int TaskExecutionSpec::SpillbackCount() const {
  return spillback_count_;
}

void TaskExecutionSpec::IncrementSpillbackCount() {
  ++spillback_count_;
}

int64_t TaskExecutionSpec::LastTimeStamp() const {
  return last_timestamp_;
}

void TaskExecutionSpec::SetLastTimeStamp(int64_t new_timestamp) {
  last_timestamp_ = new_timestamp;
}

int64_t TaskExecutionSpec::NumDependencies() const {
  TaskSpec *spec = Spec();
  int64_t num_dependencies = TaskSpec_num_args(spec);
  num_dependencies += execution_dependencies_.size();
  return num_dependencies;
}

int TaskExecutionSpec::DependencyIdCount(int64_t dependency_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = TaskSpec_num_args(spec);
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return TaskSpec_arg_id_count(spec, dependency_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    CHECK((size_t) dependency_index < execution_dependencies_.size());
    /* All elements in the execution dependency list have exactly one ID. */
    return 1;
  }
}

ObjectID TaskExecutionSpec::DependencyId(int64_t dependency_index,
                                         int64_t id_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = TaskSpec_num_args(spec);
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return TaskSpec_arg_id(spec, dependency_index, id_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    CHECK((size_t) dependency_index < execution_dependencies_.size());
    return execution_dependencies_[dependency_index];
  }
}

bool TaskExecutionSpec::DependsOn(ObjectID object_id) const {
  // Iterate through the task arguments to see if it contains object_id.
  TaskSpec *spec = Spec();
  int64_t num_args = TaskSpec_num_args(spec);
  for (int i = 0; i < num_args; ++i) {
    int count = TaskSpec_arg_id_count(spec, i);
    for (int j = 0; j < count; j++) {
      ObjectID arg_id = TaskSpec_arg_id(spec, i, j);
      if (arg_id == object_id) {
        return true;
      }
    }
  }
  // Iterate through the execution dependencies to see if it contains object_id.
  for (auto dependency_id : execution_dependencies_) {
    if (dependency_id == object_id) {
      return true;
    }
  }
  // The requested object ID was not a task argument or an execution dependency.
  // This task is not dependent on it.
  return false;
}

bool TaskExecutionSpec::IsStaticDependency(int64_t dependency_index) const {
  TaskSpec *spec = Spec();
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. If the requested dependency index is a task
   * argument, then it is a task dependency. */
  int64_t num_args = TaskSpec_num_args(spec);
  return (dependency_index < num_args);
}

}

#endif TASK_EXECUTION_SPECIFICATION_CC
