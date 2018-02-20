#ifndef TASK_CC
#define TASK_CC

#include "Task.h"

#include "common.h"

namespace ray {

const TaskExecutionSpecification &Task::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const {
  return task_spec_;
}

int64_t Task::NumDependencies() const {
  int64_t num_dependencies = task_spec_.NumArgs();
  num_dependencies += task_execution_spec_.ExecutionDependencies().size();
  return num_dependencies;
}

int Task::DependencyIdCount(int64_t dependency_index) const {
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = task_spec_.NumArgs();
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return task_spec_.ArgIdCount(dependency_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    CHECK((size_t) dependency_index < task_execution_spec_.ExecutionDependencies().size());
    /* All elements in the execution dependency list have exactly one ID. */
    return 1;
  }
}

ObjectID Task::DependencyId(int64_t dependency_index,
                                         int64_t id_index) const {
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. Find the total number of task arguments so
   * that we can index into the correct list. */
  int64_t num_args = task_spec_.NumArgs();
  if (dependency_index < num_args) {
    /* Index into the task arguments. */
    return task_spec_.ArgId(dependency_index, id_index);
  } else {
    /* Index into the execution dependencies. */
    dependency_index -= num_args;
    CHECK((size_t) dependency_index < task_execution_spec_.ExecutionDependencies().size());
    return task_execution_spec_.ExecutionDependencies()[dependency_index];
  }
}

bool Task::DependsOn(ObjectID object_id) const {
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
  for (auto dependency_id : task_execution_spec_.ExecutionDependencies()) {
    if (dependency_id == object_id) {
      return true;
    }
  }
  // The requested object ID was not a task argument or an execution dependency.
  // This task is not dependent on it.
  return false;
}

bool Task::IsStaticDependency(int64_t dependency_index) const {
  /* The first dependencies are the arguments of the task itself, followed by
   * the execution dependencies. If the requested dependency index is a task
   * argument, then it is a task dependency. */
  int64_t num_args = task_spec_.NumArgs();
  return (dependency_index < num_args);
}

} // end namespace ray
#endif
