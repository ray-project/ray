#ifndef TASK_CC
#define TASK_CC

#include "Task.h"

namespace ray {

int64_t Task::NumDependencies() const {
  // Combine the task dependencies from the mutable and immutable parts of the
  // task. I.e., add execution dependencies size to task spec num dependencies.
  return 0;
}

const TaskExecutionSpec &Task::GetTaskExecutionSpec() const {
  throw std::runtime_error("Method not implemented");
}

const TaskSpecification &Task::GetTaskSpecification() const {
  return task_spec_;
}

} // end namespace ray
#endif
