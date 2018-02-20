#ifndef TASK_H
#define TASK_H

#include "TaskSpecification.h"
#include "TaskExecutionSpecification.h"

#include <inttypes.h>

namespace ray {
class Task {
public:
  Task(TaskSpecification task_spec): task_exe_spec_(TaskExecutionSpec()), task_spec_(task_spec) {}
  const TaskExecutionSpec &GetTaskExecutionSpec() const;
  const TaskSpecification &GetTaskSpecification() const;
  int64_t NumDependencies() const;
  virtual ~Task() {}
private:
  /// Task execution specification object, consisting of all dynamic/mutable
  /// information about this task.
  TaskExecutionSpec task_exe_spec_;
  /// Task specification object, consisting of immutable information about
  /// this task, including resource demand, object dependencies, etc.
  TaskSpecification task_spec_;

};

} // end namespace ray

#endif
