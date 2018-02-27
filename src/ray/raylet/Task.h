#ifndef TASK_H
#define TASK_H

#include "TaskSpecification.h"
#include "TaskExecutionSpecification.h"

#include <inttypes.h>

namespace ray {
class Task {
public:
  Task(const TaskExecutionSpecification &execution_spec,
       const TaskSpecification &task_spec):
      task_execution_spec_(execution_spec), task_spec_(task_spec) {}
  const TaskExecutionSpecification &GetTaskExecutionSpec() const;
  const TaskSpecification &GetTaskSpecification() const;
  virtual ~Task() {}

  /// Get the dependencies. This comprises the immutable task arguments and the
  /// mutable execution dependencies.
  ///
  /// @return The object dependencies.
  const std::vector<ObjectID> GetDependencies() const;

  /// Compute whether the task is dependent on an object ID.
  ///
  /// @param object_id The object ID that the task may be dependent on.
  /// @return bool This returns true if the task is dependent on the given
  ///         object ID and false otherwise.
  bool DependsOn(ObjectID object_id) const;

private:
  /// Task execution specification object, consisting of all dynamic/mutable
  /// information about this task.
  TaskExecutionSpecification task_execution_spec_;
  /// Task specification object, consisting of immutable information about
  /// this task, including resource demand, object dependencies, etc.
  TaskSpecification task_spec_;

};

} // end namespace ray

#endif
