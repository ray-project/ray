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

  /// Get the number of dependencies. This comprises the immutable task
  /// arguments and the mutable execution dependencies.
  ///
  /// @return The number of dependencies.
  int64_t NumDependencies() const;

  /// Get the number of object IDs at the given dependency index.
  ///
  /// @param dependency_index The dependency index whose object IDs to count.
  /// @return The number of object IDs at the given dependency_index.
  int DependencyIdCount(int64_t dependency_index) const;

  /// Get the object ID of a given dependency index.
  ///
  /// @param dependency_index The index at which we should look up the object
  ///        ID.
  /// @param id_index The index of the object ID.
  ObjectID DependencyId(int64_t dependency_index, int64_t id_index) const;

  /// Compute whether the task is dependent on an object ID.
  ///
  /// @param object_id The object ID that the task may be dependent on.
  /// @return bool This returns true if the task is dependent on the given
  ///         object ID and false otherwise.
  bool DependsOn(ObjectID object_id) const;

  /// Returns whether the given dependency index is a static dependency (an
  /// argument of the immutable task).
  ///
  /// @param dependency_index The requested dependency index.
  /// @return bool This returns true if the requested dependency index is
  ///         immutable (an argument of the task).
  bool IsStaticDependency(int64_t dependency_index) const;

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
