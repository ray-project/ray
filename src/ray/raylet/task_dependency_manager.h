#ifndef RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
#define RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H

// clang-format off
#include "ray/id.h"
#include "ray/raylet/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/reconstruction_policy.h"
// clang-format on

namespace ray {

namespace raylet {

class ReconstructionPolicy;

/// \class TaskDependencyManager
///
/// Responsible for managing task dependencies for all queued tasks that are
/// waiting or ready to execute locally. For each queued task, the manager
/// determines which object dependencies are missing. These are the objects
/// that are neither in the local object store, nor will they be created by a
/// locally queued task. These objects may only be made available by object
/// transfer from a remote node or reconstruction.
class TaskDependencyManager {
 public:
  /// Create a task dependency manager.
  TaskDependencyManager();

  /// Check whether an object is locally available.
  ///
  /// \param object_id The object to check for.
  /// \return Whether the object is local.
  bool CheckObjectLocal(const ObjectID &object_id) const;

  // Subscribe to objects required by the task. Returns whether the given task
  // is ready to run.
  bool SubscribeDependencies(const TaskID &task_id,
                             const std::vector<ObjectID> &required_objects,
                             std::vector<ObjectID> &remote_objects);

  // Unsubscribe from objects required by the task.
  void UnsubscribeDependencies(const TaskID &task_id,
                               std::vector<ObjectID> &canceled_objects);

  // The task is pending creation. Any objects that it creates are no longer needed.
  void TaskPending(const Task &task, std::vector<ObjectID> &canceled_objects);

  // The task is no longer pending creation. Any objects that it creates that
  // are not already local will not appear.
  void TaskCanceled(const TaskID &task_id, std::vector<ObjectID> &remote_objects);

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  std::vector<TaskID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  std::vector<TaskID> HandleObjectMissing(const ray::ObjectID &object_id);

 private:
  struct TaskDependencies {
    /// The objects that the task is dependent on. These must be local before
    /// the task is ready to execute.
    std::vector<ObjectID> object_dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    int64_t num_missing_dependencies;
  };

  /// A mapping from task ID of each subscribed task to its list of
  /// dependencies.
  std::unordered_map<ray::TaskID, TaskDependencies> task_dependencies_;
  // A mapping from object ID of each object that is not locally available to
  // the list of subscribed tasks that are dependent on it.
  std::unordered_map<ray::ObjectID, std::vector<ray::TaskID>> remote_object_dependencies_;
  std::unordered_set<ray::ObjectID> local_objects_;
  std::unordered_set<ray::TaskID> pending_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
