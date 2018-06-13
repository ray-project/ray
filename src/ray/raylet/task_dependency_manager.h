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
/// Responsible for managing object dependencies for tasks.  The caller can
/// subscribe to object dependencies for a task. The task manager will
/// determine which object dependencies are remote. These are the objects that
/// are neither in the local object store, nor will they be created by a
/// locally queued task. The task manager will request that these objects be
/// made available locally, either by object transfer from a remote node or
/// reconstruction. The task manager will also cancel these objects if they are
/// no longer needed by any task.
class TaskDependencyManager {
 public:
  /// Create a task dependency manager.
  TaskDependencyManager(ObjectManagerInterface &object_manager);

  /// Check whether an object is locally available.
  ///
  /// \param object_id The object to check for.
  /// \return Whether the object is local.
  bool CheckObjectLocal(const ObjectID &object_id) const;

  /// Subscribe to object depedencies required by the task and check whether
  /// all dependencies are fulfilled. This will track this task's dependencies
  /// until UnsubscribeDependencies is called on the same task ID. If any
  /// dependencies are remote, then they will be requested. When the last
  /// remote dependency later appears locally via a call to HandleObjectLocal,
  /// the subscribed task will be returned by the HandleObjectLocal call,
  /// signifying that it is ready to run. This method may be called multiple
  /// times per task.
  ///
  /// \param task_id The ID of the task whose dependencies to subscribe to.
  /// \param required_objects The objects required by the task.
  /// \return Whether all of the given dependencies for the given task are
  /// local.
  bool SubscribeDependencies(const TaskID &task_id,
                             const std::vector<ObjectID> &required_objects);

  /// Unsubscribe from the object dependencies required by this task. If the
  /// objects were remote and are no longer required by any subscribed task,
  /// then they will be canceled.
  ///
  /// \param task_id The ID of the task whose dependencies to unsubscribe from.
  void UnsubscribeDependencies(const TaskID &task_id);

  /// Mark that the given task is pending execution. Any objects that it creates
  /// are now considered to be pending creation. If there are any subscribed
  /// tasks that depend on these objects, then the objects will be canceled.
  ///
  /// \param task The task that is pending execution.
  void TaskPending(const Task &task);

  /// Mark that the given task is no longer pending execution. Any objects that
  /// it creates that are not already local are now considered to be remote. If
  /// there are any subscribed tasks that depend on these objects, then the
  /// objects will be requested.
  ///
  /// \param task_id The ID of the task to cancel.
  void TaskCanceled(const TaskID &task_id);

  /// Handle an object becoming locally available. If there are any subscribed
  /// tasks that depend on this object, then the object will be canceled.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that now
  /// have all of their dependencies fulfilled, once this object was made
  /// local.
  std::vector<TaskID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available. If there are any
  /// subscribed tasks that depend on this object, then the object will be
  /// requested.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that
  /// previously had all of their dependencies fulfilled, but are now missing
  /// this object dependency.
  std::vector<TaskID> HandleObjectMissing(const ray::ObjectID &object_id);

 private:
  using ObjectDependencyMap = std::unordered_map<ray::ObjectID, std::vector<ray::TaskID>>;

  /// A struct to represent the object dependencies of a task.
  struct TaskDependencies {
    /// The objects that the task is dependent on. These must be local before
    /// the task is ready to execute.
    std::unordered_set<ObjectID> object_dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    int64_t num_missing_dependencies;
  };

  /// Check whether the given object needs to be made available through object
  /// transfer or reconstruction. These are objects for which: (1) there is a
  /// subscribed task dependent on it, (2) the object is not local, and (3) the
  /// task that creates the object is not pending execution locally.
  bool CheckObjectRequired(const ObjectID &object_id) const;
  /// If the given object is required, then request that the object be made
  /// available through object transfer or reconstruction.
  void HandleRemoteDependencyRequired(const ObjectID &object_id);
  /// If the given object is no longer required, then cancel any in-progress
  /// operations to make the object available through object transfer or
  /// reconstruction.
  void HandleRemoteDependencyCanceled(const ObjectID &object_id);

  ObjectManagerInterface &object_manager_;
  /// A mapping from task ID of each subscribed task to its list of object
  /// dependencies.
  std::unordered_map<ray::TaskID, TaskDependencies> task_dependencies_;
  /// All tasks whose outputs are required by a subscribed task. This is a
  /// mapping from task ID to information about the objects that the task
  /// creates, either by return value or by `ray.put`. For each object, we
  /// store the IDs of the subscribed tasks that are dependent on the object.
  std::unordered_map<ray::TaskID, ObjectDependencyMap> required_tasks_;
  /// Objects that are required by a subscribed task, are not local, and are
  /// not created by a pending task. For these objects, there are pending
  /// operations to make the object available.
  std::unordered_set<ray::ObjectID> required_objects_;
  /// The set of locally available objects.
  std::unordered_set<ray::ObjectID> local_objects_;
  /// The set of tasks that are pending execution. Any objects created by these
  /// tasks that are not already local are pending creation.
  std::unordered_set<ray::TaskID> pending_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
