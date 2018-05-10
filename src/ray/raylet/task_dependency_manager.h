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

  // Subscribe to object depedencies required by the task and check whether all
  // dependencies are fulfilled. This will track this task's dependencies until
  // UnsubscribeDependencies is called on the same task ID. If any dependencies
  // are missing, then when the last missing dependency later appears locally
  // via a call to HandleObjectLocal, the subscribed task will be returned.
  // This method may be called multiple times per task.
  //
  // \param task_id The ID of the task whose dependencies to subscribe to.
  // \param required_objects The objects required by the task.
  // \param remote_objects A reference to the objects that are now remote. This
  // method will append any object dependencies of the subscribed task that are
  // not local or pending creation by a locally queued task. The caller is
  // responsible for making these dependencies available locally.
  // \return Whether all of the given dependencies for the given task are
  // local.
  bool SubscribeDependencies(const TaskID &task_id,
                             const std::vector<ObjectID> &required_objects,
                             std::vector<ObjectID> &remote_objects);

  // Unsubscribe from the object dependencies required by this task.
  //
  // \param task_id The ID of the task whose dependencies to unsubscribe from.
  // \param canceled_objects A reference to the objects that are no longer
  // needed. This method will append any object dependencies of the
  // unsubscribed task that are no longer needed by any subscribed task.
  void UnsubscribeDependencies(const TaskID &task_id,
                               std::vector<ObjectID> &canceled_objects);

  // Mark that the given task is pending execution. Any objects that it creates
  // are now considered to be pending creation.
  //
  // \param task The task that is pending execution.
  // \param canceled_objects A reference to the objects that are no longer
  // needed. This method will append any objects that will be created by the
  // given task, if there is some subscribed task that depends on the object.
  // Since the objects are now pending creation, they no longer need to be
  // fetched or reconstructed.
  void TaskPending(const Task &task, std::vector<ObjectID> &canceled_objects);

  // Mark that the given task is no longer pending execution. Any objects that
  // it creates that are not already local are now considered to be remote.
  //
  // \param task_id The ID of the task to cancel.
  // \param remote_objects A reference to the objects that are now remote. This
  // method will append any objects that would have been created by the given
  // task, if there is some subscribed task that depends on the object and if
  // the object is not already local. Since the task is canceled, these objects
  // will only appear locally if fetched from a remote node or reconstructed.
  void TaskCanceled(const TaskID &task_id, std::vector<ObjectID> &remote_objects);

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that now
  /// have all of their dependencies fulfilled, once this object was made
  /// local.
  std::vector<TaskID> HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  /// \return A list of task IDs. This contains all subscribed tasks that
  /// previously had all of their dependencies fulfilled, but are now missing
  /// this object dependency.
  std::vector<TaskID> HandleObjectMissing(const ray::ObjectID &object_id);

 private:
  /// A struct to represent the object dependencies of a task.
  struct TaskDependencies {
    /// The objects that the task is dependent on. These must be local before
    /// the task is ready to execute.
    std::unordered_set<ObjectID> object_dependencies;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    int64_t num_missing_dependencies;
  };

  /// A mapping from task ID of each subscribed task to its list of object
  /// dependencies.
  std::unordered_map<ray::TaskID, TaskDependencies> task_dependencies_;
  /// A mapping from object ID of each object that is not locally available to
  /// the list of subscribed tasks that are dependent on it.
  std::unordered_map<ray::ObjectID, std::vector<ray::TaskID>> remote_object_dependencies_;
  /// The set of locally available objects.
  std::unordered_set<ray::ObjectID> local_objects_;
  /// The set of tasks that are pending execution. Any objects created by these
  /// tasks that are not already local are pending creation.
  std::unordered_set<ray::TaskID> pending_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
