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
/// Responsible for managing task dependencies. Tasks that have object
/// dependencies that are missing locally should contact the manager to try to
/// make their dependencies available.
class TaskDependencyManager {
 public:
  /// Create a task dependency manager.
  ///
  /// \param object_manager A reference to the object manager so that the task
  /// dependency manager can issue requests to transfer objects.
  /// \param handler The handler to call for subscribed tasks whose
  /// dependencies have become available locally.
  TaskDependencyManager(ObjectManager &object_manager,
                        // ReconstructionPolicy &reconstruction_policy,
                        std::function<void(const TaskID &)> handler);

  /// Check whether a task's object dependencies are locally available.
  ///
  /// \param task The task whose object dependencies will be checked.
  /// \return Whether the task's object dependencies are ready.
  bool TaskReady(const Task &task) const;

  /// Subscribe to a task that has missing dependencies. The manager will
  /// attempt to make any missing dependencies available locally by transfer or
  /// by reconstruction. The registered handler will be called when the task's
  /// dependencies become locally available.
  ///
  /// \param task The task with missing dependencies.
  void SubscribeTaskReady(const Task &task);

  /// Stop waiting for a task's dependencies to become available.
  ///
  /// \param task_id The task ID of the task with missing dependencies.
  void UnsubscribeTaskReady(const TaskID &task_id);

  /// Mark an object as locally available. This is used for objects that do not
  /// have a stored value (e.g., actor execution dependencies).
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  void MarkDependencyReady(const ObjectID &object_id);

 private:
  /// Check whether the given list of objects are ready.
  bool argumentsReady(const std::vector<ObjectID> arguments) const;
  /// Handle an object added to the object store.
  void handleObjectReady(const ray::ObjectID &object_id);
  /// A reference to the object manager so that we can issue Pull requests of
  /// missing objects.
  ObjectManager &object_manager_;
  /// A mapping from task ID of each subscribed task to its list of
  /// dependencies.
  std::unordered_map<ray::TaskID, std::vector<ray::ObjectID>, UniqueIDHasher>
      task_dependencies_;
  // A mapping from object ID of each object that is not locally available to
  // the list of subscribed tasks that are dependent on it.
  std::unordered_map<ray::ObjectID, std::vector<ray::TaskID>, UniqueIDHasher>
      remote_object_dependencies_;
  // The set of locally available objects.
  std::unordered_set<ray::ObjectID, UniqueIDHasher> local_objects_;
  // The callback to call when a subscribed task becomes ready.
  std::function<void(const TaskID &)> task_ready_callback_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
