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
  TaskDependencyManager(std::function<void(const ObjectID)> object_missing_handler,
                        std::function<void(const TaskID &)> task_ready_handler,
                        std::function<void(const TaskID &)> task_waiting_handler);

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

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  void HandleObjectReady(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  void HandleObjectMissing(const ray::ObjectID &object_id);

 private:
  enum class ObjectAvailability : unsigned int {
    kRemote = 0,
    kWaiting,
    kCreating,
    kLocal,
  };

  struct ObjectEntry {
    std::vector<ray::TaskID> dependent_tasks;
    ObjectAvailability status;
  };

  struct TaskEntry {
    std::vector<ObjectID> arguments;
    std::vector<ObjectID> returns;
    int64_t num_missing_arguments;
  };

  /// Check whether the given list of objects are ready.
  bool argumentsReady(const std::vector<ObjectID> arguments) const;

  std::function<void(const ObjectID &)> object_missing_callback_;
  // The callback to call when a subscribed task becomes ready.
  std::function<void(const TaskID &)> task_ready_callback_;
  // The callback to call when a subscribed task becomes ready.
  std::function<void(const TaskID &)> task_waiting_callback_;
  /// A mapping from task ID of each subscribed task to its list of
  /// dependencies.
  std::unordered_map<ray::TaskID, TaskEntry, UniqueIDHasher> task_dependencies_;
  // Information about each object that is locally available or pending
  // creation by a locally queued task.
  std::unordered_map<ray::ObjectID, ObjectEntry, UniqueIDHasher> local_objects_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_TASK_DEPENDENCY_MANAGER_H
