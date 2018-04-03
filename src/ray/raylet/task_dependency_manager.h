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
  ///
  /// \param object_remote_handler The handler to call for an object that is
  /// required by a waiting task. This handler is only called for objects that
  /// are neither local nor are they going to be created by a queued task.
  /// \param task_ready_handler The handler to call for waiting tasks that are
  /// now ready to run because all dependencies are available locally.
  /// \param task_waiting_handler The handler to call for ready tasks that must
  /// now wait to execute, e.g., due to an evicted dependency.
  TaskDependencyManager(std::function<void(const ObjectID)> object_remote_handler,
                        std::function<void(const TaskID &)> task_ready_handler,
                        std::function<void(const TaskID &)> task_waiting_handler);

  /// Subscribe to a task that is waiting or ready to execute. The registered
  /// remote object handler will be called for any missing dependencies that
  /// need to be transferred from another node or reconstructed. The registered
  /// task ready handler will be called when the task's dependencies become
  /// locally available. The registered task waiting handler will be called
  /// when one or more of the task's dependencies is not locally available.
  ///
  /// \param task The locally queued task.
  void SubscribeTask(const Task &task);

  /// Unsubscribe from a task that was locally queued that has now been
  /// forwarded to a remote node. Its return values are now considered to be
  /// remote.
  ///
  /// \param task_id The task ID of the forwarded task.
  void UnsubscribeForwardedTask(const TaskID &task_id);

  /// Unsubscribe from a task that was locally queued and has now begun
  /// execution. This assumes that the return values are being constructed, and
  /// that notifications about their local availability will eventually be
  /// received.
  ///
  /// \param task_id The task ID of the executing task.
  void UnsubscribeExecutingTask(const TaskID &task_id);

  /// Handle an object becoming locally available.
  ///
  /// \param object_id The object ID of the object to mark as locally
  /// available.
  void HandleObjectLocal(const ray::ObjectID &object_id);

  /// Handle an object that is no longer locally available.
  ///
  /// \param object_id The object ID of the object that was previously locally
  /// available.
  void HandleObjectMissing(const ray::ObjectID &object_id);

 private:
  /// The status of the object, with respect to the local object store.
  enum class ObjectAvailability : unsigned int {
    /// The object is not local and will not be created by any locally queued
    /// task.
    kRemote = 0,
    /// The task that creates this object is waiting to be executed. If the
    /// task is dequeued and forwarded to another node, then the object will be
    /// marked as remote. If the task is dequeued and begins execution, then
    /// the object will be marked as being constructed.
    kWaiting,
    /// The task that creates this object has started execution and will create
    /// the object upon completion.  The object will be marked as local once
    /// the notification that it is local is received.
    kConstructing,
    /// The object is local. The object will be marked as remote if a
    /// notification that it is missing is received.
    kLocal,
  };

  struct ObjectEntry {
    /// The IDs of the tasks that are queued locally and dependent on this
    /// object.
    std::vector<ray::TaskID> dependent_tasks;
    /// The object's availability on the local node, with respect to the local
    /// object store.
    ObjectAvailability status;
  };

  struct TaskEntry {
    /// The objects that the task is dependent on. These must be local before
    /// the task is ready to execute.
    std::vector<ObjectID> arguments;
    /// The objects that the task returns. These will become local once the
    /// task finishes execution.
    std::vector<ObjectID> returns;
    /// The number of object arguments that are not available locally. This
    /// must be zero before the task is ready to execute.
    int64_t num_missing_arguments;
  };

  /// Unsubscribe from a task and mark the object availability of its return
  /// values with the given status, if they are not already local.
  void UnsubscribeTask(const TaskID &task_id, ObjectAvailability outputs_status);
  /// The callback to call if an object is required by a queued task, not
  /// available locally, and will not be created by a queued task.
  std::function<void(const ObjectID &)> object_remote_callback_;
  // The callback to call when a subscribed task becomes ready for execution.
  std::function<void(const TaskID &)> task_ready_callback_;
  // The callback to call when a subscribed task is no longer ready for
  // execution.
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
