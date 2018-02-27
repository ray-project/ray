#ifndef TASK_DEPENDENCY_MANAGER_H
#define TASK_DEPENDENCY_MANAGER_H


#include "ray/id.h"
#include "Task.h"
#include "ray/om/object_manager.h"
#include "reconstruction_policy.h"

namespace ray {

class ReconstructionPolicy;

class TaskDependencyManager {
 public:
  TaskDependencyManager(
      ObjectManager &object_manager,
      ReconstructionPolicy &reconstruction_policy,
      boost::function<void(const TaskID&)> handler);
  // Check whether a task's object dependencies are locally available and is
  // therefore ready to run.
  bool TaskReady(const Task &task) const;
  // Subscribe to the TaskReady callback for a task that has missing
  // dependencies. The registered TaskReady callback will be called when the
  // task's dependencies are locally available.
  void SubscribeTaskReady(const Task &task);
  // Stop waiting for a task's dependencies to become available.
  void UnsubscribeTaskReady(const TaskID &task_id);
  // Mark an object as locally available.
  void MarkDependencyReady(const ObjectID &object);
 private:
  // Check whether the given list of objects are ready.
  bool argumentsReady(const std::vector<ObjectID> arguments) const;
  // Handle an object added to the object store.
  void handleObjectReady(const ray::ObjectID& object_id);
  // A reference to the object manager so that we can issue Pull requests of
  // missing objects.
  ObjectManager &object_manager_;
  // A reference to the reconstruction policy so that we can decide when
  // objects should be reconstructed.
  ReconstructionPolicy &reconstruction_policy_;
  // A mapping from task ID of each subscribed task to its list of
  // dependencies.
  std::unordered_map<ray::TaskID, std::vector<ray::ObjectID>, UniqueIDHasher> task_dependencies_;
  // A mapping from object ID of each object that is not locally available to
  // the list of subscribed tasks that are dependent on it.
  std::unordered_map<ray::ObjectID, std::vector<ray::TaskID>, UniqueIDHasher> remote_object_dependencies_;
  // The set of locally available objects.
  std::unordered_set<ray::ObjectID, UniqueIDHasher> local_objects_;
  // The callback to call when a subscribed task becomes ready.
  boost::function<void(const TaskID&)> task_ready_callback_;
};

} // end namespace ray

#endif  // TASK_DEPENDENCY_MANAGER_H
