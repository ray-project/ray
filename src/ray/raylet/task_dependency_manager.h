#ifndef TASK_DEPENDENCY_MANAGER_H
#define TASK_DEPENDENCY_MANAGER_H


#include "ray/id.h"
#include "Task.h"
#include "object_manager.h"
#include "reconstruction_policy.h"

namespace ray {

class ObjectManager;
class ReconstructionPolicy;

class TaskDependencyManager {
 public:
  TaskDependencyManager(
      ObjectManager &object_manager,
      ReconstructionPolicy &reconstruction_policy);
  bool TaskReady(Task &task);
  void RegisterTask(Task &task);
  void DeregisterTask(Task &task);
  void RegisterTaskReadyHandler(boost::function<void(const TaskID&)> handler);
  void MarkDependencyReady(const ObjectID &object);
 private:
  ObjectManager &object_manager_;
  ReconstructionPolicy &reconstruction_policy_;
};

} // end namespace ray

#endif  // TASK_DEPENDENCY_MANAGER_H
