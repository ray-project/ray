#ifndef TASK_DEPENDENCY_MANAGER_CC
#define TASK_DEPENDENCY_MANAGER_CC

#include "task_dependency_manager.h"

using namespace std;
namespace ray {


TaskDependencyManager::TaskDependencyManager(
    ObjectManager &object_manager,
  ReconstructionPolicy &reconstruction_policy)
    : object_manager_(object_manager),
      reconstruction_policy_(reconstruction_policy) {
}

bool TaskDependencyManager::TaskReady(Task &task) {
  throw std::runtime_error("Method not implemented");
}

void TaskDependencyManager::RegisterTask(Task &task) {
  throw std::runtime_error("Method not implemented");
}

void TaskDependencyManager::DeregisterTask(Task &task) {
  throw std::runtime_error("Method not implemented");
}

void TaskDependencyManager::RegisterTaskReadyHandler(boost::function<void(const TaskID&)> handler) {
  throw std::runtime_error("Method not implemented");
}

void TaskDependencyManager::MarkDependencyReady(const ObjectID &object) {
  throw std::runtime_error("Method not implemented");
}

} // end namespace ray

#endif  // TASK_DEPENDENCY_MANAGER_CC
