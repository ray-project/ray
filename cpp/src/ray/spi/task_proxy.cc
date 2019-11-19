
#include <utility>

#include "../runtime/ray_runtime.h"
#include "task_proxy.h"

namespace ray {

TaskProxy::TaskProxy(std::unique_ptr<TaskInterface> scheduler) {
  _taskInterface = std::move(scheduler);
}

std::unique_ptr<UniqueId> TaskProxy::submit(const InvocationSpec &invocation) {
  TaskSpec ts;
  ts.taskId = invocation.taskId;
  ts.actorId = invocation.actorId;
  ts.actorCounter = invocation.actorCounter;
  RayRuntime &rayRuntime = RayRuntime::getInstance();
  TaskSpec *current = rayRuntime.getCurrentTask();
  ts.driverId = current->driverId;
  ts.parentTaskId = current->taskId;
  ts.parentCounter = rayRuntime.getNextPutIndex();
  ts.args = std::move(invocation.args);
  ts.set_func_offset(invocation.func_offset);
  ts.set_exec_func_offset(invocation.exec_func_offset);
  ts.returnIds = buildReturnIds(invocation.taskId, 1);
  auto rt = ts.returnIds.front()->copy();
  rayRuntime.setCurrentTask(ts);
  _taskInterface->submitTask(std::move(ts));
  return rt;
}

std::unique_ptr<TaskSpec> TaskProxy::getTask() { return _taskInterface->getTask(); }

void TaskProxy::markTaskPutDependency(const UniqueId &taskId, const UniqueId &objectId) {
  _taskInterface->markTaskPutDependency(taskId, objectId);
}

void TaskProxy::reconstructObject(const UniqueId &objectId) {
  _taskInterface->reconstructObject(objectId);
}

void TaskProxy::notifyUnblocked() { _taskInterface->notifyUnblocked(); }

const UniqueId &TaskProxy::getSchedulerId() { return _taskInterface->getSchedulerId(); }

std::unique_ptr<UniqueId> TaskProxy::buildReturnId(const UniqueId &taskId, int index) {
  return taskId.taskComputeReturnId(index);
}

std::list<std::unique_ptr<UniqueId> > TaskProxy::buildReturnIds(const UniqueId &taskId,
                                                                int returnCount) {
  std::list<std::unique_ptr<UniqueId> > returnIds;
  for (int i = 0; i < returnCount; i++) {
    returnIds.push_back(taskId.taskComputeReturnId(i));
  }
  return returnIds;
}

}  // namespace ray