
#include "RayRuntime.h"

#include <cassert>

#include "../agent.h"
#include <ray/api.h>
#include <ray/api/RayMode.h>
#include "InvocationExecutor.h"
#include "RayDevRuntime.h"
#include "RayNativeRuntime.h"

namespace ray {

std::unique_ptr<RayRuntime> RayRuntime::_ins;
std::once_flag RayRuntime::isInited;

RayRuntime &RayRuntime::init(std::shared_ptr<RayConfig> params) {
  doInit(params);
  Ray::init();
  return *_ins;
}

RayRuntime &RayRuntime::doInit(std::shared_ptr<RayConfig> params) {
  std::call_once(isInited, [params] {
    if (params->runMode == RunMode::SINGLE_PROCESS) {
      _ins.reset(new RayDevRuntime(params));
      Ray_agent_init();
    } else {
      _ins.reset(new RayNativeRuntime(params));
    }
  });

  assert(_ins);
  return *_ins;
}

RayRuntime &RayRuntime::getInstance() {
  if (!_ins) {
    exit(1);
  }
  return *_ins;
}

void RayRuntime::put(std::vector< ::ray::blob> &&data,
                     const UniqueId &objectId, const UniqueId &taskId) {
  _taskProxy->markTaskPutDependency(taskId, objectId);
  _objectProxy->put(objectId, std::forward<std::vector< ::ray::blob> >(data));
}

std::unique_ptr<UniqueId> RayRuntime::put(std::vector< ::ray::blob> &&data) {
  const UniqueId &taskId = _worker->getCurrentTaskId();
  std::unique_ptr<UniqueId> objectId = _worker->getCurrentTaskNextPutId();
  put(std::forward<std::vector< ::ray::blob> >(data), *objectId, taskId);
  return objectId;
}

del_unique_ptr< ::ray::blob> RayRuntime::get(const UniqueId &objectId) {
  bool wasBlocked = false;

  // Do an initial fetch.
  _objectProxy->fetch(objectId);

  // Get the object. We initially try to get the object immediately.
  auto ret = _objectProxy->get(objectId, 0);

  wasBlocked = (!ret);

  // Try reconstructing the object. Try to get it until at least getTimeoutMs
  // milliseconds passes, then repeat.
  while (!ret) {
    _taskProxy->reconstructObject(objectId);

    // Do another fetch
    _objectProxy->fetch(objectId);

    ret = _objectProxy->get(objectId);
  }

  if (wasBlocked) {
    _taskProxy->notifyUnblocked();
  }

  return ret;
}

std::unique_ptr<UniqueId> RayRuntime::call(remote_function_ptr_holder &fptr,
                                           std::vector< ::ray::blob> &&args) {
  InvocationSpec invocationSpec;
  UniqueId uid;
  uid.random();
  invocationSpec.taskId = uid;  // TODO: make it deterministic
  invocationSpec.actorId = nilUniqueId;
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  if (_worker->getCurrentWorkerRunMode() == RunMode::SINGLE_PROCESS) {
    std::unique_ptr<UniqueId> uId = _taskProxy->submit(invocationSpec);
    auto ts = _taskProxy->getTask();
    execute(*ts);
    return uId;
  } else {
    return _taskProxy->submit(invocationSpec);
  }
}

std::unique_ptr<UniqueId> RayRuntime::create(remote_function_ptr_holder &fptr,
                                             std::vector< ::ray::blob> &&args) {
  return _ins->create(fptr, std::move(args));
}

std::unique_ptr<UniqueId> RayRuntime::call(const remote_function_ptr_holder &fptr,
                                           const UniqueId &actor,
                                           std::vector< ::ray::blob> &&args) {
  InvocationSpec invocationSpec;
  UniqueId uid;
  uid.random();
  invocationSpec.taskId = uid;  // TODO: make it deterministic
  invocationSpec.actorId = actor;
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  if (_worker->getCurrentWorkerRunMode() == RunMode::SINGLE_PROCESS) {
    std::unique_ptr<UniqueId> uId = _taskProxy->submit(invocationSpec);
    auto ts = _taskProxy->getTask();
    execute(*ts);
    return uId;
  } else {
    return _taskProxy->submit(invocationSpec);
  }
}

void RayRuntime::execute(const TaskSpec &taskSpec) {
  if (_worker->getCurrentWorkerRunMode() == RunMode::SINGLE_PROCESS) {
    char *actor = NULL;
    if (taskSpec.actorId == nilUniqueId) {
    } else {
      actor = get_actor_ptr(taskSpec.actorId);
    }
    InvocationExecutor::execute(taskSpec, dylib_base_addr, actor);
  } else {
    // TODO:singlebox and cluster mode process
  }
}

char *RayRuntime::get_actor_ptr(const UniqueId &id) { return _ins->get_actor_ptr(id); }

TaskSpec *RayRuntime::getCurrentTask() { return _worker->getCurrentTask(); }

void RayRuntime::setCurrentTask(TaskSpec &task) { _worker->setCurrentTask(task); }

int RayRuntime::getNextPutIndex() { return _worker->getNextPutIndex(); }

const UniqueId &RayRuntime::getCurrentTaskId() { return _worker->getCurrentTaskId(); }

}  // namespace ray