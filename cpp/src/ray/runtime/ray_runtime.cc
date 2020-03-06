
#include "ray_runtime.h"

#include <cassert>

#include <ray/api.h>
#include <ray/api/ray_mode.h>
#include "../agent.h"
#include "ray_dev_runtime.h"
#include "ray_native_runtime.h"
#include "task/invocation_executor.h"

namespace ray {

std::unique_ptr<RayRuntime> RayRuntime::_ins;
std::once_flag RayRuntime::isInited;

RayRuntime &RayRuntime::init(std::shared_ptr<RayConfig> config) {
  doInit(config);
  Ray::init();
  return *_ins;
}

RayRuntime &RayRuntime::doInit(std::shared_ptr<RayConfig> config) {
  std::call_once(isInited, [config] {
    if (config->runMode == RunMode::SINGLE_PROCESS) {
      _ins.reset(new RayDevRuntime(config));
      Ray_agent_init();
    } else {
      _ins.reset(new RayNativeRuntime(config));
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

void RayRuntime::put(std::shared_ptr<msgpack::sbuffer> data, const UniqueId &objectId,
                     const UniqueId &taskId) {
  _objectStore->put(objectId, data);
}

UniqueId RayRuntime::put(std::shared_ptr<msgpack::sbuffer> data) {
  const UniqueId &taskId = _worker->getCurrentTaskId();
  std::unique_ptr<UniqueId> objectId = _worker->getCurrentTaskNextPutId();
  put(data, *objectId, taskId);
  return *objectId;
}

std::shared_ptr<msgpack::sbuffer> RayRuntime::get(const UniqueId &objectId) {
  return _objectStore->get(objectId, 0);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> RayRuntime::get(const std::vector<UniqueId> &objects) {
  return _objectStore->get(objects, 0);
}

WaitResultInternal RayRuntime::wait(const std::vector<UniqueId> &objects, int num_objects, int64_t timeout_ms) {
  return _objectStore->wait(objects, num_objects, timeout_ms);
}

std::unique_ptr<UniqueId> RayRuntime::call(remote_function_ptr_holder &fptr,
                                           std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  UniqueId uid;
  uid.random();
  invocationSpec.taskId = uid;  // TODO: make it deterministic
  invocationSpec.actorId = nilUniqueId;
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return _taskSubmitter->submitTask(invocationSpec);
}

std::unique_ptr<UniqueId> RayRuntime::create(remote_function_ptr_holder &fptr,
                                            std::shared_ptr<msgpack::sbuffer> args) {
  return _ins->create(fptr, args);
}

std::unique_ptr<UniqueId> RayRuntime::call(const remote_function_ptr_holder &fptr,
                                           const UniqueId &actor,
                                           std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  UniqueId uid;
  uid.random();
  invocationSpec.taskId = uid;  // TODO: make it deterministic
  invocationSpec.actorId = actor;
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return _taskSubmitter->submitActorTask(invocationSpec);
}

char *RayRuntime::get_actor_ptr(const UniqueId &id) { return _ins->get_actor_ptr(id); }

TaskSpec *RayRuntime::getCurrentTask() { return _worker->getCurrentTask(); }

void RayRuntime::setCurrentTask(TaskSpec &task) { _worker->setCurrentTask(task); }

int RayRuntime::getNextPutIndex() { return _worker->getNextPutIndex(); }

const UniqueId &RayRuntime::getCurrentTaskId() { return _worker->getCurrentTaskId(); }

}  // namespace ray