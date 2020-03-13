
#include "abstract_ray_runtime.h"

#include <cassert>

#include <ray/api.h>
#include <ray/api/ray_mode.h>
#include "../agent.h"
#include "ray_dev_runtime.h"
#include "ray_native_runtime.h"
#include "task/invocation_executor.h"

namespace ray { namespace api {

std::unique_ptr<AbstractRayRuntime> AbstractRayRuntime::_ins;
std::once_flag AbstractRayRuntime::isInited;

AbstractRayRuntime &AbstractRayRuntime::init(std::shared_ptr<RayConfig> config) {
  doInit(config);
  Ray::init();
  return *_ins;
}

AbstractRayRuntime &AbstractRayRuntime::doInit(std::shared_ptr<RayConfig> config) {
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

AbstractRayRuntime &AbstractRayRuntime::getInstance() {
  if (!_ins) {
    exit(1);
  }
  return *_ins;
}

void AbstractRayRuntime::put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &objectId, const TaskID &taskId) {
  _objectStore->put(objectId, data);
}

ObjectID AbstractRayRuntime::put(std::shared_ptr<msgpack::sbuffer> data) {
  const TaskID &taskId = _worker->GetCurrentTaskID();
  ObjectID objectId = ObjectID::ForPut(_worker->GetCurrentTaskID(),
                                _worker->GetNextPutIndex(),
                                static_cast<uint8_t>(TaskTransportType::RAYLET));
  put(data, objectId, taskId);
  return objectId;
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::get(const ObjectID &objectId) {
  return _objectStore->get(objectId, -1);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::get(
    const std::vector<ObjectID> &objects) {
  return _objectStore->get(objects, -1);
}

WaitResultInternal AbstractRayRuntime::wait(const std::vector<ObjectID> &objects,
                                            int num_objects, int64_t timeout_ms) {
  return _objectStore->wait(objects, num_objects, timeout_ms);
}

ObjectID AbstractRayRuntime::call(
    remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.taskId = TaskID::ForFakeTask();  // TODO: make it from different task
  invocationSpec.actorId = ActorID::Nil();
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return _taskSubmitter->submitTask(invocationSpec);
}

ActorID AbstractRayRuntime::create(
    remote_function_ptr_holder &fptr, std::shared_ptr<msgpack::sbuffer> args) {
  return _ins->create(fptr, args);
}

ObjectID AbstractRayRuntime::call(
    const remote_function_ptr_holder &fptr, const ActorID &actor,
    std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.taskId = TaskID::ForFakeTask();  // TODO: make it from different task
  invocationSpec.actorId = actor;
  invocationSpec.args = args;
  invocationSpec.func_offset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.exec_func_offset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return _taskSubmitter->submitActorTask(invocationSpec);
}

const TaskID &AbstractRayRuntime::getCurrentTaskId() {
  return _worker->GetCurrentTaskID();
}

ActorID AbstractRayRuntime::getNextActorID() {
  const int next_task_index = _worker->GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(_worker->GetCurrentJobID(), _worker->GetCurrentTaskID(),
      next_task_index);
  return actor_id;
}

}  }// namespace ray::api