
#include "abstract_ray_runtime.h"

#include <cassert>

#include <ray/api.h>
#include <ray/api/ray_mode.h>
#include "../address_helper.h"
#include "ray_dev_runtime.h"
#include "ray_native_runtime.h"
#include "task/invocation_executor.h"

namespace ray {
namespace api {

std::unique_ptr<AbstractRayRuntime> AbstractRayRuntime::ins_;
std::once_flag AbstractRayRuntime::isInited_;

AbstractRayRuntime &AbstractRayRuntime::Init(std::shared_ptr<RayConfig> config) {
  DoInit(config);
  Ray::Init();
  return *ins_;
}

AbstractRayRuntime &AbstractRayRuntime::DoInit(std::shared_ptr<RayConfig> config) {
  std::call_once(isInited_, [config] {
    if (config->runMode == RunMode::SINGLE_PROCESS) {
      ins_.reset(new RayDevRuntime(config));
      AddressHelperInit();
    } else {
      ins_.reset(new RayNativeRuntime(config));
    }
  });

  assert(ins_);
  return *ins_;
}

AbstractRayRuntime &AbstractRayRuntime::GetInstance() {
  if (!ins_) {
    exit(1);
  }
  return *ins_;
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &objectId, const TaskID &taskId) {
  objectStore_->Put(objectId, data);
}

ObjectID AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  const TaskID &taskId = worker_->GetCurrentTaskID();
  ObjectID objectId =
      ObjectID::ForPut(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex(),
                       static_cast<uint8_t>(TaskTransportType::RAYLET));
  Put(data, objectId, taskId);
  return objectId;
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::Get(const ObjectID &objectId) {
  return objectStore_->Get(objectId, -1);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::Get(
    const std::vector<ObjectID> &objects) {
  return objectStore_->Get(objects, -1);
}

WaitResult AbstractRayRuntime::Wait(const std::vector<ObjectID> &objects, int num_objects,
                                    int64_t timeout_ms) {
  return objectStore_->Wait(objects, num_objects, timeout_ms);
}

ObjectID AbstractRayRuntime::Call(remote_function_ptr_holder &fptr,
                                  std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.taskId =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actorId = ActorID::Nil();
  invocationSpec.args = args;
  invocationSpec.funcOffset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.execFuncOffset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return taskSubmitter_->SubmitTask(invocationSpec);
}

ActorID AbstractRayRuntime::CreateActor(remote_function_ptr_holder &fptr,
                                        std::shared_ptr<msgpack::sbuffer> args) {
  return ins_->CreateActor(fptr, args);
}

ObjectID AbstractRayRuntime::CallActor(const remote_function_ptr_holder &fptr,
                                       const ActorID &actor,
                                       std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.taskId =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actorId = actor;
  invocationSpec.args = args;
  invocationSpec.funcOffset = (int32_t)(fptr.value[0] - dylib_base_addr);
  invocationSpec.execFuncOffset = (int32_t)(fptr.value[1] - dylib_base_addr);
  return taskSubmitter_->SubmitActorTask(invocationSpec);
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return worker_->GetCurrentTaskID();
}

const JobID &AbstractRayRuntime::GetCurrentJobId() { return worker_->GetCurrentJobID(); }

ActorID AbstractRayRuntime::GetNextActorID() {
  const int next_task_index = worker_->GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_->GetCurrentJobID(),
                                       worker_->GetCurrentTaskID(), next_task_index);
  return actor_id;
}

}  // namespace api
}  // namespace ray