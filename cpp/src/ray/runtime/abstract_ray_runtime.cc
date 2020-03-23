
#include "abstract_ray_runtime.h"

#include <cassert>

#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_exception.h>
#include "../util/address_helper.h"
#include "ray_dev_runtime.h"

namespace ray {
namespace api {

std::unique_ptr<AbstractRayRuntime> AbstractRayRuntime::ins_;
std::once_flag AbstractRayRuntime::isInited_;

AbstractRayRuntime &AbstractRayRuntime::DoInit(std::shared_ptr<RayConfig> config) {
  std::call_once(isInited_, [config] {
    if (config->runMode == RunMode::SINGLE_PROCESS) {
      ins_.reset(new RayDevRuntime(config));
      GenerateBaseAddressOfCurrentLibrary();
    } else {
      throw RayException("Only single process mode supported now");
    }
  });

  assert(ins_);
  return *ins_;
}

AbstractRayRuntime &AbstractRayRuntime::GetInstance() {
  RAY_CHECK(ins_ != nullptr);
  return *ins_;
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &object_id) {
  objectStore_->Put(object_id, data);
}

ObjectID AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::ForPut(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex(),
                       static_cast<uint8_t>(TaskTransportType::RAYLET));
  Put(data, object_id);
  return object_id;
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::Get(const ObjectID &object_id) {
  return objectStore_->Get(object_id, -1);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::Get(
    const std::vector<ObjectID> &objects) {
  return objectStore_->Get(objects, -1);
}

WaitResult AbstractRayRuntime::Wait(const std::vector<ObjectID> &objects, int num_objects,
                                    int64_t timeout_ms) {
  return objectStore_->Wait(objects, num_objects, timeout_ms);
}

ObjectID AbstractRayRuntime::Call(RemoteFunctionPtrHolder &fptr,
                                  std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actor_id = ActorID::Nil();
  invocationSpec.args = args;
  invocationSpec.func_offset = (size_t)(fptr.value[0] - dynamic_library_base_addr);
  invocationSpec.exec_func_offset = (size_t)(fptr.value[1] - dynamic_library_base_addr);
  return taskSubmitter_->SubmitTask(invocationSpec);
}

ActorID AbstractRayRuntime::CreateActor(RemoteFunctionPtrHolder &fptr,
                                        std::shared_ptr<msgpack::sbuffer> args) {
  return taskSubmitter_->CreateActor(fptr, args);
}

ObjectID AbstractRayRuntime::CallActor(const RemoteFunctionPtrHolder &fptr,
                                       const ActorID &actor,
                                       std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actor_id = actor;
  invocationSpec.args = args;
  invocationSpec.func_offset = (size_t)(fptr.value[0] - dynamic_library_base_addr);
  invocationSpec.exec_func_offset = (size_t)(fptr.value[1] - dynamic_library_base_addr);
  return taskSubmitter_->SubmitActorTask(invocationSpec);
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return worker_->GetCurrentTaskID();
}

const JobID &AbstractRayRuntime::GetCurrentJobID() { return worker_->GetCurrentJobID(); }

ActorID AbstractRayRuntime::GetNextActorID() {
  const int next_task_index = worker_->GetNextTaskIndex();
  const ActorID actor_id = ActorID::Of(worker_->GetCurrentJobID(),
                                       worker_->GetCurrentTaskID(), next_task_index);
  return actor_id;
}

}  // namespace api
}  // namespace ray
