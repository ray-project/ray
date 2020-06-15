
#include "abstract_ray_runtime.h"

#include <cassert>

#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_exception.h>
#include "../util/address_helper.h"
#include "../util/process_helper.h"
#include "local_mode_ray_runtime.h"
#include "native_ray_runtime.h"

namespace ray {
namespace api {
AbstractRayRuntime *AbstractRayRuntime::DoInit(std::shared_ptr<RayConfig> config) {
  AbstractRayRuntime *runtime;
  if (config->run_mode == RunMode::SINGLE_PROCESS) {
    GenerateBaseAddressOfCurrentLibrary();
    runtime = new LocalModeRayRuntime(config);
  } else {
    ProcessHelper::RayStart();
    runtime = new NativeRayRuntime(config);
  }
  RAY_CHECK(runtime);
  return runtime;
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &object_id) {
  object_store_->Put(object_id, data);
}

ObjectID AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::ForPut(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex());
  Put(data, object_id);
  return object_id;
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::Get(const ObjectID &object_id) {
  return object_store_->Get(object_id, -1);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::Get(
    const std::vector<ObjectID> &ids) {
  return object_store_->Get(ids, -1);
}

WaitResult AbstractRayRuntime::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                    int timeout_ms) {
  return object_store_->Wait(ids, num_objects, timeout_ms);
}

ObjectID AbstractRayRuntime::Call(RemoteFunctionPtrHolder &fptr,
                                  std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actor_id = ActorID::Nil();
  invocationSpec.args = args;
  invocationSpec.func_offset =
      (size_t)(fptr.function_pointer - dynamic_library_base_addr);
  invocationSpec.exec_func_offset =
      (size_t)(fptr.exec_function_pointer - dynamic_library_base_addr);
  return task_submitter_->SubmitTask(invocationSpec);
}

ActorID AbstractRayRuntime::CreateActor(RemoteFunctionPtrHolder &fptr,
                                        std::shared_ptr<msgpack::sbuffer> args) {
  return task_submitter_->CreateActor(fptr, args);
}

ObjectID AbstractRayRuntime::CallActor(const RemoteFunctionPtrHolder &fptr,
                                       const ActorID &actor,
                                       std::shared_ptr<msgpack::sbuffer> args) {
  InvocationSpec invocationSpec;
  invocationSpec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocationSpec.actor_id = actor;
  invocationSpec.args = args;
  invocationSpec.func_offset =
      (size_t)(fptr.function_pointer - dynamic_library_base_addr);
  invocationSpec.exec_func_offset =
      (size_t)(fptr.exec_function_pointer - dynamic_library_base_addr);
  return task_submitter_->SubmitActorTask(invocationSpec);
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

const std::unique_ptr<WorkerContext> &AbstractRayRuntime::GetWorkerContext() {
  return worker_;
}

}  // namespace api
}  // namespace ray
