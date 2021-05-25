
#include "abstract_ray_runtime.h"

#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_exception.h>

#include <cassert>

#include "../util/process_helper.h"
#include "local_mode_ray_runtime.h"
#include "native_ray_runtime.h"

namespace ray {
namespace api {
std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::abstract_ray_runtime_ = nullptr;

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::DoInit(
    std::shared_ptr<RayConfig> config) {
  std::shared_ptr<AbstractRayRuntime> runtime;
  if (config->run_mode == RunMode::SINGLE_PROCESS) {
    runtime = std::shared_ptr<AbstractRayRuntime>(new LocalModeRayRuntime(config));
  } else {
    ProcessHelper::GetInstance().RayStart(config, TaskExecutor::ExecuteTask);
    runtime = std::shared_ptr<AbstractRayRuntime>(new NativeRayRuntime(config));
  }
  runtime->config_ = config;
  RAY_CHECK(runtime);
  abstract_ray_runtime_ = runtime;
  return runtime;
}

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::GetInstance() {
  return abstract_ray_runtime_;
}

void AbstractRayRuntime::DoShutdown(std::shared_ptr<RayConfig> config) {
  if (config->run_mode == RunMode::CLUSTER) {
    ProcessHelper::GetInstance().RayStop(config);
  }
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             ObjectID *object_id) {
  object_store_->Put(data, object_id);
}

void AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data,
                             const ObjectID &object_id) {
  object_store_->Put(data, object_id);
}

ObjectID AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::FromIndex(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex());
  Put(data, &object_id);
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

std::unique_ptr<::ray::TaskArg> ToTaskArgValue(std::unique_ptr<ray::api::TaskArg> &arg) {
  auto value = arg->GetValue()->GetData();
  auto buf = std::make_shared<::ray::LocalMemoryBuffer>(value->Data(), value->Size());
  auto meta_value = arg->GetValue()->GetMetadata();
  auto meta_buf = meta_value ? std::make_shared<::ray::LocalMemoryBuffer>(
                                   meta_value->Data(), meta_value->Size())
                             : nullptr;
  auto ray_object =
      std::make_shared<::ray::RayObject>(buf, meta_buf, arg->GetValue()->GetNestedIds());
  return absl::make_unique<::ray::TaskArgByValue>(ray_object);
}

std::unique_ptr<::ray::TaskArg> ToTaskArgRef(std::unique_ptr<ray::api::TaskArg> &arg) {
  ray::rpc::Address ray_addr{};
  auto addr = arg->GetAddress();
  ray_addr.set_raylet_id(std::move(addr.raylet_id));
  ray_addr.set_ip_address(std::move(addr.ip_address));
  ray_addr.set_port(addr.port);
  ray_addr.set_worker_id(std::move(addr.worker_id));
  return absl::make_unique<::ray::TaskArgByReference>(arg->GetObjectID(),
                                                      std::move(ray_addr));
}

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    std::vector<std::unique_ptr<ray::api::TaskArg>> &args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto &arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg->GetArgType() == ArgType::ArgByValue) {
      ray_arg = ToTaskArgValue(arg);
    } else {
      ray_arg = ToTaskArgRef(arg);
    }
    ray_args.push_back(std::move(ray_arg));
  }

  return ray_args;
}

InvocationSpec BuildInvocationSpec(TaskType task_type, std::string lib_name,
                                   const RemoteFunctionHolder &remote_function_holder,
                                   std::vector<std::unique_ptr<ray::api::TaskArg>> &args,
                                   const ActorID &actor) {
  InvocationSpec invocation_spec;
  invocation_spec.task_type = task_type;
  invocation_spec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocation_spec.lib_name = lib_name;
  invocation_spec.remote_function_holder = remote_function_holder;
  invocation_spec.actor_id = actor;
  invocation_spec.args = TransformArgs(args);
  return invocation_spec;
}

ObjectID AbstractRayRuntime::Call(const RemoteFunctionHolder &remote_function_holder,
                                  std::vector<std::unique_ptr<ray::api::TaskArg>> &args) {
  auto invocation_spec =
      BuildInvocationSpec(TaskType::NORMAL_TASK, this->config_->lib_name,
                          remote_function_holder, args, ActorID::Nil());
  return task_submitter_->SubmitTask(invocation_spec);
}

ActorID AbstractRayRuntime::CreateActor(
    const RemoteFunctionHolder &remote_function_holder,
    std::vector<std::unique_ptr<ray::api::TaskArg>> &args) {
  auto invocation_spec =
      BuildInvocationSpec(TaskType::ACTOR_CREATION_TASK, this->config_->lib_name,
                          remote_function_holder, args, ActorID::Nil());
  return task_submitter_->CreateActor(invocation_spec);
}

ObjectID AbstractRayRuntime::CallActor(
    const RemoteFunctionHolder &remote_function_holder, const ActorID &actor,
    std::vector<std::unique_ptr<ray::api::TaskArg>> &args) {
  auto invocation_spec = BuildInvocationSpec(
      TaskType::ACTOR_TASK, this->config_->lib_name, remote_function_holder, args, actor);
  return task_submitter_->SubmitActorTask(invocation_spec);
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return worker_->GetCurrentTaskID();
}

const JobID &AbstractRayRuntime::GetCurrentJobID() { return worker_->GetCurrentJobID(); }

const std::unique_ptr<WorkerContext> &AbstractRayRuntime::GetWorkerContext() {
  return worker_;
}

void AddLocalReference(const ObjectID &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.AddLocalReference(id);
  }
}

void RemoveLocalReference(const ObjectID &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.RemoveLocalReference(id);
  }
}

}  // namespace api
}  // namespace ray
