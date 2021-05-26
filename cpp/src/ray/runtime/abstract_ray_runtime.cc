
#include "abstract_ray_runtime.h"

#include <ray/api.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_exception.h>

#include <cassert>

#include "../util/process_helper.h"
#include "local_mode_ray_runtime.h"
#include "native_ray_runtime.h"

namespace ray {

namespace internal {
msgpack::sbuffer PackError(std::string error_msg) {
  msgpack::sbuffer sbuffer;
  msgpack::packer<msgpack::sbuffer> packer(sbuffer);
  packer.pack(msgpack::type::nil_t());
  packer.pack(std::make_tuple((int)ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION,
                              std::move(error_msg)));

  return sbuffer;
}
}  // namespace internal
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

std::vector<bool> AbstractRayRuntime::Wait(const std::vector<ObjectID> &ids,
                                           int num_objects, int timeout_ms) {
  return object_store_->Wait(ids, num_objects, timeout_ms);
}

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    std::vector<ray::api::TaskArg> &args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto &arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg.buf) {
      auto &buffer = *arg.buf;
      auto memory_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
      ray_arg = absl::make_unique<ray::TaskArgByValue>(std::make_shared<ray::RayObject>(
          memory_buffer, nullptr, std::vector<ObjectID>()));
    } else {
      RAY_CHECK(arg.id);
      ray_arg = absl::make_unique<ray::TaskArgByReference>(*arg.id, ray::rpc::Address{});
    }
    ray_args.push_back(std::move(ray_arg));
  }

  return ray_args;
}

InvocationSpec BuildInvocationSpec1(TaskType task_type, std::string lib_name,
                                    const RemoteFunctionHolder &remote_function_holder,
                                    std::vector<ray::api::TaskArg> &args,
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
                                  std::vector<ray::api::TaskArg> &args) {
  auto invocation_spec =
      BuildInvocationSpec1(TaskType::NORMAL_TASK, this->config_->lib_name,
                           remote_function_holder, args, ActorID::Nil());
  return task_submitter_->SubmitTask(invocation_spec);
}

ActorID AbstractRayRuntime::CreateActor(
    const RemoteFunctionHolder &remote_function_holder,
    std::vector<ray::api::TaskArg> &args) {
  auto invocation_spec =
      BuildInvocationSpec1(TaskType::ACTOR_CREATION_TASK, this->config_->lib_name,
                           remote_function_holder, args, ActorID::Nil());
  return task_submitter_->CreateActor(invocation_spec);
}

ObjectID AbstractRayRuntime::CallActor(const RemoteFunctionHolder &remote_function_holder,
                                       const ActorID &actor,
                                       std::vector<ray::api::TaskArg> &args) {
  auto invocation_spec = BuildInvocationSpec1(
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
