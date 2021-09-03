// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "abstract_ray_runtime.h"

#include <ray/api.h>
#include <ray/api/ray_exception.h>
#include <ray/util/logging.h>

#include <cassert>

#include "../config_internal.h"
#include "../util/function_helper.h"
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
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::WorkerType;

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::abstract_ray_runtime_ = nullptr;

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::DoInit() {
  std::shared_ptr<AbstractRayRuntime> runtime;
  if (ConfigInternal::Instance().run_mode == RunMode::SINGLE_PROCESS) {
    runtime = std::shared_ptr<AbstractRayRuntime>(new LocalModeRayRuntime());
  } else {
    ProcessHelper::GetInstance().RayStart(TaskExecutor::ExecuteTask);
    runtime = std::shared_ptr<AbstractRayRuntime>(new NativeRayRuntime());
    RAY_LOG(INFO) << "Native ray runtime started.";
    if (ConfigInternal::Instance().worker_type == WorkerType::WORKER) {
      // Load functions from code search path.
      FunctionHelper::GetInstance().LoadFunctionsFromPaths(
          ConfigInternal::Instance().code_search_path);
    }
  }
  RAY_CHECK(runtime);
  abstract_ray_runtime_ = runtime;
  return runtime;
}

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::GetInstance() {
  return abstract_ray_runtime_;
}

void AbstractRayRuntime::DoShutdown() {
  if (ConfigInternal::Instance().run_mode == RunMode::CLUSTER) {
    ProcessHelper::GetInstance().RayStop();
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

std::string AbstractRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::FromIndex(worker_->GetCurrentTaskID(), worker_->GetNextPutIndex());
  Put(data, &object_id);
  return object_id.Binary();
}

std::shared_ptr<msgpack::sbuffer> AbstractRayRuntime::Get(const std::string &object_id) {
  return object_store_->Get(ObjectID::FromBinary(object_id), -1);
}

inline static std::vector<ObjectID> StringIDsToObjectIDs(
    const std::vector<std::string> &ids) {
  std::vector<ObjectID> object_ids;
  for (std::string id : ids) {
    object_ids.push_back(ObjectID::FromBinary(id));
  }
  return object_ids;
}

std::vector<std::shared_ptr<msgpack::sbuffer>> AbstractRayRuntime::Get(
    const std::vector<std::string> &ids) {
  return object_store_->Get(StringIDsToObjectIDs(ids), -1);
}

std::vector<bool> AbstractRayRuntime::Wait(const std::vector<std::string> &ids,
                                           int num_objects, int timeout_ms) {
  return object_store_->Wait(StringIDsToObjectIDs(ids), num_objects, timeout_ms);
}

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    std::vector<ray::internal::TaskArg> &args) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto &arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg.buf) {
      auto &buffer = *arg.buf;
      auto memory_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
      ray_arg = absl::make_unique<ray::TaskArgByValue>(std::make_shared<ray::RayObject>(
          memory_buffer, nullptr, std::vector<rpc::ObjectReference>()));
    } else {
      RAY_CHECK(arg.id);
      ray_arg = absl::make_unique<ray::TaskArgByReference>(ObjectID::FromBinary(*arg.id),
                                                           ray::rpc::Address{},
                                                           /*call_site=*/"");
    }
    ray_args.push_back(std::move(ray_arg));
  }

  return ray_args;
}

InvocationSpec BuildInvocationSpec1(TaskType task_type,
                                    const RemoteFunctionHolder &remote_function_holder,
                                    std::vector<ray::internal::TaskArg> &args,
                                    const ActorID &actor) {
  InvocationSpec invocation_spec;
  invocation_spec.task_type = task_type;
  invocation_spec.task_id =
      TaskID::ForFakeTask();  // TODO(Guyang Song): make it from different task
  invocation_spec.remote_function_holder = remote_function_holder;
  invocation_spec.actor_id = actor;
  invocation_spec.args = TransformArgs(args);
  return invocation_spec;
}

std::string AbstractRayRuntime::Call(const RemoteFunctionHolder &remote_function_holder,
                                     std::vector<ray::internal::TaskArg> &args,
                                     const CallOptions &task_options) {
  auto invocation_spec = BuildInvocationSpec1(
      TaskType::NORMAL_TASK, remote_function_holder, args, ActorID::Nil());
  return task_submitter_->SubmitTask(invocation_spec, task_options).Binary();
}

std::string AbstractRayRuntime::CreateActor(
    const RemoteFunctionHolder &remote_function_holder,
    std::vector<ray::internal::TaskArg> &args,
    const ActorCreationOptions &create_options) {
  auto invocation_spec = BuildInvocationSpec1(
      TaskType::ACTOR_CREATION_TASK, remote_function_holder, args, ActorID::Nil());
  return task_submitter_->CreateActor(invocation_spec, create_options).Binary();
}

std::string AbstractRayRuntime::CallActor(
    const RemoteFunctionHolder &remote_function_holder, const std::string &actor,
    std::vector<ray::internal::TaskArg> &args, const CallOptions &call_options) {
  auto invocation_spec = BuildInvocationSpec1(
      TaskType::ACTOR_TASK, remote_function_holder, args, ActorID::FromBinary(actor));
  return task_submitter_->SubmitActorTask(invocation_spec, call_options).Binary();
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return worker_->GetCurrentTaskID();
}

const JobID &AbstractRayRuntime::GetCurrentJobID() { return worker_->GetCurrentJobID(); }

const std::unique_ptr<WorkerContext> &AbstractRayRuntime::GetWorkerContext() {
  return worker_;
}

void AbstractRayRuntime::AddLocalReference(const std::string &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.AddLocalReference(ObjectID::FromBinary(id));
  }
}

void AbstractRayRuntime::RemoveLocalReference(const std::string &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.RemoveLocalReference(ObjectID::FromBinary(id));
  }
}

std::string GetFullName(bool global, const std::string &name) {
  if (name.empty()) {
    return "";
  }
  return global ? name
                : CoreWorkerProcess::GetCoreWorker().GetCurrentJobId().Hex() + "-" + name;
}

/// TODO(qicosmos): Now only support global name, will support the name of a current job.
std::string AbstractRayRuntime::GetActorId(bool global, const std::string &actor_name) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  auto full_actor_name = GetFullName(global, actor_name);
  auto pair = core_worker.GetNamedActorHandle(actor_name, "");
  if (!pair.second.ok()) {
    RAY_LOG(WARNING) << pair.second.message();
    return "";
  }

  std::string actor_id;
  auto actor_handle = pair.first;
  RAY_CHECK(actor_handle);
  return actor_handle->GetActorID().Binary();
}

void AbstractRayRuntime::KillActor(const std::string &str_actor_id, bool no_restart) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  ray::ActorID actor_id = ray::ActorID::FromBinary(str_actor_id);
  Status status = core_worker.KillActor(actor_id, true, no_restart);
  if (!status.ok()) {
    throw RayException(status.message());
  }
}

void AbstractRayRuntime::ExitActor() {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  if (ConfigInternal::Instance().worker_type != WorkerType::WORKER ||
      core_worker.GetActorId().IsNil()) {
    throw std::logic_error("This shouldn't be called on a non-actor worker.");
  }
  throw RayIntentionalSystemExitException("SystemExit");
}

ray::PlacementGroup AbstractRayRuntime::CreatePlacementGroup(
    const ray::internal::PlacementGroupCreationOptions &create_options) {
  return task_submitter_->CreatePlacementGroup(create_options);
}

void AbstractRayRuntime::RemovePlacementGroup(const std::string &group_id) {
  return task_submitter_->RemovePlacementGroup(group_id);
}

bool AbstractRayRuntime::WaitPlacementGroupReady(const std::string &group_id,
                                                 int timeout_seconds) {
  return task_submitter_->WaitPlacementGroupReady(group_id, timeout_seconds);
}

}  // namespace internal
}  // namespace ray
