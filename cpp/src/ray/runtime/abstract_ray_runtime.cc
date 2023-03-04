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
  }
  RAY_CHECK(runtime);
  internal::RayRuntimeHolder::Instance().Init(runtime);
  if (ConfigInternal::Instance().worker_type == WorkerType::WORKER) {
    // Load functions from code search path.
    FunctionHelper::GetInstance().LoadFunctionsFromPaths(
        ConfigInternal::Instance().code_search_path);
  }
  abstract_ray_runtime_ = runtime;
  return runtime;
}

std::shared_ptr<AbstractRayRuntime> AbstractRayRuntime::GetInstance() {
  return abstract_ray_runtime_;
}

void AbstractRayRuntime::DoShutdown() {
  abstract_ray_runtime_ = nullptr;
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
  ObjectID object_id;
  object_store_->Put(data, &object_id);
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
                                           int num_objects,
                                           int timeout_ms) {
  return object_store_->Wait(StringIDsToObjectIDs(ids), num_objects, timeout_ms);
}

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    std::vector<ray::internal::TaskArg> &args, bool cross_lang) {
  std::vector<std::unique_ptr<::ray::TaskArg>> ray_args;
  for (auto &arg : args) {
    std::unique_ptr<::ray::TaskArg> ray_arg = nullptr;
    if (arg.buf) {
      auto &buffer = *arg.buf;
      auto memory_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(buffer.data()), buffer.size(), true);
      std::shared_ptr<Buffer> metadata = nullptr;
      if (cross_lang) {
        auto meta_str = arg.meta_str;
        metadata = std::make_shared<ray::LocalMemoryBuffer>(
            reinterpret_cast<uint8_t *>(const_cast<char *>(meta_str.data())),
            meta_str.size(),
            true);
      }
      ray_arg = absl::make_unique<ray::TaskArgByValue>(std::make_shared<ray::RayObject>(
          memory_buffer, metadata, std::vector<rpc::ObjectReference>()));
    } else {
      RAY_CHECK(arg.id);
      auto id = ObjectID::FromBinary(*arg.id);
      auto owner_address = ray::rpc::Address{};
      if (ConfigInternal::Instance().run_mode == RunMode::CLUSTER) {
        auto &core_worker = CoreWorkerProcess::GetCoreWorker();
        owner_address = core_worker.GetOwnerAddressOrDie(id);
      }
      ray_arg = absl::make_unique<ray::TaskArgByReference>(id,
                                                           owner_address,
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
  invocation_spec.remote_function_holder = remote_function_holder;
  invocation_spec.actor_id = actor;
  invocation_spec.args =
      TransformArgs(args, remote_function_holder.lang_type != LangType::CPP);
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
    const RemoteFunctionHolder &remote_function_holder,
    const std::string &actor,
    std::vector<ray::internal::TaskArg> &args,
    const CallOptions &call_options) {
  InvocationSpec invocation_spec{};
  if (remote_function_holder.lang_type == LangType::PYTHON) {
    const auto native_actor_handle = CoreWorkerProcess::GetCoreWorker().GetActorHandle(
        ray::ActorID::FromBinary(actor));
    auto function_descriptor = native_actor_handle->ActorCreationTaskFunctionDescriptor();
    auto typed_descriptor = function_descriptor->As<PythonFunctionDescriptor>();
    RemoteFunctionHolder func_holder = remote_function_holder;
    func_holder.module_name = typed_descriptor->ModuleName();
    func_holder.class_name = typed_descriptor->ClassName();
    invocation_spec = BuildInvocationSpec1(
        TaskType::ACTOR_TASK, func_holder, args, ActorID::FromBinary(actor));
  } else if (remote_function_holder.lang_type == LangType::JAVA) {
    const auto native_actor_handle = CoreWorkerProcess::GetCoreWorker().GetActorHandle(
        ray::ActorID::FromBinary(actor));
    auto function_descriptor = native_actor_handle->ActorCreationTaskFunctionDescriptor();
    auto typed_descriptor = function_descriptor->As<JavaFunctionDescriptor>();
    RemoteFunctionHolder func_holder = remote_function_holder;
    func_holder.class_name = typed_descriptor->ClassName();
    invocation_spec = BuildInvocationSpec1(
        TaskType::ACTOR_TASK, func_holder, args, ActorID::FromBinary(actor));
  } else {
    invocation_spec = BuildInvocationSpec1(
        TaskType::ACTOR_TASK, remote_function_holder, args, ActorID::FromBinary(actor));
  }

  return task_submitter_->SubmitActorTask(invocation_spec, call_options).Binary();
}

const TaskID &AbstractRayRuntime::GetCurrentTaskId() {
  return GetWorkerContext().GetCurrentTaskID();
}

JobID AbstractRayRuntime::GetCurrentJobID() {
  return GetWorkerContext().GetCurrentJobID();
}

const ActorID &AbstractRayRuntime::GetCurrentActorID() {
  return GetWorkerContext().GetCurrentActorID();
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

std::string AbstractRayRuntime::GetActorId(const std::string &actor_name,
                                           const std::string &ray_namespace) {
  auto actor_id = task_submitter_->GetActor(actor_name, ray_namespace);
  if (actor_id.IsNil()) {
    return "";
  }

  return actor_id.Binary();
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

const std::unique_ptr<ray::gcs::GlobalStateAccessor>
    &AbstractRayRuntime::GetGlobalStateAccessor() {
  return global_state_accessor_;
}

bool AbstractRayRuntime::WasCurrentActorRestarted() {
  if (ConfigInternal::Instance().run_mode == RunMode::SINGLE_PROCESS) {
    return false;
  }

  const auto &actor_id = GetCurrentActorID();
  auto byte_ptr = global_state_accessor_->GetActorInfo(actor_id);
  if (byte_ptr == nullptr) {
    return false;
  }

  rpc::ActorTableData actor_table_data;
  bool r = actor_table_data.ParseFromString(*byte_ptr);
  if (!r) {
    throw RayException("Received invalid protobuf data from GCS.");
  }

  return actor_table_data.num_restarts() != 0;
}

ray::PlacementGroup AbstractRayRuntime::CreatePlacementGroup(
    const ray::PlacementGroupCreationOptions &create_options) {
  return task_submitter_->CreatePlacementGroup(create_options);
}

void AbstractRayRuntime::RemovePlacementGroup(const std::string &group_id) {
  return task_submitter_->RemovePlacementGroup(group_id);
}

bool AbstractRayRuntime::WaitPlacementGroupReady(const std::string &group_id,
                                                 int64_t timeout_seconds) {
  return task_submitter_->WaitPlacementGroupReady(group_id, timeout_seconds);
}

PlacementGroup AbstractRayRuntime::GeneratePlacementGroup(const std::string &str) {
  rpc::PlacementGroupTableData pg_table_data;
  bool r = pg_table_data.ParseFromString(str);
  if (!r) {
    throw RayException("Received invalid protobuf data from GCS.");
  }

  PlacementGroupCreationOptions options;
  options.name = pg_table_data.name();
  auto &bundles = options.bundles;
  for (auto &bundle : bundles) {
    options.bundles.emplace_back(bundle);
  }
  options.strategy = PlacementStrategy(pg_table_data.strategy());
  PlacementGroup group(pg_table_data.placement_group_id(),
                       std::move(options),
                       PlacementGroupState(pg_table_data.state()));
  return group;
}

std::vector<PlacementGroup> AbstractRayRuntime::GetAllPlacementGroups() {
  std::vector<std::string> list = global_state_accessor_->GetAllPlacementGroupInfo();
  std::vector<PlacementGroup> groups;
  for (auto &str : list) {
    PlacementGroup group = GeneratePlacementGroup(str);
    groups.push_back(std::move(group));
  }

  return groups;
}

PlacementGroup AbstractRayRuntime::GetPlacementGroupById(const std::string &id) {
  PlacementGroupID pg_id = PlacementGroupID::FromBinary(id);
  auto str_ptr = global_state_accessor_->GetPlacementGroupInfo(pg_id);
  if (str_ptr == nullptr) {
    return {};
  }
  PlacementGroup group = GeneratePlacementGroup(*str_ptr);
  return group;
}

PlacementGroup AbstractRayRuntime::GetPlacementGroup(const std::string &name) {
  // TODO(WangTaoTheTonic): Add namespace support for placement group.
  auto str_ptr = global_state_accessor_->GetPlacementGroupByName(
      name, CoreWorkerProcess::GetCoreWorker().GetJobConfig().ray_namespace());
  if (str_ptr == nullptr) {
    return {};
  }
  PlacementGroup group = GeneratePlacementGroup(*str_ptr);
  return group;
}

std::string AbstractRayRuntime::GetNamespace() {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  return core_worker.GetJobConfig().ray_namespace();
}

std::string AbstractRayRuntime::SerializeActorHandle(const std::string &actor_id) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  std::string output;
  ObjectID actor_handle_id;
  auto status = core_worker.SerializeActorHandle(
      ActorID::FromBinary(actor_id), &output, &actor_handle_id);
  return output;
}

std::string AbstractRayRuntime::DeserializeAndRegisterActorHandle(
    const std::string &serialized_actor_handle) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  return core_worker
      .DeserializeAndRegisterActorHandle(serialized_actor_handle, ObjectID::Nil())
      .Binary();
}

}  // namespace internal
}  // namespace ray
