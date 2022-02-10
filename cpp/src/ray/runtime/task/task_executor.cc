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

#include "task_executor.h"

#include <ray/api/common_types.h>

#include <memory>

#include "../../util/function_helper.h"
#include "../abstract_ray_runtime.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"

namespace ray {

namespace internal {
/// Execute remote functions by networking stream.
msgpack::sbuffer TaskExecutionHandler(const std::string &func_name,
                                      const ArgsBufferList &args_buffer,
                                      msgpack::sbuffer *actor_ptr) {
  if (func_name.empty()) {
    throw std::invalid_argument("Task function name is empty");
  }

  msgpack::sbuffer result;
  do {
    if (actor_ptr) {
      auto func_ptr = FunctionManager::Instance().GetMemberFunction(func_name);
      if (func_ptr == nullptr) {
        result = PackError("unknown actor task: " + func_name);
        break;
      }
      result = (*func_ptr)(actor_ptr, args_buffer);
    } else {
      auto func_ptr = FunctionManager::Instance().GetFunction(func_name);
      if (func_ptr == nullptr) {
        result = PackError("unknown function: " + func_name);
        break;
      }
      result = (*func_ptr)(args_buffer);
    }
  } while (0);

  return result;
}

auto &init_func_manager = FunctionManager::Instance();

FunctionManager &GetFunctionManager() { return init_func_manager; }

std::pair<const RemoteFunctionMap_t &, const RemoteMemberFunctionMap_t &>
GetRemoteFunctions() {
  return init_func_manager.GetRemoteFunctions();
}

void InitRayRuntime(std::shared_ptr<RayRuntime> runtime) {
  RayRuntimeHolder::Instance().Init(runtime);
}
}  // namespace internal

namespace internal {

using ray::core::CoreWorkerProcess;

std::shared_ptr<msgpack::sbuffer> TaskExecutor::current_actor_ = nullptr;

// TODO(SongGuyang): Make a common task execution function used for both local mode and
// cluster mode.
std::unique_ptr<ObjectID> TaskExecutor::Execute(InvocationSpec &invocation) {
  return std::make_unique<ObjectID>();
};

/// TODO(qicosmos): Need to add more details of the error messages, such as object id,
/// task id etc.
std::pair<Status, std::shared_ptr<msgpack::sbuffer>> GetExecuteResult(
    const std::string &func_name, const ArgsBufferList &args_buffer,
    msgpack::sbuffer *actor_ptr) {
  try {
    EntryFuntion entry_function;
    if (actor_ptr == nullptr) {
      entry_function = FunctionHelper::GetInstance().GetExecutableFunctions(func_name);
    } else {
      entry_function =
          FunctionHelper::GetInstance().GetExecutableMemberFunctions(func_name);
    }
    RAY_LOG(DEBUG) << "Get executable function " << func_name << " ok.";
    auto result = entry_function(func_name, args_buffer, actor_ptr);
    RAY_LOG(DEBUG) << "Execute function " << func_name << " ok.";
    return std::make_pair(ray::Status::OK(),
                          std::make_shared<msgpack::sbuffer>(std::move(result)));
  } catch (RayIntentionalSystemExitException &e) {
    return std::make_pair(ray::Status::IntentionalSystemExit(), nullptr);
  } catch (RayException &e) {
    return std::make_pair(ray::Status::NotFound(e.what()), nullptr);
  } catch (msgpack::type_error &e) {
    return std::make_pair(
        ray::Status::Invalid(std::string("invalid arguments: ") + e.what()), nullptr);
  } catch (const std::invalid_argument &e) {
    return std::make_pair(
        ray::Status::Invalid(std::string("function execute exception: ") + e.what()),
        nullptr);
  } catch (const std::exception &e) {
    return std::make_pair(
        ray::Status::Invalid(std::string("function execute exception: ") + e.what()),
        nullptr);
  } catch (...) {
    return std::make_pair(ray::Status::UnknownError(std::string("unknown exception")),
                          nullptr);
  }
}

Status TaskExecutor::ExecuteTask(
    ray::TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args_buffer,
    const std::vector<rpc::ObjectReference> &arg_refs,
    const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
    std::vector<std::shared_ptr<ray::RayObject>> *results,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_application_level_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute) {
  RAY_LOG(INFO) << "Execute task: " << TaskType_Name(task_type);
  RAY_CHECK(ray_function.GetLanguage() == ray::Language::CPP);
  auto function_descriptor = ray_function.GetFunctionDescriptor();
  RAY_CHECK(function_descriptor->Type() ==
            ray::FunctionDescriptorType::kCppFunctionDescriptor);
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
  std::string func_name = typed_descriptor->FunctionName();

  Status status{};
  std::shared_ptr<msgpack::sbuffer> data = nullptr;
  ArgsBufferList ray_args_buffer;
  for (size_t i = 0; i < args_buffer.size(); i++) {
    auto &arg = args_buffer.at(i);
    msgpack::sbuffer sbuf;
    sbuf.write((const char *)(arg->GetData()->Data()), arg->GetData()->Size());
    ray_args_buffer.push_back(std::move(sbuf));
  }
  if (task_type == ray::TaskType::ACTOR_CREATION_TASK) {
    std::tie(status, data) = GetExecuteResult(func_name, ray_args_buffer, nullptr);
    current_actor_ = data;
  } else if (task_type == ray::TaskType::ACTOR_TASK) {
    RAY_CHECK(current_actor_ != nullptr);
    std::tie(status, data) =
        GetExecuteResult(func_name, ray_args_buffer, current_actor_.get());
  } else {  // NORMAL_TASK
    std::tie(status, data) = GetExecuteResult(func_name, ray_args_buffer, nullptr);
  }

  std::shared_ptr<ray::LocalMemoryBuffer> meta_buffer = nullptr;
  if (!status.ok()) {
    if (status.IsIntentionalSystemExit()) {
      return status;
    } else {
      RAY_EVENT(ERROR, EL_RAY_CPP_TASK_FAILED)
              .WithField("task_type", TaskType_Name(task_type))
              .WithField("function_name", func_name)
          << "C++ task failed: " << status.ToString();
    }

    std::string meta_str = std::to_string(ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION);
    meta_buffer = std::make_shared<ray::LocalMemoryBuffer>(
        reinterpret_cast<uint8_t *>(&meta_str[0]), meta_str.size(), true);

    msgpack::sbuffer buf;
    std::string msg = status.ToString();
    buf.write(msg.data(), msg.size());
    data = std::make_shared<msgpack::sbuffer>(std::move(buf));
  }

  results->resize(return_ids.size(), nullptr);
  if (task_type != ray::TaskType::ACTOR_CREATION_TASK) {
    size_t data_size = data->size();
    auto &result_id = return_ids[0];
    auto result_ptr = &(*results)[0];
    int64_t task_output_inlined_bytes = 0;
    RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
        result_id, data_size, meta_buffer, std::vector<ray::ObjectID>(),
        &task_output_inlined_bytes, result_ptr));

    auto result = *result_ptr;
    if (result != nullptr) {
      if (result->HasData()) {
        memcpy(result->GetData()->Data(), data->data(), data_size);
      }
    }

    RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().SealReturnObject(result_id, result));
  } else {
    if (!status.ok()) {
      return ray::Status::CreationTaskError();
    }
  }
  return ray::Status::OK();
}

void TaskExecutor::Invoke(
    const TaskSpecification &task_spec, std::shared_ptr<msgpack::sbuffer> actor,
    AbstractRayRuntime *runtime,
    std::unordered_map<ActorID, std::unique_ptr<ActorContext>> &actor_contexts,
    absl::Mutex &actor_contexts_mutex) {
  ArgsBufferList args_buffer;
  for (size_t i = 0; i < task_spec.NumArgs(); i++) {
    if (task_spec.ArgByRef(i)) {
      const auto &id = task_spec.ArgId(i).Binary();
      msgpack::sbuffer sbuf;
      sbuf.write(id.data(), id.size());
      args_buffer.push_back(std::move(sbuf));
    } else {
      msgpack::sbuffer sbuf;
      sbuf.write((const char *)task_spec.ArgData(i), task_spec.ArgDataSize(i));
      args_buffer.push_back(std::move(sbuf));
    }
  }

  auto function_descriptor = task_spec.FunctionDescriptor();
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();

  std::shared_ptr<msgpack::sbuffer> data;
  try {
    if (actor) {
      auto result = TaskExecutionHandler(typed_descriptor->FunctionName(), args_buffer,
                                         actor.get());
      data = std::make_shared<msgpack::sbuffer>(std::move(result));
      runtime->Put(std::move(data), task_spec.ReturnId(0));
    } else {
      auto result =
          TaskExecutionHandler(typed_descriptor->FunctionName(), args_buffer, nullptr);
      data = std::make_shared<msgpack::sbuffer>(std::move(result));
      if (task_spec.IsActorCreationTask()) {
        std::unique_ptr<ActorContext> actorContext(new ActorContext());
        actorContext->current_actor = data;
        absl::MutexLock lock(&actor_contexts_mutex);
        actor_contexts.emplace(task_spec.ActorCreationId(), std::move(actorContext));
      } else {
        runtime->Put(std::move(data), task_spec.ReturnId(0));
      }
    }
  } catch (std::exception &e) {
    auto result = PackError(e.what());
    auto data = std::make_shared<msgpack::sbuffer>(std::move(result));
    runtime->Put(std::move(data), task_spec.ReturnId(0));
  }
}

}  // namespace internal
}  // namespace ray
