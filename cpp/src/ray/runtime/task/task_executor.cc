
#include "task_executor.h"

#include <ray/api/common_types.h>

#include <memory>

#include "../../util/function_helper.h"
#include "../abstract_ray_runtime.h"

namespace ray {

namespace internal {
/// Execute remote functions by networking stream.
msgpack::sbuffer TaskExecutionHandler(const std::string &func_name,
                                      const std::vector<msgpack::sbuffer> &args_buffer,
                                      msgpack::sbuffer *actor_ptr) {
  if (func_name.empty()) {
    return PackError("Task function name is empty");
  }

  msgpack::sbuffer result;
  do {
    try {
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
    } catch (const std::exception &ex) {
      result = PackError(ex.what());
    }
  } while (0);

  return result;
}

auto &init_func_manager = FunctionManager::Instance();

FunctionManager &GetFunctionManager() { return init_func_manager; }

std::vector<std::string> GetRemoteFunctionNames() {
  return init_func_manager.GetRemoteFunctionNames();
}
}  // namespace internal

namespace api {

std::shared_ptr<msgpack::sbuffer> TaskExecutor::current_actor_ = nullptr;

TaskExecutor::TaskExecutor(AbstractRayRuntime &abstract_ray_tuntime_)
    : abstract_ray_tuntime_(abstract_ray_tuntime_) {}

// TODO(Guyang Song): Make a common task execution function used for both local mode and
// cluster mode.
std::unique_ptr<ObjectID> TaskExecutor::Execute(InvocationSpec &invocation) {
  abstract_ray_tuntime_.GetWorkerContext();
  return std::make_unique<ObjectID>();
};

std::pair<Status, std::shared_ptr<msgpack::sbuffer>> GetExecuteResult(
    const std::string &lib_name, const std::string &func_name,
    const std::vector<msgpack::sbuffer> &args_buffer, msgpack::sbuffer *actor_ptr) {
  auto entry_func = FunctionHelper::GetInstance().GetEntryFunction(lib_name);
  if (entry_func == nullptr) {
    return std::make_pair(ray::Status::NotFound(lib_name + " not found"), nullptr);
  }

  RAY_LOG(DEBUG) << "Get execute function" << func_name << " ok";
  auto result = entry_func(func_name, args_buffer, actor_ptr);
  RAY_LOG(DEBUG) << "Execute function" << func_name << " ok";
  return std::make_pair(ray::Status::OK(),
                        std::make_shared<msgpack::sbuffer>(std::move(result)));
}

Status TaskExecutor::ExecuteTask(
    ray::TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args_buffer,
    const std::vector<ObjectID> &arg_reference_ids,
    const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
    std::vector<std::shared_ptr<ray::RayObject>> *results,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "Execute task: " << TaskType_Name(task_type);
  RAY_CHECK(ray_function.GetLanguage() == ray::Language::CPP);
  auto function_descriptor = ray_function.GetFunctionDescriptor();
  RAY_CHECK(function_descriptor->Type() ==
            ray::FunctionDescriptorType::kCppFunctionDescriptor);
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
  std::string lib_name = typed_descriptor->LibName();
  std::string func_name = typed_descriptor->FunctionName();

  Status status{};
  std::shared_ptr<msgpack::sbuffer> data = nullptr;
  std::vector<msgpack::sbuffer> ray_args_buffer;
  for (auto &arg : args_buffer) {
    msgpack::sbuffer sbuf;
    sbuf.write((const char *)(arg->GetData()->Data()), arg->GetData()->Size());
    ray_args_buffer.push_back(std::move(sbuf));
  }
  if (task_type == ray::TaskType::ACTOR_CREATION_TASK) {
    std::tie(status, data) =
        GetExecuteResult(lib_name, func_name, ray_args_buffer, nullptr);
    current_actor_ = data;
  } else if (task_type == ray::TaskType::ACTOR_TASK) {
    RAY_CHECK(current_actor_ != nullptr);
    std::tie(status, data) =
        GetExecuteResult(lib_name, func_name, ray_args_buffer, current_actor_.get());
  } else {  // NORMAL_TASK
    std::tie(status, data) =
        GetExecuteResult(lib_name, func_name, ray_args_buffer, nullptr);
  }

  if (!status.ok()) {
    return status;
  }

  results->resize(return_ids.size(), nullptr);
  if (task_type != ray::TaskType::ACTOR_CREATION_TASK) {
    size_t data_size = data->size();
    auto &result_id = return_ids[0];
    auto result_ptr = &(*results)[0];
    RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObject(
        result_id, data_size, nullptr, std::vector<ray::ObjectID>(), result_ptr));

    auto result = *result_ptr;
    if (result != nullptr) {
      if (result->HasData()) {
        memcpy(result->GetData()->Data(), data->data(), data_size);
      }
    }

    RAY_CHECK_OK(
        ray::CoreWorkerProcess::GetCoreWorker().SealReturnObject(result_id, result));
  }
  return ray::Status::OK();
}

void TaskExecutor::Invoke(
    const TaskSpecification &task_spec, std::shared_ptr<msgpack::sbuffer> actor,
    AbstractRayRuntime *runtime,
    std::unordered_map<ActorID, std::unique_ptr<ActorContext>> &actor_contexts,
    absl::Mutex &actor_contexts_mutex) {
  std::vector<msgpack::sbuffer> args_buffer;
  for (size_t i = 0; i < task_spec.NumArgs(); i++) {
    if (task_spec.ArgByRef(i)) {
      auto arg = runtime->Get(task_spec.ArgId(i).Binary());
      args_buffer.push_back(std::move(*arg));
    } else {
      msgpack::sbuffer sbuf;
      sbuf.write((const char *)task_spec.ArgData(i), task_spec.ArgDataSize(i));
      args_buffer.push_back(std::move(sbuf));
    }
  }

  auto function_descriptor = task_spec.FunctionDescriptor();
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();

  std::shared_ptr<msgpack::sbuffer> data;
  if (actor) {
    auto result = internal::TaskExecutionHandler(typed_descriptor->FunctionName(),
                                                 args_buffer, actor.get());
    data = std::make_shared<msgpack::sbuffer>(std::move(result));
    runtime->Put(std::move(data), task_spec.ReturnId(0));
  } else {
    auto result = internal::TaskExecutionHandler(typed_descriptor->FunctionName(),
                                                 args_buffer, nullptr);
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
}

}  // namespace api
}  // namespace ray
