
#include "task_executor.h"

#include <memory>

#include "../../util/address_helper.h"
#include "../../util/function_helper.h"
#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {

std::shared_ptr<msgpack::sbuffer> TaskExecutor::current_actor_ = nullptr;

TaskExecutor::TaskExecutor(AbstractRayRuntime &abstract_ray_tuntime_)
    : abstract_ray_tuntime_(abstract_ray_tuntime_) {}

// TODO(Guyang Song): Make a common task execution function used for both local mode and
// cluster mode.
std::unique_ptr<ObjectID> TaskExecutor::Execute(InvocationSpec &invocation) {
  abstract_ray_tuntime_.GetWorkerContext();
  return std::unique_ptr<ObjectID>(new ObjectID());
};

Status TaskExecutor::ExecuteTask(
    TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<RayObject>> &args_buffer,
    const std::vector<ObjectID> &arg_reference_ids,
    const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
    std::vector<std::shared_ptr<RayObject>> *results) {
  RAY_LOG(INFO) << "TaskExecutor::ExecuteTask";
  RAY_CHECK(ray_function.GetLanguage() == Language::CPP);
  auto function_descriptor = ray_function.GetFunctionDescriptor();
  RAY_CHECK(function_descriptor->Type() ==
            ray::FunctionDescriptorType::kCppFunctionDescriptor);
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
  std::string lib_name = typed_descriptor->LibName();
  std::string func_offset = typed_descriptor->FunctionOffset();
  std::string exec_func_offset = typed_descriptor->ExecFunctionOffset();
  auto base_addr = FunctionHelper::GetInstance().GetBaseAddress(lib_name);
  std::shared_ptr<msgpack::sbuffer> data = nullptr;
  if (task_type == TaskType::ACTOR_CREATION_TASK) {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset,
        const std::vector<std::shared_ptr<RayObject>> &args_buffer);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_buffer);
    current_actor_ = data;
  } else if (task_type == TaskType::ACTOR_TASK) {
    RAY_CHECK(current_actor_ != nullptr);
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset,
        const std::vector<std::shared_ptr<RayObject>> &args_buffer,
        std::shared_ptr<msgpack::sbuffer> object);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_buffer, current_actor_);
  } else {  // NORMAL_TASK
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset,
        const std::vector<std::shared_ptr<RayObject>> &args_buffer);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_buffer);
  }

  std::vector<size_t> data_sizes;
  std::vector<std::shared_ptr<ray::Buffer>> metadatas;
  std::vector<std::vector<ray::ObjectID>> contained_object_ids;
  if (task_type != TaskType::ACTOR_CREATION_TASK) {
    metadatas.push_back(nullptr);
    data_sizes.push_back(data->size());
    contained_object_ids.push_back(std::vector<ray::ObjectID>());
  }

  RAY_CHECK_OK(ray::CoreWorkerProcess::GetCoreWorker().AllocateReturnObjects(
      return_ids, data_sizes, metadatas, contained_object_ids, results));
  if (task_type != TaskType::ACTOR_CREATION_TASK) {
    auto result = (*results)[0];
    if (result != nullptr) {
      if (result->HasData()) {
        memcpy(result->GetData()->Data(), data->data(), data_sizes[0]);
      }
    }
  }
  return ray::Status::OK();
}

void TaskExecutor::Invoke(
    const TaskSpecification &task_spec, std::shared_ptr<msgpack::sbuffer> actor,
    AbstractRayRuntime *runtime, const uintptr_t base_addr,
    std::unordered_map<ActorID, std::unique_ptr<ActorContext>> &actor_contexts,
    absl::Mutex &actor_contexts_mutex) {
  std::vector<std::shared_ptr<RayObject>> args_buffer;
  for (size_t i = 0; i < task_spec.NumArgs(); i++) {
    std::shared_ptr<::ray::LocalMemoryBuffer> memory_buffer = nullptr;
    if (task_spec.ArgByRef(i)) {
      auto arg = runtime->Get(task_spec.ArgId(i));
      memory_buffer = std::make_shared<::ray::LocalMemoryBuffer>(
          reinterpret_cast<uint8_t *>(arg->data()), arg->size(), true);
    } else {
      memory_buffer = std::make_shared<::ray::LocalMemoryBuffer>(
          const_cast<uint8_t *>(task_spec.ArgData(i)), task_spec.ArgDataSize(i), true);
    }
    args_buffer.emplace_back(
        std::make_shared<RayObject>(memory_buffer, nullptr, std::vector<ObjectID>()));
  }

  auto function_descriptor = task_spec.FunctionDescriptor();
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
  std::shared_ptr<msgpack::sbuffer> data;
  if (actor) {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset,
        std::vector<std::shared_ptr<RayObject>> & args_buffer,
        std::shared_ptr<msgpack::sbuffer> object);
    unsigned long offset = std::stoul(typed_descriptor->ExecFunctionOffset());
    auto address = base_addr + offset;
    ExecFunction exec_function = (ExecFunction)(address);
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_buffer, actor);
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset,
        std::vector<std::shared_ptr<RayObject>> & args_buffer);
    ExecFunction exec_function =
        (ExecFunction)(base_addr + std::stoul(typed_descriptor->ExecFunctionOffset()));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_buffer);
  }
  if (task_spec.IsActorCreationTask()) {
    std::unique_ptr<ActorContext> actorContext(new ActorContext());
    actorContext->current_actor = data;
    absl::MutexLock lock(&actor_contexts_mutex);
    actor_contexts.emplace(task_spec.ActorCreationId(), std::move(actorContext));
  } else {
    runtime->Put(std::move(data), task_spec.ReturnId(0));
  }
}

}  // namespace api
}  // namespace ray
