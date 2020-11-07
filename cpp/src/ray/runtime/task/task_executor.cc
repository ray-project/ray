
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
std::unique_ptr<ObjectID> TaskExecutor::Execute(const InvocationSpec &invocation) {
  abstract_ray_tuntime_.GetWorkerContext();
  return std::unique_ptr<ObjectID>(new ObjectID());
};

Status TaskExecutor::ExecuteTask(
    TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<RayObject>> &args,
    const std::vector<ObjectID> &arg_reference_ids,
    const std::vector<ObjectID> &return_ids,
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
  std::shared_ptr<msgpack::sbuffer> args_sbuffer;
  if (args.size() > 0) {
    auto args_buffer = args[0]->GetData();
    args_sbuffer = std::make_shared<msgpack::sbuffer>(args_buffer->Size());
    /// TODO(Guyang Song): Avoid the memory copy.
    args_sbuffer->write(reinterpret_cast<const char *>(args_buffer->Data()),
                        args_buffer->Size());
  } else {
    args_sbuffer = std::make_shared<msgpack::sbuffer>();
  }
  auto base_addr = FunctionHelper::GetInstance().GetBaseAddress(lib_name);
  std::shared_ptr<msgpack::sbuffer> data = nullptr;
  if (task_type == TaskType::ACTOR_CREATION_TASK) {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_sbuffer);
    current_actor_ = data;
  } else if (task_type == TaskType::ACTOR_TASK) {
    RAY_CHECK(current_actor_ != nullptr);
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
        std::shared_ptr<msgpack::sbuffer> object);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_sbuffer, current_actor_);
  } else {  // NORMAL_TASK
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    ExecFunction exec_function = (ExecFunction)(base_addr + std::stoul(exec_func_offset));
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args_sbuffer);
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

void TaskExecutor::Invoke(const TaskSpecification &task_spec,
                          std::shared_ptr<msgpack::sbuffer> actor,
                          AbstractRayRuntime *runtime, const uintptr_t base_addr) {
  auto args = std::make_shared<msgpack::sbuffer>(task_spec.ArgDataSize(0));
  /// TODO(Guyang Song): Avoid the memory copy.
  args->write(reinterpret_cast<const char *>(task_spec.ArgData(0)),
              task_spec.ArgDataSize(0));
  auto function_descriptor = task_spec.FunctionDescriptor();
  auto typed_descriptor = function_descriptor->As<ray::CppFunctionDescriptor>();
  std::shared_ptr<msgpack::sbuffer> data;
  if (actor) {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
        std::shared_ptr<msgpack::sbuffer> object);
    unsigned long offset = std::stoul(typed_descriptor->ExecFunctionOffset());
    auto address = base_addr + offset;
    ExecFunction exec_function = (ExecFunction)(address);
    data = (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()),
                            args, actor);
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    ExecFunction exec_function =
        (ExecFunction)(base_addr + std::stoul(typed_descriptor->ExecFunctionOffset()));
    data =
        (*exec_function)(base_addr, std::stoul(typed_descriptor->FunctionOffset()), args);
  }
  runtime->Put(std::move(data), task_spec.ReturnId(0));
}

}  // namespace api
}  // namespace ray
