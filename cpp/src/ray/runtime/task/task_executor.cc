
#include <memory>

#include "../../util/address_helper.h"
#include "../abstract_ray_runtime.h"
#include "task_executor.h"

namespace ray {
namespace api {

// TODO(Guyang Song): Make a common task execution function used for both local mode and
// cluster mode.
std::unique_ptr<ObjectID> TaskExecutor::Execute(const InvocationSpec &invocation) {
  return std::unique_ptr<ObjectID>(new ObjectID());
};

void TaskExecutor::Invoke(const TaskSpecification &task_spec,
                          std::shared_ptr<msgpack::sbuffer> actor,
                          AbstractRayRuntime *runtime) {
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
    ExecFunction exec_function = (ExecFunction)(
        dynamic_library_base_addr + std::stoul(typed_descriptor->ExecFunctionOffset()));
    data = (*exec_function)(dynamic_library_base_addr,
                            std::stoul(typed_descriptor->FunctionOffset()), args, actor);
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
        uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    ExecFunction exec_function = (ExecFunction)(
        dynamic_library_base_addr + std::stoul(typed_descriptor->ExecFunctionOffset()));
    data = (*exec_function)(dynamic_library_base_addr,
                            std::stoul(typed_descriptor->FunctionOffset()), args);
  }
  runtime->Put(std::move(data), task_spec.ReturnId(0, ray::TaskTransportType::RAYLET));
}
}  // namespace api
}  // namespace ray