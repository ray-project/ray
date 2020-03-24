
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

void TaskExecutor::Invoke(const TaskSpecification &taskSpec,
                          std::shared_ptr<msgpack::sbuffer> actor) {
  auto args = std::make_shared<msgpack::sbuffer>(taskSpec.ArgDataSize(0));
  /// TODO(Guyang Song): Avoid the memory copy.
  args->write(reinterpret_cast<const char *>(taskSpec.ArgData(0)),
              taskSpec.ArgDataSize(0));
  auto functionDescriptor = taskSpec.FunctionDescriptor();
  auto typed_descriptor = functionDescriptor->As<ray::CppFunctionDescriptor>();
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
  AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
  rayRuntime.Put(std::move(data), taskSpec.ReturnId(0, ray::TaskTransportType::RAYLET));
}
}  // namespace api
}  // namespace ray