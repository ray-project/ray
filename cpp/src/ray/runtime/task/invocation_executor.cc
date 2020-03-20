#include "invocation_executor.h"
#include "../../address_helper.h"
#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {

void InvocationExecutor::Execute(const TaskSpecification &taskSpec,
                                 std::shared_ptr<msgpack::sbuffer> actor) {
  auto args = std::make_shared<msgpack::sbuffer>(taskSpec.ArgDataSize(0));
  /// TODO(Guyang Song): Avoid the memory copy.
  args->write(reinterpret_cast<const char *>(taskSpec.ArgData(0)),
              taskSpec.ArgDataSize(0));
  auto functionDescriptor = taskSpec.FunctionDescriptor();
  auto typed_descriptor = functionDescriptor->As<ray::CppFunctionDescriptor>();
  if (actor) {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, long func_offset, std::shared_ptr<msgpack::sbuffer> args,
        std::shared_ptr<msgpack::sbuffer> object);
    EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(
        dylib_base_addr + std::stol(typed_descriptor->ExecFunctionOffset()));
    auto data = (*exec_function)(
        dylib_base_addr, std::stol(typed_descriptor->FunctionOffset()), args, actor);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
    rayRuntime.Put(std::move(data), taskSpec.ReturnId(0, ray::TaskTransportType::RAYLET),
                   taskSpec.TaskId());
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, long func_offset, std::shared_ptr<msgpack::sbuffer> args);
    EXEC_FUNCTION exec_function = (EXEC_FUNCTION)(
        dylib_base_addr + std::stol(typed_descriptor->ExecFunctionOffset()));
    auto data = (*exec_function)(dylib_base_addr,
                                 std::stol(typed_descriptor->FunctionOffset()), args);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
    rayRuntime.Put(std::move(data), taskSpec.ReturnId(0, ray::TaskTransportType::RAYLET),
                   taskSpec.TaskId());
  }
}
}  // namespace api
}  // namespace ray
