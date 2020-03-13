#include "invocation_executor.h"
#include "../../agent.h"
#include "../abstract_ray_runtime.h"

namespace ray { namespace api {

void InvocationExecutor::Execute(const TaskSpec &taskSpec,
                                 std::shared_ptr<msgpack::sbuffer> actor) {
  if (actor) {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
        std::shared_ptr<msgpack::sbuffer> object);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.GetexecFuncOffset());
    auto data = (*exec_function)(dylib_base_addr, taskSpec.GetFuncOffset(),
                                 taskSpec.args, actor);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
    rayRuntime.Put(std::move(data), taskSpec.returnId, taskSpec.taskId);
  } else {
    typedef std::shared_ptr<msgpack::sbuffer> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, std::shared_ptr<msgpack::sbuffer> args);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.GetexecFuncOffset());
    auto data =
        (*exec_function)(dylib_base_addr, taskSpec.GetFuncOffset(), taskSpec.args);
    AbstractRayRuntime &rayRuntime = AbstractRayRuntime::GetInstance();
    rayRuntime.Put(std::move(data), taskSpec.returnId, taskSpec.taskId);
  }
}
}  }// namespace ray::api