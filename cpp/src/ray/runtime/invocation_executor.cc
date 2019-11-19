#include "invocation_executor.h"
#include "ray_runtime.h"
#include "../util/blob_util.h"

namespace ray {

void InvocationExecutor::execute(const TaskSpec &taskSpec, uintptr_t dylib_base_addr,
                                 char *actor) {
  if (actor != NULL) {
    ::ray::blob *actor_blob = (::ray::blob *)actor;
    typedef std::vector< ::ray::blob> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, const ::ray::blob &args,
        ::ray::blob &object);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.get_exec_func_offset());
    ::ray::blob args = blob_merge(taskSpec.args);
    auto data =
        (*exec_function)(dylib_base_addr, taskSpec.get_func_offset(), args, *actor_blob);
    RayRuntime &rayRuntime = RayRuntime::getInstance();
    rayRuntime.put(std::move(data), *taskSpec.returnIds.front(), taskSpec.taskId);
  } else {
    typedef std::vector< ::ray::blob> (*EXEC_FUNCTION)(
        uintptr_t base_addr, int32_t func_offset, const ::ray::blob &args);
    EXEC_FUNCTION exec_function =
        (EXEC_FUNCTION)(dylib_base_addr + taskSpec.get_exec_func_offset());
    ::ray::blob args = blob_merge(taskSpec.args);
    auto data = (*exec_function)(dylib_base_addr, taskSpec.get_func_offset(), args);
    RayRuntime &rayRuntime = RayRuntime::getInstance();
    rayRuntime.put(std::move(data), *taskSpec.returnIds.front(), taskSpec.taskId);
  }
}
}