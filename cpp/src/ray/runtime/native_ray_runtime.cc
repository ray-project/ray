
#include "native_ray_runtime.h"

#include <ray/api.h>

#include "./object/native_object_store.h"
#include "./object/object_store.h"
#include "./task/native_task_submitter.h"

namespace ray {
namespace api {

NativeRayRuntime::NativeRayRuntime() {
  object_store_ = std::unique_ptr<ObjectStore>(new NativeObjectStore());
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new NativeTaskSubmitter());
  task_executor_ = std::make_unique<TaskExecutor>(*this);
}

}  // namespace api
}  // namespace ray