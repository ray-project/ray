
#include "native_ray_runtime.h"

#include <ray/api.h>

#include "../util/address_helper.h"
#include "./object/native_object_store.h"
#include "./object/object_store.h"
#include "./task/native_task_submitter.h"

namespace ray {
namespace api {

NativeRayRuntime::NativeRayRuntime(std::shared_ptr<RayConfig> config) {
  config_ = config;
  object_store_ = std::unique_ptr<ObjectStore>(new NativeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new NativeTaskSubmitter());
  task_executor_ = std::make_unique<TaskExecutor>(*this);
}

}  // namespace api
}  // namespace ray