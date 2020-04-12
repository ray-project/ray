
#include "local_mode_ray_runtime.h"

#include <ray/api.h>
#include "../util/address_helper.h"
#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {
namespace api {

LocalModeRayRuntime::LocalModeRayRuntime(std::shared_ptr<RayConfig> config) {
  config_ = config;
  worker_ = std::unique_ptr<WorkerContext>(new WorkerContext(
      WorkerType::DRIVER, ComputeDriverIdFromJob(JobID::Nil()), JobID::Nil()));
  object_store_ = std::unique_ptr<ObjectStore>(new LocalModeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter(*this));
}

}  // namespace api
}  // namespace ray