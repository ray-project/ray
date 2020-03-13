
#include "ray_dev_runtime.h"

#include <ray/api.h>
#include "../agent.h"
#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {
namespace api {

RayDevRuntime::RayDevRuntime(std::shared_ptr<RayConfig> config) {
  _config = config;
  _worker =
      std::unique_ptr<WorkerContext>(new WorkerContext(WorkerType::DRIVER, JobID::Nil()));
  _objectStore = std::unique_ptr<ObjectStore>(new LocalModeObjectStore());
  _taskSubmitter = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter());
}

ActorID RayDevRuntime::Create(remote_function_ptr_holder &fptr,
                              std::shared_ptr<msgpack::sbuffer> args) {
  return _taskSubmitter->CreateActor(fptr, args);
}

}  // namespace api
}  // namespace ray