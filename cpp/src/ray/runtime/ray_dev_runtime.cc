
#include "ray_dev_runtime.h"

#include <ray/api.h>
#include "../agent.h"
#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {

std::unordered_map<UniqueId, char *> RayDevRuntime::_actors;

RayDevRuntime::RayDevRuntime(std::shared_ptr<RayConfig> config) {
  _config = config;
  _worker = std::unique_ptr<Worker>(new Worker(config));
  _objectStore = std::unique_ptr<ObjectStore>(new LocalModeObjectStore());
  _taskSubmitter = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter());
}

std::unique_ptr<UniqueId> RayDevRuntime::create(remote_function_ptr_holder &fptr,
                                                std::shared_ptr<msgpack::sbuffer> args) {
  return _taskSubmitter.get()->createActor(fptr, args);
}

char *RayDevRuntime::get_actor_ptr(const UniqueId &id) {
  auto it = _actors.find(id);
  if (it != _actors.end()) {
    return it->second;
  } else {
    return nullptr;
  }
}

}  // namespace ray