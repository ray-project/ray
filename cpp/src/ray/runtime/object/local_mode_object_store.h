
#pragma once

#include <unordered_map>
#include "ray/core.h"

#include "../local_mode_ray_runtime.h"
#include "object_store.h"

namespace ray {
namespace api {

class LocalModeObjectStore : public ObjectStore {
 public:
  LocalModeObjectStore(LocalModeRayRuntime &local_mode_ray_tuntime);

  WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects, int timeout_ms);

 private:
  void PutRaw(const ObjectID &object_id, std::shared_ptr<msgpack::sbuffer> data);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id, int timeout_ms);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(const std::vector<ObjectID> &ids,
                                                        int timeout_ms);

  std::unique_ptr<::ray::CoreWorkerMemoryStore> memory_store_;

  LocalModeRayRuntime &local_mode_ray_tuntime_;
};

}  // namespace api
}  // namespace ray