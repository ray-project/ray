
#pragma once

#include <unordered_map>

#include "../native_ray_runtime.h"
#include "object_store.h"
#include "ray/core.h"

namespace ray {
namespace api {

class NativeObjectStore : public ObjectStore {
 public:
  NativeObjectStore(NativeRayRuntime &native_ray_tuntime);

  WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects, int timeout_ms);

 private:
  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  void PutRaw(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id, int timeout_ms);

  std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(const std::vector<ObjectID> &ids,
                                                        int timeout_ms);

  NativeRayRuntime &native_ray_tuntime_;
};

}  // namespace api
}  // namespace ray