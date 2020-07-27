
#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include <ray/api/ray_exception.h>
#include "../abstract_ray_runtime.h"
#include "native_object_store.h"

namespace ray {
namespace api {
NativeObjectStore::NativeObjectStore(NativeRayRuntime &native_ray_tuntime)
    : native_ray_tuntime_(native_ray_tuntime) {}

void NativeObjectStore::PutRaw(const ObjectID &object_id,
                               std::shared_ptr<msgpack::sbuffer> data) {}

std::shared_ptr<msgpack::sbuffer> NativeObjectStore::GetRaw(const ObjectID &object_id,
                                                            int timeout_ms) {
  return nullptr;
}

std::vector<std::shared_ptr<msgpack::sbuffer>> NativeObjectStore::GetRaw(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  return std::vector<std::shared_ptr<msgpack::sbuffer>>();
}

WaitResult NativeObjectStore::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                   int timeout_ms) {
  native_ray_tuntime_.GetWorkerContext();
  return WaitResult();
}
}  // namespace api
}  // namespace ray