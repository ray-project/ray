
#include "native_object_store.h"

#include <ray/api/ray_exception.h>

#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {
NativeObjectStore::NativeObjectStore(NativeRayRuntime &native_ray_tuntime)
    : native_ray_tuntime_(native_ray_tuntime) {}

void NativeObjectStore::PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                               ObjectID *object_id) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  auto buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data->data()), data->size(), true);
  auto status = core_worker.Put(
      ::ray::RayObject(buffer, nullptr, std::vector<ObjectID>()), {}, object_id);
  if (!status.ok()) {
    throw RayException("Put object error");
  }
  return;
}

void NativeObjectStore::PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                               const ObjectID &object_id) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  auto buffer = std::make_shared<::ray::LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data->data()), data->size(), true);
  auto status = core_worker.Put(
      ::ray::RayObject(buffer, nullptr, std::vector<ObjectID>()), {}, object_id);
  if (!status.ok()) {
    throw RayException("Put object error");
  }
  return;
}

std::shared_ptr<msgpack::sbuffer> NativeObjectStore::GetRaw(const ObjectID &object_id,
                                                            int timeout_ms) {
  std::vector<ObjectID> object_ids;
  object_ids.push_back(object_id);
  auto buffers = GetRaw(object_ids, timeout_ms);
  RAY_CHECK(buffers.size() == 1);
  return buffers[0];
}

std::vector<std::shared_ptr<msgpack::sbuffer>> NativeObjectStore::GetRaw(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  std::vector<std::shared_ptr<::ray::RayObject>> results;
  ::ray::Status status = core_worker.Get(ids, timeout_ms, &results);
  if (!status.ok()) {
    throw RayException("Get object error: " + status.ToString());
  }
  RAY_CHECK(results.size() == ids.size());
  std::vector<std::shared_ptr<msgpack::sbuffer>> result_sbuffers;
  result_sbuffers.reserve(results.size());
  for (size_t i = 0; i < results.size(); i++) {
    auto data_buffer = results[i]->GetData();
    auto sbuffer = std::make_shared<msgpack::sbuffer>(data_buffer->Size());
    sbuffer->write(reinterpret_cast<const char *>(data_buffer->Data()),
                   data_buffer->Size());
    result_sbuffers.push_back(sbuffer);
  }
  return result_sbuffers;
}

WaitResult NativeObjectStore::Wait(const std::vector<ObjectID> &ids, int num_objects,
                                   int timeout_ms) {
  native_ray_tuntime_.GetWorkerContext();
  return WaitResult();
}
}  // namespace api
}  // namespace ray