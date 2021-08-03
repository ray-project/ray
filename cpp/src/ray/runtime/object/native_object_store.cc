
#include "native_object_store.h"

#include <ray/api/ray_exception.h>

#include <algorithm>
#include <chrono>
#include <list>
#include <thread>

#include "../abstract_ray_runtime.h"

namespace ray {
namespace api {

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

void NativeObjectStore::CheckException(const std::string &meta_str,
                                       const std::string &data_str) {
  if (meta_str == std::to_string(ray::rpc::ErrorType::WORKER_DIED)) {
    throw RayWorkerException(data_str);
  } else if (meta_str == std::to_string(ray::rpc::ErrorType::ACTOR_DIED)) {
    throw RayActorException(data_str);
  } else if (meta_str == std::to_string(ray::rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE)) {
    throw UnreconstructableException(data_str);
  } else if (meta_str == std::to_string(ray::rpc::ErrorType::TASK_EXECUTION_EXCEPTION)) {
    throw RayTaskException(data_str);
  }
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
    auto meta = results[i]->GetMetadata();
    auto data_buffer = results[i]->GetData();
    if (meta != nullptr) {
      bool empty = data_buffer->Size() == 0;
      std::string data_str =
          empty ? "" : std::string((char *)data_buffer->Data(), data_buffer->Size());
      std::string meta_str((char *)meta->Data(), meta->Size());
      CheckException(meta_str, data_str);
    }

    auto sbuffer = std::make_shared<msgpack::sbuffer>(data_buffer->Size());
    sbuffer->write(reinterpret_cast<const char *>(data_buffer->Data()),
                   data_buffer->Size());
    result_sbuffers.push_back(sbuffer);
  }
  return result_sbuffers;
}

std::vector<bool> NativeObjectStore::Wait(const std::vector<ObjectID> &ids,
                                          int num_objects, int timeout_ms) {
  std::vector<bool> results;
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  // TODO(guyang.sgy): Support `fetch_local` option in API.
  // Simply set `fetch_local` to be true.
  ::ray::Status status = core_worker.Wait(ids, num_objects, timeout_ms, &results, true);
  if (!status.ok()) {
    throw RayException("Wait object error: " + status.ToString());
  }
  return results;
}

void NativeObjectStore::AddLocalReference(const std::string &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.AddLocalReference(ObjectID::FromBinary(id));
  }
}

void NativeObjectStore::RemoveLocalReference(const std::string &id) {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.RemoveLocalReference(ObjectID::FromBinary(id));
  }
}
}  // namespace api
}  // namespace ray