#include "ray/core_worker/object_interface.h"
#include <algorithm>
#include "ray/common/ray_config.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    const std::string &store_socket)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_socket_(store_socket) {
  AddStoreProvider(StoreProviderType::LOCAL_PLASMA);
  AddStoreProvider(StoreProviderType::PLASMA);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex(),
                                     /*transport_type=*/0);
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  return store_providers_[StoreProviderType::PLASMA]->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(ids.size(), nullptr);

  std::vector<ObjectID> direct_call_return_ids;
  std::vector<ObjectID> other_ids;
  for (const auto &object_id : ids) {
    if (object_id.IsReturnObject() &&
        object_id.GetTransportType() ==
            static_cast<int>(TaskTransportType::DIRECT_ACTOR)) {
      direct_call_return_ids.push_back(object_id);
    } else {
      other_ids.push_back(object_id);
    }
  }

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> object_map(ids.size());
  auto start_time = current_time_ms();
  // Fetch non-direct-call objects using `PLASMA` store provider.
  if (!other_ids.empty()) {
    std::vector<std::shared_ptr<RayObject>> objects;
    RAY_RETURN_NOT_OK(store_providers_[StoreProviderType::PLASMA]->Get(
        other_ids, timeout_ms, worker_context_.GetCurrentTaskID(), &objects));
    RAY_CHECK(other_ids.size() == objects.size());
    for (int i = 0; i < objects.size(); i++) {
      object_map.emplace(ids[i], objects[i]);
    }
  }

  int64_t duration = current_time_ms() - start_time;
  int64_t left_timeout_ms =
      (timeout_ms == -1) ? timeout_ms : std::max(0LL, timeout_ms - duration);

  // Fetch direct call return objects using `LOCAL_PLASMA` store provider.
  if (!direct_call_return_ids.empty()) {
    std::vector<std::shared_ptr<RayObject>> objects;
    RAY_RETURN_NOT_OK(store_providers_[StoreProviderType::LOCAL_PLASMA]->Get(
        direct_call_return_ids, left_timeout_ms, worker_context_.GetCurrentTaskID(),
        &objects));
    RAY_CHECK(direct_call_return_ids.size() == objects.size());
    for (int i = 0; i < objects.size(); i++) {
      object_map.emplace(ids[i], objects[i]);
    }
  }

  for (int i = 0; i < ids.size(); i++) {
    (*results)[i] = object_map[ids[i]];
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return store_providers_[StoreProviderType::PLASMA]->Wait(
      object_ids, num_objects, timeout_ms, worker_context_.GetCurrentTaskID(), results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return store_providers_[StoreProviderType::PLASMA]->Delete(object_ids, local_only,
                                                             delete_creating_tasks);
}

void CoreWorkerObjectInterface::AddStoreProvider(StoreProviderType type) {
  store_providers_.emplace(type, CreateStoreProvider(type));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerObjectInterface::CreateStoreProvider(
    StoreProviderType type) const {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(store_socket_, raylet_client_));
  default:
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    return nullptr;
  }
}

}  // namespace ray
