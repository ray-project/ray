#include <algorithm>

#include "ray/common/ray_config.h"
#include "ray/core_worker/object_interface.h"
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

  // Divide the object ids into two groups: direct call return objects and the rest,
  // and de-duplicate for each group.
  std::unordered_set<ObjectID> direct_call_return_ids;
  std::unordered_set<ObjectID> other_ids;
  for (const auto &object_id : ids) {
    if (object_id.IsReturnObject() &&
        object_id.GetTransportType() ==
            static_cast<int>(TaskTransportType::DIRECT_ACTOR)) {
      direct_call_return_ids.insert(object_id);
    } else {
      other_ids.insert(object_id);
    }
  }

  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects;
  auto start_time = current_time_ms();
  // Fetch non-direct-call objects using `PLASMA` store provider.
  RAY_RETURN_NOT_OK(Get(StoreProviderType::PLASMA, other_ids, timeout_ms, &objects));
  int64_t duration = current_time_ms() - start_time;
  int64_t left_timeout_ms =
      (timeout_ms == -1) ? timeout_ms
                         : std::max(static_cast<int64_t>(0), timeout_ms - duration);

  // Fetch direct call return objects using `LOCAL_PLASMA` store provider.
  RAY_RETURN_NOT_OK(Get(StoreProviderType::LOCAL_PLASMA, direct_call_return_ids,
                        left_timeout_ms, &objects));

  for (size_t i = 0; i < ids.size(); i++) {
    (*results)[i] = objects[ids[i]];
  }

  return Status::OK();
}

Status CoreWorkerObjectInterface::Get(
    StoreProviderType type, const std::unordered_set<ObjectID> &object_ids,
    int64_t timeout_ms,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  std::vector<ObjectID> ids(object_ids.begin(), object_ids.end());
  if (!ids.empty()) {
    std::vector<std::shared_ptr<RayObject>> objects;
    RAY_RETURN_NOT_OK(store_providers_[type]->Get(
        ids, timeout_ms, worker_context_.GetCurrentTaskID(), &objects));
    RAY_CHECK(ids.size() == objects.size());
    for (size_t i = 0; i < objects.size(); i++) {
      (*results).emplace(ids[i], objects[i]);
    }
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
