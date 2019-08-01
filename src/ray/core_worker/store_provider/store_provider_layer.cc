#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"


namespace ray {

CoreWorkerStoreProviderLayer::CoreWorkerStoreProviderLayer(
    const WorkerContext &worker_context,
    const std::string &store_socket,
    std::unique_ptr<RayletClient> &raylet_client)
    : worker_context_(worker_context),
      store_socket_(store_socket),
      raylet_client_(raylet_client) {
  AddStoreProvider(StoreProviderType::LOCAL_PLASMA);
  AddStoreProvider(StoreProviderType::PLASMA);
}

Status CoreWorkerStoreProviderLayer::Put(StoreProviderType type, 
                                      const RayObject &object,
                                      const ObjectID &object_id) {
  return store_providers_[type]->Put(object, object_id);
}

Status CoreWorkerStoreProviderLayer::Get(StoreProviderType type,
                                      const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  return store_providers_[type]->Get(ids, timeout_ms, results);
}

Status CoreWorkerStoreProviderLayer::Wait(StoreProviderType type,
                                       const std::vector<ObjectID> &object_ids,
                                       int num_objects, 
                                       int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return store_providers_[type]->Wait(object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerStoreProviderLayer::Delete(StoreProviderType type,
                                         const std::vector<ObjectID> &object_ids,
                                         bool local_only,
                                         bool delete_creating_tasks) {
  return store_providers_[type]->Delete(object_ids, local_only, delete_creating_tasks);
}

void CoreWorkerStoreProviderLayer::AddStoreProvider(StoreProviderType type) {
  store_providers_.emplace(type, CreateStoreProvider(type));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerStoreProviderLayer::CreateStoreProvider(
    StoreProviderType type) const {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
    break;
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(worker_context_, store_socket_, raylet_client_));
    break;
  default:
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    break;
  }
}







}  // namespace ray
