#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"


namespace ray {

CoreWorkerStoreProviderLayer::CoreWorkerStoreProviderLayer(
    std::unique_ptr<RayletClient> &raylet_client,
    const std::string &store_socket)
    : raylet_client_(raylet_client),
      store_socket_(store_socket) {
  AddStoreProvider(StoreProviderType::LOCAL_PLASMA);
  AddStoreProvider(StoreProviderType::PLASMA);
}

Status CoreWorkerObjectInterface::Put(StoreProviderType type, 
                                      const RayObject &object,
                                      const ObjectID &object_id) {
  return store_providers_[type]->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(StoreProviderType type,
                                      const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  return store_providers_[type]->Get(ids, timeout_ms, results);
}

Status CoreWorkerObjectInterface::Wait(StoreProviderType type,
                                       const std::vector<ObjectID> &object_ids,
                                       int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return store_providers_[type]->Wait(object_ids, timeout_ms, results);
}

Status CoreWorkerObjectInterface::Delete(StoreProviderType type,
                                         const std::vector<ObjectID> &object_ids) {
  return store_providers_[type]->Delete(object_ids);
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
        new CoreWorkerPlasmaStoreProvider(store_socket_, raylet_client_));
    break;
  default:
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    break;
  }
}







}  // namespace ray
