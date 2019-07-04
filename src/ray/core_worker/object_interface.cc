#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    const std::string &store_socket)
    : worker_context_(worker_context), raylet_client_(raylet_client) {
  store_providers_.emplace(
      static_cast<int>(StoreProviderType::PLASMA),
      std::unique_ptr<CoreWorkerStoreProvider>(
          new CoreWorkerPlasmaStoreProvider(store_socket, raylet_client_)));
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex());
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Get(ids, timeout_ms, worker_context_.GetCurrentTaskID(),
                                     results);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Wait(object_ids, num_objects, timeout_ms,
                                      worker_context_.GetCurrentTaskID(), results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Delete(object_ids, local_only, delete_creating_tasks);
}

}  // namespace ray
