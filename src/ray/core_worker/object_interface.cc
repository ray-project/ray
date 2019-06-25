#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(CoreWorker &core_worker)
    : core_worker_(core_worker) {
  store_providers_.emplace(
      static_cast<int>(StoreProviderType::PLASMA),
      std::unique_ptr<CoreWorkerStoreProvider>(new CoreWorkerPlasmaStoreProvider(
          core_worker_.store_client_, core_worker_.store_client_mutex_,
          core_worker_.raylet_client_)));
}

Status CoreWorkerObjectInterface::Put(const Buffer &buffer, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(core_worker_.worker_context_.GetCurrentTaskID(),
                                     core_worker_.worker_context_.GetNextPutIndex());
  *object_id = put_id;
  return Put(buffer, put_id);
}

Status CoreWorkerObjectInterface::Put(const Buffer &buffer, const ObjectID &object_id) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Put(buffer, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<Buffer>> *results) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Get(
      ids, timeout_ms, core_worker_.worker_context_.GetCurrentTaskID(), results);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Wait(object_ids, num_objects, timeout_ms,
                                      core_worker_.worker_context_.GetCurrentTaskID(),
                                      results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  auto type = static_cast<int>(StoreProviderType::PLASMA);
  return store_providers_[type]->Delete(object_ids, local_only, delete_creating_tasks);
}

}  // namespace ray
