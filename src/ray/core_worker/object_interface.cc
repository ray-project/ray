#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/single_process.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    RunMode run_mode, WorkerContext &worker_context,
    std::unique_ptr<RayletClient> &raylet_client, const std::string &store_socket)
    : run_mode_(run_mode),
      worker_context_(worker_context),
      raylet_client_(raylet_client) {
  switch (run_mode) {
  case RunMode::CLUSTER:
    store_providers_.emplace(
        static_cast<int>(StoreProviderType::PLASMA),
        std::make_shared<CoreWorkerPlasmaStoreProvider>(store_socket, raylet_client_));
    break;
  case RunMode::SINGLE_PROCESS:
    store_providers_.emplace(static_cast<int>(StoreProviderType::MOCK),
                             SingleProcess::Instance().StoreProvider());
    break;
  default:
    RAY_LOG(FATAL) << "Invalid run mode.";
  }
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex());
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  return GetStoreProvider()->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  return GetStoreProvider()->Get(ids, timeout_ms, worker_context_.GetCurrentTaskID(),
                                 results);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return GetStoreProvider()->Wait(object_ids, num_objects, timeout_ms,
                                  worker_context_.GetCurrentTaskID(), results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return GetStoreProvider()->Delete(object_ids, local_only, delete_creating_tasks);
}

std::shared_ptr<CoreWorkerStoreProvider> CoreWorkerObjectInterface::GetStoreProvider() {
  switch (run_mode_) {
  case RunMode::CLUSTER:
    return store_providers_[static_cast<int>(StoreProviderType::PLASMA)];
  case RunMode::SINGLE_PROCESS:
    return store_providers_[static_cast<int>(StoreProviderType::MOCK)];
  default:
    RAY_LOG(FATAL) << "Invalid run mode.";
  }
}

}  // namespace ray
