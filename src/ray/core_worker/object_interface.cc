#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    WorkerContext &worker_context, std::unique_ptr<RayletClient> &raylet_client,
    CoreWorkerStoreProviderLayer &store_provider_layer)
    : worker_context_(worker_context),
      raylet_client_(raylet_client),
      store_provider_layer_(store_provider_layer) {
}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_.GetCurrentTaskID(),
                                     worker_context_.GetNextPutIndex());
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  // TODO(zhijunfu): should determine transport based on object id, and then
  // find the store provider that the transport is using.                                        
  return store_provider_layer_.Put(StoreProviderType::PLASMA, object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  // TODO(zhijunfu): should determine transport based on object id, and then
  // find the store provider that the transport is using.
  return store_provider_layer_.Get(StoreProviderType::PLASMA,
      ids, timeout_ms, results);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  // TODO: if this is plasma, call the raylet client's wait.
  // otherwise, use a loop to invoke 
  return store_provider_layer_.Wait(StoreProviderType::PLASMA,
      object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return store_provider_layer_.Delete(StoreProviderType::PLASMA, object_ids, local_only,
                                      delete_creating_tasks);
}

}  // namespace ray
