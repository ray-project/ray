#include "ray/core_worker/object_interface.h"
#include "ray/common/ray_config.h"

namespace ray {

CoreWorkerObjectInterface::CoreWorkerObjectInterface(
    std::shared_ptr<WorkerContext> worker_context,
    std::shared_ptr<CoreWorkerStoreProvider> store_provider)
    : worker_context_(worker_context), store_provider_(store_provider) {}

Status CoreWorkerObjectInterface::Put(const RayObject &object, ObjectID *object_id) {
  ObjectID put_id = ObjectID::ForPut(worker_context_->GetCurrentTaskID(),
                                     worker_context_->GetNextPutIndex());
  *object_id = put_id;
  return Put(object, put_id);
}

Status CoreWorkerObjectInterface::Put(const RayObject &object,
                                      const ObjectID &object_id) {
  return store_provider_->Put(object, object_id);
}

Status CoreWorkerObjectInterface::Get(const std::vector<ObjectID> &ids,
                                      int64_t timeout_ms,
                                      std::vector<std::shared_ptr<RayObject>> *results) {
  return store_provider_->Get(ids, timeout_ms, worker_context_->GetCurrentTaskID(),
                              results);
}

Status CoreWorkerObjectInterface::Wait(const std::vector<ObjectID> &object_ids,
                                       int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results) {
  return store_provider_->Wait(object_ids, num_objects, timeout_ms,
                               worker_context_->GetCurrentTaskID(), results);
}

Status CoreWorkerObjectInterface::Delete(const std::vector<ObjectID> &object_ids,
                                         bool local_only, bool delete_creating_tasks) {
  return store_provider_->Delete(object_ids, local_only, delete_creating_tasks);
}

}  // namespace ray
