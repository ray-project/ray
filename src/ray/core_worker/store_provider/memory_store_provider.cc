#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include <condition_variable>

namespace ray {

//
// CoreWorkerMemoryStoreProvider functions
//
CoreWorkerMemoryStoreProvider::CoreWorkerMemoryStoreProvider(
    std::shared_ptr<CoreWorkerMemoryStore> store)
    : store_(store) { RAY_CHECK(store != nullptr); }

Status CoreWorkerMemoryStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  return store_->Put(object, object_id);
}

Status CoreWorkerMemoryStoreProvider::Get(const std::vector<ObjectID> &ids,
                                          int64_t timeout_ms, const TaskID &task_id,
                                          std::vector<std::shared_ptr<RayObject>> *results) {
  return store_->Get(ids, timeout_ms, results);
}

Status CoreWorkerMemoryStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::vector<bool> *results) {
  return store_->Wait(object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerMemoryStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  store_->Delete(object_ids);
  return Status::OK();
}

}  // namespace ray
