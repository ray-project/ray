#include "ray/core_worker/store_provider/memory_store_provider.h"
#include <condition_variable>
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"

namespace ray {

//
// CoreWorkerMemoryStoreProvider functions
//
CoreWorkerMemoryStoreProvider::CoreWorkerMemoryStoreProvider(
    std::shared_ptr<CoreWorkerMemoryStore> store)
    : store_(store) {
  RAY_CHECK(store != nullptr);
}

Status CoreWorkerMemoryStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  auto status = store_->Put(object_id, object);
  if (status.IsObjectExists()) {
    // Object already exists in store, treat it as ok.
    return Status::OK();
  } else {
    return status;
  }
}

Status CoreWorkerMemoryStoreProvider::Get(
    const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
    const TaskID &task_id,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  const std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_RETURN_NOT_OK(
      store_->Get(id_vector, id_vector.size(), timeout_ms, true, &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      (*results)[id_vector[i]] = result_objects[i];
    }
  }
  return Status::OK();
}

Status CoreWorkerMemoryStoreProvider::Wait(const std::unordered_set<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::unordered_set<ObjectID> *ready) {
  std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_CHECK(object_ids.size() == id_vector.size());
  RAY_RETURN_NOT_OK(
      store_->Get(id_vector, num_objects, timeout_ms, false, &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      ready->insert(id_vector[i]);
    }
  }

  return Status::OK();
}

Status CoreWorkerMemoryStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  store_->Delete(object_ids);
  return Status::OK();
}

}  // namespace ray
