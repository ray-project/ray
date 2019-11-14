#include "ray/core_worker/store_provider/memory_store_provider.h"
#include <condition_variable>
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {

CoreWorkerMemoryStoreProvider::CoreWorkerMemoryStoreProvider(
    std::shared_ptr<CoreWorkerMemoryStore> store)
    : store_(store) {
  RAY_CHECK(store != nullptr);
}

Status CoreWorkerMemoryStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  Status status = store_->Put(object_id, object);
  if (status.IsObjectExists()) {
    // Object already exists in store, treat it as ok.
    return Status::OK();
  }
  return status;
}

Status CoreWorkerMemoryStoreProvider::Get(
    const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
    const TaskID &task_id,
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
    bool *got_exception) {
  const std::vector<ObjectID> id_vector(object_ids.begin(), object_ids.end());
  std::vector<std::shared_ptr<RayObject>> result_objects;
  RAY_RETURN_NOT_OK(
      store_->Get(id_vector, id_vector.size(), timeout_ms, true, &result_objects));

  for (size_t i = 0; i < id_vector.size(); i++) {
    if (result_objects[i] != nullptr) {
      (*results)[id_vector[i]] = result_objects[i];
      if (result_objects[i]->IsException()) {
        *got_exception = true;
      }
    }
  }
  return Status::OK();
}

Status CoreWorkerMemoryStoreProvider::Contains(const ObjectID &object_id,
                                               bool *has_object) {
  *has_object = store_->Contains(object_id);
  return Status::OK();
}

Status CoreWorkerMemoryStoreProvider::Wait(
    const absl::flat_hash_set<ObjectID> &object_ids, int num_objects, int64_t timeout_ms,
    const TaskID &task_id, absl::flat_hash_set<ObjectID> *ready) {
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

Status CoreWorkerMemoryStoreProvider::Delete(
    const absl::flat_hash_set<ObjectID> &object_ids) {
  std::vector<ObjectID> object_id_vector(object_ids.begin(), object_ids.end());
  store_->Delete(object_id_vector);
  return Status::OK();
}

}  // namespace ray
