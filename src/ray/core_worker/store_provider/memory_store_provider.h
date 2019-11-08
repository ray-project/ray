#ifndef RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H
#define RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

/// The class provides implementations for accessing local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see direct_actor_transport.cc).
/// See `CoreWorkerStoreProvider` for the semantics of public methods.
class CoreWorkerMemoryStoreProvider {
 public:
  CoreWorkerMemoryStoreProvider(std::shared_ptr<CoreWorkerMemoryStore> store);

  Status Put(const RayObject &object, const ObjectID &object_id);

  Status Get(const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
             const TaskID &task_id,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
             bool *got_exception);

  Status Contains(const ObjectID &object_id, bool *has_object);

  /// Note that `num_objects` must equal to number of items in `object_ids`.
  Status Wait(const absl::flat_hash_set<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              absl::flat_hash_set<ObjectID> *ready);

  /// Note that `local_only` must be true, and `delete_creating_tasks` must be false here.
  Status Delete(const absl::flat_hash_set<ObjectID> &object_ids);

 private:
  /// Implementation.
  std::shared_ptr<CoreWorkerMemoryStore> store_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H
