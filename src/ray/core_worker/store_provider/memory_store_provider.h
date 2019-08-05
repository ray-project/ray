#ifndef RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H
#define RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

class CoreWorker;

/// The class provides implementations for accessing local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see direct_actor_transport.cc).
class CoreWorkerMemoryStoreProvider : public CoreWorkerStoreProvider {
 public:
  CoreWorkerMemoryStoreProvider(std::shared_ptr<CoreWorkerMemoryStore> store);

  /// See `CoreWorkerStoreProvider::Put` for semantics.
  Status Put(const RayObject &object, const ObjectID &object_id) override;

  /// See `CoreWorkerStoreProvider::Get` for semantics.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms, const TaskID &task_id,
             std::vector<std::shared_ptr<RayObject>> *results) override;

  /// See `CoreWorkerStoreProvider::Wait` for semantics.
  /// Note that `num_objects` must equal to number of items in `object_ids`.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const TaskID &task_id,
              std::vector<bool> *results) override;

  /// See `CoreWorkerStoreProvider::Delete` for semantics.
  /// Note that `local_only` must be true, and `delete_creating_tasks` must be false here.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                bool delete_creating_tasks = false) override;

 private:
  /// Implementation.
  std::shared_ptr<CoreWorkerMemoryStore> store_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MEMORY_STORE_PROVIDER_H
