#ifndef RAY_CORE_WORKER_MEMORY_STORE_H
#define RAY_CORE_WORKER_MEMORY_STORE_H

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

class GetOrWaitRequest;
class CoreWorkerMemoryStore;

/// The class provides implementations for local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see direct_actor_transport.cc).
class CoreWorkerMemoryStore {
 public:
  CoreWorkerMemoryStore();
  ~CoreWorkerMemoryStore(){};

  Status Put(const RayObject &object, const ObjectID &object_id);
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, std::vector<bool> *results);
  void Delete(const std::vector<ObjectID> &object_ids);

 private:
  Status GetOrWait(const std::vector<ObjectID> &ids, int64_t timeout_ms,
                   std::vector<std::shared_ptr<RayObject>> *results, bool is_get);

  /// Map from object ID to `RayObject`.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;

  /// Map from object ID to its get/wait requests.
  std::unordered_map<ObjectID, std::vector<std::shared_ptr<GetOrWaitRequest>>>
      object_get_requests_;

  /// Protect the two maps above.
  std::mutex lock_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MEMORY_STORE_H
