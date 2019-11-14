#ifndef RAY_CORE_WORKER_MEMORY_STORE_H
#define RAY_CORE_WORKER_MEMORY_STORE_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"

namespace ray {

class GetRequest;
class CoreWorkerMemoryStore;

/// The class provides implementations for local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see direct_actor_transport.cc).
class CoreWorkerMemoryStore {
 public:
  CoreWorkerMemoryStore();
  ~CoreWorkerMemoryStore(){};

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object_id Object ID specified by user.
  /// \param[in] object The ray object.
  /// \return Status.
  Status Put(const ObjectID &object_id, const RayObject &object);

  /// Get a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to get. Duplicates are not allowed.
  /// \param[in] num_objects Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] remove_after_get When to remove the objects from store after `Get`
  /// finishes.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &object_ids, int num_objects, int64_t timeout_ms,
             bool remove_after_get, std::vector<std::shared_ptr<RayObject>> *results);

  /// Asynchronously get an object from the object store. The object will not be removed
  /// from storage after GetAsync (TODO(ekl): integrate this with object GC).
  ///
  /// \param[in] object_id The object id to get.
  /// \param[in] callback The callback to run with the reference to the retrieved
  ///            object value once available.
  void GetAsync(const ObjectID &object_id,
                std::function<void(std::shared_ptr<RayObject>)> callback);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \return Void.
  void Delete(const std::vector<ObjectID> &object_ids);

  /// Check whether this store contains the object.
  ///
  /// \param[in] object_id The object to check.
  /// \return Whether the store has the object.
  bool Contains(const ObjectID &object_id);

 private:
  /// Map from object ID to `RayObject`.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> objects_ GUARDED_BY(mu_);

  /// Map from object ID to its get requests.
  absl::flat_hash_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_ GUARDED_BY(mu_);

  /// Map from object ID to its async get requests.
  absl::flat_hash_map<ObjectID,
                      std::vector<std::function<void(std::shared_ptr<RayObject>)>>>
      object_async_get_requests_ GUARDED_BY(mu_);

  /// Protect the two maps above.
  absl::Mutex mu_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MEMORY_STORE_H
