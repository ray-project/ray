#ifndef RAY_CORE_WORKER_MEMORY_STORE_H
#define RAY_CORE_WORKER_MEMORY_STORE_H

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"

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

  /// Create and return a buffer in the object store that can be directly written
  /// into. After writing to the buffer, the caller must call `Seal()` to finalize
  /// the object. The `Create()` and `Seal()` combination is an alternative interface
  /// to `Put()` that allows frontends to avoid an extra copy when possible.
  ///
  /// \param[in] metadata Metadata of the object to be written.
  /// \param[in] data_size Size of the object to be written.
  /// \param[in] object_id Object ID specified by the user.
  /// \param[out] data Buffer for the user to write the object into.
  /// \return Status.
  Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                const ObjectID &object_id, std::shared_ptr<Buffer> *data);

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `Create()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \return Status.
  Status Seal(const ObjectID &object_id);

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

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \return Void.
  void Delete(const std::vector<ObjectID> &object_ids);

 private:
  /// Map from object ID to `RayObject`.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;

  /// Map from object ID to its get requests.
  std::unordered_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_;

  /// Protect the two maps above.
  std::mutex lock_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_MEMORY_STORE_H
