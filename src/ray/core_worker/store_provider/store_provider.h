#ifndef RAY_CORE_WORKER_STORE_PROVIDER_H
#define RAY_CORE_WORKER_STORE_PROVIDER_H

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"

namespace ray {

/// Provider interface for store access. Store provider should inherit from this class and
/// provide implementions for the methods. The actual store provider may use a plasma
/// store or local memory store in worker process, or possibly other types of storage.

class CoreWorkerStoreProvider {
 public:
  CoreWorkerStoreProvider() {}

  virtual ~CoreWorkerStoreProvider() {}

  /// Set options for this client's interactions with the object store.
  ///
  /// \param[in] name Unique name for this object store client.
  /// \param[in] limit The maximum amount of memory in bytes that this client
  /// can use in the object store.
  virtual Status SetClientOptions(std::string name, int64_t limit_bytes) = 0;

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  virtual Status Put(const RayObject &object, const ObjectID &object_id) = 0;

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
  virtual Status Create(const std::shared_ptr<Buffer> &metadata, const size_t data_size,
                        const ObjectID &object_id, std::shared_ptr<Buffer> *data) = 0;

  /// Finalize placing an object into the object store. This should be called after
  /// a corresponding `Create()` call and then writing into the returned buffer.
  ///
  /// \param[in] object_id Object ID corresponding to the object.
  /// \return Status.
  virtual Status Seal(const ObjectID &object_id) = 0;

  /// Get a set of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Map of objects to write results into. Get will only add to this
  /// map, not clear or remove from it, so the caller can pass in a non-empty map.
  /// \param[out] got_exception Set to true if any of the fetched results were an
  /// exception.
  /// \return Status.
  virtual Status Get(const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
                     const TaskID &task_id,
                     std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results,
                     bool *got_exception) = 0;

  /// Return whether or not the object store contains the given object.
  ///
  /// \param[in] object_id ID of the objects to check for.
  /// \param[out] has_object Whether or not the object is present.
  /// \return Status.
  virtual Status Contains(const ObjectID &object_id, bool *has_object) = 0;

  /// Wait for a list of objects to appear in the object store. Objects that appear will
  /// be added to the ready set.
  ///
  /// \param[in] object_ids IDs of the objects to wait for.
  /// \param[in] num_objects Number of objects that should appear before returning.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] task_id ID for the current task.
  /// \param[out] ready IDs of objects that have appeared. Wait will only add to this
  /// set, not clear or remove from it, so the caller can pass in a non-empty set.
  /// \return Status.
  virtual Status Wait(const std::unordered_set<ObjectID> &object_ids, int num_objects,
                      int64_t timeout_ms, const TaskID &task_id,
                      std::unordered_set<ObjectID> *ready) = 0;

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  virtual Status Delete(const std::vector<ObjectID> &object_ids, bool local_only = true,
                        bool delete_creating_tasks = false) = 0;

  /// Get a string describing object store memory usage for debugging purposes.
  ///
  /// \return std::string The string describing memory usage.
  virtual std::string MemoryUsageString() = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_STORE_PROVIDER_H
