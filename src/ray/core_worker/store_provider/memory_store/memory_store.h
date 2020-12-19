#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/reference_count.h"

namespace ray {

struct MemoryStoreStats {
  int32_t num_in_plasma = 0;
  int32_t num_local_objects = 0;
  int64_t used_object_store_memory = 0;
};

class GetRequest;
class CoreWorkerMemoryStore;

/// The class provides implementations for local process memory store.
/// An example usage for this is to retrieve the returned objects from direct
/// actor call (see direct_actor_transport.cc).
class CoreWorkerMemoryStore {
 public:
  /// Create a memory store.
  ///
  /// \param[in] store_in_plasma If not null, this is used to spill to plasma.
  /// \param[in] counter If not null, this enables ref counting for local objects,
  ///            and the `remove_after_get` flag for Get() will be ignored.
  /// \param[in] raylet_client If not null, used to notify tasks blocked / unblocked.
  CoreWorkerMemoryStore(
      std::function<void(const RayObject &, const ObjectID &)> store_in_plasma = nullptr,
      std::shared_ptr<ReferenceCounter> counter = nullptr,
      std::shared_ptr<raylet::RayletClient> raylet_client = nullptr,
      std::function<Status()> check_signals = nullptr);
  ~CoreWorkerMemoryStore(){};

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Whether the object was put into the memory store. If false, then
  /// this is because the object was promoted to and stored in plasma instead.
  bool Put(const RayObject &object, const ObjectID &object_id);

  /// Get a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to get. Duplicates are not allowed.
  /// \param[in] num_objects Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[in] ctx The current worker context.
  /// \param[in] remove_after_get When to remove the objects from store after `Get`
  /// finishes. This has no effect if ref counting is enabled.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &object_ids, int num_objects, int64_t timeout_ms,
             const WorkerContext &ctx, bool remove_after_get,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Convenience wrapper around Get() that stores results in a given result map.
  Status Get(const absl::flat_hash_set<ObjectID> &object_ids, int64_t timeout_ms,
             const WorkerContext &ctx,
             absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> *results,
             bool *got_exception);

  /// Convenience wrapper around Get() that stores ready objects in a given result set.
  Status Wait(const absl::flat_hash_set<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, const WorkerContext &ctx,
              absl::flat_hash_set<ObjectID> *ready);

  /// Asynchronously get an object from the object store. The object will not be removed
  /// from storage after GetAsync (TODO(ekl): integrate this with object GC).
  ///
  /// \param[in] object_id The object id to get.
  /// \param[in] callback The callback to run with the reference to the retrieved
  ///            object value once available.
  void GetAsync(const ObjectID &object_id,
                std::function<void(std::shared_ptr<RayObject>)> callback);

  /// Get a single object if available. If the object is not local yet, or if the object
  /// is local but is ErrorType::OBJECT_IN_PLASMA, then nullptr will be returned, and
  /// the store will ensure the object is promoted to plasma once available.
  ///
  /// \param[in] object_id The object id to get.
  /// \return pointer to the local object, or nullptr if promoted to plasma.
  std::shared_ptr<RayObject> GetOrPromoteToPlasma(const ObjectID &object_id);

  /// Delete a list of objects from the object store.
  /// NOTE(swang): Objects that contain IsInPlasmaError will not be
  /// deleted from the in-memory store. Instead, any future Get
  /// calls should check with plasma to see whether the object has
  /// been deleted.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[out] plasma_ids_to_delete This will be extended to
  /// include the IDs of the plasma objects to delete, based on the
  /// in-memory objects that contained InPlasmaError.
  /// \return Void.
  void Delete(const absl::flat_hash_set<ObjectID> &object_ids,
              absl::flat_hash_set<ObjectID> *plasma_ids_to_delete);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \return Void.
  void Delete(const std::vector<ObjectID> &object_ids);

  /// Check whether this store contains the object.
  ///
  /// \param[in] object_id The object to check.
  /// \param[out] in_plasma Set to true if the object was spilled to plasma.
  /// Will only be true if the store contains the object.
  /// \return Whether the store has the object.
  bool Contains(const ObjectID &object_id, bool *in_plasma);

  /// Returns the number of objects in this store.
  ///
  /// \return Count of objects in the store.
  int Size() {
    absl::MutexLock lock(&mu_);
    return objects_.size();
  }

  /// Returns stats data of memory usage.
  ///
  /// \return number of local objects and used memory size.
  MemoryStoreStats GetMemoryStoreStatisticalData();

  /// Returns the memory usage of this store.
  ///
  /// \return Total size of objects in the store.
  uint64_t UsedMemory();

 private:
  /// See the public version of `Get` for meaning of the other arguments.
  /// \param[in] abort_if_any_object_is_exception Whether we should abort if any object
  /// is an exception.
  Status GetImpl(const std::vector<ObjectID> &object_ids, int num_objects,
                 int64_t timeout_ms, const WorkerContext &ctx, bool remove_after_get,
                 std::vector<std::shared_ptr<RayObject>> *results,
                 bool abort_if_any_object_is_exception);

  /// Optional callback for putting objects into the plasma store.
  std::function<void(const RayObject &, const ObjectID &)> store_in_plasma_;

  /// If enabled, holds a reference to local worker ref counter. TODO(ekl) make this
  /// mandatory once Java is supported.
  std::shared_ptr<ReferenceCounter> ref_counter_ = nullptr;

  // If set, this will be used to notify worker blocked / unblocked on get calls.
  std::shared_ptr<raylet::RayletClient> raylet_client_ = nullptr;

  /// Protects the data structures below.
  mutable absl::Mutex mu_;

  /// Set of objects that should be promoted to plasma once available.
  absl::flat_hash_set<ObjectID> promoted_to_plasma_ GUARDED_BY(mu_);

  /// Map from object ID to `RayObject`.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> objects_ GUARDED_BY(mu_);

  /// Map from object ID to its get requests.
  absl::flat_hash_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>>
      object_get_requests_ GUARDED_BY(mu_);

  /// Map from object ID to its async get requests.
  absl::flat_hash_map<ObjectID,
                      std::vector<std::function<void(std::shared_ptr<RayObject>)>>>
      object_async_get_requests_ GUARDED_BY(mu_);

  /// Function passed in to be called to check for signals (e.g., Ctrl-C).
  std::function<Status()> check_signals_;
};

}  // namespace ray
