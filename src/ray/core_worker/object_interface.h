#ifndef RAY_CORE_WORKER_OBJECT_INTERFACE_H
#define RAY_CORE_WORKER_OBJECT_INTERFACE_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

class CoreWorker;
class CoreWorkerStoreProvider;
class CoreWorkerMemoryStore;

/// The interface that contains all `CoreWorker` methods related to the object store.
class CoreWorkerObjectInterface {
 public:
  /// \param[in] worker_context WorkerContext of the parent CoreWorker.
  /// \param[in] store_socket Path to the plasma store socket.
  /// \param[in] use_memory_store Whether or not to use the in-memory object store
  ///            in addition to the plasma store.
  CoreWorkerObjectInterface(WorkerContext &worker_context,
                            std::unique_ptr<RayletClient> &raylet_client,
                            const std::string &store_socket,
                            bool use_memory_store = true);

  /// Set options for this client's interactions with the object store.
  ///
  /// \param[in] name Unique name for this object store client.
  /// \param[in] limit The maximum amount of memory in bytes that this client
  /// can use in the object store.
  Status SetClientOptions(std::string name, int64_t limit_bytes);

  /// Put an object into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object, ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by the user.
  /// \return Status.
  Status Put(const RayObject &object, const ObjectID &object_id);

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

  /// Get a list of objects from the object store. Objects that failed to be retrieved
  /// will be returned as nullptrs.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Return whether or not the object store contains the given object.
  ///
  /// \param[in] object_id ID of the objects to check for.
  /// \param[out] has_object Whether or not the object is present.
  /// \return Status.
  Status Contains(const ObjectID &object_id, bool *has_object);

  /// Wait for a list of objects to appear in the object store.
  /// Duplicate object ids are supported, and `num_objects` includes duplicate ids in this
  /// case.
  /// TODO(zhijunfu): it is probably more clear in semantics to just fail when there
  /// are duplicates, and require it to be handled at application level.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_objects Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects,
              int64_t timeout_ms, std::vector<bool> *results);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  Status Delete(const std::vector<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

  /// Get a string describing object store memory usage for debugging purposes.
  ///
  /// \return std::string The string describing memory usage.
  std::string MemoryUsageString();

 private:
  /// Helper function to group object IDs by the store provider that should be used
  /// for them.
  ///
  /// \param[in] object_ids Object IDs to group.
  /// \param[out] results Map of provider type to object IDs.
  void GroupObjectIdsByStoreProvider(
      const std::vector<ObjectID> &object_ids,
      EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>> *results);

  /// Helper function to get a set of objects from different store providers.
  ///
  /// \param[in] ids_per_provider A map from store provider type to the set of
  //             object ids for that store provider.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's -1.
  /// \param[in/out] num_objects Number of objects that should appear before returning.
  /// Should be updated as objects are added to the ready set.
  /// \param[in/out] results A set that holds objects that are ready.
  /// \return Status.
  Status WaitFromMultipleStoreProviders(
      EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>> &ids_per_provider,
      int64_t timeout_ms, int *num_objects, std::unordered_set<ObjectID> *results);

  /// Create a new store provider for the specified type on demand.
  std::unique_ptr<CoreWorkerStoreProvider> CreateStoreProvider(
      StoreProviderType type) const;

  /// Add a store provider for the specified type.
  void AddStoreProvider(StoreProviderType type);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;
  /// Reference to the parent CoreWorker's raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;

  std::string store_socket_;
  bool use_memory_store_;

  /// In-memory store for return objects. This is used for `MEMORY` store provider.
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  /// All the store providers supported.
  EnumUnorderedMap<StoreProviderType, std::unique_ptr<CoreWorkerStoreProvider>>
      store_providers_;

  friend class CoreWorkerTaskInterface;

  /// TODO(zhijunfu): This is necessary as direct call task submitter needs to create
  /// a local plasma store provider, later we can refactor ObjectInterface to add a
  /// `ObjectProviderLayer`, which will encapsulate the functionalities to get or create
  /// a specific `StoreProvider`, and this can be removed then.
  friend class CoreWorkerDirectActorTaskSubmitter;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_OBJECT_INTERFACE_H
