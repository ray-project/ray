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

/// The interface that contains all `CoreWorker` methods that are related to object store.
class CoreWorkerObjectInterface {
 public:
  CoreWorkerObjectInterface(WorkerContext &worker_context,
                            std::unique_ptr<RayletClient> &raylet_client,
                            const std::string &store_socket);

  /// Put an object into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[out] object_id Generated ID of the object.
  /// \return Status.
  Status Put(const RayObject &object, ObjectID *object_id);

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] object The ray object.
  /// \param[in] object_id Object ID specified by user.
  /// \return Status.
  Status Put(const RayObject &object, const ObjectID &object_id);

  /// Get a list of objects from the object store. Duplicate object ids are supported.
  ///
  /// \param[in] ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

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

 private:
  /// Helper function to get a list of objects from different store providers.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] ids_per_provider A map from store provider type to the set of
  //             object ids for that store provider.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's -1.
  /// \param[in/out] num_objects Number of objects that should appear before returning.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status WaitFromMultipleStoreProviders(
      const std::vector<ObjectID> &object_ids,
      const EnumUnorderedMap<StoreProviderType, std::unordered_set<ObjectID>>
          &ids_per_provider,
      int64_t timeout_ms, int *num_objects, std::vector<bool> *results);

  /// Helper function to get a list of objects from a specific store provider.
  ///
  /// \param[in] type The type of store provider to use.
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's -1.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status GetFromStoreProvider(
      StoreProviderType type, const std::unordered_set<ObjectID> &object_ids,
      int64_t timeout_ms,
      std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results);

  /// Helper function to wait a list of objects from a specific store provider.
  ///
  /// \param[in] type The type of store provider to use.
  /// \param[in] object_ids IDs of the objects to wait for.
  /// \param[in] num_objects Number of objects that should appear before returning.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status WaitFromStoreProvider(StoreProviderType type,
                               const std::unordered_set<ObjectID> &object_ids,
                               int num_objects, int64_t timeout_ms,
                               std::unordered_set<ObjectID> *results);

  /// Create a new store provider for the specified type on demand.
  std::unique_ptr<CoreWorkerStoreProvider> CreateStoreProvider(
      StoreProviderType type) const;

  /// Add a store provider for the specified type.
  void AddStoreProvider(StoreProviderType type);

  /// Reference to the parent CoreWorker's context.
  WorkerContext &worker_context_;
  /// Reference to the parent CoreWorker's raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;

  /// Store socket name.
  std::string store_socket_;

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
