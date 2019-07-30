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

using rpc::RayletClient;

class CoreWorker;
class CoreWorkerStoreProvider;

/// The interface that contains all `CoreWorker` methods that are related to the object store.
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
  Status Create(const std::shared_ptr<Buffer> &metadata,
		const size_t data_size,
                const ObjectID &object_id,
                std::shared_ptr<Buffer> *data);

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

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] IDs of the objects to wait for.
  /// \param[in] num_returns Number of objects that should appear.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's negative.
  /// \param[out] results A bitset that indicates each object has appeared or not.
  /// \return Status.
  Status Wait(const std::vector<ObjectID> &object_ids, int num_objects, int64_t timeout_ms,
                                       std::vector<bool> *results);

  /// Free a list of objects from the object store.
  ///
  /// \param[in] object_ids IDs of the objects to delete.
  /// \param[in] local_only Whether only delete the objects in local node, or all nodes in
  /// the cluster.
  /// \param[in] delete_creating_tasks Whether also delete the tasks that
  /// created these objects.
  /// \return Status.
  Status Free(const std::vector<ObjectID> &object_ids, bool local_only,
                bool delete_creating_tasks);

 private:
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
