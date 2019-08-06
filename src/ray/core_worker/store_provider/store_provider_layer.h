#ifndef RAY_CORE_WORKER_STORE_PROVIDER_LAYER_H
#define RAY_CORE_WORKER_STORE_PROVIDER_LAYER_H

#include "plasma/client.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/rpc/raylet/raylet_client.h"

namespace ray {

using rpc::RayletClient;

/// The interface that contains all `CoreWorker` methods that are related to object store.
class CoreWorkerStoreProviderLayer {
 public:
  CoreWorkerStoreProviderLayer(const WorkerContext &worker_context,
                            const std::string &store_socket,
                            std::unique_ptr<RayletClient> &raylet_client);

  ~CoreWorkerStoreProviderLayer() {}

  /// Put an object with specified ID into object store.
  ///
  /// \param[in] type The type of the store provider to use.
  /// For the rest see `CoreWorkerStoreProvider` for semantics.
  Status Put(StoreProviderType type, const RayObject &object, const ObjectID &object_id);

  /// Get a list of objects from the object store.
  ///
  /// \param[in] type The type of the store provider to use.
  /// For the rest see `CoreWorkerStoreProvider` for semantics.
  Status Get(StoreProviderType type, const std::vector<ObjectID> &ids, int64_t timeout_ms,
             std::vector<std::shared_ptr<RayObject>> *results);

  /// Wait for a list of objects to appear in the object store.
  ///
  /// \param[in] type The type of the store provider to use.
  /// For the rest see `CoreWorkerStoreProvider` for semantics.
  Status Wait(StoreProviderType type, const std::vector<ObjectID> &object_ids,
              int num_objects, int64_t timeout_ms, std::vector<bool> *results);

  /// Delete a list of objects from the object store.
  ///
  /// \param[in] type The type of the store provider to use.
  /// For the rest see `CoreWorkerStoreProvider` for semantics.
  Status Delete(StoreProviderType type, const std::vector<ObjectID> &object_ids,
                bool local_only = true, bool delete_creating_tasks = false);

  /// Create a new store provider for the specified type on demand.
  ///
  /// \param[in] type The type of the store provider to create.
  /// \return The created store provider.
  std::unique_ptr<CoreWorkerStoreProvider> CreateStoreProvider(
      StoreProviderType type) const;

 private:
  /// Whether the buffer represents an exception object.
  ///
  /// \param[in] object Object data.
  /// \return Whether it represents an exception object.
  static bool IsException(const RayObject &object);

  /// Print a warning if we've attempted too many times, but some objects are still
  /// unavailable.
  ///
  /// \param[in] num_attemps The number of attempted times.
  /// \param[in] unready The unready objects.
  static void WarnIfAttemptedTooManyTimes(
      int num_attempts, const std::unordered_map<ObjectID, int> &unready);

  bool ObjectsDone(const std::vector<ObjectID> &object_ids);

  /// Helper function to add a store provider for the specified type.
  void AddStoreProvider(StoreProviderType type);

  /// Reference to `WorkerContext`.
  const WorkerContext &worker_context_;

  /// Store socket name.
  const std::string store_socket_;

  /// Reference to the CoreWorker's raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;

  /// All the store providers supported.
  EnumUnorderedMap<StoreProviderType, std::unique_ptr<CoreWorkerStoreProvider>>
      store_providers_;
};

}  // namespace ray



#endif  // RAY_CORE_WORKER_STORE_PROVIDER_LAYER_H
