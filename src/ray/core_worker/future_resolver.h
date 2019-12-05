#ifndef RAY_CORE_WORKER_FUTURE_RESOLVER_H
#define RAY_CORE_WORKER_FUTURE_RESOLVER_H

#include <memory>

#include "ray/common/id.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

// Resolve values for futures that were given to us before the value
// was available. This class is thread-safe.
class FutureResolver {
 public:
  FutureResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                 rpc::ClientFactoryFn client_factory)
      : in_memory_store_(store), client_factory_(client_factory) {}

  /// Resolve the value for a future. This will periodically contact the given
  /// owner until the owner dies or the owner has finished creating the object.
  /// In either case, this will put an OBJECT_IN_PLASMA error as the future's
  /// value.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] owner_id The ID of the task or actor that owns the future.
  /// \param[in] owner_address The address of the task or actor that owns the
  /// future.
  void ResolveFutureAsync(const ObjectID &object_id, const TaskID &owner_id,
                          const rpc::Address &owner_address);

  /// Returns whether resolution of an object is in progress.
  ///
  /// \param[in] object_id ID of the object to query.
  /// \return Whether the object is pending.
  bool IsObjectPending(const ObjectID &object_id) {
    absl::ReaderMutexLock lock(&mu_);
    return pending_.find(object_id) != pending_.end();
  }

 private:
  /// Used to store values of resolved futures.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Factory for producing new core worker clients.
  const rpc::ClientFactoryFn client_factory_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;

  /// Cache of gRPC clients to the objects' owners.
  absl::flat_hash_map<TaskID, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      owner_clients_ GUARDED_BY(mu_);

  /// Set of object ids we are currently resolving.
  absl::flat_hash_set<ObjectID> pending_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_FUTURE_RESOLVER_H
