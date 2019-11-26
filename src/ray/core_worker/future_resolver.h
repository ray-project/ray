#ifndef RAY_CORE_WORKER_FUTURE_RESOLVER_H
#define RAY_CORE_WORKER_FUTURE_RESOLVER_H

#include <memory>

#include "ray/common/id.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

/// Max time between requests to the owner to check whether the object has been
/// evicted.
const int kWaitObjectEvictionMilliseconds = 100;

// Resolve values for futures that were given to us before the value
// was available. This class is thread-safe.
class FutureResolver {
 public:
  FutureResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                 rpc::ClientFactoryFn client_factory, boost::asio::io_service &io_service,
                 int wait_object_eviction_milliseconds = kWaitObjectEvictionMilliseconds)
      : in_memory_store_(store),
        client_factory_(client_factory),
        io_service_(io_service),
        wait_object_eviction_milliseconds_(wait_object_eviction_milliseconds) {}

  void ResolveFutureAsync(const ObjectID &object_id, const TaskID &owner_id,
                          const rpc::Address &owner_address);

 private:
  void AttemptFutureResolution(const ObjectID &object_id, const TaskID &owner_id,
                               std::shared_ptr<boost::asio::deadline_timer> timer)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Used to set timers.
  boost::asio::io_service &io_service_;

  /// Used to store values of resolved futures.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Factory for producing new core worker clients.
  const rpc::ClientFactoryFn client_factory_;

  const int wait_object_eviction_milliseconds_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;

  /// Cache of gRPC clients to the objects' owners.
  absl::flat_hash_map<TaskID, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      owner_clients_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_FUTURE_RESOLVER_H
