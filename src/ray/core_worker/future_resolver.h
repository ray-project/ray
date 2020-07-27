// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "ray/common/id.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

// Resolve values for futures that were given to us before the value
// was available. This class is thread-safe.
class FutureResolver {
 public:
  FutureResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                 rpc::ClientFactoryFn client_factory, const rpc::Address &rpc_address)
      : in_memory_store_(store),
        client_factory_(client_factory),
        rpc_address_(rpc_address) {}

  /// Resolve the value for a future. This will periodically contact the given
  /// owner until the owner dies or the owner has finished creating the object.
  /// In either case, this will put an OBJECT_IN_PLASMA error as the future's
  /// value.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] owner_address The address of the task or actor that owns the
  /// future.
  void ResolveFutureAsync(const ObjectID &object_id, const rpc::Address &owner_address);

 private:
  /// Used to store values of resolved futures.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Factory for producing new core worker clients.
  const rpc::ClientFactoryFn client_factory_;

  /// Address of our RPC server. Used to notify borrowed objects' owners of our
  /// address, so the owner can contact us to ask when our reference to the
  /// object has gone out of scope.
  const rpc::Address rpc_address_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;

  /// Cache of gRPC clients to the objects' owners.
  absl::flat_hash_map<WorkerID, std::shared_ptr<rpc::CoreWorkerClientInterface>>
      owner_clients_ GUARDED_BY(mu_);
};

}  // namespace ray
