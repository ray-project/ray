// Copyright 2020 The Ray Authors.
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

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client.h"

using absl::optional;
using std::shared_ptr;

namespace ray {
namespace rpc {

class CoreWorkerClientPool {
 public:
  CoreWorkerClientPool() = delete;

  /// Creates a CoreWorkerClientPool based on the low-level ClientCallManager.
  CoreWorkerClientPool(rpc::ClientCallManager &ccm)
      : client_factory_(defaultClientFactory(ccm)){};

  /// Creates a CoreWorkerClientPool by a given connection function.
  CoreWorkerClientPool(ClientFactoryFn client_factory)
      : client_factory_(client_factory){};

  /// Returns an existing Interface if one exists, or an empty optional
  /// otherwise.
  /// Any returned pointer is borrowed, and expected to be used briefly.
  optional<shared_ptr<CoreWorkerClientInterface>> GetByID(ray::WorkerID id);

  /// Returns an open CoreWorkerClientInterface if one exists, and connect to one
  /// if it does not. The returned pointer is borrowed, and expected to be used
  /// briefly.
  shared_ptr<CoreWorkerClientInterface> GetOrConnect(const Address &addr_proto);

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  void Disconnect(ray::WorkerID id);

 private:
  /// Provides the default client factory function. Providing this function to the
  /// construtor aids migration but is ultimately a thing that should be
  /// deprecated and brought internal to the pool, so this is our bridge.
  ClientFactoryFn defaultClientFactory(rpc::ClientCallManager &ccm) const {
    return [&](const rpc::Address &addr) {
      return std::shared_ptr<rpc::CoreWorkerClient>(new rpc::CoreWorkerClient(addr, ccm));
    };
  };

  /// This factory function does the connection to CoreWorkerClient, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one)
  ClientFactoryFn client_factory_;

  absl::Mutex mu_;

  /// A pool of open connections by WorkerID. Clients can reuse the connection
  /// objects in this pool by requesting them.
  absl::flat_hash_map<ray::WorkerID, shared_ptr<CoreWorkerClientInterface>> client_map_
      GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
