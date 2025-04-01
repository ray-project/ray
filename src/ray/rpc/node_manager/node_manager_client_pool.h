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

#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"

namespace ray {
namespace rpc {

using RayletClientFactoryFn =
    std::function<std::shared_ptr<ray::RayletClientInterface>(const rpc::Address &)>;
class NodeManagerClientPool {
 public:
  NodeManagerClientPool() = delete;

  /// Return an existing NodeManagerWorkerClient if exists, and connect to one if it does
  /// not. The returned pointer is borrowed, and expected to be used briefly.
  std::optional<std::shared_ptr<ray::RayletClientInterface>> GetOrConnectByID(
      ray::NodeID id);

  /// Return an existing NodeManagerWorkerClient if exists, and connect to one if it does
  /// not. The returned pointer is borrowed, and expected to be used briefly.
  /// The function is guaranteed to return the non-nullptr.
  std::shared_ptr<ray::RayletClientInterface> GetOrConnectByAddress(
      const rpc::Address &address);

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  void Disconnect(ray::NodeID id);

  explicit NodeManagerClientPool(rpc::ClientCallManager &ccm)
      : client_factory_(defaultClientFactory(ccm)){};

  explicit NodeManagerClientPool(RayletClientFactoryFn client_factory)
      : client_factory_(client_factory){};

 private:
  /// Provides the default client factory function. Providing this function to the
  /// construtor aids migration but is ultimately a thing that should be
  /// deprecated and brought internal to the pool, so this is our bridge.
  RayletClientFactoryFn defaultClientFactory(rpc::ClientCallManager &ccm) const {
    return [&](const rpc::Address &addr) {
      auto nm_client = NodeManagerWorkerClient::make(addr.ip_address(), addr.port(), ccm);
      std::shared_ptr<ray::RayletClientInterface> raylet_client =
          std::make_shared<ray::raylet::RayletClient>(nm_client);
      return raylet_client;
    };
  };

  absl::Mutex mu_;

  /// This factory function does the connection to NodeManagerWorkerClient, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one)
  RayletClientFactoryFn client_factory_;

  /// A pool of open connections by host:port. Clients can reuse the connection
  /// objects in this pool by requesting them
  absl::flat_hash_map<ray::NodeID, std::shared_ptr<ray::RayletClientInterface>>
      client_map_ ABSL_GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
