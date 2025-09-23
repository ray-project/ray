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
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/raylet_client_interface.h"

namespace ray {
namespace rpc {

using RayletClientFactoryFn =
    std::function<std::shared_ptr<ray::RayletClientInterface>(const rpc::Address &)>;
class RayletClientPool {
 public:
  /// Default unavailable_timeout_callback for retryable rpc's used by client factories on
  /// raylet.
  static std::function<void()> GetDefaultUnavailableTimeoutCallback(
      gcs::GcsClient *gcs_client,
      rpc::RayletClientPool *raylet_client_pool,
      const rpc::Address &addr);

  /// Return an existing RayletClient if exists or nullptr if it does not.
  /// The returned pointer is expected to be used briefly.
  std::shared_ptr<ray::RayletClientInterface> GetByID(ray::NodeID id);

  /// Return an existing RayletClient if exists or connect to one if it does
  /// not. The returned pointer is expected to be used briefly.
  /// The function is guaranteed to return the non-nullptr.
  std::shared_ptr<ray::RayletClientInterface> GetOrConnectByAddress(
      const rpc::Address &address);

  /// Removes a connection to the worker from the pool, if one exists. Since the
  /// shared pointer will no longer be retained in the pool, the connection will
  /// be open until it's no longer used, at which time it will disconnect.
  void Disconnect(ray::NodeID id);

  explicit RayletClientPool(RayletClientFactoryFn client_factory)
      : client_factory_(std::move(client_factory)){};

  static rpc::Address GenerateRayletAddress(const NodeID &node_id,
                                            const std::string &ip_address,
                                            int port);

 private:
  absl::Mutex mu_;

  /// This factory function makes the connection to the NodeManagerService, and is
  /// provided by the constructor (either the default implementation, above, or a
  /// provided one).
  RayletClientFactoryFn client_factory_;

  /// A pool of open connections by host:port. Clients can reuse the connection
  /// objects in this pool by requesting them
  absl::flat_hash_map<ray::NodeID, std::shared_ptr<ray::RayletClientInterface>>
      client_map_ ABSL_GUARDED_BY(mu_);
};

}  // namespace rpc
}  // namespace ray
