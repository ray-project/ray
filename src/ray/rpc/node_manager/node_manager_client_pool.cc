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

#include "ray/rpc/node_manager/node_manager_client_pool.h"

#include <memory>

namespace ray {
namespace rpc {

std::shared_ptr<ray::RayletClientInterface> NodeManagerClientPool::GetOrConnectByAddress(
    const rpc::Address &address) {
  RAY_CHECK(address.raylet_id() != "");
  absl::MutexLock lock(&mu_);
  auto raylet_id = NodeID::FromBinary(address.raylet_id());
  auto it = client_map_.find(raylet_id);
  if (it != client_map_.end()) {
    RAY_CHECK(it->second != nullptr);
    return it->second;
  }
  auto connection = client_factory_(address);
  client_map_[raylet_id] = connection;

  RAY_LOG(DEBUG) << "Connected to raylet " << raylet_id << " at " << address.ip_address()
                 << ":" << address.port();
  RAY_CHECK(connection != nullptr);
  return connection;
}

std::optional<std::shared_ptr<ray::RayletClientInterface>>
NodeManagerClientPool::GetOrConnectByID(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return {};
  }
  return it->second;
}

void NodeManagerClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

}  // namespace rpc
}  // namespace ray
