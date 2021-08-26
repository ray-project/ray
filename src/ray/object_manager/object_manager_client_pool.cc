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

#include "ray/object_manager/object_manager_client_pool.h"

namespace ray {

std::shared_ptr<rpc::ObjectManagerClient> ObjectManagerClientPool::GetOrConnectByAddress(
    const rpc::Address &address) {
  RAY_CHECK(address.raylet_id() != "");
  auto raylet_id = NodeID::FromBinary(address.raylet_id());
  auto it = client_map_.find(raylet_id);
  if (it != client_map_.end()) {
    return it->second;
  }
  auto connection = client_factory_(address);
  client_map_[raylet_id] = connection;

  RAY_LOG(DEBUG) << "Connected to " << address.ip_address() << ":" << address.port();
  return connection;
}

absl::optional<std::shared_ptr<rpc::ObjectManagerClient>>
ObjectManagerClientPool::GetOrConnectByID(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  auto client_cached = it != client_map_.end();
  auto node_info = gcs_client_.Nodes().Get(id);

  // If the node is dead, disconnect.
  if (!node_info) {
    if (client_cached) {
      client_map_.erase(it);
    }
    return {};
  }

  if (it == client_map_.end()) {
    rpc::Address addr;
    addr.set_ip_address(node_info->node_manager_address());
    addr.set_port(node_info->object_manager_port());
    addr.set_raylet_id(node_info->node_id());
    return GetOrConnectByAddress(addr);
  }
  return it->second;
}

std::vector<std::shared_ptr<rpc::ObjectManagerClient>>
ObjectManagerClientPool::GetOrConnectAllObjectManagerClients() {
  absl::MutexLock lock(&mu_);
  std::vector<std::shared_ptr<rpc::ObjectManagerClient>> clients;
  const auto &node_map = gcs_client_.Nodes().GetAll();
  for (const auto &item : node_map) {
    rpc::Address addr;
    addr.set_ip_address(item.second.node_manager_address());
    addr.set_port(item.second.object_manager_port());
    addr.set_raylet_id(item.second.node_id());
    clients.emplace_back(GetOrConnectByAddress(addr));
  }
  return clients;
}

void ObjectManagerClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

}  // namespace ray
