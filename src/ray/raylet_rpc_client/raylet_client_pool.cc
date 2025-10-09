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

#include "ray/raylet_rpc_client/raylet_client_pool.h"

#include <memory>
#include <string>
#include <vector>

namespace ray {
namespace rpc {

std::function<void()> RayletClientPool::GetDefaultUnavailableTimeoutCallback(
    gcs::GcsClient *gcs_client,
    rpc::RayletClientPool *raylet_client_pool,
    const rpc::Address &addr) {
  return [addr, gcs_client, raylet_client_pool]() {
    const NodeID node_id = NodeID::FromBinary(addr.node_id());

    auto gcs_check_node_alive = [node_id, addr, raylet_client_pool, gcs_client]() {
      gcs_client->Nodes().AsyncGetAll(
          [addr, node_id, raylet_client_pool](const Status &status,
                                              std::vector<rpc::GcsNodeInfo> &&nodes) {
            if (!status.ok()) {
              // Will try again when unavailable timeout callback is retried.
              RAY_LOG(INFO) << "Failed to get node info from GCS";
              return;
            }
            if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
              // The node is dead or GCS doesn't know about this node.
              // There's only two reasons the GCS doesn't know about the node:
              // 1. The node isn't registered yet.
              // 2. The GCS erased the dead node based on
              //    maximum_gcs_dead_node_cached_count.
              // In this case, it must be 2 since there's no way for a component to
              // know about a remote node id until the gcs has registered it.
              RAY_LOG(INFO).WithField(node_id)
                  << "Disconnecting raylet client because its node is dead";
              raylet_client_pool->Disconnect(node_id);
              return;
            }
          },
          -1,
          {node_id});
    };

    if (gcs_client->Nodes().IsSubscribedToNodeChange()) {
      auto *node_info = gcs_client->Nodes().Get(node_id, /*filter_dead_nodes=*/false);
      if (node_info == nullptr) {
        // Node could be dead or info may have not made it to the subscriber cache yet.
        // Check with the GCS to confirm if the node is dead.
        gcs_check_node_alive();
        return;
      }
      if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
        RAY_LOG(INFO).WithField(node_id)
            << "Disconnecting raylet client because its node is dead.";
        raylet_client_pool->Disconnect(node_id);
        return;
      }
      // Node is alive so raylet client is alive.
      return;
    }
    // Not subscribed so ask GCS.
    gcs_check_node_alive();
  };
}

std::shared_ptr<ray::RayletClientInterface> RayletClientPool::GetOrConnectByAddress(
    const rpc::Address &address) {
  RAY_CHECK(address.node_id() != "");
  absl::MutexLock lock(&mu_);
  auto node_id = NodeID::FromBinary(address.node_id());
  auto it = client_map_.find(node_id);
  if (it != client_map_.end()) {
    RAY_CHECK(it->second != nullptr);
    return it->second;
  }
  auto connection = client_factory_(address);
  client_map_[node_id] = connection;

  RAY_LOG(DEBUG) << "Connected to raylet " << node_id << " at "
                 << BuildAddress(address.ip_address(), address.port());
  RAY_CHECK(connection != nullptr);
  return connection;
}

std::shared_ptr<ray::RayletClientInterface> RayletClientPool::GetByID(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return {};
  }
  return it->second;
}

void RayletClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

rpc::Address RayletClientPool::GenerateRayletAddress(const NodeID &node_id,
                                                     const std::string &ip_address,
                                                     int port) {
  rpc::Address address;
  address.set_ip_address(ip_address);
  address.set_port(port);
  address.set_node_id(node_id.Binary());
  return address;
}

}  // namespace rpc
}  // namespace ray
