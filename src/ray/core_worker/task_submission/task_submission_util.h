// Copyright 2025 The Ray Authors.
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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"

namespace ray {
namespace core {

/// Send a CancelLocalTask operation after checking GCS node cache for node liveness.
/// The GCS query is done because we don't store the address of the raylet in the task
/// submission path. Since it's only needed in cancellation, we check the pubsub cache
/// (and the GCS if it's not in the cache which is rare) instead of polluting the hot
/// path.
///
/// \param gcs_client GCS client to query node information.
/// \param node_id The local node ID of where the task is executing on
/// \param cancel_callback Callback containing CancelLocalTask RPC to invoke if the node
/// is alive.
/// \param failure_callback Callback invoked when CancelLocalTask RPC cannot be sent.
/// Used for cleanup.
inline void SendCancelLocalTask(std::shared_ptr<gcs::GcsClient> gcs_client,
                                const NodeID &node_id,
                                std::function<void(const rpc::Address &)> cancel_callback,
                                std::function<void()> failure_callback) {
  // Check GCS node cache. If node info is not in the cache, query the GCS instead.
  auto node_info =
      gcs_client->Nodes().GetNodeAddressAndLiveness(node_id,
                                                    /*filter_dead_nodes=*/false);
  if (!node_info) {
    gcs_client->Nodes().AsyncGetAllNodeAddressAndLiveness(
        [cancel_callback = std::move(cancel_callback),
         failure_callback = std::move(failure_callback),
         node_id](const Status &status,
                  std::vector<rpc::GcsNodeAddressAndLiveness> &&nodes) mutable {
          if (!status.ok()) {
            RAY_LOG(INFO) << "Failed to get node info from GCS";
            failure_callback();
            return;
          }
          if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
            RAY_LOG(INFO).WithField(node_id)
                << "Not sending CancelLocalTask because node is dead";
            failure_callback();
            return;
          }
          auto raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
              node_id, nodes[0].node_manager_address(), nodes[0].node_manager_port());
          cancel_callback(raylet_address);
        },
        -1,
        {node_id});
    return;
  }
  if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
    RAY_LOG(INFO).WithField(node_id)
        << "Not sending CancelLocalTask because node is dead";
    failure_callback();
    return;
  }
  auto raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
      node_id, node_info->node_manager_address(), node_info->node_manager_port());
  cancel_callback(raylet_address);
}

}  // namespace core
}  // namespace ray
