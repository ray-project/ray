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

namespace ray {
namespace core {

/// Post a CancelLocalTask operation after checking GCS node cache for node liveness.
/// The reason we query the GCS is that we don't store the address of the raylet in the
/// task submission path. Since it's only needed in cancellation, we query the GCS if it's
/// needed rather than pollute the hot path.
///
/// \param gcs_client GCS client to query node information.
/// \param io_service IO service to post the cancel operation to.
/// \param node_id The local node ID of where the task is executing on
/// \param cancel_callback Callback containing CancelLocalTask RPC to invoke if the node
/// is alive.
inline void PostCancelLocalTask(
    std::shared_ptr<gcs::GcsClient> gcs_client,
    instrumented_io_context &io_service,
    const NodeID &node_id,
    std::function<void(const rpc::GcsNodeAddressAndLiveness &)> cancel_callback,
    const std::string &operation_name = "") {
  // Cancel can execute on the user's python thread, but the GCS node cache is updated on
  // the io service thread and is not thread-safe. Hence we need to post the entire
  // cache access to the io service thread.
  io_service.post(
      [gcs_client, cancel_callback = std::move(cancel_callback), node_id]() mutable {
        // Check GCS node cache. If node info is not in the cache, query the GCS instead.
        auto *node_info =
            gcs_client->Nodes().GetNodeAddressAndLiveness(node_id,
                                                          /*filter_dead_nodes=*/false);
        if (node_info == nullptr) {
          gcs_client->Nodes().AsyncGetAllNodeAddressAndLiveness(
              [cancel_callback = std::move(cancel_callback), node_id](
                  const Status &status,
                  std::vector<rpc::GcsNodeAddressAndLiveness> &&nodes) mutable {
                if (!status.ok()) {
                  RAY_LOG(INFO) << "Failed to get node info from GCS";
                  return;
                }
                if (nodes.empty() || nodes[0].state() != rpc::GcsNodeInfo::ALIVE) {
                  RAY_LOG(INFO).WithField(node_id)
                      << "Not sending CancelLocalTask because node is dead";
                  return;
                }
                cancel_callback(nodes[0]);
              },
              -1,
              {node_id});
          return;
        }
        if (node_info->state() == rpc::GcsNodeInfo::DEAD) {
          RAY_LOG(INFO).WithField(node_id)
              << "Not sending CancelLocalTask because node is dead";
          return;
        }
        cancel_callback(*node_info);
      },
      operation_name);
}

}  // namespace core
}  // namespace ray
