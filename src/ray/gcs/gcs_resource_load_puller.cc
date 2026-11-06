// Copyright 2026 The Ray Authors.
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

#include "ray/gcs/gcs_resource_load_puller.h"

#include <utility>
#include <vector>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

GcsResourceLoadPuller::GcsResourceLoadPuller(
    instrumented_io_context &pull_io_context,
    instrumented_io_context &main_io_context,
    rpc::RayletClientPool &raylet_client_pool,
    std::function<void(rpc::ResourcesData)> apply_on_main)
    : pull_io_context_(pull_io_context),
      main_io_context_(main_io_context),
      raylet_client_pool_(raylet_client_pool),
      apply_on_main_(std::move(apply_on_main)) {}

void GcsResourceLoadPuller::Pull(std::vector<rpc::Address> raylet_addresses) {
  RAY_CHECK(pull_io_context_.get_executor().running_in_this_thread());
  absl::flat_hash_set<NodeID> current_node_ids;
  current_node_ids.reserve(raylet_addresses.size());
  for (const auto &address : raylet_addresses) {
    current_node_ids.insert(NodeID::FromBinary(address.node_id()));
  }
  for (const auto &node_id : pulled_node_ids_) {
    if (!current_node_ids.contains(node_id)) {
      raylet_client_pool_.Disconnect(node_id);
    }
  }
  pulled_node_ids_ = std::move(current_node_ids);

  for (const auto &address : raylet_addresses) {
    auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(address);
    raylet_client->GetResourceLoad(
        [apply_on_main = apply_on_main_, &main_io_context = main_io_context_](
            const Status &status, rpc::GetResourceLoadReply &&reply) {
          if (!status.ok()) {
            RAY_LOG_EVERY_N(WARNING, 10)
                << "Failed to get the resource load: " << status.ToString();
            return;
          }
          main_io_context.post(
              [apply_on_main,
               resources = std::move(*reply.mutable_resources())]() mutable {
                apply_on_main(std::move(resources));
              },
              "GcsResourceLoadPuller.apply");
        });
  }
}

}  // namespace gcs
}  // namespace ray
