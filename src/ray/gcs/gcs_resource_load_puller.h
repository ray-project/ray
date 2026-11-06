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

#pragma once

#include <functional>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Pulls every raylet's pending resource requests (autoscaler bookkeeping) on
/// a dedicated io_context, and posts each reply back to the main io_context,
/// where the consumers live.
class GcsResourceLoadPuller {
 public:
  GcsResourceLoadPuller(instrumented_io_context &pull_io_context,
                        instrumented_io_context &main_io_context,
                        rpc::RayletClientPool &raylet_client_pool,
                        std::function<void(rpc::ResourcesData)> apply_on_main);

  void Pull(std::vector<rpc::Address> raylet_addresses);

 private:
  instrumented_io_context &pull_io_context_;
  instrumented_io_context &main_io_context_;
  rpc::RayletClientPool &raylet_client_pool_;
  std::function<void(rpc::ResourcesData)> apply_on_main_;
  /// Node ids from the last Pull(), diffed against the next snapshot to remove
  /// dead raylets' pooled clients.
  absl::flat_hash_set<NodeID> pulled_node_ids_;
};

}  // namespace gcs
}  // namespace ray
