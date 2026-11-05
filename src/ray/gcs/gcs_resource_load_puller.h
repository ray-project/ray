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

#include <cstdint>
#include <functional>
#include <vector>

#include "absl/container/flat_hash_map.h"
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
///
/// Each pull carries the report version last applied for that raylet, and the
/// raylet answers `unchanged` without the report if it still matches, so an idle
/// cluster costs the main io_context nothing.
class GcsResourceLoadPuller {
 public:
  /// \param apply_on_main Applies a report on the main io_context and returns
  /// whether the consumers took it. A report they dropped (e.g. for a node they
  /// don't know about yet) is pulled in full again next time.
  GcsResourceLoadPuller(instrumented_io_context &pull_io_context,
                        instrumented_io_context &main_io_context,
                        rpc::RayletClientPool &raylet_client_pool,
                        std::function<bool(rpc::ResourcesData)> apply_on_main);

  void Pull(std::vector<rpc::Address> raylet_addresses);

 private:
  instrumented_io_context &pull_io_context_;
  instrumented_io_context &main_io_context_;
  rpc::RayletClientPool &raylet_client_pool_;
  /// Runs on the pull io_context once the main io_context applied the report at
  /// `version` for `node_id`.
  void OnApplied(const NodeID &node_id, int64_t version);

  std::function<bool(rpc::ResourcesData)> apply_on_main_;
  /// Node ids from the last Pull(), diffed against the next snapshot to remove
  /// dead raylets' pooled clients and versions.
  absl::flat_hash_set<NodeID> pulled_node_ids_;
  /// The report version most recently applied on the main io_context, per node.
  /// It is the latest applied rather than the highest, so that a stale report
  /// applied after a newer one is replaced on the next pull.
  absl::flat_hash_map<NodeID, int64_t> applied_versions_;
};

}  // namespace gcs
}  // namespace ray
