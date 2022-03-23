// Copyright 2021 The Ray Authors.
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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/placement_group.h"

namespace ray {
namespace raylet_scheduling_policy {

// Options that controls the scheduling behavior.
struct SchedulingContext {
  virtual ~SchedulingContext() = default;
};

struct BundleSchedulingContext : public SchedulingContext {
 public:
  explicit BundleSchedulingContext(
      std::shared_ptr<absl::flat_hash_map<NodeID, int64_t>> node_to_bundles,
      const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations)
      : node_to_bundles_(std::move(node_to_bundles)),
        bundle_locations_(bundle_locations) {}

  /// Key is node id, value is the number of bundles on the node.
  const std::shared_ptr<absl::flat_hash_map<NodeID, int64_t>> node_to_bundles_;
  /// The locations of existing bundles for this placement group.
  const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations_;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray