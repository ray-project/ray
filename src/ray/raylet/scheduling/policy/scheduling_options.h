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

#include "ray/common/ray_config.h"

namespace ray {
namespace raylet {
class SchedulingPolicyTest;
}
namespace raylet_scheduling_policy {

// Different scheduling types. Please refer to
// scheduling_policy.h to see detailed behaviors.
enum class SchedulingType {
  HYBRID = 0,
  SPREAD = 1,
  RANDOM = 2,
};

// Options that controls the scheduling behavior.
struct SchedulingOptions {
  static SchedulingOptions Random() {
    return SchedulingOptions(SchedulingType::RANDOM,
                             /*spread_threshold*/ 0, /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false);
  }

  // construct option for spread scheduling policy.
  static SchedulingOptions Spread(bool avoid_local_node, bool require_node_available) {
    return SchedulingOptions(SchedulingType::SPREAD,
                             /*spread_threshold*/ 0, avoid_local_node,
                             require_node_available,
                             RayConfig::instance().scheduler_avoid_gpu_nodes());
  }

  // construct option for hybrid scheduling policy.
  static SchedulingOptions Hybrid(bool avoid_local_node, bool require_node_available) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node, require_node_available,
                             RayConfig::instance().scheduler_avoid_gpu_nodes());
  }

  SchedulingType scheduling_type;
  float spread_threshold;
  bool avoid_local_node;
  bool require_node_available;
  bool avoid_gpu_nodes;

 private:
  SchedulingOptions(SchedulingType type, float spread_threshold, bool avoid_local_node,
                    bool require_node_available, bool avoid_gpu_nodes)
      : scheduling_type(type),
        spread_threshold(spread_threshold),
        avoid_local_node(avoid_local_node),
        require_node_available(require_node_available),
        avoid_gpu_nodes(avoid_gpu_nodes) {}

  friend class ::ray::raylet::SchedulingPolicyTest;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
