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
#include "ray/raylet/scheduling/policy/scheduling_context.h"

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
  NODE_AFFINITY = 3,
  BUNDLE_PACK = 4,
  BUNDLE_SPREAD = 5,
  BUNDLE_STRICT_PACK = 6,
  BUNDLE_STRICT_SPREAD = 7,
  AFFINITY_WITH_BUNDLE = 8
};

// Options that controls the scheduling behavior.
struct SchedulingOptions {
  static SchedulingOptions Random() {
    return SchedulingOptions(SchedulingType::RANDOM,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false);
  }

  // construct option for spread scheduling policy.
  static SchedulingOptions Spread(bool avoid_local_node, bool require_node_available) {
    return SchedulingOptions(SchedulingType::SPREAD,
                             /*spread_threshold*/ 0,
                             avoid_local_node,
                             require_node_available,
                             RayConfig::instance().scheduler_avoid_gpu_nodes());
  }

  // construct option for hybrid scheduling policy.
  static SchedulingOptions Hybrid(bool avoid_local_node, bool require_node_available) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             RayConfig::instance().scheduler_avoid_gpu_nodes());
  }

  static SchedulingOptions NodeAffinity(bool avoid_local_node,
                                        bool require_node_available,
                                        std::string node_id,
                                        bool soft) {
    SchedulingOptions scheduling_options =
        Hybrid(avoid_local_node, require_node_available);
    scheduling_options.scheduling_type = SchedulingType::NODE_AFFINITY;
    scheduling_options.node_affinity_node_id = node_id;
    scheduling_options.node_affinity_soft = soft;
    return scheduling_options;
  }

  // construct option for affinity with bundle scheduling policy.
  static SchedulingOptions AffinityWithBundle(const BundleID &bundle_id) {
    auto scheduling_context =
        std::make_unique<AffinityWithBundleSchedulingContext>(bundle_id);
    return SchedulingOptions(
        SchedulingType::AFFINITY_WITH_BUNDLE,
        /*spread_threshold*/ 0,
        /*avoid_local_node*/ false,
        /*require_node_available*/ true,
        /*avoid_gpu_nodes*/ RayConfig::instance().scheduler_avoid_gpu_nodes(),
        /*max_cpu_fraction_per_node*/ 0,
        std::move(scheduling_context));
  }

  /*
   * Bundle scheduling options.
   */

  // construct option for soft pack scheduling policy.
  static SchedulingOptions BundlePack(double max_cpu_fraction_per_node = 1.0) {
    return SchedulingOptions(SchedulingType::BUNDLE_PACK,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false,
                             /*max_cpu_fraction_per_node*/ max_cpu_fraction_per_node);
  }

  // construct option for strict spread scheduling policy.
  static SchedulingOptions BundleSpread(double max_cpu_fraction_per_node = 1.0) {
    return SchedulingOptions(SchedulingType::BUNDLE_SPREAD,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false,
                             /*max_cpu_fraction_per_node*/ max_cpu_fraction_per_node);
  }

  // construct option for strict pack scheduling policy.
  static SchedulingOptions BundleStrictPack(double max_cpu_fraction_per_node = 1.0) {
    return SchedulingOptions(SchedulingType::BUNDLE_STRICT_PACK,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false,
                             /*max_cpu_fraction_per_node*/ max_cpu_fraction_per_node);
  }

  // construct option for strict spread scheduling policy.
  static SchedulingOptions BundleStrictSpread(
      double max_cpu_fraction_per_node = 1.0,
      std::unique_ptr<SchedulingContext> scheduling_context = nullptr) {
    return SchedulingOptions(SchedulingType::BUNDLE_STRICT_SPREAD,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false,
                             /*max_cpu_fraction_per_node*/ max_cpu_fraction_per_node,
                             /*scheduling_context*/ std::move(scheduling_context));
  }

  SchedulingType scheduling_type;
  float spread_threshold;
  bool avoid_local_node;
  bool require_node_available;
  bool avoid_gpu_nodes;
  // Maximum reservable CPU fraction per node. It is applied across multiple
  // bundles, individually. E.g., when you have 2 bundles {CPU: 4} from 2 different
  // scheduilng request, and there's one node with {CPU: 8}, only 1 bundle from 1 request
  // can be scheduled on this node. This is only used for bundle scheduling policies
  // (bundle pack, spread).
  double max_cpu_fraction_per_node;
  std::shared_ptr<SchedulingContext> scheduling_context;
  std::string node_affinity_node_id;
  bool node_affinity_soft = false;

 private:
  SchedulingOptions(SchedulingType type,
                    float spread_threshold,
                    bool avoid_local_node,
                    bool require_node_available,
                    bool avoid_gpu_nodes,
                    double max_cpu_fraction_per_node = 1.0,
                    std::shared_ptr<SchedulingContext> scheduling_context = nullptr)
      : scheduling_type(type),
        spread_threshold(spread_threshold),
        avoid_local_node(avoid_local_node),
        require_node_available(require_node_available),
        avoid_gpu_nodes(avoid_gpu_nodes),
        max_cpu_fraction_per_node(max_cpu_fraction_per_node),
        scheduling_context(std::move(scheduling_context)) {}

  friend class ::ray::raylet::SchedulingPolicyTest;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
