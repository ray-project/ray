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
}  // namespace raylet
namespace raylet_scheduling_policy {

class HybridSchedulingPolicyTest;

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
  AFFINITY_WITH_BUNDLE = 8,
  NODE_LABEL = 9
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
  static SchedulingOptions Hybrid(bool avoid_local_node,
                                  bool require_node_available,
                                  const std::string &preferred_node_id = std::string()) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             RayConfig::instance().scheduler_avoid_gpu_nodes(),
                             /*scheduling_context*/ nullptr,
                             preferred_node_id);
  }

  static SchedulingOptions NodeAffinity(bool avoid_local_node,
                                        bool require_node_available,
                                        std::string node_id,
                                        bool soft,
                                        bool spill_on_unavailable = false,
                                        bool fail_on_unavailable = false) {
    if (spill_on_unavailable) {
      RAY_CHECK(soft) << "spill_on_unavailable only works with soft == true";
    }
    if (fail_on_unavailable) {
      RAY_CHECK(!soft) << "fail_on_unavailable only works with soft == false";
    }
    SchedulingOptions scheduling_options =
        Hybrid(avoid_local_node, require_node_available);
    scheduling_options.scheduling_type_ = SchedulingType::NODE_AFFINITY;
    scheduling_options.node_affinity_node_id_ = node_id;
    scheduling_options.node_affinity_soft_ = soft;
    scheduling_options.node_affinity_spill_on_unavailable_ = spill_on_unavailable;
    scheduling_options.node_affinity_fail_on_unavailable_ = fail_on_unavailable;
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
        std::move(scheduling_context));
  }

  static SchedulingOptions NodeLabelScheduling(
      const rpc::SchedulingStrategy &scheduling_strategy) {
    auto scheduling_context =
        std::make_unique<NodeLabelSchedulingContext>(scheduling_strategy);
    return SchedulingOptions(
        SchedulingType::NODE_LABEL,
        /*spread_threshold*/ 0,
        /*avoid_local_node*/ false,
        /*require_node_available*/ true,
        /*avoid_gpu_nodes*/ RayConfig::instance().scheduler_avoid_gpu_nodes(),
        std::move(scheduling_context));
  }
  /*
   * Bundle scheduling options.
   */

  // construct option for soft pack scheduling policy.
  static SchedulingOptions BundlePack() {
    return SchedulingOptions(SchedulingType::BUNDLE_PACK,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false);
  }

  // construct option for strict spread scheduling policy.
  static SchedulingOptions BundleSpread() {
    return SchedulingOptions(SchedulingType::BUNDLE_SPREAD,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false);
  }

  // construct option for strict pack scheduling policy.
  static SchedulingOptions BundleStrictPack(
      scheduling::NodeID soft_target_node_id = scheduling::NodeID::Nil()) {
    SchedulingOptions scheduling_options =
        SchedulingOptions(SchedulingType::BUNDLE_STRICT_PACK,
                          /*spread_threshold*/ 0,
                          /*avoid_local_node*/ false,
                          /*require_node_available*/ true,
                          /*avoid_gpu_nodes*/ false);
    scheduling_options.bundle_strict_pack_soft_target_node_id_ = soft_target_node_id;
    return scheduling_options;
  }

  // construct option for strict spread scheduling policy.
  static SchedulingOptions BundleStrictSpread(
      std::unique_ptr<SchedulingContext> scheduling_context = nullptr) {
    return SchedulingOptions(SchedulingType::BUNDLE_STRICT_SPREAD,
                             /*spread_threshold*/ 0,
                             /*avoid_local_node*/ false,
                             /*require_node_available*/ true,
                             /*avoid_gpu_nodes*/ false,
                             /*scheduling_context*/ std::move(scheduling_context));
  }

  SchedulingType scheduling_type_;
  float spread_threshold_;
  bool avoid_local_node_;
  bool require_node_available_;
  bool avoid_gpu_nodes_;
  // ID of the target node where bundles should be placed
  // iff the target node has enough available resources.
  // Otherwise, the bundles can be placed elsewhere.
  // This is only used by PG STRICT_PACK scheduling.
  scheduling::NodeID bundle_strict_pack_soft_target_node_id_ = scheduling::NodeID::Nil();
  std::shared_ptr<SchedulingContext> scheduling_context_;
  std::string node_affinity_node_id_;
  bool node_affinity_soft_ = false;
  bool node_affinity_spill_on_unavailable_ = false;
  bool node_affinity_fail_on_unavailable_ = false;
  // The node where the task is preferred to be placed. By default, this node id
  // is empty, which means no preferred node.
  std::string preferred_node_id_;
  int32_t schedule_top_k_absolute_;
  float scheduler_top_k_fraction_;

 private:
  SchedulingOptions(
      SchedulingType type,
      float spread_threshold,
      bool avoid_local_node,
      bool require_node_available,
      bool avoid_gpu_nodes,
      std::shared_ptr<SchedulingContext> scheduling_context = nullptr,
      const std::string &preferred_node_id = std::string(),
      int32_t schedule_top_k_absolute = RayConfig::instance().scheduler_top_k_absolute(),
      float scheduler_top_k_fraction = RayConfig::instance().scheduler_top_k_fraction())
      : scheduling_type_(type),
        spread_threshold_(spread_threshold),
        avoid_local_node_(avoid_local_node),
        require_node_available_(require_node_available),
        avoid_gpu_nodes_(avoid_gpu_nodes),
        scheduling_context_(std::move(scheduling_context)),
        preferred_node_id_(preferred_node_id),
        schedule_top_k_absolute_(schedule_top_k_absolute),
        scheduler_top_k_fraction_(scheduler_top_k_fraction) {}

  friend class ::ray::raylet::SchedulingPolicyTest;
  friend class HybridSchedulingPolicyTest;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
