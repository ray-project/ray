// Copyright 2024 The Ray Authors.
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

#include <vector>

#pragma once

#include <vector>

#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/bundle_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/filters.h"
#include "ray/raylet/scheduling/policy/finalizers.h"
#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/node_affinity_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/node_label_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/random_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/scheduling_interface.h"
#include "ray/raylet/scheduling/policy/spread_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

// A task in the scheduling process.
class SchedulingTask {
 public:
  const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view;
  const ResourceRequest &resource_request;
  std::vector<scheduling::NodeID> candidate_nodes;

  SchedulingTask(const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
                 const ResourceRequest &resource_request,
                 const std::vector<scheduling::NodeID> &candidate_nodes)
      : all_nodes_view(all_nodes_view),
        resource_request(resource_request),
        candidate_nodes(candidate_nodes) {}

  SchedulingTask &Filter(const ISchedulingFilter &filter) {
    candidate_nodes = filter.Filter(all_nodes_view, candidate_nodes, resource_request);
    return *this;
  }

  SchedulingTask &FilterIf(bool should_filter, const ISchedulingFilter &filter) {
    if (should_filter) {
      candidate_nodes = filter.Filter(all_nodes_view, candidate_nodes, resource_request);
    }
    return *this;
  }

  scheduling::NodeID Finalize(ISchedulingFinalizer &finalizer) {
    return finalizer.Finalize(all_nodes_view, candidate_nodes, resource_request);
  }
};

// TODO: experimental code.
class CompositeScheduler2 {
 public:
  CompositeScheduler2(std::function<bool(scheduling::NodeID)> is_node_available,
                      scheduling::NodeID local_node_id,
                      bool is_local_node_with_raylet)
      : is_node_available_(is_node_available),
        local_node_id_(local_node_id),
        is_local_node_with_raylet_(is_local_node_with_raylet) {}

  scheduling::NodeID Schedule(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const rpc::SchedulingStrategy &scheduling_strategy,
      const ResourceRequest &resource_request,
      bool force_spillback,  // what's this?
      const std::string &preferred_node_id) {
    std::vector<scheduling::NodeID> candidate_nodes;
    for (const auto &node : all_nodes_view) {
      candidate_nodes.push_back(node.first);
    }

    // TODO: 0 CPU case.
    if (scheduling_strategy.scheduling_strategy_case() ==
        rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy) {
      return ScheduleSpread(all_nodes_view,
                            candidate_nodes,
                            resource_request,
                            /*avoid_local_node=*/force_spillback,
                            /*require_node_available=*/force_spillback);
    } else if (scheduling_strategy.scheduling_strategy_case() ==
               rpc::SchedulingStrategy::SchedulingStrategyCase::
                   kNodeAffinitySchedulingStrategy) {
      return ScheduleNodeAffinity(
          all_nodes_view,
          candidate_nodes,
          resource_request,
          /*avoid_local_node=*/force_spillback,
          /*require_node_available=*/force_spillback,
          scheduling::NodeID(
              scheduling_strategy.node_affinity_scheduling_strategy().node_id()),
          scheduling_strategy.node_affinity_scheduling_strategy().soft(),
          scheduling_strategy.node_affinity_scheduling_strategy().spill_on_unavailable(),
          scheduling_strategy.node_affinity_scheduling_strategy().fail_on_unavailable());
    } else if (scheduling_strategy.scheduling_strategy_case() ==
                   rpc::SchedulingStrategy::SchedulingStrategyCase::
                       kPlacementGroupSchedulingStrategy &&
               (!scheduling_strategy.placement_group_scheduling_strategy()
                     .placement_group_id()
                     .empty()) &&
               !is_local_node_with_raylet_) {
      // If strategy == placement group scheduling strategy and
      // placement group id is not empty and
      // local node has NO raylet (i.e. this is GCS).
      auto placement_group_id = PlacementGroupID::FromBinary(
          scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
      BundleID bundle_id =
          std::pair(placement_group_id,
                    scheduling_strategy.placement_group_scheduling_strategy()
                        .placement_group_bundle_index());
      return ScheduleBundle(all_nodes_view, candidate_nodes, resource_request, bundle_id);
    } else if (scheduling_strategy.has_node_label_scheduling_strategy()) {
      return ScheduleNodeLabel(all_nodes_view,
                               candidate_nodes,
                               resource_request,
                               scheduling_strategy.node_label_scheduling_strategy());
    } else {
      return ScheduleHybrid(all_nodes_view,
                            candidate_nodes,
                            resource_request,
                            /*avoid_local_node=*/force_spillback,
                            /*require_node_available=*/force_spillback,
                            preferred_node_id);
    }
  }

  scheduling::NodeID ScheduleSpread(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request,
      bool avoid_local_node,
      bool require_node_available) {
    SchedulingTask task{all_nodes_view, resource_request, candidate_nodes};

    return task.Filter(AliveAndFeasibleSchedulingFilter(is_node_available_))
        .FilterIf(avoid_local_node, AvoidNodeIdSchedulingFilter(local_node_id_))
        .FilterIf(require_node_available, AvailableSchedulingFilter())
        .Finalize(round_robin_finalizer_);
  }

  scheduling::NodeID ScheduleNodeAffinity(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request,
      bool avoid_local_node,
      bool require_node_available,
      scheduling::NodeID node_id,
      bool soft,
      bool spill_on_unavailable,
      bool fail_on_unavailable) {
    std::vector<scheduling::NodeID> target_nodes = {node_id};
    SchedulingTask one_node_task{all_nodes_view, resource_request, target_nodes};
    auto result =
        one_node_task.Filter(AliveAndFeasibleSchedulingFilter(is_node_available_))
            .FilterIf(spill_on_unavailable || fail_on_unavailable,
                      AvailableSchedulingFilter())
            .Finalize(first_finalizer_);
    if (!result.IsNil()) {
      return result;
    }
    if (!soft) {
      return scheduling::NodeID::Nil();
    }
    return ScheduleHybrid();
  }

  scheduling::NodeID ScheduleBundle(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request,
      const BundleID &bundle_id) {
    RAY_CHECK(false) << "coming soon, get implemented by labels.";
  }

  scheduling::NodeID ScheduleNodeLabel(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request,
      const rpc::NodeLabelSchedulingStrategy &strategy) {
    SchedulingTask task{all_nodes_view, resource_request, candidate_nodes};
    task.Filter(AliveAndFeasibleSchedulingFilter(is_node_available_))
        .Filter(NodeLabelHardSchedulingFilter(strategy.hard()));

    // Try to get only availables.
    SchedulingTask available_task{all_nodes_view, resource_request, task.candidate_nodes};
    const auto available_result = available_task.Filter(AvailableSchedulingFilter())
                                      .Filter(OptionalSchedulingFilter(
                                          NodeLabelHardSchedulingFilter(strategy.soft())))
                                      .Finalize(random_finalizer_);
    if (!available_result.IsNil()) {
      return available_result;
    }
    // No availables, fall back to feasibles.
    return task
        .Filter(OptionalSchedulingFilter(NodeLabelHardSchedulingFilter(strategy.soft())))
        .Finalize(random_finalizer_);
  }

  scheduling::NodeID ScheduleHybrid(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request,
      bool avoid_local_node,
      bool require_node_available,
      const std::string &preferred_node_id) {
    SchedulingTask task{all_nodes_view, resource_request, candidate_nodes};
    return task.Filter(AliveAndFeasibleSchedulingFilter(is_node_available_))
        .FilterIf(avoid_local_node, AvoidNodeIdSchedulingFilter(local_node_id_))
        .FilterIf(require_node_available, AvailableSchedulingFilter())
        .Finalize(random_finalizer_);
  }

 private:
  std::function<bool(scheduling::NodeID)> is_node_available_;
  const scheduling::NodeID local_node_id_;
  const bool is_local_node_with_raylet_;

  FirstSchedulingFinalizer first_finalizer_;
  RoundRobinSchedulingFinalizer round_robin_finalizer_;
  RandomSchedulingFinalizer random_finalizer_;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray
