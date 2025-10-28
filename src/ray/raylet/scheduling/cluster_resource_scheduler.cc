// Copyright 2017 The Ray Authors.
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

#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

#include <memory>
#include <string>
#include <vector>

namespace ray {

using namespace ::ray::raylet_scheduling_policy;  // NOLINT

ClusterResourceScheduler::ClusterResourceScheduler(
    instrumented_io_context &io_service,
    scheduling::NodeID local_node_id,
    const NodeResources &local_node_resources,
    std::function<bool(scheduling::NodeID)> is_node_available_fn,
    bool is_local_node_with_raylet)
    : local_node_id_(local_node_id),
      is_node_available_fn_(is_node_available_fn),
      is_local_node_with_raylet_(is_local_node_with_raylet) {
  Init(io_service,
       local_node_resources,
       /*get_used_object_store_memory=*/nullptr,
       /*get_pull_manager_at_capacity=*/nullptr,
       /*shutdown_raylet_gracefully=*/nullptr);
}

ClusterResourceScheduler::ClusterResourceScheduler(
    instrumented_io_context &io_service,
    scheduling::NodeID local_node_id,
    const absl::flat_hash_map<std::string, double> &local_node_resources,
    std::function<bool(scheduling::NodeID)> is_node_available_fn,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
    const absl::flat_hash_map<std::string, std::string> &local_node_labels)
    : local_node_id_(local_node_id), is_node_available_fn_(is_node_available_fn) {
  NodeResources node_resources = ResourceMapToNodeResources(
      local_node_resources, local_node_resources, local_node_labels);
  Init(io_service,
       node_resources,
       get_used_object_store_memory,
       get_pull_manager_at_capacity,
       shutdown_raylet_gracefully);
}

void ClusterResourceScheduler::Init(
    instrumented_io_context &io_service,
    const NodeResources &local_node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully) {
  cluster_resource_manager_ = std::make_unique<ClusterResourceManager>(io_service);
  local_resource_manager_ = std::make_unique<LocalResourceManager>(
      local_node_id_,
      local_node_resources,
      get_used_object_store_memory,
      get_pull_manager_at_capacity,
      shutdown_raylet_gracefully,
      [this](const NodeResources &local_resource_update) {
        cluster_resource_manager_->AddOrUpdateNode(local_node_id_, local_resource_update);
      });
  RAY_CHECK(!local_node_id_.IsNil());
  cluster_resource_manager_->AddOrUpdateNode(local_node_id_, local_node_resources);
  scheduling_policy_ =
      std::make_unique<raylet_scheduling_policy::CompositeSchedulingPolicy>(
          local_node_id_,
          *cluster_resource_manager_,
          /*is_node_available_fn*/
          [this](auto node_id) { return this->NodeAvailable(node_id); });
  bundle_scheduling_policy_ =
      std::make_unique<raylet_scheduling_policy::CompositeBundleSchedulingPolicy>(
          *cluster_resource_manager_,
          /*is_node_available_fn*/
          [this](auto node_id) { return this->NodeAvailable(node_id); });
}

bool ClusterResourceScheduler::NodeAvailable(scheduling::NodeID node_id) const {
  if (node_id == local_node_id_) {
    if (!is_local_node_with_raylet_) {
      return false;
    } else {
      return !local_resource_manager_->IsLocalNodeDraining();
    }
  }

  if (node_id.IsNil()) {
    return false;
  }

  RAY_CHECK(is_node_available_fn_ != nullptr);
  if (!is_node_available_fn_(node_id) ||
      cluster_resource_manager_->IsNodeDraining(node_id)) {
    return false;
  }

  return true;
}

bool ClusterResourceScheduler::IsSchedulable(const ResourceRequest &resource_request,
                                             scheduling::NodeID node_id) const {
  // It's okay if the local node's pull manager is at capacity because we
  // will eventually spill the task back from the waiting queue if its args
  // cannot be pulled.
  return cluster_resource_manager_->HasAvailableResources(
             node_id,
             resource_request,
             /*ignore_object_store_memory_requirement*/ node_id == local_node_id_) &&
         NodeAvailable(node_id);
}

namespace {
bool IsHardNodeAffinitySchedulingStrategy(
    const rpc::SchedulingStrategy &scheduling_strategy) {
  return scheduling_strategy.scheduling_strategy_case() ==
             rpc::SchedulingStrategy::SchedulingStrategyCase::
                 kNodeAffinitySchedulingStrategy &&
         !scheduling_strategy.node_affinity_scheduling_strategy().soft();
}
}  // namespace

bool ClusterResourceScheduler::IsAffinityWithBundleSchedule(
    const rpc::SchedulingStrategy &scheduling_strategy) {
  return scheduling_strategy.scheduling_strategy_case() ==
             rpc::SchedulingStrategy::SchedulingStrategyCase::
                 kPlacementGroupSchedulingStrategy &&
         (!scheduling_strategy.placement_group_scheduling_strategy()
               .placement_group_id()
               .empty());
}

scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request,
    const rpc::SchedulingStrategy &scheduling_strategy,
    bool actor_creation,
    bool force_spillback,
    const std::string &preferred_node_id,
    int64_t *total_violations,
    bool *is_infeasible) {
  // The zero cpu actor is a special case that must be handled the same way by all
  // scheduling policies, except for HARD node affnity scheduling policy.
  if (actor_creation && resource_request.IsEmpty() &&
      !IsHardNodeAffinitySchedulingStrategy(scheduling_strategy)) {
    return scheduling_policy_->Schedule(resource_request, SchedulingOptions::Random());
  }

  auto best_node_id = scheduling::NodeID::Nil();
  if (scheduling_strategy.scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy) {
    best_node_id =
        scheduling_policy_->Schedule(resource_request,
                                     SchedulingOptions::Spread(
                                         /*avoid_local_node*/ force_spillback,
                                         /*require_node_available*/ force_spillback));
  } else if (scheduling_strategy.scheduling_strategy_case() ==
             rpc::SchedulingStrategy::SchedulingStrategyCase::
                 kNodeAffinitySchedulingStrategy) {
    best_node_id = scheduling_policy_->Schedule(
        resource_request,
        SchedulingOptions::NodeAffinity(
            force_spillback,
            force_spillback,
            scheduling_strategy.node_affinity_scheduling_strategy().node_id(),
            scheduling_strategy.node_affinity_scheduling_strategy().soft(),
            scheduling_strategy.node_affinity_scheduling_strategy()
                .spill_on_unavailable(),
            scheduling_strategy.node_affinity_scheduling_strategy()
                .fail_on_unavailable()));
  } else if (IsAffinityWithBundleSchedule(scheduling_strategy) &&
             !is_local_node_with_raylet_) {
    // This scheduling strategy is only used for gcs scheduling for the time being.
    auto placement_group_id = PlacementGroupID::FromBinary(
        scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
    BundleID bundle_id =
        std::pair(placement_group_id,
                  scheduling_strategy.placement_group_scheduling_strategy()
                      .placement_group_bundle_index());
    best_node_id = scheduling_policy_->Schedule(
        resource_request, SchedulingOptions::AffinityWithBundle(bundle_id));
  } else if (scheduling_strategy.has_node_label_scheduling_strategy()) {
    best_node_id = scheduling_policy_->Schedule(
        resource_request, SchedulingOptions::NodeLabelScheduling(scheduling_strategy));
  } else {
    // TODO(Alex): Setting require_available == force_spillback is a hack in order to
    // remain bug compatible with the legacy scheduling algorithms.
    best_node_id =
        scheduling_policy_->Schedule(resource_request,
                                     SchedulingOptions::Hybrid(
                                         /*avoid_local_node*/ force_spillback,
                                         /*require_node_available*/ force_spillback,
                                         preferred_node_id));
  }

  *is_infeasible = best_node_id.IsNil();
  if (!*is_infeasible) {
    // TODO(Alex): Support soft constraints if needed later.
    *total_violations = 0;
  }

  RAY_LOG(DEBUG) << "Scheduling decision. "
                 << "forcing spillback: " << force_spillback
                 << ". Best node: " << best_node_id.ToInt() << " "
                 << (best_node_id.IsNil() ? NodeID::Nil()
                                          : NodeID::FromBinary(best_node_id.Binary()))
                 << ", is infeasible: " << *is_infeasible;
  return best_node_id;
}

scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const absl::flat_hash_map<std::string, double> &task_resources,
    const LabelSelector &label_selector,
    const rpc::SchedulingStrategy &scheduling_strategy,
    bool requires_object_store_memory,
    bool actor_creation,
    bool force_spillback,
    const std::string &preferred_node_id,
    int64_t *total_violations,
    bool *is_infeasible) {
  ResourceRequest resource_request =
      ResourceMapToResourceRequest(task_resources, requires_object_store_memory);
  resource_request.SetLabelSelector(label_selector);
  return GetBestSchedulableNode(resource_request,
                                scheduling_strategy,
                                actor_creation,
                                force_spillback,
                                preferred_node_id,
                                total_violations,
                                is_infeasible);
}

bool ClusterResourceScheduler::SubtractRemoteNodeAvailableResources(
    scheduling::NodeID node_id, const ResourceRequest &resource_request) {
  RAY_CHECK(node_id != local_node_id_);

  // Just double check this node can still schedule the resource request.
  if (!IsSchedulable(resource_request, node_id)) {
    return false;
  }
  return cluster_resource_manager_->SubtractNodeAvailableResources(node_id,
                                                                   resource_request);
}

std::string ClusterResourceScheduler::DebugString(void) const {
  std::stringstream buffer;
  buffer << "\nLocal id: " << local_node_id_.ToInt();
  buffer << " Local resources: " << local_resource_manager_->DebugString();
  buffer << " Cluster resources (at most 20 nodes are shown): "
         << cluster_resource_manager_->DebugString(/*max_num_nodes_to_include=*/20);
  return buffer.str();
}

bool ClusterResourceScheduler::AllocateRemoteTaskResources(
    scheduling::NodeID node_id,
    const absl::flat_hash_map<std::string, double> &task_resources) {
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      task_resources, /*requires_object_store_memory=*/false);
  RAY_CHECK(node_id != local_node_id_);
  return SubtractRemoteNodeAvailableResources(node_id, resource_request);
}

bool ClusterResourceScheduler::IsSchedulableOnNode(
    scheduling::NodeID node_id,
    const absl::flat_hash_map<std::string, double> &shape,
    const LabelSelector &label_selector,
    bool requires_object_store_memory) {
  auto resource_request =
      ResourceMapToResourceRequest(shape, requires_object_store_memory);
  resource_request.SetLabelSelector(label_selector);

  return IsSchedulable(resource_request, node_id);
}

scheduling::NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const LeaseSpecification &lease_spec,
    const std::string &preferred_node_id,
    bool exclude_local_node,
    bool requires_object_store_memory,
    bool *is_infeasible) {
  // This argument is used to set violation, which is an unsupported feature now.
  int64_t _unused;

  // Construct list of references to all LabelSelectors, from both the `label_selector`
  // and `fallback_strategy` arguments.
  std::vector<std::reference_wrapper<const LabelSelector>> label_selectors;
  label_selectors.push_back(std::cref(lease_spec.GetLabelSelector()));
  const auto &fallback_strategy = lease_spec.GetFallbackStrategy();
  for (const auto &fallback : fallback_strategy) {
    label_selectors.push_back(std::cref(fallback.label_selector));
  }

  scheduling::NodeID highest_priority_unavailable_node = scheduling::NodeID::Nil();
  const LabelSelector *highest_priority_unavailable_label_selector = nullptr;
  bool any_selector_is_feasible = false;

  // Try each label selector in order until a node is found.
  for (const auto &selector_ref : label_selectors) {
    const auto &label_selector = selector_ref.get();

    // If the local node is available, we should directly return it instead of
    // going through the full hybrid policy since we don't want spillback.
    if (preferred_node_id == local_node_id_.Binary() && !exclude_local_node &&
        IsSchedulableOnNode(local_node_id_,
                            lease_spec.GetRequiredPlacementResources().GetResourceMap(),
                            label_selector,
                            requires_object_store_memory)) {
      *is_infeasible = false;
      return local_node_id_;
    }

    // Find the best feasible node.
    bool current_selector_is_infeasible = false;
    scheduling::NodeID best_feasible_node = GetBestSchedulableNode(
        lease_spec.GetRequiredPlacementResources().GetResourceMap(),
        label_selector,
        lease_spec.GetMessage().scheduling_strategy(),
        requires_object_store_memory,
        lease_spec.IsActorCreationTask(),
        exclude_local_node,
        preferred_node_id,
        &_unused,
        &current_selector_is_infeasible);

    if (!best_feasible_node.IsNil()) {
      // A feasible node was found.
      any_selector_is_feasible = true;
      if (IsSchedulableOnNode(best_feasible_node,
                              lease_spec.GetRequiredPlacementResources().GetResourceMap(),
                              label_selector,
                              requires_object_store_memory)) {
        // The node is feasible and available, directly return it.
        *is_infeasible = false;
        return best_feasible_node;
      }

      // If the node is feasible but not available, save the node and label selector
      // but continue to check for the next fallback.
      if (highest_priority_unavailable_node.IsNil()) {
        highest_priority_unavailable_node = best_feasible_node;
        highest_priority_unavailable_label_selector = &label_selector;
      }
    }
  }

  // No feasible nodes were found for scheduling constraints.
  if (!any_selector_is_feasible) {
    *is_infeasible = true;
    return scheduling::NodeID::Nil();
  }

  // If the all best nodes found are not available but the local node is feasible,
  // wait on the local node.
  *is_infeasible = false;
  if ((preferred_node_id == local_node_id_.Binary()) && NodeAvailable(local_node_id_)) {
    auto resource_request = ResourceMapToResourceRequest(
        lease_spec.GetRequiredPlacementResources().GetResourceMap(),
        requires_object_store_memory);

    // Use the label selector from the highest-priority fallback that was feasible.
    // There must be at least one feasible node and selector.
    RAY_CHECK(highest_priority_unavailable_label_selector != nullptr);
    resource_request.SetLabelSelector(*highest_priority_unavailable_label_selector);

    if (cluster_resource_manager_->HasFeasibleResources(local_node_id_,
                                                        resource_request)) {
      return local_node_id_;
    }
  }

  // If the task is being scheduled by gcs, return nil to make it stay in the
  // `cluster_lease_manager`'s queue.
  if (!is_local_node_with_raylet_) {
    return scheduling::NodeID::Nil();
  }

  return highest_priority_unavailable_node;
}

SchedulingResult ClusterResourceScheduler::Schedule(
    const std::vector<const ResourceRequest *> &resource_request_list,
    SchedulingOptions options) {
  return bundle_scheduling_policy_->Schedule(resource_request_list, options);
}

}  // namespace ray
