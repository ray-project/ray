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

#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsPlacementGroupScheduler::GcsPlacementGroupScheduler(
    boost::asio::io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const gcs::GcsNodeManager &gcs_node_manager,
    ReserveResourceClientFactoryFn lease_client_factory)
    : return_timer_(io_context),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_node_manager_(gcs_node_manager),
      lease_client_factory_(std::move(lease_client_factory)) {
  scheduler_strategies_.push_back(std::make_shared<GcsPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsSpreadStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictSpreadStrategy>());
}

ScheduleMap GcsStrictPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // Aggregate required resources.
  ResourceSet required_resources;
  for (const auto &bundle : bundles) {
    required_resources.AddResources(bundle->GetRequiredResources());
  }

  // Filter candidate nodes.
  const auto &alive_nodes = context->node_manager_.GetClusterRealtimeResources();
  std::vector<std::pair<int64_t, ClientID>> candidate_nodes;
  for (auto &node : alive_nodes) {
    if (required_resources.IsSubset(*node.second)) {
      candidate_nodes.emplace_back((*context->node_to_bundles_)[node.first], node.first);
    }
  }

  // Select the node with the least number of bundles.
  ScheduleMap schedule_map;
  if (candidate_nodes.empty()) {
    return schedule_map;
  }

  std::sort(
      std::begin(candidate_nodes), std::end(candidate_nodes),
      [](const std::pair<int64_t, ClientID> &left,
         const std::pair<int64_t, ClientID> &right) { return left.first < right.first; });

  for (auto &bundle : bundles) {
    schedule_map[bundle->BundleId()] = candidate_nodes.front().second;
  }
  return schedule_map;
}

ScheduleMap GcsPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // The current algorithm is to select a node and deploy as many bundles as possible.
  // First fill up a node. If the node resource is insufficient, select a new node.
  // TODO(ffbin): We will speed this up in next PR. Currently it is a double for loop.
  ScheduleMap schedule_map;
  const auto &alive_nodes = context->node_manager_.GetClusterRealtimeResources();
  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    for (auto &node : alive_nodes) {
      if (required_resources.IsSubset(*node.second)) {
        node.second->SubtractResourcesStrict(required_resources);
        schedule_map[bundle->BundleId()] = node.first;
        break;
      }
    }
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

ScheduleMap GcsSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // When selecting nodes, if you traverse from the beginning each time, a large number of
  // bundles will be deployed to the previous nodes. So we start with the next node of the
  // last selected node.
  ScheduleMap schedule_map;
  const auto &candidate_nodes = context->node_manager_.GetClusterRealtimeResources();
  if (candidate_nodes.empty()) {
    return schedule_map;
  }

  auto iter = candidate_nodes.begin();
  auto iter_begin = iter;
  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    // Traverse all nodes from `iter_begin` to `candidate_nodes.end()` to find a node that
    // meets the resource requirements. `iter_begin` is the next node of the last selected
    // node.
    for (; iter != candidate_nodes.end(); ++iter) {
      if (required_resources.IsSubset(*iter->second)) {
        iter->second->SubtractResourcesStrict(required_resources);
        schedule_map[bundle->BundleId()] = iter->first;
        break;
      }
    }

    // We've traversed all the nodes from `iter_begin` to `candidate_nodes.end()`, but we
    // haven't found one that meets the requirements.
    // If `iter_begin` is `candidate_nodes.begin()`, it means that all nodes are not
    // satisfied, we will return directly. Otherwise, we will traverse the nodes from
    // `candidate_nodes.begin()` to `iter_begin` to find the nodes that meet the
    // requirements.
    if (iter == candidate_nodes.end()) {
      if (iter_begin != candidate_nodes.begin()) {
        // Traverse all the nodes from `candidate_nodes.begin()` to `iter_begin`.
        for (iter = candidate_nodes.begin(); iter != iter_begin; ++iter) {
          if (required_resources.IsSubset(*iter->second)) {
            iter->second->SubtractResourcesStrict(required_resources);
            schedule_map[bundle->BundleId()] = iter->first;
            break;
          }
        }
        if (iter == iter_begin) {
          // We have traversed all the nodes, so return directly.
          break;
        }
      } else {
        // We have traversed all the nodes, so return directly.
        break;
      }
    }
    // NOTE: If `iter == candidate_nodes.end()`, ++iter causes crash.
    iter_begin = ++iter;
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

ScheduleMap GcsStrictSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.
  ScheduleMap schedule_map;
  auto candidate_nodes = context->node_manager_.GetClusterRealtimeResources();

  // The number of bundles is more than the number of nodes, scheduling fails.
  if (bundles.size() > candidate_nodes.size()) {
    return schedule_map;
  }

  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    auto iter = candidate_nodes.begin();
    for (; iter != candidate_nodes.end(); ++iter) {
      if (required_resources.IsSubset(*iter->second)) {
        schedule_map[bundle->BundleId()] = iter->first;
        candidate_nodes.erase(iter);
        break;
      }
    }

    // Node resource is not satisfied, scheduling failed.
    if (iter == candidate_nodes.end()) {
      break;
    }
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

void GcsPlacementGroupScheduler::ScheduleUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> failure_callback,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> success_callback) {
  auto bundles = placement_group->GetUnplacedBundles();
  auto strategy = placement_group->GetStrategy();

  RAY_LOG(INFO) << "Scheduling placement group " << placement_group->GetName()
                << ", bundles size = " << bundles.size();
  auto selected_nodes = scheduler_strategies_[strategy]->Schedule(
      bundles, GetScheduleContext(placement_group->GetPlacementGroupID()));

  // If no nodes are available, scheduling fails.
  if (selected_nodes.empty()) {
    RAY_LOG(WARNING) << "Failed to schedule placement group "
                     << placement_group->GetName() << ", because no nodes are available.";
    failure_callback(placement_group);
    return;
  }

  // If schedule success, the decision will be set as schedule_map[bundles[pos]]
  // else will be set ClientID::Nil().
  auto bundle_locations = std::make_shared<BundleLocations>();
  // To count how many scheduler have been return, which include success and failure.
  auto finished_count = std::make_shared<size_t>();
  RAY_CHECK(
      placement_group_leasing_in_progress_.emplace(placement_group->GetPlacementGroupID())
          .second);

  /// TODO(AlisaWu): Change the strategy when reserve resource failed.
  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    const auto &node_id = selected_nodes[bundle_id];
    RAY_CHECK(node_to_bundles_when_leasing_[node_id].emplace(bundle_id).second);

    ReserveResourceFromNode(
        bundle, gcs_node_manager_.GetNode(node_id),
        [this, bundle_id, bundle, bundles, node_id, placement_group, bundle_locations,
         finished_count, failure_callback, success_callback](const Status &status) {
          auto leasing_bundles = node_to_bundles_when_leasing_.find(node_id);
          RAY_CHECK(leasing_bundles != node_to_bundles_when_leasing_.end());
          auto bundle_iter = leasing_bundles->second.find(bundle->BundleId());
          RAY_CHECK(bundle_iter != leasing_bundles->second.end());
          // Remove the bundle from the leasing map as the reply is returned from the
          // remote node.
          leasing_bundles->second.erase(bundle_iter);
          if (leasing_bundles->second.empty()) {
            node_to_bundles_when_leasing_.erase(leasing_bundles);
          }

          if (status.ok()) {
            (*bundle_locations)[bundle_id] = std::make_pair(node_id, bundle);
          }

          if (++(*finished_count) == bundles.size()) {
            OnAllBundleSchedulingRequestReturned(placement_group, bundles,
                                                 bundle_locations, failure_callback,
                                                 success_callback);
          }
        });
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupBundleResourcesIfExists(
    const PlacementGroupID &placement_group_id) {
  const auto &maybe_bundle_locations =
      bundle_location_index_.GetBundleLocations(placement_group_id);
  // If bundle location has been already removed, it means bundles
  // are already destroyed. Do nothing.
  if (!maybe_bundle_locations.has_value()) {
    return;
  }

  const auto &bundle_locations = maybe_bundle_locations.value();
  // Cancel all resource reservation.
  for (const auto &iter : *(bundle_locations)) {
    auto &bundle_spec = iter.second.second;
    auto &node_id = iter.second.first;
    CancelResourceReserve(bundle_spec, gcs_node_manager_.GetNode(node_id));
  }
  bundle_location_index_.Erase(placement_group_id);
}

void GcsPlacementGroupScheduler::MarkScheduleCancelled(
    const PlacementGroupID &placement_group_id) {
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  RAY_CHECK(it != placement_group_leasing_in_progress_.end());
  placement_group_leasing_in_progress_.erase(it);
}

void GcsPlacementGroupScheduler::ReserveResourceFromNode(
    const std::shared_ptr<BundleSpecification> &bundle,
    const std::shared_ptr<ray::rpc::GcsNodeInfo> &node, const StatusCallback &callback) {
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto node_id = ClientID::FromBinary(node->node_id());
  auto lease_client = GetOrConnectLeaseClient(remote_address);
  RAY_LOG(INFO) << "Leasing resource from node " << node_id
                << " for bundle: " << bundle->DebugString();
  lease_client->RequestResourceReserve(
      *bundle, [node_id, bundle, callback](
                   const Status &status, const rpc::RequestResourceReserveReply &reply) {
        // TODO(AlisaWu): Add placement group cancel.
        auto result = reply.success() ? Status::OK()
                                      : Status::IOError("Failed to reserve resource");
        if (result.ok()) {
          RAY_LOG(INFO) << "Finished leasing resource from " << node_id
                        << " for bundle: " << bundle->DebugString();
        } else {
          RAY_LOG(WARNING) << "Failed to lease resource from " << node_id
                           << " for bundle: " << bundle->DebugString();
        }
        callback(result);
      });
}

void GcsPlacementGroupScheduler::CancelResourceReserve(
    const std::shared_ptr<BundleSpecification> &bundle_spec,
    const std::shared_ptr<ray::rpc::GcsNodeInfo> &node) {
  if (node == nullptr) {
    RAY_LOG(WARNING) << "Node for a placement group id "
                     << bundle_spec->PlacementGroupId() << " and a bundle index, "
                     << bundle_spec->Index()
                     << " has already removed. Cancellation request will be ignored.";
    return;
  }
  auto node_id = ClientID::FromBinary(node->node_id());
  RAY_LOG(INFO) << "Cancelling the resource reserved for bundle: "
                << bundle_spec->DebugString() << " at node " << node_id;
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  auto return_client = GetOrConnectLeaseClient(remote_address);
  return_client->CancelResourceReserve(
      *bundle_spec, [bundle_spec, node_id](const Status &status,
                                           const rpc::CancelResourceReserveReply &reply) {
        RAY_LOG(INFO) << "Finished cancelling the resource reserved for bundle: "
                      << bundle_spec->DebugString() << " at node " << node_id;
      });
}

std::shared_ptr<ResourceReserveInterface>
GcsPlacementGroupScheduler::GetOrConnectLeaseClient(const rpc::Address &raylet_address) {
  auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
  auto iter = remote_lease_clients_.find(node_id);
  if (iter == remote_lease_clients_.end()) {
    auto lease_client = lease_client_factory_(raylet_address);
    iter = remote_lease_clients_.emplace(node_id, std::move(lease_client)).first;
  }
  return iter->second;
}

void GcsPlacementGroupScheduler::OnAllBundleSchedulingRequestReturned(
    const std::shared_ptr<GcsPlacementGroup> &placement_group,
    const std::vector<std::shared_ptr<BundleSpecification>> &bundles,
    const std::shared_ptr<BundleLocations> &bundle_locations,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  bundle_location_index_.AddBundleLocations(placement_group_id, bundle_locations);

  if (placement_group_leasing_in_progress_.find(placement_group_id) ==
          placement_group_leasing_in_progress_.end() ||
      bundle_locations->size() != bundles.size()) {
    // If the lease request has been already cancelled
    // or not every lease request succeeds.
    DestroyPlacementGroupBundleResourcesIfExists(placement_group_id);
    schedule_failure_handler(placement_group);
  } else {
    // If we successfully created placement group, store them to GCS.
    rpc::ScheduleData data;
    for (const auto &iter : bundles) {
      // TODO(ekl) this is a hack to get a string key for the proto
      auto key = iter->PlacementGroupId().Hex() + "_" + std::to_string(iter->Index());
      data.mutable_schedule_plan()->insert(
          {key, (*bundle_locations)[iter->BundleId()].first.Binary()});
    }
    RAY_CHECK_OK(gcs_table_storage_->PlacementGroupScheduleTable().Put(
        placement_group_id, data,
        [schedule_success_handler, placement_group](Status status) {
          schedule_success_handler(placement_group);
        }));

    for (const auto &iter : *bundle_locations) {
      const auto &location = iter.second;
      placement_group->GetMutableBundle(location.second->Index())
          ->set_node_id(location.first.Binary());
    }
  }
  // Erase leasing in progress placement group.
  // This could've been removed if the leasing request is cancelled already.
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  if (it != placement_group_leasing_in_progress_.end()) {
    placement_group_leasing_in_progress_.erase(it);
  }
}

std::unique_ptr<ScheduleContext> GcsPlacementGroupScheduler::GetScheduleContext(
    const PlacementGroupID &placement_group_id) {
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  bundle_location_index_.AddNodes(alive_nodes);

  auto node_to_bundles = std::make_shared<absl::flat_hash_map<ClientID, int64_t>>();
  for (const auto &node_it : alive_nodes) {
    const auto &node_id = node_it.first;
    const auto &bundle_locations_on_node =
        bundle_location_index_.GetBundleLocationsOnNode(node_id);
    RAY_CHECK(bundle_locations_on_node)
        << "Bundle locations haven't been registered for node id " << node_id;
    const int bundles_size = bundle_locations_on_node.value()->size();
    node_to_bundles->emplace(node_id, bundles_size);
  }

  auto &bundle_locations = bundle_location_index_.GetBundleLocations(placement_group_id);
  return std::unique_ptr<ScheduleContext>(new ScheduleContext(
      std::move(node_to_bundles), bundle_locations, gcs_node_manager_));
}

absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>
GcsPlacementGroupScheduler::GetBundlesOnNode(const ClientID &node_id) {
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles_on_node;
  const auto &maybe_bundle_locations =
      bundle_location_index_.GetBundleLocationsOnNode(node_id);
  if (maybe_bundle_locations.has_value()) {
    const auto &bundle_locations = maybe_bundle_locations.value();
    for (auto &bundle : *bundle_locations) {
      const auto &bundle_placement_group_id = bundle.first.first;
      const auto &bundle_index = bundle.first.second;
      bundles_on_node[bundle_placement_group_id].push_back(bundle_index);
    }
    bundle_location_index_.Erase(node_id);
  }
  return bundles_on_node;
}

void BundleLocationIndex::AddBundleLocations(
    const PlacementGroupID &placement_group_id,
    std::shared_ptr<BundleLocations> bundle_locations) {
  placement_group_to_bundle_locations_.emplace(placement_group_id, bundle_locations);
  for (auto iter : *bundle_locations) {
    const auto &node_id = iter.second.first;
    if (!node_to_leased_bundles_.contains(node_id)) {
      node_to_leased_bundles_[node_id] = std::make_shared<BundleLocations>();
    }
    node_to_leased_bundles_[node_id]->emplace(iter.first, iter.second);
  }
}

bool BundleLocationIndex::Erase(const ClientID &node_id) {
  const auto leased_bundles_it = node_to_leased_bundles_.find(node_id);
  if (leased_bundles_it == node_to_leased_bundles_.end()) {
    return false;
  }

  const auto &bundle_locations = leased_bundles_it->second;
  for (const auto &bundle_location : *bundle_locations) {
    // Remove corresponding placement group id.
    const auto &bundle_id = bundle_location.first;
    const auto &bundle_spec = bundle_location.second.second;
    const auto placement_group_id = bundle_spec->PlacementGroupId();
    auto placement_group_it =
        placement_group_to_bundle_locations_.find(placement_group_id);
    if (placement_group_it != placement_group_to_bundle_locations_.end()) {
      auto &pg_bundle_locations = placement_group_it->second;
      auto pg_bundle_it = pg_bundle_locations->find(bundle_id);
      if (pg_bundle_it != pg_bundle_locations->end()) {
        pg_bundle_locations->erase(pg_bundle_it);
      }
    }
  }
  node_to_leased_bundles_.erase(leased_bundles_it);
  return true;
}

bool BundleLocationIndex::Erase(const PlacementGroupID &placement_group_id) {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return false;
  }

  const auto &bundle_locations = it->second;
  // Remove bundles from node_to_leased_bundles_ because bundles are removed now.
  for (const auto &bundle_location : *bundle_locations) {
    const auto &bundle_id = bundle_location.first;
    const auto &node_id = bundle_location.second.first;
    const auto leased_bundles_it = node_to_leased_bundles_.find(node_id);
    // node could've been already dead at this point.
    if (leased_bundles_it != node_to_leased_bundles_.end()) {
      leased_bundles_it->second->erase(bundle_id);
    }
  }
  placement_group_to_bundle_locations_.erase(it);

  return true;
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocations(const PlacementGroupID &placement_group_id) {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return {};
  }
  return it->second;
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocationsOnNode(const ClientID &node_id) {
  auto it = node_to_leased_bundles_.find(node_id);
  if (it == node_to_leased_bundles_.end()) {
    return {};
  }
  return it->second;
}

void BundleLocationIndex::AddNodes(
    const absl::flat_hash_map<ClientID, std::shared_ptr<rpc::GcsNodeInfo>> &nodes) {
  for (const auto &iter : nodes) {
    if (!node_to_leased_bundles_.contains(iter.first)) {
      node_to_leased_bundles_[iter.first] = std::make_shared<BundleLocations>();
    }
  }
}

}  // namespace gcs
}  // namespace ray
