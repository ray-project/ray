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
    instrumented_io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const gcs::GcsNodeManager &gcs_node_manager, GcsResourceManager &gcs_resource_manager,
    GcsResourceScheduler &gcs_resource_scheduler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool)
    : return_timer_(io_context),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_node_manager_(gcs_node_manager),
      gcs_resource_manager_(gcs_resource_manager),
      gcs_resource_scheduler_(gcs_resource_scheduler),
      raylet_client_pool_(raylet_client_pool) {
  scheduler_strategies_.push_back(std::make_shared<GcsPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsSpreadStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictSpreadStrategy>());
}

std::vector<ResourceSet> GcsScheduleStrategy::GetRequiredResourcesFromBundles(
    const std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles) {
  std::vector<ResourceSet> required_resources;
  for (const auto &bundle : bundles) {
    required_resources.push_back(bundle->GetRequiredResources());
  }
  return required_resources;
}

ScheduleMap GcsScheduleStrategy::GenerateScheduleMap(
    const std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::vector<NodeID> &selected_nodes) {
  ScheduleMap schedule_map;
  if (!selected_nodes.empty()) {
    RAY_CHECK(bundles.size() == selected_nodes.size());
    int index = 0;
    for (const auto &bundle : bundles) {
      schedule_map[bundle->BundleId()] = selected_nodes[index++];
    }
  }
  return schedule_map;
}

ScheduleMap GcsStrictPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context,
    GcsResourceScheduler &gcs_resource_scheduler) {
  const auto &required_resources = GetRequiredResourcesFromBundles(bundles);
  const auto &selected_nodes =
      gcs_resource_scheduler.Schedule(required_resources, SchedulingType::STRICT_PACK);
  return GenerateScheduleMap(bundles, selected_nodes);
}

ScheduleMap GcsPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context,
    GcsResourceScheduler &gcs_resource_scheduler) {
  // The current algorithm is to select a node and deploy as many bundles as possible.
  // First fill up a node. If the node resource is insufficient, select a new node.
  // TODO(ffbin): We will speed this up in next PR. Currently it is a double for loop.
  const auto &required_resources = GetRequiredResourcesFromBundles(bundles);
  const auto &selected_nodes =
      gcs_resource_scheduler.Schedule(required_resources, SchedulingType::PACK);
  return GenerateScheduleMap(bundles, selected_nodes);
}

ScheduleMap GcsSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context,
    GcsResourceScheduler &gcs_resource_scheduler) {
  const auto &required_resources = GetRequiredResourcesFromBundles(bundles);
  const auto &selected_nodes =
      gcs_resource_scheduler.Schedule(required_resources, SchedulingType::SPREAD);
  return GenerateScheduleMap(bundles, selected_nodes);
}

ScheduleMap GcsStrictSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context,
    GcsResourceScheduler &gcs_resource_scheduler) {
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.

  // Filter out the nodes already scheduled by this placement group.
  absl::flat_hash_set<NodeID> nodes_in_use;
  if (context->bundle_locations_.has_value()) {
    const auto &bundle_locations = context->bundle_locations_.value();
    for (auto &bundle : *bundle_locations) {
      nodes_in_use.insert(bundle.second.first);
    }
  }

  const auto &required_resources = GetRequiredResourcesFromBundles(bundles);
  const auto &selected_nodes = gcs_resource_scheduler.Schedule(
      required_resources, SchedulingType::STRICT_SPREAD,
      /*node_filter_func=*/[&nodes_in_use](const NodeID &node_id) {
        return nodes_in_use.count(node_id) == 0;
      });
  return GenerateScheduleMap(bundles, selected_nodes);
}

void GcsPlacementGroupScheduler::ScheduleUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> failure_callback,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> success_callback) {
  // We need to ensure that the PrepareBundleResources won't be sent before the reply of
  // ReleaseUnusedBundles is returned.
  if (!nodes_of_releasing_unused_bundles_.empty()) {
    RAY_LOG(INFO) << "Failed to schedule placement group " << placement_group->GetName()
                  << ", id: " << placement_group->GetPlacementGroupID() << ", because "
                  << nodes_of_releasing_unused_bundles_.size()
                  << " nodes have not released unused bundles.";
    failure_callback(placement_group);
    return;
  }

  auto bundles = placement_group->GetUnplacedBundles();
  auto strategy = placement_group->GetStrategy();

  RAY_LOG(INFO) << "Scheduling placement group " << placement_group->GetName()
                << ", id: " << placement_group->GetPlacementGroupID()
                << ", bundles size = " << bundles.size();
  auto selected_nodes = scheduler_strategies_[strategy]->Schedule(
      bundles, GetScheduleContext(placement_group->GetPlacementGroupID()),
      gcs_resource_scheduler_);

  // If no nodes are available, scheduling fails.
  if (selected_nodes.empty()) {
    RAY_LOG(INFO) << "Failed to schedule placement group " << placement_group->GetName()
                  << ", id: " << placement_group->GetPlacementGroupID()
                  << ", because no nodes are available.";
    failure_callback(placement_group);
    return;
  }

  auto lease_status_tracker =
      std::make_shared<LeaseStatusTracker>(placement_group, bundles, selected_nodes);
  RAY_CHECK(placement_group_leasing_in_progress_
                .emplace(placement_group->GetPlacementGroupID(), lease_status_tracker)
                .second);

  /// TODO(AlisaWu): Change the strategy when reserve resource failed.
  for (const auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    const auto &node_id = selected_nodes[bundle_id];
    lease_status_tracker->MarkPreparePhaseStarted(node_id, bundle);
    // TODO(sang): The callback might not be called at all if nodes are dead. We should
    // handle this case properly.
    PrepareResources(bundle, gcs_node_manager_.GetAliveNode(node_id),
                     [this, bundle, node_id, lease_status_tracker, failure_callback,
                      success_callback](const Status &status) {
                       lease_status_tracker->MarkPrepareRequestReturned(node_id, bundle,
                                                                        status);
                       if (lease_status_tracker->AllPrepareRequestsReturned()) {
                         OnAllBundlePrepareRequestReturned(
                             lease_status_tracker, failure_callback, success_callback);
                       }
                     });
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupBundleResourcesIfExists(
    const PlacementGroupID &placement_group_id) {
  auto &bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  if (bundle_locations.has_value()) {
    // There could be leasing bundles and committed bundles at the same time if placement
    // groups are rescheduling, so we need to destroy prepared bundles and committed
    // bundles at the same time.
    DestroyPlacementGroupPreparedBundleResources(placement_group_id);
    DestroyPlacementGroupCommittedBundleResources(placement_group_id);

    // Return destroyed bundles resources to the cluster resource.
    ReturnBundleResources(bundle_locations.value());
  }
}

void GcsPlacementGroupScheduler::MarkScheduleCancelled(
    const PlacementGroupID &placement_group_id) {
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  RAY_CHECK(it != placement_group_leasing_in_progress_.end());
  it->second->MarkPlacementGroupScheduleCancelled();
}

void GcsPlacementGroupScheduler::PrepareResources(
    const std::shared_ptr<BundleSpecification> &bundle,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
    const StatusCallback &callback) {
  if (!node.has_value()) {
    callback(Status::NotFound("Node is already dead."));
    return;
  }

  const auto lease_client = GetLeaseClientFromNode(node.value());
  const auto node_id = NodeID::FromBinary(node.value()->node_id());
  RAY_LOG(DEBUG) << "Preparing resource from node " << node_id
                 << " for a bundle: " << bundle->DebugString();
  lease_client->PrepareBundleResources(
      *bundle, [node_id, bundle, callback](
                   const Status &status, const rpc::PrepareBundleResourcesReply &reply) {
        auto result = reply.success() ? Status::OK()
                                      : Status::IOError("Failed to reserve resource");
        if (result.ok()) {
          RAY_LOG(DEBUG) << "Finished leasing resource from " << node_id
                         << " for bundle: " << bundle->DebugString();
        } else {
          RAY_LOG(DEBUG) << "Failed to lease resource from " << node_id
                         << " for bundle: " << bundle->DebugString();
        }
        callback(result);
      });
}

void GcsPlacementGroupScheduler::CommitResources(
    const std::shared_ptr<BundleSpecification> &bundle,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
    const StatusCallback callback) {
  RAY_CHECK(node.has_value());
  const auto lease_client = GetLeaseClientFromNode(node.value());
  const auto node_id = NodeID::FromBinary(node.value()->node_id());

  RAY_LOG(DEBUG) << "Committing resource to a node " << node_id
                 << " for a bundle: " << bundle->DebugString();
  lease_client->CommitBundleResources(
      *bundle, [bundle, node_id, callback](const Status &status,
                                           const rpc::CommitBundleResourcesReply &reply) {
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Finished committing resource to " << node_id
                         << " for bundle: " << bundle->DebugString();
        } else {
          RAY_LOG(DEBUG) << "Failed to commit resource to " << node_id
                         << " for bundle: " << bundle->DebugString();
        }
        RAY_CHECK(callback);
        callback(status);
      });
}

void GcsPlacementGroupScheduler::CancelResourceReserve(
    const std::shared_ptr<BundleSpecification> &bundle_spec,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node) {
  if (!node.has_value()) {
    RAY_LOG(INFO) << "Node for a placement group id " << bundle_spec->PlacementGroupId()
                  << " and a bundle index, " << bundle_spec->Index()
                  << " has already removed. Cancellation request will be ignored.";
    return;
  }
  auto node_id = NodeID::FromBinary(node.value()->node_id());
  RAY_LOG(DEBUG) << "Cancelling the resource reserved for bundle: "
                 << bundle_spec->DebugString() << " at node " << node_id;
  const auto return_client = GetLeaseClientFromNode(node.value());

  return_client->CancelResourceReserve(
      *bundle_spec, [bundle_spec, node_id](const Status &status,
                                           const rpc::CancelResourceReserveReply &reply) {
        RAY_LOG(DEBUG) << "Finished cancelling the resource reserved for bundle: "
                       << bundle_spec->DebugString() << " at node " << node_id;
      });
}

std::shared_ptr<ResourceReserveInterface>
GcsPlacementGroupScheduler::GetOrConnectLeaseClient(const rpc::Address &raylet_address) {
  return raylet_client_pool_->GetOrConnectByAddress(raylet_address);
}

std::shared_ptr<ResourceReserveInterface>
GcsPlacementGroupScheduler::GetLeaseClientFromNode(
    const std::shared_ptr<ray::rpc::GcsNodeInfo> &node) {
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  return GetOrConnectLeaseClient(remote_address);
}

void GcsPlacementGroupScheduler::CommitAllBundles(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  const std::shared_ptr<BundleLocations> &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  lease_status_tracker->MarkCommitPhaseStarted();
  for (const auto &bundle_to_commit : *prepared_bundle_locations) {
    const auto &node_id = bundle_to_commit.second.first;
    const auto &node = gcs_node_manager_.GetAliveNode(node_id);
    const auto &bundle = bundle_to_commit.second.second;

    auto commit_resources_callback = [this, lease_status_tracker, bundle, node_id,
                                      schedule_failure_handler,
                                      schedule_success_handler](const Status &status) {
      lease_status_tracker->MarkCommitRequestReturned(node_id, bundle, status);
      if (lease_status_tracker->AllCommitRequestReturned()) {
        OnAllBundleCommitRequestReturned(lease_status_tracker, schedule_failure_handler,
                                         schedule_success_handler);
      }
    };

    if (node.has_value()) {
      CommitResources(bundle, node, commit_resources_callback);
    } else {
      RAY_LOG(INFO) << "Failed to commit resources because the node is dead, node id = "
                    << node_id;
      commit_resources_callback(Status::Interrupted("Node is dead"));
    }
  }
}

void GcsPlacementGroupScheduler::OnAllBundlePrepareRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  RAY_CHECK(lease_status_tracker->AllPrepareRequestsReturned())
      << "This method can be called only after all bundle scheduling requests are "
         "returned.";
  const auto &placement_group = lease_status_tracker->GetPlacementGroup();
  const auto &bundles = lease_status_tracker->GetBundlesToSchedule();
  const auto &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  if (!lease_status_tracker->AllPrepareRequestsSuccessful()) {
    // Erase the status tracker from a in-memory map if exists.
    // NOTE: A placement group may be scheduled several times to succeed.
    // If a prepare failure occurs during scheduling, we just need to release the prepared
    // bundle resources of this scheduling.
    DestroyPlacementGroupPreparedBundleResources(placement_group_id);
    auto it = placement_group_leasing_in_progress_.find(placement_group_id);
    RAY_CHECK(it != placement_group_leasing_in_progress_.end());
    placement_group_leasing_in_progress_.erase(it);
    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
    schedule_failure_handler(placement_group);
    return;
  }

  // If the prepare requests succeed, update the bundle location.
  for (const auto &iter : *prepared_bundle_locations) {
    const auto &location = iter.second;
    placement_group->GetMutableBundle(location.second->Index())
        ->set_node_id(location.first.Binary());
  }

  // Store data to GCS.
  rpc::ScheduleData data;
  for (const auto &iter : bundles) {
    // TODO(ekl) this is a hack to get a string key for the proto
    auto key = iter->PlacementGroupId().Hex() + "_" + std::to_string(iter->Index());
    data.mutable_schedule_plan()->insert(
        {key, (*prepared_bundle_locations)[iter->BundleId()].first.Binary()});
  }
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupScheduleTable().Put(
      placement_group_id, data,
      [this, schedule_success_handler, schedule_failure_handler,
       lease_status_tracker](Status status) {
        CommitAllBundles(lease_status_tracker, schedule_failure_handler,
                         schedule_success_handler);
      }));
}

void GcsPlacementGroupScheduler::OnAllBundleCommitRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  const auto &placement_group = lease_status_tracker->GetPlacementGroup();
  const auto &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  // Clean up the leasing progress map.
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  RAY_CHECK(it != placement_group_leasing_in_progress_.end());
  placement_group_leasing_in_progress_.erase(it);

  // Add a prepared bundle locations to committed bundle locations.
  committed_bundle_location_index_.AddBundleLocations(placement_group_id,
                                                      prepared_bundle_locations);

  // NOTE: If the placement group scheduling has been cancelled, we just need to destroy
  // the committed bundles. The reason is that only `RemovePlacementGroup` will mark the
  // state of placement group as `CANCELLED` and it will also destroy all prepared and
  // committed bundles of the placement group.
  // However, it cannot destroy the newly submitted bundles in this scheduling, so we need
  // to destroy them separately.
  if (lease_status_tracker->GetLeasingState() == LeasingState::CANCELLED) {
    DestroyPlacementGroupCommittedBundleResources(placement_group_id);
    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
    schedule_failure_handler(placement_group);
    return;
  }

  // Acquire bundle resources from gcs resources manager.
  const auto &committed_bundle_locations =
      lease_status_tracker->GetCommittedBundleLocations();
  AcquireBundleResources(committed_bundle_locations);

  if (!lease_status_tracker->AllCommitRequestsSuccessful()) {
    // Update the state to be reschedule so that the failure handle will reschedule the
    // failed bundles.
    const auto &uncommitted_bundle_locations =
        lease_status_tracker->GetUnCommittedBundleLocations();
    for (const auto &bundle : *uncommitted_bundle_locations) {
      placement_group->GetMutableBundle(bundle.first.second)->clear_node_id();
    }
    placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
    ReturnBundleResources(uncommitted_bundle_locations);
    schedule_failure_handler(placement_group);
  } else {
    schedule_success_handler(placement_group);
  }
}

std::unique_ptr<ScheduleContext> GcsPlacementGroupScheduler::GetScheduleContext(
    const PlacementGroupID &placement_group_id) {
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  committed_bundle_location_index_.AddNodes(alive_nodes);

  auto node_to_bundles = std::make_shared<absl::flat_hash_map<NodeID, int64_t>>();
  for (const auto &node_it : alive_nodes) {
    const auto &node_id = node_it.first;
    const auto &bundle_locations_on_node =
        committed_bundle_location_index_.GetBundleLocationsOnNode(node_id);
    RAY_CHECK(bundle_locations_on_node)
        << "Bundle locations haven't been registered for node id " << node_id;
    const int bundles_size = bundle_locations_on_node.value()->size();
    node_to_bundles->emplace(node_id, bundles_size);
  }

  auto &bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  return std::unique_ptr<ScheduleContext>(
      new ScheduleContext(std::move(node_to_bundles), bundle_locations));
}

absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>
GcsPlacementGroupScheduler::GetBundlesOnNode(const NodeID &node_id) {
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles_on_node;
  const auto &maybe_bundle_locations =
      committed_bundle_location_index_.GetBundleLocationsOnNode(node_id);
  if (maybe_bundle_locations.has_value()) {
    const auto &bundle_locations = maybe_bundle_locations.value();
    for (auto &bundle : *bundle_locations) {
      const auto &bundle_placement_group_id = bundle.first.first;
      const auto &bundle_index = bundle.first.second;
      bundles_on_node[bundle_placement_group_id].push_back(bundle_index);
    }
    committed_bundle_location_index_.Erase(node_id);
  }
  return bundles_on_node;
}

void GcsPlacementGroupScheduler::ReleaseUnusedBundles(
    const std::unordered_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles) {
  // The purpose of this function is to release bundles that may be leaked.
  // When GCS restarts, it doesn't know which bundles it has scheduled in the
  // previous lifecycle. In this case, GCS will send a list of bundle ids that
  // are still needed. And Raylet will release other bundles. If the node is
  // dead, there is no need to send the request of release unused bundles.
  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  for (const auto &alive_node : alive_nodes) {
    const auto &node_id = alive_node.first;
    nodes_of_releasing_unused_bundles_.insert(node_id);

    auto lease_client = GetLeaseClientFromNode(alive_node.second);
    auto release_unused_bundles_callback =
        [this, node_id](const Status &status,
                        const rpc::ReleaseUnusedBundlesReply &reply) {
          nodes_of_releasing_unused_bundles_.erase(node_id);
        };
    auto iter = node_to_bundles.find(alive_node.first);

    // When GCS restarts, some nodes maybe do not have bundles.
    // In this case, GCS will send an empty list.
    auto bundles_in_use =
        iter != node_to_bundles.end() ? iter->second : std::vector<rpc::Bundle>{};
    lease_client->ReleaseUnusedBundles(bundles_in_use, release_unused_bundles_callback);
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupPreparedBundleResources(
    const PlacementGroupID &placement_group_id) {
  // Get the locations of prepared bundles.
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  if (it != placement_group_leasing_in_progress_.end()) {
    const auto &leasing_context = it->second;
    const auto &leasing_bundle_locations = leasing_context->GetPreparedBundleLocations();

    // Cancel all resource reservation of prepared bundles.
    RAY_LOG(INFO) << "Cancelling all prepared bundles of a placement group, id is "
                  << placement_group_id;
    for (const auto &iter : *(leasing_bundle_locations)) {
      auto &bundle_spec = iter.second.second;
      auto &node_id = iter.second.first;
      CancelResourceReserve(bundle_spec, gcs_node_manager_.GetAliveNode(node_id));
    }
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupCommittedBundleResources(
    const PlacementGroupID &placement_group_id) {
  // Get the locations of committed bundles.
  const auto &maybe_bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  if (maybe_bundle_locations.has_value()) {
    const auto &committed_bundle_locations = maybe_bundle_locations.value();

    // Cancel all resource reservation of committed bundles.
    RAY_LOG(INFO) << "Cancelling all committed bundles of a placement group, id is "
                  << placement_group_id;
    for (const auto &iter : *(committed_bundle_locations)) {
      auto &bundle_spec = iter.second.second;
      auto &node_id = iter.second.first;
      CancelResourceReserve(bundle_spec, gcs_node_manager_.GetAliveNode(node_id));
    }
    committed_bundle_location_index_.Erase(placement_group_id);
  }
}

void GcsPlacementGroupScheduler::AcquireBundleResources(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  // Acquire bundle resources from gcs resources manager.
  for (auto &bundle : *bundle_locations) {
    gcs_resource_manager_.AcquireResources(bundle.second.first,
                                           bundle.second.second->GetRequiredResources());
  }
}

void GcsPlacementGroupScheduler::ReturnBundleResources(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  // Release bundle resources to gcs resources manager.
  for (auto &bundle : *bundle_locations) {
    gcs_resource_manager_.ReleaseResources(bundle.second.first,
                                           bundle.second.second->GetRequiredResources());
  }
}

void BundleLocationIndex::AddBundleLocations(
    const PlacementGroupID &placement_group_id,
    std::shared_ptr<BundleLocations> bundle_locations) {
  // Update `placement_group_to_bundle_locations_`.
  // The placement group may be scheduled several times to succeed, so we need to merge
  // `bundle_locations` instead of covering it directly.
  auto iter = placement_group_to_bundle_locations_.find(placement_group_id);
  if (iter == placement_group_to_bundle_locations_.end()) {
    placement_group_to_bundle_locations_.emplace(placement_group_id, bundle_locations);
  } else {
    iter->second->insert(bundle_locations->begin(), bundle_locations->end());
  }

  // Update `node_to_leased_bundles_`.
  for (auto iter : *bundle_locations) {
    const auto &node_id = iter.second.first;
    if (!node_to_leased_bundles_.contains(node_id)) {
      node_to_leased_bundles_[node_id] = std::make_shared<BundleLocations>();
    }
    node_to_leased_bundles_[node_id]->emplace(iter.first, iter.second);
  }
}

bool BundleLocationIndex::Erase(const NodeID &node_id) {
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
BundleLocationIndex::GetBundleLocationsOnNode(const NodeID &node_id) {
  auto it = node_to_leased_bundles_.find(node_id);
  if (it == node_to_leased_bundles_.end()) {
    return {};
  }
  return it->second;
}

void BundleLocationIndex::AddNodes(
    const absl::flat_hash_map<NodeID, std::shared_ptr<ray::rpc::GcsNodeInfo>> &nodes) {
  for (const auto &iter : nodes) {
    if (!node_to_leased_bundles_.contains(iter.first)) {
      node_to_leased_bundles_[iter.first] = std::make_shared<BundleLocations>();
    }
  }
}

LeaseStatusTracker::LeaseStatusTracker(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    const std::vector<std::shared_ptr<BundleSpecification>> &unplaced_bundles,
    const ScheduleMap &schedule_map)
    : placement_group_(placement_group), bundles_to_schedule_(unplaced_bundles) {
  preparing_bundle_locations_ = std::make_shared<BundleLocations>();
  uncommitted_bundle_locations_ = std::make_shared<BundleLocations>();
  committed_bundle_locations_ = std::make_shared<BundleLocations>();
  bundle_locations_ = std::make_shared<BundleLocations>();
  for (const auto &bundle : unplaced_bundles) {
    const auto &iter = schedule_map.find(bundle->BundleId());
    RAY_CHECK(iter != schedule_map.end());
    (*bundle_locations_)[bundle->BundleId()] = std::make_pair(iter->second, bundle);
  }
}

bool LeaseStatusTracker::MarkPreparePhaseStarted(
    const NodeID &node_id, std::shared_ptr<BundleSpecification> bundle) {
  const auto &bundle_id = bundle->BundleId();
  return node_to_bundles_when_preparing_[node_id].emplace(bundle_id).second;
}

void LeaseStatusTracker::MarkPrepareRequestReturned(
    const NodeID &node_id, const std::shared_ptr<BundleSpecification> bundle,
    const Status &status) {
  RAY_CHECK(prepare_request_returned_count_ <= bundles_to_schedule_.size());
  auto leasing_bundles = node_to_bundles_when_preparing_.find(node_id);
  RAY_CHECK(leasing_bundles != node_to_bundles_when_preparing_.end());
  auto bundle_iter = leasing_bundles->second.find(bundle->BundleId());
  RAY_CHECK(bundle_iter != leasing_bundles->second.end());

  // Remove the bundle from the leasing map as the reply is returned from the
  // remote node.
  leasing_bundles->second.erase(bundle_iter);
  if (leasing_bundles->second.empty()) {
    node_to_bundles_when_preparing_.erase(leasing_bundles);
  }

  // If the request succeeds, record it.
  const auto &bundle_id = bundle->BundleId();
  if (status.ok()) {
    preparing_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  }
  prepare_request_returned_count_ += 1;
}

bool LeaseStatusTracker::AllPrepareRequestsReturned() const {
  return prepare_request_returned_count_ == bundles_to_schedule_.size();
}

bool LeaseStatusTracker::AllPrepareRequestsSuccessful() const {
  return AllPrepareRequestsReturned() &&
         (preparing_bundle_locations_->size() == bundles_to_schedule_.size()) &&
         (leasing_state_ != LeasingState::CANCELLED);
}

void LeaseStatusTracker::MarkCommitRequestReturned(
    const NodeID &node_id, const std::shared_ptr<BundleSpecification> bundle,
    const Status &status) {
  commit_request_returned_count_ += 1;
  // If the request succeeds, record it.
  const auto &bundle_id = bundle->BundleId();
  if (!status.ok()) {
    uncommitted_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  } else {
    committed_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  }
}

bool LeaseStatusTracker::AllCommitRequestReturned() const {
  return commit_request_returned_count_ == bundles_to_schedule_.size();
}

bool LeaseStatusTracker::AllCommitRequestsSuccessful() const {
  // We don't check cancel state here because we shouldn't destroy bundles when
  // commit requests failed. Cancel state should be treated separately.
  return AllCommitRequestReturned() &&
         preparing_bundle_locations_->size() == bundles_to_schedule_.size() &&
         uncommitted_bundle_locations_->empty();
}

const std::shared_ptr<GcsPlacementGroup> &LeaseStatusTracker::GetPlacementGroup() const {
  return placement_group_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetPreparedBundleLocations()
    const {
  return preparing_bundle_locations_;
}

const std::shared_ptr<BundleLocations>
    &LeaseStatusTracker::GetUnCommittedBundleLocations() const {
  return uncommitted_bundle_locations_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetCommittedBundleLocations()
    const {
  return committed_bundle_locations_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetBundleLocations() const {
  return bundle_locations_;
}

const std::vector<std::shared_ptr<BundleSpecification>>
    &LeaseStatusTracker::GetBundlesToSchedule() const {
  return bundles_to_schedule_;
}

const LeasingState LeaseStatusTracker::GetLeasingState() const { return leasing_state_; }

void LeaseStatusTracker::MarkPlacementGroupScheduleCancelled() {
  UpdateLeasingState(LeasingState::CANCELLED);
}

bool LeaseStatusTracker::UpdateLeasingState(LeasingState leasing_state) {
  // If the lease was cancelled, we cannot update the state.
  if (leasing_state_ == LeasingState::CANCELLED) {
    return false;
  }
  leasing_state_ = leasing_state;
  return true;
}

void LeaseStatusTracker::MarkCommitPhaseStarted() {
  UpdateLeasingState(LeasingState::COMMITTING);
}

}  // namespace gcs
}  // namespace ray
