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
    const gcs::GcsNodeManager &gcs_node_manager,
    ClusterResourceScheduler &cluster_resource_scheduler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    gcs_syncer::RaySyncer *ray_syncer)
    : return_timer_(io_context),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_node_manager_(gcs_node_manager),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      raylet_client_pool_(raylet_client_pool),
      ray_syncer_(ray_syncer) {}

void GcsPlacementGroupScheduler::ScheduleUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    PGSchedulingFailureCallback failure_callback,
    PGSchedulingSuccessfulCallback success_callback) {
  // We need to ensure that the PrepareBundleResources won't be sent before the reply of
  // ReleaseUnusedBundles is returned.
  if (!nodes_of_releasing_unused_bundles_.empty()) {
    RAY_LOG(INFO) << "Failed to schedule placement group " << placement_group->GetName()
                  << ", id: " << placement_group->GetPlacementGroupID() << ", because "
                  << nodes_of_releasing_unused_bundles_.size()
                  << " nodes have not released unused bundles.";
    failure_callback(placement_group, /*is_feasible*/ true);
    return;
  }

  const auto &bundles = placement_group->GetUnplacedBundles();
  const auto &strategy = placement_group->GetStrategy();

  RAY_LOG(DEBUG) << "Scheduling placement group " << placement_group->GetName()
                 << ", id: " << placement_group->GetPlacementGroupID()
                 << ", bundles size = " << bundles.size();

  std::vector<const ResourceRequest *> resource_request_list;
  resource_request_list.reserve(bundles.size());
  for (const auto &bundle : bundles) {
    resource_request_list.emplace_back(&bundle->GetRequiredResources());
  }

  auto scheduling_options =
      CreateSchedulingOptions(placement_group->GetPlacementGroupID(),
                              strategy,
                              placement_group->GetMaxCpuFractionPerNode());
  auto scheduling_result =
      cluster_resource_scheduler_.Schedule(resource_request_list, scheduling_options);

  auto result_status = scheduling_result.status;
  const auto &selected_nodes = scheduling_result.selected_nodes;

  if (!result_status.IsSuccess()) {
    RAY_LOG(DEBUG)
        << "Failed to schedule placement group " << placement_group->GetName()
        << ", id: " << placement_group->GetPlacementGroupID()
        << ", because current resources can't satisfy the required resource. IsFailed: "
        << result_status.IsFailed() << " IsInfeasible: " << result_status.IsInfeasible()
        << " IsPartialSuccess: " << result_status.IsPartialSuccess();
    bool infeasible = result_status.IsInfeasible();
    // If the placement group creation has failed,
    // but if it is not infeasible, it is retryable to create.
    failure_callback(placement_group, /*is_feasible*/ !infeasible);
    return;
  }

  RAY_LOG(DEBUG) << "Can schedule a placement group "
                 << placement_group->GetPlacementGroupID()
                 << ". Selected node size: " << selected_nodes.size();

  RAY_CHECK(bundles.size() == selected_nodes.size());

  // Covert to a map of bundle to node.
  ScheduleMap bundle_to_node;
  for (size_t i = 0; i < selected_nodes.size(); ++i) {
    bundle_to_node[bundles[i]->BundleId()] =
        NodeID::FromBinary(selected_nodes[i].Binary());
  }

  auto lease_status_tracker =
      std::make_shared<LeaseStatusTracker>(placement_group, bundles, bundle_to_node);
  RAY_CHECK(placement_group_leasing_in_progress_
                .emplace(placement_group->GetPlacementGroupID(), lease_status_tracker)
                .second);

  // Acquire resources from gcs resources manager to reserve bundle resources.
  const auto &bundle_locations = lease_status_tracker->GetBundleLocations();
  AcquireBundleResources(bundle_locations);

  // Covert to a set of bundle specifications grouped by the node.
  std::unordered_map<NodeID, std::vector<std::shared_ptr<const BundleSpecification>>>
      node_to_bundles;
  for (size_t i = 0; i < selected_nodes.size(); ++i) {
    node_to_bundles[NodeID::FromBinary(selected_nodes[i].Binary())].emplace_back(
        bundles[i]);
  }

  for (const auto &entry : node_to_bundles) {
    const auto &node_id = entry.first;
    const auto &bundles_per_node = entry.second;
    for (const auto &bundle : bundles_per_node) {
      lease_status_tracker->MarkPreparePhaseStarted(node_id, bundle);
    }

    // TODO(sang): The callback might not be called at all if nodes are dead. We should
    // handle this case properly.
    PrepareResources(bundles_per_node,
                     gcs_node_manager_.GetAliveNode(node_id),
                     [this,
                      bundles_per_node,
                      node_id,
                      lease_status_tracker,
                      failure_callback,
                      success_callback](const Status &status) {
                       for (const auto &bundle : bundles_per_node) {
                         lease_status_tracker->MarkPrepareRequestReturned(
                             node_id, bundle, status);
                       }

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
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
    const StatusCallback &callback) {
  if (!node.has_value()) {
    callback(Status::NotFound("Node is already dead."));
    return;
  }

  const auto lease_client = GetLeaseClientFromNode(node.value());
  const auto node_id = NodeID::FromBinary(node.value()->node_id());
  RAY_LOG(DEBUG) << "Preparing resource from node " << node_id
                 << " for bundles: " << GetDebugStringForBundles(bundles);

  lease_client->PrepareBundleResources(
      bundles,
      [node_id, bundles, callback](const Status &status,
                                   const rpc::PrepareBundleResourcesReply &reply) {
        auto result = reply.success() ? Status::OK()
                                      : Status::IOError("Failed to reserve resource");
        if (result.ok()) {
          RAY_LOG(DEBUG) << "Finished leasing resource from " << node_id
                         << " for bundles: " << GetDebugStringForBundles(bundles);
        } else {
          RAY_LOG(DEBUG) << "Failed to lease resource from " << node_id
                         << " for bundles: " << GetDebugStringForBundles(bundles);
        }
        callback(result);
      });
}

void GcsPlacementGroupScheduler::CommitResources(
    const std::vector<std::shared_ptr<const BundleSpecification>> &bundles,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node,
    const StatusCallback callback) {
  RAY_CHECK(node.has_value());
  const auto lease_client = GetLeaseClientFromNode(node.value());
  const auto node_id = NodeID::FromBinary(node.value()->node_id());

  RAY_LOG(DEBUG) << "Committing resource to a node " << node_id
                 << " for bundles: " << GetDebugStringForBundles(bundles);
  lease_client->CommitBundleResources(
      bundles,
      [bundles, node_id, callback](const Status &status,
                                   const rpc::CommitBundleResourcesReply &reply) {
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Finished committing resource to " << node_id
                         << " for bundles: " << GetDebugStringForBundles(bundles);
        } else {
          RAY_LOG(DEBUG) << "Failed to commit resource to " << node_id
                         << " for bundles: " << GetDebugStringForBundles(bundles);
        }
        RAY_CHECK(callback);
        callback(status);
      });
}

void GcsPlacementGroupScheduler::CancelResourceReserve(
    const std::shared_ptr<const BundleSpecification> &bundle_spec,
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
      *bundle_spec,
      [this, bundle_spec, node_id](const Status &status,
                                   const rpc::CancelResourceReserveReply &reply) {
        RAY_LOG(DEBUG) << "Finished cancelling the resource reserved for bundle: "
                       << bundle_spec->DebugString() << " at node " << node_id;
        if (ray_syncer_ != nullptr) {
          auto &resources = bundle_spec->GetFormattedResources();
          rpc::NodeResourceChange node_resource_change;
          for (const auto &iter : resources) {
            node_resource_change.add_deleted_resources(iter.first);
          }
          node_resource_change.set_node_id(node_id.Binary());
          ray_syncer_->Update(std::move(node_resource_change));
        }
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
    const PGSchedulingFailureCallback &schedule_failure_handler,
    const PGSchedulingSuccessfulCallback &schedule_success_handler) {
  const std::shared_ptr<BundleLocations> &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  std::unordered_map<NodeID, std::vector<std::shared_ptr<const BundleSpecification>>>
      bundle_locations_per_node;
  for (const auto &bundle_location : *prepared_bundle_locations) {
    const auto &node_id = bundle_location.second.first;
    if (bundle_locations_per_node.find(node_id) == bundle_locations_per_node.end()) {
      bundle_locations_per_node[node_id] = {};
    }
    bundle_locations_per_node[node_id].push_back(bundle_location.second.second);
  }
  lease_status_tracker->MarkCommitPhaseStarted();

  for (const auto &node_to_bundles : bundle_locations_per_node) {
    const auto &node_id = node_to_bundles.first;
    const auto &node = gcs_node_manager_.GetAliveNode(node_id);
    const auto &bundles_per_node = node_to_bundles.second;

    auto commit_resources_callback = [this,
                                      lease_status_tracker,
                                      bundles_per_node,
                                      node_id,
                                      schedule_failure_handler,
                                      schedule_success_handler](const Status &status) {
      auto commited_bundle_locations = std::make_shared<BundleLocations>();
      for (const auto &bundle : bundles_per_node) {
        lease_status_tracker->MarkCommitRequestReturned(node_id, bundle, status);
        (*commited_bundle_locations)[bundle->BundleId()] = {node_id, bundle};

        if (ray_syncer_ != nullptr) {
          auto &resources = bundle->GetFormattedResources();
          // Push the message to syncer so that it can be broadcasted to all other nodes
          rpc::NodeResourceChange node_resource_change;
          node_resource_change.set_node_id(node_id.Binary());
          node_resource_change.mutable_updated_resources()->insert(resources.begin(),
                                                                   resources.end());
          ray_syncer_->Update(std::move(node_resource_change));
        }
      }
      // Commit the bundle resources on the remote node to the cluster resources.
      CommitBundleResources(commited_bundle_locations);

      if (lease_status_tracker->AllCommitRequestReturned()) {
        OnAllBundleCommitRequestReturned(
            lease_status_tracker, schedule_failure_handler, schedule_success_handler);
      }
    };

    if (node.has_value()) {
      CommitResources(bundles_per_node, node, commit_resources_callback);
    } else {
      RAY_LOG(INFO) << "Failed to commit resources because the node is dead, node id = "
                    << node_id;
      commit_resources_callback(Status::Interrupted("Node is dead"));
    }
  }
}

void GcsPlacementGroupScheduler::OnAllBundlePrepareRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const PGSchedulingFailureCallback &schedule_failure_handler,
    const PGSchedulingSuccessfulCallback &schedule_success_handler) {
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
    schedule_failure_handler(placement_group, /*is_feasible*/ true);
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
      placement_group_id,
      data,
      [this, schedule_success_handler, schedule_failure_handler, lease_status_tracker](
          Status status) {
        CommitAllBundles(
            lease_status_tracker, schedule_failure_handler, schedule_success_handler);
      }));
}

void GcsPlacementGroupScheduler::OnAllBundleCommitRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const PGSchedulingFailureCallback &schedule_failure_handler,
    const PGSchedulingSuccessfulCallback &schedule_success_handler) {
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
  cluster_resource_scheduler_.GetClusterResourceManager()
      .GetBundleLocationIndex()
      .AddOrUpdateBundleLocations(prepared_bundle_locations);
  // NOTE: If the placement group scheduling has been cancelled, we just need to destroy
  // the committed bundles. The reason is that only `RemovePlacementGroup` will mark the
  // state of placement group as `CANCELLED` and it will also destroy all prepared and
  // committed bundles of the placement group.
  // However, it cannot destroy the newly submitted bundles in this scheduling, so we need
  // to destroy them separately.
  if (lease_status_tracker->GetLeasingState() == LeasingState::CANCELLED) {
    DestroyPlacementGroupCommittedBundleResources(placement_group_id);
    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
    schedule_failure_handler(placement_group, /*is_feasible*/ true);
    return;
  }

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
    schedule_failure_handler(placement_group, /*is_feasible*/ true);
  } else {
    schedule_success_handler(placement_group);
  }
}

std::unique_ptr<BundleSchedulingContext>
GcsPlacementGroupScheduler::CreateSchedulingContext(
    const PlacementGroupID &placement_group_id) {
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  committed_bundle_location_index_.AddNodes(alive_nodes);
  auto bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  return std::make_unique<BundleSchedulingContext>(std::move(bundle_locations));
}

SchedulingOptions GcsPlacementGroupScheduler::CreateSchedulingOptions(
    const PlacementGroupID &placement_group_id,
    rpc::PlacementStrategy strategy,
    double max_cpu_fraction_per_node) {
  switch (strategy) {
  case rpc::PlacementStrategy::PACK:
    return SchedulingOptions::BundlePack(max_cpu_fraction_per_node);
  case rpc::PlacementStrategy::SPREAD:
    return SchedulingOptions::BundleSpread(max_cpu_fraction_per_node);
  case rpc::PlacementStrategy::STRICT_PACK:
    return SchedulingOptions::BundleStrictPack(max_cpu_fraction_per_node);
  case rpc::PlacementStrategy::STRICT_SPREAD:
    return SchedulingOptions::BundleStrictSpread(
        max_cpu_fraction_per_node, CreateSchedulingContext(placement_group_id));
  default:
    RAY_LOG(FATAL) << "Unsupported scheduling type: "
                   << rpc::PlacementStrategy_Name(strategy);
  }
  UNREACHABLE;
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
    cluster_resource_scheduler_.GetClusterResourceManager()
        .GetBundleLocationIndex()
        .Erase(node_id);
  }
  return bundles_on_node;
}

void GcsPlacementGroupScheduler::ReleaseUnusedBundles(
    const absl::flat_hash_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles) {
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

void GcsPlacementGroupScheduler::Initialize(
    const absl::flat_hash_map<PlacementGroupID,
                              std::vector<std::shared_ptr<BundleSpecification>>>
        &group_to_bundles) {
  // We need to reinitialize the `committed_bundle_location_index_`, otherwise,
  // it will get an empty bundle set when raylet fo occurred after GCS server restart.

  // Init the container that contains the map relation between node and bundle.
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  committed_bundle_location_index_.AddNodes(alive_nodes);

  for (const auto &group : group_to_bundles) {
    const auto &placement_group_id = group.first;
    std::shared_ptr<BundleLocations> committed_bundle_locations =
        std::make_shared<BundleLocations>();
    for (const auto &bundle : group.second) {
      if (!bundle->NodeId().IsNil()) {
        committed_bundle_locations->emplace(bundle->BundleId(),
                                            std::make_pair(bundle->NodeId(), bundle));
      }
    }
    committed_bundle_location_index_.AddBundleLocations(placement_group_id,
                                                        committed_bundle_locations);
    cluster_resource_scheduler_.GetClusterResourceManager()
        .GetBundleLocationIndex()
        .AddOrUpdateBundleLocations(committed_bundle_locations);
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
    cluster_resource_scheduler_.GetClusterResourceManager()
        .GetBundleLocationIndex()
        .Erase(placement_group_id);
  }
}

void GcsPlacementGroupScheduler::AcquireBundleResources(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  // Acquire bundle resources from gcs resources manager.
  auto &cluster_resource_manager =
      cluster_resource_scheduler_.GetClusterResourceManager();
  for (auto &bundle : *bundle_locations) {
    cluster_resource_manager.SubtractNodeAvailableResources(
        scheduling::NodeID(bundle.second.first.Binary()),
        bundle.second.second->GetRequiredResources());
  }
}

absl::flat_hash_map<scheduling::NodeID, ResourceRequest> ToNodeBundleResourcesMap(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  absl::flat_hash_map<scheduling::NodeID, ResourceRequest> node_bundle_resources_map;
  for (const auto &bundle : *bundle_locations) {
    auto node_id = scheduling::NodeID(bundle.second.first.Binary());
    const auto &bundle_spec = *bundle.second.second;
    auto bundle_resource_request = ResourceMapToResourceRequest(
        bundle_spec.GetFormattedResources(), /*requires_object_store_memory=*/false);
    node_bundle_resources_map[node_id] += bundle_resource_request;
  }
  return node_bundle_resources_map;
}

/// Help function to check if the resource_name has the pattern
/// {original_resource_name}_group_{placement_group_id}, which means
/// wildcard resource.
bool GcsPlacementGroupScheduler::IsPlacementGroupWildcardResource(
    const std::string &resource_name) {
  std::string_view resource_name_view(resource_name);
  std::string_view pattern("_group_");

  // The length of {placement_group_id} is fixed, so we just need to check that if the
  // length and the pos of `_group_` match.
  if (resource_name_view.size() < pattern.size() + 2 * PlacementGroupID::Size()) {
    return false;
  }

  auto idx = resource_name_view.size() - (pattern.size() + 2 * PlacementGroupID::Size());
  return resource_name_view.substr(idx, pattern.size()) == pattern;
}

void GcsPlacementGroupScheduler::CommitBundleResources(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  // Acquire bundle resources from gcs resources manager.
  auto &cluster_resource_manager =
      cluster_resource_scheduler_.GetClusterResourceManager();
  auto node_bundle_resources_map = ToNodeBundleResourcesMap(bundle_locations);
  for (const auto &[node_id, node_bundle_resources] : node_bundle_resources_map) {
    for (const auto &[resource_id, capacity] : node_bundle_resources.ToMap()) {
      // A placement group's wildcard resource has to be the sum of all related bundles.
      // Even though `ToNodeBundleResourcesMap` has already considered this,
      // it misses the scenario in which single (or subset of) bundle is rescheduled.
      // When commiting this single bundle, its wildcard resource would wrongly overwrite
      // the existing value, unless using the following additive operation.
      if (IsPlacementGroupWildcardResource(resource_id.Binary())) {
        auto new_capacity =
            capacity +
            cluster_resource_manager.GetNodeResources(node_id).total.Get(resource_id);
        cluster_resource_manager.UpdateResourceCapacity(
            node_id, resource_id, new_capacity.Double());
      } else {
        cluster_resource_manager.UpdateResourceCapacity(
            node_id, resource_id, capacity.Double());
      }
    }
  }

  for (const auto &listener : resources_changed_listeners_) {
    listener();
  }
}

void GcsPlacementGroupScheduler::ReturnBundleResources(
    const std::shared_ptr<BundleLocations> &bundle_locations) {
  // Return bundle resources to gcs resources manager should contains the following steps.
  // 1. Remove related bundle resources from nodes.
  // 2. Add resources allocated for bundles back to nodes.
  for (auto &bundle : *bundle_locations) {
    if (!TryReleasingBundleResources(bundle.second)) {
      waiting_removed_bundles_.push_back(bundle.second);
    }
  }

  for (const auto &listener : resources_changed_listeners_) {
    listener();
  }
}

void GcsPlacementGroupScheduler::AddResourcesChangedListener(
    std::function<void()> listener) {
  RAY_CHECK(listener != nullptr);
  resources_changed_listeners_.emplace_back(std::move(listener));
}

bool GcsPlacementGroupScheduler::TryReleasingBundleResources(
    const std::pair<NodeID, std::shared_ptr<const BundleSpecification>> &bundle) {
  auto &cluster_resource_manager =
      cluster_resource_scheduler_.GetClusterResourceManager();
  auto node_id = scheduling::NodeID(bundle.first.Binary());
  const auto &bundle_spec = bundle.second;
  std::vector<scheduling::ResourceID> bundle_resource_ids;
  absl::flat_hash_map<std::string, FixedPoint> wildcard_resources;
  // Subtract wildcard resources and delete bundle resources.
  for (const auto &entry : bundle_spec->GetFormattedResources()) {
    auto resource_id = scheduling::ResourceID(entry.first);
    auto capacity =
        cluster_resource_manager.GetNodeResources(node_id).total.Get(resource_id);
    if (IsPlacementGroupWildcardResource(entry.first)) {
      wildcard_resources[entry.first] = capacity - entry.second;
    } else {
      if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
        auto available_amount =
            cluster_resource_manager.GetNodeResources(node_id).available.Get(resource_id);
        if (available_amount != capacity) {
          RAY_LOG(WARNING)
              << "The resource " << entry.first
              << " now is still in use when removing bundle " << bundle_spec->Index()
              << " from placement group: " << bundle_spec->PlacementGroupId()
              << ", maybe some workers depending on this bundle have not released the "
                 "resource yet."
              << " We will try it later.";
          bundle_resource_ids.clear();
          break;
        }
      }
      bundle_resource_ids.emplace_back(resource_id);
    }
  }

  // This bundle is not ready for returning.
  if (bundle_resource_ids.empty()) {
    return false;
  }

  for (const auto &[resource_name, capacity] : wildcard_resources) {
    if (capacity == 0) {
      bundle_resource_ids.emplace_back(scheduling::ResourceID(resource_name));
    } else {
      cluster_resource_manager.UpdateResourceCapacity(
          node_id, scheduling::ResourceID(resource_name), capacity.Double());
    }
  }

  // It will affect nothing if the resource_id to be deleted does not exist in the
  // cluster_resource_manager_.
  cluster_resource_manager.DeleteResources(node_id, bundle_resource_ids);
  // Add reserved bundle resources back to the node.
  cluster_resource_manager.AddNodeAvailableResources(node_id,
                                                     bundle_spec->GetRequiredResources());
  return true;
}

void GcsPlacementGroupScheduler::HandleWaitingRemovedBundles() {
  for (auto iter = waiting_removed_bundles_.begin();
       iter != waiting_removed_bundles_.end();) {
    auto current = iter++;
    auto bundle = *current;
    if (TryReleasingBundleResources(bundle)) {
      // Release bundle successfully.
      waiting_removed_bundles_.erase(current);
    }
  }
  for (const auto &listener : resources_changed_listeners_) {
    listener();
  }
}

LeaseStatusTracker::LeaseStatusTracker(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    const std::vector<std::shared_ptr<const BundleSpecification>> &unplaced_bundles,
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
    const NodeID &node_id, const std::shared_ptr<const BundleSpecification> &bundle) {
  const auto &bundle_id = bundle->BundleId();
  return node_to_bundles_when_preparing_[node_id].emplace(bundle_id).second;
}

void LeaseStatusTracker::MarkPrepareRequestReturned(
    const NodeID &node_id,
    const std::shared_ptr<const BundleSpecification> &bundle,
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
    const NodeID &node_id,
    const std::shared_ptr<const BundleSpecification> &bundle,
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

const std::vector<std::shared_ptr<const BundleSpecification>>
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
