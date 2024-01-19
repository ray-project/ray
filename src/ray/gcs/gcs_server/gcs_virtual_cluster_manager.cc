#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "ray/common/asio/asio_util.h"
#include "ray/raylet/scheduling/policy/scheduling_context.h"

namespace ray {
namespace gcs {

namespace {

// We piggy back on the existing bundle scheduling context to do the scheduling.
// This means we behave as if a FixedSizeNodes is a placement group.
//
// HACK: A BundleSchedulingContext wants a BundleLocations, which wants a
// BundleSpecification for each bundle. However, we only have a VirtualClusterNodeSpec.
// But notice that value is never used in the BundleStrictSpreadSchedulingPolicy. So we
// can just pass in a nullptr. This is a hack because if later we change
// bundle_scheduling_policy.cc we may suddenly crash this virtual cluster code.
//
std::unique_ptr<raylet_scheduling_policy::BundleSchedulingContext>
CreateSchedulingContext(VirtualClusterID vc_id,
                        const std::unordered_set<NodeID> &already_used_nodes) {
  auto bundle_locations = std::make_shared<BundleLocations>();

  static_assert(VirtualClusterID::Size() > PlacementGroupID::Size());
  std::string binary_id = vc_id.Binary();
  binary_id.resize(PlacementGroupID::Size());
  auto fake_pg_id = PlacementGroupID::FromBinary(binary_id);

  size_t bundle_index = 0;

  for (const auto &node_id : already_used_nodes) {
    bundle_locations->emplace(BundleID(fake_pg_id, bundle_index),
                              std::make_pair(node_id, nullptr));
    bundle_index++;
  }
  return std::make_unique<raylet_scheduling_policy::BundleSchedulingContext>(
      std::move(bundle_locations));
}

SchedulingOptions CreateSchedulingOptions(
    rpc::SchedulingPolicy policy,
    VirtualClusterID vc_id,
    const std::unordered_set<NodeID> &already_used_nodes) {
  switch (policy) {
  case rpc::SCHEDULING_POLICY_PACK:
    return SchedulingOptions::BundlePack();
  case rpc::SCHEDULING_POLICY_SPREAD:
    return SchedulingOptions::BundleSpread();
  case rpc::SCHEDULING_POLICY_STRICT_SPREAD:
    return SchedulingOptions::BundleStrictSpread(
        /*max_cpu_fraction_per_node=*/1.0,
        CreateSchedulingContext(vc_id, already_used_nodes));
  default:
    RAY_LOG(FATAL) << "unknown SchedulingPolicy " << policy;
  }
  UNREACHABLE;
}

rpc::PlacementStrategy PolicyToStrategy(rpc::SchedulingPolicy policy) {
  switch (policy) {
  case rpc::SCHEDULING_POLICY_PACK:
    return rpc::PlacementStrategy::PACK;
  case rpc::SCHEDULING_POLICY_SPREAD:
    return rpc::PlacementStrategy::SPREAD;
  case rpc::SCHEDULING_POLICY_STRICT_SPREAD:
    return rpc::PlacementStrategy::STRICT_SPREAD;
  default:
    RAY_LOG(FATAL) << "unknown SchedulingPolicy " << policy;
    return rpc::PlacementStrategy::PACK;
  }
}

// Merge all vnodes in `source` into `target`.
// `source` and `target` must be for a same vc_id.
void MergeScheduledNodesFrom(ScheduledNodes &target, const ScheduledNodes &source) {
  for (const auto &[node_id, additional_vnodes] : source) {
    VirtualClusterNodesSpec &vnodes = target[node_id];
    vnodes.vc_id = additional_vnodes.vc_id;
    for (const auto &vnode : additional_vnodes.fixed_size_nodes) {
      vnodes.fixed_size_nodes.push_back(vnode);
    }
  }
}

// Merge all ScheduledNodes into one by grouping by node_id.
// Input must are for a same vc_id.
// TODO: support flex nodes.
ScheduledNodes MergeScheduledNodes(
    const std::vector<ScheduledNodes> &partial_scheduled_nodes) {
  ScheduledNodes scheduled_nodes;
  for (const auto &fixed_size_nodes : partial_scheduled_nodes) {
    MergeScheduledNodesFrom(scheduled_nodes, fixed_size_nodes);
  }
  return scheduled_nodes;
}

}  // namespace

void CreateVnodesTransaction::PrepareOneNode(NodeID node_id) {
  auto done = [this](bool success) {
    if (!success) {
      has_failed_prepares = true;
    }
    num_replied_prepares++;
    if (num_replied_prepares == scheduled_nodes.size()) {
      AllPrepared();
    }
  };

  const auto maybe_lease_client = manager->GetLeaseClientFromAliveNode(node_id);
  if (!maybe_lease_client.has_value()) {
    done(false);
    return;
  }

  maybe_lease_client->get()->PrepareVirtualCluster(
      scheduled_nodes.at(node_id),
      [done](const Status &status, const rpc::PrepareVirtualClusterReply &reply) {
        done(status.ok() && reply.success());
      });
}

void CreateVnodesTransaction::AllPrepared() {
  if (has_failed_prepares) {
    ReturnAll();
    finish_callback(false);
    return;
  } else {
    CommitAll();
  }
}

void CreateVnodesTransaction::CommitOneNode(NodeID node_id) {
  auto done = [this](bool success) {
    if (!success) {
      has_failed_commits = true;
    }
    num_replied_commits++;
    if (num_replied_commits == scheduled_nodes.size()) {
      AllCommitted();
    }
  };

  const auto maybe_lease_client = manager->GetLeaseClientFromAliveNode(node_id);
  if (!maybe_lease_client.has_value()) {
    done(false);
    return;
  }

  maybe_lease_client->get()->CommitVirtualCluster(
      vc_id, [done](const Status &status, const rpc::CommitVirtualClusterReply &reply) {
        done(status.ok());
      });
}

void CreateVnodesTransaction::CommitAll() {
  for (const auto &[node_id, vnodes] : scheduled_nodes) {
    CommitOneNode(node_id);
  }
}

void CreateVnodesTransaction::AllCommitted() {
  if (has_failed_prepares) {
    ReturnAll();
    finish_callback(false);
  } else {
    finish_callback(true);
  }
}

void CreateVnodesTransaction::ReturnOneNode(NodeID node_id) {
  // Crashes on node failure (can't find lease client)
  const auto lease_client = manager->GetLeaseClientFromAliveNode(node_id).value();
  lease_client->ReturnVirtualCluster(
      vc_id, [](const Status &status, const rpc::ReturnVirtualClusterReply &reply) {
        RAY_CHECK(status.ok());
      });
}

void CreateVnodesTransaction::ReturnAll() {
  for (const auto &[node_id, vnodes] : scheduled_nodes) {
    ReturnOneNode(node_id);
  }
}

void CreateVnodesTransaction::PrepareAll() {
  for (const auto &[node_id, vnodes] : scheduled_nodes) {
    PrepareOneNode(node_id);
  }
}

CreatingVirtualCluster::CreatingVirtualCluster(
    GcsVirtualClusterManager *manager,
    VirtualClusterSpecification vc,
    std::vector<ScheduledNodes> scheduled_fixed_size_nodes)
    : manager(manager),
      vc(vc),
      scheduled_fixed_size_nodes(std::move(scheduled_fixed_size_nodes)) {
  auto vc_id = vc.VirtualClusterId();

  transaction = std::make_unique<CreateVnodesTransaction>(
      manager,
      vc_id,
      MergeScheduledNodes(this->scheduled_fixed_size_nodes),
      [manager, vc_id](bool succeeded) {
        manager->CreatedVirtualCluster(vc_id, succeeded);
      });
}

void CreatingVirtualCluster::PrepareAll() { transaction->PrepareAll(); }

bool RunningVirtualCluster::IsSatisfied(
    const std::unordered_set<NodeID> &alive_nodes) const {
  // TODO: also look at flex
  for (const auto &fixed_size_nodes : allocated_fixed_size_nodes) {
    for (const auto &[node_id, _] : fixed_size_nodes) {
      if (alive_nodes.find(node_id) == alive_nodes.end()) {
        return false;
      }
    }
  }
  return true;
}

RecoveringVirtualCluster::RecoveringVirtualCluster(
    GcsVirtualClusterManager *manager,
    std::shared_ptr<RunningVirtualCluster> running_vc,
    std::vector<ScheduledNodes> recovering_fixed_size_nodes)
    : manager(manager),
      running_vc(std::move(running_vc)),
      recovering_fixed_size_nodes(std::move(recovering_fixed_size_nodes)) {
  auto vc_id = running_vc->vc.VirtualClusterId();

  transaction = std::make_unique<CreateVnodesTransaction>(
      manager,
      vc_id,
      MergeScheduledNodes(this->recovering_fixed_size_nodes),
      [manager, vc_id](bool succeeded) {
        manager->RecoveredVirtualCluster(vc_id, succeeded);
      });
}

void RecoveringVirtualCluster::PrepareAll() { transaction->PrepareAll(); }

GcsVirtualClusterManager::GcsVirtualClusterManager(
    instrumented_io_context &io_context,
    const gcs::GcsNodeManager &gcs_node_manager,
    ClusterResourceScheduler &cluster_resource_scheduler,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool)
    : io_context_(io_context),
      gcs_node_manager_(gcs_node_manager),
      cluster_resource_scheduler_(cluster_resource_scheduler),
      raylet_client_pool_(raylet_client_pool) {
  Tick();
}

void GcsVirtualClusterManager::HandleCreateVirtualCluster(
    rpc::CreateVirtualClusterRequest request,
    rpc::CreateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Creating virtual cluster " << request.DebugString();
  pending_virtual_clusters_.emplace_back(std::move(request.virtual_cluster_spec()));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  VirtualClusterID vc_id = VirtualClusterID::FromBinary(request.virtual_cluster_id());
  RAY_LOG(INFO) << "Removing virtual cluster " << vc_id;
  // TODO: if we have 1000s of nodes, this will be slow. Instead, consult the
  // *_virtual_clusters_ fields and find the nodes that have this vc_id.
  for (const auto &entry : gcs_node_manager_.GetAllAliveNodes()) {
    const auto lease_client = GetLeaseClientFromNode(entry.second);
    lease_client->ReturnVirtualCluster(
        vc_id, [](const Status &status, const rpc::ReturnVirtualClusterReply &reply) {
          RAY_CHECK(status.ok());
        });
  }
  // TODO: remove all vc references in the manager's fields.
  // Note for the Creating and the Recovering VCs, we can't cancel the transaction and
  // their callbacks will still be called. So we may do this for those 2 states:
  // 1. add a to_delete: std::unordered_set<VirtualClusterID> to the manager.
  // 2. in the callbacks: if hit, return the vc and clean up.
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::vector<rpc::PlacementGroupTableData>
GcsVirtualClusterManager::GetVirtualClusterLoad() const {
  std::vector<rpc::PlacementGroupTableData> load;
  for (const auto &vc : pending_virtual_clusters_) {
    // We add 1 `data` for each `fixed_size_nodes`.
    // TODO: also add load from min flex nodes.
    // Q: we should not add it as a PlacementGroupTableData, needs a return type change?
    for (const auto &fixed_size_nodes : vc.GetMessage().fixed_size_nodes()) {
      rpc::PlacementGroupTableData data;
      data.set_strategy(PolicyToStrategy(fixed_size_nodes.scheduling_policy()));
      for (const auto &vnode : fixed_size_nodes.nodes()) {
        auto *pg_bundle = data.add_bundles();
        pg_bundle->mutable_unit_resources()->insert(vnode.resources().begin(),
                                                    vnode.resources().end());
      }
      load.emplace_back(data);
    }
  }
  return load;
}

void GcsVirtualClusterManager::Tick() {
  RAY_LOG(DEBUG) << "GcsVirtualClusterManager::Tick() "
                 << pending_virtual_clusters_.size() << " pending virtual clusters, "
                 << creating_virtual_clusters_.size() << " ongoing virtual clusters.";
  RecoverVirtualClusters();
  CreateVirtualClusters();
  execute_after(
      io_context_,
      [this] { Tick(); },
      std::chrono::milliseconds(1000) /* milliseconds */);
}

void GcsVirtualClusterManager::RecoverVirtualClusters() {
  std::unordered_set<NodeID> alive_nodes;
  for (const auto &[node_id, node] : gcs_node_manager_.GetAllAliveNodes()) {
    alive_nodes.insert(node_id);
  }

  // Step 1. Find any running virtual clusters that are unsatisfied.
  // TODO: Note this happens every tick and can be slow. We could optimize this by only
  // checking on the NodeRemoved callback.
  {
    std::vector<VirtualClusterID> to_remove;

    for (const auto &[vc_id, vc] : running_virtual_clusters_) {
      if (!vc->IsSatisfied(alive_nodes)) {
        // This VC is unsatisfied, move it to unsatisfied_virtual_clusters_.
        to_remove.push_back(vc_id);
        unsatisfied_virtual_clusters_.emplace(vc_id, vc);
      }
    }
    for (const auto &vc_id : to_remove) {
      running_virtual_clusters_.erase(vc_id);
    }
  }
  // Step 2. Try to recover any unsatisfied virtual clusters.
  {
    std::vector<VirtualClusterID> to_remove;
    for (const auto &[vc_id, vc] : unsatisfied_virtual_clusters_) {
      auto recovering = ScheduleRecovery(vc, alive_nodes);
      if (recovering.has_value()) {
        // Can schedule this VC, remove from unsatisfied_virtual_clusters_. Can't do that
        // in the loop though.
        to_remove.push_back(vc_id);
        recovering_virtual_clusters_.emplace(vc_id, std::move(recovering.value()));
        recovering_virtual_clusters_.at(vc_id)->PrepareAll();
      }
    }

    for (const auto &vc_id : to_remove) {
      unsatisfied_virtual_clusters_.erase(vc_id);
    }
  }
}

void GcsVirtualClusterManager::CreateVirtualClusters() {
  if (pending_virtual_clusters_.empty()) {
    return;
  }
  if (!creating_virtual_clusters_.empty()) {
    // Wait for the ongoing virtual cluster to be created.
    return;
  }

  VirtualClusterSpecification vc = *pending_virtual_clusters_.begin();

  auto creating_vc = ScheduleNew(vc);
  if (!creating_vc.has_value()) {
    // Cluster has no free resources to create the virtual cluster,
    // wait until the next time.
    return;
  }

  pending_virtual_clusters_.pop_front();

  auto vc_id = vc.VirtualClusterId();
  creating_virtual_clusters_.emplace(vc_id, std::move(creating_vc.value()));
  creating_virtual_clusters_.at(vc_id)->PrepareAll();
}

// Schedule a fixed-size-nodes, respecting its strategy and already used nodes for this
// set of vnodes.
// If we can't schedule, return std::nullopt.
std::optional<ScheduledNodes> GcsVirtualClusterManager::Schedule(
    VirtualClusterID vc_id,
    const std::vector<VirtualClusterNodeSpec> &fixed_size_nodes,
    rpc::SchedulingPolicy policy,
    const std::unordered_set<NodeID> &already_used_nodes) {
  ScheduledNodes results;

  if (fixed_size_nodes.empty()) {
    return results;
  }
  std::vector<const ResourceRequest *> resource_request_list;
  for (const auto &vnode : fixed_size_nodes) {
    resource_request_list.emplace_back(&vnode.GetRequiredResources());
  }
  auto scheduling_result = cluster_resource_scheduler_.Schedule(
      resource_request_list, CreateSchedulingOptions(policy, vc_id, already_used_nodes));
  if (!scheduling_result.status.IsSuccess()) {
    return std::nullopt;
  }
  for (size_t i = 0; i < scheduling_result.selected_nodes.size(); ++i) {
    auto node_id = NodeID::FromBinary(scheduling_result.selected_nodes[i].Binary());
    auto &vnodes_for_node = results[node_id];
    vnodes_for_node.vc_id = vc_id;
    vnodes_for_node.fixed_size_nodes.push_back(fixed_size_nodes[i]);
  }
  return results;
}

std::optional<std::unique_ptr<CreatingVirtualCluster>>
GcsVirtualClusterManager::ScheduleNew(const VirtualClusterSpecification &vc) {
  // TODO: support flex nodes.
  VirtualClusterID vc_id = vc.VirtualClusterId();
  std::vector<ScheduledNodes> results;

  // Prepare for each set of fixed size nodes. If we can't schedule for this
  // fixed_size_node, early return. If we can, add the scheduling result to
  // physical_nodes.
  for (const auto &fixed_size_nodes : vc.GetMessage().fixed_size_nodes()) {
    std::vector<VirtualClusterNodeSpec> vnodes;
    for (const auto &vnode : fixed_size_nodes.nodes()) {
      vnodes.emplace_back(vnode, vc_id);
    }
    auto scheduling_result =
        Schedule(vc_id, vnodes, fixed_size_nodes.scheduling_policy(), {});
    if (!scheduling_result.has_value()) {
      return std::nullopt;
    }
    results.push_back(std::move(scheduling_result.value()));
  }
  return std::make_unique<CreatingVirtualCluster>(this, vc, std::move(results));
}

// Schedule a recovery for a running virtual cluster, based on the failed nodes.
// First find out which fixed_size_nodes are affected, then schedule for each of them.
std::optional<std::unique_ptr<RecoveringVirtualCluster>>
GcsVirtualClusterManager::ScheduleRecovery(
    std::shared_ptr<RunningVirtualCluster> running_vc,
    const std::unordered_set<NodeID> &alive_nodes) {
  std::vector<ScheduledNodes> result_allocated_fixed_size_nodes;
  std::vector<ScheduledNodes> result_recovering_fixed_size_nodes;

  // TODO: support flex nodes.

  for (size_t i = 0; i < running_vc->allocated_fixed_size_nodes.size(); ++i) {
    const auto &fixed_size_nodes = running_vc->vc.GetMessage().fixed_size_nodes()[i];
    const auto &allocated = running_vc->allocated_fixed_size_nodes[i];
    // For each fixed_size_nodes, find out
    // 1. dead vnodes to reschedule,
    // 2. still-alive physical nodes (for strict_spread).
    //
    // Then schedule dead vnodes and add to result->recovering_fixed_size_nodes.
    std::unordered_set<NodeID> already_used_nodes;
    std::vector<VirtualClusterNodeSpec> dead_vnodes;

    for (const auto &[node_id, vnodes] : allocated) {
      if (alive_nodes.find(node_id) != alive_nodes.end()) {
        // This node is still alive, we need to keep all its vnodes.
        already_used_nodes.insert(node_id);
      } else {
        // This node is dead, we need to reschedule vnodes on it.
        for (const auto &vnode : vnodes.fixed_size_nodes) {
          dead_vnodes.push_back(vnode);
        }
      }
    }
    auto scheduled = Schedule(running_vc->vc.VirtualClusterId(),
                              dead_vnodes,
                              fixed_size_nodes.scheduling_policy(),
                              already_used_nodes);
    if (!scheduled.has_value()) {
      return std::nullopt;
    }
    result_recovering_fixed_size_nodes.push_back(std::move(scheduled.value()));
  }
  return std::make_unique<RecoveringVirtualCluster>(
      this, running_vc, std::move(result_recovering_fixed_size_nodes));
}

void GcsVirtualClusterManager::CreatedVirtualCluster(VirtualClusterID vc_id,
                                                     bool success) {
  auto &creating_vc = creating_virtual_clusters_.at(vc_id);
  if (!success) {
    RAY_LOG(INFO) << "Failed to create virtual cluster " << vc_id << ", will retry";
    pending_virtual_clusters_.push_front(creating_vc->vc);
  } else {
    RAY_LOG(INFO) << "Created virtual cluster " << vc_id;
    auto running_vc = std::make_unique<RunningVirtualCluster>(
        creating_vc->vc, std::move(creating_vc->scheduled_fixed_size_nodes));
    running_virtual_clusters_.emplace(vc_id, std::move(running_vc));
  }
  creating_virtual_clusters_.erase(vc_id);
}

void GcsVirtualClusterManager::RecoveredVirtualCluster(VirtualClusterID vc_id,
                                                       bool success) {
  auto &recovering_vc = recovering_virtual_clusters_.at(vc_id);
  if (!success) {
    RAY_LOG(INFO) << "Failed to recover virtual cluster " << vc_id << ", will retry";
    unsatisfied_virtual_clusters_.emplace(vc_id, recovering_vc->running_vc);
  } else {
    RAY_LOG(INFO) << "Recovered virtual cluster " << vc_id;
    // TODO: TOCTOU check if there are any more node failures during the 2pc.
    running_virtual_clusters_.emplace(vc_id, recovering_vc->running_vc);
  }
  recovering_virtual_clusters_.erase(vc_id);
}

std::shared_ptr<ResourceReserveInterface>
GcsVirtualClusterManager::GetLeaseClientFromNode(
    const std::shared_ptr<rpc::GcsNodeInfo> &node) {
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());
  return raylet_client_pool_->GetOrConnectByAddress(remote_address);
}

absl::optional<std::shared_ptr<ResourceReserveInterface>>
GcsVirtualClusterManager::GetLeaseClientFromAliveNode(const NodeID &node_id) {
  auto maybe_node = gcs_node_manager_.GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    return {};
  }
  return GetLeaseClientFromNode(maybe_node.value());
}

}  // namespace gcs
}  // namespace ray
