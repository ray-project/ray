#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "ray/common/asio/asio_util.h"

namespace ray {
namespace gcs {

namespace {

SchedulingOptions PolicyToOptions(rpc::SchedulingPolicy policy) {
  switch (policy) {
  case rpc::SCHEDULING_POLICY_PACK:
    return SchedulingOptions::BundlePack();
  case rpc::SCHEDULING_POLICY_SPREAD:
    return SchedulingOptions::BundleSpread();
  case rpc::SCHEDULING_POLICY_STRICT_SPREAD:
    return SchedulingOptions::BundleStrictSpread();
  default:
    RAY_LOG(FATAL) << "unknown SchedulingPolicy " << policy;
    return SchedulingOptions::Random();
  }
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

}  // namespace

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
  pending_virtual_clusters_.emplace_back(request);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  VirtualClusterID vc_id = VirtualClusterID::FromBinary(request.virtual_cluster_id());
  RAY_LOG(INFO) << "Removing virtual cluster " << vc_id;
  for (const auto &entry : gcs_node_manager_.GetAllAliveNodes()) {
    const auto lease_client = GetLeaseClientFromNode(entry.second);
    lease_client->ReturnVirtualCluster(
        vc_id, [](const Status &status, const rpc::ReturnVirtualClusterReply &reply) {
          RAY_CHECK(status.ok());
        });
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::vector<rpc::PlacementGroupTableData>
GcsVirtualClusterManager::GetVirtualClusterLoad() const {
  std::vector<rpc::PlacementGroupTableData> load;
  for (const auto &request : pending_virtual_clusters_) {
    // We add 1 `data` for each `fixed_size_nodes`.
    // TODO: also add load from min flex nodes.
    // Q: we should not add it as a PlacementGroupTableData, needs a return type change?
    for (const auto &fixed_size_nodes :
         request.virtual_cluster_spec().fixed_size_nodes()) {
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
  CreateVirtualClusters();
  execute_after(
      io_context_,
      [this] { Tick(); },
      std::chrono::milliseconds(1000) /* milliseconds */);
}

void GcsVirtualClusterManager::CreateVirtualClusters() {
  if (pending_virtual_clusters_.empty()) {
    return;
  }
  if (!ongoing_virtual_clusters_.empty()) {
    // Wait for the ongoing virtual cluster to be created.
    return;
  }

  const rpc::CreateVirtualClusterRequest request = *pending_virtual_clusters_.begin();

  VirtualClusterID vc_id =
      VirtualClusterID::FromBinary(request.virtual_cluster_spec().virtual_cluster_id());
  auto node_to_vnodes = Schedule(request);
  if (!node_to_vnodes.has_value()) {
    // Cluster has no free resources to create the virtual cluster,
    // wait until the next time.
    return;
  }

  pending_virtual_clusters_.pop_front();
  ongoing_virtual_clusters_.emplace(vc_id, request);
  for (const auto &[node_id, vnodes] : *node_to_vnodes) {
    ongoing_virtual_clusters_.at(vc_id).nodes.emplace(node_id);
    const auto lease_client =
        GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
    lease_client->PrepareVirtualCluster(
        vnodes,
        [vc_id, this](const Status &status,
                      const rpc::PrepareVirtualClusterReply &reply) {
          auto &ongoing_virtual_cluster = ongoing_virtual_clusters_.at(vc_id);
          ongoing_virtual_cluster.num_replied_prepares++;
          if (!reply.success()) {
            ongoing_virtual_cluster.has_failed_prepares = true;
          }
          if (ongoing_virtual_cluster.num_replied_prepares !=
              ongoing_virtual_cluster.nodes.size()) {
            return;
          }
          if (ongoing_virtual_cluster.has_failed_prepares) {
            for (const auto &node_id : ongoing_virtual_cluster.nodes) {
              const auto lease_client =
                  GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
              lease_client->ReturnVirtualCluster(
                  vc_id,
                  [](const Status &status, const rpc::ReturnVirtualClusterReply &reply) {
                    RAY_CHECK(status.ok());
                  });
            }
            // Add back to the pending queue and try again later.
            pending_virtual_clusters_.push_front(ongoing_virtual_cluster.request);
          } else {
            for (const auto &node_id : ongoing_virtual_cluster.nodes) {
              const auto lease_client =
                  GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
              lease_client->CommitVirtualCluster(
                  vc_id,
                  [](const Status &status, const rpc::CommitVirtualClusterReply &reply) {
                    RAY_CHECK(status.ok());
                  });
            }
          }
          ongoing_virtual_clusters_.erase(vc_id);
        });
  }
}

std::optional<std::unordered_map<NodeID, VirtualClusterNodesSpec>>
GcsVirtualClusterManager::Schedule(const rpc::CreateVirtualClusterRequest &request) {
  const auto &spec = request.virtual_cluster_spec();
  VirtualClusterID vc_id = VirtualClusterID::FromBinary(spec.virtual_cluster_id());

  std::unordered_map<NodeID, VirtualClusterNodesSpec> node_to_vnodes;

  // Prepare for each set of fixed size nodes. If we can't schedule for this
  // fixed_size_node, early return. If we can, add the scheduling result to
  // physical_nodes.
  for (const auto &fixed_size_nodes : spec.fixed_size_nodes()) {
    std::vector<VirtualClusterNodeSpec> vnodes;
    std::vector<const ResourceRequest *> resource_request_list;
    vnodes.reserve(fixed_size_nodes.nodes().size());
    for (const auto &vnode : fixed_size_nodes.nodes()) {
      vnodes.emplace_back(vnode, vc_id);
      resource_request_list.emplace_back(&vnodes.back().GetRequiredResources());
    }
    auto scheduling_result = cluster_resource_scheduler_.Schedule(
        resource_request_list, PolicyToOptions(fixed_size_nodes.scheduling_policy()));
    if (!scheduling_result.status.IsSuccess()) {
      return std::nullopt;
    }
    for (size_t i = 0; i < scheduling_result.selected_nodes.size(); ++i) {
      auto node_id = NodeID::FromBinary(scheduling_result.selected_nodes[i].Binary());
      auto &vnodes_for_node = node_to_vnodes[node_id];
      vnodes_for_node.vc_id = vc_id;
      vnodes_for_node.fixed_size_nodes.push_back(vnodes[i]);
    }
  }

  // TODO: also schedule for flex nodes.
  return node_to_vnodes;
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

}  // namespace gcs
}  // namespace ray
