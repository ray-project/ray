#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "ray/common/asio/asio_util.h"

namespace ray {
namespace gcs {

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
    lease_client->ReturnVirtualClusterBundle(
        vc_id,
        [](const Status &status, const rpc::ReturnVirtualClusterBundleReply &reply) {
          RAY_CHECK(status.ok());
        });
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::vector<rpc::PlacementGroupTableData>
GcsVirtualClusterManager::GetVirtualClusterLoad() const {
  std::vector<rpc::PlacementGroupTableData> load;
  for (const auto &request : pending_virtual_clusters_) {
    rpc::PlacementGroupTableData data;
    for (const auto &vc_bundle : request.virtual_cluster_spec().bundles()) {
      auto *pg_bundle = data.add_bundles();
      pg_bundle->mutable_unit_resources()->insert(vc_bundle.resources().begin(),
                                                  vc_bundle.resources().end());
    }
    data.set_strategy(rpc::PlacementStrategy::STRICT_SPREAD);
    load.emplace_back(data);
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
  auto node_to_bundle = Schedule(request);
  if (!node_to_bundle.has_value()) {
    // Cluster has no free resources to create the virtual cluster,
    // wait until the next time.
    return;
  }

  pending_virtual_clusters_.pop_front();
  ongoing_virtual_clusters_.emplace(vc_id, request);
  for (const auto &entry : *node_to_bundle) {
    NodeID node_id = entry.first;
    ongoing_virtual_clusters_.at(vc_id).nodes.emplace(node_id);
    const auto lease_client =
        GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
    lease_client->PrepareVirtualClusterBundle(
        entry.second,
        [vc_id, this](const Status &status,
                      const rpc::PrepareVirtualClusterBundleReply &reply) {
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
              lease_client->ReturnVirtualClusterBundle(
                  vc_id,
                  [](const Status &status,
                     const rpc::ReturnVirtualClusterBundleReply &reply) {
                    RAY_CHECK(status.ok());
                  });
            }
            // Add back to the pending queue and try again later.
            pending_virtual_clusters_.push_front(ongoing_virtual_cluster.request);
          } else {
            for (const auto &node_id : ongoing_virtual_cluster.nodes) {
              const auto lease_client =
                  GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
              lease_client->CommitVirtualClusterBundle(
                  vc_id,
                  [](const Status &status,
                     const rpc::CommitVirtualClusterBundleReply &reply) {
                    RAY_CHECK(status.ok());
                  });
            }
          }
          ongoing_virtual_clusters_.erase(vc_id);
        });
  }
}

std::optional<std::unordered_map<NodeID, VirtualClusterBundleSpec>>
GcsVirtualClusterManager::Schedule(const rpc::CreateVirtualClusterRequest &request) {
  VirtualClusterID vc_id =
      VirtualClusterID::FromBinary(request.virtual_cluster_spec().virtual_cluster_id());
  std::vector<VirtualClusterBundleSpec> bundles;
  std::vector<const ResourceRequest *> resource_request_list;
  bundles.reserve(request.virtual_cluster_spec().bundles_size());
  for (size_t i = 0; i < request.virtual_cluster_spec().bundles_size(); i++) {
    bundles.emplace_back(request.virtual_cluster_spec().bundles(i), vc_id);
    resource_request_list.emplace_back(&bundles[i].GetRequiredResources());
  }
  auto scheduling_result = cluster_resource_scheduler_.Schedule(
      resource_request_list, SchedulingOptions::BundleStrictSpread());
  if (!scheduling_result.status.IsSuccess()) {
    return std::nullopt;
  }
  std::unordered_map<NodeID, VirtualClusterBundleSpec> node_to_bundle;
  for (size_t i = 0; i < scheduling_result.selected_nodes.size(); ++i) {
    node_to_bundle.emplace(
        NodeID::FromBinary(scheduling_result.selected_nodes[i].Binary()), bundles[i]);
  }
  return node_to_bundle;
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
