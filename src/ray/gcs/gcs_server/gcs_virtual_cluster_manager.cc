#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

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
      raylet_client_pool_(raylet_client_pool) {}

void GcsVirtualClusterManager::HandleCreateVirtualCluster(
    rpc::CreateVirtualClusterRequest request,
    rpc::CreateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Creating virtual cluster " << request.DebugString();
  VirtualClusterID vc_id =
      VirtualClusterID::FromBinary(request.virtual_cluster_spec().virtual_cluster_id());
  auto node_to_bundle = Schedule(request);
  for (const auto &entry : node_to_bundle) {
    const auto lease_client =
        GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(entry.first).value());
    lease_client->PrepareVirtualClusterBundle(
        entry.second,
        [vc_id, lease_client](const Status &status,
                              const rpc::PrepareVirtualClusterBundleReply &reply) {
          RAY_CHECK(reply.success());
          lease_client->CommitVirtualClusterBundle(
              vc_id,
              [](const Status &status,
                 const rpc::CommitVirtualClusterBundleReply &reply) {
                RAY_CHECK(status.ok());
              });
        });
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  io_context_.post([] {}, "");
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

std::unordered_map<NodeID, VirtualClusterBundleSpec> GcsVirtualClusterManager::Schedule(
    const rpc::CreateVirtualClusterRequest &request) {
  VirtualClusterID vc_id =
      VirtualClusterID::FromBinary(request.virtual_cluster_spec().virtual_cluster_id());
  std::vector<VirtualClusterBundleSpec> bundles;
  std::vector<const ResourceRequest *> resource_request_list;
  for (size_t i = 0; i < request.virtual_cluster_spec().bundles_size(); i++) {
    bundles.emplace_back(request.virtual_cluster_spec().bundles(i), vc_id);
    resource_request_list.emplace_back(&bundles[i].GetRequiredResources());
  }
  auto scheduling_result = cluster_resource_scheduler_.Schedule(
      resource_request_list, SchedulingOptions::BundleStrictSpread());
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
