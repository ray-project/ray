#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {
struct VirtualClusterCreationTracker {
  rpc::CreateVirtualClusterRequest request;
  std::unordered_set<NodeID> nodes;
  size_t num_replied_prepares = 0;
  bool has_failed_prepares = false;

  VirtualClusterCreationTracker(rpc::CreateVirtualClusterRequest request)
      : request(request) {}
};

class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
 public:
  GcsVirtualClusterManager(
      instrumented_io_context &io_context,
      const GcsNodeManager &gcs_node_manager,
      ClusterResourceScheduler &cluster_resource_scheduler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  ~GcsVirtualClusterManager() = default;

  void SetGcsResourceManager(const GcsResourceManager *gcs_resource_manager) {
    RAY_CHECK(gcs_resource_manager_ == nullptr);
    gcs_resource_manager_ = gcs_resource_manager;
  }

  void HandleCreateVirtualCluster(rpc::CreateVirtualClusterRequest request,
                                  rpc::CreateVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  std::vector<rpc::PlacementGroupTableData> GetVirtualClusterLoad() const;

 private:
  std::optional<std::unordered_map<NodeID, VirtualClusterBundleSpec>> Schedule(
      const rpc::CreateVirtualClusterRequest &request);

  std::shared_ptr<ResourceReserveInterface> GetLeaseClientFromNode(
      const std::shared_ptr<rpc::GcsNodeInfo> &node);

  int64_t IncrementSeqno() {
    int64_t ret = seqno_;
    seqno_++;
    return ret;
  }

  bool IsInited() const { return gcs_resource_manager_ != nullptr; }
  void Tick();
  void CreateVirtualClusters();
  void ScaleExistingVirtualClustersHack();

  instrumented_io_context &io_context_;
  const GcsNodeManager &gcs_node_manager_;
  // Due to init order in gcs_server we will have to construct gcs_resource_manager later
  // than this class.
  const GcsResourceManager *gcs_resource_manager_ = nullptr;
  ClusterResourceScheduler &cluster_resource_scheduler_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  std::deque<rpc::CreateVirtualClusterRequest> pending_virtual_clusters_;
  std::unordered_map<VirtualClusterID, VirtualClusterCreationTracker>
      ongoing_virtual_clusters_;
  int64_t seqno_ = 1;
};
}  // namespace gcs
}  // namespace ray
