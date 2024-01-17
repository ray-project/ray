#pragma once

#include <atomic>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/node_manager/node_manager_client_pool.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

// A Virtual Cluster has these states:
// - PENDING. A creation request is received, but not yet processed. We use a queue to do
// FIFO for creation.
// - CREATING. Initial creation of a VC: use 2pc to create initial fixed nodes and the
// flex nodes with the min amount of resources. The VC is not yet available for the users.
// - RUNNING. The VC is available for the users. It bears >= min resources. There may
// still be demands and it may scale up or down the flex nodes.
// - RECOVERING. The VC is recovering from a node failure. The min resources are not
// satisfied. Either some fixed nodes are missing, or the flex nodes does not sum up to
// the min resources. Use 2pc to recover the VC.
//
// State transitions:
// - (creation) -> PENDING
// - PENDING -> CREATING -> RUNNING
// - RUNNING -> RECOVERING -> RUNNING
// - * -> (deleted)
//
// Difference of resource allocation between states:
// - CREATING and RECOVERING uses 2pc.
// - RUNNING does not use 2pc, and can incrementally scale up or down flex nodes.
//
// `GcsVirtualClusterManager` works on ticks and events.
// Events:
// - New VC creation request. `HandleCreateVirtualCluster`
//      - add the request to the PENDING queue.
// - New VC deletion request. `HandleRemoveVirtualCluster`
//      - remove the VC from anywhere, releasing the resources.
// - (TODO) Node failure. `GcsNodeManager::HandleNodeRemoved`
//      - If the node affects a VC's fixed node, mark the VC as RECOVERING.
//      - If the node affects a VC's flex node && the VC no longer bears min resources,
//      mark the VC as RECOVERING.
//      - Sends 2pc requests for this VC.
//
// Ticks: Every Tick it drives the VC state machines:
// - (TODO do we need this?) If there's any RECOVERING VC, early returns. (no concurrent
// 2pc)
// - If there's any CREATING VC, early returns.
// - If there's no CREATING VC, and there's any PENDING VC, start creating the first
// pending one. Sends out 2pc.
// - Read the demands and scale up or down flex nodes for RUNNING VCs.
//

class GcsVirtualClusterManager;

struct CreatingVirtualCluster {
  // CREATING itself is a small state machine:
  // - PREPARE: send out 2pc requests to all nodes.
  // - COMMIT: send out 2pc requests to all nodes.
  // Each step can fail due to a node's failure. In that case we return all resources
  // and call a failure. The manager will cancel this VC, put it back to PENDING, and
  // retry later.
  //
  // Call sequence if all good:
  // (start) -> PrepareAll -> [PrepareOneNode] -> AllPrepared -> CommitAll ->
  // [CommitOneNode] -> AllCommitted -> (manager->CreatedVirtualCluster(true))
  // OR if any failure:
  // ... -> (AllPrepared | AllCommitted) -> ReturnAll -> [ReturnOneNode] -> AllReturned ->
  // (manager->CreatedVirtualCluster(false))
  //
  // Note that ReturnOneNode may also fail; but we chose to CHECK-fail it for simplicity
  // TODO: maybe we can just ignore such failure and only log a warning.
  GcsVirtualClusterManager *manager;
  VirtualClusterSpecification vc;
  std::unordered_map<NodeID, VirtualClusterNodesSpec> node_to_vnodes;

  std::atomic<size_t> num_replied_prepares = 0;
  bool has_failed_prepares = false;
  std::atomic<size_t> num_replied_commits = 0;
  bool has_failed_commits = false;

  CreatingVirtualCluster(
      GcsVirtualClusterManager *manager,
      VirtualClusterSpecification vc,
      std::unordered_map<NodeID, VirtualClusterNodesSpec> node_to_vnodes)
      : manager(manager), vc(vc), node_to_vnodes(node_to_vnodes) {}

  void PrepareAll();

 private:
  void PrepareOneNode(NodeID node_id);
  void AllPrepared();
  void CommitOneNode(NodeID node_id);
  void CommitAll();
  void AllCommitted();
  void ReturnOneNode(NodeID node_id);
  void ReturnAll();
};

struct RunningVirtualCluster {
  const VirtualClusterSpecification vc;
  std::unordered_map<NodeID, VirtualClusterNodesSpec> node_to_allocated_vnodes;

  RunningVirtualCluster(
      VirtualClusterSpecification vc,
      std::unordered_map<NodeID, VirtualClusterNodesSpec> node_to_allocated_vnodes)
      : vc(vc), node_to_allocated_vnodes(node_to_allocated_vnodes) {}
};

// TODO: not used yet.
struct RecoveringVirtualCluster {
  VirtualClusterSpecification vc;

  RecoveringVirtualCluster(VirtualClusterSpecification vc) : vc(vc) {}
};

class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
 public:
  GcsVirtualClusterManager(
      instrumented_io_context &io_context,
      const GcsNodeManager &gcs_node_manager,
      ClusterResourceScheduler &cluster_resource_scheduler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool);

  ~GcsVirtualClusterManager() = default;

  void HandleCreateVirtualCluster(rpc::CreateVirtualClusterRequest request,
                                  rpc::CreateVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  std::vector<rpc::PlacementGroupTableData> GetVirtualClusterLoad() const;

  // Internal APIs to the VC state machines.
  void CreatedVirtualCluster(VirtualClusterID vc_id, bool success);

  absl::optional<std::shared_ptr<ResourceReserveInterface>> GetLeaseClientFromAliveNode(
      const NodeID &node_id);

 private:
  std::optional<std::unordered_map<NodeID, VirtualClusterNodesSpec>> Schedule(
      const VirtualClusterSpecification &vc);

  std::shared_ptr<ResourceReserveInterface> GetLeaseClientFromNode(
      const std::shared_ptr<rpc::GcsNodeInfo> &node);

  void Tick();
  void CreateVirtualClusters();

  instrumented_io_context &io_context_;
  const GcsNodeManager &gcs_node_manager_;
  ClusterResourceScheduler &cluster_resource_scheduler_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;

  /// Virtual Clusters.
  std::deque<VirtualClusterSpecification> pending_virtual_clusters_;
  // For some reason, clang refuses to compile this without a std::unique_ptr.
  std::unordered_map<VirtualClusterID, std::unique_ptr<CreatingVirtualCluster>>
      creating_virtual_clusters_;
  std::unordered_map<VirtualClusterID, RunningVirtualCluster> running_virtual_clusters_;
  std::unordered_map<VirtualClusterID, RecoveringVirtualCluster>
      recovering_virtual_clusters_;
};
}  // namespace gcs
}  // namespace ray
