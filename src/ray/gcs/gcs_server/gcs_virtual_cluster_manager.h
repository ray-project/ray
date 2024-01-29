#pragma once

#include <atomic>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
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
// - CREATING. Initial creation; 2pc sent. The VC is not yet available for the users.
// - RUNNING. The VC is available for the users. It bears >= min resources. There may
// still be demands and it may scale up or down the flex nodes.
// - UNSATISFIED. The VC is running, but does not bear min resources. There may still be
// some tasks running.
// - RECOVERING. Recovering from UNSATISFIED; 2pc sent.
//
// State transitions:
// - (creation)     ->                      PENDING
// - PENDING        -(Tick, send out 2pc)-> CREATING
// - CREATING       -(2pc done)->           RUNNING | PENDING (if 2pc failed)
// - RUNNING        -(node failure)->       UNSATISFIED
// - UNSATISFIED    -(Tick, send out 2pc)-> RECOVERING
// - RECOVERING     -(2pc done)->           RUNNING | UNSATISFIED (if 2pc failed)
// - *              -(delete)->             (deleted)
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
//
// NOTE: there are edge cases where 2pc succeeded and after which the node fails. Those
// VCs will make it in RUNNING for a while, and then go to UNSATISFIED in the next Tick.
// TODO: If we move out the RUNNING -> UNSATISFIED check from the Tick to the NodeRemoved
// event callback, we need to be careful about this "missed event" problem.
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

// Maps each physical node to all its assigned fixed nodes.
using ScheduledNodes = std::unordered_map<NodeID, VirtualClusterNodesSpec>;

// A transaction to create a list of vnodes on multiple physical nodes, using 2pc.
// This is used in CREATING and RECOVERING virtual clusters.
// For each node, we specify a list of fixed vnodes, as well as a flex node with desired
// amounts of resources.
// The process is a state machine:
// 1. PrepareAll: send out 2pc requests to all nodes. Waits for all replies.
// 2. CommitAll: send out 2pc requests to all nodes. Waits for all replies.
// 3. invoke callback(succ).
// If any of the reply fails, we call ReturnAll, sending out Return to all nodes, and
// invoke callback(fail).
//
// Precondition: each node must have never seen the VC before.
struct CreateVnodesTransaction {
  // Call sequence if all good:
  // (start) -> PrepareAll -> [PrepareOneNode] -> AllPrepared -> CommitAll ->
  // [CommitOneNode] -> AllCommitted -> (manager->CreatedVirtualCluster(true))
  // OR if any failure:
  // ... -> (AllPrepared | AllCommitted) -> ReturnAll -> [ReturnOneNode] -> AllReturned ->
  // (manager->CreatedVirtualCluster(false))
  //
  // Note that ReturnOneNode may also fail; but we chose to CHECK-fail it for the demo.
  // This is NOT production ready since the node can fail before we send the req, and it's
  // very likely we get a failure in a Return.
  // TODO: maybe we can just ignore such failure and only log a warning.

  // `manager` is used to get lease clients to talk to nodes.
  GcsVirtualClusterManager *manager;
  VirtualClusterID vc_id;
  // {node_id -> [vnodes]}.
  ScheduledNodes scheduled_nodes;
  // TODO: support flex nodes.
  std::function<void(bool)> finish_callback;

  std::atomic<size_t> num_replied_prepares = 0;
  bool has_failed_prepares = false;
  std::atomic<size_t> num_replied_commits = 0;
  bool has_failed_commits = false;

  CreateVnodesTransaction(GcsVirtualClusterManager *manager,
                          VirtualClusterID vc_id,
                          ScheduledNodes scheduled_nodes,
                          std::function<void(bool)> finish_callback)
      : manager(manager),
        vc_id(vc_id),
        scheduled_nodes(std::move(scheduled_nodes)),
        finish_callback(finish_callback) {}
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

// We can have multiple fixed_size_nodes in a VC, each with their own scheduling policy,
// and we need to keep that info for recovery. On the other hand, we only want to send
// out 1 2pc transaction for each VC, so we need to group the vnodes.
// From: {fixed_size_nodes (by ID) -> {node -> [vnodes]}} `scheduled_fixed_size_nodes`
// To: {node -> [vnodes]} `transaction->scheduled_nodes`
//
// Populates node_id for each vnode. If node_id already exists, overwrites.
struct CreatingVirtualCluster {
  // `manager` is used to callback to inform that the VC is created or failed.
  GcsVirtualClusterManager *manager;
  VirtualClusterSpecification vc;
  // Scheduled vnodes, one for each fixed_size_nodes.
  std::vector<ScheduledNodes> scheduled_fixed_size_nodes;

  // The transaction to create the vnodes.
  std::unique_ptr<CreateVnodesTransaction> transaction;

  CreatingVirtualCluster(GcsVirtualClusterManager *manager,
                         VirtualClusterSpecification vc,
                         std::vector<ScheduledNodes> scheduled_fixed_size_nodes);
  void PrepareAll();
};

struct RunningVirtualCluster {
  const VirtualClusterSpecification vc;
  // For each fixed_size_nodes, the mapping {node -> [vnodes]}.
  std::vector<ScheduledNodes> allocated_fixed_size_nodes;

  bool IsSatisfied(const std::unordered_set<NodeID> &alive_nodes) const;

  RunningVirtualCluster(VirtualClusterSpecification vc,
                        std::vector<ScheduledNodes> allocated_fixed_size_nodes)
      : vc(vc), allocated_fixed_size_nodes(std::move(allocated_fixed_size_nodes)) {}
};

// A VC that is unsatisfied, and we have scheduled a 2pc recovery, waiting for replies.
// For each fixed_size_nodes, we have 1 entry in allocated_fixed_size_nodes and 1 entry in
// recovering_fixed_size_nodes, even if the fixed_size_nodes is empty or is not affected
// by the failure.
//
// Populates node_id for each vnode. If node_id already exists, overwrites.
struct RecoveringVirtualCluster {
  GcsVirtualClusterManager *manager;
  // Previously running VC. After recovery we will go back to this state.
  // If the VC had more than minimum resources, it would be trimmed to the min desired
  // amount in ctor of this class.
  std::shared_ptr<RunningVirtualCluster> running_vc;
  // For each fixed_size_nodes, the mapping {node -> [vnodes]} that are dead and needs a
  // recovery.
  std::vector<ScheduledNodes> recovering_fixed_size_nodes;
  // The transaction to recover the vnodes.
  std::unique_ptr<CreateVnodesTransaction> transaction;

  RecoveringVirtualCluster(GcsVirtualClusterManager *manager,
                           std::shared_ptr<RunningVirtualCluster> running_vc,
                           std::vector<ScheduledNodes> recovering_fixed_size_nodes);
  void PrepareAll();
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
  void RecoveredVirtualCluster(VirtualClusterID vc_id, bool success);

  absl::optional<std::shared_ptr<ResourceReserveInterface>> GetLeaseClientFromAliveNode(
      const NodeID &node_id);

 private:
  // Schedules a fixed-size-nodes, which may already have some nodes allocated.
  // This is internal; Use ScheduleNew or ScheduleRecovery instead.
  std::optional<ScheduledNodes> Schedule(
      VirtualClusterID vc_id,
      const std::vector<VirtualClusterNodeSpec> &fixed_size_nodes,
      rpc::SchedulingPolicy policy,
      const std::unordered_set<NodeID> &already_used_nodes);

  // Schedules a new VC. If can't schedule, returns nullopt.
  std::optional<std::unique_ptr<CreatingVirtualCluster>> ScheduleNew(
      const VirtualClusterSpecification &vc);

  // Schedules a VC recovery. If can't schedule, returns nullopt.
  std::optional<std::unique_ptr<RecoveringVirtualCluster>> ScheduleRecovery(
      std::shared_ptr<RunningVirtualCluster> running_vc,
      const std::unordered_set<NodeID> &still_alive_nodes);

  std::shared_ptr<ResourceReserveInterface> GetLeaseClientFromNode(
      const std::shared_ptr<rpc::GcsNodeInfo> &node);

  void Tick();
  void CreateVirtualClusters();
  void RecoverVirtualClusters();

  instrumented_io_context &io_context_;
  const GcsNodeManager &gcs_node_manager_;
  ClusterResourceScheduler &cluster_resource_scheduler_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;

  /// Virtual Clusters in 5 states.
  std::deque<VirtualClusterSpecification> pending_virtual_clusters_;
  std::unordered_map<VirtualClusterID, std::unique_ptr<CreatingVirtualCluster>>
      creating_virtual_clusters_;
  std::unordered_map<VirtualClusterID, std::shared_ptr<RunningVirtualCluster>>
      running_virtual_clusters_;
  // An unsatisfied VC is a RUNNING VC that does not bear min resources, due to node
  // failure. In the next Tick we will try to recover by scheduling and sending out 2pc.
  // If scheduling failed, it lies in this map until the next Tick.
  //
  // Note: we don't store the failed nodes here, because in the next Tick that list may
  // change and we always consult to source-of-truth in Tick.
  std::unordered_map<VirtualClusterID, std::shared_ptr<RunningVirtualCluster>>
      unsatisfied_virtual_clusters_;
  std::unordered_map<VirtualClusterID, std::unique_ptr<RecoveringVirtualCluster>>
      recovering_virtual_clusters_;
};
}  // namespace gcs
}  // namespace ray
