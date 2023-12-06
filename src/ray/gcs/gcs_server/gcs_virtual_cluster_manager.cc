#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "ray/common/asio/asio_util.h"

namespace ray {
namespace gcs {

namespace {

// Represents a parsed view of a resource shape.
struct RenamedResources {
  absl::flat_hash_map<VirtualClusterID, absl::flat_hash_map<std::string, int>>
      vc_resources;
  absl::flat_hash_map<std::string, int> vanilla_resources;

  // Parses a resource shape of {name -> val} by parsing the names as VC renamed ones.
  static RenamedResources Parse(const google::protobuf::Map<std::string, double> &shape) {
    RenamedResources ret;
    for (const auto &[name, val] : shape) {
      auto label = VirtualClusterBundleResourceLabel::Parse(name);
      if (label.has_value()) {
        ret.vc_resources[label->vc_id][label->original_resource] = val;
      } else {
        ret.vanilla_resources[name] = val;
      }
    }
    return ret;
  }

  int get_from_vanilla_or_zero(const std::string &name) const {
    auto name_it = vanilla_resources.find(name);
    if (name_it == vanilla_resources.end()) {
      return 0;
    }
    return name_it->second;
  }

  int get_from_vc_or_zero(VirtualClusterID vc_id, const std::string &name) const {
    auto it = vc_resources.find(vc_id);
    if (it == vc_resources.end()) {
      return 0;
    }
    const auto &map = it->second;
    auto name_it = map.find(name);
    if (name_it == map.end()) {
      return 0;
    }
    return name_it->second;
  }
};

// Figure out what to do with each demand.
// Only supports demand that all resources are in a same VC.
// Returns a schedule plan:
// - If nullopt: nothing we can do (see below)
// - If non null: plan to take these vc resources, and rename these vanilla resources.
// NOTE: the return only contains *changes* or *delta* of the resources, not total.
//
// How we plan:
// - If it can be fulilled by vc_available_resources alone: do nothing
// - If it can be fulilled by vc_available_resources +
// vanilla_available_resources: plan some renames
// - If it can't be fulfilled -> do nothing. TODO: ask parent.
std::optional<VirtualClusterBundleSpec> PlanForDemand(const rpc::ResourceDemand &demand,
                                                      RenamedResources &availables) {
  // Prevent accidental changing
  const auto &const_availables = availables;
  if (demand.shape().empty()) {
    return std::nullopt;
  }
  auto parsed_demands = RenamedResources::Parse(demand.shape());
  if (parsed_demands.vc_resources.size() == 0) {
    // non-VC demand
    return std::nullopt;
  }
  if (parsed_demands.vc_resources.size() > 1) {
    // multi-VC demand, refuse to schedule and complain.
    std::string all_vc_ids;
    for (const auto &[vc_id, _] : parsed_demands.vc_resources) {
      all_vc_ids.append(", ");
      all_vc_ids.append(vc_id.Hex());
    }
    RAY_LOG(ERROR)
        << "Refuse to schedule a demand with resources from more than 1 virtual "
           "clusters: "
        << all_vc_ids;
    return std::nullopt;
  }

  // Single VC demand.
  auto &[vc_id, vc_demands] = *parsed_demands.vc_resources.begin();
  bool feasible = true;
  for (const auto &[name, val] : vc_demands) {
    if (const_availables.get_from_vc_or_zero(vc_id, name) +
            const_availables.get_from_vanilla_or_zero(name) <
        val) {
      feasible = false;
    }
  }
  if (!feasible) {
    // TODO: ask parent for more
    return std::nullopt;
  }
  rpc::VirtualClusterBundle bundle;
  auto &resources = *bundle.mutable_resources();

  for (const auto &[name, val] : vc_demands) {
    int vc_planned = std::min(availables.get_from_vc_or_zero(vc_id, name), val);
    if (vc_planned > 0) {
      availables.vc_resources[vc_id][name] -= vc_planned;
    }
    int vanilla_planned = val - vc_planned;
    RAY_CHECK(vanilla_planned >= 0);
    if (vanilla_planned > 0) {
      resources[name] = vanilla_planned;
    }
  }
  return std::make_optional(VirtualClusterBundleSpec(std::move(bundle), vc_id));
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
    lease_client->ReturnVirtualClusterBundle(
        vc_id,
        -1,  // all seqno for this vc id.
        [request](const Status &status,
                  const rpc::ReturnVirtualClusterBundleReply &reply) {
          // TODO: error handling. One node may be already dead and return GrpcUnavailable
          // which should be fine.
          if (!status.ok()) {
            RAY_LOG(WARNING) << "Remove virtual cluster failed" << status << " request "
                             << request.DebugString();
          }
        });
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::vector<rpc::PlacementGroupTableData>
GcsVirtualClusterManager::GetVirtualClusterLoad() const {
  if (!IsInited()) {
    return {};
  }
  RAY_LOG(INFO) << "GetVirtualClusterLoad " << seqno_ << " pending count "
                << pending_virtual_clusters_.size() << " ongoing count "
                << ongoing_virtual_clusters_.size();
  std::vector<rpc::PlacementGroupTableData> load;
  for (const auto &request : pending_virtual_clusters_) {
    rpc::PlacementGroupTableData data;
    for (const auto &vc_bundle : request.virtual_cluster_spec().bundles()) {
      auto *pg_bundle = data.add_bundles();
      pg_bundle->mutable_unit_resources()->insert(vc_bundle.resources().begin(),
                                                  vc_bundle.resources().end());
    }
    data.set_strategy(rpc::PlacementStrategy::STRICT_SPREAD);
    RAY_LOG(INFO) << "GetVirtualClusterLoad load" << data.DebugString();
    load.emplace_back(data);
  }
  return load;
}

void GcsVirtualClusterManager::Tick() {
  CreateVirtualClusters();
  ScaleExistingVirtualClustersHack();

  execute_after(
      io_context_,
      [this] { Tick(); },
      std::chrono::milliseconds(1000) /* milliseconds */);
}

void GcsVirtualClusterManager::ScaleExistingVirtualClustersHack() {
  // Current limitations:
  // - on renames available resources, does not talk to parent/physical node to scale
  // - doing per-node renaming. No "work stealing".
  // - not respecting VC limitations (assumes infinite scaling up)
  // - no scale downs
  // - no fairness: just fulfill loads one by one
  // - 1 demand only asks for 1 VC's resources
  if (!IsInited()) {
    return;
  }

  const auto &resources_report_by_node = gcs_resource_manager_->NodeResourceReportView();

  RAY_LOG(INFO) << "resources_report_by_node " << resources_report_by_node.size();

  for (const auto &[node_id, resources_data] : resources_report_by_node) {
    // Current code: find vanilla availables, find renamed loads, make some renames if
    // needed
    auto available_resources =
        RenamedResources::Parse(resources_data.resources_available());

    RAY_LOG(INFO) << "available_resources "
                  << available_resources.vanilla_resources.size();

    for (const rpc::ResourceDemand &demand :
         resources_data.resource_load_by_shape().resource_demands()) {
      auto maybe_plan = PlanForDemand(demand, available_resources);
      if (maybe_plan.has_value()) {
        RAY_LOG(INFO) << "PlanForDemand " << maybe_plan->DebugString() << ", node id "
                      << node_id;
        NodeID node_id_copied = node_id;  // strange compiler hack
        VirtualClusterID vc_id = maybe_plan->GetVirtualClusterId();
        int64_t seqno = IncrementSeqno();
        // TODO: make it a function
        const auto lease_client =
            GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
        lease_client->PrepareVirtualClusterBundle(
            *maybe_plan,
            seqno,
            [vc_id, node_id_copied, seqno, this](
                const Status &status,
                const rpc::PrepareVirtualClusterBundleReply &reply) {
              if (!reply.success()) {
                // Unfortunate. Maybe print log. No need to return anything.
                return;
              }
              const auto lease_client = GetLeaseClientFromNode(
                  gcs_node_manager_.GetAliveNode(node_id_copied).value());
              lease_client->CommitVirtualClusterBundle(
                  vc_id,
                  seqno,
                  [](const Status &status,
                     const rpc::CommitVirtualClusterBundleReply &reply) {
                    RAY_CHECK(status.ok());
                  });
            });
      }
    }
  }
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

  if (node_to_bundle->empty()) {
    // No resources needed. Just return.
    return;
  }

  int64_t seqno = IncrementSeqno();

  ongoing_virtual_clusters_.emplace(vc_id, request);

  for (const auto &entry : *node_to_bundle) {
    NodeID node_id = entry.first;
    ongoing_virtual_clusters_.at(vc_id).nodes.emplace(node_id);
    const auto lease_client =
        GetLeaseClientFromNode(gcs_node_manager_.GetAliveNode(node_id).value());
    lease_client->PrepareVirtualClusterBundle(
        entry.second,
        seqno,
        [vc_id, seqno, this](const Status &status,
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
                  seqno,
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
                  seqno,
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
  for (int i = 0; i < request.virtual_cluster_spec().bundles_size(); i++) {
    bundles.emplace_back(request.virtual_cluster_spec().bundles(i), vc_id);
    resource_request_list.emplace_back(&bundles[i].GetRequiredResources());
  }
  if (resource_request_list.empty()) {
    return {};
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
