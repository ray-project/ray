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
#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {
/// Ideally, the logic related to resource calculation should be moved from
/// `gcs_resoruce_manager` to `cluster_resource_manager`, and all logic related to
/// resource modification should directly depend on `cluster_resource_manager`, while
/// `gcs_resoruce_manager` is still responsible for processing resource-related RPC
/// request. We will split several small PR to achieve this goal, so as to prevent one PR
/// from being too large to review.
///
/// 1). Remove `node_resource_usages_` related code as it could be calculated from
/// `cluseter_resource_mananger`
/// 2). Move all resource-write-related logic out from `gcs_resource_manager`
/// 3). Move `placement_group_load_` from `gcs_resource_manager` to
/// `placement_group_manager` and make `gcs_resource_manager` depend on
/// `placement_group_manager`

/// Gcs resource manager interface.
/// It is responsible for handing node resource related rpc requests and it is used for
/// actor and placement group scheduling. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public rpc::NodeResourceInfoHandler,
                           public syncer::ReceiverInterface {
 public:
  /// Create a GcsResourceManager.
  ///
  /// \param gcs_table_storage GCS table external storage accessor.
  explicit GcsResourceManager(
      instrumented_io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      ClusterResourceManager &cluster_resource_manager,
      scheduling::NodeID local_node_id_ = scheduling::NodeID::Nil());

  virtual ~GcsResourceManager() {}

  /// Handle the resource update.
  void ConsumeSyncMessage(std::shared_ptr<const syncer::RaySyncMessage> message) override;

  /// Handle get resource rpc request.
  void HandleGetResources(const rpc::GetResourcesRequest &request,
                          rpc::GetResourcesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get available resources of all nodes.
  void HandleGetAllAvailableResources(
      const rpc::GetAllAvailableResourcesRequest &request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle report resource usage rpc come from raylet.
  void HandleReportResourceUsage(const rpc::ReportResourceUsageRequest &request,
                                 rpc::ReportResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all resource usage rpc request.
  void HandleGetAllResourceUsage(const rpc::GetAllResourceUsageRequest &request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Update resources of a node
  /// \param node_id Id of a node.
  /// \param changed_resources The newly added resources for the node. Usually it's
  /// placement group resources.
  void UpdateResources(const NodeID &node_id,
                       absl::flat_hash_map<std::string, double> changed_resources);

  /// Delete resource of a node
  /// \param node_id Id of a node.
  /// \param resource_names The resources to be deleted from the node.
  void DeleteResources(const NodeID &node_id, std::vector<std::string> resource_names);

  /// Handle a node registration.
  ///
  /// \param node The specified node to add.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string ToString() const;

  std::string DebugString() const;

  /// Add resources changed listener.
  void AddResourcesChangedListener(std::function<void()> listener);

  // Update node normal task resources.
  void UpdateNodeNormalTaskResources(const NodeID &node_id,
                                     const rpc::ResourcesData &heartbeat);

  /// Update resource usage of given node.
  ///
  /// \param node_id Node id.
  /// \param request Request containing resource usage.
  void UpdateNodeResourceUsage(const NodeID &node_id,
                               const rpc::ResourcesData &resources);

  /// Process a new resource report from a node, independent of the rpc handler it came
  /// from.
  void UpdateFromResourceReport(const rpc::ResourcesData &data);

  /// Update the placement group load information so that it will be reported through
  /// heartbeat.
  ///
  /// \param placement_group_load placement group load protobuf.
  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load);

  /// Update the resource loads.
  ///
  /// \param data The resource loads reported by raylet.
  void UpdateResourceLoads(const rpc::ResourcesData &data);

 private:
  /// io context. This is to ensure thread safety. Ideally, all public
  /// funciton needs to post job to this io_context.
  instrumented_io_context &io_context_;

  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;

  /// Storage for GCS tables.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Placement group load information that is used for autoscaler.
  absl::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;
  /// The resources changed listeners.
  std::vector<std::function<void()>> resources_changed_listeners_;

  /// Debug info.
  enum CountType {
    GET_RESOURCES_REQUEST = 0,
    UPDATE_RESOURCES_REQUEST = 1,
    DELETE_RESOURCES_REQUEST = 2,
    GET_ALL_AVAILABLE_RESOURCES_REQUEST = 3,
    REPORT_RESOURCE_USAGE_REQUEST = 4,
    GET_ALL_RESOURCE_USAGE_REQUEST = 5,
    CountType_MAX = 6,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  ClusterResourceManager &cluster_resource_manager_;
  scheduling::NodeID local_node_id_;
};

}  // namespace gcs
}  // namespace ray

namespace std {
template <>
struct hash<google::protobuf::Map<std::string, double>> {
  size_t operator()(google::protobuf::Map<std::string, double> const &k) const {
    size_t seed = k.size();
    for (auto &elem : k) {
      seed ^= std::hash<std::string>()(elem.first);
      seed ^= std::hash<double>()(elem.second);
    }
    return seed;
  }
};

template <>
struct equal_to<google::protobuf::Map<std::string, double>> {
  bool operator()(const google::protobuf::Map<std::string, double> &left,
                  const google::protobuf::Map<std::string, double> &right) const {
    if (left.size() != right.size()) {
      return false;
    }
    for (const auto &entry : left) {
      auto iter = right.find(entry.first);
      if (iter == right.end() || iter->second != entry.second) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace std
