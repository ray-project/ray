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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/component_syncer.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Gcs resource manager interface.
/// It is responsible for handing node resource related rpc requests and it is used for
/// actor and placement group scheduling. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public rpc::NodeResourceInfoHandler, public syncing::Receiver {
 public:
  /// Create a GcsResourceManager.
  ///
  /// \param main_io_service The main event loop.
  /// \param gcs_publisher GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  explicit GcsResourceManager(instrumented_io_context &main_io_service,
                              std::shared_ptr<GcsPublisher> gcs_publisher,
                              std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                              bool redis_broadcast_enabled);

  virtual ~GcsResourceManager() {}

  /// Handle get resource rpc request.
  void HandleGetResources(const rpc::GetResourcesRequest &request,
                          rpc::GetResourcesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle update resource rpc request.
  void HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                             rpc::UpdateResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle delete resource rpc request.
  void HandleDeleteResources(const rpc::DeleteResourcesRequest &request,
                             rpc::DeleteResourcesReply *reply,
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

  /// Get the resources of all nodes in the cluster.
  ///
  /// \return The resources of all nodes in the cluster.
  const absl::flat_hash_map<NodeID, SchedulingResources> &GetClusterResources() const;

  void Update(std::shared_ptr<syncing::RaySyncMessage> message) override {
    main_io_service_.post([this, message](){
      rpc::ResourcesData resources;
      resources.ParseFromString(message->sync_message());
      resources.set_node_id(message->node_id());

      NodeID node_id = NodeID::FromBinary(message->node_id());
      RAY_LOG(DEBUG) << "DBG: MSG RECEIVED: " << node_id.Hex() << " "
                     << message->component_id();
      if (message->component_id() == syncing::RayComponentId::SCHEDULER) {
        if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
          UpdateNodeNormalTaskResources(node_id, resources);
        } else {
          if (node_resource_usages_.count(node_id) == 0 ||
              resources.resources_available_changed()) {
            const auto &resource_changed = MapFromProtobuf(resources.resources_available());
            SetAvailableResources(node_id, ResourceSet(resource_changed));
          }
        }
      }

      auto iter = node_resource_usages_.find(node_id);
      if (iter == node_resource_usages_.end()) {
        auto resources_data = std::make_shared<rpc::ResourcesData>();
        resources_data->CopyFrom(resources);
        node_resource_usages_[node_id] = *resources_data;
        return;
      }

      if (message->component_id() == syncing::RayComponentId::RESOURCE_MANAGER) {
        if (resources.resources_total_size() > 0) {
          (*iter->second.mutable_resources_total()) = resources.resources_total();
        }
        if (resources.resources_available_changed()) {
          (*iter->second.mutable_resources_available()) = resources.resources_available();
        }
      } else {
        if (resources.resource_load_changed()) {
          (*iter->second.mutable_resource_load()) = resources.resource_load();
        }
        if (resources.resources_normal_task_changed()) {
          (*iter->second.mutable_resources_normal_task()) =
              resources.resources_normal_task();
        }
        (*iter->second.mutable_resource_load_by_shape()) =
            resources.resource_load_by_shape();
        iter->second.set_cluster_full_of_actors_detected(
            resources.cluster_full_of_actors_detected());
      }
    },
      "UP");
  }

  /// Handle a node registration.
  ///
  /// \param node The specified node to add.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Set the available resources of the specified node.
  ///
  /// \param node_id Id of a node.
  /// \param resources Available resources of a node.
  void SetAvailableResources(const NodeID &node_id, const ResourceSet &resources);

  /// Acquire resources from the specified node. It will deduct directly from the node
  /// resources.
  ///
  /// \param node_id Id of a node.
  /// \param required_resources Resources to apply for.
  /// \return True if acquire resources successfully. False otherwise.
  bool AcquireResources(const NodeID &node_id, const ResourceSet &required_resources);

  /// Release the resources of the specified node. It will be added directly to the node
  /// resources.
  ///
  /// \param node_id Id of a node.
  /// \param acquired_resources Resources to release.
  /// \return True if release resources successfully. False otherwise.
  bool ReleaseResources(const NodeID &node_id, const ResourceSet &acquired_resources);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  std::string ToString() const;

  std::string DebugString() const;

  /// Update the total resources and available resources of the specified node.
  ///
  /// \param node_id Id of a node.
  /// \param changed_resources Changed resources of a node.
  void UpdateResourceCapacity(
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &changed_resources);

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

  /// Move the lightweight heartbeat information for broadcast into the buffer. This
  /// method MOVES the information, clearing an internal buffer, so it is NOT idempotent.
  ///
  /// \param buffer return parameter
  void GetResourceUsageBatchForBroadcast(rpc::ResourceUsageBroadcastData &buffer)
      LOCKS_EXCLUDED(resource_buffer_mutex_);

 private:
  /// Delete the scheduling resources of the specified node.
  ///
  /// \param node_id Id of a node.
  /// \param deleted_resources Deleted resources of a node.
  void DeleteResources(const NodeID &node_id,
                       const std::vector<std::string> &deleted_resources);

  /// Send any buffered resource usage as a single publish.
  void SendBatchedResourceUsage();

  /// Prelocked version of GetResourceUsageBatchForBroadcast. This is necessary for need
  /// the functionality as part of a larger transaction.
  void GetResourceUsageBatchForBroadcast_Locked(rpc::ResourceUsageBatchData &buffer)
      EXCLUSIVE_LOCKS_REQUIRED(resource_buffer_mutex_);

  instrumented_io_context &main_io_service_;
  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;
  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;

  /// Protect the lightweight heartbeat deltas which are accessed by different threads.
  absl::Mutex resource_buffer_mutex_;
  // TODO (Alex): This buffer is only needed for the legacy redis based broadcast.
  /// A buffer containing the lightweight heartbeats since the last broadcast.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> resources_buffer_
      GUARDED_BY(resource_buffer_mutex_);
  /// A buffer containing the lightweight heartbeats since the last broadcast.
  rpc::ResourceUsageBroadcastData resources_buffer_proto_
      GUARDED_BY(resource_buffer_mutex_);
  /// A publisher for publishing gcs messages.
  std::shared_ptr<GcsPublisher> gcs_publisher_;
  /// Storage for GCS tables.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Whether or not to broadcast resource usage via redis.
  const bool redis_broadcast_enabled_;
  /// Map from node id to the scheduling resources of the node.
  absl::flat_hash_map<NodeID, SchedulingResources> cluster_scheduling_resources_;
  /// Placement group load information that is used for autoscaler.
  absl::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;
  /// Normal task resources could be uploaded by 1) Raylets' periodical reporters; 2)
  /// Rejected RequestWorkerLeaseReply. So we need the timestamps to decide whether an
  /// upload is latest.
  absl::flat_hash_map<NodeID, int64_t> latest_resources_normal_task_timestamp_;
  /// The resources changed listeners.
  std::vector<std::function<void()>> resources_changed_listeners_;
  /// Max batch size for broadcasting
  size_t max_broadcasting_batch_size_;

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
};

}  // namespace gcs
}  // namespace ray
