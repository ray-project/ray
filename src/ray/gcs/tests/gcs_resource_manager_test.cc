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

#include "ray/gcs/gcs_resource_manager.h"

#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_node_manager.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {

using ::testing::_;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest()
      : cluster_resource_manager_(io_service_),
        gcs_node_manager_(std::make_unique<gcs::MockGcsNodeManager>()) {
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, cluster_resource_manager_, *gcs_node_manager_, NodeID::FromRandom());
  }

  void UpdateFromResourceViewSync(
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const absl::flat_hash_map<std::string, double> &total_resources,
      int64_t idle_ms = 0,
      bool is_draining = false,
      int64_t draining_deadline_timestamp_ms = -1) {
    syncer::ResourceViewSyncMessage resource_view_sync_message;
    for (const auto &resource : available_resources) {
      (*resource_view_sync_message.mutable_resources_available())[resource.first] =
          resource.second;
    }
    for (const auto &resource : total_resources) {
      (*resource_view_sync_message.mutable_resources_total())[resource.first] =
          resource.second;
    }
    resource_view_sync_message.set_idle_duration_ms(idle_ms);
    resource_view_sync_message.set_is_draining(is_draining);
    resource_view_sync_message.set_draining_deadline_timestamp_ms(
        draining_deadline_timestamp_ms);
    gcs_resource_manager_->UpdateFromResourceView(node_id, resource_view_sync_message);
  }

  instrumented_io_context io_service_;
  ClusterResourceManager cluster_resource_manager_;
  std::unique_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
};

TEST_F(GcsResourceManagerTest, TestBasic) {
  const std::string cpu_resource = "CPU";
  absl::flat_hash_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;

  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert(resource_map.begin(), resource_map.end());
  // Add node resources.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get and check cluster resources.
  const auto &resource_view = cluster_resource_manager_.GetResourceView();
  ASSERT_EQ(1, resource_view.size());
  scheduling::NodeID scheduling_node_id(node->node_id());
  ASSERT_TRUE(resource_view.at(scheduling_node_id).GetLocalView().labels.empty());

  auto resource_request =
      ResourceMapToResourceRequest(resource_map, /*requires_object_store_memory=*/false);

  // Test `AcquireResources`.
  ASSERT_TRUE(cluster_resource_manager_.HasAvailableResources(
      scheduling_node_id,
      resource_request,
      /*ignore_object_store_memory_requirement=*/true));
  ASSERT_TRUE(cluster_resource_manager_.SubtractNodeAvailableResources(scheduling_node_id,
                                                                       resource_request));
  ASSERT_FALSE(cluster_resource_manager_.HasAvailableResources(
      scheduling_node_id,
      resource_request,
      /*ignore_object_store_memory_requirement=*/true));

  // Test `ReleaseResources`.
  ASSERT_TRUE(cluster_resource_manager_.AddNodeAvailableResources(
      scheduling_node_id, resource_request.GetResourceSet()));
}

TEST_F(GcsResourceManagerTest, TestResourceUsageAPI) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  auto node_id = NodeID::FromBinary(node->node_id());
  rpc::GetAllResourceUsageRequest get_all_request;
  rpc::GetAllResourceUsageReply get_all_reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply, send_reply_callback);
  ASSERT_EQ(get_all_reply.resource_usage_data().batch().size(), 0);

  gcs_resource_manager_->OnNodeAdd(*node);

  syncer::ResourceViewSyncMessage resource_view_sync_message;
  (*resource_view_sync_message.mutable_resources_available())["CPU"] = 2;
  (*resource_view_sync_message.mutable_resources_total())["CPU"] = 2;
  gcs_resource_manager_->UpdateNodeResourceUsage(node_id, resource_view_sync_message);

  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply, send_reply_callback);
  ASSERT_EQ(get_all_reply.resource_usage_data().batch().size(), 1);

  gcs_resource_manager_->OnNodeDead(node_id);
  rpc::GetAllResourceUsageReply get_all_reply2;
  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply2, send_reply_callback);
  ASSERT_EQ(get_all_reply2.resource_usage_data().batch().size(), 0);

  // This will be ignored since the node is dead.
  gcs_resource_manager_->UpdateNodeResourceUsage(node_id, resource_view_sync_message);
  rpc::GetAllResourceUsageReply get_all_reply3;
  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply3, send_reply_callback);
  ASSERT_EQ(get_all_reply3.resource_usage_data().batch().size(), 0);
}

TEST_F(GcsResourceManagerTest, TestResourceUsageFromDifferentSyncMsgs) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 10});
  gcs_resource_manager_->OnNodeAdd(*node);

  syncer::ResourceViewSyncMessage resource_view_sync_message;
  resource_view_sync_message.mutable_resources_total()->insert({"CPU", 5});
  resource_view_sync_message.mutable_resources_available()->insert({"CPU", 5});

  // Update resource usage from resource view.
  {
    ASSERT_FALSE(gcs_resource_manager_->NodeResourceReportView()
                     .at(NodeID::FromBinary(node->node_id()))
                     .cluster_full_of_actors_detected());
    gcs_resource_manager_->UpdateFromResourceView(NodeID::FromBinary(node->node_id()),
                                                  resource_view_sync_message);
    ASSERT_EQ(
        cluster_resource_manager_.GetNodeResources(scheduling::NodeID(node->node_id()))
            .total.GetResourceMap()
            .at("CPU"),
        5);

    ASSERT_FALSE(gcs_resource_manager_->NodeResourceReportView()
                     .at(NodeID::FromBinary(node->node_id()))
                     .cluster_full_of_actors_detected());
  }

  // Update from syncer COMMANDS will not update the resources, but the
  // cluster_full_of_actors_detected flag. (This is how NodeManager currently
  // updates potential resources deadlock).
  {
    gcs_resource_manager_->UpdateClusterFullOfActorsDetected(
        NodeID::FromBinary(node->node_id()), true);

    // Still 5 because the syncer COMMANDS message is ignored.
    ASSERT_EQ(
        cluster_resource_manager_.GetNodeResources(scheduling::NodeID(node->node_id()))
            .total.GetResourceMap()
            .at("CPU"),
        5);

    // The flag is updated.
    ASSERT_TRUE(gcs_resource_manager_->NodeResourceReportView()
                    .at(NodeID::FromBinary(node->node_id()))
                    .cluster_full_of_actors_detected());
  }
}

TEST_F(GcsResourceManagerTest, TestSetAvailableResourcesWhenNodeDead) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 10});

  gcs_resource_manager_->OnNodeAdd(*node);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 1);

  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_resource_manager_->OnNodeDead(node_id);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 0);

  syncer::ResourceViewSyncMessage resource_view_sync_message;
  resource_view_sync_message.mutable_resources_total()->insert({"CPU", 5});
  resource_view_sync_message.mutable_resources_available()->insert({"CPU", 5});
  gcs_resource_manager_->UpdateFromResourceView(node_id, resource_view_sync_message);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 0);
}

TEST_F(GcsResourceManagerTest, TestNodeLabels) {
  const std::string cpu_resource = "CPU";
  absl::flat_hash_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;
  absl::flat_hash_map<std::string, std::string> labels = {{"key", "value"},
                                                          {"gpu_type", "a100"}};

  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert(resource_map.begin(), resource_map.end());
  node->mutable_labels()->insert(labels.begin(), labels.end());
  // Add node resources.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get and check cluster resources.
  const auto &resource_view = cluster_resource_manager_.GetResourceView();
  ASSERT_EQ(1, resource_view.size());
  scheduling::NodeID scheduling_node_id(node->node_id());
  ASSERT_EQ(resource_view.at(scheduling_node_id).GetLocalView().labels, labels);
}

TEST_F(GcsResourceManagerTest, TestGetDrainingNodes) {
  auto node1 = GenNodeInfo();
  node1->mutable_resources_total()->insert({"CPU", 10});
  gcs_resource_manager_->OnNodeAdd(*node1);
  UpdateFromResourceViewSync(
      NodeID::FromBinary(node1->node_id()),
      {/* available */ {"CPU", 10}},
      /* total*/ {{"CPU", 10}},
      /* idle_duration_ms */ 8,
      /* is_draining */ true,
      /* draining_deadline_timestamp_ms */ std::numeric_limits<int64_t>::max());

  auto node2 = GenNodeInfo();
  node2->mutable_resources_total()->insert({"CPU", 1});
  gcs_resource_manager_->OnNodeAdd(*node2);
  UpdateFromResourceViewSync(NodeID::FromBinary(node2->node_id()),
                             {/* available */ {"CPU", 1}},
                             /* total*/ {{"CPU", 1}},
                             /* idle_duration_ms */ 5,
                             /* is_draining */ false,
                             /* draining_deadline_timestamp_ms */ -1);

  rpc::GetDrainingNodesRequest request;
  rpc::GetDrainingNodesReply reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  gcs_resource_manager_->HandleGetDrainingNodes(request, &reply, send_reply_callback);
  ASSERT_EQ(reply.draining_nodes_size(), 1);
  ASSERT_EQ(reply.draining_nodes(0).node_id(), node1->node_id());
}

}  // namespace ray
