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

// clang-format off
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "src/ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"
// clang-format on

namespace ray {

namespace gcs {
using ::testing::_;

// Test suite for AutoscalerState related functionality.
class GcsAutoscalerStateManagerTest : public ::testing::Test {
 public:
  GcsAutoscalerStateManagerTest() : cluster_resource_manager_(io_service_) {
    gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
        io_service_, cluster_resource_manager_, NodeID::FromRandom());
    gcs_node_manager_ = std::make_shared<MockGcsNodeManager>();

    gcs_autoscaler_state_manager_.reset(new GcsAutoscalerStateManager(
        cluster_resource_manager_, *gcs_resource_manager_, *gcs_node_manager_));
  }

 protected:
  instrumented_io_context io_service_;
  ClusterResourceManager cluster_resource_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<MockGcsNodeManager> gcs_node_manager_;
  std::unique_ptr<GcsAutoscalerStateManager> gcs_autoscaler_state_manager_;

 public:
  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
    gcs_resource_manager_->OnNodeAdd(*node);
  }

  void RemoveNode(const NodeID &node_id) {
    gcs_node_manager_->alive_nodes_.erase(node_id);
    gcs_resource_manager_->OnNodeDead(node_id);
  }

  void CheckNodeResources(
      const rpc::autoscaler::NodeState &node_state,
      const absl::flat_hash_map<std::string, double> &total_resources,
      const absl::flat_hash_map<std::string, double> &available_resources) {
    ASSERT_EQ(node_state.total_resources_size(), total_resources.size());
    ASSERT_EQ(node_state.available_resources_size(), available_resources.size());
    for (const auto &resource : total_resources) {
      ASSERT_EQ(node_state.total_resources().at(resource.first), resource.second);
    }
    for (const auto &resource : available_resources) {
      ASSERT_EQ(node_state.available_resources().at(resource.first), resource.second);
    }
  }

  rpc::autoscaler::GetClusterResourceStateReply GetClusterResourceStateSync() {
    rpc::autoscaler::GetClusterResourceStateRequest request;
    rpc::autoscaler::GetClusterResourceStateReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    gcs_autoscaler_state_manager_->HandleGetClusterResourceState(
        request, &reply, send_reply_callback);
    return reply;
  }

  void UpdateFromResourceReportSync(
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const absl::flat_hash_map<std::string, double> &total_resources,
      bool available_resources_changed) {
    rpc::ResourcesData resources_data;
    Mocker::FillResourcesData(resources_data,
                              node_id,
                              available_resources,
                              total_resources,
                              available_resources_changed);
    gcs_resource_manager_->UpdateFromResourceReport(resources_data);
  }

  void UpdateResourceLoads(const std::string &node_id,
                           std::vector<rpc::ResourceDemand> demands,
                           bool resource_load_changed = true) {
    rpc::ResourcesData data;
    Mocker::FillResourcesData(data, node_id, demands, resource_load_changed);
    gcs_resource_manager_->UpdateResourceLoads(data);
  }

  std::string ShapeToString(const rpc::autoscaler::ResourceRequest &request) {
    // Ordered map with bundle name as the key
    std::map<std::string, double> m;
    for (const auto &resource : request.resources_bundle()) {
      m[resource.first] = resource.second;
    }
    std::stringstream ss;
    for (const auto &resource : m) {
      ss << resource.first << ":" << resource.second << ",";
    }
    auto s = ss.str();
    // Remove last ","
    return s.empty() ? "" : s.substr(0, s.size() - 1);
  }

  void CheckPendingRequests(
      const rpc::autoscaler::GetClusterResourceStateReply &reply,
      const std::unordered_map<std::string, int> &expect_requests_by_count) {
    auto pending_reqs = reply.pending_resource_requests();
    ASSERT_EQ(pending_reqs.size(), expect_requests_by_count.size());
    std::unordered_map<std::string, int> actual_requests_by_count;
    for (int i = 0; i < pending_reqs.size(); i++) {
      auto req_by_count = pending_reqs[i];
      auto req_str = ShapeToString(req_by_count.request());
      actual_requests_by_count[req_str] = req_by_count.count();
    }

    ASSERT_EQ(actual_requests_by_count.size(), expect_requests_by_count.size());
    for (const auto &req : expect_requests_by_count) {
      ASSERT_EQ(actual_requests_by_count[req.first], req.second)
          << "Request: " << req.first;
    }
  }
};

TEST_F(GcsAutoscalerStateManagerTest, TestNodeAddUpdateRemove) {
  auto node = Mocker::GenNodeInfo();

  // Adding a node.
  {
    node->mutable_resources_total()->insert({"CPU", 2});
    node->mutable_resources_total()->insert({"GPU", 1});
    node->set_instance_id("instance_1");
    AddNode(node);

    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.node_states_size(), 1);
    CheckNodeResources(reply.node_states(0),
                       /* available */ {{"CPU", 2}, {"GPU", 1}},
                       /* total */ {{"CPU", 2}, {"GPU", 1}});
  }

  // Update available resources.
  {
    UpdateFromResourceReportSync(NodeID::FromBinary(node->node_id()),
                                 {/* available */ {"CPU", 1.75}},
                                 /* total*/ {{"CPU", 2}, {"GPU", 1}},
                                 /* available_changed*/ true);

    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.node_states_size(), 1);
    CheckNodeResources(reply.node_states(0),
                       /*total*/ {{"CPU", 2}, {"GPU", 1}},
                       /*available*/ {{"CPU", 1.75}});
  }

  // Remove a node - test node states correct.
  {
    RemoveNode(NodeID::FromBinary(node->node_id()));
    gcs_resource_manager_->OnNodeDead(NodeID::FromBinary(node->node_id()));
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.node_states_size(), 0);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestBasicResourceRequests) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  // Adding a node.
  AddNode(node);

  // Get empty requests
  {
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_resource_requests_size(), 0);
  }

  // Update resource usages.
  {
    UpdateResourceLoads(node->node_id(),
                        {Mocker::GenResourceDemand({{"CPU", 1}},
                                                   /* nun_ready_queued */ 1,
                                                   /* nun_infeasible */ 1,
                                                   /* num_backlog */ 0),
                         Mocker::GenResourceDemand({{"CPU", 4}, {"GPU", 2}},
                                                   /* num_ready_queued */ 0,
                                                   /* num_infeasible */ 1,
                                                   /* num_backlog */ 1)});

    auto reply = GetClusterResourceStateSync();
    // Expect each pending resources shape to be num_infeasible + num_backlog.
    CheckPendingRequests(reply, {{"CPU:1", 1 + 1}, {"CPU:4,GPU:2", 1 + 1}});
  }

  // Remove node should clear it.
  {
    RemoveNode(NodeID::FromBinary(node->node_id()));
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_resource_requests_size(), 0);
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
