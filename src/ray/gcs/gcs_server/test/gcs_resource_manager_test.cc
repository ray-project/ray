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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {

namespace gcs {
using ::testing::_;

using ResourceBundleMap = std::unordered_map<std::string, double>;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest() : cluster_resource_manager_(io_service_) {
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, cluster_resource_manager_, NodeID::FromRandom());
  }

  instrumented_io_context io_service_;
  ClusterResourceManager cluster_resource_manager_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
};

// Test suite for AutoscalerState related functionality.
class GcsResourceManagerAutoscalerStateTest : public GcsResourceManagerTest {
 public:
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
    gcs_resource_manager_->HandleGetClusterResourceState(
        request, &reply, send_reply_callback);
    return reply;
  }

  void UpdatePlacementGroupLoad(const std::vector<rpc::PlacementGroupTableData> &data) {
    std::shared_ptr<rpc::PlacementGroupLoad> load =
        std::make_shared<rpc::PlacementGroupLoad>();
    for (auto &d : data) {
      load->add_placement_group_data()->CopyFrom(d);
    }

    gcs_resource_manager_->UpdatePlacementGroupLoad(load);
  }

  void CheckGangResourceRequests(
      const rpc::autoscaler::GetClusterResourceStateReply &reply,
      const std::unordered_map<std::string, std::vector<ResourceBundleMap>>
          &expected_data) {
    auto pending_reqs = reply.pending_gang_resource_requests();
    std::unordered_map<std::string, std::vector<ResourceBundleMap>> actual_data;
    // Parse the data.
    for (const auto &pending_pg_req : pending_reqs) {
      for (const auto &req : pending_pg_req.requests()) {
        ResourceBundleMap resource_map;
        for (const auto &resource : req.resources_bundle()) {
          resource_map[resource.first] = resource.second;
        }

        // Parse the affinity to PG.
        if (req.placement_constraints_size() == 0) {
          actual_data["no-pg-affinity"].push_back(resource_map);
          continue;
        }
        for (const auto &constraint : req.placement_constraints()) {
          if (constraint.anti_affinity().label_name() ==
              kPlacementGroupAntiAffinityLabelName) {
            auto pg_id = constraint.anti_affinity().label_value();

            actual_data[pg_id].push_back(resource_map);
          } else {
            actual_data["no-pg-affinity"].push_back(resource_map);
          }
        }
      }
    }

    for (const auto &[pg, resource_lists] : expected_data) {
      ASSERT_EQ(actual_data[pg].size(), resource_lists.size());
      std::vector<std::string> actual_resource_map_str;
      std::vector<std::string> expected_resource_map_str;

      std::transform(actual_data[pg].begin(),
                     actual_data[pg].end(),
                     std::back_inserter(actual_resource_map_str),
                     [this](const ResourceBundleMap &resource_map) {
                       return ShapeToString(resource_map);
                     });
      std::transform(resource_lists.begin(),
                     resource_lists.end(),
                     std::back_inserter(expected_resource_map_str),
                     [this](const ResourceBundleMap &resource_map) {
                       return ShapeToString(resource_map);
                     });
      // Sort and compare.
      std::sort(actual_resource_map_str.begin(), actual_resource_map_str.end());
      std::sort(expected_resource_map_str.begin(), expected_resource_map_str.end());
      for (size_t i = 0; i < actual_resource_map_str.size(); i++) {
        ASSERT_EQ(actual_resource_map_str[i], expected_resource_map_str[i]);
      }
    }
  }

  std::string ShapeToString(const ResourceBundleMap &resource_map) {
    std::stringstream ss;
    std::map<std::string, double> sorted_m;
    for (const auto &resource : resource_map) {
      sorted_m[resource.first] = resource.second;
    }

    for (const auto &resource : sorted_m) {
      ss << resource.first << ":" << resource.second << ",";
    }
    auto s = ss.str();
    // Remove last ","
    return s.empty() ? "" : s.substr(0, s.size() - 1);
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
    for (size_t i = 0; i < pending_reqs.size(); i++) {
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

TEST_F(GcsResourceManagerTest, TestBasic) {
  const std::string cpu_resource = "CPU";
  absl::flat_hash_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;

  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert(resource_map.begin(), resource_map.end());
  // Add node resources.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get and check cluster resources.
  const auto &resource_view = cluster_resource_manager_.GetResourceView();
  ASSERT_EQ(1, resource_view.size());

  scheduling::NodeID scheduling_node_id(node->node_id());
  auto resource_request =
      ResourceMapToResourceRequest(resource_map, /*requires_object_store_memory=*/false);

  // Test `AcquireResources`.
  ASSERT_TRUE(cluster_resource_manager_.HasSufficientResource(
      scheduling_node_id,
      resource_request,
      /*ignore_object_store_memory_requirement=*/true));
  ASSERT_TRUE(cluster_resource_manager_.SubtractNodeAvailableResources(scheduling_node_id,
                                                                       resource_request));
  ASSERT_FALSE(cluster_resource_manager_.HasSufficientResource(
      scheduling_node_id,
      resource_request,
      /*ignore_object_store_memory_requirement=*/true));

  // Test `ReleaseResources`.
  ASSERT_TRUE(cluster_resource_manager_.AddNodeAvailableResources(scheduling_node_id,
                                                                  resource_request));
}

TEST_F(GcsResourceManagerTest, TestResourceUsageAPI) {
  auto node = Mocker::GenNodeInfo();
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

  rpc::ReportResourceUsageRequest report_request;
  (*report_request.mutable_resources()->mutable_resources_available())["CPU"] = 2;
  (*report_request.mutable_resources()->mutable_resources_total())["CPU"] = 2;
  gcs_resource_manager_->UpdateNodeResourceUsage(node_id, report_request.resources());

  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply, send_reply_callback);
  ASSERT_EQ(get_all_reply.resource_usage_data().batch().size(), 1);

  gcs_resource_manager_->OnNodeDead(node_id);
  rpc::GetAllResourceUsageReply get_all_reply2;
  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply2, send_reply_callback);
  ASSERT_EQ(get_all_reply2.resource_usage_data().batch().size(), 0);

  // This will be ignored since the node is dead.
  gcs_resource_manager_->UpdateNodeResourceUsage(node_id, report_request.resources());
  rpc::GetAllResourceUsageReply get_all_reply3;
  gcs_resource_manager_->HandleGetAllResourceUsage(
      get_all_request, &get_all_reply3, send_reply_callback);
  ASSERT_EQ(get_all_reply3.resource_usage_data().batch().size(), 0);
}

TEST_F(GcsResourceManagerTest, TestSetAvailableResourcesWhenNodeDead) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 10});

  gcs_resource_manager_->OnNodeAdd(*node);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 1);

  auto node_id = NodeID::FromBinary(node->node_id());
  gcs_resource_manager_->OnNodeDead(node_id);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 0);

  rpc::ResourcesData resources_data;
  resources_data.set_node_id(node->node_id());
  resources_data.mutable_resources_total()->insert({"CPU", 5});
  resources_data.mutable_resources_available()->insert({"CPU", 5});
  resources_data.set_resources_available_changed(true);
  gcs_resource_manager_->UpdateFromResourceReport(resources_data);
  ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 0);
}

TEST_F(GcsResourceManagerAutoscalerStateTest, TestNodeAddUpdateRemove) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");

  // Adding a node.
  {
    gcs_resource_manager_->OnNodeAdd(*node);

    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.node_states_size(), 1);
    CheckNodeResources(
        reply.node_states(0), {{"CPU", 2}, {"GPU", 1}}, {{"CPU", 2}, {"GPU", 1}});
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
    gcs_resource_manager_->OnNodeDead(NodeID::FromBinary(node->node_id()));
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.node_states_size(), 0);
  }
}

TEST_F(GcsResourceManagerAutoscalerStateTest, TestResourceRequests) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  // Adding a node.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get empty requests
  {
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_resource_requests_size(), 0);
  }

  // Update resource usages.
  {
    UpdateResourceLoads(node->node_id(),
                        {Mocker::GenResourceDemand({{"CPU", 1}}, 1, 1, 0),
                         Mocker::GenResourceDemand({{"CPU", 4}, {"GPU", 2}}, 1, 1, 1)});

    auto reply = GetClusterResourceStateSync();
    CheckPendingRequests(reply, {{"CPU:1", 1}, {"CPU:4,GPU:2", 1}});
  }

  // Remove node should clear it.
  {
    gcs_resource_manager_->OnNodeDead(NodeID::FromBinary(node->node_id()));
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_resource_requests_size(), 0);
  }
}

TEST_F(GcsResourceManagerAutoscalerStateTest, TestClusterStateVersion) {
  ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 0);

  // Adding a node
  auto node1 = Mocker::GenNodeInfo();
  {
    node1->set_instance_id("instance_1");
    gcs_resource_manager_->OnNodeAdd(*node1);

    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 1);
  }

  // Adding multiple nodes with a token should only increment version by 1.
  {
    for (int i = 0; i < 3; i++) {
      auto node = Mocker::GenNodeInfo();
      node->set_instance_id("instance_" + std::to_string(i));
      gcs_resource_manager_->OnNodeAdd(*node);
    }
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 4);
  }

  // Resource load bumps the version if it's changed.
  {
    UpdateResourceLoads(node1->node_id(),
                        {Mocker::GenResourceDemand({{"CPU", 1}}, 1, 1, 0)});
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 5);

    UpdateResourceLoads(node1->node_id(),
                        {Mocker::GenResourceDemand({{"CPU", 1}}, 1, 1, 0)},
                        /* resource_load_changed */ false);
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 5);
  }

  // Available resources updates bump the version if it's changed.
  {
    UpdateFromResourceReportSync(NodeID::FromBinary(node1->node_id()),
                                 {/* available */ {"CPU", 1.75}},
                                 /* total*/ {{"CPU", 2}, {"GPU", 1}},
                                 /* available_changed*/ true);
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 6);

    UpdateFromResourceReportSync(NodeID::FromBinary(node1->node_id()),
                                 {/* available */ {"CPU", 1.75}},
                                 /* total*/ {{"CPU", 2}, {"GPU", 1}},
                                 /* available_changed*/ false);
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 6);
  }

  // Node removal bumps the version
  {
    gcs_resource_manager_->OnNodeDead(NodeID::FromBinary(node1->node_id()));
    ASSERT_EQ(gcs_resource_manager_->GetClusterResourceStateVersion(), 7);
  }
}

TEST_F(GcsResourceManagerAutoscalerStateTest, TestGangResourceRequests) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  // Adding a node.
  gcs_resource_manager_->OnNodeAdd(*node);

  // Get empty requests
  {
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_gang_resource_requests_size(), 0);
  }

  JobID job_id = JobID::FromInt(0);
  // A strict spread pending pg should generate pending gang resource requests.
  {
    auto pg = PlacementGroupID::Of(job_id);
    UpdatePlacementGroupLoad(
        {Mocker::GenPlacementGroupTableData(pg,
                                            job_id,
                                            {{{"CPU", 1}}, {{"GPU", 1}}},
                                            rpc::PlacementStrategy::STRICT_SPREAD,
                                            rpc::PlacementGroupTableData::PENDING)});

    auto reply = GetClusterResourceStateSync();
    CheckGangResourceRequests(reply, {{pg.Binary(), {{{"CPU", 1}}, {{"GPU", 1}}}}});
  }

  // A strict spread non-pending pg should not generate gang resource requests.
  {
    auto pg = PlacementGroupID::Of(job_id);
    UpdatePlacementGroupLoad(
        {Mocker::GenPlacementGroupTableData(pg,
                                            job_id,
                                            {{{"CPU", 2}}, {{"GPU", 2}}},
                                            rpc::PlacementStrategy::STRICT_SPREAD,
                                            rpc::PlacementGroupTableData::CREATED)});

    auto reply = GetClusterResourceStateSync();
    CheckGangResourceRequests(reply, {});
  }

  // A non strict spreading pending pg should not generate gang resource requests.
  {
    auto pg = PlacementGroupID::Of(job_id);
    UpdatePlacementGroupLoad(
        {Mocker::GenPlacementGroupTableData(pg,
                                            job_id,
                                            {{{"CPU", 1}, {"GPU", 2}}},
                                            rpc::PlacementStrategy::PACK,
                                            rpc::PlacementGroupTableData::PENDING),
         Mocker::GenPlacementGroupTableData(pg,
                                            job_id,
                                            {{{"TPU", 1}}},
                                            rpc::PlacementStrategy::STRICT_PACK,
                                            rpc::PlacementGroupTableData::RESCHEDULING)});

    auto reply = GetClusterResourceStateSync();
    CheckGangResourceRequests(
        reply,
        {{"no-pg-affinity",
          {/* from first */ {{"CPU", 1}, {"GPU", 2}}, /* from second */ {{"TPU", 1}}}}});
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
