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
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"
// clang-format on

namespace ray {

namespace gcs {
using ::testing::_;
using ::testing::Return;

using ResourceBundleMap = std::unordered_map<std::string, double>;
using BundlesOnNodeMap = absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>;

// Test suite for AutoscalerState related functionality.
class GcsAutoscalerStateManagerTest : public ::testing::Test {
 public:
  GcsAutoscalerStateManagerTest() {}

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<MockGcsNodeManager> gcs_node_manager_;
  std::unique_ptr<GcsAutoscalerStateManager> gcs_autoscaler_state_manager_;
  std::shared_ptr<MockGcsPlacementGroupManager> gcs_placement_group_manager_;

  void SetUp() override {
    cluster_resource_manager_ = std::make_unique<ClusterResourceManager>(io_service_);
    gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
        io_service_, *cluster_resource_manager_, NodeID::FromRandom());
    gcs_node_manager_ = std::make_shared<MockGcsNodeManager>();

    gcs_placement_group_manager_ =
        std::make_shared<MockGcsPlacementGroupManager>(*gcs_resource_manager_);
    gcs_autoscaler_state_manager_.reset(
        new GcsAutoscalerStateManager("fake_cluster",
                                      *cluster_resource_manager_,
                                      *gcs_resource_manager_,
                                      *gcs_node_manager_,
                                      *gcs_placement_group_manager_));
  }

 public:
  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
    gcs_resource_manager_->OnNodeAdd(*node);
  }

  void RemoveNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    const auto node_id = NodeID::FromBinary(node->node_id());
    node->set_state(rpc::GcsNodeInfo::DEAD);
    gcs_node_manager_->alive_nodes_.erase(node_id);
    gcs_node_manager_->dead_nodes_[node_id] = node;
    gcs_resource_manager_->OnNodeDead(node_id);
  }

  void CheckNodeResources(
      const rpc::autoscaler::NodeState &node_state,
      const absl::flat_hash_map<std::string, double> &total_resources,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const rpc::autoscaler::NodeStatus &status = rpc::autoscaler::NodeStatus::RUNNING,
      int64_t idle_ms = 0) {
    ASSERT_EQ(node_state.total_resources_size(), total_resources.size());
    ASSERT_EQ(node_state.available_resources_size(), available_resources.size());
    for (const auto &resource : total_resources) {
      ASSERT_EQ(node_state.total_resources().at(resource.first), resource.second);
    }
    for (const auto &resource : available_resources) {
      ASSERT_EQ(node_state.available_resources().at(resource.first), resource.second);
    }
    ASSERT_EQ(node_state.status(), status);
    ASSERT_EQ(node_state.idle_duration_ms(), idle_ms);
  }

  void CheckNodeLabels(const rpc::autoscaler::NodeState &node_state,
                       const std::unordered_map<std::string, std::string> &labels) {
    ASSERT_EQ(node_state.dynamic_labels_size(), labels.size());
    for (const auto &label : labels) {
      ASSERT_EQ(node_state.dynamic_labels().at(label.first), label.second);
    }
  }

  void RequestClusterResourceConstraint(
      const rpc::ClusterResourceConstraint &constraint) {
    rpc::autoscaler::RequestClusterResourceConstraintRequest request;
    request.mutable_cluster_resource_constraint()->CopyFrom(constraint);
    rpc::autoscaler::RequestClusterResourceConstraintReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    gcs_autoscaler_state_manager_->HandleRequestClusterResourceConstraint(
        request, &reply, send_reply_callback);
  }

  rpc::autoscaler::ClusterResourceState GetClusterResourceStateSync() {
    rpc::autoscaler::GetClusterResourceStateRequest request;
    rpc::autoscaler::GetClusterResourceStateReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    gcs_autoscaler_state_manager_->HandleGetClusterResourceState(
        request, &reply, send_reply_callback);
    return reply.cluster_resource_state();
  }

  void UpdateFromResourceReportSync(
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const absl::flat_hash_map<std::string, double> &total_resources,
      bool available_resources_changed,
      int64_t idle_ms = 0) {
    rpc::ResourcesData resources_data;
    Mocker::FillResourcesData(resources_data,
                              node_id,
                              available_resources,
                              total_resources,
                              available_resources_changed,
                              idle_ms);
    gcs_resource_manager_->UpdateFromResourceReport(resources_data);
  }

  rpc::autoscaler::GetClusterStatusReply GetClusterStatusSync() {
    rpc::autoscaler::GetClusterStatusRequest request;
    rpc::autoscaler::GetClusterStatusReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};

    gcs_autoscaler_state_manager_->HandleGetClusterStatus(
        request, &reply, send_reply_callback);
    return reply;
  }

  void UpdateResourceLoads(const std::string &node_id,
                           std::vector<rpc::ResourceDemand> demands,
                           bool resource_load_changed = true) {
    rpc::ResourcesData data;
    Mocker::FillResourcesData(data, node_id, demands, resource_load_changed);
    gcs_resource_manager_->UpdateResourceLoads(data);
  }

  void ReportAutoscalingState(const rpc::autoscaler::AutoscalingState &state) {
    rpc::autoscaler::ReportAutoscalingStateRequest request;
    request.mutable_autoscaling_state()->CopyFrom(state);
    rpc::autoscaler::ReportAutoscalingStateReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    gcs_autoscaler_state_manager_->HandleReportAutoscalingState(
        request, &reply, send_reply_callback);
  }

  std::string ShapeToString(const rpc::autoscaler::ResourceRequest &request) {
    // Ordered map with bundle name as the key
    std::map<std::string, double> m;
    for (const auto &resource : request.resources_bundle()) {
      m[resource.first] = resource.second;
    }
    return ShapeToString(m);
  }

  std::string ShapeToString(const std::map<std::string, double> &m) {
    std::stringstream ss;
    for (const auto &resource : m) {
      ss << resource.first << ":" << resource.second << ",";
    }
    auto s = ss.str();
    // Remove last ","
    return s.empty() ? "" : s.substr(0, s.size() - 1);
  }

  std::string ShapeToString(const ResourceBundleMap &resource_map) {
    std::stringstream ss;
    std::map<std::string, double> sorted_m;
    for (const auto &resource : resource_map) {
      sorted_m[resource.first] = resource.second;
    }
    return ShapeToString(sorted_m);
  }

  void CheckPendingRequests(
      const rpc::autoscaler::ClusterResourceState &state,
      const std::unordered_map<std::string, int> &expect_requests_by_count) {
    auto pending_reqs = state.pending_resource_requests();
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

  void UpdatePlacementGroupLoad(const std::vector<rpc::PlacementGroupTableData> &data) {
    std::shared_ptr<rpc::PlacementGroupLoad> load =
        std::make_shared<rpc::PlacementGroupLoad>();
    for (auto &d : data) {
      load->add_placement_group_data()->CopyFrom(d);
    }

    gcs_resource_manager_->UpdatePlacementGroupLoad(load);
  }

  void GroupResourceRequestsByConstraintForPG(
      std::unordered_map<std::string, std::vector<ResourceBundleMap>> &actual_data,
      const rpc::autoscaler::GangResourceRequest &pg_request) {
    for (const auto &req : pg_request.requests()) {
      ResourceBundleMap resource_map;
      for (const auto &resource : req.resources_bundle()) {
        resource_map[resource.first] = resource.second;
      }

      if (req.placement_constraints_size() == 0) {
        actual_data[""].push_back(resource_map);
        continue;
      }
      for (const auto &constraint : req.placement_constraints()) {
        actual_data[constraint.DebugString()].push_back(resource_map);
      }
    }
  }

  void CheckGangResourceRequests(
      const rpc::autoscaler::ClusterResourceState &state,
      const std::unordered_map<std::string, std::vector<ResourceBundleMap>>
          &expected_data) {
    auto pending_reqs = state.pending_gang_resource_requests();
    std::unordered_map<std::string, std::vector<ResourceBundleMap>> actual_data;
    // Parse the data.
    for (const auto &pending_pg_req : pending_reqs) {
      GroupResourceRequestsByConstraintForPG(actual_data, pending_pg_req);
    }

    for (const auto &[pg_label_name, resource_lists] : expected_data) {
      ASSERT_EQ(actual_data[pg_label_name].size(), resource_lists.size())
          << pg_label_name;
      std::vector<std::string> actual_resource_map_str;
      std::vector<std::string> expected_resource_map_str;

      std::transform(actual_data[pg_label_name].begin(),
                     actual_data[pg_label_name].end(),
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

  void CheckResourceRequest(const rpc::autoscaler::ResourceRequest &request,
                            const std::map<std::string, double> &expected_resources) {
    ASSERT_EQ(request.resources_bundle().size(), expected_resources.size());
    ASSERT_EQ(ShapeToString(request), ShapeToString(expected_resources));
  }
};

TEST_F(GcsAutoscalerStateManagerTest, TestGenPlacementConstraintForPlacementGroup) {
  auto pg = PlacementGroupID::Of(JobID::FromInt(0));
  {
    auto strict_spread_constraint = GenPlacementConstraintForPlacementGroup(
        pg.Hex(), rpc::PlacementStrategy::STRICT_SPREAD);
    ASSERT_TRUE(strict_spread_constraint.has_value());
    ASSERT_TRUE(strict_spread_constraint->has_anti_affinity());
    ASSERT_EQ(strict_spread_constraint->anti_affinity().label_name(),
              FormatPlacementGroupLabelName(pg.Hex()));
  }

  {
    auto strict_pack_constraint = GenPlacementConstraintForPlacementGroup(
        pg.Hex(), rpc::PlacementStrategy::STRICT_PACK);
    ASSERT_TRUE(strict_pack_constraint.has_value());
    ASSERT_TRUE(strict_pack_constraint->has_affinity());
    ASSERT_EQ(strict_pack_constraint->affinity().label_name(),
              FormatPlacementGroupLabelName(pg.Hex()));
  }

  {
    auto no_pg_constraint_for_pack =
        GenPlacementConstraintForPlacementGroup(pg.Hex(), rpc::PlacementStrategy::PACK);
    ASSERT_FALSE(no_pg_constraint_for_pack.has_value());
  }

  {
    auto no_pg_constraint_for_spread =
        GenPlacementConstraintForPlacementGroup(pg.Hex(), rpc::PlacementStrategy::SPREAD);
    ASSERT_FALSE(no_pg_constraint_for_spread.has_value());
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestNodeAddUpdateRemove) {
  auto node = Mocker::GenNodeInfo();

  // Adding a node.
  {
    node->mutable_resources_total()->insert({"CPU", 2});
    node->mutable_resources_total()->insert({"GPU", 1});
    node->set_instance_id("instance_1");
    AddNode(node);

    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /* available */ {{"CPU", 2}, {"GPU", 1}},
                       /* total */ {{"CPU", 2}, {"GPU", 1}});
  }

  // Update available resources.
  {
    UpdateFromResourceReportSync(NodeID::FromBinary(node->node_id()),
                                 {/* available */ {"CPU", 1.75}},
                                 /* total*/ {{"CPU", 2}, {"GPU", 1}},
                                 /* available_changed*/ true);

    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {{"CPU", 2}, {"GPU", 1}},
                       /*available*/ {{"CPU", 1.75}});
  }

  // Remove a node - test node states correct.
  {
    RemoveNode(node);
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {},
                       /*available*/ {},
                       rpc::autoscaler::NodeStatus::DEAD);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGetClusterStatusBasic) {
  auto node = Mocker::GenNodeInfo();

  // Test basic cluster resource.
  {
    node->mutable_resources_total()->insert({"CPU", 2});
    node->mutable_resources_total()->insert({"GPU", 1});
    node->set_instance_id("instance_1");
    AddNode(node);

    const auto reply = GetClusterStatusSync();
    const auto &state = reply.cluster_resource_state();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /* available */ {{"CPU", 2}, {"GPU", 1}},
                       /* total */ {{"CPU", 2}, {"GPU", 1}});
  }

  // Test autoscaler info.
  {
    rpc::autoscaler::AutoscalingState actual_state;
    actual_state.set_autoscaler_state_version(1);
    ReportAutoscalingState(actual_state);
    const auto reply = GetClusterStatusSync();
    const auto &state = reply.autoscaling_state();
    ASSERT_EQ(state.autoscaler_state_version(), 1);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestNodeDynamicLabelsWithPG) {
  /// Check if PGs are created on a node, the node status should include
  /// the PG labels.
  auto node = Mocker::GenNodeInfo();

  // Adding a node.
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);

  // Mock the PG manager to return bundles on a node.
  {
    auto pg1 = PlacementGroupID::Of(JobID::FromInt(0));
    auto pg2 = PlacementGroupID::Of(JobID::FromInt(1));
    EXPECT_CALL(*gcs_placement_group_manager_,
                GetBundlesOnNode(NodeID::FromBinary(node->node_id())))
        .WillRepeatedly(Return(BundlesOnNodeMap{
            {pg1, {1, 2, 3}},
            {pg2, {4, 5, 6}},
        }));

    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeLabels(state.node_states(0),
                    {{FormatPlacementGroupLabelName(pg1.Hex()), ""},
                     {FormatPlacementGroupLabelName(pg2.Hex()), ""}});
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
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.pending_resource_requests_size(), 0);
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

    const auto &state = GetClusterResourceStateSync();
    // Expect each pending resources shape to be num_infeasible + num_backlog.
    CheckPendingRequests(state, {{"CPU:1", 1 + 1}, {"CPU:4,GPU:2", 1 + 1}});
  }

  // Remove node should clear it.
  {
    RemoveNode(node);
    auto reply = GetClusterResourceStateSync();
    ASSERT_EQ(reply.pending_resource_requests_size(), 0);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGangResourceRequestsBasic) {
  auto node = Mocker::GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 1});
  node->set_instance_id("instance_1");
  // Adding a node.
  AddNode(node);

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
                                            {"", ""},
                                            rpc::PlacementStrategy::STRICT_SPREAD,
                                            rpc::PlacementGroupTableData::PENDING)});

    auto state = GetClusterResourceStateSync();
    CheckGangResourceRequests(state,
                              {{GenPlacementConstraintForPlacementGroup(
                                    pg.Hex(), rpc::PlacementStrategy::STRICT_SPREAD)
                                    ->DebugString(),
                                {{{"CPU", 1}}, {{"GPU", 1}}}}});
  }

  // A strict pack should also generate constraints.
  {
    auto pg = PlacementGroupID::Of(job_id);
    UpdatePlacementGroupLoad(
        {Mocker::GenPlacementGroupTableData(pg,
                                            job_id,
                                            {{{"CPU", 1}}, {{"GPU", 1}}},
                                            {"", ""},
                                            rpc::PlacementStrategy::STRICT_PACK,
                                            rpc::PlacementGroupTableData::PENDING)});

    auto state = GetClusterResourceStateSync();
    CheckGangResourceRequests(state,
                              {{GenPlacementConstraintForPlacementGroup(
                                    pg.Hex(), rpc::PlacementStrategy::STRICT_PACK)
                                    ->DebugString(),
                                {{{"CPU", 1}}, {{"GPU", 1}}}}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGangResourceRequestsNonStrict) {
  auto node = Mocker::GenNodeInfo();
  node->set_instance_id("instance_1");
  node->mutable_resources_total()->insert({"CPU", 1});
  // Adding a node.
  AddNode(node);
  JobID job_id1 = JobID::FromInt(0);
  JobID job_id2 = JobID::FromInt(1);

  // A non strict spreading pending pg should not generate gang resource requests
  // without affinity.
  {
    auto pg1 = PlacementGroupID::Of(job_id1);
    auto pg2 = PlacementGroupID::Of(job_id2);
    UpdatePlacementGroupLoad(
        {Mocker::GenPlacementGroupTableData(pg1,
                                            job_id1,
                                            {{{"CPU", 1}, {"GPU", 2}}},
                                            {""},
                                            rpc::PlacementStrategy::PACK,
                                            rpc::PlacementGroupTableData::PENDING),
         Mocker::GenPlacementGroupTableData(pg2,
                                            job_id2,
                                            {{{"TPU", 1}}},
                                            {""},
                                            rpc::PlacementStrategy::SPREAD,
                                            rpc::PlacementGroupTableData::PENDING)});

    const auto &state = GetClusterResourceStateSync();
    CheckGangResourceRequests(state,
                              {{/* no pg constraint */ "",
                                {/* from first */ {{"CPU", 1}, {"GPU", 2}},
                                 /* from second */ {{"TPU", 1}}}}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGangResourceRequestsPartialRescheduling) {
  auto node = Mocker::GenNodeInfo();
  node->set_instance_id("instance_1");
  node->mutable_resources_total()->insert({"CPU", 1});
  // Adding a node.
  AddNode(node);
  JobID job_id1 = JobID::FromInt(0);
  // A partially placed PG should not have unplaced bundles requests for strict spread.
  {
    auto pg1 = PlacementGroupID::Of(job_id1);
    UpdatePlacementGroupLoad({Mocker::GenPlacementGroupTableData(
        pg1,
        job_id1,
        {{{"CPU_failed_1", 1}}, {{"CPU_success_2", 2}}},
        {"", node->node_id()},
        rpc::PlacementStrategy::STRICT_SPREAD,
        rpc::PlacementGroupTableData::RESCHEDULING)});

    const auto &state = GetClusterResourceStateSync();

    // CPU_success_2 should not be reported as needed.
    CheckGangResourceRequests(state,
                              {{GenPlacementConstraintForPlacementGroup(
                                    pg1.Hex(), rpc::PlacementStrategy::STRICT_SPREAD)
                                    ->DebugString(),
                                {{{"CPU_failed_1", 1}}}}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestClusterResourcesConstraint) {
  // Get empty cluster resources constraint.
  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.cluster_resource_constraints_size(), 0);
  }

  // Generate one constraint.
  {
    RequestClusterResourceConstraint(
        Mocker::GenClusterResourcesConstraint({{{"CPU", 2}, {"GPU", 1}}}, {1}));
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.cluster_resource_constraints_size(), 1);
    ASSERT_EQ(state.cluster_resource_constraints(0).min_bundles_size(), 1);
    CheckResourceRequest(state.cluster_resource_constraints(0).min_bundles(0).request(),
                         {{"CPU", 2}, {"GPU", 1}});
  }

  // Override it
  {
    RequestClusterResourceConstraint(Mocker::GenClusterResourcesConstraint(
        {{{"CPU", 4}, {"GPU", 5}, {"TPU", 1}}}, {1}));
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.cluster_resource_constraints_size(), 1);
    ASSERT_EQ(state.cluster_resource_constraints(0).min_bundles_size(), 1);
    CheckResourceRequest(state.cluster_resource_constraints(0).min_bundles(0).request(),
                         {{"CPU", 4}, {"GPU", 5}, {"TPU", 1}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestReportAutoscalingState) {
  // Empty autoscaling state.
  {
    const auto &autoscaling_state = gcs_autoscaler_state_manager_->autoscaling_state_;
    ASSERT_EQ(autoscaling_state, absl::nullopt);
  }

  // Return the updated state.
  {
    rpc::autoscaler::AutoscalingState actual_state;
    actual_state.set_autoscaler_state_version(1);
    ReportAutoscalingState(actual_state);

    const auto &autoscaling_state = gcs_autoscaler_state_manager_->autoscaling_state_;
    ASSERT_NE(autoscaling_state, absl::nullopt);
    ASSERT_EQ(autoscaling_state->autoscaler_state_version(), 1);
  }

  // Reject an older version.
  {
    rpc::autoscaler::AutoscalingState state;
    state.set_autoscaler_state_version(0);
    ReportAutoscalingState(state);

    const auto &autoscaling_state = gcs_autoscaler_state_manager_->autoscaling_state_;
    ASSERT_NE(autoscaling_state, absl::nullopt);
    ASSERT_EQ(autoscaling_state->autoscaler_state_version(), 1);
  }

  // Update with a new version.
  {
    rpc::autoscaler::AutoscalingState state;
    state.set_autoscaler_state_version(2);
    ReportAutoscalingState(state);

    const auto &autoscaling_state = gcs_autoscaler_state_manager_->autoscaling_state_;
    ASSERT_NE(autoscaling_state, absl::nullopt);
    ASSERT_EQ(autoscaling_state->autoscaler_state_version(), 2);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestIdleTime) {
  auto node = Mocker::GenNodeInfo();

  // Adding a node.
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);

  // No report yet - so idle time should be 0.
  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {{"CPU", 2}, {"GPU", 1}},
                       /*available*/ {{"CPU", 2}, {"GPU", 1}});
  }

  // Report idle node info.
  UpdateFromResourceReportSync(NodeID::FromBinary(node->node_id()),
                               {/* available */ {"CPU", 2}, {"GPU", 1}},
                               /* total*/ {{"CPU", 2}, {"GPU", 1}},
                               /* available_changed*/ true,
                               /* idle_duration_ms */ 10);

  // Check report idle time is set.
  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {{"CPU", 2}, {"GPU", 1}},
                       /*available*/ {{"CPU", 2}, {"GPU", 1}},
                       /*status*/ rpc::autoscaler::NodeStatus::IDLE,
                       /*idle_ms*/ 10);
  }

  // Dead node should make it no longer idle.
  {
    RemoveNode(node);
    gcs_resource_manager_->OnNodeDead(NodeID::FromBinary(node->node_id()));
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {},
                       /*available*/ {},
                       rpc::autoscaler::NodeStatus::DEAD);
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
