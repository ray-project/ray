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

#include "ray/gcs/gcs_autoscaler_state_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mock/ray/gcs/gcs_actor_manager.h"
#include "mock/ray/gcs/gcs_node_manager.h"
#include "mock/ray/gcs/gcs_placement_group_manager.h"
#include "mock/ray/gcs/store_client/store_client.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/protobuf_utils.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_init_data.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/gcs/store_client_kv.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

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
  static constexpr char kRayletConfig[] = R"({"raylet_config":"this is a config"})";
  instrumented_io_context io_service_;
  std::shared_ptr<rpc::FakeRayletClient> raylet_client_;
  std::shared_ptr<rpc::RayletClientPool> client_pool_;
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<MockGcsNodeManager> gcs_node_manager_;
  std::unique_ptr<MockGcsActorManager> gcs_actor_manager_;
  std::unique_ptr<GcsAutoscalerStateManager> gcs_autoscaler_state_manager_;
  std::shared_ptr<MockGcsPlacementGroupManager> gcs_placement_group_manager_;
  std::unique_ptr<GCSFunctionManager> function_manager_;
  std::unique_ptr<RuntimeEnvManager> runtime_env_manager_;
  std::unique_ptr<GcsInternalKVManager> kv_manager_;
  std::unique_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  ray::observability::FakeGauge fake_placement_group_gauge_;
  ray::observability::FakeHistogram
      fake_placement_group_creation_latency_in_ms_histogram_;
  ray::observability::FakeHistogram
      fake_placement_group_scheduling_latency_in_ms_histogram_;
  ray::observability::FakeGauge fake_placement_group_count_gauge_;

  void SetUp() override {
    raylet_client_ = std::make_shared<rpc::FakeRayletClient>();
    client_pool_ = std::make_unique<rpc::RayletClientPool>(
        [this](const rpc::Address &) { return raylet_client_; });
    cluster_resource_manager_ = std::make_unique<ClusterResourceManager>(io_service_);
    gcs_node_manager_ = std::make_shared<MockGcsNodeManager>();
    kv_manager_ = std::make_unique<GcsInternalKVManager>(
        std::make_unique<StoreClientInternalKV>(std::make_unique<MockStoreClient>()),
        kRayletConfig,
        io_service_);
    function_manager_ =
        std::make_unique<GCSFunctionManager>(kv_manager_->GetInstance(), io_service_);
    runtime_env_manager_ = std::make_unique<RuntimeEnvManager>(
        [](const std::string &, std::function<void(bool)>) {});
    raylet_client_pool_ =
        std::make_unique<rpc::RayletClientPool>([](const rpc::Address &address) {
          return std::make_shared<rpc::FakeRayletClient>();
        });
    worker_client_pool_ =
        std::make_unique<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
          return std::make_shared<rpc::MockCoreWorkerClientInterface>();
        });
    gcs_actor_manager_ = std::make_unique<MockGcsActorManager>(*runtime_env_manager_,
                                                               *function_manager_,
                                                               *raylet_client_pool_,
                                                               *worker_client_pool_);
    gcs_resource_manager_ =
        std::make_shared<GcsResourceManager>(io_service_,
                                             *cluster_resource_manager_,
                                             *gcs_node_manager_,
                                             NodeID::FromRandom());

    gcs_placement_group_manager_ = std::make_shared<MockGcsPlacementGroupManager>(
        *gcs_resource_manager_,
        fake_placement_group_gauge_,
        fake_placement_group_creation_latency_in_ms_histogram_,
        fake_placement_group_scheduling_latency_in_ms_histogram_,
        fake_placement_group_count_gauge_);
    gcs_autoscaler_state_manager_.reset(
        new GcsAutoscalerStateManager("fake_cluster",
                                      *gcs_node_manager_,
                                      *gcs_actor_manager_,
                                      *gcs_placement_group_manager_,
                                      *client_pool_,
                                      kv_manager_->GetInstance(),
                                      io_service_,
                                      /*gcs_publisher=*/nullptr));
  }

 public:
  void AddNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    absl::MutexLock lock(&gcs_node_manager_->mutex_);
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
    gcs_autoscaler_state_manager_->OnNodeAdd(*node);
  }

  void AddNodeToNodeManagerOnly(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    absl::MutexLock lock(&gcs_node_manager_->mutex_);
    gcs_node_manager_->alive_nodes_[NodeID::FromBinary(node->node_id())] = node;
  }

  void RemoveNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
    absl::MutexLock lock(&gcs_node_manager_->mutex_);
    const auto node_id = NodeID::FromBinary(node->node_id());
    node->set_state(rpc::GcsNodeInfo::DEAD);
    gcs_node_manager_->alive_nodes_.erase(node_id);
    gcs_node_manager_->dead_nodes_[node_id] = node;
    gcs_autoscaler_state_manager_->OnNodeDead(node_id);
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
    ASSERT_EQ(node_state.labels_size(), labels.size());
    for (const auto &label : labels) {
      ASSERT_EQ(node_state.labels().at(label.first), label.second);
    }
  }

  void CheckNodeDynamicLabels(
      const rpc::autoscaler::NodeState &node_state,
      const std::unordered_map<std::string, std::string> &labels) {
    ASSERT_EQ(node_state.dynamic_labels_size(), labels.size());
    for (const auto &label : labels) {
      ASSERT_EQ(node_state.dynamic_labels().at(label.first), label.second);
    }
  }

  void RequestClusterResourceConstraint(
      const rpc::autoscaler::ClusterResourceConstraint &constraint) {
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

  bool DrainNodeSync(const NodeID &node_id,
                     const rpc::autoscaler::DrainNodeReason &reason,
                     const std::string &reason_message,
                     int64_t deadline_timestamp_ms) {
    rpc::autoscaler::DrainNodeRequest request;
    request.set_node_id(node_id.Binary());
    request.set_reason(reason);
    request.set_reason_message(reason_message);
    request.set_deadline_timestamp_ms(deadline_timestamp_ms);
    rpc::autoscaler::DrainNodeReply reply;
    auto send_reply_callback =
        [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
    gcs_autoscaler_state_manager_->HandleDrainNode(request, &reply, send_reply_callback);
    return reply.is_accepted();
  }

  void UpdateFromResourceViewSync(
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const absl::flat_hash_map<std::string, double> &total_resources,
      int64_t idle_ms = 0,
      bool is_draining = false,
      int64_t draining_deadline_timestamp_ms = -1) {
    rpc::ResourcesData resources_data;
    FillResourcesData(resources_data,
                      node_id,
                      available_resources,
                      total_resources,
                      idle_ms,
                      is_draining,
                      draining_deadline_timestamp_ms);
    gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(resources_data);
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
                           std::vector<rpc::ResourceDemand> demands) {
    rpc::ResourcesData data;
    FillResourcesData(data, node_id, demands);
    gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(data);
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
  auto node = GenNodeInfo();

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
    UpdateFromResourceViewSync(NodeID::FromBinary(node->node_id()),
                               {/* available */ {"CPU", 1.75}},
                               /* total*/ {{"CPU", 2}, {"GPU", 1}});

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
  auto node = GenNodeInfo();

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

TEST_F(GcsAutoscalerStateManagerTest, TestHandleGetClusterStatusWithOutOfOrderNodeAdd) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->set_instance_id("instance_1");
  AddNodeToNodeManagerOnly(node);

  const auto reply = GetClusterStatusSync();

  // Should have cluster resource state
  ASSERT_TRUE(reply.has_cluster_resource_state());
  const auto &state = reply.cluster_resource_state();
  ASSERT_EQ(state.node_states_size(), 1);

  // Should NOT have autoscaling state when none has been reported
  ASSERT_FALSE(reply.has_autoscaling_state());

  // Cluster resource state should still be valid
  ASSERT_GT(state.cluster_resource_state_version(), 0);
  ASSERT_EQ(state.cluster_session_name(), "fake_cluster");
}

TEST_F(GcsAutoscalerStateManagerTest, TestNodeDynamicLabelsWithPG) {
  /// Check if PGs are created on a node, the node status should include
  /// the PG labels.
  auto node = GenNodeInfo();

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
    CheckNodeDynamicLabels(state.node_states(0),
                           {{FormatPlacementGroupLabelName(pg1.Hex()), ""},
                            {FormatPlacementGroupLabelName(pg2.Hex()), ""}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestBasicResourceRequests) {
  auto node = GenNodeInfo();
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
                        {GenResourceDemand({{"CPU", 1}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 1,
                                           /* num_backlog */ 0,
                                           /* label_selectors */ {}),
                         GenResourceDemand({{"CPU", 4}, {"GPU", 2}},
                                           /* num_ready_queued */ 0,
                                           /* num_infeasible */ 1,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {})});

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
  auto node = GenNodeInfo();
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
    EXPECT_CALL(*gcs_placement_group_manager_, GetPlacementGroupLoad)
        .WillOnce(Return(GenPlacementGroupLoad(
            {GenPlacementGroupTableData(pg,
                                        job_id,
                                        {{{"CPU", 1}}, {{"GPU", 1}}},
                                        {"", ""},
                                        rpc::PlacementStrategy::STRICT_SPREAD,
                                        rpc::PlacementGroupTableData::PENDING)})));

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
    EXPECT_CALL(*gcs_placement_group_manager_, GetPlacementGroupLoad)
        .WillOnce(Return(GenPlacementGroupLoad(
            {GenPlacementGroupTableData(pg,
                                        job_id,
                                        {{{"CPU", 1}}, {{"GPU", 1}}},
                                        {"", ""},
                                        rpc::PlacementStrategy::STRICT_PACK,
                                        rpc::PlacementGroupTableData::PENDING)})));

    auto state = GetClusterResourceStateSync();
    CheckGangResourceRequests(state,
                              {{GenPlacementConstraintForPlacementGroup(
                                    pg.Hex(), rpc::PlacementStrategy::STRICT_PACK)
                                    ->DebugString(),
                                {{{"CPU", 1}}, {{"GPU", 1}}}}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGangResourceRequestsNonStrict) {
  auto node = GenNodeInfo();
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
    EXPECT_CALL(*gcs_placement_group_manager_, GetPlacementGroupLoad)
        .WillOnce(Return(GenPlacementGroupLoad(
            {GenPlacementGroupTableData(pg1,
                                        job_id1,
                                        {{{"CPU", 1}, {"GPU", 2}}},
                                        {""},
                                        rpc::PlacementStrategy::PACK,
                                        rpc::PlacementGroupTableData::PENDING),
             GenPlacementGroupTableData(pg2,
                                        job_id2,
                                        {{{"TPU", 1}}},
                                        {""},
                                        rpc::PlacementStrategy::SPREAD,
                                        rpc::PlacementGroupTableData::PENDING)})));

    const auto &state = GetClusterResourceStateSync();
    CheckGangResourceRequests(state,
                              {{/* no pg constraint */ "",
                                {/* from first */ {{"CPU", 1}, {"GPU", 2}},
                                 /* from second */ {{"TPU", 1}}}}});
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGangResourceRequestsPartialRescheduling) {
  auto node = GenNodeInfo();
  node->set_instance_id("instance_1");
  node->mutable_resources_total()->insert({"CPU", 1});
  // Adding a node.
  AddNode(node);
  JobID job_id1 = JobID::FromInt(0);
  // A partially placed PG should not have unplaced bundles requests for strict spread.
  {
    auto pg1 = PlacementGroupID::Of(job_id1);

    EXPECT_CALL(*gcs_placement_group_manager_, GetPlacementGroupLoad)
        .WillOnce(Return(GenPlacementGroupLoad(
            {GenPlacementGroupTableData(pg1,
                                        job_id1,
                                        {{{"CPU_failed_1", 1}}, {{"CPU_success_2", 2}}},
                                        {"", node->node_id()},
                                        rpc::PlacementStrategy::STRICT_SPREAD,
                                        rpc::PlacementGroupTableData::RESCHEDULING)})));

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
        GenClusterResourcesConstraint({{{"CPU", 2}, {"GPU", 1}}}, {1}));
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.cluster_resource_constraints_size(), 1);
    ASSERT_EQ(state.cluster_resource_constraints(0).resource_requests_size(), 1);
    CheckResourceRequest(
        state.cluster_resource_constraints(0).resource_requests(0).request(),
        {{"CPU", 2}, {"GPU", 1}});
  }

  // Override it
  {
    RequestClusterResourceConstraint(
        GenClusterResourcesConstraint({{{"CPU", 4}, {"GPU", 5}, {"TPU", 1}}}, {1}));
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.cluster_resource_constraints_size(), 1);
    ASSERT_EQ(state.cluster_resource_constraints(0).resource_requests_size(), 1);
    CheckResourceRequest(
        state.cluster_resource_constraints(0).resource_requests(0).request(),
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

TEST_F(GcsAutoscalerStateManagerTest, TestDrainNonAliveNode) {
  auto node = GenNodeInfo();

  // Adding a node.
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);
  RemoveNode(node);

  // Drain a dead node.
  ASSERT_TRUE(
      DrainNodeSync(NodeID::FromBinary(node->node_id()),
                    rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION,
                    "preemption",
                    std::numeric_limits<int64_t>::max()));

  // Drain a non-exist node.
  ASSERT_TRUE(
      DrainNodeSync(NodeID::FromRandom(),
                    rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION,
                    "preemption",
                    std::numeric_limits<int64_t>::max()));
}

TEST_F(GcsAutoscalerStateManagerTest, TestDrainingStatus) {
  auto node = GenNodeInfo();

  // Adding a node.
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);

  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states(0).status(), rpc::autoscaler::NodeStatus::RUNNING);
  }

  // Report draining info.
  UpdateFromResourceViewSync(
      NodeID::FromBinary(node->node_id()),
      {/* available */ {"CPU", 2}, {"GPU", 1}},
      /* total*/ {{"CPU", 2}, {"GPU", 1}},
      /* idle_duration_ms */ 10,
      /* is_draining */ true,
      /* draining_deadline_timestamp_ms */ std::numeric_limits<int64_t>::max());
  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states(0).status(), rpc::autoscaler::NodeStatus::DRAINING);
  }

  // Dead node should make it no longer draining.
  {
    RemoveNode(node);
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states(0).status(), rpc::autoscaler::NodeStatus::DEAD);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestDrainNodeRaceCondition) {
  auto node = GenNodeInfo();

  // Adding a node.
  node->mutable_resources_total()->insert({"CPU", 2});
  node->mutable_resources_total()->insert({"GPU", 1});
  node->set_instance_id("instance_1");
  AddNode(node);

  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(node->node_id());
  request.set_reason(rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  request.set_reason_message("preemption");
  request.set_deadline_timestamp_ms(std::numeric_limits<int64_t>::max());
  rpc::autoscaler::DrainNodeReply reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  gcs_autoscaler_state_manager_->HandleDrainNode(request, &reply, send_reply_callback);

  // At this point, the GCS request is not accepted yet since ralyet has not replied.
  ASSERT_FALSE(reply.is_accepted());

  // Inject a race condition on GCS: remove the node before raylet accepts the request.
  RemoveNode(node);

  // Simulates raylet accepts the drain request and replies to GCS.
  ASSERT_TRUE(raylet_client_->ReplyDrainRaylet());

  // The GCS request is accepted now.
  ASSERT_TRUE(reply.is_accepted());
}

TEST_F(GcsAutoscalerStateManagerTest, TestIdleTime) {
  auto node = GenNodeInfo();

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
  UpdateFromResourceViewSync(NodeID::FromBinary(node->node_id()),
                             {/* available */ {"CPU", 2}, {"GPU", 1}},
                             /* total*/ {{"CPU", 2}, {"GPU", 1}},
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
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.node_states_size(), 1);
    CheckNodeResources(state.node_states(0),
                       /*total*/ {},
                       /*available*/ {},
                       rpc::autoscaler::NodeStatus::DEAD);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestGcsKvManagerInternalConfig) {
  // This is really a test for GcsKvManager. However gcs_kv_manager_test.cc is a larger
  // misnomer - it does not test that class at all; it only tests StoreClientInternalKV.
  // We temporarily put this test here.
  rpc::GetInternalConfigRequest request;
  rpc::GetInternalConfigReply reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};
  kv_manager_->HandleGetInternalConfig(request, &reply, send_reply_callback);
  EXPECT_EQ(reply.config(), kRayletConfig);
}

TEST_F(GcsAutoscalerStateManagerTest,
       TestGetPerNodeInfeasibleResourceRequests_NoInfeasibleRequests) {
  // Prepare
  auto node_1 = GenNodeInfo();
  auto node_2 = GenNodeInfo();

  // Add nodes
  {
    node_1->mutable_resources_total()->insert({"CPU", 2});
    node_1->set_instance_id("instance_1");
    AddNode(node_1);
    node_2->mutable_resources_total()->insert({"CPU", 1});
    node_2->set_instance_id("instance_2");
    AddNode(node_2);
  }

  // Update resource usages
  {
    UpdateResourceLoads(node_1->node_id(),
                        {GenResourceDemand({{"GPU", 1}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 1,
                                           /* num_backlog */ 0,
                                           /* label_selectors */ {}),
                         GenResourceDemand({{"CPU", 1}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 0,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {}),
                         GenResourceDemand({{"CPU", 3}},
                                           /* num_ready_queued */ 0,
                                           /* num_infeasible */ 1,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {})});
    UpdateResourceLoads(node_2->node_id(),
                        {GenResourceDemand({{"CPU", 2}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 0,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {})});
  }

  // Update autoscaling state
  {
    rpc::autoscaler::AutoscalingState actual_state;
    actual_state.set_autoscaler_state_version(1);
    ReportAutoscalingState(actual_state);
  }

  // Execute
  const auto per_node_infeasible_requests =
      gcs_autoscaler_state_manager_->GetPerNodeInfeasibleResourceRequests();

  // Verify
  { ASSERT_TRUE(per_node_infeasible_requests.empty()); }

  // Reset
  {
    RemoveNode(node_1);
    RemoveNode(node_2);
  }
}

TEST_F(GcsAutoscalerStateManagerTest,
       TestGetPerNodeInfeasibleResourceRequests_WithInfeasibleRequests) {
  // Prepare
  auto node_1 = GenNodeInfo();
  auto node_2 = GenNodeInfo();

  // Add nodes
  {
    node_1->mutable_resources_total()->insert({"CPU", 2});
    node_1->set_instance_id("instance_1");
    AddNode(node_1);
    node_2->mutable_resources_total()->insert({"CPU", 1});
    node_2->set_instance_id("instance_2");
    AddNode(node_2);
  }

  // Update resource usages
  {
    UpdateResourceLoads(node_1->node_id(),
                        {GenResourceDemand({{"GPU", 1}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 1,
                                           /* num_backlog */ 0),
                         /* label_selectors */ {},
                         GenResourceDemand({{"CPU", 1}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 0,
                                           /* num_backlog */ 1),
                         /* label_selectors */ {},
                         GenResourceDemand({{"CPU", 3}},
                                           /* num_ready_queued */ 0,
                                           /* num_infeasible */ 1,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {})});
    UpdateResourceLoads(node_2->node_id(),
                        {GenResourceDemand({{"CPU", 2}},
                                           /* nun_ready_queued */ 1,
                                           /* nun_infeasible */ 0,
                                           /* num_backlog */ 1,
                                           /* label_selectors */ {})});
  }

  // Update autoscaling state
  {
    rpc::autoscaler::AutoscalingState actual_state;
    actual_state.set_autoscaler_state_version(1);
    auto infeasible_resource_request_1 = actual_state.add_infeasible_resource_requests();
    auto infeasible_resource_request_2 = actual_state.add_infeasible_resource_requests();
    infeasible_resource_request_1->mutable_resources_bundle()->insert({"CPU", 3});
    infeasible_resource_request_2->mutable_resources_bundle()->insert({"GPU", 2});
    ReportAutoscalingState(actual_state);
  }

  // Execute
  const auto per_node_infeasible_requests =
      gcs_autoscaler_state_manager_->GetPerNodeInfeasibleResourceRequests();

  // Verify
  {
    ASSERT_EQ(per_node_infeasible_requests.size(), 1);
    ASSERT_NE(per_node_infeasible_requests.find(NodeID::FromBinary(node_1->node_id())),
              per_node_infeasible_requests.end());
    ASSERT_EQ(
        per_node_infeasible_requests.at(NodeID::FromBinary(node_1->node_id())).size(), 1);
    ASSERT_EQ(per_node_infeasible_requests.at(NodeID::FromBinary(node_1->node_id()))
                  .at(0)
                  .size(),
              1);
    ASSERT_EQ(per_node_infeasible_requests.at(NodeID::FromBinary(node_1->node_id()))
                  .at(0)
                  .at("CPU"),
              3);
  }

  // Reset
  {
    RemoveNode(node_1);
    RemoveNode(node_2);
  }
}

TEST_F(GcsAutoscalerStateManagerTest, TestNodeLabelsAdded) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->set_instance_id("instance_1");
  (*node->mutable_labels())["accelerator-type"] = "TPU";
  (*node->mutable_labels())["region"] = "us-central1";
  AddNode(node);

  const auto &state = GetClusterResourceStateSync();
  ASSERT_EQ(state.node_states_size(), 1);

  CheckNodeLabels(state.node_states(0),
                  {{"accelerator-type", "TPU"}, {"region", "us-central1"}});
}

TEST_F(GcsAutoscalerStateManagerTest, TestGetPendingResourceRequestsWithLabelSelectors) {
  auto node = GenNodeInfo();
  node->mutable_resources_total()->insert({"CPU", 2});
  node->set_instance_id("instance_1");
  AddNode(node);

  // Add label selector to ResourceDemand
  {
    rpc::LabelSelector selector;

    auto add_constraint = [&](const std::string &key,
                              rpc::LabelSelectorOperator op,
                              const std::string &value) {
      auto *constraint = selector.add_label_constraints();
      constraint->set_label_key(key);
      constraint->set_operator_(op);
      constraint->add_label_values(value);
    };

    add_constraint("accelerator-type", rpc::LABEL_OPERATOR_IN, "TPU");
    add_constraint("node-group", rpc::LABEL_OPERATOR_NOT_IN, "gpu-group");
    add_constraint("market-type", rpc::LABEL_OPERATOR_IN, "spot");
    add_constraint("region", rpc::LABEL_OPERATOR_NOT_IN, "us-west4");

    // Simulate an infeasible request with a label selector
    UpdateResourceLoads(node->node_id(),
                        {GenResourceDemand({{"CPU", 2}},
                                           /*ready=*/0,
                                           /*infeasible=*/1,
                                           /*backlog=*/0,
                                           {selector})});
  }

  // Validate the cluster state includes the generated pending request
  {
    const auto &state = GetClusterResourceStateSync();
    ASSERT_EQ(state.pending_resource_requests_size(), 1);

    const auto &req = state.pending_resource_requests(0);
    ASSERT_EQ(req.count(), 1);
    CheckResourceRequest(req.request(), {{"CPU", 2}});

    std::unordered_map<std::string, std::pair<rpc::LabelSelectorOperator, std::string>>
        expected_vals = {
            {"accelerator-type", {rpc::LABEL_OPERATOR_IN, "TPU"}},
            {"node-group", {rpc::LABEL_OPERATOR_NOT_IN, "gpu-group"}},
            {"market-type", {rpc::LABEL_OPERATOR_IN, "spot"}},
            {"region", {rpc::LABEL_OPERATOR_NOT_IN, "us-west4"}},
        };

    ASSERT_EQ(req.request().label_selectors_size(), 1);
    const auto &parsed_selector = req.request().label_selectors(0);
    ASSERT_EQ(parsed_selector.label_constraints_size(), expected_vals.size());

    for (const auto &constraint : parsed_selector.label_constraints()) {
      const auto it = expected_vals.find(constraint.label_key());
      ASSERT_NE(it, expected_vals.end())
          << "Unexpected label key: " << constraint.label_key();
      ASSERT_EQ(constraint.operator_(), it->second.first);
      ASSERT_EQ(constraint.label_values_size(), 1);
      ASSERT_EQ(constraint.label_values(0), it->second.second);
    }
  }
}

TEST_F(GcsAutoscalerStateManagerTest,
       TestGetPendingGangResourceRequestsWithBundleSelectors) {
  rpc::PlacementGroupLoad load;

  // Create PG with two bundles with different label selectors
  auto *pg_data = load.add_placement_group_data();
  pg_data->set_state(rpc::PlacementGroupTableData::PENDING);
  auto pg_id = PlacementGroupID::Of(JobID::FromInt(1));
  pg_data->set_placement_group_id(pg_id.Binary());

  auto *bundle1 = pg_data->add_bundles();
  (*bundle1->mutable_unit_resources())["CPU"] = 2;
  (*bundle1->mutable_unit_resources())["GPU"] = 1;
  (*bundle1->mutable_label_selector())["accelerator"] = "in(A100,B200)";

  auto *bundle2 = pg_data->add_bundles();
  (*bundle2->mutable_unit_resources())["CPU"] = 4;
  (*bundle2->mutable_label_selector())["accelerator"] = "!in(TPU)";

  EXPECT_CALL(*gcs_placement_group_manager_, GetPlacementGroupLoad)
      .WillOnce(Return(std::make_shared<rpc::PlacementGroupLoad>(std::move(load))));

  const auto &state = GetClusterResourceStateSync();
  const auto &requests = state.pending_gang_resource_requests();
  ASSERT_EQ(requests.size(), 1);

  const auto &req = requests.Get(0);
  ASSERT_EQ(req.bundle_selectors_size(), 1);

  const auto &r1 = req.bundle_selectors(0).resource_requests(0);
  const auto &r2 = req.bundle_selectors(0).resource_requests(1);

  ASSERT_EQ(r1.label_selectors_size(), 1);
  ASSERT_EQ(r2.label_selectors_size(), 1);

  const auto &c1 = r1.label_selectors(0).label_constraints(0);
  const auto &c2 = r2.label_selectors(0).label_constraints(0);

  EXPECT_EQ(c1.label_key(), "accelerator");
  EXPECT_EQ(c1.operator_(), rpc::LabelSelectorOperator::LABEL_OPERATOR_IN);
  ASSERT_EQ(c1.label_values_size(), 2);
  EXPECT_THAT(absl::flat_hash_set<std::string>(c1.label_values().begin(),
                                               c1.label_values().end()),
              ::testing::UnorderedElementsAre("A100", "B200"));

  EXPECT_EQ(c2.label_key(), "accelerator");
  EXPECT_EQ(c2.operator_(), rpc::LabelSelectorOperator::LABEL_OPERATOR_NOT_IN);
  ASSERT_EQ(c2.label_values_size(), 1);
  EXPECT_EQ(c2.label_values(0), "TPU");
}

}  // namespace gcs
}  // namespace ray
