
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

#include <memory>

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/gcs/gcs_server/gcs_monitor_server.h"
#include "mock/ray/gcs/gcs_server/gcs_node_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_resource_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_placement_group_manager.h"
// clang-format on

using namespace testing;

namespace ray {

NodeResources ConstructNodeResources(
    absl::flat_hash_map<ResourceID, FixedPoint> available,
    absl::flat_hash_map<ResourceID, FixedPoint> total) {
  NodeResources resources;
  resources.available = ResourceRequest(available);
  resources.total = ResourceRequest(total);
  return resources;
}

rpc::ResourceDemand ConstructResourceDemand(
    absl::flat_hash_map<std::string, double> shape,
    int num_ready_requests_queued,
    int num_infeasible_requests_queued,
    int backlog_size) {
  rpc::ResourceDemand demand;
  demand.mutable_shape()->insert(shape.begin(), shape.end());
  demand.set_num_ready_requests_queued(num_ready_requests_queued);
  demand.set_num_infeasible_requests_queued(num_infeasible_requests_queued);
  demand.set_backlog_size(backlog_size);

  return demand;
}

std::shared_ptr<gcs::GcsPlacementGroup> ConstructPlacementGroupDemand(
    std::vector<absl::flat_hash_map<std::string, double>> bundles,
    rpc::PlacementStrategy strategy

) {
  rpc::PlacementGroupTableData data;
  for (auto &bundle : bundles) {
    auto rpc_bundle = data.add_bundles();
    rpc_bundle->mutable_unit_resources()->insert(bundle.begin(), bundle.end());
  }
  data.set_strategy(strategy);
  auto counter =
      std::make_shared<CounterMap<rpc::PlacementGroupTableData::PlacementGroupState>>();
  auto ptr = std::make_shared<gcs::GcsPlacementGroup>(data, counter);
  return ptr;
}

class GcsMonitorServerTest : public ::testing::Test {
 public:
  GcsMonitorServerTest()
      : mock_node_manager_(std::make_shared<gcs::MockGcsNodeManager>()),
        cluster_resource_manager_(io_context_),
        mock_resource_manager_(
            std::make_shared<gcs::MockGcsResourceManager>(cluster_resource_manager_)),
        mock_placement_group_manager_(
            std::make_shared<gcs::MockGcsPlacementGroupManager>(*mock_resource_manager_)),
        monitor_server_(mock_node_manager_,
                        cluster_resource_manager_,
                        mock_resource_manager_,
                        mock_placement_group_manager_) {}

  absl::flat_hash_map<NodeID, rpc::ResourcesData> &NodeResourceUsages() {
    return mock_resource_manager_->node_resource_usages_;
  }

  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> &AliveNodes() {
    return mock_node_manager_->alive_nodes_;
  }

  absl::btree_multimap<
      int64_t,
      std::pair<ExponentialBackOff, std::shared_ptr<gcs::GcsPlacementGroup>>>
      &PendingPlacementGroups() {
    return mock_placement_group_manager_->pending_placement_groups_;
  }

  std::deque<std::shared_ptr<gcs::GcsPlacementGroup>> &InfeasiblePlacementGroups() {
    return mock_placement_group_manager_->infeasible_placement_groups_;
  }

 protected:
  instrumented_io_context io_context_;
  std::shared_ptr<gcs::MockGcsNodeManager> mock_node_manager_;
  ClusterResourceManager cluster_resource_manager_;
  std::shared_ptr<gcs::MockGcsResourceManager> mock_resource_manager_;
  std::shared_ptr<gcs::MockGcsPlacementGroupManager> mock_placement_group_manager_;
  gcs::GcsMonitorServer monitor_server_;
};

TEST_F(GcsMonitorServerTest, TestRayVersion) {
  rpc::GetRayVersionRequest request;
  rpc::GetRayVersionReply reply;
  bool replied = false;
  auto send_reply_callback = [&replied](ray::Status status,
                                        std::function<void()> f1,
                                        std::function<void()> f2) { replied = true; };

  monitor_server_.HandleGetRayVersion(request, &reply, send_reply_callback);

  ASSERT_EQ(reply.version(), kRayVersion);
  ASSERT_TRUE(replied);
}

TEST_F(GcsMonitorServerTest, TestDrainAndKillNode) {
  rpc::DrainAndKillNodeRequest request;
  rpc::DrainAndKillNodeReply reply;
  bool replied = false;
  auto send_reply_callback = [&replied](ray::Status status,
                                        std::function<void()> f1,
                                        std::function<void()> f2) { replied = true; };

  *request.add_node_ids() = NodeID::FromRandom().Binary();
  *request.add_node_ids() = NodeID::FromRandom().Binary();

  EXPECT_CALL(*mock_node_manager_, DrainNode(_)).Times(Exactly(2));
  monitor_server_.HandleDrainAndKillNode(request, &reply, send_reply_callback);

  ASSERT_EQ(reply.drained_nodes().size(), 2);
  ASSERT_TRUE(replied);
}

TEST_F(GcsMonitorServerTest, TestGetSchedulingStatus) {
  rpc::GetSchedulingStatusRequest request;
  rpc::GetSchedulingStatusReply reply;
  bool replied = false;
  auto send_reply_callback = [&replied](ray::Status status,
                                        std::function<void()> f1,
                                        std::function<void()> f2) { replied = true; };

  NodeID id_1 = NodeID::FromRandom();
  NodeID id_2 = NodeID::FromRandom();
  NodeID id_3 = NodeID::FromRandom();

  {
    // Setup resource demand mocks.
    rpc::ResourcesData data;
    data.mutable_resource_load_by_shape()->add_resource_demands()->CopyFrom(
        ConstructResourceDemand(
            {
                {"CPU", 0.75},
            },
            1,
            2,
            3));
    data.mutable_resource_load_by_shape()->add_resource_demands()->CopyFrom(
        ConstructResourceDemand(
            {
                {"custom", 0.25},
            },
            1,
            1,
            1));

    data.mutable_resource_load_by_shape()->add_resource_demands()->CopyFrom(
        ConstructResourceDemand(
            {
                {"CPU", 0.75},
                {"custom", 0.25},
            },
            1,
            1,
            1));
    NodeResourceUsages()[id_1] = data;
  }
  {
    // Setup some placement group demand mocks.
    auto &pending_pgs = PendingPlacementGroups();
    for (int i = 0; i < 2; i++) {
      pending_pgs.insert(
          {0,
           {{},
            ConstructPlacementGroupDemand({{{"CPU", 1}, {"GPU", 1}}, {{"CPU", 1}}},
                                          rpc::PlacementStrategy::STRICT_SPREAD)}});
    }

    auto &infeasible_pgs = InfeasiblePlacementGroups();
    for (int i = 0; i < 3; i++) {
      infeasible_pgs.push_back(ConstructPlacementGroupDemand(
          {{{"GPU", 1}}, {{"GPU", 1}}}, rpc::PlacementStrategy::STRICT_PACK));
    }
  }
  {
    // Setup the node management mocks.
    cluster_resource_manager_.AddOrUpdateNode(
        scheduling::NodeID(id_1.Binary()),
        ConstructNodeResources(
            {{scheduling::ResourceID::CPU(), 0.5}, {scheduling::ResourceID("custom"), 4}},
            {{scheduling::ResourceID::CPU(), 1}, {scheduling::ResourceID("custom"), 8}}));
    AliveNodes()[id_1] = Mocker::GenNodeInfo(0, "1.1.1.1", "Node1");

    cluster_resource_manager_.AddOrUpdateNode(
        scheduling::NodeID(id_2.Binary()),
        ConstructNodeResources(
            {{scheduling::ResourceID::CPU(), 0.5}, {scheduling::ResourceID("custom"), 4}},
            {{scheduling::ResourceID::CPU(), 1}, {scheduling::ResourceID("custom"), 8}}));

    AliveNodes()[id_3] = Mocker::GenNodeInfo(0, "1.1.1.3", "Node1");
  }

  monitor_server_.HandleGetSchedulingStatus(request, &reply, send_reply_callback);

  ASSERT_TRUE(replied);
  {
    // Check the node_statuses field looks good.
    ASSERT_EQ(reply.node_statuses().size(), 1);
    ASSERT_EQ(reply.node_statuses(0).node_id(), id_1.Binary());
    ASSERT_EQ(reply.node_statuses(0).address(), "1.1.1.1");

    ASSERT_EQ(reply.node_statuses()[0].available_resources().size(), 2);
    ASSERT_EQ(reply.mutable_node_statuses(0)->mutable_available_resources()->at("CPU"),
              0.5);
    ASSERT_EQ(reply.mutable_node_statuses(0)->mutable_available_resources()->at("custom"),
              4);

    ASSERT_EQ(reply.node_statuses()[0].total_resources().size(), 2);
    ASSERT_EQ(reply.mutable_node_statuses(0)->mutable_total_resources()->at("CPU"), 1);
    ASSERT_EQ(reply.mutable_node_statuses(0)->mutable_total_resources()->at("custom"), 8);
  }
  {
    // Check the resource requests field looks good.
    ASSERT_EQ(reply.resource_requests().size(), 8);

    bool cpu_found = false;
    bool custom_found = false;
    bool cpu_and_custom_found = false;
    bool found_pending_pg = false;
    bool found_infeasible_pg = false;

    for (const auto &request : reply.resource_requests()) {
      RAY_LOG(ERROR) << request.DebugString();
      if (request.resource_request_type() ==
          rpc::ResourceRequest_ResourceRequestType::
              ResourceRequest_ResourceRequestType_TASK_RESERVATION) {
        ASSERT_EQ(request.bundles().size(), 1);
        const auto &resources = request.bundles()[0].resources();
        if (resources.size() == 1 && resources.begin()->first == "CPU") {
          cpu_found = true;
          ASSERT_EQ(request.count(), 6);
          ASSERT_EQ(resources.begin()->second, 0.75);
        }
        if (resources.size() == 1 && resources.begin()->first == "custom") {
          custom_found = true;
          ASSERT_EQ(request.count(), 3);
          ASSERT_EQ(resources.begin()->second, 0.25);
        }
        if (resources.size() == 2) {
          cpu_and_custom_found = true;
          ASSERT_EQ(resources.at("CPU"), 0.75);
          ASSERT_EQ(resources.at("custom"), 0.25);
          ASSERT_EQ(request.count(), 3);
        }
      } else if (request.resource_request_type() ==
                 rpc::ResourceRequest_ResourceRequestType_STRICT_SPREAD_RESERVATION) {
        found_pending_pg = true;

      } else if (request.resource_request_type() ==
                 rpc::ResourceRequest_ResourceRequestType_STRICT_PACK_RESERVATION) {
        found_infeasible_pg = true;
      }
    }

    ASSERT_TRUE(cpu_found);
    ASSERT_TRUE(custom_found);
    ASSERT_TRUE(cpu_and_custom_found);
    ASSERT_TRUE(found_pending_pg);
    ASSERT_TRUE(found_infeasible_pg);
  }
}

TEST_F(GcsMonitorServerTest, TestPlacementGroupConversion) {
  auto check_bundles = [](const rpc::ResourceRequest &request) {
    ASSERT_EQ(request.bundles().size(), 2);
    bool cpu_bundle_found = false;
    bool gpu_bundle_found = false;
    for (const auto &bundle : request.bundles()) {
      if (bundle.resources().size() == 2) {
        cpu_bundle_found =
            bundle.resources().at("CPU") == 1 && bundle.resources().at("GPU") == 1;
      } else if (bundle.resources().size() == 1) {
        gpu_bundle_found = bundle.resources().at("GPU") == 1;
      }
    }
    ASSERT_TRUE(cpu_bundle_found);
    ASSERT_TRUE(gpu_bundle_found);
  };

  {
    auto gcs_pg = ConstructPlacementGroupDemand({{{"GPU", 1}, {"CPU", 1}}, {{"GPU", 1}}},
                                                rpc::PlacementStrategy::STRICT_PACK);
    rpc::ResourceRequest request;
    GcsPlacementGroupToResourceRequest(*gcs_pg, request);
    RAY_LOG(ERROR) << request.DebugString();
    ASSERT_EQ(request.resource_request_type(),
              rpc::ResourceRequest_ResourceRequestType::
                  ResourceRequest_ResourceRequestType_STRICT_PACK_RESERVATION);
    check_bundles(request);
  }
  {
    auto gcs_pg = ConstructPlacementGroupDemand({{{"GPU", 1}, {"CPU", 1}}, {{"GPU", 1}}},
                                                rpc::PlacementStrategy::STRICT_SPREAD);
    rpc::ResourceRequest request;
    GcsPlacementGroupToResourceRequest(*gcs_pg, request);
    RAY_LOG(ERROR) << request.DebugString();
    ASSERT_EQ(request.resource_request_type(),
              rpc::ResourceRequest_ResourceRequestType::
                  ResourceRequest_ResourceRequestType_STRICT_SPREAD_RESERVATION);
    check_bundles(request);
  }
  {
    auto gcs_pg = ConstructPlacementGroupDemand({{{"GPU", 1}, {"CPU", 1}}, {{"GPU", 1}}},
                                                rpc::PlacementStrategy::PACK);
    rpc::ResourceRequest request;
    GcsPlacementGroupToResourceRequest(*gcs_pg, request);
    RAY_LOG(ERROR) << request.DebugString();
    ASSERT_EQ(request.resource_request_type(),
              rpc::ResourceRequest_ResourceRequestType::
                  ResourceRequest_ResourceRequestType_PACK_RESERVATION);
    check_bundles(request);
  }
  {
    auto gcs_pg = ConstructPlacementGroupDemand({{{"GPU", 1}, {"CPU", 1}}, {{"GPU", 1}}},
                                                rpc::PlacementStrategy::SPREAD);
    rpc::ResourceRequest request;
    GcsPlacementGroupToResourceRequest(*gcs_pg, request);
    RAY_LOG(ERROR) << request.DebugString();
    ASSERT_EQ(request.resource_request_type(),
              rpc::ResourceRequest_ResourceRequestType::
                  ResourceRequest_ResourceRequestType_SPREAD_RESERVATION);
    check_bundles(request);
  }
}

}  // namespace ray
