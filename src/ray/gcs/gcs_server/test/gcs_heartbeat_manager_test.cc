// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_heartbeat_manager.h"

#include <chrono>

#include "gtest/gtest.h"

using namespace ray;
using namespace ray::gcs;
using namespace std::chrono_literals;

class GcsHeartbeatManagerTest : public ::testing::Test {
 public:
  GcsHeartbeatManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "num_heartbeats_timeout": 2,
  "gcs_failover_worker_reconnect_timeout": 4
}
  )");
  }

  void SetUp() override {
    heartbeat_manager = std::make_unique<GcsHeartbeatManager>(
        io_service, [this](const NodeID &node_id) { dead_nodes.push_back(node_id); });
    heartbeat_manager->Start();
  }

  void AddNode(const NodeID &node_id) {
    rpc::GcsNodeInfo node_info;
    node_info.set_node_id(node_id.Binary());
    heartbeat_manager->AddNode(node_info);
  }

  void TearDown() override { heartbeat_manager->Stop(); }

  instrumented_io_context io_service;
  std::unique_ptr<GcsHeartbeatManager> heartbeat_manager;
  std::vector<NodeID> dead_nodes;
};

TEST_F(GcsHeartbeatManagerTest, TestBasicTimeout) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  AddNode(node_1);

  while (absl::Now() - start < absl::Seconds(1)) {
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(2s);

  ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
}

TEST_F(GcsHeartbeatManagerTest, TestBasicReport) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  AddNode(node_1);

  rpc::ReportHeartbeatReply reply;
  rpc::ReportHeartbeatRequest request;
  request.mutable_heartbeat()->set_node_id(node_1.Binary());
  while (absl::Now() - start < absl::Seconds(3)) {
    ASSERT_TRUE(dead_nodes.empty());
    // std::function<void(ray::Status, std::function<void()>, std::function<void()>)>'
    heartbeat_manager->HandleReportHeartbeat(request, &reply, [](auto, auto, auto) {});
  }
}

TEST_F(GcsHeartbeatManagerTest, TestBasicRestart) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  struct GcsInitDataTest : public GcsInitData {
    GcsInitDataTest() : GcsInitData(nullptr) {}
    auto &NodeData() { return node_table_data_; }
  };

  GcsInitDataTest init_data;
  rpc::GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  node_info.set_node_id(node_1.Binary());
  init_data.NodeData()[node_1] = node_info;

  heartbeat_manager->Initialize(init_data);

  while (absl::Now() - start < absl::Seconds(3)) {
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(2s);
  ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
}

TEST_F(GcsHeartbeatManagerTest, TestBasicRestart2) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  struct GcsInitDataTest : public GcsInitData {
    GcsInitDataTest() : GcsInitData(nullptr) {}
    auto &NodeData() { return node_table_data_; }
  };

  GcsInitDataTest init_data;
  rpc::GcsNodeInfo node_info;
  node_info.set_state(rpc::GcsNodeInfo::ALIVE);
  node_info.set_node_id(node_1.Binary());
  init_data.NodeData()[node_1] = node_info;

  heartbeat_manager->Initialize(init_data);

  rpc::ReportHeartbeatReply reply;
  rpc::ReportHeartbeatRequest request;

  while (absl::Now() - start < absl::Seconds(1)) {
    request.mutable_heartbeat()->set_node_id(node_1.Binary());
    heartbeat_manager->HandleReportHeartbeat(request, &reply, [](auto, auto, auto) {});
  }

  while (absl::Now() - start < absl::Seconds(1)) {
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(2s);
  ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
}
