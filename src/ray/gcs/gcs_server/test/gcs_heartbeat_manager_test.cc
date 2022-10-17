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

#include "absl/synchronization/mutex.h"
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
  "num_heartbeats_timeout": 3,
  "gcs_failover_worker_reconnect_timeout": 5
}
  )");
  }

  void SetUp() override {
    heartbeat_manager =
        std::make_unique<GcsHeartbeatManager>(io_service, [this](const NodeID &node_id) {
          absl::MutexLock lock(&mutex_);
          dead_nodes.push_back(node_id);
        });
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
  mutable absl::Mutex mutex_;
  // This field needs to be protected because it is accessed
  // by a different thread created by `heartbeat_manager`.
  std::vector<NodeID> dead_nodes GUARDED_BY(mutex_);
  ;
};

#ifndef __APPLE__
TEST_F(GcsHeartbeatManagerTest, TestBasicTimeout) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  AddNode(node_1);

  while (true) {
    absl::MutexLock lock(&mutex_);
    if (absl::Now() - start >= absl::Microseconds(1800)) {
      break;
    }
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(3s);

  {
    absl::MutexLock lock(&mutex_);
    ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
  }
}
#endif

TEST_F(GcsHeartbeatManagerTest, TestBasicReport) {
  auto node_1 = NodeID::FromRandom();
  auto start = absl::Now();
  AddNode(node_1);

  while (true) {
    absl::MutexLock lock(&mutex_);
    if (absl::Now() - start >= absl::Seconds(4)) {
      break;
    }
    ASSERT_TRUE(dead_nodes.empty());
    io_service.post(
        [&]() {
          rpc::ReportHeartbeatReply reply;
          rpc::ReportHeartbeatRequest request;
          request.mutable_heartbeat()->set_node_id(node_1.Binary());
          heartbeat_manager->HandleReportHeartbeat(
              request, &reply, [](auto, auto, auto) {});
        },
        "HandleReportHeartbeat");
    std::this_thread::sleep_for(0.1s);
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

  while (true) {
    absl::MutexLock lock(&mutex_);
    if (absl::Now() - start >= absl::Seconds(4)) {
      break;
    }
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(3s);
  {
    absl::MutexLock lock(&mutex_);
    ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
  }
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

  while (absl::Now() - start < absl::Seconds(2)) {
    io_service.post(
        [&]() {
          rpc::ReportHeartbeatReply reply;
          rpc::ReportHeartbeatRequest request;
          request.mutable_heartbeat()->set_node_id(node_1.Binary());
          heartbeat_manager->HandleReportHeartbeat(
              request, &reply, [](auto, auto, auto) {});
        },
        "HandleReportHeartbeat");
    // Added a sleep to avoid io service overloaded.
    std::this_thread::sleep_for(0.1s);
  }

  while (true) {
    absl::MutexLock lock(&mutex_);
    if (absl::Now() - start >= absl::Seconds(2)) {
      break;
    }
    ASSERT_TRUE(dead_nodes.empty());
  }

  std::this_thread::sleep_for(3s);
  {
    absl::MutexLock lock(&mutex_);
    ASSERT_EQ(std::vector<NodeID>{node_1}, dead_nodes);
  }
}
