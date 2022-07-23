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

#include "gtest/gtest.h"

using namespace ray;
using namespace ray::gcs;

class GcsHeartbeatManagerTest : public ::testing::Test {
 public:
  GcsHeartbeatManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "num_heartbeats_timeout": 4,
  "initial_num_heartbeats_timeout": 8
}
  )");
  }

  void SetUp() override {
    heartbeat_manager = std::make_unique<GcsHeartbeatManager>(
        io_service, [this](const NodeID &node_id) { dead_nodes.push_back(node_id); });
    io_thread = std::make_unique<std::thread>([this]() {
      boost::asio::io_service::work(io_service);
      io_service.run();
    });
    heartbeat_manager->Start();
  }

  void TearDown() { heartbeat_manager->Stop(); }

  instrumented_io_context io_service;
  std::unique_ptr<std::thread> io_thread;
  std::unique_ptr<GcsHeartbeatManager> heartbeat_manager;
  std::vector<NodeID> dead_nodes;
};

TEST_F(GcsHeartbeatManagerTest, TestBasic) {}
