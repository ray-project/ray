// Copyright 2020-2021 The Ray Authors.
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

#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/optional.hpp>
#include <boost/thread.hpp>
#include <cstdlib>
#include <unordered_map>

using namespace boost;
using namespace boost::asio;
using namespace boost::asio::ip;

#include <ray/rpc/grpc_server.h>

#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_health_check_manager.h"

int GetFreePort() {
  io_service io_service;
  tcp::acceptor acceptor(io_service);
  tcp::endpoint endpoint;

  // try to bind to port 0 to find a free port
  acceptor.open(tcp::v4());
  acceptor.bind(tcp::endpoint(tcp::v4(), 0));
  endpoint = acceptor.local_endpoint();
  auto port = endpoint.port();
  acceptor.close();
  return port;
}

using namespace ray;
using namespace std::literals::chrono_literals;

class GcsHealthCheckManagerTest : public ::testing::Test {
 protected:
  GcsHealthCheckManagerTest() {}
  void SetUp() override {
    grpc::EnableDefaultHealthCheckService(true);

    health_check = std::make_unique<gcs::GcsHealthCheckManager>(
        io_service,
        [this](const NodeID &id) { dead_nodes.insert(id); },
        initial_delay_ms,
        timeout_ms,
        period_ms,
        failure_threshold);
  }

  void TearDown() override {
    io_service.poll();
    io_service.stop();

    // Stop the servers.
    for (auto [_, server] : servers) {
      server->Shutdown();
    }

    // Allow gRPC to cleanup.
    boost::this_thread::sleep_for(boost::chrono::seconds(2));
  }

  NodeID AddServer(bool alive = true) {
    std::promise<int> port_promise;
    auto node_id = NodeID::FromRandom();
    auto port = GetFreePort();
    RAY_LOG(INFO) << "Get port " << port;
    auto server = std::make_shared<rpc::GrpcServer>(node_id.Hex(), port, true);

    auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    server->Run();
    if (alive) {
      server->GetServer().GetHealthCheckService()->SetServingStatus(node_id.Hex(), true);
    }
    servers.emplace(node_id, server);
    health_check->AddNode(node_id, channel);
    return node_id;
  }

  void StopServing(const NodeID &id) {
    auto iter = servers.find(id);
    RAY_CHECK(iter != servers.end());
    iter->second->GetServer().GetHealthCheckService()->SetServingStatus(false);
  }

  void StartServing(const NodeID &id) {
    auto iter = servers.find(id);
    RAY_CHECK(iter != servers.end());
    iter->second->GetServer().GetHealthCheckService()->SetServingStatus(true);
  }

  void DeleteServer(const NodeID &id) {
    auto iter = servers.find(id);
    if (iter != servers.end()) {
      iter->second->Shutdown();
      servers.erase(iter);
    }
  }

  void Run(size_t n = 1) {
    // If n == 0 it mean we just run it and return.
    if (n == 0) {
      io_service.run();
      io_service.restart();
      return;
    }

    while (n) {
      auto i = io_service.run_one();
      n -= i;
      io_service.restart();
    }
  }

  instrumented_io_context io_service;
  std::unique_ptr<gcs::GcsHealthCheckManager> health_check;
  std::unordered_map<NodeID, std::shared_ptr<rpc::GrpcServer>> servers;
  std::unordered_set<NodeID> dead_nodes;
  const int64_t initial_delay_ms = 100;
  const int64_t timeout_ms = 10;
  const int64_t period_ms = 10;
  const int64_t failure_threshold = 5;
};

TEST_F(GcsHealthCheckManagerTest, TestBasic) {
  auto node_id = AddServer();
  Run(0);  // Initial run
  ASSERT_TRUE(dead_nodes.empty());

  // Run the first health check
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  StopServing(node_id);

  for (auto i = 0; i < failure_threshold; ++i) {
    Run(2);  // One for starting RPC and one for the RPC callback.
  }

  Run();  // For failure callback.

  ASSERT_EQ(1, dead_nodes.size());
  ASSERT_TRUE(dead_nodes.count(node_id));
}

TEST_F(GcsHealthCheckManagerTest, StoppedAndResume) {
  auto node_id = AddServer();
  Run(0);  // Initial run
  ASSERT_TRUE(dead_nodes.empty());

  // Run the first health check
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  StopServing(node_id);

  for (auto i = 0; i < failure_threshold; ++i) {
    Run(2);  // One for starting RPC and one for the RPC callback.
    if (i == failure_threshold / 2) {
      StartServing(node_id);
    }
  }

  Run();  // For failure callback.

  ASSERT_EQ(0, dead_nodes.size());
}

TEST_F(GcsHealthCheckManagerTest, Crashed) {
  auto node_id = AddServer();
  Run(0);  // Initial run
  ASSERT_TRUE(dead_nodes.empty());

  // Run the first health check
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());

  // Check it again
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());

  DeleteServer(node_id);

  for (auto i = 0; i < failure_threshold; ++i) {
    Run(2);  // One for starting RPC and one for the RPC callback.
  }

  Run();  // For failure callback.

  ASSERT_EQ(1, dead_nodes.size());
  ASSERT_TRUE(dead_nodes.count(node_id));
}

TEST_F(GcsHealthCheckManagerTest, NodeRemoved) {
  auto node_id = AddServer();
  Run(0);  // Initial run
  ASSERT_TRUE(dead_nodes.empty());

  // Run the first health check
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  health_check->RemoveNode(node_id);

  // Make sure it's not monitored any more
  for (auto i = 0; i < failure_threshold; ++i) {
    io_service.poll();
  }

  ASSERT_EQ(0, dead_nodes.size());
  ASSERT_EQ(0, health_check->GetAllNodes().size());
}

TEST_F(GcsHealthCheckManagerTest, NoRegister) {
  auto node_id = AddServer(false);
  for (auto i = 0; i < failure_threshold; ++i) {
    Run(2);  // One for starting RPC and one for the RPC callback.
  }

  Run(2);
  ASSERT_EQ(1, dead_nodes.size());
  ASSERT_TRUE(dead_nodes.count(node_id));
}

TEST_F(GcsHealthCheckManagerTest, StressTest) {
  boost::asio::io_service::work work(io_service);
  std::srand(std::time(nullptr));
  auto t = std::make_unique<std::thread>([this]() { io_service.run(); });

  std::vector<NodeID> alive_nodes;

  for (int i = 0; i < 200; ++i) {
    alive_nodes.emplace_back(AddServer(true));
    std::this_thread::sleep_for(10ms);
  }

  for (size_t i = 0; i < 200000000UL; ++i) {
    auto iter = alive_nodes.begin() + std::rand() % alive_nodes.size();
    DeleteServer(*iter);
    alive_nodes.erase(iter);
    alive_nodes.emplace_back(AddServer(true));
  }
  RAY_LOG(INFO) << "Finished!";
  io_service.stop();
  t->join();
}

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");

  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ray::RayLog::InstallTerminateHandler();

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
