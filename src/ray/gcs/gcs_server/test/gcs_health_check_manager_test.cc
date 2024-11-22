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

namespace {

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

}  // namespace

using namespace ray;
using namespace std::literals::chrono_literals;

class GcsHealthCheckManagerTest : public ::testing::Test {
 protected:
  GcsHealthCheckManagerTest() = default;

  void SetUp() override {
    grpc::EnableDefaultHealthCheckService(true);

    health_check = std::make_unique<gcs::GcsHealthCheckManager>(
        io_service,
        [](const NodeID &) { return absl::InfinitePast(); },
        [this](const NodeID &id) { dead_nodes.insert(id); },
        initial_delay_ms,
        timeout_ms,
        period_ms,
        failure_threshold);
  }

  // Reset [GcsHealthCheckManager] with a new node update status callback, which indicates
  // the given node has just been communicated with, thus no need for further health
  // check.
  void ResetHealthCheckManagerWithNoHealthCheck() {
    health_check = std::make_unique<gcs::GcsHealthCheckManager>(
        io_service,
        [](const NodeID &) { return absl::Now(); },
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

  ASSERT_EQ(1, dead_nodes.size());
  ASSERT_TRUE(dead_nodes.count(node_id));
}

// Testing senario:
// - Status update is already fresh enough, so no need for further health check;
// - For `io_context::run`, which gets blocked until enqueued tasks finish; considering
// no callback will be enqueued, manually inject callback to unblock;
// - For the test case, we simply check health check manager runs fine with no health
// check rpc.
//
// TODO(hjiang): The best way to unit test is to provide a mock gcs server, and check
// there's no invocation. It requires more work to provide grpc server interface and mock.
TEST_F(GcsHealthCheckManagerTest, TestSkipHealthCheck) {
  ResetHealthCheckManagerWithNoHealthCheck();

  // Simply add a node to the system and declare it dead.
  // Health check is not expected to be called, so still considered live.
  auto node_id = AddServer();
  Run(1);
  StopServing(node_id);

  // Provide a dummy callable and make sure it's invoked.
  std::promise<void> promise{};
  io_service.dispatch([&promise]() { promise.set_value(); }, "manual callable");

  Run();
  ASSERT_TRUE(dead_nodes.empty());
  promise.get_future().get();
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

  Run(1);
  ASSERT_EQ(1, dead_nodes.size());
  ASSERT_TRUE(dead_nodes.count(node_id));
}

TEST_F(GcsHealthCheckManagerTest, StressTest) {
#ifdef _RAY_TSAN_BUILD
  GTEST_SKIP() << "Disabled in tsan because of performance";
#endif
  boost::asio::io_service::work work(io_service);
  std::srand(std::time(nullptr));
  auto t = std::make_unique<std::thread>([this]() { this->io_service.run(); });

  std::vector<NodeID> alive_nodes;

  for (int i = 0; i < 200; ++i) {
    alive_nodes.emplace_back(AddServer(true));
    std::this_thread::sleep_for(10ms);
  }

  for (size_t i = 0; i < 20000UL; ++i) {
    RAY_LOG(INFO) << "Progress: " << i << "/20000";
    auto iter = alive_nodes.begin() + std::rand() % alive_nodes.size();
    health_check->RemoveNode(*iter);
    DeleteServer(*iter);
    alive_nodes.erase(iter);
    alive_nodes.emplace_back(AddServer(true));
  }
  RAY_LOG(INFO) << "Finished!";
  io_service.stop();
  t->join();
}
