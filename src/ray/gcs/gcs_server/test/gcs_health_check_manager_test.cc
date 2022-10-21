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
#include <unordered_map>

using namespace boost;

// Copied from
// https://stackoverflow.com/questions/14191855/how-do-you-mock-the-time-for-boost-timers
class mock_time_traits {
  typedef boost::asio::deadline_timer::traits_type source_traits;

 public:
  typedef source_traits::time_type time_type;
  typedef source_traits::duration_type duration_type;

  // Note this implemenation requires set_now(...) to be called before now()
  static time_type now() { return *now_; }

  // After modifying the clock, we need to sleep the thread to give the io_service
  // the opportunity to poll and notice the change in clock time
  static void set_now(time_type t) {
    now_ = t;
    boost::this_thread::sleep_for(boost::chrono::milliseconds(2));
  }

  static time_type add(time_type t, duration_type d) { return source_traits::add(t, d); }
  static duration_type subtract(time_type t1, time_type t2) {
    return source_traits::subtract(t1, t2);
  }
  static bool less_than(time_type t1, time_type t2) {
    return source_traits::less_than(t1, t2);
  }

  // This function is called by asio to determine how often to check
  // if the timer is ready to fire. By manipulating this function, we
  // can make sure asio detects changes to now_ in a timely fashion.
  static boost::posix_time::time_duration to_posix_duration(duration_type d) {
    return d < boost::posix_time::milliseconds(1) ? d
                                                  : boost::posix_time::milliseconds(1);
  }

 private:
  static boost::optional<time_type> now_;
};

boost::optional<mock_time_traits::time_type> mock_time_traits::now_;

using mock_deadline_timer =
    boost::asio::basic_deadline_timer<boost::posix_time::ptime, mock_time_traits>;

#define _TESTING_RAY_TIMER mock_deadline_timer

#include <ray/rpc/grpc_server.h>

#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_health_check_manager.h"

using namespace ray;
using namespace std::literals::chrono_literals;

class GcsHealthCheckManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    grpc::EnableDefaultHealthCheckService(true);
    mock_time_traits::set_now(
        boost::posix_time::time_from_string("2022-01-20 0:0:0.000"));
    health_check = std::make_unique<gcs::GcsHealthCheckManager>(
        io_service, [this](const NodeID &id) { dead_nodes.insert(id); });
    port = 10000;
  }

  void TearDown() override {
    io_service.poll();
    io_service.stop();
  }

  NodeID AddServer() {
    std::promise<int> port_promise;
    auto node_id = NodeID::FromRandom();

    auto server = std::make_shared<rpc::GrpcServer>(node_id.Hex(), port, true);

    auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    server->Run();
    servers.emplace(node_id, server);
    health_check->AddNode(node_id, channel);
    ++port;
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

  void AdvanceInitialDelay() {
    auto delta = boost::posix_time::millisec(
        RayConfig::instance().health_check_initial_delay_ms() + 10);
    mock_time_traits::set_now(mock_time_traits::now() + delta);
  }

  void AdvanceNextDelay() {
    auto delta =
        boost::posix_time::millisec(RayConfig::instance().health_check_period_ms() + 10);
    mock_time_traits::set_now(mock_time_traits::now() + delta);
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

  int port;
  std::unordered_map<NodeID, std::shared_ptr<rpc::GrpcServer>> servers;
  std::unique_ptr<gcs::GcsHealthCheckManager> health_check;
  instrumented_io_context io_service;

  std::unordered_set<NodeID> dead_nodes;
};

TEST_F(GcsHealthCheckManagerTest, TestBasic) {
  auto node_id = AddServer();
  Run(0);  // Initial run
  ASSERT_TRUE(dead_nodes.empty());

  // Run the first health check
  AdvanceInitialDelay();
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  AdvanceNextDelay();
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  StopServing(node_id);

  for (auto i = 0; i < RayConfig::instance().health_check_failure_threshold(); ++i) {
    AdvanceNextDelay();
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
  AdvanceInitialDelay();
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  AdvanceNextDelay();
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  StopServing(node_id);

  for (auto i = 0; i < RayConfig::instance().health_check_failure_threshold(); ++i) {
    AdvanceNextDelay();
    Run(2);  // One for starting RPC and one for the RPC callback.
    if (i == (RayConfig::instance().health_check_failure_threshold()) / 2) {
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
  AdvanceInitialDelay();
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  AdvanceNextDelay();
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());

  // Check it again
  AdvanceNextDelay();
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());

  DeleteServer(node_id);

  for (auto i = 0; i < RayConfig::instance().health_check_failure_threshold(); ++i) {
    AdvanceNextDelay();
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
  AdvanceInitialDelay();
  Run();
  ASSERT_TRUE(dead_nodes.empty());

  AdvanceNextDelay();
  Run(2);  // One for starting RPC and one for the RPC callback.
  ASSERT_TRUE(dead_nodes.empty());
  health_check->RemoveNode(node_id);

  // Make sure it's not monitored any more
  for (auto i = 0; i < RayConfig::instance().health_check_failure_threshold(); ++i) {
    AdvanceNextDelay();
    io_service.poll();
  }

  ASSERT_EQ(0, dead_nodes.size());
  ASSERT_EQ(0, health_check->GetAllNodes().size());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
