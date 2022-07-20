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

#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

using ::testing::_;

class GcsResourceReportPollerTest : public ::testing::Test {
 public:
  GcsResourceReportPollerTest()
      : current_time_(0),
        gcs_resource_report_poller_(
            nullptr,
            [](const rpc::ResourcesData &) {},
            [this]() { return current_time_; },
            [this](
                const rpc::Address &address,
                std::shared_ptr<rpc::NodeManagerClientPool> &client_pool,
                std::function<void(const Status &,
                                   const rpc::RequestResourceReportReply &)> callback) {
              if (request_report_) {
                request_report_(address, client_pool, callback);
              }
            }

        ) {}

  void RunPollingService() {
    RAY_LOG(ERROR) << "Running service...";
    gcs_resource_report_poller_.polling_service_.run();
    gcs_resource_report_poller_.polling_service_.restart();
    RAY_LOG(ERROR) << "Done";
  }

  void Tick(int64_t inc) {
    current_time_ += inc;
    gcs_resource_report_poller_.TryPullResourceReport();
  }

  int64_t current_time_;
  std::function<void(
      const rpc::Address &,
      std::shared_ptr<rpc::NodeManagerClientPool> &,
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>)>
      request_report_;

  boost::asio::io_service io_service_;
  gcs::GcsResourceReportPoller gcs_resource_report_poller_;
};

TEST_F(GcsResourceReportPollerTest, TestBasic) {
  bool rpc_sent = false;
  request_report_ =
      [&rpc_sent](
          const rpc::Address &,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        rpc_sent = true;
        rpc::RequestResourceReportReply temp;
        callback(Status::OK(), temp);
      };

  auto node_info = Mocker::GenNodeInfo();
  gcs_resource_report_poller_.HandleNodeAdded(*node_info);
  // We try to send the rpc from the polling service.
  ASSERT_FALSE(rpc_sent);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);

  // Not enough time has passed to send another tick.
  rpc_sent = false;
  Tick(1);
  RunPollingService();
  ASSERT_FALSE(rpc_sent);

  // Now enough time has passed.
  Tick(100);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);
}

TEST_F(GcsResourceReportPollerTest, TestFailedRpc) {
  bool rpc_sent = false;
  request_report_ =
      [&rpc_sent](
          const rpc::Address &,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        RAY_LOG(ERROR) << "Requesting";
        rpc_sent = true;
        rpc::RequestResourceReportReply temp;
        callback(Status::TimedOut("error"), temp);
      };

  auto node_info = Mocker::GenNodeInfo();
  gcs_resource_report_poller_.HandleNodeAdded(*node_info);
  // We try to send the rpc from the polling service.
  ASSERT_FALSE(rpc_sent);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);

  // Not enough time has passed to send another tick.
  rpc_sent = false;
  Tick(1);
  RunPollingService();
  ASSERT_FALSE(rpc_sent);

  // Now enough time has passed.
  Tick(100);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);
}

TEST_F(GcsResourceReportPollerTest, TestMaxInFlight) {
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodes;
  for (int i = 0; i < 200; i++) {
    nodes.emplace_back(Mocker::GenNodeInfo());
  }

  std::vector<
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>>
      callbacks;

  int num_rpcs_sent = 0;
  request_report_ =
      [&](const rpc::Address &,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        num_rpcs_sent++;
        callbacks.emplace_back(callback);
      };

  for (const auto &node : nodes) {
    gcs_resource_report_poller_.HandleNodeAdded(*node);
  }
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);

  for (int i = 0; i < 100; i++) {
    const auto &callback = callbacks[i];
    rpc::RequestResourceReportReply temp;
    callback(Status::OK(), temp);
  }
  RunPollingService();
  // Requests are sent as soon as possible (don't wait for the next tick).
  ASSERT_EQ(num_rpcs_sent, 200);
}

TEST_F(GcsResourceReportPollerTest, TestNodeRemoval) {
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodes;
  for (int i = 0; i < 200; i++) {
    nodes.emplace_back(Mocker::GenNodeInfo());
  }

  std::vector<
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>>
      callbacks;

  int num_rpcs_sent = 0;
  request_report_ =
      [&](const rpc::Address &,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        num_rpcs_sent++;
        callbacks.emplace_back(callback);
      };

  for (const auto &node : nodes) {
    gcs_resource_report_poller_.HandleNodeAdded(*node);
  }
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);

  Tick(50);
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);

  // Remove the nodes while there are inflight rpcs.
  // Since we added all the nodes at once, the request order is LIFO.
  for (int i = 199; i >= 100; i--) {
    gcs_resource_report_poller_.HandleNodeRemoved(*nodes[i]);
  }

  for (int i = 0; i < 100; i++) {
    const auto &callback = callbacks[i];
    rpc::RequestResourceReportReply temp;
    callback(Status::TimedOut("Timed out"), temp);
  }
  RunPollingService();
  // Requests are sent as soon as possible (don't wait for the next tick).
  ASSERT_EQ(num_rpcs_sent, 200);
}

TEST_F(GcsResourceReportPollerTest, TestPrioritizeNewNodes) {
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodes;
  for (int i = 0; i < 200; i++) {
    nodes.emplace_back(Mocker::GenNodeInfo());
  }

  std::vector<
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>>
      callbacks;
  std::unordered_set<NodeID> nodes_requested;

  int num_rpcs_sent = 0;
  request_report_ =
      [&](const rpc::Address &address,
          std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        num_rpcs_sent++;
        callbacks.emplace_back(callback);
        auto node_id = NodeID::FromBinary(address.raylet_id());
        nodes_requested.insert(node_id);
      };

  for (int i = 0; i < 100; i++) {
    const auto &node = nodes[i];
    gcs_resource_report_poller_.HandleNodeAdded(*node);
  }
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);

  Tick(50);
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);
  ASSERT_EQ(nodes_requested.size(), 100);

  for (int i = 0; i < 100; i++) {
    const auto &callback = callbacks[i];
    rpc::RequestResourceReportReply temp;
    callback(Status::OK(), temp);
  }
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);
  ASSERT_EQ(nodes_requested.size(), 100);

  // The first wave of nodes should run at t=150.
  Tick(50);
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 100);
  ASSERT_EQ(nodes_requested.size(), 100);

  // We shouuld now request from the new nodes before repeating the first wave of nodes.
  for (int i = 100; i < 200; i++) {
    const auto &node = nodes[i];
    gcs_resource_report_poller_.HandleNodeAdded(*node);
  }
  RunPollingService();
  ASSERT_EQ(num_rpcs_sent, 200);
  ASSERT_EQ(nodes_requested.size(), 200);
}

}  // namespace gcs
}  // namespace ray
