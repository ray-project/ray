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

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

using ::testing::_;

class GcsResourceReportPollerTest : public ::testing::Test {
 public:
  GcsResourceReportPollerTest()
      : current_time_(0),
        gcs_resource_report_poller_(
            nullptr, nullptr, [](const rpc::ResourcesData &) {
                              },
            [this]() { return current_time_; },
            [this](const rpc::Address &address,
                   std::shared_ptr<rpc::NodeManagerClientPool> &client_pool,
                   std::function<void(const Status &,
                                      const rpc::RequestResourceReportReply &)>
                       callback) {
              if (request_report_) {
                request_report_(address, client_pool, callback);
              }
            }

        ) {}

  void RunPollingService() { gcs_resource_report_poller_.polling_service_.run(); }

  void Tick(int64_t inc) {
    current_time_ += inc;
    gcs_resource_report_poller_.Tick();
  }

  int64_t current_time_;
  std::function<void(
      const rpc::Address &, std::shared_ptr<rpc::NodeManagerClientPool> &,
      std::function<void(const Status &, const rpc::RequestResourceReportReply &)>)>
      request_report_;

  boost::asio::io_service io_service_;
  gcs::GcsResourceReportPoller gcs_resource_report_poller_;
};

TEST_F(GcsResourceReportPollerTest, TestBasic) {
  bool rpc_sent = false;
  request_report_ =
      [&rpc_sent](
          const rpc::Address &, std::shared_ptr<rpc::NodeManagerClientPool> &,
          std::function<void(const Status &, const rpc::RequestResourceReportReply &)>
              callback) {
        rpc_sent = true;
        rpc::RequestResourceReportReply temp;
        callback(Status::OK(), temp);
      };

  auto node_info = Mocker::GenNodeInfo();
  gcs_resource_report_poller_.HandleNodeAdded(node_info);
  // We try to send the rpc from the polling service.
  ASSERT_FALSE(rpc_sent);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);

  RAY_LOG(ERROR) << "===================";
  // Not enough time has passed to send another tick.
  rpc_sent = false;
  Tick(1);
  RunPollingService();
  ASSERT_FALSE(rpc_sent);
  RAY_LOG(ERROR) << "===================";

  // Now enough time has passed.
  Tick(100);
  RunPollingService();
  ASSERT_TRUE(rpc_sent);
}

}  // namespace gcs
}  // namespace ray
