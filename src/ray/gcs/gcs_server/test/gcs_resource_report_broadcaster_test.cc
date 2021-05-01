
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
#include "ray/gcs/gcs_server/gcs_resource_report_broadcaster.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

using ::testing::_;

class GcsResourceReportBroadcasterTest : public ::testing::Test {
 public:
  GcsResourceReportBroadcasterTest()
      : broadcaster_(
            /*raylet_client_pool*/ nullptr,
            /*get_resource_usage_batch_for_broadcast*/
            [](rpc::ResourceUsageBatchData &batch) {}

            ,
            /*send_batch*/ [this](const rpc::Address &address,
                                  std::shared_ptr<rpc::NodeManagerClientPool> &pool,
                                  std::string &data,
                                  const rpc::ClientCallback<rpc::UpdateResourceUsageReply>
                                      &callback) {}) {
    callbacks_.push_back();
  }

 private:
  std::deque<rpc::ClientCallback<rpc::UpdateResourceUsageReply>> callacks_;

  GcsResourceReportBroadcaster broadcaster_;
};

TEST_F(GcsResourceReportBroadcasterTest, TestBasic) {
  auto node_info = Mocker::GenNodeInfo();
  broadcaster_.HandleNodeAdded(node_info);
}

}  // namespace gcs
}  // namespace ray
