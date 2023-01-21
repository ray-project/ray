
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
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {
class GcsMonitorServerTest : public ::testing::Test {
 public:
  GcsMonitorServerTest() : monitor_server_() {}

 protected:
  gcs::GcsMonitorServer monitor_server_;
};

TEST_F(GcsMonitorServerTest, TestRayVersion) {
  rpc::GetRayVersionRequest request;
  rpc::GetRayVersionReply reply;
  auto send_reply_callback =
      [](ray::Status status, std::function<void()> f1, std::function<void()> f2) {};

  monitor_server_.HandleGetRayVersion(request, &reply, send_reply_callback);

  ASSERT_EQ(reply.version(), kRayVersion);
}

}  // namespace ray
