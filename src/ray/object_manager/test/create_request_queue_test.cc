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

#include "ray/object_manager/plasma/create_request_queue.h"

#include "gtest/gtest.h"
#include "ray/common/status.h"

namespace plasma {

class MockClient : public ClientInterface {
 public:
  MockClient() {}
};

TEST(CreateRequestQueueTest, TestSimple) {
  CreateRequestQueue queue;

  bool created = false;
  auto request = [&]() {
    created = true;
    return Status();
  };
  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, request);
  ASSERT_FALSE(created);
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_TRUE(created);
}

TEST(CreateRequestQueueTest, TestTransientOom) {
  CreateRequestQueue queue;

  int num_created = 0;
  Status return_status = Status::TransientObjectStoreFull("");
  auto oom_request = [&]() {
    if (return_status.ok()) {
      num_created++;
    }
    return return_status;
  };
  auto blocked_request = [&]() {
    num_created++;
    return Status();
  };

  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, oom_request);
  queue.AddRequest(client, blocked_request);

  // Transient OOM should not use up any retries.
  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_EQ(num_created, 0);
  }

  // Return OK for the first request. The second request should also be served.
  return_status = Status();
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_EQ(num_created, 2);
}

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
