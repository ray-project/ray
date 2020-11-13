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
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/0,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  bool created = false;
  auto request = [&](bool reply_on_oom, bool evict_if_full) {
    created = true;
    return Status();
  };
  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, request);
  ASSERT_FALSE(created);
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_TRUE(created);
  ASSERT_EQ(num_store_full, 0);
}

TEST(CreateRequestQueueTest, TestOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  int num_created = 0;
  auto oom_request = [&](bool reply_on_oom, bool evict_if_full) {
    if (reply_on_oom) {
      num_created++;
      return Status();
    } else {
      return Status::ObjectStoreFull("");
    }
  };
  auto blocked_request = [&](bool reply_on_oom, bool evict_if_full) {
    num_created++;
    return Status();
  };

  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, oom_request);
  queue.AddRequest(client, blocked_request);

  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_EQ(num_created, 0);
  ASSERT_EQ(num_store_full, 2);

  // Retries used up. The first request should reply with OOM and the second
  // request should also be served.
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_EQ(num_created, 2);
  ASSERT_EQ(num_store_full, 2);
}

TEST(CreateRequestQueueTest, TestTransientOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  int num_created = 0;
  Status return_status = Status::TransientObjectStoreFull("");
  auto oom_request = [&](bool reply_on_oom, bool evict_if_full) {
    if (return_status.ok()) {
      num_created++;
    }
    return return_status;
  };
  auto blocked_request = [&](bool reply_on_oom, bool evict_if_full) {
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
    ASSERT_EQ(num_store_full, 0);
  }

  // Return OK for the first request. The second request should also be served.
  return_status = Status();
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_EQ(num_created, 2);
  ASSERT_EQ(num_store_full, 0);
}

TEST(CreateRequestQueueTest, TestTransientOomThenOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  int num_created = 0;
  Status return_status = Status::TransientObjectStoreFull("");
  auto oom_request = [&](bool reply_on_oom, bool evict_if_full) {
    if (reply_on_oom) {
      num_created++;
      return Status();
    } else {
      return return_status;
    }
  };
  auto blocked_request = [&](bool reply_on_oom, bool evict_if_full) {
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
    ASSERT_EQ(num_store_full, 0);
  }

  // Now we are actually OOM.
  return_status = Status::ObjectStoreFull("");
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_EQ(num_created, 0);
  ASSERT_EQ(num_store_full, 2);

  // Retries used up. The first request should reply with OOM and the second
  // request should also be served.
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_EQ(num_created, 2);
  ASSERT_EQ(num_store_full, 2);
}

TEST(CreateRequestQueueTest, TestEvictIfFull) {
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() {});

  auto oom_request = [&](bool reply_on_oom, bool evict_if_full) {
    RAY_CHECK(evict_if_full);
    return Status::ObjectStoreFull("");
  };

  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, oom_request);
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
}

TEST(CreateRequestQueueTest, TestNoEvictIfFull) {
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/false,
      /*on_store_full=*/[&]() {});

  bool first_try = true;
  auto oom_request = [&](bool reply_on_oom, bool evict_if_full) {
    if (first_try) {
      RAY_CHECK(!evict_if_full);
      first_try = false;
    } else {
      RAY_CHECK(evict_if_full);
    }
    return Status::ObjectStoreFull("");
  };

  auto client = std::make_shared<MockClient>();
  queue.AddRequest(client, oom_request);
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
}

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
