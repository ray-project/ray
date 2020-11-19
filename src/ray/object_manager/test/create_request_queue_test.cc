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

#define ASSERT_REQUEST_UNFINISHED(queue, req_id) { \
  PlasmaObject result = {}; \
  Status status; \
  ASSERT_FALSE(queue.GetRequestResult(req_id, &result, &status)); \
} \

#define ASSERT_REQUEST_FINISHED(queue, req_id, expected_status) { \
  PlasmaObject result = {}; \
  Status status; \
 \
  ASSERT_TRUE(queue.GetRequestResult(req_id, &result, &status)); \
  if (expected_status.ok()) { \
    ASSERT_EQ(result.store_fd, 1234); \
  } \
  ASSERT_EQ(status.code(), expected_status.code()); \
} \

TEST(CreateRequestQueueTest, TestSimple) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/0,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  auto request = [&](bool evict_if_full, PlasmaObject *result) {
    result->store_fd = 1234;
    return Status::OK();
  };
  auto client = std::make_shared<MockClient>();
  auto req_id = queue.AddRequest(client, request);
  ASSERT_REQUEST_UNFINISHED(queue, req_id);

  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id, Status::OK());
  ASSERT_EQ(num_store_full, 0);
  // Request gets cleaned up after we get it.
  ASSERT_REQUEST_UNFINISHED(queue, req_id);

  auto req_id1 = queue.AddRequest(client, request);
  auto req_id2 = queue.AddRequest(client, request);
  auto req_id3 = queue.AddRequest(client, request);
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  ASSERT_REQUEST_UNFINISHED(queue, req_id3);

  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, Status::OK());
  ASSERT_REQUEST_FINISHED(queue, req_id2, Status::OK());
  ASSERT_REQUEST_FINISHED(queue, req_id3, Status::OK());
  ASSERT_EQ(num_store_full, 0);
  // Request gets cleaned up after we get it.
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  ASSERT_REQUEST_UNFINISHED(queue, req_id3);
}

TEST(CreateRequestQueueTest, TestOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    return Status::ObjectStoreFull("");
  };
  auto blocked_request = [&](bool evict_if_full, PlasmaObject *result) {
    result->store_fd = 1234;
    return Status::OK();
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(client, oom_request);
  auto req_id2 = queue.AddRequest(client, blocked_request);

  // Neither request was fulfilled.
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  ASSERT_EQ(num_store_full, 2);

  // Retries used up. The first request should reply with OOM and the second
  // request should also be served.
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_EQ(num_store_full, 2);

  // Both requests fulfilled.
  ASSERT_REQUEST_FINISHED(queue, req_id1, Status::ObjectStoreFull(""));
  ASSERT_REQUEST_FINISHED(queue, req_id2, Status::OK());
}

TEST(CreateRequestQueueTest, TestOomInfiniteRetry) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/-1,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    return Status::ObjectStoreFull("");
  };
  auto blocked_request = [&](bool evict_if_full, PlasmaObject *result) {
    result->store_fd = 1234;
    return Status::OK();
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(client, oom_request);
  auto req_id2 = queue.AddRequest(client, blocked_request);

  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
    ASSERT_EQ(num_store_full, i + 1);
  }

  // Neither request was fulfilled.
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
}

TEST(CreateRequestQueueTest, TestTransientOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  auto return_status = Status::TransientObjectStoreFull("");
  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    if (return_status.ok()) {
      result->store_fd = 1234;
    }
    return return_status;
  };
  auto blocked_request = [&](bool evict_if_full, PlasmaObject *result) {
    result->store_fd = 1234;
    return Status::OK();
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(client, oom_request);
  auto req_id2 = queue.AddRequest(client, blocked_request);

  // Transient OOM should not use up any retries.
  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
    ASSERT_EQ(num_store_full, 0);
  }

  // Return OK for the first request. The second request should also be served.
  return_status = Status::OK();
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, Status::OK());
  ASSERT_REQUEST_FINISHED(queue, req_id2, Status::OK());
}

TEST(CreateRequestQueueTest, TestTransientOomThenOom) {
  int num_store_full = 0;
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() { num_store_full++; });

  auto return_status = Status::TransientObjectStoreFull("");
  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    if (return_status.ok()) {
      result->store_fd = 1234;
    }
    return return_status;
  };
  auto blocked_request = [&](bool evict_if_full, PlasmaObject *result) {
    result->store_fd = 1234;
    return Status::OK();
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(client, oom_request);
  auto req_id2 = queue.AddRequest(client, blocked_request);

  // Transient OOM should not use up any retries.
  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
    ASSERT_EQ(num_store_full, 0);
  }

  // Now we are actually OOM.
  return_status = Status::ObjectStoreFull("");
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  ASSERT_EQ(num_store_full, 2);

  // Retries used up. The first request should reply with OOM and the second
  // request should also be served.
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, Status::ObjectStoreFull(""));
  ASSERT_REQUEST_FINISHED(queue, req_id2, Status::OK());
  ASSERT_EQ(num_store_full, 2);
}

TEST(CreateRequestQueueTest, TestEvictIfFull) {
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/true,
      /*on_store_full=*/[&]() {});

  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    RAY_CHECK(evict_if_full);
    return Status::ObjectStoreFull("");
  };

  auto client = std::make_shared<MockClient>();
  static_cast<void>(queue.AddRequest(client, oom_request));
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
}

TEST(CreateRequestQueueTest, TestNoEvictIfFull) {
  CreateRequestQueue queue(
      /*max_retries=*/2,
      /*evict_if_full=*/false,
      /*on_store_full=*/[&]() {});

  bool first_try = true;
  auto oom_request = [&](bool evict_if_full, PlasmaObject *result) {
    if (first_try) {
      RAY_CHECK(!evict_if_full);
      first_try = false;
    } else {
      RAY_CHECK(evict_if_full);
    }
    return Status::ObjectStoreFull("");
  };

  auto client = std::make_shared<MockClient>();
  static_cast<void>(queue.AddRequest(client, oom_request));
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
}

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
