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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/util/filesystem.h"

namespace plasma {

class MockClient : public ClientInterface {
 public:
  MOCK_METHOD1(SendFd, Status(MEMFD_TYPE));
  MOCK_METHOD0(GetObjectIDs, const std::unordered_set<ray::ObjectID> &());
  MOCK_METHOD1(MarkObjectAsUsed, void(const ObjectID &object_id));
  MOCK_METHOD1(MarkObjectAsUnused, void(const ObjectID &object_id));
};

#define ASSERT_REQUEST_UNFINISHED(queue, req_id)                    \
  {                                                                 \
    PlasmaObject result = {};                                       \
    PlasmaError status;                                             \
    ASSERT_FALSE(queue.GetRequestResult(req_id, &result, &status)); \
  }

#define ASSERT_REQUEST_FINISHED(queue, req_id, expected_status)    \
  {                                                                \
    PlasmaObject result = {};                                      \
    PlasmaError status;                                            \
                                                                   \
    ASSERT_TRUE(queue.GetRequestResult(req_id, &result, &status)); \
    if (expected_status == PlasmaError::OK) {                      \
      ASSERT_EQ(result.data_size, 1234);                           \
    }                                                              \
    ASSERT_EQ(status, expected_status);                            \
  }

class CreateRequestQueueTest : public ::testing::Test {
 public:
  CreateRequestQueueTest()
      : oom_grace_period_s_(1),
        current_time_ns_(0),
        monitor_({"/"}, 1),
        queue_(
            monitor_,
            /*oom_grace_period_s=*/oom_grace_period_s_,
            /*spill_object_callback=*/[&]() { return false; },
            /*on_global_gc=*/[&]() { num_global_gc_++; },
            /*get_time=*/[&]() { return current_time_ns_; },
            /*debug_dump_handler*/ nullptr) {}

  void AssertNoLeaks() {
    ASSERT_TRUE(queue_.queue_.empty());
    ASSERT_TRUE(queue_.fulfilled_requests_.empty());
  }

  void TearDown() { current_time_ns_ = 0; }

  int64_t oom_grace_period_s_;
  int64_t current_time_ns_;
  ray::FileSystemMonitor monitor_;
  CreateRequestQueue queue_;
  int num_global_gc_ = 0;
};

TEST_F(CreateRequestQueueTest, TestSimple) {
  auto request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };
  // Advance the clock without processing objects. This shouldn't have an impact.
  current_time_ns_ += 10e9;
  auto client = std::make_shared<MockClient>();
  auto req_id = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id);

  ASSERT_TRUE(queue_.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue_, req_id, PlasmaError::OK);
  ASSERT_EQ(num_global_gc_, 0);
  // Request gets cleaned up after we get it.
  ASSERT_REQUEST_FINISHED(queue_, req_id, PlasmaError::UnexpectedError);

  auto req_id1 = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  auto req_id2 = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  auto req_id3 = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id2);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id3);

  ASSERT_TRUE(queue_.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue_, req_id1, PlasmaError::OK);
  ASSERT_REQUEST_FINISHED(queue_, req_id2, PlasmaError::OK);
  ASSERT_REQUEST_FINISHED(queue_, req_id3, PlasmaError::OK);
  ASSERT_EQ(num_global_gc_, 0);
  // Request gets cleaned up after we get it.
  ASSERT_REQUEST_FINISHED(queue_, req_id1, PlasmaError::UnexpectedError);
  ASSERT_REQUEST_FINISHED(queue_, req_id2, PlasmaError::UnexpectedError);
  ASSERT_REQUEST_FINISHED(queue_, req_id3, PlasmaError::UnexpectedError);
  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestOom) {
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    return PlasmaError::OutOfMemory;
  };
  auto blocked_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue_.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue_.AddRequest(ObjectID::Nil(), client, blocked_request, 1234);

  // Neither request was fulfilled.
  ASSERT_TRUE(queue_.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue_.ProcessRequests().IsObjectStoreFull());
  ASSERT_REQUEST_UNFINISHED(queue_, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id2);
  ASSERT_EQ(num_global_gc_, 2);

  // Grace period is done. The first request should reply with OutOfDisk and the second
  // request should also be served.
  current_time_ns_ += oom_grace_period_s_ * 2e9;
  ASSERT_TRUE(queue_.ProcessRequests().ok());
  ASSERT_EQ(num_global_gc_, 3);

  // Both requests fulfilled.
  ASSERT_REQUEST_FINISHED(queue_, req_id1, PlasmaError::OutOfDisk);
  ASSERT_REQUEST_FINISHED(queue_, req_id2, PlasmaError::OK);

  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestFallbackAllocator) {
  int num_fallbacks = 0;
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    if (fallback) {
      result->data_size = 1234;
      num_fallbacks += 1;
      return PlasmaError::OK;
    } else {
      return PlasmaError::OutOfMemory;
    }
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue_.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue_.AddRequest(ObjectID::Nil(), client, oom_request, 1234);

  // Neither request was fulfilled.
  ASSERT_TRUE(queue_.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue_.ProcessRequests().IsObjectStoreFull());
  ASSERT_REQUEST_UNFINISHED(queue_, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue_, req_id2);
  ASSERT_EQ(num_fallbacks, 0);

  // Grace period is done. The first request should reply with OOM and the second
  // request should also be served.
  current_time_ns_ += oom_grace_period_s_ * 2e9;
  ASSERT_TRUE(queue_.ProcessRequests().ok());
  ASSERT_EQ(num_fallbacks, 2);

  // Both requests fulfilled.
  ASSERT_REQUEST_FINISHED(queue_, req_id1, PlasmaError::OK);
  ASSERT_REQUEST_FINISHED(queue_, req_id2, PlasmaError::OK);

  AssertNoLeaks();
}

TEST(CreateRequestQueueParameterTest, TestOomInfiniteRetry) {
  int num_global_gc_ = 0;
  int64_t current_time_ns;
  ray::FileSystemMonitor monitor{{"/"}, 1};
  CreateRequestQueue queue(
      monitor,
      /*oom_grace_period_s=*/100,
      // Spilling is failing.
      /*spill_object_callback=*/[&]() { return false; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns; });

  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    return PlasmaError::OutOfMemory;
  };
  auto blocked_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue.AddRequest(ObjectID::Nil(), client, blocked_request, 1234);

  for (int i = 0; i < 10; i++) {
    // Advance 1 second.
    current_time_ns += 1e9;
    ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
    ASSERT_EQ(num_global_gc_, i + 1);
  }

  // Neither request was fulfilled.
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
}

TEST_F(CreateRequestQueueTest, TestTransientOom) {
  ray::FileSystemMonitor monitor{{"/"}, 1};
  CreateRequestQueue queue(
      monitor,
      /*oom_grace_period_s=*/oom_grace_period_s_,
      /*spill_object_callback=*/[&]() { return true; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns_; });

  auto return_status = PlasmaError::OutOfMemory;
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    if (return_status == PlasmaError::OK) {
      result->data_size = 1234;
    }
    return return_status;
  };
  auto blocked_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue.AddRequest(ObjectID::Nil(), client, blocked_request, 1234);

  // Transient OOM should happen until the grace period.
  for (int i = 0; i < 9; i++) {
    // Advance 0.1 seconds. OOM grace period is 1 second, so it should return transient
    // error.
    current_time_ns_ += 1e8;
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
    ASSERT_EQ(num_global_gc_, i + 1);
  }

  current_time_ns_ += oom_grace_period_s_ * 2e9;
  // Return OK for the first request. The second request should also be served.
  return_status = PlasmaError::OK;
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, PlasmaError::OK);
  ASSERT_REQUEST_FINISHED(queue, req_id2, PlasmaError::OK);

  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestOomTimerWithSpilling) {
  int spill_object_callback_ret = true;
  CreateRequestQueue queue(
      monitor_,
      /*oom_grace_period_s=*/oom_grace_period_s_,
      /*spill_object_callback=*/
      [&]() { return spill_object_callback_ret; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns_; });

  auto return_status = PlasmaError::OutOfMemory;
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    if (return_status == PlasmaError::OK) {
      result->data_size = 1234;
    }
    return return_status;
  };
  auto blocked_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue.AddRequest(ObjectID::Nil(), client, blocked_request, 1234);

  // Transient OOM should happen while spilling is in progress.
  for (int i = 0; i < 10; i++) {
    // Advance 0.1 seconds. OOM grace period is 1 second.
    current_time_ns_ += 1e8;
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
    ASSERT_EQ(num_global_gc_, i + 1);
  }

  // Now spilling is done.
  spill_object_callback_ret = false;

  // ObjectStoreFull errors should happen until the grace period.
  for (int i = 0; i < 10; i++) {
    // Advance 0.1 seconds. OOM grace period is 1 second.
    current_time_ns_ += 1e8;
    ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  }

  // Grace period is done. The first request should reply with OOM and the second
  // request should also be served.
  current_time_ns_ += oom_grace_period_s_ * 2e9;

  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, PlasmaError::OutOfDisk);
  ASSERT_REQUEST_FINISHED(queue, req_id2, PlasmaError::OK);

  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestTransientOomThenOom) {
  bool is_spilling_possible = true;
  CreateRequestQueue queue(
      monitor_,
      /*oom_grace_period_s=*/oom_grace_period_s_,
      /*spill_object_callback=*/[&]() { return is_spilling_possible; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns_; });

  auto return_status = PlasmaError::OutOfMemory;
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    if (return_status == PlasmaError::OK) {
      result->data_size = 1234;
    }
    return return_status;
  };
  auto blocked_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);
  auto req_id2 = queue.AddRequest(ObjectID::Nil(), client, blocked_request, 1234);

  // Transient OOM should not use up any until grace period is done.
  for (int i = 0; i < 3; i++) {
    // Advance 0.1 seconds. OOM grace period is 1 second.
    current_time_ns_ += 1e8;
    ASSERT_TRUE(queue.ProcessRequests().IsTransientObjectStoreFull());
    ASSERT_REQUEST_UNFINISHED(queue, req_id1);
    ASSERT_REQUEST_UNFINISHED(queue, req_id2);
    ASSERT_EQ(num_global_gc_, i + 1);
  }

  // Now spilling is not possible. We should start raising OOM with retry.
  is_spilling_possible = false;
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  ASSERT_REQUEST_UNFINISHED(queue, req_id1);
  ASSERT_REQUEST_UNFINISHED(queue, req_id2);
  ASSERT_EQ(num_global_gc_, 5);

  // Grace period is done. The first request should reply with OOM and the second
  // request should also be served.
  current_time_ns_ += oom_grace_period_s_ * 2e9;
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, PlasmaError::OutOfDisk);
  ASSERT_REQUEST_FINISHED(queue, req_id2, PlasmaError::OK);
  ASSERT_EQ(num_global_gc_, 6);

  AssertNoLeaks();
}

TEST(CreateRequestQueueParameterTest, TestNoEvictIfFull) {
  int64_t current_time_ns = 0;
  ray::FileSystemMonitor monitor{{"/"}, 1};
  CreateRequestQueue queue(
      monitor,
      /*oom_grace_period_s=*/1,
      /*spill_object_callback=*/[&]() { return false; },
      /*on_global_gc=*/[&]() {},
      /*get_time=*/[&]() { return current_time_ns; });

  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    return PlasmaError::OutOfMemory;
  };

  auto client = std::make_shared<MockClient>();
  static_cast<void>(queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234));
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  current_time_ns += 1e8;
  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
}

TEST_F(CreateRequestQueueTest, TestClientDisconnected) {
  auto request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };

  // Client makes two requests. One is processed, the other is still in the
  // queue.
  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  ASSERT_TRUE(queue_.ProcessRequests().ok());
  auto req_id2 = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);

  // Another client makes a concurrent request.
  auto client2 = std::make_shared<MockClient>();
  auto req_id3 = queue_.AddRequest(ObjectID::Nil(), client2, request, 1234);

  // Client disconnects.
  queue_.RemoveDisconnectedClientRequests(client);

  // Both requests should be cleaned up.
  ASSERT_REQUEST_FINISHED(queue_, req_id1, PlasmaError::UnexpectedError);
  ASSERT_REQUEST_FINISHED(queue_, req_id2, PlasmaError::UnexpectedError);
  // Other client's request was fulfilled.
  ASSERT_TRUE(queue_.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue_, req_id3, PlasmaError::OK);
  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestTryRequestImmediately) {
  auto request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    result->data_size = 1234;
    return PlasmaError::OK;
  };
  auto client = std::make_shared<MockClient>();

  // Queue is empty, request can be fulfilled.
  auto result = queue_.TryRequestImmediately(ObjectID::Nil(), client, request, 1234);
  ASSERT_EQ(result.first.data_size, 1234);
  ASSERT_EQ(result.second, PlasmaError::OK);

  // Request would block.
  auto req_id = queue_.AddRequest(ObjectID::Nil(), client, request, 1234);
  result = queue_.TryRequestImmediately(ObjectID::Nil(), client, request, 1234);
  result = queue_.TryRequestImmediately(ObjectID::Nil(), client, request, 1234);
  ASSERT_EQ(result.first.data_size, 1234);
  ASSERT_TRUE(queue_.ProcessRequests().ok());

  // Queue is empty again, request can be fulfilled.
  result = queue_.TryRequestImmediately(ObjectID::Nil(), client, request, 1234);
  ASSERT_EQ(result.first.data_size, 1234);
  ASSERT_EQ(result.second, PlasmaError::OK);

  // Queue is empty, but request would block. Check that we do not attempt to
  // retry the request.
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    return PlasmaError::OutOfMemory;
  };
  result = queue_.TryRequestImmediately(ObjectID::Nil(), client, oom_request, 1234);
  ASSERT_EQ(result.first.data_size, 0);
  ASSERT_EQ(result.second, PlasmaError::OutOfMemory);

  ASSERT_REQUEST_FINISHED(queue_, req_id, PlasmaError::OK);
  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestOOMAndOOD) {
  ray::FileSystemMonitor out_of_disk_monitor{{"/"}, /*capacity_threshold*/ 0};
  bool is_spilling_possible = true;
  CreateRequestQueue queue(
      out_of_disk_monitor,
      /*oom_grace_period_s=*/oom_grace_period_s_,
      /*spill_object_callback=*/[&]() { return is_spilling_possible; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns_; });

  auto return_status = PlasmaError::OutOfMemory;
  auto oom_request = [&](bool fallback, PlasmaObject *result, bool *spill_requested) {
    if (return_status == PlasmaError::OK) {
      result->data_size = 1234;
    }
    return return_status;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);

  // Should fail with out of disk.
  ASSERT_TRUE(queue.ProcessRequests().IsOutOfDisk());
  ASSERT_REQUEST_FINISHED(queue, req_id1, PlasmaError::OutOfDisk);
  AssertNoLeaks();
}

TEST_F(CreateRequestQueueTest, TestFallbackAllocationFailled) {
  ray::FileSystemMonitor out_of_disk_monitor{{"/tmp"}, /*capacity_threshold*/ 1};
  bool is_spilling_possible = false;
  CreateRequestQueue queue(
      out_of_disk_monitor,
      /*oom_grace_period_s=*/oom_grace_period_s_,
      /*spill_object_callback=*/[&]() { return is_spilling_possible; },
      /*on_global_gc=*/[&]() { num_global_gc_++; },
      /*get_time=*/[&]() { return current_time_ns_; });

  auto return_status = PlasmaError::OutOfMemory;
  size_t num_calls = 0;
  auto oom_request =
      [&](bool fallback, PlasmaObject *result, bool *spill_requested) -> PlasmaError {
    if (num_calls <= 1) {
      EXPECT_FALSE(fallback);
    } else {
      EXPECT_TRUE(fallback);
    }
    num_calls++;
    return return_status;
  };

  auto client = std::make_shared<MockClient>();
  auto req_id1 = queue.AddRequest(ObjectID::Nil(), client, oom_request, 1234);

  ASSERT_TRUE(queue.ProcessRequests().IsObjectStoreFull());
  current_time_ns_ += oom_grace_period_s_ * 2e9;
  ASSERT_TRUE(queue.ProcessRequests().ok());
  ASSERT_REQUEST_FINISHED(queue, req_id1, PlasmaError::OutOfDisk);
  ASSERT_EQ(num_calls, 3);
  AssertNoLeaks();
}

}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
