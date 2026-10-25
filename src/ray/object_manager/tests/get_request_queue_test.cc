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

#include "ray/object_manager/plasma/get_request_queue.h"

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"

using ray::ObjectID;
using ray::ObjectInfo;
using ray::Status;
using testing::Test;

namespace plasma {

class FakeClient : public ClientInterface {
 public:
  Status SendFd(MEMFD_TYPE fd) override { return Status::OK(); }
  const std::unordered_set<ObjectID> &GetObjectIDs() override { return object_ids_; }
  void MarkObjectAsUsed(const ObjectID &object_id,
                        std::optional<MEMFD_TYPE> fallback_allocated_fd) override {}
  bool MarkObjectAsUnused(const ObjectID &object_id) override { return true; }

  std::unordered_set<ObjectID> object_ids_;
};

// Hand-written fake for IObjectLifecycleManager. GetObject returns the object
// registered in `objects` for a given id (or nullptr), and records how many
// times it was called so tests can assert on interaction counts.
class FakeObjectLifecycleManager : public IObjectLifecycleManager {
 public:
  std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ObjectInfo &object_info,
      plasma::flatbuf::ObjectSource source,
      bool fallback_allocator) override {
    return {nullptr, flatbuf::PlasmaError::OK};
  }
  const LocalObject *GetObject(const ObjectID &object_id) const override {
    get_object_call_count++;
    auto it = objects.find(object_id);
    return it == objects.end() ? nullptr : it->second;
  }
  const LocalObject *SealObject(const ObjectID &object_id) override { return nullptr; }
  flatbuf::PlasmaError AbortObject(const ObjectID &object_id) override {
    return flatbuf::PlasmaError::OK;
  }
  flatbuf::PlasmaError DeleteObject(const ObjectID &object_id) override {
    return flatbuf::PlasmaError::OK;
  }
  bool AddReference(const ObjectID &object_id) override { return true; }
  bool RemoveReference(const ObjectID &object_id) override { return true; }

  absl::flat_hash_map<ObjectID, const LocalObject *> objects;
  mutable int get_object_call_count = 0;
};

struct GetRequestQueueTest : public Test {
 public:
  GetRequestQueueTest() : io_work_(io_context_.get_executor()) {}
  void SetUp() override {
    Test::SetUp();
    object_id1 = ObjectID::FromRandom();
    object_id2 = ObjectID::FromRandom();
    object1.object_info_.data_size = 10;
    object1.object_info_.metadata_size = 0;
    object2.object_info_.data_size = 10;
    object2.object_info_.metadata_size = 0;
  }

  void TearDown() override { io_context_.stop(); }

 protected:
  void MarkObject(LocalObject &object, ObjectState state) { object.state_ = state; }

  void MarkObjectFallbackAllocated(LocalObject &object,
                                   bool fallback_allocated,
                                   MEMFD_TYPE fd) {
    object.allocation_.fallback_allocated_ = fallback_allocated;
    object.allocation_.fd_ = fd;
  }

  bool IsGetRequestExist(GetRequestQueue &queue, const ObjectID &object_id) {
    return queue.IsGetRequestExist(object_id);
  }

  int64_t GetRequestCount(GetRequestQueue &queue, const ObjectID &object_id) {
    return queue.GetRequestCount(object_id);
  }

  std::vector<std::shared_ptr<GetRequest>> GetRequests(GetRequestQueue &queue,
                                                       const ObjectID &object_id) {
    auto it = queue.object_get_requests_.find(object_id);
    if (it == queue.object_get_requests_.end()) {
      return {};
    }
    return it->second;
  }

  void RemoveGetRequest(GetRequestQueue &queue,
                        const std::shared_ptr<GetRequest> &get_request) {
    queue.RemoveGetRequest(get_request);
  }

  void AssertNoLeak(GetRequestQueue &queue) {
    EXPECT_FALSE(IsGetRequestExist(queue, object_id1));
    EXPECT_FALSE(IsGetRequestExist(queue, object_id2));
  }

 protected:
  instrumented_io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  std::thread thread_;
  LocalObject object1{Allocation()};
  LocalObject object2{Allocation()};
  ObjectID object_id1;
  ObjectID object_id2;
};

TEST_F(GetRequestQueueTest, TestObjectSealed) {
  bool satisfied = false;
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { satisfied = true; });
  auto client = std::make_shared<FakeClient>();

  /// Test object has been satisfied.
  std::vector<ObjectID> object_ids{object_id1};
  /// Mark the object as already sealed.
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  get_request_queue.AddRequest(client, object_ids, 1000);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 1);
  EXPECT_TRUE(satisfied);

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestObjectTimeout) {
  std::promise<bool> promise;
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
  auto client = std::make_shared<FakeClient>();

  /// Test object not satisfied, time out.
  std::vector<ObjectID> object_ids{object_id1};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  get_request_queue.AddRequest(client, object_ids, 1000);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 1);
  /// This trigger timeout
  io_context_.run_one();
  promise.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestObjectNotSealed) {
  std::promise<bool> promise;
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
  auto client = std::make_shared<FakeClient>();

  /// Test object not satisfied, then sealed.
  std::vector<ObjectID> object_ids{object_id1};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  get_request_queue.AddRequest(client, object_ids, /*timeout_ms*/ -1);
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  get_request_queue.MarkObjectSealed(object_id1);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 2);
  promise.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestMultipleObjects) {
  std::promise<bool> promise1, promise2, promise3;
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {
        if (object_id == object_id1) {
          promise1.set_value(true);
        }
        if (object_id == object_id2) {
          promise2.set_value(true);
        }
      },
      [&](const std::shared_ptr<GetRequest> &get_req) { promise3.set_value(true); });
  auto client = std::make_shared<FakeClient>();

  /// Test get request of multiple objects, one sealed, one timed out.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  object_lifecycle_manager.objects[object_id2] = &object2;
  get_request_queue.AddRequest(client, object_ids, 1000);
  promise1.get_future().get();
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));
  MarkObject(object2, ObjectState::PLASMA_SEALED);
  get_request_queue.MarkObjectSealed(object_id2);
  io_context_.run_one();
  promise2.get_future().get();
  promise3.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestFallbackAllocatedFdArePassed) {
  std::promise<bool> promise1, promise2, promise3;
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {
        if (object_id == object_id1) {
          EXPECT_FALSE(fallback_allocated_fd.has_value());
          promise1.set_value(true);
        }
        if (object_id == object_id2) {
          EXPECT_TRUE(fallback_allocated_fd.has_value());
          promise2.set_value(true);
        }
      },
      [&](const std::shared_ptr<GetRequest> &get_req) { promise3.set_value(true); });
  auto client = std::make_shared<FakeClient>();

  /// Test get request of multiple objects, one sealed, one timed out.
  /// object1 is in main memory, object2 is fallback-allocated.

  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  MEMFD_TYPE fd{INT2FD(101), 42};
  MarkObjectFallbackAllocated(object2, true, fd);

  object_lifecycle_manager.objects[object_id1] = &object1;
  object_lifecycle_manager.objects[object_id2] = &object2;
  get_request_queue.AddRequest(client, object_ids, 1000);
  promise1.get_future().get();
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));
  MarkObject(object2, ObjectState::PLASMA_SEALED);
  get_request_queue.MarkObjectSealed(object_id2);
  io_context_.run_one();
  promise2.get_future().get();
  promise3.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestDuplicateObjects) {
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<FakeClient>();

  /// Test get request of duplicated objects.
  std::vector<ObjectID> object_ids{object_id1, object_id2, object_id1};
  /// Set state to PLASMA_CREATED, so we can check them using IsGetRequestExist.
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  object_lifecycle_manager.objects[object_id2] = &object2;
  get_request_queue.AddRequest(client, object_ids, 1000);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 2);
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));
  EXPECT_EQ(1, GetRequestCount(get_request_queue, object_id1));
  EXPECT_EQ(1, GetRequestCount(get_request_queue, object_id2));
}

TEST_F(GetRequestQueueTest, TestRemoveAll) {
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<FakeClient>();

  /// Test get request two not-sealed objects, remove all requests for this client.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  object_lifecycle_manager.objects[object_id2] = &object2;
  get_request_queue.AddRequest(client, object_ids, 1000);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 2);

  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));

  get_request_queue.RemoveGetRequestsForClient(client);
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id2));

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestRemoveTwice) {
  FakeObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id,
          std::optional<MEMFD_TYPE> fallback_allocated_fd,
          const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<FakeClient>();

  /// Test get request two not-sealed objects, remove all requests for this client.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  object_lifecycle_manager.objects[object_id1] = &object1;
  object_lifecycle_manager.objects[object_id2] = &object2;
  get_request_queue.AddRequest(client, object_ids, 1000);
  EXPECT_EQ(object_lifecycle_manager.get_object_call_count, 2);

  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));

  auto dangling_get_request = GetRequests(get_request_queue, object_id1).at(0);

  get_request_queue.RemoveGetRequestsForClient(client);
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id2));

  AssertNoLeak(get_request_queue);

  ASSERT_NO_THROW(RemoveGetRequest(get_request_queue, dangling_get_request));
}
}  // namespace plasma
