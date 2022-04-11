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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ray;
using namespace testing;

namespace plasma {

class MockClient : public ClientInterface {
 public:
  MOCK_METHOD1(SendFd, Status(MEMFD_TYPE));
  MOCK_METHOD0(GetObjectIDs, const std::unordered_set<ray::ObjectID> &());
  MOCK_METHOD1(MarkObjectAsUsed, void(const ObjectID &object_id));
  MOCK_METHOD1(MarkObjectAsUnused, void(const ObjectID &object_id));
};

class MockObjectLifecycleManager : public IObjectLifecycleManager {
 public:
  MOCK_METHOD3(CreateObject,
               std::pair<const LocalObject *, flatbuf::PlasmaError>(
                   const ray::ObjectInfo &object_info,
                   plasma::flatbuf::ObjectSource source,
                   bool fallback_allocator));
  MOCK_CONST_METHOD1(GetObject, const LocalObject *(const ObjectID &object_id));
  MOCK_METHOD1(SealObject, const LocalObject *(const ObjectID &object_id));
  MOCK_METHOD1(AbortObject, flatbuf::PlasmaError(const ObjectID &object_id));
  MOCK_METHOD1(DeleteObject, flatbuf::PlasmaError(const ObjectID &object_id));
  MOCK_METHOD1(AddReference, bool(const ObjectID &object_id));
  MOCK_METHOD1(RemoveReference, bool(const ObjectID &object_id));
};

struct GetRequestQueueTest : public Test {
 public:
  GetRequestQueueTest() : io_work_(io_context_) {}
  void SetUp() override {
    Test::SetUp();
    object_id1 = ObjectID::FromRandom();
    object_id2 = ObjectID::FromRandom();
    object1.object_info.data_size = 10;
    object1.object_info.metadata_size = 0;
    object2.object_info.data_size = 10;
    object2.object_info.metadata_size = 0;
  }

  void TearDown() override { io_context_.stop(); }

 protected:
  void MarkObject(LocalObject &object, ObjectState state) { object.state = state; }

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
  boost::asio::io_service::work io_work_;
  std::thread thread_;
  LocalObject object1{Allocation()};
  LocalObject object2{Allocation()};
  ObjectID object_id1;
  ObjectID object_id2;
};

TEST_F(GetRequestQueueTest, TestObjectSealed) {
  bool satisfied = false;
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { satisfied = true; });
  auto client = std::make_shared<MockClient>();

  /// Test object has been satisfied.
  std::vector<ObjectID> object_ids{object_id1};
  /// Mock the object already sealed.
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_)).Times(1).WillOnce(Return(&object1));
  get_request_queue.AddRequest(client, object_ids, 1000, false);
  EXPECT_TRUE(satisfied);

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestObjectTimeout) {
  std::promise<bool> promise;
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
  auto client = std::make_shared<MockClient>();

  /// Test object not satisfied, time out.
  std::vector<ObjectID> object_ids{object_id1};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_)).Times(1).WillOnce(Return(&object1));
  get_request_queue.AddRequest(client, object_ids, 1000, false);
  /// This trigger timeout
  io_context_.run_one();
  promise.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestObjectNotSealed) {
  std::promise<bool> promise;
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
  auto client = std::make_shared<MockClient>();

  /// Test object not satisfied, then sealed.
  std::vector<ObjectID> object_ids{object_id1};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_))
      .Times(2)
      .WillRepeatedly(Return(&object1));
  get_request_queue.AddRequest(client, object_ids, /*timeout_ms*/ -1, false);
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  get_request_queue.MarkObjectSealed(object_id1);
  promise.get_future().get();

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestMultipleObjects) {
  std::promise<bool> promise1, promise2, promise3;
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {
        if (object_id == object_id1) {
          promise1.set_value(true);
        }
        if (object_id == object_id2) {
          promise2.set_value(true);
        }
      },
      [&](const std::shared_ptr<GetRequest> &get_req) { promise3.set_value(true); });
  auto client = std::make_shared<MockClient>();

  /// Test get request of mulitiple objects, one sealed, one timed out.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_SEALED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(Eq(object_id1)))
      .WillRepeatedly(Return(&object1));
  EXPECT_CALL(object_lifecycle_manager, GetObject(Eq(object_id2)))
      .WillRepeatedly(Return(&object2));
  get_request_queue.AddRequest(client, object_ids, 1000, false);
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
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<MockClient>();

  /// Test get request of duplicated objects.
  std::vector<ObjectID> object_ids{object_id1, object_id2, object_id1};
  /// Set state to PLASMA_CREATED, so we can check them using IsGetRequestExist.
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_))
      .Times(2)
      .WillOnce(Return(&object1))
      .WillOnce(Return(&object2));
  get_request_queue.AddRequest(client, object_ids, 1000, false);
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));
  EXPECT_EQ(1, GetRequestCount(get_request_queue, object_id1));
  EXPECT_EQ(1, GetRequestCount(get_request_queue, object_id2));
}

TEST_F(GetRequestQueueTest, TestRemoveAll) {
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<MockClient>();

  /// Test get request two not-sealed objects, remove all requests for this client.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_))
      .Times(2)
      .WillOnce(Return(&object1))
      .WillOnce(Return(&object2));
  get_request_queue.AddRequest(client, object_ids, 1000, false);

  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_TRUE(IsGetRequestExist(get_request_queue, object_id2));

  get_request_queue.RemoveGetRequestsForClient(client);
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id1));
  EXPECT_FALSE(IsGetRequestExist(get_request_queue, object_id2));

  AssertNoLeak(get_request_queue);
}

TEST_F(GetRequestQueueTest, TestRemoveTwice) {
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(
      io_context_,
      object_lifecycle_manager,
      [&](const ObjectID &object_id, const auto &request) {},
      [&](const std::shared_ptr<GetRequest> &get_req) {});
  auto client = std::make_shared<MockClient>();

  /// Test get request two not-sealed objects, remove all requests for this client.
  std::vector<ObjectID> object_ids{object_id1, object_id2};
  MarkObject(object1, ObjectState::PLASMA_CREATED);
  MarkObject(object2, ObjectState::PLASMA_CREATED);
  EXPECT_CALL(object_lifecycle_manager, GetObject(_))
      .Times(2)
      .WillOnce(Return(&object1))
      .WillOnce(Return(&object2));
  get_request_queue.AddRequest(client, object_ids, 1000, false);

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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
