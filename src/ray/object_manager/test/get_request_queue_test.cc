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

/// Test1: AddRequest with object satisfied
/// Test2: AddRequest with object unsatisfied
/// Test3: Test Object sealed
/// Test4: Test Remove

namespace plasma {

class MockClient : public ClientInterface {
 public:
  MOCK_METHOD1(SendFd, Status(MEMFD_TYPE));
  MOCK_METHOD0(GetObjectIDs, std::unordered_set<ray::ObjectID> &());
};

class MockObjectLifecycleManager : public IObjectLifecycleManager {
 public:
  MOCK_METHOD3(CreateObject,
               std::pair<const LocalObject *, flatbuf::PlasmaError>(
                   const ray::ObjectInfo &object_info,
                   plasma::flatbuf::ObjectSource source, bool fallback_allocator));
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
    thread_ = std::thread([this] { io_context_.run(); });
  }

  void TearDown() override {
    io_context_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 protected:
  instrumented_io_context io_context_;
  boost::asio::io_service::work io_work_;
  std::thread thread_;
};

TEST_F(GetRequestQueueTest, TestAddRequest) {
  MockObjectLifecycleManager object_lifecycle_manager;
  GetRequestQueue get_request_queue(io_context_, object_lifecycle_manager);
  auto client = std::make_shared<MockClient>();
  ObjectID object_id = ObjectID::FromRandom();
  std::vector<ObjectID> object_ids{object_id};
  LocalObject object1{Allocation()};
  object1.object_info.data_size = 10;
  object1.object_info.metadata_size = 0;
  /// Test object has been satisfied.
  {
    /// Mock the object already sealed.
    object1.state = ObjectState::PLASMA_SEALED;
    EXPECT_CALL(object_lifecycle_manager, GetObject(_))
        .Times(1)
        .WillOnce(Return(&object1));
    bool satisfied = false;
    get_request_queue.AddRequest(
        client, object_ids, 1000, false,
        [&](const ObjectID &object_id, const auto &request) {},
        [&](const std::shared_ptr<GetRequest> &get_req) { satisfied = true; });
    EXPECT_TRUE(satisfied);
  }

  /// Test object not satisfied, time out.
  {
    object1.state = ObjectState::PLASMA_CREATED;
    EXPECT_CALL(object_lifecycle_manager, GetObject(_))
        .Times(1)
        .WillOnce(Return(&object1));
    std::promise<bool> promise;
    get_request_queue.AddRequest(
        client, object_ids, 1000, false,
        [&](const ObjectID &object_id, const auto &request) {},
        [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
    promise.get_future().get();
  }

  get_request_queue.RemoveGetRequestsForClient(client);
  EXPECT_FALSE(get_request_queue.IsGetRequestExist(object_id));

  /// Test object not satisfied, then sealed.
  {
    object1.state = ObjectState::PLASMA_CREATED;
    EXPECT_CALL(object_lifecycle_manager, GetObject(_))
        .Times(2)
        .WillRepeatedly(Return(&object1));
    std::promise<bool> promise;
    get_request_queue.AddRequest(
        client, object_ids, /*timeout_ms*/ -1, false,
        [&](const ObjectID &object_id, const auto &request) {},
        [&](const std::shared_ptr<GetRequest> &get_req) { promise.set_value(true); });
    object1.state = ObjectState::PLASMA_SEALED;
    get_request_queue.ObjectSealed(object_id);
    promise.get_future().get();
  }
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
