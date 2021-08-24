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
    manager_ = std::make_shared<MockObjectLifecycleManager>();
    get_request_queue_ = std::make_shared<GetRequestQueue>(io_context_, manager_);
  }

  void TearDown() {
    io_context_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 protected:
  instrumented_io_context io_context_;
  boost::asio::io_service::work io_work_;
  std::thread thread_;
  std::shared_ptr<GetRequestQueue> get_request_queue_;
  std::shared_ptr<MockObjectLifecycleManager> manager_;
};

TEST_F(GetRequestQueueTest, TestAddRequest) {
  auto client = std::make_shared<MockClient>();
  std::vector<ObjectID> object_ids{ObjectID::FromRandom()};
  LocalObject object1{Allocation()};
  object1.object_info.data_size = 10;
  object1.object_info.metadata_size = 0;
  /// Mock the object already sealed.
  object1.state = ObjectState::PLASMA_SEALED;
  EXPECT_CALL(*manager_, GetObject(_)).Times(1).WillOnce(Return(&object1));
  bool satisfied = false;
  get_request_queue_->AddRequest(client, object_ids, 1000, false,
                                 [&](const std::shared_ptr<GetRequest> &get_req) {
                                   RAY_LOG(INFO) << "AddRequest callback";
                                   satisfied = true;
                                 });
  EXPECT_TRUE(satisfied);
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
