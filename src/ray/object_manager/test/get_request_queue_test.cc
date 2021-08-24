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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

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

class MockEvictionPolicy : public IEvictionPolicy {
 public:
  MOCK_METHOD2(ObjectCreated, void(const ObjectID &, bool));
  MOCK_METHOD2(RequireSpace, int64_t(int64_t, std::vector<ObjectID> *));
  MOCK_METHOD1(BeginObjectAccess, void(const ObjectID &));
  MOCK_METHOD1(EndObjectAccess, void(const ObjectID &));
  MOCK_METHOD2(ChooseObjectsToEvict, int64_t(int64_t, std::vector<ObjectID> *));
  MOCK_METHOD1(RemoveObject, void(const ObjectID &));
  MOCK_CONST_METHOD0(DebugString, std::string());
};

class MockObjectStore : public IObjectStore {
 public:
  MOCK_METHOD3(CreateObject, const LocalObject *(const ray::ObjectInfo &,
                                                 plasma::flatbuf::ObjectSource, bool));
  MOCK_CONST_METHOD1(GetObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(SealObject, const LocalObject *(const ObjectID &));
  MOCK_METHOD1(DeleteObject, bool(const ObjectID &));
  MOCK_CONST_METHOD0(GetNumBytesCreatedTotal, int64_t());
  MOCK_CONST_METHOD0(GetNumBytesUnsealed, int64_t());
  MOCK_CONST_METHOD0(GetNumObjectsUnsealed, int64_t());
  MOCK_CONST_METHOD1(GetDebugDump, void(std::stringstream &buffer));
};

struct GetRequestQueueTest : public Test {
 public:
  GetRequestQueueTest() : io_work_(io_context_) {}
  void SetUp() override {
    Test::SetUp();
    thread_ = std::thread([this] {
      io_context_.run();
    });
    
    auto eviction_policy = std::make_unique<MockEvictionPolicy>();
    auto object_store = std::make_unique<MockObjectStore>();
    manager_ = std::make_unique<ObjectLifecycleManager>(
	    ObjectLifecycleManager(std::move(object_store), std::move(eviction_policy),
                               [this](auto &id) {}));
    get_request_queue_ = std::make_shared<GetRequestQueue>(io_context_, *manager_);
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
  std::unique_ptr<ObjectLifecycleManager> manager_;
};

TEST_F(GetRequestQueueTest, TestAddRequest) {
  auto client = std::make_shared<MockClient>();
  std::vector<ObjectID> object_ids{ObjectID::FromRandom()};
  EXPECT_CALL(manager_, GetObject()).Times(1).WillOnce();
  get_request_queue_->AddRequest(client, object_ids, 1000, false, [](const std::shared_ptr<GetRequest> &get_req) {
    RAY_LOG(INFO) << "AddRequest callback";
  });
}
} // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
