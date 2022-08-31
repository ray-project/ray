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

#include "ray/core_worker/object_barrier.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
namespace core {

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  void BatchAssignObjectOwner(
      const rpc::BatchAssignObjectOwnerRequest &request,
      const rpc::ClientCallback<rpc::BatchAssignObjectOwnerReply> &callback) {
    callback(Status::OK(), rpc::BatchAssignObjectOwnerReply());
  }
};

class ObjectBarrierTest : public ::testing::Test {
 public:
  ObjectBarrierTest() {}

  void SetUp() override {
    client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
        [&](const rpc::Address &) { return std::make_shared<MockWorkerClient>(); });
    object_barrier = std::make_unique<ObjectBarrier>(io_service, std::move(client_pool));
    io_thread = std::thread([this]() {
      boost::asio::io_service::work work(io_service);
      io_service.run();
    });
  }
  void TearDown() override {
    io_service.stop();
    io_thread.join();
  }

  std::thread io_thread;
  instrumented_io_context io_service;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool;
  std::shared_ptr<ObjectBarrier> object_barrier;
};

TEST_F(ObjectBarrierTest, TestAddAssignOwnerRequest) {
  int global_object_number = 1;
  auto task_id = TaskID::FromRandom(JobID::FromInt(1));
  auto object_id = ObjectID::FromIndex(task_id, global_object_number++);

  auto owner_address = rpc::Address();
  owner_address.set_raylet_id(NodeID::FromRandom().Binary());
  owner_address.set_ip_address("127.0.0.1");
  owner_address.set_port(8090);
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());

  auto current_address = rpc::Address();
  current_address.set_raylet_id(NodeID::FromRandom().Binary());
  current_address.set_ip_address("127.0.0.2");
  current_address.set_port(8090);
  current_address.set_worker_id(WorkerID::FromRandom().Binary());

  bool flag = false;
  auto cb = [&flag](const Status &status) { flag = true; };

  io_service.post(
      [this, object_id, &owner_address, &current_address, cb = std::move(cb)]() {
        object_barrier->AddAssignOwnerRequest(
            object_id, owner_address, current_address, {}, "", 1, std::move(cb));
      },
      "ObjectBarrierTest.TestAddAssignOwnerRequest");
  ASSERT_FALSE(flag);
  std::this_thread::sleep_for(std::chrono::milliseconds(
      2 * RayConfig::instance().delay_send_assign_owner_request_ms()));
  ASSERT_TRUE(flag);

  absl::flat_hash_set<ObjectID> set;
  int num_cb = 0;
  for (int i = 0; i < RayConfig::instance().assign_owner_batch_size(); i++) {
    auto object_id = ObjectID::FromIndex(task_id, global_object_number++);
    io_service.post(
        [this, object_id, &owner_address, &current_address, &num_cb]() {
          object_barrier->AddAssignOwnerRequest(
              object_id,
              owner_address,
              current_address,
              {},
              "",
              1,
              [&num_cb](const Status &status) { num_cb++; });
        },
        "ObjectBarrierTest.TestAddAssignOwnerRequest");
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  ASSERT_TRUE(RayConfig::instance().assign_owner_batch_size() == num_cb);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}