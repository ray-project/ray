// Copyright 2023 The Ray Authors.
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

#include "ray/rpc/worker/core_worker_client_pool.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace rpc {
class MockCoreWorkerClient : public CoreWorkerClientInterface {
 public:
  explicit MockCoreWorkerClient(
      std::function<void()> unavailable_timeout_callback = nullptr)
      : unavailable_timeout_callback_(std::move(unavailable_timeout_callback)) {}

  bool IsIdleAfterRPCs() const override { return is_idle_after_rpcs; }

  bool is_idle_after_rpcs = false;
  std::function<void()> unavailable_timeout_callback_;
};

class CoreWorkerClientPoolTest : public ::testing::Test {
 public:
  static rpc::Address CreateRandomAddress(const std::string &addr) {
    rpc::Address address;
    address.set_ip_address(addr);
    address.set_raylet_id(NodeID::FromRandom().Binary());
    address.set_worker_id(WorkerID::FromRandom().Binary());
    return address;
  }
};

TEST_F(CoreWorkerClientPoolTest, TestGC) {
  // Test to make sure idle clients are removed eventually.

  CoreWorkerClientPool client_pool(
      [&](const rpc::Address &addr) { return std::make_shared<MockCoreWorkerClient>(); });

  rpc::Address address1 = CreateRandomAddress("1");
  rpc::Address address2 = CreateRandomAddress("2");
  auto client1 = client_pool.GetOrConnect(address1);
  ASSERT_EQ(client_pool.Size(), 1);
  auto client2 = client_pool.GetOrConnect(address2);
  ASSERT_EQ(client_pool.Size(), 2);
  client_pool.Disconnect(WorkerID::FromBinary(address2.worker_id()));
  ASSERT_EQ(client_pool.Size(), 1);
  ASSERT_EQ(client1.get(), client_pool.GetOrConnect(address1).get());
  ASSERT_EQ(client_pool.Size(), 1);
  client2 = client_pool.GetOrConnect(address2);
  ASSERT_EQ(client_pool.Size(), 2);
  dynamic_cast<MockCoreWorkerClient *>(client1.get())->is_idle_after_rpcs = true;
  // Client 1 will be removed since it's idle.
  ASSERT_EQ(client2.get(), client_pool.GetOrConnect(address2).get());
  ASSERT_EQ(client_pool.Size(), 1);
}

class MockGcsClientNodeAccessor : public gcs::NodeInfoAccessor {
 public:
  MockGcsClientNodeAccessor() : gcs::NodeInfoAccessor(nullptr) {}

  bool IsSubscribedToNodeChange() const override { return true; }

  const rpc::GcsNodeInfo *Get(const NodeID &node_id,
                              bool filter_dead_nodes) const override {
    return nullptr;
  }
};

class MockGcsClient : public gcs::GcsClient {
 public:
  MockGcsClient() {
    this->node_accessor_ = std::make_unique<MockGcsClientNodeAccessor>();
  }
};

TEST_F(CoreWorkerClientPoolTest, TestGetDefaultUnavailableTimeoutCallbackNodeDead) {
  // This will return dead node.
  std::unique_ptr<gcs::GcsClient> gcs_client = std::make_unique<MockGcsClient>();
  instrumented_io_context io_context;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager =
      std::make_unique<rpc::ClientCallManager>(io_context, false);

  std::unique_ptr<CoreWorkerClientPool> client_pool;
  client_pool = std::make_unique<CoreWorkerClientPool>([&](const rpc::Address &addr) {
    return std::make_shared<MockCoreWorkerClient>(
        CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
            gcs_client.get(), client_pool.get(), client_call_manager.get(), addr));
  });

  auto core_worker_client = client_pool->GetOrConnect(CreateRandomAddress("1"));
  ASSERT_EQ(client_pool->Size(), 1);
  dynamic_cast<MockCoreWorkerClient *>(core_worker_client.get())
      ->unavailable_timeout_callback_();
  // Expect disconnect because node should be dead.
  ASSERT_EQ(client_pool->Size(), 0);
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
