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
#include <vector>

#include "gmock/gmock.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {
namespace rpc {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class MockCoreWorkerClient : public CoreWorkerClientInterface {
 public:
  explicit MockCoreWorkerClient(
      std::function<void()> unavailable_timeout_callback = nullptr)
      : unavailable_timeout_callback_(std::move(unavailable_timeout_callback)) {}

  bool IsIdleAfterRPCs() const override { return is_idle_after_rpcs; }

  bool is_idle_after_rpcs = false;
  std::function<void()> unavailable_timeout_callback_;
};

namespace {

rpc::Address CreateRandomAddress(const std::string &addr) {
  rpc::Address address;
  address.set_ip_address(addr);
  address.set_raylet_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  return address;
}

}  // namespace

TEST(CoreWorkerClientPoolTest, TestGC) {
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
  explicit MockGcsClientNodeAccessor(bool is_subscribed_to_node_change)
      : gcs::NodeInfoAccessor(nullptr),
        is_subscribed_to_node_change_(is_subscribed_to_node_change) {}

  bool IsSubscribedToNodeChange() const override { return is_subscribed_to_node_change_; }

  MOCK_METHOD(const rpc::GcsNodeInfo *, Get, (const NodeID &, bool), (const, override));

  MOCK_METHOD(void,
              AsyncGetAll,
              (const gcs::MultiItemCallback<rpc::GcsNodeInfo> &,
               int64_t,
               std::optional<NodeID>),
              (override));

 private:
  bool is_subscribed_to_node_change_;
};

class MockGcsClient : public gcs::GcsClient {
 public:
  explicit MockGcsClient(bool is_subscribed_to_node_change) {
    this->node_accessor_ =
        std::make_unique<MockGcsClientNodeAccessor>(is_subscribed_to_node_change);
  }

  MockGcsClientNodeAccessor &MockNodeAccessor() {
    return dynamic_cast<MockGcsClientNodeAccessor &>(*this->node_accessor_);
  }
};

class DefaultUnavailableTimeoutCallbackTest : public ::testing::TestWithParam<bool> {
 public:
  DefaultUnavailableTimeoutCallbackTest()
      : is_subscribed_to_node_change_(GetParam()),
        gcs_client_(is_subscribed_to_node_change_),
        raylet_client_(std::make_shared<MockRayletClientInterface>()),
        client_pool_(
            std::make_unique<CoreWorkerClientPool>([this](const rpc::Address &addr) {
              return std::make_shared<MockCoreWorkerClient>(
                  CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
                      &this->gcs_client_,
                      this->client_pool_.get(),
                      [this](const std::string &, int32_t) {
                        return this->raylet_client_;
                      },
                      addr));
            })) {}

  bool is_subscribed_to_node_change_;
  MockGcsClient gcs_client_;
  std::shared_ptr<MockRayletClientInterface> raylet_client_;
  std::unique_ptr<CoreWorkerClientPool> client_pool_;
};

TEST_P(DefaultUnavailableTimeoutCallbackTest, NodeDeath) {
  // Add 2 worker clients to the pool.
  // worker_client_1 unavailable calls:
  // 1. Node info hasn't been cached yet, but GCS knows it's alive.
  // 2. Node is alive and worker is alive.
  // 3. Node is dead according to cache + GCS, should disconnect.
  // worker_client_2 unavailable calls:
  // 1. Subscriber cache and GCS don't know about node. Means the node is dead and the GCS
  //    had to discard to keep its cache size in check, should disconnect.

  auto &mock_node_accessor = gcs_client_.MockNodeAccessor();
  auto invoke_with_node_info_vector = [](std::vector<rpc::GcsNodeInfo> node_info_vector) {
    return Invoke(
        [node_info_vector](const gcs::MultiItemCallback<rpc::GcsNodeInfo> &callback,
                           int64_t,
                           std::optional<NodeID>) {
          callback(Status::OK(), node_info_vector);
        });
  };

  auto worker_1_address = CreateRandomAddress("1");
  auto worker_2_address = CreateRandomAddress("2");
  auto worker_1_client = dynamic_cast<MockCoreWorkerClient *>(
      client_pool_->GetOrConnect(worker_1_address).get());
  ASSERT_EQ(client_pool_->Size(), 1);
  auto worker_2_client = dynamic_cast<MockCoreWorkerClient *>(
      client_pool_->GetOrConnect(worker_2_address).get());
  ASSERT_EQ(client_pool_->Size(), 2);

  auto worker_1_node_id = NodeID::FromBinary(worker_1_address.raylet_id());
  auto worker_2_node_id = NodeID::FromBinary(worker_2_address.raylet_id());

  rpc::GcsNodeInfo node_info_alive;
  node_info_alive.set_state(rpc::GcsNodeInfo::ALIVE);
  rpc::GcsNodeInfo node_info_dead;
  node_info_dead.set_state(rpc::GcsNodeInfo::DEAD);
  if (is_subscribed_to_node_change_) {
    EXPECT_CALL(mock_node_accessor, Get(worker_1_node_id, /*filter_dead_nodes=*/false))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(&node_info_alive))
        .WillOnce(Return(&node_info_dead));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAll(_, _, std::make_optional(worker_1_node_id)))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}));
    EXPECT_CALL(mock_node_accessor, Get(worker_2_node_id, /*filter_dead_nodes=*/false))
        .WillOnce(Return(nullptr));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAll(_, _, std::make_optional(worker_2_node_id)))
        .WillOnce(invoke_with_node_info_vector({}));
  } else {
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAll(_, _, std::make_optional(worker_1_node_id)))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}))
        .WillOnce(invoke_with_node_info_vector({node_info_dead}));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAll(_, _, std::make_optional(worker_2_node_id)))
        .WillOnce(invoke_with_node_info_vector({}));
  }

  // Worker is alive when node is alive.
  EXPECT_CALL(*raylet_client_, IsLocalWorkerDead(_, _))
      .Times(2)
      .WillRepeatedly(
          Invoke([](const WorkerID &,
                    const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
            rpc::IsLocalWorkerDeadReply reply;
            reply.set_is_dead(false);
            callback(Status::OK(), std::move(reply));
          }));

  worker_1_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 2);
  worker_1_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 2);
  worker_1_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 1);
  worker_2_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 0);
}

TEST_P(DefaultUnavailableTimeoutCallbackTest, WorkerDeath) {
  // Add the client to the pool.
  // 1st call - Node is alive and worker is alive.
  // 2nd call - Node is alive and worker is dead, client should be disconnected.

  auto core_worker_client = dynamic_cast<MockCoreWorkerClient *>(
      client_pool_->GetOrConnect(CreateRandomAddress("1")).get());
  ASSERT_EQ(client_pool_->Size(), 1);

  rpc::GcsNodeInfo node_info_alive;
  node_info_alive.set_state(rpc::GcsNodeInfo::ALIVE);
  if (is_subscribed_to_node_change_) {
    EXPECT_CALL(gcs_client_.MockNodeAccessor(), Get(_, /*filter_dead_nodes=*/false))
        .Times(2)
        .WillRepeatedly(Return(&node_info_alive));
  } else {
    EXPECT_CALL(gcs_client_.MockNodeAccessor(), AsyncGetAll(_, _, _))
        .Times(2)
        .WillRepeatedly(Invoke(
            [&](const gcs::MultiItemCallback<rpc::GcsNodeInfo> &callback,
                int64_t,
                std::optional<NodeID>) { callback(Status::OK(), {node_info_alive}); }));
  }

  EXPECT_CALL(*raylet_client_, IsLocalWorkerDead(_, _))
      .WillOnce(
          Invoke([](const WorkerID &,
                    const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
            rpc::IsLocalWorkerDeadReply reply;
            reply.set_is_dead(false);
            callback(Status::OK(), std::move(reply));
          }))
      .WillOnce(
          Invoke([](const WorkerID &,
                    const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
            rpc::IsLocalWorkerDeadReply reply;
            reply.set_is_dead(true);
            callback(Status::OK(), std::move(reply));
          }));

  // Disconnects the second time.
  core_worker_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 1);
  core_worker_client->unavailable_timeout_callback_();
  ASSERT_EQ(client_pool_->Size(), 0);
}

INSTANTIATE_TEST_SUITE_P(IsSubscribedToNodeChange,
                         DefaultUnavailableTimeoutCallbackTest,
                         ::testing::Values(true, false));

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
