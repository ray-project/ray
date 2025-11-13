// Copyright 2025 The Ray Authors.
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

#include "ray/raylet_rpc_client/raylet_client_pool.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace rpc {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class MockRayletClient : public FakeRayletClient {
 public:
  explicit MockRayletClient(std::function<void()> unavailable_timeout_callback = nullptr)
      : unavailable_timeout_callback_(std::move(unavailable_timeout_callback)) {}

  std::function<void()> unavailable_timeout_callback_;
};

namespace {

Address CreateRandomAddress(const std::string &addr) {
  Address address;
  address.set_ip_address(addr);
  address.set_node_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  return address;
}

}  // namespace

class MockGcsClientNodeAccessor : public gcs::NodeInfoAccessor {
 public:
  explicit MockGcsClientNodeAccessor(bool is_subscribed_to_node_change)
      : gcs::NodeInfoAccessor(nullptr),
        is_subscribed_to_node_change_(is_subscribed_to_node_change) {}

  bool IsSubscribedToNodeChange() const override { return is_subscribed_to_node_change_; }

  MOCK_METHOD(const rpc::GcsNodeAddressAndLiveness *,
              GetNodeAddressAndLiveness,
              (const NodeID &, bool),
              (const, override));

  MOCK_METHOD(void,
              AsyncGetAllNodeAddressAndLiveness,
              (const gcs::MultiItemCallback<rpc::GcsNodeAddressAndLiveness> &,
               int64_t,
               const std::vector<NodeID> &),
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
        raylet_client_pool_(
            std::make_unique<RayletClientPool>([this](const Address &addr) {
              return std::make_shared<MockRayletClient>(
                  RayletClientPool::GetDefaultUnavailableTimeoutCallback(
                      &this->gcs_client_, this->raylet_client_pool_.get(), addr));
            })) {}

  bool is_subscribed_to_node_change_;
  MockGcsClient gcs_client_;
  std::unique_ptr<RayletClientPool> raylet_client_pool_;
};

bool CheckRayletClientPoolHasClient(RayletClientPool &raylet_client_pool,
                                    const NodeID &node_id) {
  absl::MutexLock lock(&raylet_client_pool.mu_);
  return raylet_client_pool.client_map_.contains(node_id);
}

TEST_P(DefaultUnavailableTimeoutCallbackTest, NodeDeath) {
  // Add 2 raylet clients to the pool.
  // raylet_client_1 unavailable calls:
  // 1. Node info hasn't been cached yet, but GCS knows it's alive.
  // 2. Node info has been cached and GCS knows it's alive.
  // 3. Node is dead according to cache + GCS, should disconnect.
  // raylet_client_2 unavailable calls:
  // 1. Subscriber cache and GCS don't know about node. Means the node is dead and the GCS
  //    had to discard to keep its cache size in check, should disconnect.

  auto &mock_node_accessor = gcs_client_.MockNodeAccessor();
  auto invoke_with_node_info_vector =
      [](std::vector<GcsNodeAddressAndLiveness> node_info_vector) {
        return Invoke(
            [node_info_vector](
                const gcs::MultiItemCallback<rpc::GcsNodeAddressAndLiveness> &callback,
                int64_t,
                const std::vector<NodeID> &) {
              callback(Status::OK(), node_info_vector);
            });
      };

  auto raylet_client_1_address = CreateRandomAddress("1");
  auto raylet_client_2_address = CreateRandomAddress("2");
  auto raylet_client_1_node_id = NodeID::FromBinary(raylet_client_1_address.node_id());
  auto raylet_client_2_node_id = NodeID::FromBinary(raylet_client_2_address.node_id());

  auto raylet_client_1 = dynamic_cast<MockRayletClient *>(
      raylet_client_pool_->GetOrConnectByAddress(raylet_client_1_address).get());
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_1_node_id));
  auto raylet_client_2 = dynamic_cast<MockRayletClient *>(
      raylet_client_pool_->GetOrConnectByAddress(raylet_client_2_address).get());
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_2_node_id));

  GcsNodeAddressAndLiveness node_info_alive;
  node_info_alive.set_state(GcsNodeInfo::ALIVE);
  GcsNodeAddressAndLiveness node_info_dead;
  node_info_dead.set_state(GcsNodeInfo::DEAD);
  if (is_subscribed_to_node_change_) {
    EXPECT_CALL(
        mock_node_accessor,
        GetNodeAddressAndLiveness(raylet_client_1_node_id, /*filter_dead_nodes=*/false))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(&node_info_alive))
        .WillOnce(Return(&node_info_dead));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAllNodeAddressAndLiveness(
                    _, _, std::vector<NodeID>{raylet_client_1_node_id}))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}));
    EXPECT_CALL(
        mock_node_accessor,
        GetNodeAddressAndLiveness(raylet_client_2_node_id, /*filter_dead_nodes=*/false))
        .WillOnce(Return(nullptr));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAllNodeAddressAndLiveness(
                    _, _, std::vector<NodeID>{raylet_client_2_node_id}))
        .WillOnce(invoke_with_node_info_vector({}));
  } else {
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAllNodeAddressAndLiveness(
                    _, _, std::vector<NodeID>{raylet_client_1_node_id}))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}))
        .WillOnce(invoke_with_node_info_vector({node_info_alive}))
        .WillOnce(invoke_with_node_info_vector({node_info_dead}));
    EXPECT_CALL(mock_node_accessor,
                AsyncGetAllNodeAddressAndLiveness(
                    _, _, std::vector<NodeID>{raylet_client_2_node_id}))
        .WillOnce(invoke_with_node_info_vector({}));
  }

  raylet_client_1->unavailable_timeout_callback_();
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_1_node_id));
  raylet_client_1->unavailable_timeout_callback_();
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_1_node_id));
  raylet_client_1->unavailable_timeout_callback_();
  ASSERT_FALSE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_1_node_id));
  raylet_client_2->unavailable_timeout_callback_();
  ASSERT_FALSE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_2_node_id));
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
