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

#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/gcs_rpc_client/accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace rpc {

class FakeRayletClientForTest : public FakeRayletClient {
 public:
  explicit FakeRayletClientForTest(
      std::function<void()> unavailable_timeout_callback = nullptr)
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

// Hand-written fake node accessor. GetNodeAddressAndLiveness and
// AsyncGetAllNodeAddressAndLiveness serve queued responses (keyed by node id) in
// call order and record the calls, replacing the gmock sequenced expectations.
class FakeGcsClientNodeAccessor : public gcs::NodeInfoAccessor {
 public:
  explicit FakeGcsClientNodeAccessor(bool is_subscribed_to_node_change)
      : gcs::NodeInfoAccessor(nullptr),
        is_subscribed_to_node_change_(is_subscribed_to_node_change) {}

  bool IsSubscribedToNodeChange() const override { return is_subscribed_to_node_change_; }

  std::optional<rpc::GcsNodeAddressAndLiveness> GetNodeAddressAndLiveness(
      const NodeID &node_id, bool filter_dead_nodes) const override {
    get_node_address_and_liveness_calls.push_back(node_id);
    auto it = get_node_address_and_liveness_responses.find(node_id);
    RAY_CHECK(it != get_node_address_and_liveness_responses.end() && !it->second.empty());
    auto response = it->second.front();
    it->second.pop_front();
    return response;
  }

  void AsyncGetAllNodeAddressAndLiveness(
      const rpc::MultiItemCallback<rpc::GcsNodeAddressAndLiveness> &callback,
      int64_t timeout_ms,
      const std::vector<NodeID> &node_ids) override {
    async_get_all_calls.push_back(node_ids);
    RAY_CHECK_EQ(node_ids.size(), static_cast<size_t>(1));
    auto it = async_get_all_responses.find(node_ids[0]);
    RAY_CHECK(it != async_get_all_responses.end() && !it->second.empty());
    auto response = it->second.front();
    it->second.pop_front();
    callback(Status::OK(), std::move(response));
  }

  // Returns true when every queued response has been consumed, mirroring gmock's
  // WillOnce exhaustion check.
  bool AllResponsesConsumed() const {
    for (const auto &entry : get_node_address_and_liveness_responses) {
      if (!entry.second.empty()) {
        return false;
      }
    }
    for (const auto &entry : async_get_all_responses) {
      if (!entry.second.empty()) {
        return false;
      }
    }
    return true;
  }

  bool is_subscribed_to_node_change_;
  mutable absl::flat_hash_map<NodeID,
                              std::deque<std::optional<rpc::GcsNodeAddressAndLiveness>>>
      get_node_address_and_liveness_responses;
  mutable std::vector<NodeID> get_node_address_and_liveness_calls;
  absl::flat_hash_map<NodeID, std::deque<std::vector<rpc::GcsNodeAddressAndLiveness>>>
      async_get_all_responses;
  std::vector<std::vector<NodeID>> async_get_all_calls;
};

class FakeGcsClient : public gcs::GcsClient {
 public:
  explicit FakeGcsClient(bool is_subscribed_to_node_change,
                         gcs::GcsClientOptions &options)
      : GcsClient(options) {
    this->node_accessor_ =
        std::make_unique<FakeGcsClientNodeAccessor>(is_subscribed_to_node_change);
  }

  FakeGcsClientNodeAccessor &FakeNodeAccessor() {
    return static_cast<FakeGcsClientNodeAccessor &>(*this->node_accessor_);
  }
};

class DefaultUnavailableTimeoutCallbackTest : public ::testing::TestWithParam<bool> {
 public:
  DefaultUnavailableTimeoutCallbackTest()
      : is_subscribed_to_node_change_(GetParam()),
        options("127.0.0.1",
                6379,
                ClusterID::Nil(),
                /*allow_cluster_id_nil=*/true,
                /*fetch_cluster_id_if_nil=*/false),
        gcs_client_(is_subscribed_to_node_change_, options),
        raylet_client_pool_(
            std::make_unique<RayletClientPool>([this](const Address &addr) {
              return std::make_shared<FakeRayletClientForTest>(
                  RayletClientPool::GetDefaultUnavailableTimeoutCallback(
                      &this->gcs_client_, this->raylet_client_pool_.get(), addr));
            })) {}

  bool is_subscribed_to_node_change_;
  gcs::GcsClientOptions options;
  FakeGcsClient gcs_client_;
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

  auto &fake_node_accessor = gcs_client_.FakeNodeAccessor();

  auto raylet_client_1_address = CreateRandomAddress("1");
  auto raylet_client_2_address = CreateRandomAddress("2");
  auto raylet_client_1_node_id = NodeID::FromBinary(raylet_client_1_address.node_id());
  auto raylet_client_2_node_id = NodeID::FromBinary(raylet_client_2_address.node_id());

  auto raylet_client_1 = dynamic_cast<FakeRayletClientForTest *>(
      raylet_client_pool_->GetOrConnectByAddress(raylet_client_1_address).get());
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_1_node_id));
  auto raylet_client_2 = dynamic_cast<FakeRayletClientForTest *>(
      raylet_client_pool_->GetOrConnectByAddress(raylet_client_2_address).get());
  ASSERT_TRUE(
      CheckRayletClientPoolHasClient(*raylet_client_pool_, raylet_client_2_node_id));

  GcsNodeAddressAndLiveness node_info_alive;
  node_info_alive.set_state(GcsNodeInfo::ALIVE);
  GcsNodeAddressAndLiveness node_info_dead;
  node_info_dead.set_state(GcsNodeInfo::DEAD);
  if (is_subscribed_to_node_change_) {
    fake_node_accessor.get_node_address_and_liveness_responses[raylet_client_1_node_id] =
        {std::nullopt, node_info_alive, node_info_dead};
    fake_node_accessor.async_get_all_responses[raylet_client_1_node_id] = {
        {node_info_alive}};
    fake_node_accessor.get_node_address_and_liveness_responses[raylet_client_2_node_id] =
        {std::nullopt};
    fake_node_accessor.async_get_all_responses[raylet_client_2_node_id] = {{}};
  } else {
    fake_node_accessor.async_get_all_responses[raylet_client_1_node_id] = {
        {node_info_alive}, {node_info_alive}, {node_info_dead}};
    fake_node_accessor.async_get_all_responses[raylet_client_2_node_id] = {{}};
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

  EXPECT_TRUE(fake_node_accessor.AllResponsesConsumed());
}

INSTANTIATE_TEST_SUITE_P(IsSubscribedToNodeChange,
                         DefaultUnavailableTimeoutCallbackTest,
                         ::testing::Values(true, false));

}  // namespace rpc
}  // namespace ray
