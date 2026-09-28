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

#include "ray/core_worker_rpc_client/core_worker_client_pool.h"

#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs_rpc_client/accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"

namespace ray {
namespace rpc {

class FakeCoreWorkerClientForTest : public rpc::FakeCoreWorkerClient {
 public:
  explicit FakeCoreWorkerClientForTest(
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
  address.set_node_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  return address;
}

}  // namespace

void AssertID(WorkerID worker_id, CoreWorkerClientPool &client_pool, bool contains) {
  absl::MutexLock lock(&client_pool.mu_);
  if (contains) {
    ASSERT_NE(client_pool.worker_client_map_.find(worker_id),
              client_pool.worker_client_map_.end());
  } else {
    ASSERT_EQ(client_pool.worker_client_map_.find(worker_id),
              client_pool.worker_client_map_.end());
  }
}

TEST(CoreWorkerClientPoolTest, TestGC) {
  // Test to make sure idle clients are removed eventually.

  CoreWorkerClientPool client_pool([&](const rpc::Address &addr) {
    return std::make_shared<FakeCoreWorkerClientForTest>();
  });

  rpc::Address address1 = CreateRandomAddress("1");
  rpc::Address address2 = CreateRandomAddress("2");
  auto worker_id1 = WorkerID::FromBinary(address1.worker_id());
  auto worker_id2 = WorkerID::FromBinary(address2.worker_id());
  auto client1 = client_pool.GetOrConnect(address1);
  AssertID(worker_id1, client_pool, true);
  auto client2 = client_pool.GetOrConnect(address2);
  AssertID(worker_id2, client_pool, true);
  client_pool.Disconnect(worker_id2);
  AssertID(worker_id2, client_pool, false);
  AssertID(worker_id1, client_pool, true);
  client2 = client_pool.GetOrConnect(address2);
  AssertID(worker_id2, client_pool, true);
  dynamic_cast<FakeCoreWorkerClientForTest *>(client1.get())->is_idle_after_rpcs = true;
  // Client 1 will be removed since it's idle.
  client_pool.GetOrConnect(address2);
  AssertID(worker_id2, client_pool, true);
  AssertID(worker_id1, client_pool, false);
}

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
        raylet_client_pool_(std::make_unique<RayletClientPool>(
            [](const rpc::Address &) { return std::make_shared<FakeRayletClient>(); })),
        client_pool_(
            std::make_unique<CoreWorkerClientPool>([this](const rpc::Address &addr) {
              return std::make_shared<FakeCoreWorkerClientForTest>(
                  CoreWorkerClientPool::GetDefaultUnavailableTimeoutCallback(
                      &this->gcs_client_,
                      this->client_pool_.get(),
                      this->raylet_client_pool_.get(),
                      addr));
            })) {}

  bool is_subscribed_to_node_change_;
  gcs::GcsClientOptions options;
  FakeGcsClient gcs_client_;
  std::unique_ptr<RayletClientPool> raylet_client_pool_;
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

  auto &fake_node_accessor = gcs_client_.FakeNodeAccessor();

  auto worker_1_address = CreateRandomAddress("1");
  auto worker_2_address = CreateRandomAddress("2");
  auto worker_id1 = WorkerID::FromBinary(worker_1_address.worker_id());
  auto worker_id2 = WorkerID::FromBinary(worker_2_address.worker_id());
  auto worker_1_client = dynamic_cast<FakeCoreWorkerClientForTest *>(
      client_pool_->GetOrConnect(worker_1_address).get());
  AssertID(worker_id1, *client_pool_, true);
  auto worker_2_client = dynamic_cast<FakeCoreWorkerClientForTest *>(
      client_pool_->GetOrConnect(worker_2_address).get());
  AssertID(worker_id2, *client_pool_, true);

  auto worker_1_node_id = NodeID::FromBinary(worker_1_address.node_id());
  auto worker_2_node_id = NodeID::FromBinary(worker_2_address.node_id());

  rpc::GcsNodeAddressAndLiveness node_info_alive;
  node_info_alive.set_state(rpc::GcsNodeInfo::ALIVE);
  rpc::GcsNodeAddressAndLiveness node_info_dead;
  node_info_dead.set_state(rpc::GcsNodeInfo::DEAD);
  if (is_subscribed_to_node_change_) {
    fake_node_accessor.get_node_address_and_liveness_responses[worker_1_node_id] = {
        std::nullopt, node_info_alive, node_info_dead};
    fake_node_accessor.async_get_all_responses[worker_1_node_id] = {{node_info_alive}};
    fake_node_accessor.get_node_address_and_liveness_responses[worker_2_node_id] = {
        std::nullopt};
    fake_node_accessor.async_get_all_responses[worker_2_node_id] = {{}};
  } else {
    fake_node_accessor.async_get_all_responses[worker_1_node_id] = {
        {node_info_alive}, {node_info_alive}, {node_info_dead}};
    fake_node_accessor.async_get_all_responses[worker_2_node_id] = {{}};
  }

  auto raylet_client = std::dynamic_pointer_cast<FakeRayletClient>(
      raylet_client_pool_->GetOrConnectByAddress(worker_1_address));
  // Worker is alive when node is alive.
  raylet_client->is_local_worker_dead_hook =
      [](const WorkerID &,
         const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) {
        rpc::IsLocalWorkerDeadReply reply;
        reply.set_is_dead(false);
        callback(Status::OK(), std::move(reply));
      };

  worker_1_client->unavailable_timeout_callback_();
  AssertID(worker_id1, *client_pool_, true);
  worker_1_client->unavailable_timeout_callback_();
  AssertID(worker_id1, *client_pool_, true);
  worker_1_client->unavailable_timeout_callback_();
  AssertID(worker_id1, *client_pool_, false);
  worker_2_client->unavailable_timeout_callback_();
  AssertID(worker_id2, *client_pool_, false);

  // Worker is alive on both node-alive checks (calls 1 and 2 for worker_1).
  EXPECT_EQ(raylet_client->num_is_local_worker_dead_requests, 2);
  EXPECT_TRUE(fake_node_accessor.AllResponsesConsumed());
}

TEST_P(DefaultUnavailableTimeoutCallbackTest, WorkerDeath) {
  // Add the client to the pool.
  // 1st call - Node is alive and worker is alive.
  // 2nd call - Node is alive and worker is dead, client should be disconnected.

  auto worker_address = CreateRandomAddress("1");
  auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  auto worker_node_id = NodeID::FromBinary(worker_address.node_id());
  auto core_worker_client = dynamic_cast<FakeCoreWorkerClientForTest *>(
      client_pool_->GetOrConnect(worker_address).get());
  AssertID(worker_id, *client_pool_, true);

  auto &fake_node_accessor = gcs_client_.FakeNodeAccessor();
  rpc::GcsNodeAddressAndLiveness node_info_alive;
  node_info_alive.set_state(rpc::GcsNodeInfo::ALIVE);
  if (is_subscribed_to_node_change_) {
    fake_node_accessor.get_node_address_and_liveness_responses[worker_node_id] = {
        node_info_alive, node_info_alive};
  } else {
    fake_node_accessor.async_get_all_responses[worker_node_id] = {{node_info_alive},
                                                                  {node_info_alive}};
  }

  auto raylet_client = std::dynamic_pointer_cast<FakeRayletClient>(
      raylet_client_pool_->GetOrConnectByAddress(worker_address));
  // Worker is alive on the first check and dead on the second.
  raylet_client->is_local_worker_dead_hook =
      [call_count = 0](
          const WorkerID &,
          const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback) mutable {
        rpc::IsLocalWorkerDeadReply reply;
        reply.set_is_dead(call_count >= 1);
        ++call_count;
        callback(Status::OK(), std::move(reply));
      };

  // Disconnects the second time.
  core_worker_client->unavailable_timeout_callback_();
  AssertID(worker_id, *client_pool_, true);
  core_worker_client->unavailable_timeout_callback_();
  AssertID(worker_id, *client_pool_, false);

  EXPECT_EQ(raylet_client->num_is_local_worker_dead_requests, 2);
  EXPECT_TRUE(fake_node_accessor.AllResponsesConsumed());
}

INSTANTIATE_TEST_SUITE_P(IsSubscribedToNodeChange,
                         DefaultUnavailableTimeoutCallbackTest,
                         ::testing::Values(true, false));

}  // namespace rpc
}  // namespace ray
