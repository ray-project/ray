// Copyright 2026 The Ray Authors.
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

#include "ray/gcs/gcs_resource_load_puller.h"

#include <atomic>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest.h"
#include "ray/asio/asio_util.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace gcs {

namespace {

class MockRayletClient : public rpc::FakeRayletClient {
 public:
  void GetResourceLoad(const rpc::ClientCallback<rpc::GetResourceLoadReply> &) override {
    num_calls_++;
  }

  int NumCalls() { return num_calls_; }

 private:
  std::atomic<int> num_calls_{0};
};

rpc::Address AddressOf(const NodeID &node_id) {
  rpc::Address address;
  address.set_node_id(node_id.Binary());
  address.set_ip_address("127.0.0.1");
  address.set_port(1234);
  return address;
}

}  // namespace

class GcsResourceLoadPullerTest : public ::testing::Test {
 protected:
  GcsResourceLoadPullerTest()
      : pull_io_thread_("test_pull",
                        /*enable_lag_probe=*/false,
                        /*used_for_health_check=*/false) {}

  std::shared_ptr<MockRayletClient> ClientFor(const NodeID &node_id) {
    absl::MutexLock lock(&mutex_);
    auto &client = clients_[node_id];
    if (client == nullptr) {
      client = std::make_shared<MockRayletClient>();
    }
    return client;
  }

  int FactoryCalls(const NodeID &node_id) {
    absl::MutexLock lock(&mutex_);
    return factory_calls_[node_id];
  }

  std::unique_ptr<GcsResourceLoadPuller> MakePuller() {
    pool_ = std::make_unique<rpc::RayletClientPool>([this](const rpc::Address &address) {
      const auto node_id = NodeID::FromBinary(address.node_id());
      {
        absl::MutexLock lock(&mutex_);
        factory_calls_[node_id]++;
      }
      return ClientFor(node_id);
    });
    return std::make_unique<GcsResourceLoadPuller>(pull_io_thread_.GetIoService(),
                                                   pull_io_thread_.GetIoService(),
                                                   *pool_,
                                                   [](rpc::ResourcesData) {});
  }

  void PullOnPullThread(GcsResourceLoadPuller &puller,
                        std::vector<rpc::Address> raylet_addresses) {
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    pull_io_thread_.GetIoService().post(
        [&puller, done, raylet_addresses = std::move(raylet_addresses)]() mutable {
          puller.Pull(std::move(raylet_addresses));
          done->set_value();
        },
        "GcsResourceLoadPullerTest.pull");
    future.wait();
  }

  InstrumentedIOContextWithThread pull_io_thread_;
  absl::Mutex mutex_;
  absl::flat_hash_map<NodeID, std::shared_ptr<MockRayletClient>> clients_;
  absl::flat_hash_map<NodeID, int> factory_calls_;
  std::unique_ptr<rpc::RayletClientPool> pool_;
};

// Each Pull() receives the latest alive nodes, so a node absent from it is no
// longer alive and must be dropped from the client pool automatically.
TEST_F(GcsResourceLoadPullerTest, DisconnectsRayletsThatLeftTheSnapshot) {
  const NodeID node1 = NodeID::FromRandom();
  const NodeID node2 = NodeID::FromRandom();
  auto puller = MakePuller();

  PullOnPullThread(*puller, {AddressOf(node1), AddressOf(node2)});
  EXPECT_EQ(ClientFor(node1)->NumCalls(), 1);
  EXPECT_EQ(ClientFor(node2)->NumCalls(), 1);

  PullOnPullThread(*puller, {AddressOf(node2)});
  EXPECT_EQ(ClientFor(node2)->NumCalls(), 2);

  PullOnPullThread(*puller, {AddressOf(node1), AddressOf(node2)});
  EXPECT_EQ(ClientFor(node1)->NumCalls(), 2);
  EXPECT_EQ(FactoryCalls(node1), 2);
  EXPECT_EQ(FactoryCalls(node2), 1);
}

}  // namespace gcs
}  // namespace ray
