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
#include <cstdint>
#include <functional>
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

class FakeRayletClientForTest : public rpc::FakeRayletClient {
 public:
  void GetResourceLoad(
      int64_t last_seen_version,
      const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback) override {
    absl::MutexLock lock(&mutex_);
    num_calls_++;
    last_seen_version_ = last_seen_version;
    callback_ = callback;
  }

  int NumCalls() {
    absl::MutexLock lock(&mutex_);
    return num_calls_;
  }

  int64_t LastSeenVersion() {
    absl::MutexLock lock(&mutex_);
    return last_seen_version_;
  }

  rpc::ClientCallback<rpc::GetResourceLoadReply> Callback() {
    absl::MutexLock lock(&mutex_);
    return callback_;
  }

 private:
  absl::Mutex mutex_;
  int num_calls_ ABSL_GUARDED_BY(mutex_) = 0;
  int64_t last_seen_version_ ABSL_GUARDED_BY(mutex_) = -1;
  rpc::ClientCallback<rpc::GetResourceLoadReply> callback_ ABSL_GUARDED_BY(mutex_);
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

  std::shared_ptr<FakeRayletClientForTest> ClientFor(const NodeID &node_id) {
    absl::MutexLock lock(&mutex_);
    auto &client = clients_[node_id];
    if (client == nullptr) {
      client = std::make_shared<FakeRayletClientForTest>();
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
    // The test runs the main io_context on the pull thread too, so the apply
    // and the version bookkeeping are serialized on one thread.
    return std::make_unique<GcsResourceLoadPuller>(pull_io_thread_.GetIoService(),
                                                   pull_io_thread_.GetIoService(),
                                                   *pool_,
                                                   [this](rpc::ResourcesData) {
                                                     num_applied_++;
                                                     return apply_succeeds_.load();
                                                   });
  }

  void RunOnPullThread(std::function<void()> fn) {
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    pull_io_thread_.GetIoService().post(
        [fn = std::move(fn), done]() {
          fn();
          done->set_value();
        },
        "GcsResourceLoadPullerTest.run");
    future.wait();
  }

  void PullOnPullThread(GcsResourceLoadPuller &puller,
                        std::vector<rpc::Address> raylet_addresses) {
    RunOnPullThread(
        [&puller, &raylet_addresses]() { puller.Pull(std::move(raylet_addresses)); });
  }

  /// Answers the node's outstanding pull and waits until the apply and the
  /// version bookkeeping it posts have run: each is posted behind the previous
  /// step, so two more round trips through the queue flush both.
  void Reply(const NodeID &node_id, const rpc::GetResourceLoadReply &reply) {
    auto callback = ClientFor(node_id)->Callback();
    RunOnPullThread([callback, reply = reply]() mutable {
      callback(Status::OK(), std::move(reply));
    });
    RunOnPullThread([]() {});
    RunOnPullThread([]() {});
  }

  static rpc::GetResourceLoadReply FullReply(const NodeID &node_id, int64_t version) {
    rpc::GetResourceLoadReply reply;
    reply.set_version(version);
    reply.mutable_resources()->set_node_id(node_id.Binary());
    return reply;
  }

  static rpc::GetResourceLoadReply UnchangedReply(int64_t version) {
    rpc::GetResourceLoadReply reply;
    reply.set_version(version);
    reply.set_unchanged(true);
    return reply;
  }

  InstrumentedIOContextWithThread pull_io_thread_;
  absl::Mutex mutex_;
  absl::flat_hash_map<NodeID, std::shared_ptr<FakeRayletClientForTest>> clients_;
  absl::flat_hash_map<NodeID, int> factory_calls_;
  std::unique_ptr<rpc::RayletClientPool> pool_;
  std::atomic<int> num_applied_{0};
  std::atomic<bool> apply_succeeds_{true};
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

// The puller sends the version it last applied, and an unchanged reply is
// dropped without touching the main io_context.
TEST_F(GcsResourceLoadPullerTest, SendsAppliedVersionAndSkipsUnchangedReplies) {
  const NodeID node = NodeID::FromRandom();
  auto puller = MakePuller();

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 0);
  Reply(node, FullReply(node, 3));
  EXPECT_EQ(num_applied_, 1);

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 3);
  Reply(node, UnchangedReply(3));
  EXPECT_EQ(num_applied_, 1);

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 3);
  Reply(node, FullReply(node, 4));
  EXPECT_EQ(num_applied_, 2);

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 4);
}

// A report the consumers dropped must not be recorded as applied, or the node
// would answer `unchanged` forever and its load would never reach them.
TEST_F(GcsResourceLoadPullerTest, RefetchesFullReportWhenApplyIsRejected) {
  const NodeID node = NodeID::FromRandom();
  auto puller = MakePuller();

  apply_succeeds_ = false;
  PullOnPullThread(*puller, {AddressOf(node)});
  Reply(node, FullReply(node, 3));
  EXPECT_EQ(num_applied_, 1);

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 0);
}

// The recorded version is the latest applied, not the highest, so a stale
// report applied after a newer one is replaced on the next pull.
TEST_F(GcsResourceLoadPullerTest, TracksLatestAppliedVersion) {
  const NodeID node = NodeID::FromRandom();
  auto puller = MakePuller();

  PullOnPullThread(*puller, {AddressOf(node)});
  Reply(node, FullReply(node, 5));
  Reply(node, FullReply(node, 4));

  PullOnPullThread(*puller, {AddressOf(node)});
  EXPECT_EQ(ClientFor(node)->LastSeenVersion(), 4);
}

// A node that leaves the snapshot forgets its version, as does a late apply for
// it, so the node is pulled in full if it shows up again.
TEST_F(GcsResourceLoadPullerTest, ForgetsVersionOfNodeThatLeftTheSnapshot) {
  const NodeID node1 = NodeID::FromRandom();
  const NodeID node2 = NodeID::FromRandom();
  auto puller = MakePuller();

  PullOnPullThread(*puller, {AddressOf(node1), AddressOf(node2)});
  Reply(node1, FullReply(node1, 3));
  PullOnPullThread(*puller, {AddressOf(node2)});
  Reply(node2, FullReply(node2, 7));
  // node1's reply to the first pull lands after node1 left.
  Reply(node1, FullReply(node1, 4));

  PullOnPullThread(*puller, {AddressOf(node1), AddressOf(node2)});
  EXPECT_EQ(ClientFor(node1)->LastSeenVersion(), 0);
  EXPECT_EQ(ClientFor(node2)->LastSeenVersion(), 7);
}

}  // namespace gcs
}  // namespace ray
