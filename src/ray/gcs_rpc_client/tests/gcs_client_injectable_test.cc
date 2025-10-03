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

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/gcs_rpc_client/accessor_factory_interface.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor_interface.h"
#include "ray/gcs_rpc_client/accessors/node_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"

namespace ray {

namespace pubsub {
class GcsSubscriber;
}

namespace rpc {
class GcsRpcClient;
}

namespace gcs {

// Mock GcsRpcClient - empty class for testing
class TestGcsRpcClient {
 public:
  TestGcsRpcClient() = default;
};

// Mock GcsSubscriber - empty class for testing
class TestGcsSubscriber {
 public:
  TestGcsSubscriber() = default;
};

// Test implementation of GcsClientContext
class TestGcsClientContext : public GcsClientContext {
 public:
  TestGcsClientContext(std::shared_ptr<TestGcsRpcClient> rpc_client,
                       std::unique_ptr<TestGcsSubscriber> subscriber)
      : rpc_client_(rpc_client), subscriber_(std::move(subscriber)) {}

  pubsub::GcsSubscriber &GetGcsSubscriber() override {
    // Cast our mock to the expected type
    return *reinterpret_cast<pubsub::GcsSubscriber *>(subscriber_.get());
  }

  rpc::GcsRpcClient &GetGcsRpcClient() override {
    // Cast our mock to the expected type
    return *reinterpret_cast<rpc::GcsRpcClient *>(rpc_client_.get());
  }

  bool isInitialized() const override { return true; }

  void Disconnect() override {}

  void setGcsRpcClient(std::shared_ptr<rpc::GcsRpcClient> client) override {}

  void setGcsSubscriber(std::unique_ptr<pubsub::GcsSubscriber> subscriber) override {}

 private:
  std::shared_ptr<TestGcsRpcClient> rpc_client_;
  std::unique_ptr<TestGcsSubscriber> subscriber_;
};

// Test NodeInfoAccessor implementation
class TestNodeInfoAccessor : public NodeInfoAccessorInterface {
 public:
  TestNodeInfoAccessor(GcsClientContext *client_impl) : is_fake_(true) {}
  ~TestNodeInfoAccessor() override = default;

  bool IsFake() const { return is_fake_; }

  Status RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                      const StatusCallback &callback) override {
    return Status::OK();
  }

  void UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                      std::function<void()> unregister_done_callback) override {}

  const NodeID &GetSelfId() const override {
    static NodeID id = NodeID::Nil();
    return id;
  }

  const rpc::GcsNodeInfo &GetSelfInfo() const override {
    static rpc::GcsNodeInfo info;
    return info;
  }

  void AsyncRegister(const rpc::GcsNodeInfo &node_info,
                     const StatusCallback &callback) override {}

  void AsyncCheckSelfAlive(const std::function<void(Status, bool)> &callback,
                           int64_t timeout_ms) override {}

  void AsyncCheckAlive(const std::vector<NodeID> &node_ids,
                       int64_t timeout_ms,
                       const MultiItemCallback<bool> &callback) override {}

  void AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback,
                   int64_t timeout_ms,
                   const std::vector<NodeID> &node_ids = {}) override {}

  void AsyncSubscribeToNodeChange(
      std::function<void(NodeID, const rpc::GcsNodeInfo &)> subscribe,
      StatusCallback done) override {}

  const rpc::GcsNodeInfo *Get(const NodeID &node_id,
                              bool filter_dead_nodes = true) const override {
    return nullptr;
  }

  const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &GetAll() const override {
    static absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> empty;
    return empty;
  }

  StatusOr<std::vector<rpc::GcsNodeInfo>> GetAllNoCache(
      int64_t timeout_ms,
      std::optional<rpc::GcsNodeInfo::GcsNodeState> state_filter = std::nullopt,
      std::optional<rpc::GetAllNodeInfoRequest::NodeSelector> node_selector =
          std::nullopt) override {
    return std::vector<rpc::GcsNodeInfo>();
  }

  Status CheckAlive(const std::vector<NodeID> &node_ids,
                    int64_t timeout_ms,
                    std::vector<bool> &nodes_alive) override {
    return Status::OK();
  }

  Status DrainNodes(const std::vector<NodeID> &node_ids,
                    int64_t timeout_ms,
                    std::vector<std::string> &drained_node_ids) override {
    return Status::OK();
  }

  bool IsNodeDead(const NodeID &node_id) const override { return false; }

  void AsyncResubscribe() override {}

  void HandleNotification(rpc::GcsNodeInfo &&node_info) override {}

  bool IsSubscribedToNodeChange() const override { return false; }

 private:
  bool is_fake_;
};

// Custom AccessorFactory that provides FakeNodeInfoAccessor and a real
// ActorInfoAccessor
class MixedAccessorFactory : public AccessorFactoryInterface {
 public:
  MixedAccessorFactory() = default;
  ~MixedAccessorFactory() override = default;

  std::unique_ptr<ActorInfoAccessorInterface> CreateActorInfoAccessor(
      GcsClientContext *client_impl) override {
    // Return real implementation
    return std::make_unique<ActorInfoAccessor>(client_impl);
  }

  std::unique_ptr<NodeInfoAccessorInterface> CreateNodeInfoAccessor(
      GcsClientContext *client_impl) override {
    // Return fake implementation
    return std::make_unique<TestNodeInfoAccessor>(client_impl);
  }

  // Not needed for this test - return nullptr
  std::unique_ptr<JobInfoAccessorInterface> CreateJobInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<NodeResourceInfoAccessorInterface> CreateNodeResourceInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<ErrorInfoAccessorInterface> CreateErrorInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<TaskInfoAccessorInterface> CreateTaskInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<WorkerInfoAccessorInterface> CreateWorkerInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<PlacementGroupInfoAccessorInterface> CreatePlacementGroupInfoAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<InternalKVAccessorInterface> CreateInternalKVAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<RuntimeEnvAccessorInterface> CreateRuntimeEnvAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<AutoscalerStateAccessorInterface> CreateAutoscalerStateAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }

  std::unique_ptr<PublisherAccessorInterface> CreatePublisherAccessor(
      GcsClientContext *client_impl) override {
    return nullptr;
  }
};

TEST(GcsClientInjectableTest, GcsClientWithMixedAccessors) {
  // Create mock RPC client and subscriber
  auto rpc_client = std::make_shared<TestGcsRpcClient>();
  auto subscriber = std::make_unique<TestGcsSubscriber>();

  // Create GCS client context
  auto context =
      std::make_unique<TestGcsClientContext>(rpc_client, std::move(subscriber));

  // Create custom accessor factory
  auto factory = std::make_unique<MixedAccessorFactory>();

  // Create GcsClient with custom context and factory
  GcsClientOptions options;
  GcsClient gcs_client(
      options, UniqueID::FromRandom(), std::move(factory), std::move(context));

  // Connect the client
  instrumented_io_context io_service;
  Status status = gcs_client.Connect(io_service, -1);
  ASSERT_TRUE(status.ok());

  // Verify that NodeInfoAccessor is the fake implementation
  auto &node_accessor = gcs_client.Nodes();
  auto fake_node_accessor = dynamic_cast<TestNodeInfoAccessor *>(&node_accessor);
  ASSERT_NE(fake_node_accessor, nullptr);
  EXPECT_TRUE(fake_node_accessor->IsFake());

  // Verify that ActorInfoAccessor is the real implementation
  auto &actor_accessor = gcs_client.Actors();
  auto real_actor_accessor = dynamic_cast<ActorInfoAccessor *>(&actor_accessor);
  ASSERT_NE(real_actor_accessor, nullptr);
}

}  // namespace gcs
}  // namespace ray
