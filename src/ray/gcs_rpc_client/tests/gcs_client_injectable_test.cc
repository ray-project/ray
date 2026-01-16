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
class FakeGcsClientContext : public GcsClientContext {
 public:
  FakeGcsClientContext(std::shared_ptr<TestGcsRpcClient> rpc_client,
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

  bool IsInitialized() const override { return true; }

  void Disconnect() override {}

  void SetGcsRpcClient(std::shared_ptr<rpc::GcsRpcClient> client) override {}

  void SetGcsSubscriber(std::unique_ptr<pubsub::GcsSubscriber> subscriber) override {}

 private:
  std::shared_ptr<TestGcsRpcClient> rpc_client_;
  std::unique_ptr<TestGcsSubscriber> subscriber_;
};

// Test NodeInfoAccessor implementation
class TestActorInfoAccessor : public ActorInfoAccessorInterface {
 public:
  explicit TestActorInfoAccessor(GcsClientContext *client_impl) : is_fake_(true) {}
  ~TestActorInfoAccessor() override = default;

  bool IsFake() const { return is_fake_; }

  void AsyncGet(const ActorID &actor_id,
                const OptionalItemCallback<rpc::ActorTableData> &callback) override {}
  void AsyncGetAllByFilter(const std::optional<ActorID> &actor_id,
                           const std::optional<JobID> &job_id,
                           const std::optional<std::string> &actor_state_name,
                           const MultiItemCallback<rpc::ActorTableData> &callback,
                           int64_t timeout_ms = -1) override {}
  void AsyncGetByName(const std::string &name,
                      const std::string &ray_namespace,
                      const OptionalItemCallback<rpc::ActorTableData> &callback,
                      int64_t timeout_ms = -1) override {}
  Status SyncGetByName(const std::string &name,
                       const std::string &ray_namespace,
                       rpc::ActorTableData &actor_table_data,
                       rpc::TaskSpec &task_spec) override {
    return Status::OK();
  }
  Status SyncListNamedActors(
      bool all_namespaces,
      const std::string &ray_namespace,
      std::vector<std::pair<std::string, std::string>> &actors) override {
    return Status::OK();
  }
  void AsyncReportActorOutOfScope(const ActorID &actor_id,
                                  uint64_t num_restarts_due_to_lineage_reconstruction,
                                  const StatusCallback &callback,
                                  int64_t timeout_ms = -1) override {}
  void AsyncRegisterActor(const TaskSpecification &task_spec,
                          const StatusCallback &callback,
                          int64_t timeout_ms = -1) override {}
  void AsyncRestartActorForLineageReconstruction(
      const ActorID &actor_id,
      uint64_t num_restarts_due_to_lineage_reconstructions,
      const StatusCallback &callback,
      int64_t timeout_ms = -1) override {}
  Status SyncRegisterActor(const ray::TaskSpecification &task_spec) override {
    return Status::OK();
  }
  void AsyncKillActor(const ActorID &actor_id,
                      bool force_kill,
                      bool no_restart,
                      const StatusCallback &callback,
                      int64_t timeout_ms = -1) override {}
  void AsyncCreateActor(
      const TaskSpecification &task_spec,
      const rpc::ClientCallback<rpc::CreateActorReply> &callback) override {}
  void AsyncSubscribe(const ActorID &actor_id,
                      const SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe,
                      const StatusCallback &done) override {}
  void AsyncResubscribe() override {}
  void AsyncUnsubscribe(const ActorID &actor_id) override {}
  bool IsActorUnsubscribed(const ActorID &actor_id) override { return false; }

 private:
  bool is_fake_;
};

// Custom AccessorFactory that provides FakeActorInfoAccessor
class MixedAccessorFactory : public AccessorFactoryInterface {
 public:
  MixedAccessorFactory() = default;
  ~MixedAccessorFactory() override = default;

  std::unique_ptr<ActorInfoAccessorInterface> CreateActorInfoAccessor(
      GcsClientContext *client_impl) override {
    // Return mock implementation
    return std::make_unique<TestActorInfoAccessor>(client_impl);
  }
};

TEST(GcsClientInjectableTest, AccessorFactoryReturnsInjectedAccessorIfDefaultOverriden) {
  // Create mock RPC client and subscriber
  auto rpc_client = std::make_shared<TestGcsRpcClient>();
  auto subscriber = std::make_unique<TestGcsSubscriber>();

  // Create GCS client context
  auto context =
      std::make_unique<FakeGcsClientContext>(rpc_client, std::move(subscriber));

  // Create custom accessor factory
  auto factory = std::make_unique<MixedAccessorFactory>();

  // Create GcsClient with custom context and factory
  GcsClientOptions options;
  GcsClient gcs_client(
      options, "", UniqueID::FromRandom(), std::move(factory), std::move(context));

  // Connect the client
  instrumented_io_context io_service;
  Status status = gcs_client.Connect(io_service, -1);
  ASSERT_TRUE(status.ok());

  // Verify that NodeInfoAccessor is the fake implementation
  auto &actor_accessor = gcs_client.Actors();
  auto fake_actor_accessor = dynamic_cast<TestActorInfoAccessor *>(&actor_accessor);
  ASSERT_NE(fake_actor_accessor, nullptr);
  EXPECT_TRUE(fake_actor_accessor->IsFake());
}

}  // namespace gcs
}  // namespace ray
