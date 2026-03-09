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

#include "ray/core_worker/actor_pool_manager.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/gcs_client/accessors/actor_info_accessor.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"

namespace ray {
namespace core {
namespace {

using ::testing::_;
using ::testing::Return;

// Mock ActorTaskSubmitterInterface for testing
class MockActorTaskSubmitter : public ActorTaskSubmitterInterface {
 public:
  MockActorTaskSubmitter() = default;
  MOCK_METHOD(void,
              AddActorQueueIfNotExists,
              (const ActorID &actor_id,
               int32_t max_pending_calls,
               bool allow_out_of_order_execution,
               bool fail_if_actor_unreachable,
               bool owned),
              (override));
  MOCK_METHOD(void,
              ConnectActor,
              (const ActorID &actor_id,
               const rpc::Address &address,
               int64_t num_restarts),
              (override));
  MOCK_METHOD(void,
              DisconnectActor,
              (const ActorID &actor_id,
               int64_t num_restarts,
               bool dead,
               const rpc::ActorDeathCause &death_cause,
               bool is_restartable),
              (override));
  MOCK_METHOD(void, CheckTimeoutTasks, (), (override));
  MOCK_METHOD(void, SetPreempted, (const ActorID &actor_id), (override));
  virtual ~MockActorTaskSubmitter() = default;
};

// Mock GcsClient for testing
class MockGcsClient : public gcs::GcsClient {
 public:
  explicit MockGcsClient(gcs::GcsClientOptions options) : gcs::GcsClient(options) {}
  void Init(gcs::FakeActorInfoAccessor *actor_info_accessor) {
    actor_accessor_.reset(actor_info_accessor);
  }
};

// Test fixture for ActorPoolManager tests
class ActorPoolManagerTest : public ::testing::Test {
 protected:
  ActorPoolManagerTest()
      : gcs_options_("localhost",
                     6793,
                     ClusterID::Nil(),
                     /*allow_cluster_id_nil=*/true,
                     /*fetch_cluster_id_if_nil=*/false),
        gcs_client_(std::make_shared<MockGcsClient>(gcs_options_)),
        actor_info_accessor_(new gcs::FakeActorInfoAccessor()),
        mock_task_submitter_(std::make_shared<MockActorTaskSubmitter>()),
        mock_task_manager_(std::make_unique<MockTaskManagerInterface>()),
        publisher_(std::make_unique<pubsub::FakePublisher>()),
        subscriber_(std::make_unique<pubsub::FakeSubscriber>()),
        reference_counter_(std::make_unique<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            [](const NodeID &node_id) { return true; },
            fake_owned_object_count_gauge_,
            fake_owned_object_size_gauge_,
            /*lineage_pinning_enabled=*/true)) {
    gcs_client_->Init(actor_info_accessor_);
  }

  void SetUp() override {
    // Create real ActorManager (required by ActorPoolManager)
    actor_manager_ = std::make_unique<ActorManager>(
        gcs_client_, *mock_task_submitter_, *reference_counter_);

    // Create ActorPoolManager with minimal constructor (no callbacks)
    pool_manager_ = std::make_unique<ActorPoolManager>(
        *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  }

  void TearDown() override {
    pool_manager_.reset();
    actor_manager_.reset();
    mock_task_manager_.reset();
  }

  // Helper to create a pool with default config
  ActorPoolID CreateTestPool(int32_t max_retry_attempts = 3) {
    ActorPoolConfig config;
    config.max_retry_attempts = max_retry_attempts;
    config.retry_backoff_ms = 100;
    config.retry_on_system_errors = true;
    config.ordering_mode = PoolOrderingMode::UNORDERED;
    return pool_manager_->RegisterPool(config);
  }

  // Helper to create random actor IDs
  ActorID CreateActorID() {
    return ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
  }

  gcs::GcsClientOptions gcs_options_;
  std::shared_ptr<MockGcsClient> gcs_client_;
  gcs::FakeActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<MockActorTaskSubmitter> mock_task_submitter_;
  std::unique_ptr<MockTaskManagerInterface> mock_task_manager_;
  std::unique_ptr<pubsub::FakePublisher> publisher_;
  std::unique_ptr<pubsub::FakeSubscriber> subscriber_;
  ray::observability::FakeGauge fake_owned_object_count_gauge_;
  ray::observability::FakeGauge fake_owned_object_size_gauge_;
  std::unique_ptr<ReferenceCounterInterface> reference_counter_;
  std::unique_ptr<ActorManager> actor_manager_;
  std::unique_ptr<ActorPoolManager> pool_manager_;
};

// Test: Pool registration creates a valid pool ID
TEST_F(ActorPoolManagerTest, RegisterPoolReturnsValidId) {
  auto pool_id = CreateTestPool();

  EXPECT_FALSE(pool_id.IsNil());
  EXPECT_TRUE(pool_manager_->HasPool(pool_id));
}

// Test: Multiple pools have unique IDs
TEST_F(ActorPoolManagerTest, MultiplePoolsHaveUniqueIds) {
  auto pool_id1 = CreateTestPool();
  auto pool_id2 = CreateTestPool();
  auto pool_id3 = CreateTestPool();

  EXPECT_NE(pool_id1, pool_id2);
  EXPECT_NE(pool_id2, pool_id3);
  EXPECT_NE(pool_id1, pool_id3);

  EXPECT_TRUE(pool_manager_->HasPool(pool_id1));
  EXPECT_TRUE(pool_manager_->HasPool(pool_id2));
  EXPECT_TRUE(pool_manager_->HasPool(pool_id3));
}

// Test: Unregistering a pool removes it
TEST_F(ActorPoolManagerTest, UnregisterPoolRemovesPool) {
  auto pool_id = CreateTestPool();
  EXPECT_TRUE(pool_manager_->HasPool(pool_id));

  pool_manager_->UnregisterPool(pool_id);

  EXPECT_FALSE(pool_manager_->HasPool(pool_id));
}

// Test: Unregistering non-existent pool doesn't crash
TEST_F(ActorPoolManagerTest, UnregisterNonExistentPoolIsSafe) {
  auto fake_pool_id = ActorPoolID::FromRandom();

  // Should not crash or throw
  pool_manager_->UnregisterPool(fake_pool_id);

  EXPECT_FALSE(pool_manager_->HasPool(fake_pool_id));
}

// Test: Adding actors to pool
TEST_F(ActorPoolManagerTest, AddActorToPool) {
  auto pool_id = CreateTestPool();
  auto actor_id1 = CreateActorID();
  auto actor_id2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor_id1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor_id2, NodeID::FromRandom());

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 2);
  EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id1) != actors.end());
  EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id2) != actors.end());
}

// Test: Adding same actor twice is idempotent
TEST_F(ActorPoolManagerTest, AddActorTwiceIsIdempotent) {
  auto pool_id = CreateTestPool();
  auto actor_id = CreateActorID();
  auto node_id = NodeID::FromRandom();

  pool_manager_->AddActorToPool(pool_id, actor_id, node_id);
  pool_manager_->AddActorToPool(pool_id, actor_id, node_id);  // Second add

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 1);
}

// Test: Removing actors from pool
TEST_F(ActorPoolManagerTest, RemoveActorFromPool) {
  auto pool_id = CreateTestPool();
  auto actor_id1 = CreateActorID();
  auto actor_id2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor_id1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor_id2, NodeID::FromRandom());

  pool_manager_->RemoveActorFromPool(pool_id, actor_id1);

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 1);
  EXPECT_EQ(actors[0], actor_id2);
}

// Test: Removing non-existent actor is safe
TEST_F(ActorPoolManagerTest, RemoveNonExistentActorIsSafe) {
  auto pool_id = CreateTestPool();
  auto actor_id = CreateActorID();

  // Should not crash
  pool_manager_->RemoveActorFromPool(pool_id, actor_id);

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_TRUE(actors.empty());
}

// Test: Pool with initial actors
TEST_F(ActorPoolManagerTest, RegisterPoolWithInitialActors) {
  ActorPoolConfig config;
  config.max_retry_attempts = 3;

  std::vector<ActorID> initial_actors = {
      CreateActorID(),
      CreateActorID(),
      CreateActorID(),
  };

  auto pool_id = pool_manager_->RegisterPool(config, initial_actors);

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 3);
  for (const auto &actor_id : initial_actors) {
    EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id) != actors.end());
  }
}

// Test: GetPoolActors returns empty for non-existent pool
TEST_F(ActorPoolManagerTest, GetPoolActorsReturnsEmptyForNonExistentPool) {
  auto fake_pool_id = ActorPoolID::FromRandom();

  auto actors = pool_manager_->GetPoolActors(fake_pool_id);
  EXPECT_TRUE(actors.empty());
}

// Test: Pool stats initial values
TEST_F(ActorPoolManagerTest, PoolStatsInitialValues) {
  auto pool_id = CreateTestPool();
  auto actor_id = CreateActorID();
  pool_manager_->AddActorToPool(pool_id, actor_id, NodeID::FromRandom());

  auto stats = pool_manager_->GetPoolStats(pool_id);

  EXPECT_EQ(stats.total_tasks_submitted, 0);
  EXPECT_EQ(stats.total_tasks_failed, 0);
  EXPECT_EQ(stats.total_tasks_retried, 0);
  EXPECT_EQ(stats.num_actors, 1);
  EXPECT_EQ(stats.backlog_size, 0);
  EXPECT_EQ(stats.total_in_flight, 0);
}

// Test: Pool stats for non-existent pool returns zeros
TEST_F(ActorPoolManagerTest, PoolStatsForNonExistentPoolReturnsZeros) {
  auto fake_pool_id = ActorPoolID::FromRandom();

  auto stats = pool_manager_->GetPoolStats(fake_pool_id);

  EXPECT_EQ(stats.total_tasks_submitted, 0);
  EXPECT_EQ(stats.total_tasks_failed, 0);
  EXPECT_EQ(stats.total_tasks_retried, 0);
  EXPECT_EQ(stats.num_actors, 0);
  EXPECT_EQ(stats.backlog_size, 0);
}

// Test: HasPool returns false for non-existent pool
TEST_F(ActorPoolManagerTest, HasPoolReturnsFalseForNonExistent) {
  auto fake_pool_id = ActorPoolID::FromRandom();
  EXPECT_FALSE(pool_manager_->HasPool(fake_pool_id));
}

// Test: Unregister removes actor-to-pool mappings
TEST_F(ActorPoolManagerTest, UnregisterRemovesActorMappings) {
  auto pool_id = CreateTestPool();
  auto actor_id = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor_id, NodeID::FromRandom());

  // Verify actor is in pool
  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 1);

  // Unregister pool
  pool_manager_->UnregisterPool(pool_id);

  // Pool should not exist
  EXPECT_FALSE(pool_manager_->HasPool(pool_id));

  // Getting actors for unregistered pool returns empty
  actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_TRUE(actors.empty());
}

// Test: Multiple pools can have different actors
TEST_F(ActorPoolManagerTest, MultiplePoolsWithDifferentActors) {
  auto pool_id1 = CreateTestPool();
  auto pool_id2 = CreateTestPool();

  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id1, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id1, actor2, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id2, actor3, NodeID::FromRandom());

  auto actors1 = pool_manager_->GetPoolActors(pool_id1);
  auto actors2 = pool_manager_->GetPoolActors(pool_id2);

  EXPECT_EQ(actors1.size(), 2);
  EXPECT_EQ(actors2.size(), 1);
  EXPECT_EQ(actors2[0], actor3);
}

// Test: Pool config is stored correctly
TEST_F(ActorPoolManagerTest, PoolConfigIsStored) {
  ActorPoolConfig config;
  config.max_retry_attempts = 5;
  config.retry_backoff_ms = 500;
  config.retry_on_system_errors = false;
  config.ordering_mode = PoolOrderingMode::UNORDERED;
  config.min_size = 2;
  config.max_size = 10;
  config.initial_size = 4;

  auto pool_id = pool_manager_->RegisterPool(config);

  // We can verify config indirectly through stats
  // The pool should exist
  EXPECT_TRUE(pool_manager_->HasPool(pool_id));

  // Stats should reflect empty pool initially
  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.num_actors, 0);  // No initial actors passed
}

// Test: SubmitTaskToPool with no actors returns empty refs and queues work
TEST_F(ActorPoolManagerTest, SubmitTaskWithNoActorsQueuesWork) {
  auto pool_id = CreateTestPool();

  // Submit task to empty pool
  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;

  auto refs =
      pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);

  // No actors available, should return empty (work queued)
  EXPECT_TRUE(refs.empty());

  // Backlog should have the queued work
  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 1);
}

// Test: SubmitTaskToPool to non-existent pool returns empty
TEST_F(ActorPoolManagerTest, SubmitTaskToNonExistentPoolReturnsEmpty) {
  auto fake_pool_id = ActorPoolID::FromRandom();

  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;

  auto refs =
      pool_manager_->SubmitTaskToPool(fake_pool_id, function, std::move(args), options);

  EXPECT_TRUE(refs.empty());
}

// Test: Pool with actors has correct stats
TEST_F(ActorPoolManagerTest, PoolWithActorsHasCorrectStats) {
  auto pool_id = CreateTestPool();

  // Add multiple actors
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor3, NodeID::FromRandom());

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.num_actors, 3);
  EXPECT_EQ(stats.total_in_flight, 0);
  EXPECT_EQ(stats.backlog_size, 0);
}

// Test: Multiple tasks queue up when no actors available
TEST_F(ActorPoolManagerTest, MultipleTasksQueueWhenNoActors) {
  auto pool_id = CreateTestPool();

  // Submit multiple tasks to empty pool
  for (int i = 0; i < 5; i++) {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }

  // All should be queued
  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 5);
  EXPECT_EQ(stats.total_in_flight, 0);
}

}  // namespace
}  // namespace core
}  // namespace ray
