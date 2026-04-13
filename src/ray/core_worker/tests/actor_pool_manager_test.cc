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

#include "absl/container/flat_hash_set.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/gcs_client/accessors/actor_info_accessor.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"

namespace ray {
namespace core {

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

TEST_F(ActorPoolManagerTest, UnregisterPoolCleansTrackedWorkItems) {
  auto pool_id = CreateTestPool();

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    PoolWorkItem work_item;
    work_item.pool_id = pool_id;
    work_item.work_item_id = TaskID::FromRandom(JobID());
    pool_manager_->TrackWorkItem(std::move(work_item));
  }

  pool_manager_->UnregisterPool(pool_id);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_TRUE(pool_manager_->work_items_.empty());
    EXPECT_EQ(pool_manager_->pool_to_work_items_.find(pool_id),
              pool_manager_->pool_to_work_items_.end());
  }
}

TEST_F(ActorPoolManagerTest, UnregisterPoolOnlyCleansItsOwnTrackedWorkItems) {
  auto pool_id1 = CreateTestPool();
  auto pool_id2 = CreateTestPool();
  TaskID pool2_work_item_id = TaskID::FromRandom(JobID());

  {
    absl::MutexLock lock(&pool_manager_->mu_);

    PoolWorkItem pool1_work_item;
    pool1_work_item.pool_id = pool_id1;
    pool1_work_item.work_item_id = TaskID::FromRandom(JobID());
    pool_manager_->TrackWorkItem(std::move(pool1_work_item));

    PoolWorkItem pool2_work_item;
    pool2_work_item.pool_id = pool_id2;
    pool2_work_item.work_item_id = pool2_work_item_id;
    pool_manager_->TrackWorkItem(std::move(pool2_work_item));
  }

  pool_manager_->UnregisterPool(pool_id1);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_EQ(pool_manager_->work_items_.size(), 1);
    EXPECT_TRUE(pool_manager_->work_items_.contains(pool2_work_item_id));
    EXPECT_EQ(pool_manager_->pool_to_work_items_.find(pool_id1),
              pool_manager_->pool_to_work_items_.end());
    auto pool2_it = pool_manager_->pool_to_work_items_.find(pool_id2);
    ASSERT_NE(pool2_it, pool_manager_->pool_to_work_items_.end());
    EXPECT_TRUE(pool2_it->second.contains(pool2_work_item_id));
  }
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

// Test: SelectActorForTask returns least-loaded actor
TEST_F(ActorPoolManagerTest, SelectActorForTaskReturnsLeastLoaded) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_FALSE(selected.IsNil());
  EXPECT_TRUE(selected == actor1 || selected == actor2);
}

// Test: SelectActorForTask returns Nil when pool is empty
TEST_F(ActorPoolManagerTest, SelectActorForTaskReturnsNilWhenEmpty) {
  auto pool_id = CreateTestPool();

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_TRUE(selected.IsNil());
}

// Test: SelectActorForTask returns Nil for non-existent pool
TEST_F(ActorPoolManagerTest, SelectActorForTaskReturnsNilForNonExistentPool) {
  auto selected = pool_manager_->SelectActorForTask(ActorPoolID::FromRandom());
  EXPECT_TRUE(selected.IsNil());
}

// Test: SelectActorForTask skips dead actors
TEST_F(ActorPoolManagerTest, SelectActorForTaskSkipsDeadActors) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  // Mark actor1 as dead by notifying failure for a task on it.
  // We simulate this by removing the actor from the pool.
  pool_manager_->RemoveActorFromPool(pool_id, actor1);

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_EQ(selected, actor2);
}

// =========================================================================
// MockLocalityDataProvider for locality-aware scheduling tests
// =========================================================================

class MockLocalityDataProvider : public LocalityDataProviderInterface {
 public:
  MockLocalityDataProvider() {}

  explicit MockLocalityDataProvider(
      absl::flat_hash_map<ObjectID, LocalityData> locality_data)
      : locality_data_(std::move(locality_data)) {}

  std::optional<LocalityData> GetLocalityData(const ObjectID &object_id) const override {
    auto it = locality_data_.find(object_id);
    if (it == locality_data_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  ~MockLocalityDataProvider() override = default;

  absl::flat_hash_map<ObjectID, LocalityData> locality_data_;
};

// Test fixture with locality support
class ActorPoolManagerLocalityTest : public ::testing::Test {
 protected:
  ActorPoolManagerLocalityTest()
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
    actor_manager_ = std::make_unique<ActorManager>(
        gcs_client_, *mock_task_submitter_, *reference_counter_);
  }

  void TearDown() override {
    pool_manager_.reset();
    actor_manager_.reset();
    mock_task_manager_.reset();
  }

  // Helper to create a pool with default config
  ActorPoolID CreateTestPool(int32_t max_tasks_in_flight = 2) {
    ActorPoolConfig config;
    config.max_retry_attempts = 3;
    config.retry_backoff_ms = 100;
    config.retry_on_system_errors = true;

    config.max_tasks_in_flight_per_actor = max_tasks_in_flight;
    return pool_manager_->RegisterPool(config);
  }

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
  std::unique_ptr<MockLocalityDataProvider> locality_provider_;
  std::unique_ptr<ActorPoolManager> pool_manager_;
};

// =========================================================================
// Locality-Aware Ranking Tests
// =========================================================================

// Test: Actor on node with data wins over remote actor
TEST_F(ActorPoolManagerLocalityTest, LocalitySelectsDataLocalActor) {
  NodeID local_node = NodeID::FromRandom();
  NodeID remote_node = NodeID::FromRandom();

  ObjectID obj1 = ObjectID::FromRandom();
  LocalityData data1;
  data1.object_size = 1000;
  data1.nodes_containing_object = {local_node};

  locality_provider_ = std::make_unique<MockLocalityDataProvider>(
      absl::flat_hash_map<ObjectID, LocalityData>{{obj1, data1}});

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  // Set locality provider directly (it's a private member, but we have FRIEND_TEST)
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto local_actor = CreateActorID();
  auto remote_actor = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, local_actor, local_node);
  pool_manager_->AddActorToPool(pool_id, remote_actor, remote_node);

  auto selected = pool_manager_->SelectActorForTask(pool_id, {obj1});
  EXPECT_EQ(selected, local_actor);
}

// Test: Two actors on same node, less loaded wins
TEST_F(ActorPoolManagerLocalityTest, LocalityTiebreakerIsLoad) {
  NodeID node = NodeID::FromRandom();

  ObjectID obj1 = ObjectID::FromRandom();
  LocalityData data1;
  data1.object_size = 1000;
  data1.nodes_containing_object = {node};

  locality_provider_ = std::make_unique<MockLocalityDataProvider>(
      absl::flat_hash_map<ObjectID, LocalityData>{{obj1, data1}});

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node);
  pool_manager_->AddActorToPool(pool_id, actor2, node);

  // Simulate actor1 having 1 task in flight
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 1;
  }

  auto selected = pool_manager_->SelectActorForTask(pool_id, {obj1});
  EXPECT_EQ(selected, actor2);
}

// Test: Unknown objects → load-only ranking
TEST_F(ActorPoolManagerLocalityTest, NoLocalityDataFallsBackToLoad) {
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();

  // No locality data for any objects
  locality_provider_ = std::make_unique<MockLocalityDataProvider>();

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node1);
  pool_manager_->AddActorToPool(pool_id, actor2, node2);

  // Actor1 has more load
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 2;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 0;
  }

  ObjectID unknown_obj = ObjectID::FromRandom();
  auto selected = pool_manager_->SelectActorForTask(pool_id, {unknown_obj});
  EXPECT_EQ(selected, actor2);
}

// Test: Empty arg_ids falls back to load
TEST_F(ActorPoolManagerLocalityTest, EmptyArgIdsFallsBackToLoad) {
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();

  locality_provider_ = std::make_unique<MockLocalityDataProvider>();

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node1);
  pool_manager_->AddActorToPool(pool_id, actor2, node2);

  // Actor1 has more load
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 3;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 1;
  }

  // No arg_ids
  auto selected = pool_manager_->SelectActorForTask(pool_id, {});
  EXPECT_EQ(selected, actor2);
}

// Test: Capacity-constrained selection returns Nil when all actors are full.
TEST_F(ActorPoolManagerLocalityTest, RequireAvailableCapacityReturnsNilWhenFull) {
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();

  locality_provider_ = std::make_unique<MockLocalityDataProvider>();

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool(/*max_tasks_in_flight=*/1);
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node1);
  pool_manager_->AddActorToPool(pool_id, actor2, node2);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 1;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 1;
  }

  EXPECT_TRUE(
      pool_manager_->SelectActorForTask(pool_id, {}, ActorID::Nil(), true).IsNil());
  EXPECT_FALSE(
      pool_manager_->SelectActorForTask(pool_id, {}, ActorID::Nil(), false).IsNil());
}

// Test: Equal-ranked selections rotate instead of always choosing the first actor.
TEST_F(ActorPoolManagerLocalityTest, EqualRankSelectionsRotateAcrossActors) {
  NodeID node = NodeID::FromRandom();

  locality_provider_ = std::make_unique<MockLocalityDataProvider>();

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool(/*max_tasks_in_flight=*/2);
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node);
  pool_manager_->AddActorToPool(pool_id, actor2, node);
  pool_manager_->AddActorToPool(pool_id, actor3, node);

  std::vector<ActorID> selections;
  selections.push_back(pool_manager_->SelectActorForTask(pool_id));
  selections.push_back(pool_manager_->SelectActorForTask(pool_id));
  selections.push_back(pool_manager_->SelectActorForTask(pool_id));

  EXPECT_EQ(absl::flat_hash_set<ActorID>(selections.begin(), selections.end()).size(), 3);
}

// Test: Actor on node with more bytes wins
TEST_F(ActorPoolManagerLocalityTest, LargerDataNodeWins) {
  NodeID node_small = NodeID::FromRandom();
  NodeID node_large = NodeID::FromRandom();

  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  LocalityData data1;
  data1.object_size = 100;
  data1.nodes_containing_object = {node_small};
  LocalityData data2;
  data2.object_size = 10000;
  data2.nodes_containing_object = {node_large};

  locality_provider_ = std::make_unique<MockLocalityDataProvider>(
      absl::flat_hash_map<ObjectID, LocalityData>{{obj1, data1}, {obj2, data2}});

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto actor_small = CreateActorID();
  auto actor_large = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor_small, node_small);
  pool_manager_->AddActorToPool(pool_id, actor_large, node_large);

  auto selected = pool_manager_->SelectActorForTask(pool_id, {obj1, obj2});
  EXPECT_EQ(selected, actor_large);
}

// Test: Null provider falls back to load
TEST_F(ActorPoolManagerLocalityTest, NullProviderFallsBackToLoad) {
  // No locality provider at all
  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  // locality_data_provider_ is already nullptr

  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  // Actor1 has more load
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 5;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 1;
  }

  ObjectID obj = ObjectID::FromRandom();
  auto selected = pool_manager_->SelectActorForTask(pool_id, {obj});
  // Should not crash, should select actor2 (less loaded)
  EXPECT_EQ(selected, actor2);
}

TEST_F(ActorPoolManagerLocalityTest,
       DrainQueuedWorkUsesLocalActorWhenActorsBecomeAvailable) {
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();

  ObjectID obj = ObjectID::FromRandom();
  LocalityData data;
  data.object_size = 1000;
  data.nodes_containing_object = {node2};

  locality_provider_ = std::make_unique<MockLocalityDataProvider>(
      absl::flat_hash_map<ObjectID, LocalityData>{{obj, data}});

  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool(/*max_tasks_in_flight=*/1);
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, node1);
  pool_manager_->AddActorToPool(pool_id, actor2, node2);
  pool_manager_->OnActorDead(actor1);
  pool_manager_->OnActorDead(actor2);

  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  args.push_back(std::make_unique<TaskArgByReference>(
      obj, rpc::Address(), "DrainQueuedWorkUsesLocalActorWhenActorsBecomeAvailable"));
  TaskOptions options;
  pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 1);
  EXPECT_EQ(stats.total_in_flight, 0);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].is_alive = true;
    pool_manager_->pools_[pool_id].actor_states[actor2].is_alive = true;
  }

  pool_manager_->DrainWorkQueue(pool_id);

  stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 0);
  EXPECT_EQ(stats.total_in_flight, 1);
  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor1), 0);
  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor2), 1);
}

// =========================================================================
// Backpressure Tests
// =========================================================================

// Test: GetOccupiedTaskSlots returns in-flight + backlog
TEST_F(ActorPoolManagerTest, GetOccupiedTaskSlots) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  // Initially zero
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 0);

  // Simulate some in-flight tasks
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 3;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 2;
  }
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 5);

  // Also add backlog
  {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    // Remove actors temporarily to force queueing
    pool_manager_->RemoveActorFromPool(pool_id, actor1);
    pool_manager_->RemoveActorFromPool(pool_id, actor2);
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }
  // in_flight is now 0 (actors removed) + 1 backlog = 1
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 1);

  // Non-existent pool returns 0
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(ActorPoolID::FromRandom()), 0);
}

// Test: GetNumActiveActors returns actors with tasks in flight
TEST_F(ActorPoolManagerTest, GetNumActiveActors) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor3, NodeID::FromRandom());

  // Initially zero
  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 0);

  // Simulate some in-flight tasks
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 2;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 0;
    pool_manager_->pools_[pool_id].actor_states[actor3].num_tasks_in_flight = 1;
  }
  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 2);

  // Non-existent pool returns 0
  EXPECT_EQ(pool_manager_->GetNumActiveActors(ActorPoolID::FromRandom()), 0);
}

TEST_F(ActorPoolManagerTest, OnTaskSubmittedTracksPerActorInflight) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor1), 0);
  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor2), 0);

  pool_manager_->OnTaskSubmitted(actor1);
  pool_manager_->OnTaskSubmitted(actor1);
  pool_manager_->OnTaskSubmitted(actor2);

  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor1), 2);
  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor2), 1);
  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 2);
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 3);
}

// =========================================================================
// Actor Death/Restart Recovery Tests
// =========================================================================

// Test: OnTaskFailed with ACTOR_DIED marks actor as not alive
TEST_F(ActorPoolManagerTest, OnTaskFailedMarksActorDead) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  // Simulate actor1 having a task in flight
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 1;
  }

  // Create a work item in work_items_ so OnTaskFailed can find it
  TaskID work_item_id = TaskID::FromRandom(JobID());
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    PoolWorkItem work_item;
    work_item.pool_id = pool_id;
    work_item.work_item_id = work_item_id;
    work_item.attempt_number = 0;
    pool_manager_->TrackWorkItem(std::move(work_item));
  }

  // Simulate task failure with ACTOR_DIED error
  rpc::RayErrorInfo error_info;
  error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
  error_info.set_error_message("Actor died");

  pool_manager_->OnPoolTaskComplete(pool_id,
                                    work_item_id,
                                    TaskID::FromRandom(JobID()),
                                    actor1,
                                    Status::IOError("actor died"),
                                    &error_info);

  // Actor1 should be marked as not alive
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_FALSE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    // Actor2 should still be alive
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor2].is_alive);
  }

  // SelectActorForTask should only return actor2
  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_EQ(selected, actor2);
}

TEST_F(ActorPoolManagerTest, LateTaskCompletionAfterUnregisterIsIgnoredWithoutLeak) {
  auto pool_id = CreateTestPool();
  auto actor_id = CreateActorID();
  TaskID work_item_id = TaskID::FromRandom(JobID());

  pool_manager_->AddActorToPool(pool_id, actor_id, NodeID::FromRandom());

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    PoolWorkItem work_item;
    work_item.pool_id = pool_id;
    work_item.work_item_id = work_item_id;
    pool_manager_->TrackWorkItem(std::move(work_item));
  }

  pool_manager_->UnregisterPool(pool_id);
  pool_manager_->OnPoolTaskComplete(pool_id,
                                    work_item_id,
                                    TaskID::FromRandom(JobID()),
                                    actor_id,
                                    Status::OK(),
                                    nullptr);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_TRUE(pool_manager_->work_items_.empty());
    EXPECT_EQ(pool_manager_->pool_to_work_items_.find(pool_id),
              pool_manager_->pool_to_work_items_.end());
  }
}

// Test: OnActorAlive marks actor alive and drains work queue
TEST_F(ActorPoolManagerTest, OnActorAliveDrainsQueue) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());

  // Mark actor as dead
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].is_alive = false;
  }

  // Submit a task — should be queued since no alive actors
  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;
  pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);

  // Verify task is queued
  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 1);

  // Simulate actor coming alive (e.g. after restart)
  pool_manager_->OnActorAlive(actor1, NodeID::FromRandom());

  // Actor should be alive again
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    EXPECT_EQ(pool_manager_->pools_[pool_id].actor_states[actor1].consecutive_failures,
              0);
  }

  // Work queue should have been drained. Check that the drain
  // attempted to process the queued item.
  stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.total_in_flight, 1);
  EXPECT_EQ(stats.backlog_size, 0);
}

// Test: OnActorDead marks actor as not alive
TEST_F(ActorPoolManagerTest, OnActorDeadMarksActorNotAlive) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  // Both should be alive initially
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor2].is_alive);
  }

  // Mark actor1 as dead via OnActorDead
  pool_manager_->OnActorDead(actor1);

  // Actor1 should be not alive, actor2 still alive
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_FALSE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor2].is_alive);
  }

  // SelectActorForTask should skip actor1
  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_EQ(selected, actor2);
}

TEST_F(ActorPoolManagerTest, OnActorDeadClearsInflightCount) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->OnTaskSubmitted(actor1);
  pool_manager_->OnTaskSubmitted(actor1);

  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor1), 2);

  pool_manager_->OnActorDead(actor1);

  EXPECT_EQ(pool_manager_->GetActorTasksInFlight(pool_id, actor1), 0);
  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 0);
}

// Test: OnActorDead for non-pool actor is safe
TEST_F(ActorPoolManagerTest, OnActorDeadForNonPoolActorIsSafe) {
  auto actor_id = CreateActorID();

  // Should not crash
  pool_manager_->OnActorDead(actor_id);
}

// Test: OnActorAlive for non-pool actor is safe
TEST_F(ActorPoolManagerTest, OnActorAliveForNonPoolActorIsSafe) {
  auto actor_id = CreateActorID();

  // Should not crash
  pool_manager_->OnActorAlive(actor_id, NodeID::FromRandom());
}

// Test: Full cycle — actor dies, work queued, actor restarts, work drained
TEST_F(ActorPoolManagerTest, FullDeathRestartRecoveryCycle) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());

  // Mark actor as dead
  pool_manager_->OnActorDead(actor1);

  // Submit tasks to empty pool (all actors dead) — should be queued
  for (int i = 0; i < 3; i++) {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 3);
  EXPECT_EQ(stats.total_in_flight, 0);

  // Actor restarts
  pool_manager_->OnActorAlive(actor1, NodeID::FromRandom());

  // All queued work should have been drained
  stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 0);
  // All 3 items get submitted (in_flight incremented)
  EXPECT_EQ(stats.total_in_flight, 3);
}

}  // namespace core
}  // namespace ray
