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
#include "ray/core_worker/lease_policy.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"

namespace ray {
namespace core {

class MockActorTaskSubmitter : public ActorTaskSubmitterInterface {
 public:
  MockActorTaskSubmitter() = default;
  MOCK_METHOD(void,
              AddActorQueueIfNotExists,
              (const ActorID &, int32_t, bool, bool, bool),
              (override));
  MOCK_METHOD(void,
              ConnectActor,
              (const ActorID &, const rpc::Address &, int64_t),
              (override));
  MOCK_METHOD(void,
              DisconnectActor,
              (const ActorID &, int64_t, bool, const rpc::ActorDeathCause &, bool),
              (override));
  MOCK_METHOD(void, CheckTimeoutTasks, (), (override));
  MOCK_METHOD(void, SetPreempted, (const ActorID &), (override));
  virtual ~MockActorTaskSubmitter() = default;
};

class MockGcsClient : public gcs::GcsClient {
 public:
  explicit MockGcsClient(gcs::GcsClientOptions options) : gcs::GcsClient(options) {}
  void Init(gcs::FakeActorInfoAccessor *accessor) { actor_accessor_.reset(accessor); }
};

class MockLocalityDataProvider : public LocalityDataProviderInterface {
 public:
  MockLocalityDataProvider() = default;
  explicit MockLocalityDataProvider(
      absl::flat_hash_map<ObjectID, LocalityData> locality_data)
      : locality_data_(std::move(locality_data)) {}
  std::optional<LocalityData> GetLocalityData(const ObjectID &object_id) const override {
    auto it = locality_data_.find(object_id);
    return (it != locality_data_.end()) ? std::optional(it->second) : std::nullopt;
  }
  ~MockLocalityDataProvider() override = default;
  absl::flat_hash_map<ObjectID, LocalityData> locality_data_;
};

// Base test fixture
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
            [](const NodeID &) { return true; },
            fake_owned_object_count_gauge_,
            fake_owned_object_size_gauge_,
            /*lineage_pinning_enabled=*/true)) {
    gcs_client_->Init(actor_info_accessor_);
  }

  void SetUp() override {
    actor_manager_ = std::make_unique<ActorManager>(
        gcs_client_, *mock_task_submitter_, *reference_counter_);
    pool_manager_ = std::make_unique<ActorPoolManager>(
        *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  }

  void TearDown() override {
    pool_manager_.reset();
    actor_manager_.reset();
    mock_task_manager_.reset();
  }

  ActorPoolID CreateTestPool(int32_t max_retry_attempts = 3) {
    ActorPoolConfig config;
    config.max_retry_attempts = max_retry_attempts;
    config.retry_backoff_ms = 100;
    config.retry_on_system_errors = true;
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
  std::unique_ptr<ActorPoolManager> pool_manager_;
};

// Locality test fixture — inherits base, defers pool_manager_ creation to each test.
class ActorPoolManagerLocalityTest : public ActorPoolManagerTest {
 protected:
  void SetUp() override {
    actor_manager_ = std::make_unique<ActorManager>(
        gcs_client_, *mock_task_submitter_, *reference_counter_);
  }

  ActorPoolID CreateTestPool(int32_t max_tasks_in_flight = 2) {
    ActorPoolConfig config;
    config.max_retry_attempts = 3;
    config.retry_backoff_ms = 100;
    config.retry_on_system_errors = true;
    config.max_tasks_in_flight_per_actor = max_tasks_in_flight;
    return pool_manager_->RegisterPool(config);
  }

  std::unique_ptr<MockLocalityDataProvider> locality_provider_;
};

// =========================================================================
// Pool Registration and Lifecycle
// =========================================================================

TEST_F(ActorPoolManagerTest, RegisterAndUnregisterPool) {
  // Multiple pools have unique, valid IDs
  auto pool_id1 = CreateTestPool();
  auto pool_id2 = CreateTestPool();
  auto pool_id3 = CreateTestPool();

  EXPECT_FALSE(pool_id1.IsNil());
  EXPECT_NE(pool_id1, pool_id2);
  EXPECT_NE(pool_id2, pool_id3);
  EXPECT_NE(pool_id1, pool_id3);
  EXPECT_TRUE(pool_manager_->HasPool(pool_id1));
  EXPECT_TRUE(pool_manager_->HasPool(pool_id2));
  EXPECT_TRUE(pool_manager_->HasPool(pool_id3));

  // Unregister removes pool and actor mappings
  auto actor_id = CreateActorID();
  pool_manager_->AddActorToPool(pool_id1, actor_id, NodeID::FromRandom());
  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id1).size(), 1);

  pool_manager_->UnregisterPool(pool_id1);
  EXPECT_FALSE(pool_manager_->HasPool(pool_id1));
  EXPECT_TRUE(pool_manager_->GetPoolActors(pool_id1).empty());

  // Unregistering non-existent pool is safe
  pool_manager_->UnregisterPool(ActorPoolID::FromRandom());
}

TEST_F(ActorPoolManagerTest, RegisterPoolWithInitialActors) {
  ActorPoolConfig config;
  config.max_retry_attempts = 3;

  std::vector<ActorID> initial_actors = {
      CreateActorID(), CreateActorID(), CreateActorID()};
  auto pool_id = pool_manager_->RegisterPool(config, initial_actors);

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 3);
  for (const auto &actor_id : initial_actors) {
    EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id) != actors.end());
  }
}

// =========================================================================
// Actor Management
// =========================================================================

TEST_F(ActorPoolManagerTest, AddAndRemoveActors) {
  auto pool_id = CreateTestPool();
  auto actor_id1 = CreateActorID();
  auto actor_id2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor_id1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor_id2, NodeID::FromRandom());

  auto actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 2);
  EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id1) != actors.end());
  EXPECT_TRUE(std::find(actors.begin(), actors.end(), actor_id2) != actors.end());

  // Adding same actor twice is idempotent
  pool_manager_->AddActorToPool(pool_id, actor_id1, NodeID::FromRandom());
  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id).size(), 2);

  // Remove one actor
  pool_manager_->RemoveActorFromPool(pool_id, actor_id1);
  actors = pool_manager_->GetPoolActors(pool_id);
  EXPECT_EQ(actors.size(), 1);
  EXPECT_EQ(actors[0], actor_id2);

  // Removing non-existent actor is safe
  pool_manager_->RemoveActorFromPool(pool_id, CreateActorID());
  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id).size(), 1);
}

TEST_F(ActorPoolManagerTest, MultiplePoolsWithDifferentActors) {
  auto pool_id1 = CreateTestPool();
  auto pool_id2 = CreateTestPool();

  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id1, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id1, actor2, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id2, actor3, NodeID::FromRandom());

  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id1).size(), 2);
  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id2).size(), 1);
  EXPECT_EQ(pool_manager_->GetPoolActors(pool_id2)[0], actor3);
}

// =========================================================================
// Non-Existent Pool Behavior
// =========================================================================

TEST_F(ActorPoolManagerTest, NonExistentPoolReturnsSafeDefaults) {
  auto fake_pool_id = ActorPoolID::FromRandom();

  EXPECT_FALSE(pool_manager_->HasPool(fake_pool_id));
  EXPECT_TRUE(pool_manager_->GetPoolActors(fake_pool_id).empty());
  EXPECT_TRUE(pool_manager_->SelectActorForTask(fake_pool_id).IsNil());
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(fake_pool_id), 0);
  EXPECT_EQ(pool_manager_->GetNumActiveActors(fake_pool_id), 0);

  auto stats = pool_manager_->GetPoolStats(fake_pool_id);
  EXPECT_EQ(stats.total_tasks_submitted, 0);
  EXPECT_EQ(stats.total_tasks_failed, 0);
  EXPECT_EQ(stats.num_actors, 0);
  EXPECT_EQ(stats.backlog_size, 0);

  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;
  EXPECT_TRUE(
      pool_manager_->SubmitTaskToPool(fake_pool_id, function, std::move(args), options)
          .empty());
}

// =========================================================================
// Pool Stats
// =========================================================================

TEST_F(ActorPoolManagerTest, PoolStatsReflectState) {
  auto pool_id = CreateTestPool();

  // Empty pool
  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.total_tasks_submitted, 0);
  EXPECT_EQ(stats.total_tasks_failed, 0);
  EXPECT_EQ(stats.total_tasks_retried, 0);
  EXPECT_EQ(stats.num_actors, 0);
  EXPECT_EQ(stats.backlog_size, 0);
  EXPECT_EQ(stats.total_in_flight, 0);

  // After adding actors
  pool_manager_->AddActorToPool(pool_id, CreateActorID(), NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, CreateActorID(), NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, CreateActorID(), NodeID::FromRandom());

  stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.num_actors, 3);
  EXPECT_EQ(stats.total_in_flight, 0);
  EXPECT_EQ(stats.backlog_size, 0);
}

// =========================================================================
// Task Submission
// =========================================================================

TEST_F(ActorPoolManagerTest, SubmitTaskWithNoActorsQueuesWork) {
  auto pool_id = CreateTestPool();

  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;
  auto refs =
      pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);

  EXPECT_TRUE(refs.empty());
  EXPECT_EQ(pool_manager_->GetPoolStats(pool_id).backlog_size, 1);
}

TEST_F(ActorPoolManagerTest, MultipleTasksQueueWhenNoActors) {
  auto pool_id = CreateTestPool();

  for (int i = 0; i < 5; i++) {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 5);
  EXPECT_EQ(stats.total_in_flight, 0);
}

// =========================================================================
// Actor Selection
// =========================================================================

TEST_F(ActorPoolManagerTest, SelectActorForTask) {
  auto pool_id = CreateTestPool();

  // Empty pool returns Nil
  EXPECT_TRUE(pool_manager_->SelectActorForTask(pool_id).IsNil());

  // With actors, returns one of them
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_FALSE(selected.IsNil());
  EXPECT_TRUE(selected == actor1 || selected == actor2);
}

// =========================================================================
// Locality-Aware Ranking
// =========================================================================

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
  pool_manager_->locality_data_provider_ = locality_provider_.get();

  auto pool_id = CreateTestPool();
  auto local_actor = CreateActorID();
  auto remote_actor = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, local_actor, local_node);
  pool_manager_->AddActorToPool(pool_id, remote_actor, remote_node);

  EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {obj1}), local_actor);
}

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

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 1;
  }

  EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {obj1}), actor2);
}

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

  EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {obj1, obj2}), actor_large);
}

// Merges three fallback scenarios: unknown objects, empty arg_ids, null provider.
TEST_F(ActorPoolManagerLocalityTest, FallsBackToLoadWithoutLocalityData) {
  NodeID node1 = NodeID::FromRandom();
  NodeID node2 = NodeID::FromRandom();

  // Helper: create pool with two actors where actor1 is more loaded.
  auto setup_pool_with_load = [&]() -> std::pair<ActorPoolID, ActorID> {
    auto pool_id = CreateTestPool();
    auto actor1 = CreateActorID();
    auto actor2 = CreateActorID();
    pool_manager_->AddActorToPool(pool_id, actor1, node1);
    pool_manager_->AddActorToPool(pool_id, actor2, node2);
    {
      absl::MutexLock lock(&pool_manager_->mu_);
      pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 3;
    }
    return {pool_id, actor2};
  };

  // Case 1: Provider has no data for the object — falls back to load
  locality_provider_ = std::make_unique<MockLocalityDataProvider>();
  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  pool_manager_->locality_data_provider_ = locality_provider_.get();
  {
    auto [pool_id, expected] = setup_pool_with_load();
    EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {ObjectID::FromRandom()}),
              expected);
  }

  // Case 2: Empty arg_ids — falls back to load
  {
    auto [pool_id, expected] = setup_pool_with_load();
    EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {}), expected);
  }

  // Case 3: Null locality provider — falls back to load
  pool_manager_ = std::make_unique<ActorPoolManager>(
      *actor_manager_, *mock_task_submitter_, *mock_task_manager_);
  {
    auto [pool_id, expected] = setup_pool_with_load();
    EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id, {ObjectID::FromRandom()}),
              expected);
  }
}

// =========================================================================
// Backpressure
// =========================================================================

TEST_F(ActorPoolManagerTest, GetOccupiedTaskSlots) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 0);

  // Simulate in-flight tasks
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 3;
    pool_manager_->pools_[pool_id].actor_states[actor2].num_tasks_in_flight = 2;
  }
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 5);

  // Remove actors and submit to force queueing — verify backlog counted
  pool_manager_->RemoveActorFromPool(pool_id, actor1);
  pool_manager_->RemoveActorFromPool(pool_id, actor2);
  {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }
  EXPECT_EQ(pool_manager_->GetOccupiedTaskSlots(pool_id), 1);
}

TEST_F(ActorPoolManagerTest, GetNumActiveActors) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();
  auto actor3 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor3, NodeID::FromRandom());

  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 0);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 2;
    pool_manager_->pools_[pool_id].actor_states[actor3].num_tasks_in_flight = 1;
  }
  EXPECT_EQ(pool_manager_->GetNumActiveActors(pool_id), 2);
}

// =========================================================================
// Actor Death/Restart Recovery
// =========================================================================

TEST_F(ActorPoolManagerTest, OnTaskFailedMarksActorDead) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].num_tasks_in_flight = 1;
  }

  // Create a work item so OnTaskFailed can find it for retry
  TaskID work_item_id = TaskID::FromRandom(JobID());
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    PoolWorkItem work_item;
    work_item.work_item_id = work_item_id;
    work_item.attempt_number = 0;
    pool_manager_->work_items_[work_item_id] = std::move(work_item);
  }

  rpc::RayErrorInfo error_info;
  error_info.set_error_type(rpc::ErrorType::ACTOR_DIED);
  error_info.set_error_message("Actor died");

  pool_manager_->OnPoolTaskComplete(pool_id,
                                    work_item_id,
                                    TaskID::FromRandom(JobID()),
                                    actor1,
                                    Status::IOError("actor died"),
                                    &error_info);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_FALSE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor2].is_alive);
  }

  EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id), actor2);
}

TEST_F(ActorPoolManagerTest, OnActorAliveDrainsQueue) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());

  // Mark actor dead
  {
    absl::MutexLock lock(&pool_manager_->mu_);
    pool_manager_->pools_[pool_id].actor_states[actor1].is_alive = false;
  }

  // Submit — should queue since no alive actors
  RayFunction function;
  std::vector<std::unique_ptr<TaskArg>> args;
  TaskOptions options;
  pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  EXPECT_EQ(pool_manager_->GetPoolStats(pool_id).backlog_size, 1);

  // Actor comes alive — queue should drain
  pool_manager_->OnActorAlive(actor1, NodeID::FromRandom());

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
  }

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.total_in_flight, 1);
  EXPECT_EQ(stats.backlog_size, 0);
}

TEST_F(ActorPoolManagerTest, OnActorDeadMarksActorNotAlive) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();
  auto actor2 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->AddActorToPool(pool_id, actor2, NodeID::FromRandom());

  pool_manager_->OnActorDead(actor1);

  {
    absl::MutexLock lock(&pool_manager_->mu_);
    EXPECT_FALSE(pool_manager_->pools_[pool_id].actor_states[actor1].is_alive);
    EXPECT_TRUE(pool_manager_->pools_[pool_id].actor_states[actor2].is_alive);
  }

  EXPECT_EQ(pool_manager_->SelectActorForTask(pool_id), actor2);
}

TEST_F(ActorPoolManagerTest, NonPoolActorCallbacksAreSafe) {
  auto actor_id = CreateActorID();

  // Should not crash for actors not in any pool
  pool_manager_->OnActorDead(actor_id);
  pool_manager_->OnActorAlive(actor_id, NodeID::FromRandom());
}

TEST_F(ActorPoolManagerTest, FullDeathRestartRecoveryCycle) {
  auto pool_id = CreateTestPool();
  auto actor1 = CreateActorID();

  pool_manager_->AddActorToPool(pool_id, actor1, NodeID::FromRandom());
  pool_manager_->OnActorDead(actor1);

  // Submit tasks — all should queue (actor dead)
  for (int i = 0; i < 3; i++) {
    RayFunction function;
    std::vector<std::unique_ptr<TaskArg>> args;
    TaskOptions options;
    pool_manager_->SubmitTaskToPool(pool_id, function, std::move(args), options);
  }

  auto stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 3);
  EXPECT_EQ(stats.total_in_flight, 0);

  // Actor restarts — all queued work drained
  pool_manager_->OnActorAlive(actor1, NodeID::FromRandom());

  stats = pool_manager_->GetPoolStats(pool_id);
  EXPECT_EQ(stats.backlog_size, 0);
  EXPECT_EQ(stats.total_in_flight, 3);
}

}  // namespace core
}  // namespace ray
