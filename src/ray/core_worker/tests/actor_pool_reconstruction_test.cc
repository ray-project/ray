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

#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/gcs_client/accessors/actor_info_accessor.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/actor_management/actor_manager.h"
#include "ray/core_worker/actor_pool_manager.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"

namespace ray {
namespace core {
namespace {

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
};

class MockGcsClient : public gcs::GcsClient {
 public:
  explicit MockGcsClient(gcs::GcsClientOptions options) : gcs::GcsClient(options) {}
  void Init(gcs::FakeActorInfoAccessor *accessor) { actor_accessor_.reset(accessor); }
};

// Integration test fixture for pool-bound reconstruction.
// Wires ActorPoolManager with real ActorManager but mock submitter/task manager.
class PoolReconstructionTest : public ::testing::Test {
 protected:
  PoolReconstructionTest()
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
            fake_gauge1_,
            fake_gauge2_,
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
  }

  ActorID MakeActorID() {
    return ActorID::Of(JobID::FromInt(1), TaskID::FromRandom(JobID::FromInt(1)), 0);
  }

  ActorPoolID CreatePool(const std::vector<ActorID> &actors) {
    ActorPoolConfig config;
    config.max_retry_attempts = 3;
    config.retry_backoff_ms = 100;
    config.retry_on_system_errors = true;
    config.ordering_mode = PoolOrderingMode::UNORDERED;
    auto pool_id = pool_manager_->RegisterPool(config, actors);
    for (const auto &a : actors) {
      pool_manager_->AddActorToPool(pool_id, a, NodeID::FromRandom());
    }
    return pool_id;
  }

  gcs::GcsClientOptions gcs_options_;
  std::shared_ptr<MockGcsClient> gcs_client_;
  gcs::FakeActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<MockActorTaskSubmitter> mock_task_submitter_;
  std::unique_ptr<MockTaskManagerInterface> mock_task_manager_;
  std::unique_ptr<pubsub::FakePublisher> publisher_;
  std::unique_ptr<pubsub::FakeSubscriber> subscriber_;
  ray::observability::FakeGauge fake_gauge1_;
  ray::observability::FakeGauge fake_gauge2_;
  std::unique_ptr<ReferenceCounterInterface> reference_counter_;
  std::unique_ptr<ActorManager> actor_manager_;
  std::unique_ptr<ActorPoolManager> pool_manager_;
};

// Reconstruction should select a different actor when the original is removed.
TEST_F(PoolReconstructionTest, SelectsDifferentActorAfterRemoval) {
  auto actor1 = MakeActorID();
  auto actor2 = MakeActorID();
  auto pool_id = CreatePool({actor1, actor2});

  // Simulate actor1 dying: remove from pool.
  pool_manager_->RemoveActorFromPool(pool_id, actor1);

  // Selection should only return actor2.
  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_EQ(selected, actor2);

  // Selecting again should still return actor2 (only healthy actor).
  selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_EQ(selected, actor2);
}

// Two pools: simulate cascading reconstruction where Pool1's output feeds Pool2.
// When a Pool1 actor dies, reconstruction should redirect within Pool1.
// When a Pool2 actor dies, reconstruction should redirect within Pool2.
TEST_F(PoolReconstructionTest, CascadingReconstructionAcrossTwoPools) {
  // Pool1: 3 actors
  auto p1_a1 = MakeActorID();
  auto p1_a2 = MakeActorID();
  auto p1_a3 = MakeActorID();
  auto pool1 = CreatePool({p1_a1, p1_a2, p1_a3});

  // Pool2: 2 actors
  auto p2_a1 = MakeActorID();
  auto p2_a2 = MakeActorID();
  auto pool2 = CreatePool({p2_a1, p2_a2});

  // Simulate node failure killing p1_a1 and p2_a1 (co-located on same node).
  pool_manager_->RemoveActorFromPool(pool1, p1_a1);
  pool_manager_->RemoveActorFromPool(pool2, p2_a1);

  // Pool1 reconstruction redirects to surviving actors.
  auto sel1 = pool_manager_->SelectActorForTask(pool1);
  EXPECT_TRUE(sel1 == p1_a2 || sel1 == p1_a3);

  // Pool2 reconstruction redirects to the sole surviving actor.
  auto sel2 = pool_manager_->SelectActorForTask(pool2);
  EXPECT_EQ(sel2, p2_a2);

  // Pools are independent — pool1 selection doesn't affect pool2.
  EXPECT_TRUE(pool_manager_->HasPool(pool1));
  EXPECT_TRUE(pool_manager_->HasPool(pool2));
  EXPECT_EQ(pool_manager_->GetPoolActors(pool1).size(), 2);
  EXPECT_EQ(pool_manager_->GetPoolActors(pool2).size(), 1);
}

// When no actors are available, SelectActorForTask returns Nil.
TEST_F(PoolReconstructionTest, ReturnsNilWhenNoActorsAvailable) {
  auto actor1 = MakeActorID();
  auto pool_id = CreatePool({actor1});

  pool_manager_->RemoveActorFromPool(pool_id, actor1);

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_TRUE(selected.IsNil());
}

// When the pool itself is gone, SelectActorForTask returns Nil.
TEST_F(PoolReconstructionTest, ReturnsNilWhenPoolGone) {
  auto actor1 = MakeActorID();
  auto pool_id = CreatePool({actor1});

  pool_manager_->UnregisterPool(pool_id);

  auto selected = pool_manager_->SelectActorForTask(pool_id);
  EXPECT_TRUE(selected.IsNil());
}

}  // namespace
}  // namespace core
}  // namespace ray
