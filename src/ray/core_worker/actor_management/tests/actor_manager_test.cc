// Copyright 2017 The Ray Authors.
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

#include "ray/core_worker/actor_management/actor_manager.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/gcs_rpc_client/accessors/actor_info_accessor_interface.h"
#include "ray/gcs_rpc_client/accessors/fake_actor_info_accessor.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"

namespace ray {
namespace core {

// Hand-written fake that records how many times ConnectActor and
// DisconnectActor are invoked so tests can assert on the counts.
class FakeActorTaskSubmitter : public ActorTaskSubmitterInterface {
 public:
  FakeActorTaskSubmitter() = default;

  void AddActorQueueIfNotExists(const ActorID &actor_id,
                                int32_t max_pending_calls,
                                bool allow_out_of_order_execution,
                                bool fail_if_actor_unreachable,
                                bool owned) override {
    add_actor_queue_count++;
  }

  void ConnectActor(const ActorID &actor_id,
                    const rpc::Address &address,
                    int64_t num_restarts) override {
    connect_actor_count++;
  }

  void DisconnectActor(const ActorID &actor_id,
                       int64_t num_restarts,
                       bool dead,
                       const rpc::ActorDeathCause &death_cause,
                       bool is_restartable) override {
    disconnect_actor_count++;
  }

  void CheckTimeoutTasks() override {}

  void SetPreempted(const ActorID &actor_id) override {}

  ~FakeActorTaskSubmitter() override = default;

  int add_actor_queue_count = 0;
  int connect_actor_count = 0;
  int disconnect_actor_count = 0;
};

class ActorManagerTest : public ::testing::Test {
 public:
  ActorManagerTest()
      : gcs_client_(std::make_shared<gcs::FakeGcsClient>()),
        actor_info_accessor_(gcs_client_->fake_actor_accessor),
        actor_task_submitter_(new FakeActorTaskSubmitter()),
        publisher_(std::make_unique<pubsub::FakePublisher>()),
        subscriber_(std::make_unique<pubsub::FakeSubscriber>()),
        fake_owned_object_count_gauge_(),
        fake_owned_object_size_gauge_(),
        reference_counter_(std::make_unique<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            [](const NodeID &node_id) { return true; },
            [](const ObjectID &, const absl::flat_hash_set<NodeID> &, int64_t, bool) {},
            fake_owned_object_count_gauge_,
            fake_owned_object_size_gauge_,
            /*lineage_pinning_enabled=*/true)) {}

  ~ActorManagerTest() {}

  void SetUp() {
    actor_manager_ = std::make_shared<ActorManager>(
        gcs_client_, *actor_task_submitter_, *reference_counter_);
  }

  void TearDown() { actor_manager_.reset(); }

  ActorID AddActorHandle(const std::string &ray_namespace = "",
                         const std::string &actor_name = "") const {
    JobID job_id = JobID::FromInt(1);
    const TaskID task_id = TaskID::ForDriverTask(job_id);
    ActorID actor_id = ActorID::Of(job_id, task_id, 1);
    const auto caller_address = rpc::Address();
    const auto call_site = "";
    RayFunction function(Language::PYTHON,
                         FunctionDescriptorBuilder::BuildPython("", "", "", ""));

    auto actor_handle = absl::make_unique<ActorHandle>(actor_id,
                                                       TaskID::Nil(),
                                                       rpc::Address(),
                                                       job_id,
                                                       ObjectID::FromRandom(),
                                                       function.GetLanguage(),
                                                       function.GetFunctionDescriptor(),
                                                       "",
                                                       0,
                                                       actor_name,
                                                       ray_namespace,
                                                       -1,
                                                       false);

    actor_manager_->EmplaceNewActorHandle(std::move(actor_handle),
                                          call_site,
                                          caller_address,
                                          /*owned*/ true);
    actor_manager_->SubscribeActorState(actor_id);
    return actor_id;
  }

  std::shared_ptr<gcs::FakeGcsClient> gcs_client_;
  gcs::FakeActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<FakeActorTaskSubmitter> actor_task_submitter_;
  std::unique_ptr<pubsub::FakePublisher> publisher_;
  std::unique_ptr<pubsub::FakeSubscriber> subscriber_;
  ray::observability::FakeGauge fake_owned_object_count_gauge_;
  ray::observability::FakeGauge fake_owned_object_size_gauge_;
  std::unique_ptr<ReferenceCounterInterface> reference_counter_;
  std::shared_ptr<ActorManager> actor_manager_;
};

TEST_F(ActorManagerTest, TestAddAndGetActorHandleEndToEnd) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  RayFunction function(Language::PYTHON,
                       FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  auto actor_handle = absl::make_unique<ActorHandle>(actor_id,
                                                     TaskID::Nil(),
                                                     rpc::Address(),
                                                     job_id,
                                                     ObjectID::FromRandom(),
                                                     function.GetLanguage(),
                                                     function.GetFunctionDescriptor(),
                                                     "",
                                                     0,
                                                     "",
                                                     "",
                                                     -1,
                                                     false);

  // Add an actor handle.
  ASSERT_TRUE(actor_manager_->EmplaceNewActorHandle(
      std::move(actor_handle), call_site, caller_address, true))
      << "Emplacing a new actor handle with unseen actor id should add a new actor "
         "handle, "
         "but got actor handle already exists.";
  actor_manager_->SubscribeActorState(actor_id);

  // Make sure the subscription request is sent to GCS.
  ASSERT_TRUE(actor_info_accessor_->CheckSubscriptionRequested(actor_id));
  ASSERT_TRUE(actor_manager_->CheckActorHandleExists(actor_id));

  auto actor_handle2 = absl::make_unique<ActorHandle>(actor_id,
                                                      TaskID::Nil(),
                                                      rpc::Address(),
                                                      job_id,
                                                      ObjectID::FromRandom(),
                                                      function.GetLanguage(),
                                                      function.GetFunctionDescriptor(),
                                                      "",
                                                      0,
                                                      "",
                                                      "",
                                                      -1,
                                                      false);
  // Make sure the same actor id adding will return false (repeated emplace should not add
  // a new actor handle)
  ASSERT_FALSE(actor_manager_->EmplaceNewActorHandle(
      std::move(actor_handle2), call_site, caller_address, true))
      << "Emplacing an actor handle with an existing actor id should return false, "
         "but the handle was added anyway.";
  actor_manager_->SubscribeActorState(actor_id);

  // Make sure we can get an actor handle correctly.
  const std::shared_ptr<ActorHandle> actor_handle_to_get =
      actor_manager_->GetActorHandle(actor_id);
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);

  // Check after the actor is created, if it is connected to an actor.
  actor_task_submitter_->connect_actor_count = 0;
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);
  ASSERT_EQ(actor_task_submitter_->connect_actor_count, 1);

  // Now actor state is updated to DEAD. Make sure it is disconnected.
  actor_task_submitter_->disconnect_actor_count = 0;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::DEAD);
  actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data);
  ASSERT_EQ(actor_task_submitter_->disconnect_actor_count, 1);
}

TEST_F(ActorManagerTest, TestCheckActorHandleDoesntExists) {
  JobID job_id = JobID::FromInt(2);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  ASSERT_FALSE(actor_manager_->CheckActorHandleExists(actor_id));
}

TEST_F(ActorManagerTest, RegisterActorHandles) {
  JobID job_id = JobID::FromInt(1);
  const TaskID task_id = TaskID::ForDriverTask(job_id);
  ActorID actor_id = ActorID::Of(job_id, task_id, 1);
  const auto caller_address = rpc::Address();
  const auto call_site = "";
  RayFunction function(Language::PYTHON,
                       FunctionDescriptorBuilder::BuildPython("", "", "", ""));
  auto actor_handle = absl::make_unique<ActorHandle>(actor_id,
                                                     TaskID::Nil(),
                                                     rpc::Address(),
                                                     job_id,
                                                     ObjectID::FromRandom(),
                                                     function.GetLanguage(),
                                                     function.GetFunctionDescriptor(),
                                                     "",
                                                     0,
                                                     "",
                                                     "",
                                                     -1,
                                                     false);
  ObjectID outer_object_id = ObjectID::Nil();

  // Since RegisterActor happens in a non-owner worker, we should
  // make sure it borrows an object.
  ActorID returned_actor_id = actor_manager_->RegisterActorHandle(std::move(actor_handle),
                                                                  outer_object_id,
                                                                  call_site,
                                                                  caller_address,
                                                                  /*add_local_ref=*/true);
  ASSERT_TRUE(returned_actor_id == actor_id);
  // Let's try to get the handle and make sure it works.
  const std::shared_ptr<ActorHandle> actor_handle_to_get =
      actor_manager_->GetActorHandle(actor_id);
  ASSERT_TRUE(actor_handle_to_get->GetActorID() == actor_id);
  ASSERT_TRUE(actor_handle_to_get->CreationJobID() == job_id);
}

TEST_F(ActorManagerTest, TestActorStateNotificationPending) {
  ActorID actor_id = AddActorHandle();
  // Nothing happens if state is pending.
  actor_task_submitter_->connect_actor_count = 0;
  actor_task_submitter_->disconnect_actor_count = 0;
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::PENDING_CREATION);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
  ASSERT_EQ(actor_task_submitter_->connect_actor_count, 0);
  ASSERT_EQ(actor_task_submitter_->disconnect_actor_count, 0);
}

TEST_F(ActorManagerTest, TestActorStateNotificationRestarting) {
  ActorID actor_id = AddActorHandle();
  // Should disconnect to an actor when actor is restarting.
  actor_task_submitter_->connect_actor_count = 0;
  actor_task_submitter_->disconnect_actor_count = 0;
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::RESTARTING);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
  ASSERT_EQ(actor_task_submitter_->connect_actor_count, 0);
  ASSERT_EQ(actor_task_submitter_->disconnect_actor_count, 1);
}

TEST_F(ActorManagerTest, TestActorStateNotificationDead) {
  ActorID actor_id = AddActorHandle();
  // Should disconnect to an actor when actor is dead.
  actor_task_submitter_->connect_actor_count = 0;
  actor_task_submitter_->disconnect_actor_count = 0;
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::DEAD);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
  ASSERT_EQ(actor_task_submitter_->connect_actor_count, 0);
  ASSERT_EQ(actor_task_submitter_->disconnect_actor_count, 1);
}

TEST_F(ActorManagerTest, TestActorStateNotificationAlive) {
  ActorID actor_id = AddActorHandle();
  // Should connect to an actor when actor is alive.
  actor_task_submitter_->connect_actor_count = 0;
  actor_task_submitter_->disconnect_actor_count = 0;
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  ASSERT_TRUE(
      actor_info_accessor_->ActorStateNotificationPublished(actor_id, actor_table_data));
  ASSERT_EQ(actor_task_submitter_->connect_actor_count, 1);
  ASSERT_EQ(actor_task_submitter_->disconnect_actor_count, 0);
}

///
/// Verify `SubscribeActorState` is idempotent
///
TEST_F(ActorManagerTest, TestActorStateIsOnlySubscribedOnce) {
  ActorID actor_id = AddActorHandle();
  // Make sure the AsyncSubscribe is invoked.
  ASSERT_EQ(actor_info_accessor_->actor_subscribed_times_[actor_id], 1);

  // Try subscribing again.
  actor_manager_->SubscribeActorState(actor_id);
  // Make sure the AsyncSubscribe won't be invoked anymore.
  ASSERT_EQ(actor_info_accessor_->actor_subscribed_times_[actor_id], 1);
}

TEST_F(ActorManagerTest, TestNamedActorIsKilledAfterSubscribeFinished) {
  std::string ray_namespace = "default_ray_namespace";
  std::string actor_name = "actor_name";
  ActorID actor_id = AddActorHandle(ray_namespace, actor_name);
  // Make sure the actor is valid.
  ASSERT_FALSE(actor_manager_->IsActorKilledOrOutOfScope(actor_id));
  // Make sure the finished callback is cached as it is not reached yet.
  ASSERT_TRUE(actor_info_accessor_->subscribe_finished_callback_map_.contains(actor_id));

  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  actor_table_data.set_ray_namespace(ray_namespace);
  actor_table_data.set_name(actor_name);
  // The callback for successful subscription reached.
  ASSERT_TRUE(actor_info_accessor_->ActorSubscribeFinished(actor_id, actor_table_data));
  // Make sure the finished callback is removed.
  ASSERT_FALSE(actor_info_accessor_->subscribe_finished_callback_map_.contains(actor_id));

  // Make sure the named actor will be put into `cached_actor_name_to_ids_`
  auto cached_actor_name = GenerateCachedActorName(ray_namespace, actor_name);
  ASSERT_TRUE(actor_manager_->GetCachedNamedActorID(cached_actor_name) == actor_id);

  // The actor is killed.
  actor_manager_->OnActorKilled(actor_id);
  // Make sure the actor is invalid.
  ASSERT_TRUE(actor_manager_->IsActorKilledOrOutOfScope(actor_id));

  // Make sure the named actor will not be deleted from `cached_actor_name_to_ids_`
  ASSERT_TRUE(actor_manager_->GetCachedNamedActorID(cached_actor_name).IsNil());
}

TEST_F(ActorManagerTest, TestNamedActorIsKilledBeforeSubscribeFinished) {
  std::string ray_namespace = "default_ray_namespace";
  std::string actor_name = "actor_name";
  ActorID actor_id = AddActorHandle(ray_namespace, actor_name);
  // Make sure the actor is valid.
  ASSERT_FALSE(actor_manager_->IsActorKilledOrOutOfScope(actor_id));
  // Make sure the finished callback is cached as it is not reached yet.
  ASSERT_TRUE(actor_info_accessor_->subscribe_finished_callback_map_.contains(actor_id));

  // The actor is killed.
  actor_manager_->OnActorKilled(actor_id);
  // Make sure the actor is invalid.
  ASSERT_TRUE(actor_manager_->IsActorKilledOrOutOfScope(actor_id));

  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_state(rpc::ActorTableData::ALIVE);
  actor_table_data.set_ray_namespace(ray_namespace);
  actor_table_data.set_name(actor_name);
  // The callback for successful subscription reached.
  ASSERT_TRUE(actor_info_accessor_->ActorSubscribeFinished(actor_id, actor_table_data));
  // Make sure the finished callback is removed.
  ASSERT_FALSE(actor_info_accessor_->subscribe_finished_callback_map_.contains(actor_id));

  // Make sure the named actor will not be put into `cached_actor_name_to_ids_`
  auto cached_actor_name = GenerateCachedActorName(ray_namespace, actor_name);
  ASSERT_TRUE(actor_manager_->GetCachedNamedActorID(cached_actor_name).IsNil());
}

}  // namespace core
}  // namespace ray
