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

#include "ray/core_worker/actor_management/actor_creator.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "ray/common/test_utils.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

namespace ray {
namespace core {

class ActorCreatorTest : public ::testing::Test {
 public:
  ActorCreatorTest() {}
  void SetUp() override {
    gcs_client = std::make_shared<ray::gcs::MockGcsClient>();
    actor_creator = std::make_unique<ActorCreator>(gcs_client->Actors());
  }
  TaskSpecification GetTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    rpc::ActorCreationTaskSpec actor_creation_task_spec;
    actor_creation_task_spec.set_actor_id(actor_id.Binary());
    task_spec.mutable_actor_creation_task_spec()->CopyFrom(actor_creation_task_spec);
    return TaskSpecification(task_spec);
  }
  std::shared_ptr<ray::gcs::MockGcsClient> gcs_client;
  std::unique_ptr<ActorCreator> actor_creator;
};

TEST_F(ActorCreatorTest, IsRegister) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  auto task_spec = GetTaskSpec(actor_id);
  actor_creator->AsyncRegisterActor(task_spec, nullptr);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  gcs_client->mock_actor_accessor->async_register_actor_callback_(Status::OK());
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
}

TEST_F(ActorCreatorTest, AsyncWaitForFinish) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto task_spec = GetTaskSpec(actor_id);
  int count = 0;
  auto per_finish_cb = [&count](Status status) {
    ASSERT_TRUE(status.ok());
    count++;
  };
  actor_creator->AsyncRegisterActor(task_spec, per_finish_cb);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  for (int i = 0; i < 10; ++i) {
    actor_creator->AsyncWaitForActorRegisterFinish(actor_id, per_finish_cb);
  }
  gcs_client->mock_actor_accessor->async_register_actor_callback_(Status::OK());
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  ASSERT_EQ(11, count);
}

TEST_F(ActorCreatorTest, AsyncRegisterActorBatch) {
  auto actor_id1 = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto actor_id2 = ActorID::FromHex("f4ce02420592ca68c1738a0d02000000");
  auto task_spec1 = GetTaskSpec(actor_id1);
  auto task_spec2 = GetTaskSpec(actor_id2);

  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id1));
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id2));

  int batch_cb_count = 0;
  actor_creator->AsyncRegisterActorBatch({task_spec1, task_spec2},
                                         [&batch_cb_count](Status status) {
                                           ASSERT_TRUE(status.ok());
                                           batch_cb_count++;
                                         });

  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id1));
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id2));

  ASSERT_TRUE(gcs_client->mock_actor_accessor->async_register_actor_batch_callback_ !=
              nullptr);
  gcs_client->mock_actor_accessor->async_register_actor_batch_callback_(Status::OK());

  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id1));
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id2));
  ASSERT_EQ(1, batch_cb_count);
}

TEST_F(ActorCreatorTest, AsyncRegisterActorBatchEmpty) {
  int batch_cb_count = 0;
  actor_creator->AsyncRegisterActorBatch({}, [&batch_cb_count](Status status) {
    ASSERT_TRUE(status.ok());
    batch_cb_count++;
  });
  ASSERT_EQ(1, batch_cb_count);
}

TEST_F(ActorCreatorTest, AsyncRegisterActorBatchWithWaiters) {
  auto actor_id1 = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto actor_id2 = ActorID::FromHex("f4ce02420592ca68c1738a0d02000000");
  auto task_spec1 = GetTaskSpec(actor_id1);
  auto task_spec2 = GetTaskSpec(actor_id2);

  int batch_cb_count = 0;
  int waiter1_cb_count = 0;
  int waiter2_cb_count = 0;

  actor_creator->AsyncRegisterActorBatch({task_spec1, task_spec2},
                                         [&batch_cb_count](Status status) {
                                           ASSERT_TRUE(status.ok());
                                           batch_cb_count++;
                                         });

  actor_creator->AsyncWaitForActorRegisterFinish(actor_id1,
                                                 [&waiter1_cb_count](Status status) {
                                                   ASSERT_TRUE(status.ok());
                                                   waiter1_cb_count++;
                                                 });
  actor_creator->AsyncWaitForActorRegisterFinish(actor_id2,
                                                 [&waiter2_cb_count](Status status) {
                                                   ASSERT_TRUE(status.ok());
                                                   waiter2_cb_count++;
                                                 });

  ASSERT_TRUE(gcs_client->mock_actor_accessor->async_register_actor_batch_callback_ !=
              nullptr);
  gcs_client->mock_actor_accessor->async_register_actor_batch_callback_(Status::OK());

  ASSERT_EQ(1, batch_cb_count);
  ASSERT_EQ(1, waiter1_cb_count);
  ASSERT_EQ(1, waiter2_cb_count);
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id1));
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id2));
}

TEST_F(ActorCreatorTest, AsyncRegisterActorBatchFailure) {
  auto actor_id1 = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto task_spec1 = GetTaskSpec(actor_id1);

  int batch_cb_count = 0;
  int waiter_cb_count = 0;

  actor_creator->AsyncRegisterActorBatch({task_spec1}, [&batch_cb_count](Status status) {
    ASSERT_TRUE(status.IsIOError());
    batch_cb_count++;
  });

  actor_creator->AsyncWaitForActorRegisterFinish(actor_id1,
                                                 [&waiter_cb_count](Status status) {
                                                   ASSERT_TRUE(status.IsIOError());
                                                   waiter_cb_count++;
                                                 });

  ASSERT_TRUE(gcs_client->mock_actor_accessor->async_register_actor_batch_callback_ !=
              nullptr);
  gcs_client->mock_actor_accessor->async_register_actor_batch_callback_(
      Status::IOError("GCS error"));

  ASSERT_EQ(1, batch_cb_count);
  ASSERT_EQ(1, waiter_cb_count);
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id1));
}

}  // namespace core
}  // namespace ray
