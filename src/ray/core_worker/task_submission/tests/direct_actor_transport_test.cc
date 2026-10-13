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

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "ray/core_worker/actor_management/actor_creator.h"
#include "ray/core_worker/fake_task_manager_interface.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_submission/actor_task_submitter.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/util/clock.h"

namespace ray {
namespace core {

class DirectTaskTransportTest : public ::testing::Test {
 public:
  DirectTaskTransportTest()
      : io_work(io_context.get_executor()), store_io_context("DirectTaskTransportTest") {}

  void SetUp() override {
    gcs_client = std::make_shared<ray::gcs::FakeGcsClient>();
    actor_creator = std::make_unique<ActorCreator>(gcs_client->Actors());

    task_manager = std::make_shared<FakeTaskManagerInterface>();
    client_pool = std::make_shared<rpc::CoreWorkerClientPool>(
        [&](const rpc::Address &) { return nullptr; });
    raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
        [](const rpc::Address &) -> std::shared_ptr<RayletClientInterface> {
          return nullptr;
        });
    memory_store =
        std::make_unique<CoreWorkerMemoryStore>(store_io_context.GetIoService(), clock);
    publisher = std::make_unique<pubsub::FakePublisher>();
    subscriber = std::make_unique<pubsub::FakeSubscriber>();
    reference_counter = std::make_shared<ReferenceCounter>(
        rpc::Address(),
        publisher.get(),
        subscriber.get(),
        /*is_node_dead=*/[](const NodeID &) { return false; },
        /*free_object_on_nodes_async=*/
        [](const ObjectID &, const absl::flat_hash_set<NodeID> &) {},
        fake_owned_object_count_gauge,
        fake_owned_object_size_gauge,
        /*lineage_pinning_enabled=*/false);
    actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
        *client_pool,
        *raylet_client_pool,
        gcs_client,
        *memory_store,
        *task_manager,
        *actor_creator,
        [](const ObjectID &object_id) { return std::nullopt; },
        nullptr,
        io_context,
        reference_counter,
        clock);
  }

  TaskSpecification GetActorTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_type(rpc::TaskType::ACTOR_TASK);
    task_spec.mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
    task_spec.set_task_id(
        TaskID::ForActorTask(JobID::FromInt(10), TaskID::Nil(), 0, actor_id).Binary());
    return TaskSpecification(task_spec);
  }

  TaskSpecification GetActorCreationTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_task_id(TaskID::ForActorCreationTask(actor_id).Binary());
    task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    rpc::ActorCreationTaskSpec actor_creation_task_spec;
    actor_creation_task_spec.set_actor_id(actor_id.Binary());
    task_spec.mutable_actor_creation_task_spec()->CopyFrom(actor_creation_task_spec);
    return TaskSpecification(task_spec);
  }

 protected:
  bool CheckSubmitTask(TaskSpecification task) {
    actor_task_submitter->SubmitTask(task);
    return 1 == io_context.poll_one();
  }

 protected:
  instrumented_io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work;
  InstrumentedIOContextWithThread store_io_context;
  Clock clock;
  std::unique_ptr<ActorTaskSubmitter> actor_task_submitter;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool;
  std::unique_ptr<CoreWorkerMemoryStore> memory_store;
  std::shared_ptr<FakeTaskManagerInterface> task_manager;
  std::unique_ptr<ActorCreator> actor_creator;
  std::shared_ptr<ray::gcs::FakeGcsClient> gcs_client;
  std::unique_ptr<pubsub::FakePublisher> publisher;
  std::unique_ptr<pubsub::FakeSubscriber> subscriber;
  ray::observability::FakeGauge fake_owned_object_count_gauge;
  ray::observability::FakeGauge fake_owned_object_size_gauge;
  std::shared_ptr<ReferenceCounterInterface> reference_counter;
};

TEST_F(DirectTaskTransportTest, ActorCreationOk) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  actor_task_submitter->SubmitActorCreationTask(creation_task_spec);
  gcs_client->fake_actor_accessor->async_create_actor_callback_(Status::OK(),
                                                                rpc::CreateActorReply());
  ASSERT_EQ(task_manager->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager->complete_pending_task_calls[0], creation_task_spec.TaskId());
}

TEST_F(DirectTaskTransportTest, ActorCreationFail) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  actor_task_submitter->SubmitActorCreationTask(creation_task_spec);
  gcs_client->fake_actor_accessor->async_create_actor_callback_(Status::IOError(""),
                                                                rpc::CreateActorReply());
  EXPECT_EQ(task_manager->complete_pending_task_calls.size(), 0);
  ASSERT_EQ(task_manager->fail_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager->fail_pending_task_calls[0], creation_task_spec.TaskId());
  EXPECT_EQ(task_manager->fail_pending_task_error_types[0],
            rpc::ErrorType::ACTOR_CREATION_FAILED);
}

TEST_F(DirectTaskTransportTest, ActorRegisterFailure) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_TRUE(ObjectID::IsActorID(ObjectID::ForActorHandle(actor_id)));
  ASSERT_EQ(actor_id, ObjectID::ToActorID(ObjectID::ForActorHandle(actor_id)));
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  auto task_spec = GetActorTaskSpec(actor_id);
  auto task_arg = task_spec.GetMutableMessage().add_args();
  auto inline_obj_ref = task_arg->add_nested_inlined_refs();
  inline_obj_ref->set_object_id(ObjectID::ForActorHandle(actor_id).Binary());
  actor_creator->AsyncRegisterActor(creation_task_spec, nullptr);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  actor_task_submitter->AddActorQueueIfNotExists(actor_id,
                                                 -1,
                                                 /*allow_out_of_order_execution*/ false,
                                                 /*fail_if_actor_unreachable*/ true,
                                                 /*owned*/ false);
  ASSERT_TRUE(CheckSubmitTask(task_spec));
  gcs_client->fake_actor_accessor->async_register_actor_callback_(Status::IOError(""));
  ASSERT_EQ(task_manager->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager->fail_or_retry_pending_task_calls[0], task_spec.TaskId());
  EXPECT_EQ(task_manager->fail_or_retry_pending_task_error_types[0],
            rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED);
}

TEST_F(DirectTaskTransportTest, ActorRegisterOk) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_TRUE(ObjectID::IsActorID(ObjectID::ForActorHandle(actor_id)));
  ASSERT_EQ(actor_id, ObjectID::ToActorID(ObjectID::ForActorHandle(actor_id)));
  auto creation_task_spec = GetActorCreationTaskSpec(actor_id);
  auto task_spec = GetActorTaskSpec(actor_id);
  auto task_arg = task_spec.GetMutableMessage().add_args();
  auto inline_obj_ref = task_arg->add_nested_inlined_refs();
  inline_obj_ref->set_object_id(ObjectID::ForActorHandle(actor_id).Binary());
  actor_creator->AsyncRegisterActor(creation_task_spec, nullptr);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  actor_task_submitter->AddActorQueueIfNotExists(actor_id,
                                                 -1,
                                                 /*allow_out_of_order_execution*/ false,
                                                 /*fail_if_actor_unreachable*/ true,
                                                 /*owned*/ false);
  ASSERT_TRUE(CheckSubmitTask(task_spec));
  gcs_client->fake_actor_accessor->async_register_actor_callback_(Status::OK());
  EXPECT_EQ(task_manager->fail_or_retry_pending_task_calls.size(), 0);
}

}  // namespace core
}  // namespace ray
