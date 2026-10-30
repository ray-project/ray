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

#include "ray/core_worker/task_submission/actor_task_submitter.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/actor_management/fake_actor_creator.h"
#include "ray/core_worker/fake_task_manager_interface.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/gcs_rpc_client/fake_gcs_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/raylet_rpc_client/raylet_client_pool.h"
#include "ray/util/clock.h"

namespace ray::core {

// Asserts that `actual` equals `expected` element-by-element on the recorded
// sequence numbers.
void ExpectSeqNosEq(const std::vector<int64_t> &actual,
                    const std::vector<int64_t> &expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(actual[i], expected[i]) << "mismatch at index " << i;
  }
}

// Returns whether `id` appears in the recorded-call vector `v`.
bool Contains(const std::vector<TaskID> &v, const TaskID &id) {
  return std::find(v.begin(), v.end(), id) != v.end();
}

rpc::ActorDeathCause CreateMockDeathCause() {
  ray::rpc::ActorDeathCause death_cause;
  death_cause.mutable_runtime_env_failed_context()->set_error_message("failed");
  return death_cause;
}

TaskSpecification CreateActorTaskHelper(ActorID actor_id,
                                        WorkerID caller_worker_id,
                                        int64_t counter,
                                        TaskID caller_id = TaskID::Nil()) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::FromRandom(actor_id.JobId()).Binary());
  task.GetMutableMessage().set_attempt_number(0);
  task.GetMutableMessage().set_caller_id(caller_id.Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_caller_address()->set_worker_id(
      caller_worker_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(counter);
  task.GetMutableMessage().set_num_returns(0);
  return task;
}

class FakeWorkerClient : public rpc::FakeCoreWorkerClient {
 public:
  const rpc::Address &Addr() const override { return addr; }

  void PushActorTask(std::unique_ptr<rpc::PushTaskRequest> request,
                     bool skip_queue,
                     rpc::ClientCallback<rpc::PushTaskReply> &&callback) override {
    received_seq_nos.push_back(request->sequence_number());
    callbacks.emplace(std::make_pair(TaskID::FromBinary(request->task_spec().task_id()),
                                     request->task_spec().attempt_number()),
                      callback);
  }

  bool ReplyPushTask(TaskAttempt task_attempt, Status status) {
    if (callbacks.size() == 0 || callbacks.find(task_attempt) == callbacks.end()) {
      return false;
    }
    auto &callback = callbacks[task_attempt];
    callback(status, rpc::PushTaskReply());
    callbacks.erase(task_attempt);
    return true;
  }

  rpc::Address addr;
  absl::flat_hash_map<TaskAttempt, rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  std::vector<int64_t> received_seq_nos;
  int64_t acked_seqno = 0;
};

class ActorTaskSubmitterTest : public ::testing::TestWithParam<bool> {
 public:
  ActorTaskSubmitterTest()
      : io_work(io_context.get_executor()),
        client_pool_(std::make_shared<rpc::CoreWorkerClientPool>(
            [&](const rpc::Address &addr) { return worker_client_; })),
        raylet_client_pool_(std::make_shared<rpc::RayletClientPool>(
            [](const rpc::Address &) -> std::shared_ptr<RayletClientInterface> {
              return nullptr;
            })),
        worker_client_(std::make_shared<FakeWorkerClient>()),
        store_(std::make_shared<CoreWorkerMemoryStore>(io_context, clock_)),
        task_manager_(std::make_shared<FakeTaskManagerInterface>()),
        fake_gcs_client_(std::make_shared<gcs::FakeGcsClient>()),
        publisher_(std::make_unique<pubsub::FakePublisher>()),
        subscriber_(std::make_unique<pubsub::FakeSubscriber>()),
        fake_owned_object_count_gauge_(),
        fake_owned_object_size_gauge_(),
        reference_counter_(std::make_shared<ReferenceCounter>(
            rpc::Address(),
            publisher_.get(),
            subscriber_.get(),
            /*is_node_dead=*/[](const NodeID &) { return false; },
            /*free_object_on_nodes_async=*/
            [](const ObjectID &, const absl::flat_hash_set<NodeID> &) {},
            fake_owned_object_count_gauge_,
            fake_owned_object_size_gauge_,
            /*lineage_pinning_enabled=*/false)),
        submitter_(
            *client_pool_,
            *raylet_client_pool_,
            fake_gcs_client_,
            *store_,
            *task_manager_,
            actor_creator_,
            [](const ObjectID &object_id) { return std::nullopt; },
            [this](const ActorID &actor_id, const std::string &, int64_t num_queued) {
              last_queue_warning_ = num_queued;
            },
            io_context,
            reference_counter_,
            clock_) {}

  void TearDown() override { io_context.stop(); }

  int64_t last_queue_warning_ = 0;
  FakeActorCreator actor_creator_;
  Clock clock_;
  instrumented_io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool_;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::shared_ptr<FakeWorkerClient> worker_client_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<FakeTaskManagerInterface> task_manager_;
  std::shared_ptr<gcs::FakeGcsClient> fake_gcs_client_;
  std::unique_ptr<pubsub::FakePublisher> publisher_;
  std::unique_ptr<pubsub::FakeSubscriber> subscriber_;
  ray::observability::FakeGauge fake_owned_object_count_gauge_;
  ray::observability::FakeGauge fake_owned_object_size_gauge_;
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;
  ActorTaskSubmitter submitter_;
};

TEST_P(ActorTaskSubmitterTest, TestSubmitTask) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);

  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  const size_t expected_completions = worker_client_->callbacks.size();
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK());
  worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK());
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), expected_completions);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 0);
  ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1});

  // Connect to the actor again.
  // Because the IP and port of address are not modified, it will skip directly and will
  // not reset `received_seq_nos`.
  submitter_.ConnectActor(actor_id, addr, 0);
  ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1});
}

TEST_P(ActorTaskSubmitterTest, TestQueueingWarning) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);

  for (int i = 0; i < 7500; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    submitter_.SubmitTask(task);
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_TRUE(worker_client_->ReplyPushTask(task.GetTaskAttempt(), Status::OK()));
  }
  ASSERT_EQ(last_queue_warning_, 0);

  for (int i = 7500; i < 15000; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    submitter_.SubmitTask(task);
    ASSERT_EQ(io_context.poll_one(), 1);
    /* no ack */
  }
  ASSERT_EQ(last_queue_warning_, 5000);

  for (int i = 15000; i < 35000; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    submitter_.SubmitTask(task);
    ASSERT_EQ(io_context.poll_one(), 1);
    /* no ack */
  }
  ASSERT_EQ(last_queue_warning_, 20000);
}

TEST_P(ActorTaskSubmitterTest, TestDependencies) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor with different arguments.
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  task1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj1.Binary());
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj2.Binary());
  reference_counter_->AddOwnedObject(
      obj1, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);
  reference_counter_->AddOwnedObject(
      obj2, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);

  // Neither task can be submitted yet because they are still waiting on
  // dependencies.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Put the dependencies in the store in the same order as task submission.
  auto data = GenerateRandomObject();

  // Each Put schedules a callback onto io_context, and let's run it.
  store_->Put(*data, obj1, reference_counter_->HasReference(obj1));
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  store_->Put(*data, obj2, reference_counter_->HasReference(obj2));
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1});
}

TEST_P(ActorTaskSubmitterTest, TestOutOfOrderDependencies) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor with different arguments.
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  task1.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj1.Binary());
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj2.Binary());
  reference_counter_->AddOwnedObject(
      obj1, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);
  reference_counter_->AddOwnedObject(
      obj2, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);

  // Neither task can be submitted yet because they are still waiting on
  // dependencies.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  if (allow_out_of_order_execution) {
    // Put the dependencies in the store in the opposite order of task
    // submission.
    auto data = GenerateRandomObject();
    // task2 is submitted first as we allow out of order execution.
    store_->Put(*data, obj2, reference_counter_->HasReference(obj2));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 1);
    ExpectSeqNosEq(worker_client_->received_seq_nos, {1});
    // then task1 is submitted
    store_->Put(*data, obj1, reference_counter_->HasReference(obj1));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 2);
    ExpectSeqNosEq(worker_client_->received_seq_nos, {1, 0});
  } else {
    // Put the dependencies in the store in the opposite order of task
    // submission.
    auto data = GenerateRandomObject();
    store_->Put(*data, obj2, reference_counter_->HasReference(obj2));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 0);
    store_->Put(*data, obj1, reference_counter_->HasReference(obj1));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 2);
    ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1});
  }
}

TEST_P(ActorTaskSubmitterTest, TestActorDead) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor. One depends on an object that is not yet available.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  ObjectID obj = ObjectID::FromRandom();
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  // Simulate the actor dying. All in-flight tasks should get failed.
  task_manager_->fail_or_retry_pending_task_calls.clear();
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::IOError("")));
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task1.TaskId());
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 0);

  const auto death_cause = CreateMockDeathCause();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 0);

  // Actor marked as dead. All queued tasks should get failed.
  task_manager_->fail_or_retry_pending_task_calls.clear();
  submitter_.DisconnectActor(
      actor_id, 2, /*dead=*/true, death_cause, /*is_restartable=*/false);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task2.TaskId());
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartNoRetry) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  auto task4 = CreateActorTaskHelper(actor_id, worker_id, 3);
  // Submit three tasks.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task3);
  ASSERT_EQ(io_context.poll_one(), 1);

  // First task finishes. Second task fails.
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::IOError("")));

  // Simulate the actor failing.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, /*num_restarts=*/1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // Third task fails after the actor is disconnected. It should not get
  // retried.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::IOError("")));

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  submitter_.SubmitTask(task4);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task4.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->callbacks.empty());

  // task1 and task4 complete; task2 and task3 fail without retry.
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 2);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task1.TaskId());
  EXPECT_EQ(task_manager_->complete_pending_task_calls[1], task4.TaskId());
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 2);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task2.TaskId());
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[1], task3.TaskId());
  // task1, task2 failed, task3 failed, task4
  ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1, 2, 3});
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartRetry) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  auto task4 = CreateActorTaskHelper(actor_id, worker_id, 3);
  // Submit three tasks.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task3);
  ASSERT_EQ(io_context.poll_one(), 1);

  // All tasks will eventually finish. Tasks 2 and 3 will be retried.
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_return = true;
  // First task finishes. Second task fails.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::IOError("")));

  // Simulate the actor failing.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, /*num_restarts=*/1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // Third task fails after the actor is disconnected.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::IOError("")));

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  // A new task is submitted.
  submitter_.SubmitTask(task4);
  ASSERT_EQ(io_context.poll_one(), 1);
  // Tasks 2 and 3 get retried. In the real world, the seq_no of these two tasks should be
  // updated to 4 and 5 by `CoreWorker::InternalHeartbeat`.
  task2.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task2.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(4);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  task3.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task3.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(5);
  submitter_.SubmitTask(task3);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task4.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::OK()));

  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 4);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 2);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task2.TaskId());
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[1], task3.TaskId());
  // task1, task2 failed, task3 failed, task4, task2 retry, task3 retry
  ExpectSeqNosEq(worker_client_->received_seq_nos, {0, 1, 2, 3, 4, 5});
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartOutOfOrderRetry) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  // Submit three tasks.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task3);
  ASSERT_EQ(io_context.poll_one(), 1);
  // All tasks will eventually finish. Task 2 will be retried.
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_return = true;
  // First task finishes. Second task hang. Third task finishes.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::OK()));
  // Simulate the actor failing.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::IOError("")));
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);

  // Upon re-connect, task 2 (failed) should be retried.
  // Retry task 2 manually (simulating task_manager and SendPendingTask's behavior)
  task2.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task2.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(3);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);

  // Only task2 should be submitted. task 3 (completed) should not be retried.
  ASSERT_EQ(worker_client_->callbacks.size(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));

  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 3);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task2.TaskId());
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartOutOfOrderGcs) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  // Submit a task.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task1.TaskId());

  // Actor restarts, but we don't receive the disconnect message until later.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  // Submit a task.
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task2.TaskId());

  // We receive the RESTART message late. Nothing happens.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // Submit a task.
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  submitter_.SubmitTask(task3);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task3.TaskId());

  // The actor dies twice. We receive the last RESTART message first.
  submitter_.DisconnectActor(
      actor_id, 3, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // Submit a task.
  auto task4 = CreateActorTaskHelper(actor_id, worker_id, 3);
  submitter_.SubmitTask(task4);
  ASSERT_EQ(io_context.poll_one(), 1);
  // Tasks submitted when the actor is in RESTARTING state will fail immediately.
  // This happens in an io_service.post. Search `SendPendingTasks_ForceFail` to locate
  // the code.
  task_manager_->fail_or_retry_pending_task_calls.clear();
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task4.TaskId());

  // We receive the late messages. Nothing happens.
  addr.set_port(2);
  submitter_.ConnectActor(actor_id, addr, 2);
  submitter_.DisconnectActor(
      actor_id, 2, /*dead=*/false, death_cause, /*is_restartable=*/true);

  // The actor dies permanently.
  submitter_.DisconnectActor(
      actor_id, 3, /*dead=*/true, death_cause, /*is_restartable=*/false);

  // We receive more late messages. Nothing happens because the actor is dead.
  submitter_.DisconnectActor(
      actor_id, 4, /*dead=*/false, death_cause, /*is_restartable=*/true);
  addr.set_port(3);
  submitter_.ConnectActor(actor_id, addr, 4);
  // Submit a task.
  auto task5 = CreateActorTaskHelper(actor_id, worker_id, 4);
  task_manager_->fail_or_retry_pending_task_calls.clear();
  submitter_.SubmitTask(task5);
  ASSERT_EQ(io_context.poll_one(), 0);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task5.TaskId());
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartFailInflightTasks) {
  const auto allow_out_of_order_execution = GetParam();
  const auto caller_worker_id = WorkerID::FromRandom();
  rpc::Address actor_addr1;
  actor_addr1.set_worker_id(WorkerID::FromRandom().Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ false,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, actor_addr1, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create 3 tasks for the actor.
  auto task1_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 0);
  auto task2_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 1);
  auto task3_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 2);
  // Submit a task.
  submitter_.SubmitTask(task1_first_attempt);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task1_first_attempt.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task1_first_attempt.TaskId());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Submit 2 tasks.
  submitter_.SubmitTask(task2_first_attempt);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task3_first_attempt);
  ASSERT_EQ(io_context.poll_one(), 1);
  // Actor failed, but the task replies are delayed (or in some scenarios, lost).
  // We should still be able to fail the inflight tasks.
  const auto death_cause = CreateMockDeathCause();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 2);
  EXPECT_TRUE(Contains(task_manager_->fail_or_retry_pending_task_calls,
                       task2_first_attempt.TaskId()));
  EXPECT_TRUE(Contains(task_manager_->fail_or_retry_pending_task_calls,
                       task3_first_attempt.TaskId()));
  // We haven't called the RPC callback yet, mimicking the situation
  // where they might be delayed by gRPC or the network.
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  // Submit retries for task2 and task3.
  auto task2_second_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 3);
  task2_second_attempt.GetMutableMessage().set_task_id(
      task2_first_attempt.TaskIdBinary());
  task2_second_attempt.GetMutableMessage().set_attempt_number(
      task2_first_attempt.AttemptNumber() + 1);
  auto task3_second_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 4);
  task3_second_attempt.GetMutableMessage().set_task_id(
      task3_first_attempt.TaskIdBinary());
  task3_second_attempt.GetMutableMessage().set_attempt_number(
      task3_first_attempt.AttemptNumber() + 1);
  submitter_.SubmitTask(task2_second_attempt);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task3_second_attempt);
  ASSERT_EQ(io_context.poll_one(), 1);

  // Restart the actor.
  rpc::Address actor_addr2;
  actor_addr2.set_worker_id(WorkerID::FromRandom().Binary());
  submitter_.ConnectActor(actor_id, actor_addr2, 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 4);

  // The task reply of the first attempt of task2 is now received.
  // Since the first attempt is already failed, it will not
  // be marked as failed or finished again.
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  // First attempt of task2 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task2_first_attempt.GetTaskAttempt(), Status::OK()));
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 0);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 0);
  // Still have RPC callbacks for the first attempt of task3 and second attempts of task2
  // and task3.
  ASSERT_EQ(worker_client_->callbacks.size(), 3);

  task_manager_->complete_pending_task_calls.clear();
  // Second attempt of task2 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task2_second_attempt.GetTaskAttempt(), Status::OK()));
  // Second attempt of task3 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task3_second_attempt.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 2);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task2_second_attempt.TaskId());
  EXPECT_EQ(task_manager_->complete_pending_task_calls[1], task3_second_attempt.TaskId());
  // Still have RPC callbacks for the first attempt of task3.
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  // The task reply of the first attempt of task3 is now received.
  // Since the first attempt is already failed, it will not
  // be marked as failed or finished again.
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  // First attempt of task3 replied with error.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3_first_attempt.GetTaskAttempt(),
                                            Status::IOError("")));
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 0);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartFastFail) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  // Submit a task.
  submitter_.SubmitTask(task1);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task1.TaskId());

  // Actor failed and is now restarting.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);

  // Submit a new task. This task should fail immediately because "max_task_retries" is 0.
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  submitter_.SubmitTask(task2);
  ASSERT_EQ(io_context.poll_one(), 1);
  task_manager_->complete_pending_task_calls.clear();
  task_manager_->fail_or_retry_pending_task_calls.clear();
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 0);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task2.TaskId());
}

// Regression test for #44719. The actor handle's default policy says to fail fast, but
// this individual task has a retry budget. A known restart must not synthesize a failed
// attempt and consume that budget before an actor RPC is sent.
TEST_P(ActorTaskSubmitterTest, TestPerTaskRetryOverrideBuffersDuringRestart) {
  const auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable=*/true,
                                      /*owned=*/false);
  submitter_.ConnectActor(actor_id, addr, /*num_restarts=*/0);

  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(actor_id,
                             /*num_restarts=*/1,
                             /*dead=*/false,
                             death_cause,
                             /*is_restartable=*/true);

  auto task = CreateActorTaskHelper(actor_id, worker_id, 0);
  task.GetMutableMessage().set_max_retries(1);
  ASSERT_EQ(task.AttemptNumber(), 0);
  ASSERT_EQ(task.MaxRetries(), 1);

  submitter_.SubmitTask(task);
  ASSERT_EQ(io_context.poll_one(), 1);

  // Dependency resolution is complete, but a known restart must not create a local
  // failure callback or an actor RPC.
  EXPECT_EQ(io_context.poll(), 0);
  EXPECT_TRUE(task_manager_->fail_or_retry_pending_task_calls.empty());
  EXPECT_TRUE(task_manager_->complete_pending_task_calls.empty());
  EXPECT_EQ(worker_client_->callbacks.size(), 0);
  ExpectSeqNosEq(worker_client_->received_seq_nos, {});
  EXPECT_EQ(submitter_.NumPendingTasks(actor_id), 1);

  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, /*num_restarts=*/1);

  EXPECT_EQ(worker_client_->callbacks.size(), 1);
  ExpectSeqNosEq(worker_client_->received_seq_nos, {0});
  ASSERT_TRUE(worker_client_->callbacks.contains(task.GetTaskAttempt()));
  EXPECT_TRUE(worker_client_->ReplyPushTask(task.GetTaskAttempt(), Status::OK()));

  EXPECT_TRUE(task_manager_->fail_or_retry_pending_task_calls.empty());
  ASSERT_EQ(task_manager_->complete_pending_task_calls.size(), 1);
  EXPECT_EQ(task_manager_->complete_pending_task_calls[0], task.TaskId());
}

// Reverse mismatch for #44719. The actor handle's default policy says to buffer, but
// this individual dependency-ready head task explicitly has no retry budget.
TEST_P(ActorTaskSubmitterTest, TestPerTaskZeroRetryOverrideFailsDuringRestart) {
  const auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable=*/false,
                                      /*owned=*/false);
  submitter_.ConnectActor(actor_id, addr, /*num_restarts=*/0);

  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(actor_id,
                             /*num_restarts=*/1,
                             /*dead=*/false,
                             death_cause,
                             /*is_restartable=*/true);

  auto task = CreateActorTaskHelper(actor_id, worker_id, 0);
  task.GetMutableMessage().set_max_retries(0);
  task_manager_->fail_or_retry_pending_task_return = false;

  submitter_.SubmitTask(task);
  ASSERT_EQ(io_context.poll_one(), 1);

  EXPECT_EQ(worker_client_->callbacks.size(), 0);
  EXPECT_EQ(io_context.poll(), 1);
  EXPECT_EQ(submitter_.NumPendingTasks(actor_id), 0);

  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_error_types.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], task.TaskId());
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_error_types[0],
            rpc::ErrorType::ACTOR_UNAVAILABLE);
}

TEST_P(ActorTaskSubmitterTest, TestPendingTasks) {
  auto allow_out_of_order_execution = GetParam();
  int32_t max_pending_calls = 10;
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      max_pending_calls,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);

  std::vector<TaskSpecification> tasks;
  // Submit number of `max_pending_calls` tasks would be OK.
  for (int32_t i = 0; i < max_pending_calls; i++) {
    ASSERT_FALSE(submitter_.PendingTasksFull(actor_id));
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    tasks.push_back(task);
    submitter_.SubmitTask(task);
    ASSERT_EQ(io_context.poll_one(), 1);
  }

  // Then the queue should be full.
  ASSERT_TRUE(submitter_.PendingTasksFull(actor_id));

  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 10);

  // After task 0 reply comes, the queue turn to not full.
  ASSERT_TRUE(worker_client_->ReplyPushTask(tasks[0].GetTaskAttempt(), Status::OK()));
  tasks.erase(tasks.begin());
  ASSERT_FALSE(submitter_.PendingTasksFull(actor_id));

  // We can submit task 10, but after that the queue is full.
  auto task = CreateActorTaskHelper(actor_id, worker_id, 10);
  tasks.push_back(task);
  submitter_.SubmitTask(task);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.PendingTasksFull(actor_id));

  // All the replies comes, the queue shouble be empty.
  for (auto &task_spec : tasks) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(task_spec.GetTaskAttempt(), Status::OK()));
  }
  ASSERT_FALSE(submitter_.PendingTasksFull(actor_id));
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartResubmit) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);

  // Generator is pushed to worker -> generator queued for resubmit -> comes back from
  // worker -> resubmit happens.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  submitter_.SubmitTask(task1);
  io_context.run_one();
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);
  ASSERT_TRUE(submitter_.QueueGeneratorForResubmit(task1));
  task_manager_->mark_generator_failed_and_resubmit_calls.clear();
  worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK());
  ASSERT_EQ(task_manager_->mark_generator_failed_and_resubmit_calls.size(), 1);
  EXPECT_EQ(task_manager_->mark_generator_failed_and_resubmit_calls[0], task1.TaskId());
}

TEST(SequentialActorSubmitQueueTest, RestartFailureRemovesOnlyZeroRetryPrefix) {
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  const auto worker_id = WorkerID::FromRandom();
  SequentialActorSubmitQueue queue;

  const std::vector<int> max_retries = {0, 0, 0, 3, 0};
  for (size_t i = 0; i < max_retries.size(); i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    task.GetMutableMessage().set_max_retries(max_retries[i]);
    queue.Emplace("", i, task);
    queue.MarkDependencyResolved("", i);
  }

  const auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  std::vector<int64_t> failed_sequences;
  for (const auto &task : tasks_to_fail) {
    failed_sequences.push_back(task.ConcurrencyGroupSequenceNumber());
  }
  ExpectSeqNosEq(failed_sequences, {0, 1, 2});

  // Sequence 3 is a retryable ordering barrier, so sequence 4 remains for reconnect.
  std::vector<int64_t> reconnect_sequence;
  while (auto task = queue.PopNextTaskToSend()) {
    reconnect_sequence.push_back(task->first.ConcurrencyGroupSequenceNumber());
  }
  ExpectSeqNosEq(reconnect_sequence, {3, 4});
}

TEST(SequentialActorSubmitQueueTest, RestartFailureAfterBarrierCancellation) {
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  const auto worker_id = WorkerID::FromRandom();
  SequentialActorSubmitQueue queue;

  for (const auto &[sequence_no, max_retries] :
       std::vector<std::pair<int64_t, int>>{{0, 3}, {1, 0}, {2, 3}}) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, sequence_no);
    task.GetMutableMessage().set_max_retries(max_retries);
    queue.Emplace("", sequence_no, task);
    queue.MarkDependencyResolved("", sequence_no);
  }

  EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());

  queue.MarkTaskCanceled("", 0);
  auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  ASSERT_EQ(tasks_to_fail.size(), 1);
  const auto &task_to_fail = tasks_to_fail.front();
  EXPECT_EQ(task_to_fail.ConcurrencyGroupSequenceNumber(), 1);
  EXPECT_EQ(task_to_fail.MaxRetries(), 0);

  auto reconnect_task = queue.PopNextTaskToSend();
  ASSERT_TRUE(reconnect_task.has_value());
  EXPECT_EQ(reconnect_task->first.ConcurrencyGroupSequenceNumber(), 2);
  EXPECT_EQ(reconnect_task->first.MaxRetries(), 3);
  EXPECT_TRUE(queue.Empty());
}

TEST(SequentialActorSubmitQueueTest, RestartFailureIsIndependentPerConcurrencyGroup) {
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  const auto worker_id = WorkerID::FromRandom();
  SequentialActorSubmitQueue queue;

  auto task_a = CreateActorTaskHelper(actor_id, worker_id, 0);
  task_a.GetMutableMessage().set_concurrency_group_name("group_a");
  task_a.GetMutableMessage().set_max_retries(3);
  queue.Emplace("group_a", 0, task_a);
  queue.MarkDependencyResolved("group_a", 0);

  auto task_b = CreateActorTaskHelper(actor_id, worker_id, 0);
  task_b.GetMutableMessage().set_concurrency_group_name("group_b");
  task_b.GetMutableMessage().set_max_retries(0);
  queue.Emplace("group_b", 0, task_b);
  queue.MarkDependencyResolved("group_b", 0);

  auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  ASSERT_EQ(tasks_to_fail.size(), 1);
  const auto &task_to_fail = tasks_to_fail.front();
  EXPECT_EQ(task_to_fail.ConcurrencyGroupName(), "group_b");

  auto reconnect_task = queue.PopNextTaskToSend();
  ASSERT_TRUE(reconnect_task.has_value());
  EXPECT_EQ(reconnect_task->first.ConcurrencyGroupName(), "group_a");
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, RestartFailureSelectsInitialReadyZeroRetryTasks) {
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  const auto worker_id = WorkerID::FromRandom();
  OutofOrderActorSubmitQueue queue;

  const std::vector<int> max_retries = {3, 0, 0, 3, 0, 3, 0, 0, 3, 0};
  for (size_t i = 0; i < max_retries.size(); i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    task.GetMutableMessage().set_max_retries(max_retries[i]);
    if (i == 2 || i == 6) {
      // Use synthetic retry attempts with MaxRetries() == 0 to verify that
      // IsRetry() takes precedence. MaxRetries() is the configured policy,
      // not TaskManager's remaining retry budget.
      task.GetMutableMessage().set_attempt_number(1);
    }
    queue.Emplace("", i, task);
    queue.MarkDependencyResolved("", i);
  }

  const auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  std::vector<int64_t> failed_sequences;
  for (const auto &task : tasks_to_fail) {
    failed_sequences.push_back(task.ConcurrencyGroupSequenceNumber());
  }
  ExpectSeqNosEq(failed_sequences, {1, 4, 7, 9});

  std::vector<int64_t> reconnect_sequences;
  while (auto task = queue.PopNextTaskToSend()) {
    reconnect_sequences.push_back(task->first.ConcurrencyGroupSequenceNumber());
  }
  ExpectSeqNosEq(reconnect_sequences, {0, 2, 3, 5, 6, 8});
}

class SequentialActorTaskSubmitterTest : public ActorTaskSubmitterTest {};

TEST_F(SequentialActorTaskSubmitterTest, TestDependencyFailureReevaluatesRestartQueue) {
  rpc::Address addr;
  const auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  const auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      /*allow_out_of_order_execution=*/false,
                                      /*fail_if_actor_unreachable=*/false,
                                      /*owned=*/false);
  submitter_.ConnectActor(actor_id, addr, /*num_restarts=*/0);
  submitter_.DisconnectActor(actor_id,
                             /*num_restarts=*/1,
                             /*dead=*/false,
                             CreateMockDeathCause(),
                             /*is_restartable=*/true);

  actor_creator_.actor_pending = true;
  const auto dependency_actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 1);
  auto blocked_head = CreateActorTaskHelper(actor_id, worker_id, 0);
  auto *arg = blocked_head.GetMutableMessage().add_args();
  arg->add_nested_inlined_refs()->set_object_id(
      ObjectID::ForActorHandle(dependency_actor_id).Binary());

  auto zero_retry_follower = CreateActorTaskHelper(actor_id, worker_id, 1);
  zero_retry_follower.GetMutableMessage().set_max_retries(0);

  submitter_.SubmitTask(blocked_head);
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(actor_creator_.callbacks.size(), 1);

  submitter_.SubmitTask(zero_retry_follower);
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_EQ(worker_client_->callbacks.size(), 0);

  task_manager_->fail_or_retry_pending_task_return = false;
  ASSERT_TRUE(task_manager_->fail_or_retry_pending_task_calls.empty());
  ASSERT_TRUE(task_manager_->fail_or_retry_pending_task_error_types.empty());

  auto dependency_callback = std::move(actor_creator_.callbacks.front());
  actor_creator_.callbacks.pop_front();
  dependency_callback(Status::IOError("dependency actor creation failed"));

  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 1);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_error_types.size(), 1);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[0], blocked_head.TaskId());
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_error_types[0],
            rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED);

  EXPECT_EQ(io_context.poll(), 1);
  EXPECT_EQ(worker_client_->callbacks.size(), 0);

  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_calls.size(), 2);
  ASSERT_EQ(task_manager_->fail_or_retry_pending_task_error_types.size(), 2);
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_calls[1],
            zero_retry_follower.TaskId());
  EXPECT_EQ(task_manager_->fail_or_retry_pending_task_error_types[1],
            rpc::ErrorType::ACTOR_UNAVAILABLE);
}

// Test that when the head task of an actor's queue is cancelled,
// subsequent tasks with resolved dependencies can proceed.
//
// Scenario:
// - task_a has an unresolved dependency
// - task_b has no dependencies (resolved immediately)
// - In sequential mode, task_b is queued behind task_a
// - Cancel task_a
// - task_b should now execute
TEST_P(ActorTaskSubmitterTest, TestCancelHeadUnblocksQueue) {
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  ObjectID obj1 = ObjectID::FromRandom();
  auto task_a = CreateActorTaskHelper(actor_id, worker_id, 0);
  task_a.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj1.Binary());
  auto task_b = CreateActorTaskHelper(actor_id, worker_id, 1);

  reference_counter_->AddOwnedObject(
      obj1, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);

  submitter_.SubmitTask(task_a);
  ASSERT_EQ(io_context.poll_one(), 1);
  submitter_.SubmitTask(task_b);
  ASSERT_EQ(io_context.poll_one(), 1);

  if (allow_out_of_order_execution) {
    // In out-of-order mode, task_b is sent immediately after its dependencies
    // resolve, regardless of task_a's state.
    ASSERT_EQ(worker_client_->callbacks.size(), 1);
    ExpectSeqNosEq(worker_client_->received_seq_nos, {1});

    task_manager_->is_task_pending_return = true;
    submitter_.CancelTask(task_a, /*recursive=*/false);
    ASSERT_EQ(worker_client_->callbacks.size(), 1);
  } else {
    // In sequential mode, task_b is blocked by task_a even though task_b's
    // dependencies are already resolved.
    ASSERT_EQ(worker_client_->callbacks.size(), 0);

    // At this point, task_b has already resolved its dependencies and will not
    // trigger SendPendingTasks again. If CancelTask does not call SendPendingTasks
    // and handle correctly, task_b will be stuck forever.
    task_manager_->is_task_pending_return = true;
    submitter_.CancelTask(task_a, /*recursive=*/false);

    ASSERT_EQ(worker_client_->callbacks.size(), 1);
    ExpectSeqNosEq(worker_client_->received_seq_nos, {1});
  }
}

TEST_P(ActorTaskSubmitterTest, TestPerConcurrencyGroupSequencing) {
  // Test that tasks in different concurrency groups have independent sequencing
  // and do not block each other. When group_a's first task is blocked on a dependency,
  // group_b's tasks should still be sent.
  auto allow_out_of_order_execution = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      allow_out_of_order_execution,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  auto make_task = [actor_id, worker_id](int seq_no, const std::string &group_name) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, seq_no);
    task.GetMutableMessage().set_concurrency_group_name(group_name);
    return task;
  };

  // group_a task 0 has an unresolved dependency, the rest have no deps.
  ObjectID obj_a = ObjectID::FromRandom();
  auto task_a0 = make_task(0, "group_a");
  task_a0.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      obj_a.Binary());
  reference_counter_->AddOwnedObject(
      obj_a, {}, addr, "", 0, LineageReconstructionEligibility::INELIGIBLE_PUT, true);
  auto task_a1 = make_task(1, "group_a");
  auto task_b0 = make_task(0, "group_b");
  auto task_b1 = make_task(1, "group_b");

  submitter_.SubmitTask(task_a0);
  io_context.run_one();
  submitter_.SubmitTask(task_b0);
  submitter_.SubmitTask(task_b1);
  io_context.run_one();
  io_context.run_one();
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  submitter_.SubmitTask(task_a1);
  io_context.run_one();
  if (allow_out_of_order_execution) {
    ASSERT_EQ(worker_client_->callbacks.size(), 3);
  } else {
    ASSERT_EQ(worker_client_->callbacks.size(), 2);
  }

  auto data = GenerateRandomObject();
  store_->Put(*data, obj_a, true);
  io_context.run_one();
  ASSERT_EQ(worker_client_->callbacks.size(), 4);

  task_manager_->complete_pending_task_calls.clear();
  while (!worker_client_->callbacks.empty()) {
    auto it = worker_client_->callbacks.begin();
    worker_client_->ReplyPushTask(it->first, Status::OK());
  }
  EXPECT_EQ(task_manager_->complete_pending_task_calls.size(), 4);

  ASSERT_EQ(worker_client_->received_seq_nos.size(), 4);
}

INSTANTIATE_TEST_SUITE_P(AllowOutOfOrderExecution,
                         ActorTaskSubmitterTest,
                         ::testing::Values(true, false));

}  // namespace ray::core
