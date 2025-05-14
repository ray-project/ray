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

#include "ray/core_worker/transport/actor_task_submitter.h"

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "mock/ray/core_worker/actor_creator.h"
#include "mock/ray/core_worker/reference_count.h"
#include "mock/ray/core_worker/task_manager.h"
#include "ray/common/test_util.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray::core {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;
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
  task.GetMutableMessage().mutable_actor_task_spec()->set_sequence_number(counter);
  task.GetMutableMessage().set_num_returns(0);
  return task;
}

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
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
  std::vector<uint64_t> received_seq_nos;
  int64_t acked_seqno = 0;
};

class ActorTaskSubmitterTest : public ::testing::TestWithParam<bool> {
 public:
  ActorTaskSubmitterTest()
      : client_pool_(
            std::make_shared<rpc::CoreWorkerClientPool>([&](const rpc::Address &addr) {
              num_clients_connected_++;
              return worker_client_;
            })),
        worker_client_(std::make_shared<MockWorkerClient>()),
        store_(std::make_shared<CoreWorkerMemoryStore>(io_context)),
        task_finisher_(std::make_shared<MockTaskFinisherInterface>()),
        io_work(io_context.get_executor()),
        reference_counter_(std::make_shared<MockReferenceCounter>()),
        submitter_(
            *client_pool_,
            *store_,
            *task_finisher_,
            actor_creator_,
            [this](const ActorID &actor_id, int64_t num_queued) {
              last_queue_warning_ = num_queued;
            },
            io_context,
            reference_counter_) {}

  void TearDown() override { io_context.stop(); }

  int num_clients_connected_ = 0;
  int64_t last_queue_warning_ = 0;
  MockActorCreatorInterface actor_creator_;
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<MockTaskFinisherInterface> task_finisher_;
  instrumented_io_context io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work;
  std::shared_ptr<MockReferenceCounter> reference_counter_;
  ActorTaskSubmitter submitter_;
};

TEST_P(ActorTaskSubmitterTest, TestSubmitTask) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);

  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _, _))
      .Times(worker_client_->callbacks.size());
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(_, _, _, _, _, _)).Times(0);
  worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK());
  worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK());
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));

  // Connect to the actor again.
  // Because the IP and port of address are not modified, it will skip directly and will
  // not reset `received_seq_nos`.
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
}

TEST_P(ActorTaskSubmitterTest, TestQueueingWarning) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);

  for (int i = 0; i < 7500; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    ASSERT_TRUE(submitter_.SubmitTask(task).ok());
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_TRUE(worker_client_->ReplyPushTask(task.GetTaskAttempt(), Status::OK()));
  }
  ASSERT_EQ(last_queue_warning_, 0);

  for (int i = 7500; i < 15000; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    ASSERT_TRUE(submitter_.SubmitTask(task).ok());
    ASSERT_EQ(io_context.poll_one(), 1);
    /* no ack */
  }
  ASSERT_EQ(last_queue_warning_, 5000);

  for (int i = 15000; i < 35000; i++) {
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    ASSERT_TRUE(submitter_.SubmitTask(task).ok());
    ASSERT_EQ(io_context.poll_one(), 1);
    /* no ack */
  }
  ASSERT_EQ(last_queue_warning_, 20000);
}

TEST_P(ActorTaskSubmitterTest, TestDependencies) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
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

  // Neither task can be submitted yet because they are still waiting on
  // dependencies.
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Put the dependencies in the store in the same order as task submission.
  auto data = GenerateRandomObject();

  // Each Put schedules a callback onto io_context, and let's run it.
  ASSERT_TRUE(store_->Put(*data, obj1));
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  ASSERT_TRUE(store_->Put(*data, obj2));
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
}

TEST_P(ActorTaskSubmitterTest, TestOutOfOrderDependencies) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
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

  // Neither task can be submitted yet because they are still waiting on
  // dependencies.
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  if (execute_out_of_order) {
    // Put the dependencies in the store in the opposite order of task
    // submission.
    auto data = GenerateRandomObject();
    // task2 is submitted first as we allow out of order execution.
    ASSERT_TRUE(store_->Put(*data, obj2));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 1);
    ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(1));
    // then task1 is submitted
    ASSERT_TRUE(store_->Put(*data, obj1));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 2);
    ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(1, 0));
  } else {
    // Put the dependencies in the store in the opposite order of task
    // submission.
    auto data = GenerateRandomObject();
    ASSERT_TRUE(store_->Put(*data, obj2));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 0);
    ASSERT_TRUE(store_->Put(*data, obj1));
    ASSERT_EQ(io_context.poll_one(), 1);
    ASSERT_EQ(worker_client_->callbacks.size(), 2);
    ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
  }
}

TEST_P(ActorTaskSubmitterTest, TestActorDead) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor. One depends on an object that is not yet available.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  ObjectID obj = ObjectID::FromRandom();
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  // Simulate the actor dying. All in-flight tasks should get failed.
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task1.TaskId(), _, _, _, _, _))
      .Times(1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _, _)).Times(0);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::IOError("")));

  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(_, _, _, _, _, _)).Times(0);
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // Actor marked as dead. All queued tasks should get failed.
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task2.TaskId(), _, _, _, _, _))
      .Times(1);
  submitter_.DisconnectActor(
      actor_id, 2, /*dead=*/true, death_cause, /*is_restartable=*/false);
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartNoRetry) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
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
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  ASSERT_EQ(io_context.poll_one(), 1);

  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _, _)).Times(1);
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task2.TaskId(), _, _, _, _, _))
      .Times(1);
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task3.TaskId(), _, _, _, _, _))
      .Times(1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task4.TaskId(), _, _, _)).Times(1);
  // First task finishes. Second task fails.
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
  ASSERT_TRUE(submitter_.SubmitTask(task4).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task4.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->callbacks.empty());
  // task1, task2 failed, task3 failed, task4
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1, 2, 3));
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartRetry) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
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
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  ASSERT_EQ(io_context.poll_one(), 1);

  // All tasks will eventually finish.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _, _)).Times(4);
  // Tasks 2 and 3 will be retried.
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task2.TaskId(), _, _, _, _, _))
      .Times(1)
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task3.TaskId(), _, _, _, _, _))
      .Times(1)
      .WillRepeatedly(Return(true));
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
  ASSERT_TRUE(submitter_.SubmitTask(task4).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  // Tasks 2 and 3 get retried. In the real world, the seq_no of these two tasks should be
  // updated to 4 and 5 by `CoreWorker::InternalHeartbeat`.
  task2.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task2.GetMutableMessage().mutable_actor_task_spec()->set_sequence_number(4);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  task3.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task3.GetMutableMessage().mutable_actor_task_spec()->set_sequence_number(5);
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task4.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::OK()));
  // task1, task2 failed, task3 failed, task4, task2 retry, task3 retry
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1, 2, 3, 4, 5));
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartOutOfOrderRetry) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
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
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  // All tasks will eventually finish.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _, _)).Times(3);

  // Tasks 2 will be retried
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task2.TaskId(), _, _, _, _, _))
      .Times(1)
      .WillRepeatedly(Return(true));
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
  // Retry task 2 manually (simulating task_finisher and SendPendingTask's behavior)
  task2.GetMutableMessage().set_attempt_number(task2.AttemptNumber() + 1);
  task2.GetMutableMessage().mutable_actor_task_spec()->set_sequence_number(3);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);

  // Only task2 should be submitted. task 3 (completed) should not be retried.
  ASSERT_EQ(worker_client_->callbacks.size(), 1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartOutOfOrderGcs) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_EQ(num_clients_connected_, 1);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  // Submit a task.
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));

  // Actor restarts, but we don't receive the disconnect message until later.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task2.TaskId(), _, _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task2.GetTaskAttempt(), Status::OK()));

  // We receive the RESTART message late. Nothing happens.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task3.TaskId(), _, _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3.GetTaskAttempt(), Status::OK()));

  // The actor dies twice. We receive the last RESTART message first.
  submitter_.DisconnectActor(
      actor_id, 3, /*dead=*/false, death_cause, /*is_restartable=*/true);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  auto task4 = CreateActorTaskHelper(actor_id, worker_id, 3);
  ASSERT_TRUE(submitter_.SubmitTask(task4).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  // Tasks submitted when the actor is in RESTARTING state will fail immediately.
  // This happens in an io_service.post. Search `SendPendingTasks_ForceFail` to locate
  // the code.
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task4.TaskId(), _, _, _, _, _))
      .Times(1);
  ASSERT_EQ(io_context.poll_one(), 1);

  // We receive the late messages. Nothing happens.
  addr.set_port(2);
  submitter_.ConnectActor(actor_id, addr, 2);
  submitter_.DisconnectActor(
      actor_id, 2, /*dead=*/false, death_cause, /*is_restartable=*/true);
  ASSERT_EQ(num_clients_connected_, 2);

  // The actor dies permanently.
  submitter_.DisconnectActor(
      actor_id, 3, /*dead=*/true, death_cause, /*is_restartable=*/false);
  ASSERT_EQ(num_clients_connected_, 2);

  // We receive more late messages. Nothing happens because the actor is dead.
  submitter_.DisconnectActor(
      actor_id, 4, /*dead=*/false, death_cause, /*is_restartable=*/true);
  addr.set_port(3);
  submitter_.ConnectActor(actor_id, addr, 4);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  auto task5 = CreateActorTaskHelper(actor_id, worker_id, 4);
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task5.TaskId(), _, _, _, _, _))
      .Times(1);
  ASSERT_TRUE(submitter_.SubmitTask(task5).ok());
  ASSERT_EQ(io_context.poll_one(), 0);
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartFailInflightTasks) {
  const auto execute_out_of_order = GetParam();
  const auto caller_worker_id = WorkerID::FromRandom();
  rpc::Address actor_addr1;
  actor_addr1.set_worker_id(WorkerID::FromRandom().Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ false,
                                      /*owned*/ false);
  submitter_.ConnectActor(actor_id, actor_addr1, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_EQ(num_clients_connected_, 1);

  // Create 3 tasks for the actor.
  auto task1_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 0);
  auto task2_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 1);
  auto task3_first_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 2);
  // Submit a task.
  ASSERT_TRUE(submitter_.SubmitTask(task1_first_attempt).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1_first_attempt.TaskId(), _, _, _))
      .Times(1);
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task1_first_attempt.GetTaskAttempt(), Status::OK()));
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Submit 2 tasks.
  ASSERT_TRUE(submitter_.SubmitTask(task2_first_attempt).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task3_first_attempt).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  // Actor failed, but the task replies are delayed (or in some scenarios, lost).
  // We should still be able to fail the inflight tasks.
  EXPECT_CALL(*task_finisher_,
              FailOrRetryPendingTask(task2_first_attempt.TaskId(), _, _, _, _, _))
      .Times(1);
  EXPECT_CALL(*task_finisher_,
              FailOrRetryPendingTask(task3_first_attempt.TaskId(), _, _, _, _, _))
      .Times(1);
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);
  // We haven't called the RPC callback yet, mimicking the situation
  // where they might be delayed by gRPC or the network.
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  // Submit retries for task2 and task3.
  auto task2_second_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 3);
  task2_second_attempt.GetMutableMessage().set_task_id(
      task2_first_attempt.TaskId().Binary());
  task2_second_attempt.GetMutableMessage().set_attempt_number(
      task2_first_attempt.AttemptNumber() + 1);
  auto task3_second_attempt = CreateActorTaskHelper(actor_id, caller_worker_id, 4);
  task3_second_attempt.GetMutableMessage().set_task_id(
      task3_first_attempt.TaskId().Binary());
  task3_second_attempt.GetMutableMessage().set_attempt_number(
      task3_first_attempt.AttemptNumber() + 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2_second_attempt).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.SubmitTask(task3_second_attempt).ok());
  ASSERT_EQ(io_context.poll_one(), 1);

  // Restart the actor.
  rpc::Address actor_addr2;
  actor_addr2.set_worker_id(WorkerID::FromRandom().Binary());
  submitter_.ConnectActor(actor_id, actor_addr2, 1);
  ASSERT_EQ(worker_client_->callbacks.size(), 4);

  // The task reply of the first attempt of task2 is now received.
  // Since the first attempt is already failed, it will not
  // be marked as failed or finished again.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task2_first_attempt.TaskId(), _, _, _))
      .Times(0);
  EXPECT_CALL(*task_finisher_,
              FailOrRetryPendingTask(task2_first_attempt.TaskId(), _, _, _, _, _))
      .Times(0);
  // First attempt of task2 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task2_first_attempt.GetTaskAttempt(), Status::OK()));
  // Still have RPC callbacks for the first attempt of task3 and second attempts of task2
  // and task3.
  ASSERT_EQ(worker_client_->callbacks.size(), 3);

  EXPECT_CALL(*task_finisher_,
              CompletePendingTask(task2_second_attempt.TaskId(), _, _, _))
      .Times(1);
  EXPECT_CALL(*task_finisher_,
              CompletePendingTask(task3_second_attempt.TaskId(), _, _, _))
      .Times(1);
  // Second attempt of task2 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task2_second_attempt.GetTaskAttempt(), Status::OK()));
  // Second attempt of task3 replied with OK.
  ASSERT_TRUE(
      worker_client_->ReplyPushTask(task3_second_attempt.GetTaskAttempt(), Status::OK()));
  // Still have RPC callbacks for the first attempt of task3.
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  // The task reply of the first attempt of task3 is now received.
  // Since the first attempt is already failed, it will not
  // be marked as failed or finished again.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task3_first_attempt.TaskId(), _, _, _))
      .Times(0);
  EXPECT_CALL(*task_finisher_,
              FailOrRetryPendingTask(task3_first_attempt.TaskId(), _, _, _, _, _))
      .Times(0);
  // First attempt of task3 replied with error.
  ASSERT_TRUE(worker_client_->ReplyPushTask(task3_first_attempt.GetTaskAttempt(),
                                            Status::IOError("")));
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
}

TEST_P(ActorTaskSubmitterTest, TestActorRestartFastFail) {
  auto execute_out_of_order = GetParam();
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      -1,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_EQ(num_clients_connected_, 1);

  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  // Submit a task.
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(task1.GetTaskAttempt(), Status::OK()));

  // Actor failed and is now restarting.
  const auto death_cause = CreateMockDeathCause();
  submitter_.DisconnectActor(
      actor_id, 1, /*dead=*/false, death_cause, /*is_restartable=*/true);

  // Submit a new task. This task should fail immediately because "max_task_retries" is 0.
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task2.TaskId(), _, _, _)).Times(0);
  EXPECT_CALL(*task_finisher_, FailOrRetryPendingTask(task2.TaskId(), _, _, _, _, _))
      .Times(1);
  ASSERT_EQ(io_context.poll_one(), 1);
}

TEST_P(ActorTaskSubmitterTest, TestPendingTasks) {
  auto execute_out_of_order = GetParam();
  int32_t max_pending_calls = 10;
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id,
                                      max_pending_calls,
                                      execute_out_of_order,
                                      /*fail_if_actor_unreachable*/ true,
                                      /*owned*/ false);
  addr.set_port(0);

  std::vector<TaskSpecification> tasks;
  // Submit number of `max_pending_calls` tasks would be OK.
  for (int32_t i = 0; i < max_pending_calls; i++) {
    ASSERT_FALSE(submitter_.PendingTasksFull(actor_id));
    auto task = CreateActorTaskHelper(actor_id, worker_id, i);
    tasks.push_back(task);
    ASSERT_TRUE(submitter_.SubmitTask(task).ok());
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
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  ASSERT_EQ(io_context.poll_one(), 1);
  ASSERT_TRUE(submitter_.PendingTasksFull(actor_id));

  // All the replies comes, the queue shouble be empty.
  for (auto &task : tasks) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(task.GetTaskAttempt(), Status::OK()));
  }
  ASSERT_FALSE(submitter_.PendingTasksFull(actor_id));
}

INSTANTIATE_TEST_SUITE_P(ExecuteOutOfOrder,
                         ActorTaskSubmitterTest,
                         ::testing::Values(true, false));

}  // namespace ray::core
