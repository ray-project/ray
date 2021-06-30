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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

TaskSpecification CreateActorTaskHelper(ActorID actor_id, WorkerID caller_worker_id,
                                        int64_t counter,
                                        TaskID caller_id = TaskID::Nil()) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task.GetMutableMessage().set_caller_id(caller_id.Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_caller_address()->set_worker_id(
      caller_worker_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(counter);
  task.GetMutableMessage().set_num_returns(1);
  return task;
}

rpc::PushTaskRequest CreatePushTaskRequestHelper(ActorID actor_id, int64_t counter,
                                                 WorkerID caller_worker_id,
                                                 TaskID caller_id,
                                                 int64_t caller_timestamp) {
  auto task_spec = CreateActorTaskHelper(actor_id, caller_worker_id, counter, caller_id);

  rpc::PushTaskRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request.set_sequence_number(request.task_spec().actor_task_spec().actor_counter());
  request.set_client_processed_up_to(-1);
  return request;
}

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  const rpc::Address &Addr() const override { return addr; }

  void PushActorTask(std::unique_ptr<rpc::PushTaskRequest> request, bool skip_queue,
                     const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    received_seq_nos.push_back(request->sequence_number());
    callbacks.push_back(callback);
  }

  bool ReplyPushTask(Status status = Status::OK(), size_t index = 0) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.at(index);
    callback(status, rpc::PushTaskReply());
    callbacks.erase(callbacks.begin() + index);
    return true;
  }

  rpc::Address addr;
  std::vector<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  std::vector<uint64_t> received_seq_nos;
};

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  MOCK_METHOD3(CompletePendingTask, void(const TaskID &, const rpc::PushTaskReply &,
                                         const rpc::Address &addr));
  MOCK_METHOD5(PendingTaskFailed,
               bool(const TaskID &task_id, rpc::ErrorType error_type, Status *status,
                    const std::shared_ptr<rpc::RayException> &creation_task_exception,
                    bool immediately_mark_object_fail));

  MOCK_METHOD2(OnTaskDependenciesInlined,
               void(const std::vector<ObjectID> &, const std::vector<ObjectID> &));

  MOCK_METHOD1(MarkTaskCanceled, bool(const TaskID &task_id));

  MOCK_CONST_METHOD1(GetTaskSpec,
                     absl::optional<TaskSpecification>(const TaskID &task_id));

  MOCK_METHOD4(MarkPendingTaskFailed,
               void(const TaskID &task_id, const TaskSpecification &spec,
                    rpc::ErrorType error_type,
                    const std::shared_ptr<rpc::RayException> &creation_task_exception));
};

class DirectActorSubmitterTest : public ::testing::Test {
 public:
  DirectActorSubmitterTest()
      : worker_client_(std::shared_ptr<MockWorkerClient>(new MockWorkerClient())),
        store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        task_finisher_(std::make_shared<MockTaskFinisher>()),
        submitter_(
            std::make_shared<rpc::CoreWorkerClientPool>([&](const rpc::Address &addr) {
              num_clients_connected_++;
              return worker_client_;
            }),
            store_, task_finisher_) {}

  int num_clients_connected_ = 0;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<MockTaskFinisher> task_finisher_;
  CoreWorkerDirectActorTaskSubmitter submitter_;
};

TEST_F(DirectActorSubmitterTest, TestSubmitTask) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);

  auto task = CreateActorTaskHelper(actor_id, worker_id, 0);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  task = CreateActorTaskHelper(actor_id, worker_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  EXPECT_CALL(*task_finisher_, CompletePendingTask(TaskID::Nil(), _, _))
      .Times(worker_client_->callbacks.size());
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(_, _, _, _, _)).Times(0);
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask());
  }
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));

  // Connect to the actor again.
  // Because the IP and port of address are not modified, it will skip directly and will
  // not reset `received_seq_nos`.
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
}

TEST_F(DirectActorSubmitterTest, TestDependencies) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
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
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Put the dependencies in the store in the same order as task submission.
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store_->Put(*data, obj1));
  ASSERT_EQ(worker_client_->callbacks.size(), 1);
  ASSERT_TRUE(store_->Put(*data, obj2));
  ASSERT_EQ(worker_client_->callbacks.size(), 2);
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
}

TEST_F(DirectActorSubmitterTest, TestOutOfOrderDependencies) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
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
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Put the dependencies in the store in the opposite order of task
  // submission.
  auto data = GenerateRandomObject();
  ASSERT_TRUE(store_->Put(*data, obj2));
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_TRUE(store_->Put(*data, obj1));
  ASSERT_EQ(worker_client_->callbacks.size(), 2);
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1));
}

TEST_F(DirectActorSubmitterTest, TestActorDead) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor. One depends on an object that is not yet available.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  ObjectID obj = ObjectID::FromRandom();
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  task2.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(obj.Binary());
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  // Simulate the actor dying. All in-flight tasks should get failed.
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task1.TaskId(), _, _, _, _)).Times(1);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _)).Times(0);
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));
  }

  EXPECT_CALL(*task_finisher_, PendingTaskFailed(_, _, _, _, _)).Times(0);
  submitter_.DisconnectActor(actor_id, 0, /*dead=*/false);
  // Actor marked as dead. All queued tasks should get failed.
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task2.TaskId(), _, _, _, _)).Times(1);
  submitter_.DisconnectActor(actor_id, 1, /*dead=*/true);
}

TEST_F(DirectActorSubmitterTest, TestActorRestartNoRetry) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
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
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());

  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _)).Times(2);
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task2.TaskId(), _, _, _, _)).Times(2);
  // First task finishes. Second task fails.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));

  // Simulate the actor failing.
  submitter_.DisconnectActor(actor_id, 0, /*dead=*/false);
  // Third task fails after the actor is disconnected. It should not get
  // retried.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task4).ok());
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));
  ASSERT_TRUE(worker_client_->callbacks.empty());
  // Actor counter restarts at 0 after the actor is restarted.
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1, 2, 0));
}

TEST_F(DirectActorSubmitterTest, TestActorRestartRetry) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
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
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());

  // All tasks will eventually finish.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _)).Times(4);
  // Tasks 2 and 3 will be retried.
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task2.TaskId(), _, _, _, _))
      .Times(2)
      .WillRepeatedly(Return(true));
  // First task finishes. Second task fails.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));

  // Simulate the actor failing.
  submitter_.DisconnectActor(actor_id, 0, /*dead=*/false);
  // Third task fails after the actor is disconnected.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  // A new task is submitted.
  ASSERT_TRUE(submitter_.SubmitTask(task4).ok());
  // Tasks 2 and 3 get retried.
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));
  }
  // Actor counter restarts at 0 after the actor is restarted. New task cannot
  // execute until after tasks 2 and 3 are re-executed.
  ASSERT_THAT(worker_client_->received_seq_nos, ElementsAre(0, 1, 2, 2, 0, 1));
}

TEST_F(DirectActorSubmitterTest, TestActorRestartOutOfOrderRetry) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create four tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, worker_id, 0);
  auto task2 = CreateActorTaskHelper(actor_id, worker_id, 1);
  auto task3 = CreateActorTaskHelper(actor_id, worker_id, 2);
  // Submit three tasks.
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task3).ok());
  // All tasks will eventually finish.
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task1.TaskId(), _, _)).Times(3);

  // Tasks 2 will be retried
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task2.TaskId(), _, _, _, _))
      .Times(1)
      .WillRepeatedly(Return(true));
  // First task finishes. Second task hang. Third task finishes.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK(), /*index=*/0));
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK(), /*index=*/1));
  // Simulate the actor failing.
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError(""), /*index=*/0));
  submitter_.DisconnectActor(actor_id, 0, /*dead=*/false);

  // Actor gets restarted.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  // Upon re-connect, task 2 (failed) and 3 (completed) should be both retried.
  // Retry task 2 manually (simulating task_finisher and SendPendingTask's behavior)
  // Retry task 3 should happen via event loop
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());

  // Both task2 and task3 should be submitted.
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  // Finishes all task
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));
  }
}

TEST_F(DirectActorSubmitterTest, TestActorRestartOutOfOrderGcs) {
  rpc::Address addr;
  auto worker_id = WorkerID::FromRandom();
  addr.set_worker_id(worker_id.Binary());
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.AddActorQueueIfNotExists(actor_id);
  addr.set_port(0);
  submitter_.ConnectActor(actor_id, addr, 0);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_EQ(num_clients_connected_, 1);

  // Create four tasks for the actor.
  auto task = CreateActorTaskHelper(actor_id, worker_id, 0);
  // Submit a task.
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task.TaskId(), _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));

  // Actor restarts, but we don't receive the disconnect message until later.
  addr.set_port(1);
  submitter_.ConnectActor(actor_id, addr, 1);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  task = CreateActorTaskHelper(actor_id, worker_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task.TaskId(), _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));

  // We receive the RESTART message late. Nothing happens.
  submitter_.DisconnectActor(actor_id, 1, /*dead=*/false);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  task = CreateActorTaskHelper(actor_id, worker_id, 2);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task.TaskId(), _, _)).Times(1);
  ASSERT_TRUE(worker_client_->ReplyPushTask(Status::OK()));

  // The actor dies twice. We receive the last RESTART message first.
  submitter_.DisconnectActor(actor_id, 3, /*dead=*/false);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  task = CreateActorTaskHelper(actor_id, worker_id, 3);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  EXPECT_CALL(*task_finisher_, CompletePendingTask(task.TaskId(), _, _)).Times(0);
  ASSERT_FALSE(worker_client_->ReplyPushTask(Status::OK()));

  // We receive the late messages. Nothing happens.
  addr.set_port(2);
  submitter_.ConnectActor(actor_id, addr, 2);
  submitter_.DisconnectActor(actor_id, 2, /*dead=*/false);
  ASSERT_EQ(num_clients_connected_, 2);

  // The actor dies permanently. All tasks are failed.
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task.TaskId(), _, _, _, _)).Times(1);
  submitter_.DisconnectActor(actor_id, 3, /*dead=*/true);
  ASSERT_EQ(num_clients_connected_, 2);

  // We receive more late messages. Nothing happens because the actor is dead.
  submitter_.DisconnectActor(actor_id, 4, /*dead=*/false);
  addr.set_port(3);
  submitter_.ConnectActor(actor_id, addr, 4);
  ASSERT_EQ(num_clients_connected_, 2);
  // Submit a task.
  task = CreateActorTaskHelper(actor_id, worker_id, 4);
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(task.TaskId(), _, _, _, _)).Times(1);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
}

class MockDependencyWaiter : public DependencyWaiter {
 public:
  MOCK_METHOD2(Wait, void(const std::vector<rpc::ObjectReference> &dependencies,
                          std::function<void()> on_dependencies_available));

  virtual ~MockDependencyWaiter() {}
};

class MockWorkerContext : public WorkerContext {
 public:
  MockWorkerContext(WorkerType worker_type, const JobID &job_id)
      : WorkerContext(worker_type, WorkerID::FromRandom(), job_id) {
    current_actor_is_direct_call_ = true;
  }
};

class DirectActorReceiverTest : public ::testing::Test {
 public:
  DirectActorReceiverTest()
      : worker_context_(WorkerType::WORKER, JobID::FromInt(0)),
        worker_client_(std::shared_ptr<MockWorkerClient>(new MockWorkerClient())),
        dependency_waiter_(std::make_shared<MockDependencyWaiter>()) {
    auto execute_task =
        std::bind(&DirectActorReceiverTest::MockExecuteTask, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    receiver_ = std::make_unique<CoreWorkerDirectTaskReceiver>(
        worker_context_, main_io_service_, execute_task, [] { return Status::OK(); });
    receiver_->Init(std::make_shared<rpc::CoreWorkerClientPool>(
                        [&](const rpc::Address &addr) { return worker_client_; }),
                    rpc_address_, dependency_waiter_);
  }

  Status MockExecuteTask(const TaskSpecification &task_spec,
                         const std::shared_ptr<ResourceMappingType> &resource_ids,
                         std::vector<std::shared_ptr<RayObject>> *return_objects,
                         ReferenceCounter::ReferenceTableProto *borrowed_refs) {
    return Status::OK();
  }

  void StartIOService() { main_io_service_.run(); }

  void StopIOService() {
    // We must delete the receiver before stopping the IO service, since it
    // contains timers referencing the service.
    receiver_.reset();
    main_io_service_.stop();
  }

  std::unique_ptr<CoreWorkerDirectTaskReceiver> receiver_;

 private:
  rpc::Address rpc_address_;
  MockWorkerContext worker_context_;
  instrumented_io_context main_io_service_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<DependencyWaiter> dependency_waiter_;
};

TEST_F(DirectActorReceiverTest, TestNewTaskFromDifferentWorker) {
  TaskID current_task_id = TaskID::Nil();
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  WorkerID worker_id = WorkerID::FromRandom();
  TaskID caller_id =
      TaskID::ForActorTask(JobID::FromInt(0), current_task_id, 0, actor_id);

  int64_t curr_timestamp = current_sys_time_ms();
  int64_t old_timestamp = curr_timestamp - 1000;
  int64_t new_timestamp = curr_timestamp + 1000;

  int callback_count = 0;

  // Push a task request with actor counter 0. This should scucceed
  // on the receiver.
  {
    auto request =
        CreatePushTaskRequestHelper(actor_id, 0, worker_id, caller_id, curr_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status, std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  // Push a task request with actor counter 1. This should scucceed
  // on the receiver.
  {
    auto request =
        CreatePushTaskRequestHelper(actor_id, 1, worker_id, caller_id, curr_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status, std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  // Create another request with the same caller id, but a different worker id,
  // and a newer timestamp. This simulates caller reconstruction.
  // Note that here the task request still has counter 0, which should be
  // ignored normally, but here it's from a different worker and with a newer
  // timestamp, in this case it should succeed.
  {
    auto worker_id = WorkerID::FromRandom();
    auto request =
        CreatePushTaskRequestHelper(actor_id, 0, worker_id, caller_id, new_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status, std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  // Push a task request with actor counter 1, but with a different worker id,
  // and a older timstamp. In this case the request should fail.
  {
    auto worker_id = WorkerID::FromRandom();
    auto request =
        CreatePushTaskRequestHelper(actor_id, 1, worker_id, caller_id, old_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status, std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(!status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  StartIOService();

  // Wait for all the callbacks to be invoked.
  auto condition_func = [&callback_count]() -> bool { return callback_count == 4; };

  ASSERT_TRUE(WaitForCondition(condition_func, 10 * 1000));

  StopIOService();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}
