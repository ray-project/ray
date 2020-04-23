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
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

using ::testing::_;

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  const rpc::Address &Addr() const override { return addr; }

  ray::Status PushActorTask(
      std::unique_ptr<rpc::PushTaskRequest> request,
      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    RAY_CHECK(counter == request->task_spec().actor_task_spec().actor_counter());
    counter++;
    callbacks.push_back(callback);
    return Status::OK();
  }

  bool ReplyPushTask(Status status = Status::OK()) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    callback(status, rpc::PushTaskReply());
    callbacks.pop_front();
    return true;
  }

  rpc::Address addr;
  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  uint64_t counter = 0;
};

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  MOCK_METHOD3(CompletePendingTask, void(const TaskID &, const rpc::PushTaskReply &,
                                         const rpc::Address &addr));
  MOCK_METHOD3(PendingTaskFailed,
               void(const TaskID &task_id, rpc::ErrorType error_type, Status *status));

  MOCK_METHOD2(OnTaskDependenciesInlined,
               void(const std::vector<ObjectID> &, const std::vector<ObjectID> &));

  MOCK_METHOD1(MarkTaskCanceled, bool(const TaskID &task_id));
};

TaskSpecification CreateActorTaskHelper(ActorID actor_id, int64_t counter) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(counter);
  return task;
}

class DirectActorSubmitterTest : public ::testing::Test {
 public:
  DirectActorSubmitterTest()
      : worker_client_(std::shared_ptr<MockWorkerClient>(new MockWorkerClient())),
        store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        task_finisher_(std::make_shared<MockTaskFinisher>()),
        submitter_(address_, [&](const rpc::Address &addr) { return worker_client_; },
                   store_, task_finisher_) {}

  rpc::Address address_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<MockTaskFinisher> task_finisher_;
  CoreWorkerDirectActorTaskSubmitter submitter_;
};

TEST_F(DirectActorSubmitterTest, TestSubmitTask) {
  rpc::Address addr;
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);

  auto task = CreateActorTaskHelper(actor_id, 0);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  submitter_.ConnectActor(actor_id, addr);
  ASSERT_EQ(worker_client_->callbacks.size(), 1);

  task = CreateActorTaskHelper(actor_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  EXPECT_CALL(*task_finisher_, CompletePendingTask(TaskID::Nil(), _, _))
      .Times(worker_client_->callbacks.size());
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(_, _, _)).Times(0);
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask());
  }
}

TEST_F(DirectActorSubmitterTest, TestDependencies) {
  rpc::Address addr;
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.ConnectActor(actor_id, addr);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor with different arguments.
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto task1 = CreateActorTaskHelper(actor_id, 0);
  task1.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  auto task2 = CreateActorTaskHelper(actor_id, 1);
  task2.GetMutableMessage().add_args()->add_object_ids(obj2.Binary());

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
}

TEST_F(DirectActorSubmitterTest, TestOutOfOrderDependencies) {
  rpc::Address addr;
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  submitter_.ConnectActor(actor_id, addr);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor with different arguments.
  ObjectID obj1 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  ObjectID obj2 = ObjectID::FromRandom().WithTransportType(TaskTransportType::DIRECT);
  auto task1 = CreateActorTaskHelper(actor_id, 0);
  task1.GetMutableMessage().add_args()->add_object_ids(obj1.Binary());
  auto task2 = CreateActorTaskHelper(actor_id, 1);
  task2.GetMutableMessage().add_args()->add_object_ids(obj2.Binary());

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
}

TEST_F(DirectActorSubmitterTest, TestActorFailure) {
  rpc::Address addr;
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  gcs::ActorTableData actor_data;
  submitter_.ConnectActor(actor_id, addr);
  ASSERT_EQ(worker_client_->callbacks.size(), 0);

  // Create two tasks for the actor.
  auto task1 = CreateActorTaskHelper(actor_id, 0);
  auto task2 = CreateActorTaskHelper(actor_id, 1);
  ASSERT_TRUE(submitter_.SubmitTask(task1).ok());
  ASSERT_TRUE(submitter_.SubmitTask(task2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 2);

  // Simulate the actor dying. All submitted tasks should get failed.
  EXPECT_CALL(*task_finisher_, PendingTaskFailed(_, _, _)).Times(2);
  EXPECT_CALL(*task_finisher_, CompletePendingTask(_, _, _)).Times(0);
  while (!worker_client_->callbacks.empty()) {
    ASSERT_TRUE(worker_client_->ReplyPushTask(Status::IOError("")));
  }
}

class MockDependencyWaiterInterface : public DependencyWaiterInterface {
 public:
  virtual Status WaitForDirectActorCallArgs(const std::vector<ObjectID> &object_ids,
                                            int64_t tag) override {
    return Status::OK();
  }
};

TaskSpecification CreateActorTaskHelper(ActorID actor_id, int64_t counter,
                                        TaskID caller_id) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task.GetMutableMessage().set_caller_id(caller_id.Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(counter);
  task.GetMutableMessage().set_num_returns(1);
  return task;
}

rpc::PushTaskRequest CreatePushTaskRequestHelper(ActorID actor_id, int64_t counter,
                                                 WorkerID caller_worker_id,
                                                 TaskID caller_id,
                                                 int64_t caller_timestamp) {
  auto task_spec = CreateActorTaskHelper(actor_id, counter, caller_id);
  rpc::Address rpc_address;
  rpc_address.set_worker_id(caller_worker_id.Binary());

  rpc::PushTaskRequest request;
  request.mutable_caller_address()->CopyFrom(rpc_address);
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request.set_caller_version(caller_timestamp);
  request.set_sequence_number(request.task_spec().actor_task_spec().actor_counter());
  request.set_client_processed_up_to(-1);
  return request;
}

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
        dependency_client_(std::make_shared<MockDependencyWaiterInterface>()) {
    auto execute_task =
        std::bind(&DirectActorReceiverTest::MockExecuteTask, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    receiver_ = std::unique_ptr<CoreWorkerDirectTaskReceiver>(
        new CoreWorkerDirectTaskReceiver(worker_context_, main_io_service_, execute_task,
                                         [] { return Status::OK(); }));
    receiver_->Init([&](const rpc::Address &addr) { return worker_client_; },
                    rpc_address_, dependency_client_);
  }

  Status MockExecuteTask(const TaskSpecification &task_spec,
                         const std::shared_ptr<ResourceMappingType> &resource_ids,
                         std::vector<std::shared_ptr<RayObject>> *return_objects,
                         ReferenceCounter::ReferenceTableProto *borrowed_refs) {
    return Status::OK();
  }

  void StartIOService() { main_io_service_.run(); }

  void StopIOService() { main_io_service_.stop(); }

  std::unique_ptr<CoreWorkerDirectTaskReceiver> receiver_;

 private:
  rpc::Address rpc_address_;
  MockWorkerContext worker_context_;
  boost::asio::io_service main_io_service_;
  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<DependencyWaiterInterface> dependency_client_;
};

TEST_F(DirectActorReceiverTest, TestNewTaskFromDifferentWorker) {
  rpc::Address addr;
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
    receiver_->HandlePushTask(request, &reply, reply_callback);
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
    receiver_->HandlePushTask(request, &reply, reply_callback);
  }

  // Create another request with the same caller id, but a differnt worker id,
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
    receiver_->HandlePushTask(request, &reply, reply_callback);
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
    receiver_->HandlePushTask(request, &reply, reply_callback);
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
