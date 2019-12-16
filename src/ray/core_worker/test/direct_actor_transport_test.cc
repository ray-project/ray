#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/direct_task_transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/util/test_util.h"

namespace ray {

using ::testing::_;

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
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

  std::list<rpc::ClientCallback<rpc::PushTaskReply>> callbacks;
  uint64_t counter = 0;
};

class MockTaskFinisher : public TaskFinisherInterface {
 public:
  MockTaskFinisher() {}

  MOCK_METHOD3(CompletePendingTask, void(const TaskID &, const rpc::PushTaskReply &,
                                         const rpc::Address *addr));
  MOCK_METHOD3(PendingTaskFailed,
               void(const TaskID &task_id, rpc::ErrorType error_type, Status *status));
};

TaskSpecification CreateActorTaskHelper(ActorID actor_id, int64_t counter) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(counter);
  return task;
}

class DirectActorTransportTest : public ::testing::Test {
 public:
  DirectActorTransportTest()
      : worker_client_(std::shared_ptr<MockWorkerClient>(new MockWorkerClient())),
        store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        task_finisher_(std::make_shared<MockTaskFinisher>()),
        submitter_([&](const std::string ip, int port) { return worker_client_; }, store_,
                   task_finisher_) {}

  std::shared_ptr<MockWorkerClient> worker_client_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<MockTaskFinisher> task_finisher_;
  CoreWorkerDirectActorTaskSubmitter submitter_;
};

TEST_F(DirectActorTransportTest, TestSubmitTask) {
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

TEST_F(DirectActorTransportTest, TestDependencies) {
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
  ASSERT_TRUE(store_->Put(*data, obj1).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 1);
  ASSERT_TRUE(store_->Put(*data, obj2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 2);
}

TEST_F(DirectActorTransportTest, TestOutOfOrderDependencies) {
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
  ASSERT_TRUE(store_->Put(*data, obj2).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 0);
  ASSERT_TRUE(store_->Put(*data, obj1).ok());
  ASSERT_EQ(worker_client_->callbacks.size(), 2);
}

TEST_F(DirectActorTransportTest, TestActorFailure) {
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
