#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/util/test_util.h"

namespace ray {

TaskSpecification CreateTaskHelper(uint64_t num_returns) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::ForFakeTask().Binary());
  task.GetMutableMessage().set_num_returns(num_returns);
  return task;
}

class MockActorManager : public ActorManagerInterface {
  void PublishCreatedActor(const TaskSpecification &actor_creation_task,
                           const rpc::Address &address) override {
    num_publishes += 1;
  }

  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override {
    num_terminations += 1;
  }

  int num_publishes = 0;
  int num_terminations = 0;
};

class TaskManagerTest : public ::testing::Test {
 public:
  TaskManagerTest()
      : store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        actor_manager_(std::shared_ptr<ActorManagerInterface>(new MockActorManager())),
        manager_(store_, actor_manager_, [this](const TaskSpecification &spec) {
          num_retries_++;
          return Status::OK();
        }) {}

  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<ActorManagerInterface> actor_manager_;
  TaskManager manager_;
  int num_retries_ = 0;
};

TEST_F(TaskManagerTest, TestTaskSuccess) {
  auto spec = CreateTaskHelper(1);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(spec);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  manager_.CompletePendingTask(spec.TaskId(), reply, nullptr);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_FALSE(results[0]->IsException());
  ASSERT_EQ(std::memcmp(results[0]->GetData()->Data(), return_object->data().data(),
                        return_object->data().size()),
            0);
  ASSERT_EQ(num_retries_, 0);
}

TEST_F(TaskManagerTest, TestTaskFailure) {
  auto spec = CreateTaskHelper(1);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(spec);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  auto error = rpc::ErrorType::WORKER_DIED;
  manager_.PendingTaskFailed(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);
  ASSERT_EQ(num_retries_, 0);
}

TEST_F(TaskManagerTest, TestTaskRetry) {
  auto spec = CreateTaskHelper(1);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(spec, num_retries);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  auto error = rpc::ErrorType::WORKER_DIED;
  for (int i = 0; i < num_retries; i++) {
    manager_.PendingTaskFailed(spec.TaskId(), error);
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    std::vector<std::shared_ptr<RayObject>> results;
    ASSERT_FALSE(store_->Get({return_id}, 1, 0, ctx, false, &results).ok());
    ASSERT_EQ(num_retries_, i + 1);
  }

  manager_.PendingTaskFailed(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
