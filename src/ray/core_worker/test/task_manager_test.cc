#include "gtest/gtest.h"

#include "ray/common/task/task_spec.h"
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

class TaskManagerTest : public ::testing::Test {
 public:
  TaskManagerTest()
      : store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        manager_(store_) {}

  std::shared_ptr<CoreWorkerMemoryStore> store_;
  TaskManager manager_;
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
  manager_.CompletePendingTask(spec.TaskId(), reply);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_FALSE(results[0]->IsException());
  ASSERT_EQ(std::memcmp(results[0]->GetData()->Data(), return_object->data().data(),
                        return_object->data().size()),
            0);
}

TEST_F(TaskManagerTest, TestTaskFailure) {
  auto spec = CreateTaskHelper(1);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(spec);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  auto error = rpc::ErrorType::WORKER_DIED;
  manager_.FailPendingTask(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
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
