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

#include "ray/core_worker/task_manager.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

TaskSpecification CreateTaskHelper(uint64_t num_returns,
                                   std::vector<ObjectID> dependencies) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::ForFakeTask().Binary());
  task.GetMutableMessage().set_num_returns(num_returns);
  for (const ObjectID &dep : dependencies) {
    task.GetMutableMessage().add_args()->add_object_ids(dep.Binary());
  }
  return task;
}

class MockActorManager : public ActorManagerInterface {
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override {
    num_terminations += 1;
  }

  int num_terminations = 0;
};

class TaskManagerTest : public ::testing::Test {
 public:
  TaskManagerTest()
      : store_(std::shared_ptr<CoreWorkerMemoryStore>(new CoreWorkerMemoryStore())),
        reference_counter_(
            std::shared_ptr<ReferenceCounter>(new ReferenceCounter(rpc::Address()))),
        actor_manager_(std::shared_ptr<ActorManagerInterface>(new MockActorManager())),
        manager_(store_, reference_counter_, actor_manager_,
                 [this](const TaskSpecification &spec) {
                   num_retries_++;
                   return Status::OK();
                 }) {}

  std::shared_ptr<CoreWorkerMemoryStore> store_;
  std::shared_ptr<ReferenceCounter> reference_counter_;
  std::shared_ptr<ActorManagerInterface> actor_manager_;
  TaskManager manager_;
  int num_retries_ = 0;
};

TEST_F(TaskManagerTest, TestTaskSuccess) {
  TaskID caller_id = TaskID::Nil();
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(caller_id, caller_address, spec);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address());
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_FALSE(results[0]->IsException());
  ASSERT_EQ(std::memcmp(results[0]->GetData()->Data(), return_object->data().data(),
                        return_object->data().size()),
            0);
  ASSERT_EQ(num_retries_, 0);

  std::vector<ObjectID> removed;
  reference_counter_->AddLocalReference(return_id);
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestTaskFailure) {
  TaskID caller_id = TaskID::Nil();
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(caller_id, caller_address, spec);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  auto error = rpc::ErrorType::WORKER_DIED;
  manager_.PendingTaskFailed(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);
  ASSERT_EQ(num_retries_, 0);

  std::vector<ObjectID> removed;
  reference_counter_->AddLocalReference(return_id);
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestTaskRetry) {
  TaskID caller_id = TaskID::Nil();
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_id, caller_address, spec, num_retries);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0, TaskTransportType::DIRECT);
  WorkerContext ctx(WorkerType::WORKER, JobID::FromInt(0));

  auto error = rpc::ErrorType::WORKER_DIED;
  for (int i = 0; i < num_retries; i++) {
    manager_.PendingTaskFailed(spec.TaskId(), error);
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
    std::vector<std::shared_ptr<RayObject>> results;
    ASSERT_FALSE(store_->Get({return_id}, 1, 0, ctx, false, &results).ok());
    ASSERT_EQ(num_retries_, i + 1);
  }

  manager_.PendingTaskFailed(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);

  std::vector<ObjectID> removed;
  reference_counter_->AddLocalReference(return_id);
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
