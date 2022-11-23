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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/pubsub/mock_pubsub.h"

namespace ray {
namespace core {

TaskSpecification CreateTaskHelper(uint64_t num_returns,
                                   std::vector<ObjectID> dependencies,
                                   bool dynamic_returns = false) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::FromRandom(JobID::FromInt(1)).Binary());
  task.GetMutableMessage().set_num_returns(num_returns);
  for (const ObjectID &dep : dependencies) {
    task.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
        dep.Binary());
  }

  if (dynamic_returns) {
    task.GetMutableMessage().set_returns_dynamic(true);
  }

  return task;
}

rpc::Address GetRandomWorkerAddr() {
  rpc::Address addr;
  addr.set_worker_id(WorkerID::FromRandom().Binary());
  return addr;
}

class TaskManagerTest : public ::testing::Test {
 public:
  TaskManagerTest(bool lineage_pinning_enabled = false,
                  int64_t max_lineage_bytes = 1024 * 1024 * 1024)
      : addr_(GetRandomWorkerAddr()),
        publisher_(std::make_shared<mock_pubsub::MockPublisher>()),
        subscriber_(std::make_shared<mock_pubsub::MockSubscriber>()),
        reference_counter_(std::shared_ptr<ReferenceCounter>(new ReferenceCounter(
            addr_,
            publisher_.get(),
            subscriber_.get(),
            [this](const NodeID &node_id) { return all_nodes_alive_; },
            lineage_pinning_enabled))),
        store_(std::shared_ptr<CoreWorkerMemoryStore>(
            new CoreWorkerMemoryStore(reference_counter_))),
        manager_(
            store_,
            reference_counter_,
            [this](const RayObject &object, const ObjectID &object_id) {
              stored_in_plasma.insert(object_id);
            },
            [this](TaskSpecification &spec, bool object_recovery, uint32_t delay_ms) {
              num_retries_++;
              last_delay_ms_ = delay_ms;
              last_object_recovery_ = object_recovery;
              return Status::OK();
            },
            [](const JobID &job_id,
               const std::string &type,
               const std::string &error_message,
               double timestamp) { return Status::OK(); },
            max_lineage_bytes) {}

  virtual void TearDown() { AssertNoLeaks(); }

  void AssertNoLeaks() {
    absl::MutexLock lock(&manager_.mu_);
    ASSERT_EQ(manager_.submissible_tasks_.size(), 0);
    ASSERT_EQ(manager_.num_pending_tasks_, 0);
    ASSERT_EQ(manager_.total_lineage_footprint_bytes_, 0);
  }

  rpc::Address addr_;
  std::shared_ptr<mock_pubsub::MockPublisher> publisher_;
  std::shared_ptr<mock_pubsub::MockSubscriber> subscriber_;
  std::shared_ptr<ReferenceCounter> reference_counter_;
  std::shared_ptr<CoreWorkerMemoryStore> store_;
  bool all_nodes_alive_ = true;
  TaskManager manager_;
  int num_retries_ = 0;
  uint32_t last_delay_ms_ = 0;
  bool last_object_recovery_ = false;
  std::unordered_set<ObjectID> stored_in_plasma;
};

class TaskManagerLineageTest : public TaskManagerTest {
 public:
  TaskManagerLineageTest() : TaskManagerTest(true, /*max_lineage_bytes=*/10000) {}
};

TEST_F(TaskManagerTest, TestTaskSuccess) {
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(caller_address, spec, "");
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_FALSE(results[0]->IsException());
  ASSERT_EQ(std::memcmp(results[0]->GetData()->Data(),
                        return_object->data().data(),
                        return_object->data().size()),
            0);
  ASSERT_EQ(num_retries_, 0);

  std::vector<ObjectID> removed;
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestTaskFailure) {
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(caller_address, spec, "");
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  auto error = rpc::ErrorType::WORKER_DIED;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);
  ASSERT_EQ(num_retries_, 0);

  std::vector<ObjectID> removed;
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestPlasmaConcurrentFailure) {
  rpc::Address caller_address;
  auto spec = CreateTaskHelper(1, {});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  manager_.AddPendingTask(caller_address, spec, "");
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));

  ASSERT_TRUE(reference_counter_->FlushObjectsToRecover().empty());
  all_nodes_alive_ = false;

  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);

  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));

  std::vector<std::shared_ptr<RayObject>> results;
  // Caller of FlushObjectsToRecover is responsible for deleting the object
  // from the in-memory store and recovering the object.
  ASSERT_TRUE(store_->Get({return_id}, 1, 0, ctx, false, &results).ok());
  auto objects_to_recover = reference_counter_->FlushObjectsToRecover();
  ASSERT_EQ(objects_to_recover.size(), 1);
  ASSERT_EQ(objects_to_recover[0], return_id);
}

TEST_F(TaskManagerTest, TestFailPendingTask) {
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  manager_.FailPendingTask(spec.TaskId(), rpc::ErrorType::LOCAL_RAYLET_DIED);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, rpc::ErrorType::LOCAL_RAYLET_DIED);

  std::vector<ObjectID> removed;
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestTaskReconstruction) {
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  auto error = rpc::ErrorType::WORKER_DIED;
  for (int i = 0; i < num_retries; i++) {
    RAY_LOG(INFO) << "Retry " << i;
    manager_.FailOrRetryPendingTask(spec.TaskId(), error);
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));
    ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
    std::vector<std::shared_ptr<RayObject>> results;
    ASSERT_FALSE(store_->Get({return_id}, 1, 0, ctx, false, &results).ok());
    ASSERT_EQ(num_retries_, i + 1);
    ASSERT_EQ(last_delay_ms_, RayConfig::instance().task_retry_delay_ms());
    ASSERT_EQ(last_object_recovery_, false);
  }

  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // Only the return object reference should remain.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);

  std::vector<ObjectID> removed;
  reference_counter_->RemoveLocalReference(return_id, &removed);
  ASSERT_EQ(removed[0], return_id);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

TEST_F(TaskManagerTest, TestTaskKill) {
  rpc::Address caller_address;
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);
  auto return_id = spec.ReturnId(0);
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));

  manager_.MarkTaskCanceled(spec.TaskId());
  auto error = rpc::ErrorType::TASK_CANCELLED;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, error);
}

TEST_F(TaskManagerTest, TestTaskOomKillNoOomRetryFailsImmediately) {
  RayConfig::instance().initialize(R"({"task_oom_retries": 0})");

  {
    int num_retries = 10;

    rpc::Address caller_address;
    auto spec = CreateTaskHelper(1, {});
    manager_.AddPendingTask(caller_address, spec, "", num_retries);
    auto return_id = spec.ReturnId(0);

    auto error = rpc::ErrorType::OUT_OF_MEMORY;
    manager_.FailOrRetryPendingTask(spec.TaskId(), error);

    std::vector<std::shared_ptr<RayObject>> results;
    WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
    RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
    ASSERT_EQ(results.size(), 1);
    rpc::ErrorType stored_error;
    ASSERT_TRUE(results[0]->IsException(&stored_error));
    ASSERT_EQ(stored_error, error);
  }

  {
    int num_retries = -1;

    rpc::Address caller_address;
    auto spec = CreateTaskHelper(1, {});
    manager_.AddPendingTask(caller_address, spec, "", num_retries);
    auto return_id = spec.ReturnId(0);

    auto error = rpc::ErrorType::OUT_OF_MEMORY;
    manager_.FailOrRetryPendingTask(spec.TaskId(), error);

    std::vector<std::shared_ptr<RayObject>> results;
    WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
    RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
    ASSERT_EQ(results.size(), 1);
    rpc::ErrorType stored_error;
    ASSERT_TRUE(results[0]->IsException(&stored_error));
    ASSERT_EQ(stored_error, error);
  }
}

TEST_F(TaskManagerTest, TestTaskOomAndNonOomKillReturnsLastError) {
  RayConfig::instance().initialize(R"({"task_oom_retries": 1})");
  int num_retries = 1;

  rpc::Address caller_address;
  auto spec = CreateTaskHelper(1, {});
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);

  ASSERT_EQ(num_retries_, 0);
  ray::rpc::ErrorType error;

  error = rpc::ErrorType::OUT_OF_MEMORY;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(last_delay_ms_, RayConfig::instance().task_oom_retry_delay_base_ms());
  ASSERT_EQ(last_object_recovery_, false);

  error = rpc::ErrorType::WORKER_DIED;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_EQ(num_retries_, 2);
  ASSERT_EQ(last_delay_ms_, RayConfig::instance().task_retry_delay_ms());
  ASSERT_EQ(last_object_recovery_, false);

  error = rpc::ErrorType::WORKER_DIED;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_EQ(num_retries_, 2);

  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, rpc::ErrorType::WORKER_DIED);
}

TEST_F(TaskManagerTest, TestTaskNotRetriableOomFailsImmediatelyEvenWithOomRetryCounter) {
  RayConfig::instance().initialize(R"({"task_oom_retries": 1})");
  int num_retries = 0;

  rpc::Address caller_address;
  auto spec = CreateTaskHelper(1, {});
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);

  ASSERT_EQ(num_retries_, 0);
  ray::rpc::ErrorType error;

  error = rpc::ErrorType::OUT_OF_MEMORY;
  manager_.FailOrRetryPendingTask(spec.TaskId(), error);
  ASSERT_EQ(num_retries_, 0);

  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  rpc::ErrorType stored_error;
  ASSERT_TRUE(results[0]->IsException(&stored_error));
  ASSERT_EQ(stored_error, rpc::ErrorType::OUT_OF_MEMORY);
}

// Test to make sure that the task spec and dependencies for an object are
// evicted when lineage pinning is disabled in the ReferenceCounter.
TEST_F(TaskManagerTest, TestLineageEvicted) {
  rpc::Address caller_address;
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);

  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  auto return_id = spec.ReturnId(0);
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  // The task is still pinned because its return ID is still in scope.
  ASSERT_TRUE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // The dependencies should not be pinned because lineage pinning is
  // disabled.
  ASSERT_FALSE(reference_counter_->HasReference(dep1));
  ASSERT_FALSE(reference_counter_->HasReference(dep2));
  ASSERT_TRUE(reference_counter_->HasReference(return_id));

  // Once the return ID goes out of scope, the task spec and its dependencies
  // are released.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_FALSE(reference_counter_->HasReference(return_id));
}

TEST_F(TaskManagerTest, TestLocalityDataAdded) {
  auto spec = CreateTaskHelper(1, {});
  auto return_id = spec.ReturnId(0);
  auto node_id = NodeID::FromRandom();
  int object_size = 100;
  store_->GetAsync(return_id, [&](std::shared_ptr<RayObject> obj) {
    // By the time the return object is available to get, we should be able
    // to get the locality data too.
    auto locality_data = reference_counter_->GetLocalityData(return_id);
    ASSERT_TRUE(locality_data.has_value());
    ASSERT_EQ(locality_data->object_size, object_size);
    ASSERT_TRUE(locality_data->nodes_containing_object.contains(node_id));
  });

  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  return_object->set_in_plasma(true);
  return_object->set_size(object_size);
  rpc::Address worker_addr;
  worker_addr.set_raylet_id(node_id.Binary());
  manager_.AddPendingTask(rpc::Address(), spec, "", 0);
  manager_.CompletePendingTask(spec.TaskId(), reply, worker_addr, false);
}

// Test to make sure that the task spec and dependencies for an object are
// pinned when lineage pinning is enabled in the ReferenceCounter.
TEST_F(TaskManagerLineageTest, TestLineagePinned) {
  rpc::Address caller_address;
  // Submit a task with 2 arguments.
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);

  // The task completes.
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  // The task should still be in the lineage because its return ID is in scope.
  ASSERT_TRUE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_TRUE(reference_counter_->HasReference(dep1));
  ASSERT_TRUE(reference_counter_->HasReference(dep2));
  ASSERT_TRUE(reference_counter_->HasReference(return_id));

  // All lineage should be erased.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_FALSE(reference_counter_->HasReference(dep1));
  ASSERT_FALSE(reference_counter_->HasReference(dep2));
  ASSERT_FALSE(reference_counter_->HasReference(return_id));
}

// Test to make sure that the task spec and dependencies for an object are
// evicted if the object is returned by value, instead of stored in plasma.
TEST_F(TaskManagerLineageTest, TestDirectObjectNoLineage) {
  rpc::Address caller_address;
  // Submit a task with 2 arguments.
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);

  // The task completes.
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  return_object->set_in_plasma(false);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  // All lineage should be erased because the return object was not stored in
  // plasma.
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(reference_counter_->HasReference(dep1));
  ASSERT_FALSE(reference_counter_->HasReference(dep2));
  ASSERT_TRUE(reference_counter_->HasReference(return_id));
}

// Test to make sure that the task spec and dependencies for an object are
// pinned if the object goes out of scope before the task finishes. This is
// needed in case the pending task fails and needs to be retried.
TEST_F(TaskManagerLineageTest, TestLineagePinnedOutOfOrder) {
  rpc::Address caller_address;
  // Submit a task with 2 arguments.
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  int num_retries = 3;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);

  // The return ID goes out of scope. The lineage should still be pinned
  // because the task has not completed yet.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_TRUE(reference_counter_->HasReference(dep1));
  ASSERT_TRUE(reference_counter_->HasReference(dep2));
  ASSERT_FALSE(reference_counter_->HasReference(return_id));

  // The task completes.
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  // All lineage should be erased.
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(reference_counter_->HasReference(dep1));
  ASSERT_FALSE(reference_counter_->HasReference(dep2));
  ASSERT_FALSE(reference_counter_->HasReference(return_id));
}

// Test for pinning the lineage of an object, where the lineage is a chain of
// tasks that each depend on the previous. All tasks should be pinned until the
// final object goes out of scope.
TEST_F(TaskManagerLineageTest, TestRecursiveLineagePinned) {
  rpc::Address caller_address;

  ObjectID dep = ObjectID::FromRandom();
  for (int i = 0; i < 3; i++) {
    auto spec = CreateTaskHelper(1, {dep});
    int num_retries = 3;
    manager_.AddPendingTask(caller_address, spec, "", num_retries);
    auto return_id = spec.ReturnId(0);

    // The task completes.
    manager_.MarkDependenciesResolved(spec.TaskId());
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object = reply.add_return_objects();
    return_object->set_object_id(return_id.Binary());
    auto data = GenerateRandomBuffer();
    return_object->set_data(data->Data(), data->Size());
    return_object->set_in_plasma(true);
    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);

    // All tasks should be pinned in the lineage.
    ASSERT_EQ(manager_.NumSubmissibleTasks(), i + 1);
    // All objects in the lineage of the newest return ID, plus the return ID
    // itself, should be pinned.
    ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), i + 2);

    reference_counter_->RemoveLocalReference(dep, nullptr);
    dep = return_id;
  }

  // The task's return ID goes out of scope before the task finishes.
  reference_counter_->RemoveLocalReference(dep, nullptr);
  ASSERT_EQ(manager_.NumSubmissibleTasks(), 0);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

// Test for evicting the lineage of an object passed by value, where the
// lineage is a chain of tasks that each depend on the previous and each return
// a direct value. All tasks should be evicted as soon as they complete, even
// though the final object is still in scope.
TEST_F(TaskManagerLineageTest, TestRecursiveDirectObjectNoLineage) {
  rpc::Address caller_address;

  ObjectID dep = ObjectID::FromRandom();
  reference_counter_->AddLocalReference(dep, "");
  for (int i = 0; i < 3; i++) {
    auto spec = CreateTaskHelper(1, {dep});
    int num_retries = 3;
    manager_.AddPendingTask(caller_address, spec, "", num_retries);
    auto return_id = spec.ReturnId(0);
    reference_counter_->RemoveLocalReference(dep, nullptr);

    // The task completes.
    manager_.MarkDependenciesResolved(spec.TaskId());
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object = reply.add_return_objects();
    return_object->set_object_id(return_id.Binary());
    auto data = GenerateRandomBuffer();
    return_object->set_data(data->Data(), data->Size());
    return_object->set_in_plasma(false);
    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);

    // No tasks should be pinned because they returned direct objects.
    ASSERT_EQ(manager_.NumSubmissibleTasks(), 0);
    // Only the newest return ID should be in scope because all objects in the
    // lineage were direct.
    ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 1);

    dep = return_id;
  }

  reference_counter_->RemoveLocalReference(dep, nullptr);
  ASSERT_EQ(manager_.NumSubmissibleTasks(), 0);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

// Test to make sure that the task manager only resubmits tasks whose specs are
// pinned and that are not already pending execution.
TEST_F(TaskManagerLineageTest, TestResubmitTask) {
  rpc::Address caller_address;
  // Submit a task with 2 arguments.
  ObjectID dep1 = ObjectID::FromRandom();
  ObjectID dep2 = ObjectID::FromRandom();
  auto spec = CreateTaskHelper(1, {dep1, dep2});
  auto return_id = spec.ReturnId(0);
  int num_retries = 3;

  // Cannot resubmit a task whose spec we do not have.
  std::vector<ObjectID> resubmitted_task_deps;
  ASSERT_FALSE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_TRUE(resubmitted_task_deps.empty());
  ASSERT_EQ(num_retries_, 0);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  // A task that is already pending does not get resubmitted.
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_TRUE(resubmitted_task_deps.empty());
  ASSERT_EQ(num_retries_, 0);
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  // The task completes.
  manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
  ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  // The task finished, its return ID is still in scope, and the return object
  // was stored in plasma. It is okay to resubmit it now.
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_EQ(resubmitted_task_deps, spec.GetDependencyIds());
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(last_delay_ms_, 0);
  ASSERT_EQ(last_object_recovery_, true);
  resubmitted_task_deps.clear();
  ASSERT_TRUE(reference_counter_->IsObjectPendingCreation(return_id));

  // The return ID goes out of scope.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  // The task is still pending execution.
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  // A task that is already pending does not get resubmitted.
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_TRUE(resubmitted_task_deps.empty());
  ASSERT_EQ(num_retries_, 1);
  // Object is out of scope, so no longer pending creation.
  ASSERT_FALSE(reference_counter_->IsObjectPendingCreation(return_id));

  // The resubmitted task finishes.
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  ASSERT_FALSE(manager_.IsTaskPending(spec.TaskId()));
  // The task cannot be resubmitted because its spec has been released.
  ASSERT_FALSE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_TRUE(resubmitted_task_deps.empty());
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

// Test resubmission for a task that was successfully executed once and stored
// its return values in plasma. On re-execution, the task's return values
// should be stored in plasma again, even if the worker returns its values
// directly.
TEST_F(TaskManagerLineageTest, TestResubmittedTaskNondeterministicReturns) {
  rpc::Address caller_address;
  auto spec = CreateTaskHelper(2, {});
  auto return_id1 = spec.ReturnId(0);
  auto return_id2 = spec.ReturnId(1);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/1);
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

  // The task completes. Both return objects are stored in plasma.
  {
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object1 = reply.add_return_objects();
    return_object1->set_object_id(return_id1.Binary());
    auto data = GenerateRandomBuffer();
    return_object1->set_data(data->Data(), data->Size());
    return_object1->set_in_plasma(true);
    auto return_object2 = reply.add_return_objects();
    return_object2->set_object_id(return_id2.Binary());
    return_object2->set_data(data->Data(), data->Size());
    return_object2->set_in_plasma(true);
    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  }

  // The task finished, its return ID is still in scope, and the return object
  // was stored in plasma. It is okay to resubmit it now.
  ASSERT_TRUE(stored_in_plasma.empty());
  std::vector<ObjectID> resubmitted_task_deps;
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(last_delay_ms_, 0);
  ASSERT_EQ(last_object_recovery_, true);

  // The re-executed task completes again. One of the return objects is now
  // returned directly.
  {
    reference_counter_->AddLocalReference(return_id1, "");
    reference_counter_->AddLocalReference(return_id2, "");
    manager_.MarkDependenciesResolved(spec.TaskId());
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object1 = reply.add_return_objects();
    return_object1->set_object_id(return_id1.Binary());
    auto data = GenerateRandomBuffer();
    return_object1->set_data(data->Data(), data->Size());
    return_object1->set_in_plasma(false);
    auto return_object2 = reply.add_return_objects();
    return_object2->set_object_id(return_id2.Binary());
    return_object2->set_data(data->Data(), data->Size());
    return_object2->set_in_plasma(true);
    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  }
  ASSERT_TRUE(stored_in_plasma.count(return_id1));
  ASSERT_FALSE(stored_in_plasma.count(return_id2));
}

// Test that we update ref counter correctly for tasks with
// num_returns="dynamic".
TEST_F(TaskManagerLineageTest, TestResubmittedTaskFails) {
  rpc::Address caller_address;
  auto spec = CreateTaskHelper(2, {});
  auto return_id1 = spec.ReturnId(0);
  auto return_id2 = spec.ReturnId(1);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/1);
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

  // The task completes. One return object is stored in plasma.
  {
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object1 = reply.add_return_objects();
    return_object1->set_object_id(return_id1.Binary());
    auto data = GenerateRandomBuffer();
    return_object1->set_data(data->Data(), data->Size());
    return_object1->set_in_plasma(true);
    auto return_object2 = reply.add_return_objects();
    return_object2->set_object_id(return_id2.Binary());
    return_object2->set_data(data->Data(), data->Size());
    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  }

  // The task finished, its return ID is still in scope, and the return object
  // was stored in plasma. It is okay to resubmit it now.
  ASSERT_TRUE(stored_in_plasma.empty());
  std::vector<ObjectID> resubmitted_task_deps;
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(last_delay_ms_, 0);
  ASSERT_EQ(last_object_recovery_, true);

  // The re-executed task fails due to worker crashed.
  {
    reference_counter_->AddLocalReference(return_id1, "");
    reference_counter_->AddLocalReference(return_id2, "");
    manager_.MarkDependenciesResolved(spec.TaskId());
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

    manager_.FailOrRetryPendingTask(spec.TaskId(), rpc::ErrorType::WORKER_DIED);
  }
  ASSERT_TRUE(stored_in_plasma.count(return_id1));
  ASSERT_FALSE(stored_in_plasma.count(return_id2));
}

// Test submission and resubmission for a task with dynamic returns.
TEST_F(TaskManagerLineageTest, TestDynamicReturnsTask) {
  auto spec = CreateTaskHelper(1, {}, /*dynamic_returns=*/true);
  auto return_id = spec.ReturnId(0);
  manager_.AddPendingTask(addr_, spec, "", /*num_retries=*/1);
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

  std::vector<ObjectID> dynamic_return_ids;

  // The task completes and returns dynamic returns.
  {
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object = reply.add_return_objects();
    return_object->set_object_id(return_id.Binary());
    auto data = GenerateRandomBuffer();
    return_object->set_data(data->Data(), data->Size());

    for (int i = 0; i < 3; i++) {
      auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
      dynamic_return_ids.push_back(dynamic_return_id);
      auto dynamic_return_object = reply.add_dynamic_return_objects();
      dynamic_return_object->set_object_id(dynamic_return_id.Binary());
      dynamic_return_object->set_data(data->Data(), data->Size());
      dynamic_return_object->set_in_plasma(true);
    }

    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  }

  // The task finished, its return ID is still in scope, and the return object
  // was stored in plasma. It is okay to resubmit it now.
  ASSERT_TRUE(stored_in_plasma.empty());

  // Generator ref and all 3 internal refs are in scope.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 4);
  for (const auto &dynamic_return_id : dynamic_return_ids) {
    rpc::Address owner_addr;
    ASSERT_TRUE(reference_counter_->GetOwner(dynamic_return_id, &owner_addr));
    ASSERT_EQ(owner_addr.worker_id(), addr_.worker_id());
  }

  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  std::vector<std::shared_ptr<RayObject>> results;
  RAY_CHECK_OK(store_->Get(dynamic_return_ids, 3, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 3);
  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(results[i]->IsInPlasmaError());
  }
  // If we remove the generator ref, all internal refs also go out of scope.
  // This is equivalent to deleting the generator ObjectRef without iterating
  // over its internal ObjectRefs.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
}

// Test resubmission for a task with num_returns="dynamic" that returns objects
// in plasma. If the task fails, we should store errors for all internal
// ObjectRefs in plasma.
TEST_F(TaskManagerLineageTest, TestResubmittedDynamicReturnsTaskFails) {
  auto spec = CreateTaskHelper(1, {}, /*dynamic_returns=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(addr_, spec, "", /*num_retries=*/1);
  manager_.MarkDependenciesResolved(spec.TaskId());
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

  std::vector<ObjectID> dynamic_return_ids;

  // The task completes and returns dynamic returns.
  {
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    rpc::PushTaskReply reply;
    auto return_object = reply.add_return_objects();
    return_object->set_object_id(generator_id.Binary());
    auto data = GenerateRandomBuffer();
    return_object->set_data(data->Data(), data->Size());

    for (int i = 0; i < 3; i++) {
      auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
      dynamic_return_ids.push_back(dynamic_return_id);
      auto dynamic_return_object = reply.add_dynamic_return_objects();
      dynamic_return_object->set_object_id(dynamic_return_id.Binary());
      dynamic_return_object->set_data(data->Data(), data->Size());
      dynamic_return_object->set_in_plasma(true);
    }

    manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);
  }

  // Resubmit the task.
  ASSERT_TRUE(stored_in_plasma.empty());
  std::vector<ObjectID> resubmitted_task_deps;
  ASSERT_TRUE(manager_.ResubmitTask(spec.TaskId(), &resubmitted_task_deps));
  ASSERT_EQ(num_retries_, 1);
  ASSERT_EQ(last_delay_ms_, 0);
  ASSERT_EQ(last_object_recovery_, true);

  // Dereference the generator to a list of its internal ObjectRefs.
  for (const auto &dynamic_return_id : dynamic_return_ids) {
    reference_counter_->AddLocalReference(dynamic_return_id, "");
  }
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  store_->Delete({generator_id});

  // The re-executed task fails.
  {
    manager_.MarkDependenciesResolved(spec.TaskId());
    ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
    ASSERT_FALSE(manager_.IsTaskWaitingForExecution(spec.TaskId()));
    manager_.MarkTaskWaitingForExecution(spec.TaskId(), NodeID::FromRandom());
    ASSERT_TRUE(manager_.IsTaskWaitingForExecution(spec.TaskId()));

    manager_.FailOrRetryPendingTask(spec.TaskId(), rpc::ErrorType::WORKER_DIED);
  }

  // No error stored for the generator ID, which should have gone out of scope.
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  std::vector<std::shared_ptr<RayObject>> results;
  ASSERT_FALSE(store_->Get({generator_id}, 1, 0, ctx, false, &results).ok());

  // The internal ObjectRefs have the right error.
  RAY_CHECK_OK(store_->Get(dynamic_return_ids, 3, -1, ctx, false, &results));
  ASSERT_EQ(results.size(), 3);
  for (int i = 0; i < 3; i++) {
    rpc::ErrorType stored_error;
    ASSERT_TRUE(results[i]->IsException(&stored_error));
    ASSERT_EQ(stored_error, rpc::ErrorType::OBJECT_IN_PLASMA);
  }
  ASSERT_EQ(stored_in_plasma.size(), 3);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
