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
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/pubsub/subscriber.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_event_buffer.h"

namespace ray {
namespace core {

TaskSpecification CreateTaskHelper(uint64_t num_returns,
                                   std::vector<ObjectID> dependencies,
                                   bool dynamic_returns = false,
                                   bool streaming_generator = false,
                                   int64_t generator_backpressure_num_objects = -1) {
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
  if (streaming_generator) {
    task.GetMutableMessage().set_streaming_generator(true);
    task.GetMutableMessage().set_generator_backpressure_num_objects(
        generator_backpressure_num_objects);
  }

  return task;
}

rpc::Address GetRandomWorkerAddr() {
  rpc::Address addr;
  addr.set_worker_id(WorkerID::FromRandom().Binary());
  return addr;
}

rpc::ReportGeneratorItemReturnsRequest GetIntermediateTaskReturn(
    int64_t idx,
    bool finished,
    const ObjectID &generator_id,
    const ObjectID &dynamic_return_id,
    std::shared_ptr<Buffer> data,
    bool set_in_plasma) {
  rpc::ReportGeneratorItemReturnsRequest request;
  rpc::Address addr;
  request.mutable_worker_addr()->CopyFrom(addr);
  request.set_item_index(idx);
  request.set_generator_id(generator_id.Binary());
  auto dynamic_return_object = request.add_dynamic_return_objects();
  dynamic_return_object->set_object_id(dynamic_return_id.Binary());
  dynamic_return_object->set_data(data->Data(), data->Size());
  dynamic_return_object->set_in_plasma(set_in_plasma);
  return request;
}

rpc::ReportGeneratorItemReturnsRequest GetEoFTaskReturn(int64_t idx,
                                                        const ObjectID &generator_id) {
  rpc::ReportGeneratorItemReturnsRequest request;
  rpc::Address addr;
  request.mutable_worker_addr()->CopyFrom(addr);
  request.set_item_index(idx);
  request.set_generator_id(generator_id.Binary());
  return request;
}

class MockTaskEventBuffer : public worker::TaskEventBuffer {
 public:
  MOCK_METHOD(void,
              AddTaskEvent,
              (std::unique_ptr<worker::TaskEvent> task_event),
              (override));

  MOCK_METHOD(void, FlushEvents, (bool forced), (override));

  MOCK_METHOD(Status, Start, (bool manual_flush), (override));

  MOCK_METHOD(void, Stop, (), (override));

  MOCK_METHOD(bool, Enabled, (), (const, override));

  MOCK_METHOD(std::string, DebugString, (), (override));
};

class TaskManagerTest : public ::testing::Test {
 public:
  explicit TaskManagerTest(bool lineage_pinning_enabled = false,
                           int64_t max_lineage_bytes = 1024 * 1024 * 1024)
      : lineage_pinning_enabled_(lineage_pinning_enabled),
        addr_(GetRandomWorkerAddr()),
        publisher_(std::make_shared<pubsub::MockPublisher>()),
        subscriber_(std::make_shared<pubsub::MockSubscriber>()),
        task_event_buffer_mock_(std::make_unique<MockTaskEventBuffer>()),
        reference_counter_(std::make_shared<ReferenceCounter>(
            addr_,
            publisher_.get(),
            subscriber_.get(),
            [this](const NodeID &node_id) { return all_nodes_alive_; },
            lineage_pinning_enabled)),
        io_context_("TaskManagerTest"),
        store_(std::make_shared<CoreWorkerMemoryStore>(io_context_.GetIoService(),
                                                       reference_counter_.get())),
        manager_(
            *store_,
            *reference_counter_,
            [this](const RayObject &object, const ObjectID &object_id) {
              stored_in_plasma.insert(object_id);
            },
            [this](TaskSpecification &spec,
                   bool object_recovery,
                   bool update_seqno,
                   uint32_t delay_ms) {
              num_retries_++;
              last_delay_ms_ = delay_ms;
              last_object_recovery_ = object_recovery;
              return Status::OK();
            },
            [](const JobID &job_id,
               const std::string &type,
               const std::string &error_message,
               double timestamp) { return Status::OK(); },
            max_lineage_bytes,
            *task_event_buffer_mock_.get()) {}

  virtual void TearDown() { AssertNoLeaks(); }

  void AssertNoLeaks() {
    absl::MutexLock lock(&manager_.mu_);
    ASSERT_EQ(manager_.submissible_tasks_.size(), 0);
    ASSERT_EQ(manager_.num_pending_tasks_, 0);
    ASSERT_EQ(manager_.total_lineage_footprint_bytes_, 0);
  }

  void CompletePendingStreamingTask(const TaskSpecification &spec,
                                    const rpc::Address &caller_address,
                                    int64_t num_streaming_generator_returns,
                                    bool set_in_plasma = false) {
    rpc::PushTaskReply reply;
    for (size_t i = 0; i < spec.NumReturns(); i++) {
      const auto return_id = spec.ReturnId(i);
      auto return_object = reply.add_return_objects();
      return_object->set_object_id(return_id.Binary());
      auto data = GenerateRandomBuffer();
      return_object->set_data(data->Data(), data->Size());
    }
    for (int64_t i = 0; i < num_streaming_generator_returns; i++) {
      auto return_id_proto = reply.add_streaming_generator_return_ids();
      return_id_proto->set_object_id(spec.ReturnId(i + 1).Binary());
      return_id_proto->set_is_plasma_object(set_in_plasma);
    }
    manager_.CompletePendingTask(spec.TaskId(), reply, caller_address, false);
  }

  bool lineage_pinning_enabled_;
  rpc::Address addr_;
  std::shared_ptr<pubsub::MockPublisher> publisher_;
  std::shared_ptr<pubsub::MockSubscriber> subscriber_;
  std::unique_ptr<MockTaskEventBuffer> task_event_buffer_mock_;
  std::shared_ptr<ReferenceCounter> reference_counter_;
  InstrumentedIOContextWithThread io_context_;
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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

TEST_F(TaskManagerTest, TestTaskOomInfiniteRetry) {
  RayConfig::instance().initialize(R"({"task_oom_retries": -1})");

  rpc::Address caller_address;
  auto spec = CreateTaskHelper(1, {});
  int num_retries = 1;
  manager_.AddPendingTask(caller_address, spec, "", num_retries);

  for (int i = 0; i < 10000; i++) {
    ASSERT_EQ(num_retries_, i);
    manager_.FailOrRetryPendingTask(spec.TaskId(), rpc::ErrorType::OUT_OF_MEMORY);
  }

  manager_.MarkTaskCanceled(spec.TaskId());
  manager_.FailOrRetryPendingTask(spec.TaskId(), rpc::ErrorType::TASK_CANCELLED);
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

TEST_F(TaskManagerTest, TestFailsImmediatelyOverridesRetry) {
  RayConfig::instance().initialize(R"({"task_oom_retries": 1})");

  {
    ray::rpc::ErrorType error = rpc::ErrorType::OUT_OF_MEMORY;

    rpc::Address caller_address;
    auto spec = CreateTaskHelper(1, {});
    manager_.AddPendingTask(caller_address, spec, "", /*max retries*/ 10);
    auto return_id = spec.ReturnId(0);

    manager_.FailOrRetryPendingTask(spec.TaskId(),
                                    error,
                                    /*status*/ nullptr,
                                    /*error info*/ nullptr,
                                    /*mark object failed*/ true,
                                    /*fail immediately*/ true);

    std::vector<std::shared_ptr<RayObject>> results;
    WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
    RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
    ASSERT_EQ(results.size(), 1);
    rpc::ErrorType stored_error;
    ASSERT_TRUE(results[0]->IsException(&stored_error));
    ASSERT_EQ(stored_error, error);
  }

  {
    ray::rpc::ErrorType error = rpc::ErrorType::WORKER_DIED;

    rpc::Address caller_address;
    auto spec = CreateTaskHelper(1, {});
    manager_.AddPendingTask(caller_address, spec, "", /*max retries*/ 10);
    auto return_id = spec.ReturnId(0);

    manager_.FailOrRetryPendingTask(spec.TaskId(),
                                    error,
                                    /*status*/ nullptr,
                                    /*error info*/ nullptr,
                                    /*mark object failed*/ true,
                                    /*fail immediately*/ true);

    std::vector<std::shared_ptr<RayObject>> results;
    WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
    RAY_CHECK_OK(store_->Get({return_id}, 1, 0, ctx, false, &results));
    ASSERT_EQ(results.size(), 1);
    rpc::ErrorType stored_error;
    ASSERT_TRUE(results[0]->IsException(&stored_error));
    ASSERT_EQ(stored_error, error);
  }
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
  store_->GetAsync(
      return_id, [return_id, object_size, node_id, this](std::shared_ptr<RayObject> obj) {
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

// Test to make sure that the task spec and actor
// for an actor task return object are
// pinned when lineage pinning is enabled in the ReferenceCounter.
TEST_F(TaskManagerLineageTest, TestActorLineagePinned) {
  rpc::Address caller_address;
  ActorID actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  const ObjectID actor_creation_dummy_object_id =
      ObjectID::FromIndex(TaskID::ForActorCreationTask(actor_id), /*index=*/1);
  int num_retries = 3;
  TaskSpecBuilder builder;
  builder.SetCommonTaskSpec(
      TaskID::ForActorTask(JobID::Nil(), TaskID::Nil(), 0, actor_id),
      "dummy_actor_task",
      Language::PYTHON,
      FunctionDescriptorBuilder::BuildPython("a", "", "", ""),
      JobID::Nil(),
      rpc::JobConfig(),
      TaskID::Nil(),
      0,
      TaskID::Nil(),
      rpc::Address(),
      1,
      false,
      false,
      -1,
      {},
      {},
      "",
      0,
      TaskID::Nil(),
      "");
  builder.SetActorTaskSpec(
      actor_id, actor_creation_dummy_object_id, num_retries, false, "", 0);
  TaskSpecification spec = std::move(builder).ConsumeAndBuild();

  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  manager_.AddPendingTask(caller_address, spec, "", num_retries);
  auto return_id = spec.ReturnId(0);
  ASSERT_TRUE(manager_.IsTaskPending(spec.TaskId()));
  // 2 objects are in scope: actor handle and actor task return object.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 2);

  // The task completes.
  rpc::PushTaskReply reply;
  auto return_object = reply.add_return_objects();
  return_object->set_object_id(return_id.Binary());
  auto data = GenerateRandomBuffer();
  return_object->set_data(data->Data(), data->Size());
  return_object->set_in_plasma(true);
  manager_.CompletePendingTask(spec.TaskId(), reply, rpc::Address(), false);

  // The task should still be in the lineage because its return ID is in scope.
  ASSERT_TRUE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_TRUE(reference_counter_->HasReference(spec.ActorCreationDummyObjectId()));
  ASSERT_TRUE(reference_counter_->HasReference(return_id));

  // All lineage should be erased.
  reference_counter_->RemoveLocalReference(return_id, nullptr);
  ASSERT_FALSE(manager_.IsTaskSubmissible(spec.TaskId()));
  ASSERT_FALSE(reference_counter_->HasReference(spec.ActorCreationDummyObjectId()));
  ASSERT_FALSE(reference_counter_->HasReference(return_id));
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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
    manager_.MarkTaskWaitingForExecution(
        spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());
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

TEST_F(TaskManagerTest, TestObjectRefStreamCreateDelete) {
  /**
   * Test create and deletion of stream works.
   * CREATE EXISTS (true) DELETE EXISTS (false)
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);
  ASSERT_TRUE(manager_.ObjectRefStreamExists(generator_id));
  // Deletion does not succeed until task is completed too.
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_TRUE(manager_.ObjectRefStreamExists(generator_id));
  // Test TryDelObjectRefStream is idempotent
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_TRUE(manager_.ObjectRefStreamExists(generator_id));

  // Task completes. Deletion succeeds.
  CompletePendingStreamingTask(spec, caller_address, 0);
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_FALSE(manager_.ObjectRefStreamExists(generator_id));
}

TEST_F(TaskManagerTest, TestObjectRefStreamDeletedStreamIgnored) {
  /**
   * Test that when DELETE is called, all subsequent Writes are ignored.
   * CREATE DELETE WRITE READ
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);
  CompletePendingStreamingTask(spec, caller_address, 0);
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_FALSE(manager_.ObjectRefStreamExists(generator_id));

  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();

  // WRITE
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
}

TEST_F(TaskManagerTest, TestObjectRefStreamBasic) {
  /**
   * Test the basic cases (write -> read).
   * CREATE WRITE, WRITE, WRITEEoF, READ, READ, KeyERROR DELETE
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  auto last_idx = 2;
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<std::shared_ptr<Buffer>> datas;
  for (auto i = 0; i < last_idx; i++) {
    auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
    dynamic_return_ids.push_back(dynamic_return_id);
    auto data = GenerateRandomBuffer();
    datas.push_back(data);

    auto req = GetIntermediateTaskReturn(
        /*idx*/ i,
        /*finished*/ false,
        generator_id,
        /*dynamic_return_id*/ dynamic_return_id,
        /*data*/ data,
        /*set_in_plasma*/ false);
    // WRITE * 2
    ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
        req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  }

  CompletePendingStreamingTask(spec, caller_address, last_idx);

  // Verify PeekObjectRefStream is idempotent and doesn't consume indexes.
  for (auto i = 0; i < 10; i++) {
    auto [obj_id, ready] = manager_.PeekObjectRefStream(generator_id);
    ASSERT_EQ(obj_id, dynamic_return_ids[0]);
  }

  ObjectID obj_id;
  for (auto i = 0; i < last_idx; i++) {
    // READ * 2
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj_id, dynamic_return_ids[i]);
  }
  // READ (EoF)
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.IsObjectRefEndOfStream());
  // DELETE
  manager_.TryDelObjectRefStream(generator_id);
}

TEST_F(TaskManagerTest, TestObjectRefStreamCancellation) {
  /**
   * Test streaming generator task cancelled during execution. The caller
   * should receive an EOF error the next time it tries to read from the stream
   * after the task has been marked cancelled, even if we already received a
   * value for that return index.
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  auto last_idx = 2;
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<std::shared_ptr<Buffer>> datas;
  for (auto i = 0; i < last_idx; i++) {
    auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
    dynamic_return_ids.push_back(dynamic_return_id);
    auto data = GenerateRandomBuffer();
    datas.push_back(data);

    auto req = GetIntermediateTaskReturn(
        /*idx*/ i,
        /*finished*/ false,
        generator_id,
        /*dynamic_return_id*/ dynamic_return_id,
        /*data*/ data,
        /*set_in_plasma*/ false);
    // WRITE * 2
    ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
        req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  }

  // Read first object.
  {
    ObjectID obj_id;
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj_id, dynamic_return_ids[0]);
  }

  manager_.MarkTaskCanceled(spec.TaskId());
  auto error = rpc::ErrorType::WORKER_DIED;
  ASSERT_FALSE(manager_.FailOrRetryPendingTask(spec.TaskId(), error));

  // Next object should return EOS error, even though we have a value stored
  // for the object.
  {
    ObjectID obj_id;
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.IsObjectRefEndOfStream());
    ASSERT_EQ(obj_id, dynamic_return_ids[1]);
  }

  manager_.TryDelObjectRefStream(generator_id);
}

TEST_F(TaskManagerTest, TestObjectRefStreamCancellationOutOfOrderReports) {
  /**
   * Test streaming generator task cancelled during execution, and the caller
   * receives out-of-order item reports. The caller should receive an EOF error
   * the next time it tries to read from the stream after the task has been
   * marked cancelled, instead of hanging waiting for that index to be
   * reported.
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  std::vector<int64_t> idx_to_report = {0, 3};
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<std::shared_ptr<Buffer>> datas;
  for (auto idx : idx_to_report) {
    auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), idx + 2);
    dynamic_return_ids.push_back(dynamic_return_id);
    auto data = GenerateRandomBuffer();
    datas.push_back(data);

    auto req = GetIntermediateTaskReturn(
        /*idx*/ idx,
        /*finished*/ false,
        generator_id,
        /*dynamic_return_id*/ dynamic_return_id,
        /*data*/ data,
        /*set_in_plasma*/ false);
    // WRITE * 2
    ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
        req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  }

  // Read first object.
  {
    ObjectID obj_id;
    int64_t idx_expected = 0;
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj_id, ObjectID::FromIndex(spec.TaskId(), idx_expected + 2));
  }

  manager_.MarkTaskCanceled(spec.TaskId());
  auto error = rpc::ErrorType::WORKER_DIED;
  ASSERT_FALSE(manager_.FailOrRetryPendingTask(spec.TaskId(), error));

  // Next object should return EOS error instead of blocking the caller to wait
  // for the index to be reported.
  {
    ObjectID obj_id;
    int64_t idx_expected = 1;
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.IsObjectRefEndOfStream());
    ASSERT_EQ(obj_id, ObjectID::FromIndex(spec.TaskId(), idx_expected + 2));
  }

  manager_.TryDelObjectRefStream(generator_id);
}

TEST_F(TaskManagerTest, TestPeekObjectReady) {
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);

  {
    auto [obj_id, ready] = manager_.PeekObjectRefStream(generator_id);
    ASSERT_FALSE(ready);
  }
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  {
    auto [obj_id, ready] = manager_.PeekObjectRefStream(generator_id);
    ASSERT_TRUE(ready);
  }
  CompletePendingStreamingTask(spec, caller_address, 1);
}

TEST_F(TaskManagerTest, TestObjectRefStreamMixture) {
  /**
   * Test the basic cases, but write and read are mixed up.
   * CREATE WRITE READ WRITE READ WRITEEoF KeyError DELETE
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  auto last_idx = 2;
  std::vector<ObjectID> dynamic_return_ids;
  std::vector<std::shared_ptr<Buffer>> datas;
  for (auto i = 0; i < last_idx; i++) {
    auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
    dynamic_return_ids.push_back(dynamic_return_id);
    auto data = GenerateRandomBuffer();
    datas.push_back(data);

    auto req = GetIntermediateTaskReturn(
        /*idx*/ i,
        /*finished*/ false,
        generator_id,
        /*dynamic_return_id*/ dynamic_return_id,
        /*data*/ data,
        /*set_in_plasma*/ false);
    // WRITE
    ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
        req, /*execution_signal_callback*/ [](Status, int64_t) {}));
    // READ
    ObjectID obj_id;
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj_id, dynamic_return_ids[i]);
  }
  // WRITEEoF
  CompletePendingStreamingTask(spec, caller_address, 2);

  ObjectID obj_id;
  // READ (EoF)
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.IsObjectRefEndOfStream());
  // DELETE
  manager_.TryDelObjectRefStream(generator_id);
}

TEST_F(TaskManagerTest, TestObjectRefEndOfStream) {
  /**
   * Test that after writing EoF, write/read doesn't work.
   * CREATE WRITE WRITEEoF, WRITE(verify no op) DELETE
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  CompletePendingStreamingTask(spec, caller_address, 1);
  // READ (works)
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // WRITE
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // READ (doesn't works because EoF is already written)
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.IsObjectRefEndOfStream());
}

TEST_F(TaskManagerTest, TestObjectRefStreamIndexDiscarded) {
  /**
   * Test that when the ObjectRefStream is already written
   * the WRITE will be ignored.
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // READ
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // WRITE to the first index again.
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // READ (New write will be ignored).
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, ObjectID::Nil());
  CompletePendingStreamingTask(spec, caller_address, 1);
}

TEST_F(TaskManagerTest, TestObjectRefStreamReadIgnoredWhenNothingWritten) {
  /**
   * Test read will return Nil if nothing was written.
   * CREATE READ (no op) WRITE READ (working) READ (no op)
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  // READ (no-op)
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, ObjectID::Nil());

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // READ (works this time)
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // READ (nothing should return)
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, ObjectID::Nil());
  CompletePendingStreamingTask(spec, caller_address, 1);
}

TEST_F(TaskManagerTest, TestObjectRefStreamEndtoEnd) {
  /**
   * Test e2e
   * (task submitted -> report intermediate task return -> task finished)
   * This also tests if we can read / write stream before / after task finishes.
   */
  // Submit a task.
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  // The results are reported before the task is finished.
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // NumObjectIDsInScope == Generator + intermediate result.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 2);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  RAY_CHECK_OK(store_->Get({dynamic_return_id}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);

  // Make sure you can read.
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // Finish the task.
  CompletePendingStreamingTask(spec, caller_address, 2);

  // Test you can write to the stream after task finishes.
  // TODO(sang): Make sure this doesn't happen by ensuring the ordering
  // from the executor side.
  auto dynamic_return_id2 = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id2,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // NumObjectIDsInScope == Generator + 2 intermediate result.
  results.clear();
  RAY_CHECK_OK(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);

  // Make sure you can read.
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id2);

  // Nothing more to read.
  status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.IsObjectRefEndOfStream());

  manager_.TryDelObjectRefStream(generator_id);
}

TEST_F(TaskManagerTest, TestObjectRefStreamDelCleanReferences) {
  /**
   * Verify DEL cleans all references/objects and ignore all future WRITE.
   *
   * CREATE WRITE WRITE DEL (make sure no refs are leaked)
   */
  // Submit a task so that generator ID will be available
  // to the reference counter.
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // WRITE 2
  auto dynamic_return_id2 = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id2,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // NumObjectIDsInScope == Generator + 2 WRITE
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  // 2 in memory objects.
  ASSERT_EQ(store_->Size(), 2);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  RAY_CHECK_OK(store_->Get({dynamic_return_id}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  results.clear();
  RAY_CHECK_OK(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  results.clear();

  // DELETE. This should clean all references except generator id.
  CompletePendingStreamingTask(spec, caller_address, 2);
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  // All the in memory objects should be cleaned up. The generator ref returns
  // a direct result that would be GCed once it goes out of scope.
  ASSERT_EQ(store_->Size(), 1);
  ASSERT_TRUE(store_->Get({dynamic_return_id}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();
  ASSERT_TRUE(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();

  // Clean up the generator ID. Now all lineage is safe to remove.
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));

  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);

  // NOTE: We panic if READ is called after DELETE. The
  // API caller should guarantee this doesn't happen.
  // So we don't test it.
  // WRITE 3. Should be ignored.
  auto dynamic_return_id3 = ObjectID::FromIndex(spec.TaskId(), 4);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 2,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id3,
      /*data*/ data,
      /*set_in_plasma*/ false);
  bool signal_called = false;
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [&](Status status, int64_t) {
        signal_called = true;
      }));
  ASSERT_TRUE(signal_called);
  // The write should have been no op. No refs and no obj values.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  // All the in memory objects should be cleaned up.
  ASSERT_EQ(store_->Size(), 1);
  ASSERT_TRUE(store_->Get({dynamic_return_id3}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();
}

TEST_F(TaskManagerTest, TestObjectRefStreamDelCleanReferencesLineageInScope) {
  /**
   * Verify DEL cleans all references/objects and ignore all future WRITE.
   * However, stream and task metadata is not deleted until the generator refs'
   * lineage has gone out of scope.
   *
   * CREATE WRITE WRITE DEL (make sure no refs are leaked)
   */
  // Submit a task so that generator ID will be available
  // to the reference counter.
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ true);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  // WRITE 2
  auto dynamic_return_id2 = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id2,
      /*data*/ data,
      /*set_in_plasma*/ true);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // NumObjectIDsInScope == Generator + 2 WRITE
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 3);
  // 2 in memory objects.
  ASSERT_EQ(store_->Size(), 2);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(0));
  RAY_CHECK_OK(store_->Get({dynamic_return_id}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  results.clear();

  // Consume one ref.
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // Write one ref that will stay unconsumed.
  RAY_CHECK_OK(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results));
  ASSERT_EQ(results.size(), 1);
  results.clear();

  // DELETE. This should clean all references except generator id.
  CompletePendingStreamingTask(spec, caller_address, 2);
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  // All the unconsumed objects should be cleaned up. The generator ref returns
  // a direct result that would be GCed once it goes out of scope.
  ASSERT_EQ(store_->Size(), 2);
  ASSERT_TRUE(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();

  // Clean up the generator ID.
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  // Clean up the consumed dynamic return.
  reference_counter_->RemoveLocalReference(dynamic_return_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(dynamic_return_id));

  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  // All the unconsumed in memory objects should be cleaned up. Check for 2
  // in-memory objects: one consumed object ref and the generator ref.
  ASSERT_EQ(store_->Size(), 2);
  ASSERT_TRUE(store_->Get({dynamic_return_id2}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();

  // NOTE: We panic if READ is called after DELETE. The
  // API caller should guarantee this doesn't happen.
  // So we don't test it.
  // WRITE 3. Should be ignored.
  auto dynamic_return_id3 = ObjectID::FromIndex(spec.TaskId(), 4);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 2,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id3,
      /*data*/ data,
      /*set_in_plasma*/ false);
  bool signal_called = false;
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [&](Status, int64_t) { signal_called = true; }));
  ASSERT_TRUE(signal_called);
  // The write should have been no op. No refs and no obj values except the generator id.
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  // All the unconsumed in memory objects should be cleaned up. Check for 2
  // in-memory objects: one consumed object ref and the generator ref.
  ASSERT_EQ(store_->Size(), 2);
  ASSERT_TRUE(store_->Get({dynamic_return_id3}, 1, 1, ctx, false, &results).IsTimedOut());
  results.clear();
}

TEST_F(TaskManagerTest, TestObjectRefStreamDelCleanReferencesLineageBeforeTaskCompletes) {
  /**
   * Verify DEL cleans all references/objects and ignore all future WRITE.
   * However, stream and task metadata is not deleted until the generator refs'
   * lineage has gone out of scope.
   *
   * CREATE WRITE WRITE DEL (make sure no refs are leaked)
   */
  // Submit a task so that generator ID will be available
  // to the reference counter.
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  // WRITE
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ true);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // NumObjectIDsInScope == Generator + 2 WRITE
  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 2);
  // 2 in memory objects.
  ASSERT_EQ(store_->Size(), 1);

  // Consume one ref.
  ObjectID obj_id;
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(obj_id, dynamic_return_id);

  // Clear consumers' references.
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  reference_counter_->RemoveLocalReference(dynamic_return_id, nullptr);
  // Stream metadata cannot be GCed because EOF not written yet.
  ASSERT_FALSE(manager_.TryDelObjectRefStream(generator_id));
  // DELETE. This should clean all references except generator id.
  CompletePendingStreamingTask(spec, caller_address, 2);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
}

TEST_F(TaskManagerTest, TestObjectRefStreamOutofOrder) {
  /**
   * Test the case where the task return RPC is received out of order
   */
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);

  auto last_idx = 2;
  std::vector<ObjectID> dynamic_return_ids;
  // EoF reported first.
  CompletePendingStreamingTask(spec, caller_address, 2);

  // Write index 1 -> 0
  for (auto i = last_idx - 1; i > -1; i--) {
    auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), i + 2);
    dynamic_return_ids.insert(dynamic_return_ids.begin(), dynamic_return_id);
    auto data = GenerateRandomBuffer();

    auto req = GetIntermediateTaskReturn(
        /*idx*/ i,
        /*finished*/ false,
        generator_id,
        /*dynamic_return_id*/ dynamic_return_id,
        /*data*/ data,
        /*set_in_plasma*/ false);
    // WRITE * 2
    ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
        req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  }

  // Verify read works.
  ObjectID obj_id;
  for (auto i = 0; i < last_idx; i++) {
    auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(obj_id, dynamic_return_ids[i]);
    reference_counter_->RemoveLocalReference(obj_id, nullptr);
  }
  reference_counter_->RemoveLocalReference(generator_id, nullptr);

  // READ (EoF)
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(status.IsObjectRefEndOfStream());
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
}

TEST_F(TaskManagerTest, TestObjectRefStreamDelOutOfOrder) {
  /**
   * Verify there's no leak when we delete a ObjectRefStream
   * that has out of order WRITEs.
   * WRITE index 1 -> Del -> Write index 0. Both 0 and 1 have to be
   * deleted.
   */
  // Submit a generator task.
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  // WRITE to index 1
  auto dynamic_return_id_index_1 = ObjectID::FromIndex(spec.TaskId(), 3);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id_index_1,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  ASSERT_TRUE(reference_counter_->HasReference(dynamic_return_id_index_1));

  // Delete the stream. This should remove references from ^.
  CompletePendingStreamingTask(spec, caller_address, 0);
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_1));

  // WRITE to index 0. It should fail because the stream has been removed.
  auto dynamic_return_id_index_0 = ObjectID::FromIndex(spec.TaskId(), 2);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id_index_0,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_0));

  ASSERT_EQ(reference_counter_->NumObjectIDsInScope(), 0);
  // All the objects except the generator ref should be cleaned up.
  ASSERT_EQ(store_->Size(), 1);
}

TEST_F(TaskManagerTest, TestObjectRefStreamTemporarilyOwnGeneratorReturnRefIfNeeded) {
  /**
   * Test TemporarilyOwnGeneratorReturnRefIfNeeded
   */
  rpc::Address caller_address;
  auto spec =
      CreateTaskHelper(1, {}, /*dynamic_returns=*/true, /*is_streaming_generator=*/true);
  auto generator_id = spec.ReturnId(0);

  /**
   * Test TemporarilyOwnGeneratorReturnRefIfNeeded is no-op when the stream is
   * not created yet.
   */
  auto dynamic_return_id_index_0 = ObjectID::FromIndex(spec.TaskId(), 2);
  manager_.TemporarilyOwnGeneratorReturnRefIfNeeded(dynamic_return_id_index_0,
                                                    generator_id);
  // It is no-op if the object ref stream is not created.
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_0));

  /**
   * Submit a generator task.
   */
  manager_.AddPendingTask(caller_address, spec, "", /*num_retries=*/0);
  manager_.MarkDependenciesResolved(spec.TaskId());
  manager_.MarkTaskWaitingForExecution(
      spec.TaskId(), NodeID::FromRandom(), WorkerID::FromRandom());

  /**
   * Test TemporarilyOwnGeneratorReturnRefIfNeeded called before any
   * HandleReportGeneratorItemReturns adds a reference.
   */
  manager_.TemporarilyOwnGeneratorReturnRefIfNeeded(dynamic_return_id_index_0,
                                                    generator_id);
  // We has a reference to this object before the ref is
  // reported via HandleReportGeneratorItemReturns.
  ASSERT_TRUE(reference_counter_->HasReference(dynamic_return_id_index_0));

  /**
   * Test TemporarilyOwnGeneratorReturnRefIfNeeded called after the
   * ref consumed / removed will be no-op.
   */
  // WRITE 0 -> WRITE 1
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id_index_0,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));
  auto dynamic_return_id_index_1 = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id_index_1,
      /*data*/ data,
      /*set_in_plasma*/ false);
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req, /*execution_signal_callback*/ [](Status, int64_t) {}));

  // READ 0 -> READ 1
  for (auto i = 0; i < 2; i++) {
    ObjectID object_id;
    auto status = manager_.TryReadObjectRefStream(generator_id, &object_id);
    ASSERT_TRUE(status.ok());
  }

  std::vector<ObjectID> removed;
  reference_counter_->RemoveLocalReference(dynamic_return_id_index_1, &removed);
  ASSERT_EQ(removed.size(), 1UL);
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_1));
  // If the ref has been already consumed and deleted,
  // this shouldn't add a reference.
  manager_.TemporarilyOwnGeneratorReturnRefIfNeeded(dynamic_return_id_index_1,
                                                    generator_id);
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_1));

  /**
   * Test TemporarilyOwnGeneratorReturnRefIfNeeded called but
   * HandleReportGeneratorItemReturns is never called. In this case, when
   * the stream is deleted these refs should be cleaned up.
   */
  auto dynamic_return_id_index_2 = ObjectID::FromIndex(spec.TaskId(), 4);
  manager_.TemporarilyOwnGeneratorReturnRefIfNeeded(dynamic_return_id_index_2,
                                                    generator_id);
  ASSERT_TRUE(reference_counter_->HasReference(dynamic_return_id_index_2));
  CompletePendingStreamingTask(spec, caller_address, 2);
  manager_.TryDelObjectRefStream(generator_id);
  ASSERT_FALSE(reference_counter_->HasReference(dynamic_return_id_index_2));
}

TEST_F(TaskManagerTest, TestObjectRefStreamBackpressure) {
  /**
   * Test the RPC is not replied when backpressured.
   * Test the RPC is replied when the stream is deleted.
   * Test the RPC is replied when the data is consumed.
   */
  auto spec = CreateTaskHelper(1,
                               {},
                               /*dynamic_returns=*/true,
                               /*is_streaming_generator=*/true,
                               /*generator_backpressure_num_objects*/ 2);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 0);

  /// 1 generate, 0 consumed, 2 threshold -> should signal immediately.
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  bool signal_called = false;
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&signal_called](Status status,
                                                     int64_t num_objects_consumed) {
        signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 0);
      }));
  ASSERT_TRUE(signal_called);

  /// 2 generate, 0 consumed, 2 threshold -> backpressured
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  signal_called = false;
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&signal_called](Status status,
                                                     int64_t num_objects_consumed) {
        signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 1);
      }));
  ASSERT_FALSE(signal_called);

  ObjectID obj_id;
  // Read should signal the executor.
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(signal_called);
  reference_counter_->RemoveLocalReference(obj_id, nullptr);

  /// 3 generate, 1 consumed, 2 threshold -> backpressured
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 4);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 2,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  signal_called = false;
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&signal_called](Status status,
                                                     int64_t num_objects_consumed) {
        signal_called = true;
        ASSERT_TRUE(status.IsNotFound());
        ASSERT_EQ(num_objects_consumed, -1);
      }));
  ASSERT_FALSE(signal_called);

  // Deleting the stream should send a signal.
  CompletePendingStreamingTask(spec, caller_address, 2);
  reference_counter_->RemoveLocalReference(generator_id, nullptr);
  ASSERT_TRUE(manager_.TryDelObjectRefStream(generator_id));
  ASSERT_TRUE(signal_called);

  /// No need to test out of order case. It won't be different.
}

TEST_F(TaskManagerTest, TestBackpressureAfterReconstruction) {
  // Consumed objects should be signaled immediately.
  // Unconsumed objects should not be.
  auto spec = CreateTaskHelper(1,
                               {},
                               /*dynamic_returns=*/true,
                               /*is_streaming_generator=*/true,
                               /*generator_backpressure_num_objects*/ 2);
  auto generator_id = spec.ReturnId(0);
  rpc::Address caller_address;
  manager_.AddPendingTask(caller_address, spec, "", 1);

  /// 1 generate, 0 consumed, 2 threshold -> should signal immediately.
  auto dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  auto data = GenerateRandomBuffer();
  auto req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  bool signal_called = false;
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&signal_called](Status status,
                                                     int64_t num_objects_consumed) {
        signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 0);
      }));
  ASSERT_TRUE(signal_called);

  /// 2 generate, 0 consumed, 2 threshold -> backpressured
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  signal_called = false;
  ASSERT_TRUE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&signal_called](Status status,
                                                     int64_t num_objects_consumed) {
        signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 1);
      }));
  ASSERT_FALSE(signal_called);

  // Worker failure. New worker should start reporting the task.
  auto error = rpc::ErrorType::WORKER_DIED;
  ASSERT_TRUE(manager_.FailOrRetryPendingTask(spec.TaskId(), error));

  // Two report will come again. The first one should reply immediately (because)
  // it is already replied and the second one should be backpressured.
  /// 1 generate, 0 consumed, 2 threshold -> should signal immediately.
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 2);
  req = GetIntermediateTaskReturn(
      /*idx*/ 0,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  bool retry_signal_called = false;
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&retry_signal_called](Status status,
                                                           int64_t num_objects_consumed) {
        retry_signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 0);
      }));
  ASSERT_TRUE(retry_signal_called);

  /// 2 generate, 0 consumed, 2 threshold -> backpressured
  dynamic_return_id = ObjectID::FromIndex(spec.TaskId(), 3);
  data = GenerateRandomBuffer();
  req = GetIntermediateTaskReturn(
      /*idx*/ 1,
      /*finished*/ false,
      generator_id,
      /*dynamic_return_id*/ dynamic_return_id,
      /*data*/ data,
      /*set_in_plasma*/ false);
  retry_signal_called = false;
  ASSERT_FALSE(manager_.HandleReportGeneratorItemReturns(
      req,
      /*execution_signal_callback*/ [&retry_signal_called](Status status,
                                                           int64_t num_objects_consumed) {
        retry_signal_called = true;
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(num_objects_consumed, 1);
      }));
  // Backpressured.
  ASSERT_FALSE(retry_signal_called);

  ObjectID obj_id;
  // Read should signal both executor.
  auto status = manager_.TryReadObjectRefStream(generator_id, &obj_id);
  ASSERT_TRUE(signal_called);
  ASSERT_TRUE(retry_signal_called);
  CompletePendingStreamingTask(spec, caller_address, 2);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
