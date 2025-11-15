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
#include "ray/core_worker/task_execution/task_receiver.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/core_worker_client_interface.h"
#include "ray/util/time.h"

namespace ray {
namespace core {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

TaskSpecification CreateActorTaskHelper(ActorID actor_id,
                                        WorkerID caller_worker_id,
                                        int64_t counter,
                                        TaskID caller_id = TaskID::Nil()) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::FromRandom(actor_id.JobId()).Binary());
  task.GetMutableMessage().set_caller_id(caller_id.Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task.GetMutableMessage().mutable_caller_address()->set_worker_id(
      caller_worker_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_sequence_number(counter);
  task.GetMutableMessage().set_num_returns(0);
  return task;
}

rpc::PushTaskRequest CreatePushTaskRequestHelper(ActorID actor_id,
                                                 int64_t counter,
                                                 WorkerID caller_worker_id,
                                                 TaskID caller_id,
                                                 int64_t caller_timestamp) {
  auto task_spec = CreateActorTaskHelper(actor_id, caller_worker_id, counter, caller_id);

  rpc::PushTaskRequest request;
  request.mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  request.set_sequence_number(request.task_spec().actor_task_spec().sequence_number());
  request.set_client_processed_up_to(-1);
  return request;
}

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  const rpc::Address &Addr() const override { return addr; }

  void PushActorTask(std::unique_ptr<rpc::PushTaskRequest> request,
                     bool skip_queue,
                     rpc::ClientCallback<rpc::PushTaskReply> &&callback) override {
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
  int64_t acked_seqno = 0;
};

class MockDependencyWaiter : public DependencyWaiter {
 public:
  MOCK_METHOD2(Wait,
               void(const std::vector<rpc::ObjectReference> &dependencies,
                    std::function<void()> on_dependencies_available));
};

class MockTaskEventBuffer : public worker::TaskEventBuffer {
 public:
  void AddTaskEvent(std::unique_ptr<worker::TaskEvent> task_event) override {}

  void FlushEvents(bool forced) override {}

  Status Start(bool auto_flush = true) override { return Status::OK(); }

  void Stop() override {}

  bool Enabled() const override { return true; }

  bool RecordTaskStatusEventIfNeeded(
      const TaskID &task_id,
      const JobID &job_id,
      int32_t attempt_number,
      const TaskSpecification &spec,
      rpc::TaskStatus status,
      bool include_task_info,
      std::optional<const worker::TaskStatusEvent::TaskStateUpdate> state_update)
      override {
    return true;
  }

  std::string DebugString() override { return ""; }

  std::string GetSessionName() const override { return "test-session-name"; }
};

class TaskReceiverTest : public ::testing::Test {
 public:
  TaskReceiverTest() : dependency_waiter_(std::make_unique<MockDependencyWaiter>()) {
    auto execute_task = std::bind(&TaskReceiverTest::MockExecuteTask,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2,
                                  std::placeholders::_3,
                                  std::placeholders::_4,
                                  std::placeholders::_5,
                                  std::placeholders::_6);
    RayConfig::instance().initialize(
        R"({"actor_scheduling_queue_max_reorder_wait_seconds": 1})");
    receiver_ = std::make_unique<TaskReceiver>(
        task_execution_service_,
        task_event_buffer_,
        execute_task,
        *dependency_waiter_,
        /* initialize_thread_callback= */ []() { return []() { return; }; },
        /* actor_creation_task_done= */ []() { return Status::OK(); });
  }

  Status MockExecuteTask(
      const TaskSpecification &task_spec,
      std::optional<ResourceMappingType> resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>>
          *dynamic_return_objects,
      std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
      RepeatedObjectRefCount *borrowed_refs) {
    return Status::OK();
  }

  void StartIOService() { task_execution_service_.run(); }

  void StopIOService() {
    // We must delete the receiver before stopping the IO service, since it
    // contains timers referencing the service.
    receiver_.reset();
    task_execution_service_.stop();
  }

  std::unique_ptr<TaskReceiver> receiver_;

  instrumented_io_context task_execution_service_;
  MockTaskEventBuffer task_event_buffer_;
  std::unique_ptr<DependencyWaiter> dependency_waiter_;
};

TEST_F(TaskReceiverTest, TestNewTaskFromDifferentWorker) {
  TaskID current_task_id = TaskID::Nil();
  ActorID actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  WorkerID worker_id = WorkerID::FromRandom();
  TaskID caller_id =
      TaskID::ForActorTask(JobID::FromInt(0), current_task_id, 0, actor_id);

  int64_t curr_timestamp = current_sys_time_ms();
  int64_t old_timestamp = curr_timestamp - 1000;
  int64_t new_timestamp = curr_timestamp + 1000;

  int callback_count = 0;

  // Push a task request with actor counter 0. This should succeed
  // on the receiver.
  {
    auto request =
        CreatePushTaskRequestHelper(actor_id, 0, worker_id, caller_id, curr_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status,
                                            std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  // Push a task request with actor counter 1. This should succeed
  // on the receiver.
  {
    auto request =
        CreatePushTaskRequestHelper(actor_id, 1, worker_id, caller_id, curr_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status,
                                            std::function<void()> success,
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
    worker_id = WorkerID::FromRandom();
    auto request =
        CreatePushTaskRequestHelper(actor_id, 0, worker_id, caller_id, new_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status,
                                            std::function<void()> success,
                                            std::function<void()> failure) {
      ++callback_count;
      ASSERT_TRUE(status.ok());
    };
    receiver_->HandleTask(request, &reply, reply_callback);
  }

  // Push a task request with actor counter 1, but with a different worker id,
  // and a older timestamp. In this case the request should fail.
  {
    worker_id = WorkerID::FromRandom();
    auto request =
        CreatePushTaskRequestHelper(actor_id, 1, worker_id, caller_id, old_timestamp);
    rpc::PushTaskReply reply;
    auto reply_callback = [&callback_count](Status status,
                                            std::function<void()> success,
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

}  // namespace core
}  // namespace ray
