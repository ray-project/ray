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

#include "ray/core_worker/transport/scheduling_queue.h"

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/transport/actor_scheduling_queue.h"
#include "ray/core_worker/transport/task_receiver.h"

// using namespace std::chrono_literals;
using std::chrono_literals::operator""s;

namespace ray {
namespace core {

// Helper function that returns a condition checker to verify if a variable equals a
// target value. It uses an atomic variable to avoid race conditions between the main
// thread and the underlying executor (i.e., thread), which may result in errors from
// ASAN.
std::function<bool()> CreateEqualsConditionChecker(const std::atomic<int> *var,
                                                   int target) {
  return [var, target]() { return var->load() == target; };
}

class MockWaiter : public DependencyWaiter {
 public:
  MockWaiter() {}

  void Wait(const std::vector<rpc::ObjectReference> &dependencies,
            std::function<void()> on_dependencies_available) override {
    callbacks_.push_back([on_dependencies_available]() { on_dependencies_available(); });
  }

  void Complete(int index) { callbacks_[index](); }

 private:
  std::vector<std::function<void()>> callbacks_;
};

class MockTaskEventBuffer : public worker::TaskEventBuffer {
 public:
  void AddTaskEvent(std::unique_ptr<worker::TaskEvent> task_event) override {
    task_events.emplace_back(std::move(task_event));
  }

  void FlushEvents(bool forced) override {}

  Status Start(bool auto_flush = true) override { return Status::OK(); }

  void Stop() override {}

  bool Enabled() const override { return true; }

  std::string DebugString() override { return ""; }

  std::vector<std::unique_ptr<worker::TaskEvent>> task_events;
};

TEST(SchedulingQueueTest, TestTaskEvents) {
  // Test task events are recorded.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  JobID job_id = JobID::FromInt(1);
  TaskID task_id_1 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_job_id(job_id.Binary());
  task_spec_without_dependency.GetMutableMessage().set_task_id(task_id_1.Binary());
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_without_dependency.GetMutableMessage().set_enable_task_events(true);

  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec_without_dependency);
  ASSERT_EQ(task_event_buffer.task_events.size(), 1UL);
  rpc::TaskEvents rpc_task_events;
  task_event_buffer.task_events[0]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY));
  ASSERT_EQ(rpc_task_events.job_id(), job_id.Binary());
  ASSERT_EQ(rpc_task_events.task_id(), task_id_1.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 0);

  TaskID task_id_2 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_task_id(task_id_2.Binary());
  task_spec_with_dependency.GetMutableMessage().set_attempt_number(1);
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage().set_enable_task_events(true);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(ObjectID::FromRandom().Binary());
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);
  waiter.Complete(0);
  ASSERT_EQ(task_event_buffer.task_events.size(), 3UL);
  task_event_buffer.task_events[1]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH));
  ASSERT_EQ(rpc_task_events.task_id(), task_id_2.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 1);
  task_event_buffer.task_events[2]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY));
  ASSERT_EQ(rpc_task_events.task_id(), task_id_2.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 1);

  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 2);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestInOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(2, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr, task_spec);
  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestWaitForObjects) {
  ObjectID obj = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  std::atomic<int> n_ok(0);
  std::atomic<int> n_rej(0);

  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(obj.Binary());
  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec_without_dependency);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);
  queue.Add(2, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);

  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 1), 1000));

  waiter.Complete(0);
  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 2), 1000));

  waiter.Complete(2);
  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 2), 1000));

  waiter.Complete(1);

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 4);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestWaitForObjectsNotSubjectToSeqTimeout) {
  ObjectID obj = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  std::atomic<int> n_ok(0);
  std::atomic<int> n_rej(0);

  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(obj.Binary());
  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec_without_dependency);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);

  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 1), 1000));
  io_service.run();
  ASSERT_EQ(n_rej, 0);
  waiter.Complete(0);

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestOutOfOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.Add(2, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec);
  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestSeqWaitTimeout) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  std::atomic<int> n_ok(0);
  std::atomic<int> n_rej(0);

  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.Add(2, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr, task_spec);
  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 1), 1000));
  ASSERT_EQ(n_rej, 0);
  io_service.run();
  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_ok, 1), 1000));
  ASSERT_TRUE(WaitForCondition(CreateEqualsConditionChecker(&n_rej, 2), 1000));
  queue.Add(4, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(5, -1, fn_ok, fn_rej, nullptr, task_spec);

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 3);
  ASSERT_EQ(n_rej, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestSkipAlreadyProcessedByClient) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  ActorSchedulingQueue queue(io_service, waiter, task_event_buffer, pool_manager, 1);
  std::atomic<int> n_ok(0);
  std::atomic<int> n_rej(0);
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.Add(2, 2, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(3, 2, fn_ok, fn_rej, nullptr, task_spec);
  queue.Add(1, 2, fn_ok, fn_rej, nullptr, task_spec);
  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestCancelQueuedTask) {
  std::unique_ptr<SchedulingQueue> queue = std::make_unique<NormalSchedulingQueue>();
  ASSERT_TRUE(queue->TaskQueueEmpty());
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::NORMAL_TASK);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, task_spec);
  ASSERT_TRUE(queue->CancelTaskIfFound(TaskID::Nil()));
  ASSERT_FALSE(queue->TaskQueueEmpty());
  queue->ScheduleRequests();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 1);

  queue->Stop();
}

TEST(OutOfOrderActorSchedulingQueueTest, TestTaskEvents) {
  // Test task events are recorded.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  OutOfOrderActorSchedulingQueue queue(io_service,
                                       waiter,
                                       task_event_buffer,
                                       pool_manager,
                                       /*fiber_state_manager=*/nullptr,
                                       /*is_asyncio=*/false,
                                       /*fiber_max_concurrency=*/1,
                                       /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](const TaskSpecification &task_spec,
                       rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  JobID job_id = JobID::FromInt(1);
  TaskID task_id_1 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_job_id(job_id.Binary());
  task_spec_without_dependency.GetMutableMessage().set_task_id(task_id_1.Binary());
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_without_dependency.GetMutableMessage().set_enable_task_events(true);

  queue.Add(0, -1, fn_ok, fn_rej, nullptr, task_spec_without_dependency);
  ASSERT_EQ(task_event_buffer.task_events.size(), 1UL);
  rpc::TaskEvents rpc_task_events;
  task_event_buffer.task_events[0]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY));
  ASSERT_EQ(rpc_task_events.job_id(), job_id.Binary());
  ASSERT_EQ(rpc_task_events.task_id(), task_id_1.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 0);

  TaskID task_id_2 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_task_id(task_id_2.Binary());
  task_spec_with_dependency.GetMutableMessage().set_attempt_number(1);
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage().set_enable_task_events(true);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(ObjectID::FromRandom().Binary());
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, task_spec_with_dependency);
  waiter.Complete(0);
  ASSERT_EQ(task_event_buffer.task_events.size(), 3UL);
  task_event_buffer.task_events[1]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ARGS_FETCH));
  ASSERT_EQ(rpc_task_events.task_id(), task_id_2.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 1);
  task_event_buffer.task_events[2]->ToRpcTaskEvents(&rpc_task_events);
  ASSERT_TRUE(rpc_task_events.state_updates().state_ts_ns().contains(
      rpc::TaskStatus::PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY));
  ASSERT_EQ(rpc_task_events.task_id(), task_id_2.Binary());
  ASSERT_EQ(rpc_task_events.attempt_number(), 1);

  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_ok, 2);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(OutOfOrderActorSchedulingQueueTest, TestSameTaskMultipleAttempts) {
  // Test that if multiple attempts of the same task are received,
  // the next attempt only runs after the previous attempt finishes.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;
  OutOfOrderActorSchedulingQueue queue(
      io_service,
      waiter,
      task_event_buffer,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      /*fiber_state_manager=*/nullptr,
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{});
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  auto fn_ok_1 = [&attempt_1_start_promise, &attempt_1_finish_promise](
                     const TaskSpecification &task_spec,
                     rpc::SendReplyCallback callback) {
    attempt_1_start_promise.set_value();
    attempt_1_finish_promise.get_future().wait();
  };
  std::promise<void> attempt_2_start_promise;
  auto fn_ok_2 = [&attempt_2_start_promise](const TaskSpecification &task_spec,
                                            rpc::SendReplyCallback callback) {
    attempt_2_start_promise.set_value();
  };
  int n_rej = 0;
  auto fn_rej = [&n_rej](const TaskSpecification &task_spec,
                         const Status &status,
                         rpc::SendReplyCallback callback) { n_rej++; };
  TaskSpecification task_spec_1;
  task_spec_1.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_1.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_1.GetMutableMessage().set_attempt_number(1);
  queue.Add(-1, -1, fn_ok_1, fn_rej, nullptr, task_spec_1);
  attempt_1_start_promise.get_future().wait();
  TaskSpecification task_spec_2;
  task_spec_2.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_2.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_2.GetMutableMessage().set_attempt_number(2);
  queue.Add(-1, -1, fn_ok_2, fn_rej, nullptr, task_spec_2);
  io_service.poll();
  // Attempt 2 should only start after attempt 1 finishes.
  auto attempt_2_start_future = attempt_2_start_promise.get_future();
  ASSERT_TRUE(attempt_2_start_future.wait_for(1s) == std::future_status::timeout);

  // Finish attempt 1 so attempt 2 can run.
  attempt_1_finish_promise.set_value();
  while (attempt_2_start_future.wait_for(1s) != std::future_status::ready) {
    io_service.restart();
    io_service.poll();
  }

  ASSERT_EQ(n_rej, 0);
  auto no_leak = [&queue] {
    absl::MutexLock lock(&queue.mu_);
    return queue.queued_actor_tasks_.empty() &&
           queue.pending_task_id_to_is_canceled.empty();
  };
  ASSERT_TRUE(WaitForCondition(no_leak, 10000));

  queue.Stop();
}

TEST(OutOfOrderActorSchedulingQueueTest, TestSameTaskMultipleAttemptsCancellation) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;
  OutOfOrderActorSchedulingQueue queue(
      io_service,
      waiter,
      task_event_buffer,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      /*fiber_state_manager=*/nullptr,
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{});
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  auto fn_ok_1 = [&attempt_1_start_promise, &attempt_1_finish_promise](
                     const TaskSpecification &task_spec,
                     rpc::SendReplyCallback callback) {
    attempt_1_start_promise.set_value();
    attempt_1_finish_promise.get_future().wait();
  };
  auto fn_rej_1 = [](const TaskSpecification &task_spec,
                     const Status &status,
                     rpc::SendReplyCallback callback) { ASSERT_FALSE(true); };
  TaskSpecification task_spec_1;
  task_spec_1.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_1.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_1.GetMutableMessage().set_attempt_number(1);
  queue.Add(-1, -1, fn_ok_1, fn_rej_1, nullptr, task_spec_1);
  attempt_1_start_promise.get_future().wait();

  auto fn_ok_2 = [](const TaskSpecification &task_spec, rpc::SendReplyCallback callback) {
    ASSERT_FALSE(true);
  };
  std::atomic<bool> attempt_2_cancelled = false;
  auto fn_rej_2 = [&attempt_2_cancelled](const TaskSpecification &task_spec,
                                         const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_2_cancelled.store(true);
  };
  TaskSpecification task_spec_2;
  task_spec_2.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_2.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_2.GetMutableMessage().set_attempt_number(2);
  queue.Add(-1, -1, fn_ok_2, fn_rej_2, nullptr, task_spec_2);

  auto fn_ok_4 = [](const TaskSpecification &task_spec, rpc::SendReplyCallback callback) {
    ASSERT_FALSE(true);
  };
  std::atomic<bool> attempt_4_cancelled = false;
  auto fn_rej_4 = [&attempt_4_cancelled](const TaskSpecification &task_spec,
                                         const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_4_cancelled.store(true);
  };
  // Adding attempt 4 should cancel the old attempt 2
  TaskSpecification task_spec_4;
  task_spec_4.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_4.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_4.GetMutableMessage().set_attempt_number(4);
  queue.Add(-1, -1, fn_ok_4, fn_rej_4, nullptr, task_spec_4);
  ASSERT_TRUE(attempt_2_cancelled.load());

  auto fn_ok_3 = [](const TaskSpecification &task_spec, rpc::SendReplyCallback callback) {
    ASSERT_FALSE(true);
  };
  std::atomic<bool> attempt_3_cancelled = false;
  auto fn_rej_3 = [&attempt_3_cancelled](const TaskSpecification &task_spec,
                                         const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_3_cancelled.store(true);
  };
  // Attempt 3 should be cancelled immediately since there is attempt 4
  // in the queue.
  TaskSpecification task_spec_3;
  task_spec_3.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_3.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_3.GetMutableMessage().set_attempt_number(3);
  queue.Add(-1, -1, fn_ok_3, fn_rej_3, nullptr, task_spec_3);
  ASSERT_TRUE(attempt_3_cancelled.load());

  // Attempt 4 should be cancelled.
  queue.CancelTaskIfFound(task_id);
  attempt_1_finish_promise.set_value();
  while (!attempt_4_cancelled.load()) {
    io_service.restart();
    io_service.poll();
  }

  auto no_leak = [&queue] {
    absl::MutexLock lock(&queue.mu_);
    return queue.queued_actor_tasks_.empty() &&
           queue.pending_task_id_to_is_canceled.empty();
  };
  ASSERT_TRUE(WaitForCondition(no_leak, 10000));

  queue.Stop();
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
