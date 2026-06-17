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
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/core_worker/task_execution/ordered_actor_task_execution_queue.h"
#include "ray/core_worker/task_execution/unordered_actor_task_execution_queue.h"

// using namespace std::chrono_literals;
using std::chrono_literals::operator""s;

namespace ray {
namespace core {

class MockWaiter : public ActorTaskExecutionArgWaiterInterface {
 public:
  MockWaiter() {}

  void AsyncWait(const std::vector<rpc::ObjectReference> &dependencies,
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

  bool RecordTaskStatusEventIfNeeded(
      const TaskID &task_id,
      const JobID &job_id,
      int32_t attempt_number,
      const TaskSpecification &spec,
      rpc::TaskStatus status,
      bool include_task_info,
      std::optional<const worker::TaskStatusEvent::TaskStateUpdate> state_update)
      override {
    AddTaskEvent(std::make_unique<worker::TaskStatusEvent>(
        task_id,
        job_id,
        attempt_number,
        status,
        /* timestamp */ absl::GetCurrentTimeNanos(),
        /*is_actor_task_event=*/spec.IsActorTask(),
        "test-session-name",
        GetNodeID(),
        include_task_info ? std::make_shared<const TaskSpecification>(spec) : nullptr,
        std::move(state_update)));
    return true;
  }

  std::string GetSessionName() const override { return "test-session-name"; }

  std::vector<std::unique_ptr<worker::TaskEvent>> task_events;

  NodeID GetNodeID() const override { return NodeID::Nil(); }
};

namespace {

// Construct a TaskToExecute container for tests. The per-task execute/cancel behavior is
// supplied to the queue at construction time (not per task), so the container only needs
// to carry the task spec. The reply and send_reply_callback are dummy implementations
// that are never inspected by the tests' queue-level callbacks.
TaskToExecute MakeTaskToExecute(const TaskSpecification &task_spec) {
  static rpc::PushTaskReply dummy_reply;
  return TaskToExecute(
      task_spec,
      /*resource_ids=*/std::nullopt,
      &dummy_reply,
      [](const Status &, std::function<void()>, std::function<void()>) {});
}

}  // namespace

TEST(OrderedActorTaskExecutionQueueTest, TestTaskEvents) {
  // Test task events are recorded.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  int n_executed = 0;
  int n_canceled = 0;
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id_1 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_job_id(job_id.Binary());
  task_spec_without_dependency.GetMutableMessage().set_task_id(task_id_1.Binary());
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_without_dependency.GetMutableMessage().set_enable_task_events(true);

  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec_without_dependency));
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
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec_with_dependency));
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

  ASSERT_EQ(n_executed, 2);
  ASSERT_EQ(n_canceled, 0);

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, TestInOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  int n_executed = 0;
  int n_canceled = 0;
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(2, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(3, -1, MakeTaskToExecute(task_spec));
  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_executed, 4);
  ASSERT_EQ(n_canceled, 0);

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, ShutdownCancelsQueuedAndWaitsForRunning) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  // One running task that blocks until we signal. The queued task (with a dependency)
  // stays queued and is cancelled by Stop().
  std::promise<void> running_started;
  std::promise<void> allow_finish;
  std::atomic<int> n_rejected{0};
  auto execute_task_blocking = [&running_started, &allow_finish](TaskToExecute &task) {
    running_started.set_value();
    allow_finish.get_future().wait();
  };
  auto cancel_task_count = [&n_rejected](const TaskToExecute &task,
                                         const Status &status) {
    if (status.IsSchedulingCancelled()) {
      n_rejected.fetch_add(1);
    }
  };

  OrderedActorTaskExecutionQueue queue(io_service,
                                       waiter,
                                       task_event_buffer,
                                       pool_manager,
                                       1,
                                       execute_task_blocking,
                                       cancel_task_count);
  TaskSpecification ts;
  ts.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  // Enqueue a running task and a queued task.
  queue.EnqueueTask(0, -1, MakeTaskToExecute(ts));
  // Make the queued task have a dependency so it stays queued and will be cancelled by
  // Stop().
  TaskSpecification ts_dep;
  ts_dep.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  ts_dep.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
      ObjectID::FromRandom().Binary());
  queue.EnqueueTask(1, -1, MakeTaskToExecute(ts_dep));
  io_service.poll();
  running_started.get_future().wait();

  // Call Stop() from another thread to avoid blocking this thread before allowing finish.
  std::thread stopper([&]() { queue.Stop(); });
  // Finish the running task so Stop can join.
  allow_finish.set_value();
  stopper.join();
  ASSERT_EQ(n_rejected.load(), 1);
}

TEST(OrderedActorTaskExecutionQueueTest, TestWaitForObjects) {
  ObjectID obj = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  std::atomic<int> n_executed(0);
  std::atomic<int> n_canceled(0);
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(obj.Binary());
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec_without_dependency));
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec_with_dependency));
  queue.EnqueueTask(2, -1, MakeTaskToExecute(task_spec_with_dependency));
  queue.EnqueueTask(3, -1, MakeTaskToExecute(task_spec_with_dependency));

  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 1; }, 1000));

  waiter.Complete(0);
  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 2; }, 1000));

  waiter.Complete(2);
  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 2; }, 1000));

  waiter.Complete(1);

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_executed, 4);

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, TestWaitForObjectsNotSubjectToSeqTimeout) {
  ObjectID obj = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  std::atomic<int> n_executed(0);
  std::atomic<int> n_canceled(0);
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  TaskSpecification task_spec_with_dependency;
  task_spec_with_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_with_dependency.GetMutableMessage()
      .add_args()
      ->mutable_object_ref()
      ->set_object_id(obj.Binary());
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec_without_dependency));
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec_with_dependency));

  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 1; }, 1000));
  io_service.run();
  ASSERT_EQ(n_canceled, 0);
  waiter.Complete(0);

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_executed, 2);

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, TestSeqWaitTimeout) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  std::atomic<int> n_executed(0);
  std::atomic<int> n_canceled(0);
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.EnqueueTask(2, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(3, -1, MakeTaskToExecute(task_spec));
  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 1; }, 1000));
  ASSERT_EQ(n_canceled, 0);
  io_service.run();
  ASSERT_TRUE(WaitForCondition([&n_executed]() { return n_executed == 1; }, 1000));
  ASSERT_TRUE(WaitForCondition([&n_canceled]() { return n_canceled == 2; }, 1000));
  queue.EnqueueTask(4, -1, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(5, -1, MakeTaskToExecute(task_spec));

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_executed, 3);
  ASSERT_EQ(n_canceled, 2);

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, TestSkipAlreadyProcessedByClient) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  std::atomic<int> n_executed(0);
  std::atomic<int> n_canceled(0);
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 1, execute_task, cancel_task);
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  queue.EnqueueTask(2, 2, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(3, 2, MakeTaskToExecute(task_spec));
  queue.EnqueueTask(1, 2, MakeTaskToExecute(task_spec));
  io_service.run();

  // Wait for all tasks to finish.
  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(n_executed, 1);
  ASSERT_EQ(n_canceled, 2);

  queue.Stop();
}

namespace {

TaskSpecification CreateActorTaskSpec(int64_t seq_no,
                                      bool is_retry = false,
                                      bool dependency = false) {
  TaskSpecification task_spec;
  task_spec.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(seq_no);
  task_spec.GetMutableMessage().set_attempt_number(is_retry ? 1 : 0);
  if (dependency) {
    task_spec.GetMutableMessage().add_args()->mutable_object_ref()->set_object_id(
        ObjectID::FromRandom().Binary());
  }
  return task_spec;
}

}  // namespace

TEST(OrderedActorTaskExecutionQueueTest, TestRetryInOrderOrderedActorTaskExecutionQueue) {
  // Setup
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;
  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  std::vector<int64_t> accept_seq_nos;
  std::vector<int64_t> reject_seq_nos;
  std::atomic<int> n_accept = 0;
  auto execute_task = [&accept_seq_nos, &n_accept](TaskToExecute &task) {
    accept_seq_nos.push_back(task.TaskSpec().ConcurrencyGroupSequenceNumber());
    n_accept++;
  };
  auto cancel_task = [&reject_seq_nos](const TaskToExecute &task, const Status &status) {
    reject_seq_nos.push_back(task.TaskSpec().ConcurrencyGroupSequenceNumber());
  };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 2, execute_task, cancel_task);

  // Submitting 0 with dep, 1, 3 (retry of 2), and 4 (with client_processed_up_to = 2 bc 2
  // failed to send), 6 (retry of 5) with dep.
  // 0 and 1 will be cancelled due to the client_processed_up_to = 2.
  // 3 (retry of 2) should get executed. Then, 4 should be executed. Then 6 (retry of 5)
  // once the dependency is fetched.
  auto task_spec_0 = CreateActorTaskSpec(0, /*is_retry=*/false, /*dependency=*/true);
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec_0));
  auto task_spec_1 = CreateActorTaskSpec(1);
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec_1));
  auto task_spec_2_retry = CreateActorTaskSpec(3, /*is_retry=*/true);
  queue.EnqueueTask(3, -1, MakeTaskToExecute(task_spec_2_retry));
  auto task_spec_4 = CreateActorTaskSpec(4);
  queue.EnqueueTask(4, 2, MakeTaskToExecute(task_spec_4));
  auto task_spec_5_retry = CreateActorTaskSpec(6, /*is_retry=*/true, /*dependency=*/true);
  queue.EnqueueTask(6, -1, MakeTaskToExecute(task_spec_5_retry));

  io_service.run();

  ASSERT_TRUE(WaitForCondition([&n_accept]() { return n_accept == 2; }, 1000));
  // seq_no 6 is index 1 for the mock waiter because only 2 tasks had deps.
  waiter.Complete(1);
  ASSERT_TRUE(WaitForCondition([&n_accept]() { return n_accept == 3; }, 1000));

  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  ASSERT_EQ(accept_seq_nos, (std::vector<int64_t>{3, 4, 6}));
  ASSERT_EQ(reject_seq_nos, (std::vector<int64_t>{0, 1}));

  queue.Stop();
}

TEST(OrderedActorTaskExecutionQueueTest, TestPerConcurrencyGroupOrdering) {
  // Test that tasks in different concurrency groups are sequenced independently.
  // group "b" tasks should execute even when group "a" is waiting for a missing seq_no.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;
  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"a", 1, {}},
                                                   ConcurrencyGroup{"b", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  // Track accepted tasks as (group_name, seq_no) pairs.
  std::vector<std::pair<std::string, int64_t>> accepted;
  std::atomic<int> n_accept = 0;
  auto execute_task = [&accepted, &n_accept](TaskToExecute &task) {
    accepted.emplace_back(task.TaskSpec().ConcurrencyGroupName(),
                          task.TaskSpec().ConcurrencyGroupSequenceNumber());
    n_accept++;
  };
  auto cancel_task = [](const TaskToExecute &, const Status &) { FAIL(); };

  OrderedActorTaskExecutionQueue queue(
      io_service, waiter, task_event_buffer, pool_manager, 2, execute_task, cancel_task);

  auto make_task = [](const std::string &group, int64_t seq_no) {
    auto spec = CreateActorTaskSpec(seq_no);
    spec.GetMutableMessage().set_concurrency_group_name(group);
    return spec;
  };

  auto task_a0 = make_task("a", 0);
  auto task_a1 = make_task("a", 1);
  auto task_b0 = make_task("b", 0);
  auto task_b1 = make_task("b", 1);

  // Sequence no 0 missing from group a, so that should block until 1 arrives.
  // Concurrency group b should be ready to go though.
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_a1));
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_b0));
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_b1));

  io_service.run_one();
  io_service.run_one();
  ASSERT_TRUE(WaitForCondition([&n_accept]() { return n_accept == 2; }, 1000));
  ASSERT_EQ(n_accept, 2);

  // Now enqueue group "a" seq 0, which unblocks group "a".
  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_a0));
  io_service.run_one();
  io_service.run_one();
  ASSERT_TRUE(WaitForCondition([&n_accept]() { return n_accept == 4; }, 1000));

  ASSERT_EQ(n_accept, 4);
  std::vector<std::pair<std::string, int64_t>> expected_accepted{
      {"b", 0}, {"b", 1}, {"a", 0}, {"a", 1}};
  ASSERT_EQ(accepted, expected_accepted);

  auto default_executor = pool_manager->GetDefaultExecutor();
  default_executor->Join();

  queue.Stop();
}

TEST(UnorderedActorTaskExecutionQueueTest, TestTaskEvents) {
  // Test task events are recorded.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::vector<ConcurrencyGroup> concurrency_groups{ConcurrencyGroup{"io", 1, {}}};
  auto pool_manager =
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(concurrency_groups);

  int n_executed = 0;
  int n_canceled = 0;
  auto execute_task = [&n_executed](TaskToExecute &task) { n_executed++; };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  UnorderedActorTaskExecutionQueue queue(io_service,
                                         waiter,
                                         task_event_buffer,
                                         pool_manager,
                                         /*fiber_state_manager=*/nullptr,
                                         /*is_asyncio=*/false,
                                         /*fiber_max_concurrency=*/1,
                                         /*concurrency_groups=*/{},
                                         execute_task,
                                         cancel_task);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id_1 = TaskID::FromRandom(job_id);
  TaskSpecification task_spec_without_dependency;
  task_spec_without_dependency.GetMutableMessage().set_job_id(job_id.Binary());
  task_spec_without_dependency.GetMutableMessage().set_task_id(task_id_1.Binary());
  task_spec_without_dependency.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_without_dependency.GetMutableMessage().set_enable_task_events(true);

  queue.EnqueueTask(0, -1, MakeTaskToExecute(task_spec_without_dependency));
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
  queue.EnqueueTask(1, -1, MakeTaskToExecute(task_spec_with_dependency));
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

  ASSERT_EQ(n_executed, 2);
  ASSERT_EQ(n_canceled, 0);

  queue.Stop();
}

TEST(UnorderedActorTaskExecutionQueueTest, TestSameTaskMultipleAttempts) {
  // Test that if multiple attempts of the same task are received,
  // the next attempt only runs after the previous attempt finishes.
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  std::promise<void> attempt_2_start_promise;
  int n_canceled = 0;
  // The per-attempt behavior is dispatched on the task's attempt number.
  auto execute_task = [&attempt_1_start_promise,
                       &attempt_1_finish_promise,
                       &attempt_2_start_promise](TaskToExecute &task) {
    if (task.TaskSpec().AttemptNumber() == 1) {
      attempt_1_start_promise.set_value();
      attempt_1_finish_promise.get_future().wait();
    } else {
      attempt_2_start_promise.set_value();
    }
  };
  auto cancel_task = [&n_canceled](const TaskToExecute &task, const Status &status) {
    n_canceled++;
  };

  UnorderedActorTaskExecutionQueue queue(
      io_service,
      waiter,
      task_event_buffer,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      /*fiber_state_manager=*/nullptr,
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{},
      execute_task,
      cancel_task);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  TaskSpecification task_spec_1;
  task_spec_1.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_1.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_1.GetMutableMessage().set_attempt_number(1);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_1));
  attempt_1_start_promise.get_future().wait();
  TaskSpecification task_spec_2;
  task_spec_2.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_2.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_2.GetMutableMessage().set_attempt_number(2);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_2));
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

  ASSERT_EQ(n_canceled, 0);
  auto no_leak = [&queue] {
    absl::MutexLock lock(&queue.mu_);
    return queue.queued_actor_tasks_.empty() &&
           queue.pending_task_id_to_is_canceled.empty();
  };
  ASSERT_TRUE(WaitForCondition(no_leak, 10000));

  queue.Stop();
}

TEST(UnorderedActorTaskExecutionQueueTest, TestSameTaskMultipleAttemptsCancellation) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockTaskEventBuffer task_event_buffer;

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  std::atomic<bool> attempt_2_cancelled = false;
  std::atomic<bool> attempt_3_cancelled = false;
  std::atomic<bool> attempt_4_cancelled = false;
  // Only attempt 1 ever executes; all other attempts are expected to be cancelled. The
  // per-attempt behavior is dispatched on the task's attempt number.
  auto execute_task = [&attempt_1_start_promise,
                       &attempt_1_finish_promise](TaskToExecute &task) {
    if (task.TaskSpec().AttemptNumber() == 1) {
      attempt_1_start_promise.set_value();
      attempt_1_finish_promise.get_future().wait();
    } else {
      FAIL() << "Unexpected execution of attempt " << task.TaskSpec().AttemptNumber();
    }
  };
  auto cancel_task = [&attempt_2_cancelled, &attempt_3_cancelled, &attempt_4_cancelled](
                         const TaskToExecute &task, const Status &status) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    switch (task.TaskSpec().AttemptNumber()) {
    case 2:
      attempt_2_cancelled.store(true);
      break;
    case 3:
      attempt_3_cancelled.store(true);
      break;
    case 4:
      attempt_4_cancelled.store(true);
      break;
    default:
      FAIL() << "Unexpected cancellation of attempt " << task.TaskSpec().AttemptNumber();
    }
  };

  UnorderedActorTaskExecutionQueue queue(
      io_service,
      waiter,
      task_event_buffer,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      /*fiber_state_manager=*/nullptr,
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{},
      execute_task,
      cancel_task);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  TaskSpecification task_spec_1;
  task_spec_1.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_1.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_1.GetMutableMessage().set_attempt_number(1);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_1));
  attempt_1_start_promise.get_future().wait();

  TaskSpecification task_spec_2;
  task_spec_2.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_2.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_2.GetMutableMessage().set_attempt_number(2);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_2));

  // Adding attempt 4 should cancel the old attempt 2
  TaskSpecification task_spec_4;
  task_spec_4.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_4.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_4.GetMutableMessage().set_attempt_number(4);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_4));
  ASSERT_TRUE(attempt_2_cancelled.load());

  // Attempt 3 should be cancelled immediately since there is attempt 4
  // in the queue.
  TaskSpecification task_spec_3;
  task_spec_3.GetMutableMessage().set_type(TaskType::ACTOR_TASK);
  task_spec_3.GetMutableMessage().set_task_id(task_id.Binary());
  task_spec_3.GetMutableMessage().set_attempt_number(3);
  queue.EnqueueTask(-1, -1, MakeTaskToExecute(task_spec_3));
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
