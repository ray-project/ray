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

#include <thread>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/transport/task_receiver.h"

using namespace std::chrono_literals;

namespace ray {
namespace core {

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

TEST(SchedulingQueueTest, TestInOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(0,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(1,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(2,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(3,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestWaitForObjects) {
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;

  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(0,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(1,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            ObjectIdsToRefs({obj1}));
  queue.Add(2,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            ObjectIdsToRefs({obj2}));
  queue.Add(3,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            ObjectIdsToRefs({obj3}));

  ASSERT_EQ(n_ok, 1);

  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(2);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(1);
  ASSERT_EQ(n_ok, 4);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestWaitForObjectsNotSubjectToSeqTimeout) {
  ObjectID obj1 = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;

  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(0,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(1,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            ObjectIdsToRefs({obj1}));

  ASSERT_EQ(n_ok, 1);
  io_service.run();
  ASSERT_EQ(n_rej, 0);
  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestOutOfOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(2,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(0,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(3,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(1,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestSeqWaitTimeout) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(2,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(0,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(3,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 0);
  io_service.run();  // immediately triggers timeout
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);
  queue.Add(4,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(5,
            -1,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  ASSERT_EQ(n_ok, 3);
  ASSERT_EQ(n_rej, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestSkipAlreadyProcessedByClient) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  ActorSchedulingQueue queue(io_service,
                             waiter,
                             std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(),
                             std::make_shared<ConcurrencyGroupManager<FiberState>>(),
                             /*is_asyncio=*/false,
                             /*fiber_max_concurrency=*/1,
                             /*concurrency_groups=*/{});
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(2,
            2,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(3,
            2,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  queue.Add(1,
            2,
            fn_ok,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            TaskID::Nil(),
            /*attempt_number=*/0,
            {});
  io_service.run();
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);

  queue.Stop();
}

TEST(SchedulingQueueTest, TestCancelQueuedTask) {
  std::unique_ptr<SchedulingQueue> queue = std::make_unique<NormalSchedulingQueue>();
  ASSERT_TRUE(queue->TaskQueueEmpty());
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, "", FunctionDescriptorBuilder::Empty());
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, "", FunctionDescriptorBuilder::Empty());
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, "", FunctionDescriptorBuilder::Empty());
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, "", FunctionDescriptorBuilder::Empty());
  queue->Add(-1, -1, fn_ok, fn_rej, nullptr, "", FunctionDescriptorBuilder::Empty());
  ASSERT_TRUE(queue->CancelTaskIfFound(TaskID::Nil()));
  ASSERT_FALSE(queue->TaskQueueEmpty());
  queue->ScheduleRequests();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 1);

  queue->Stop();
}

TEST(OutOfOrderActorSchedulingQueueTest, TestSameTaskMultipleAttempts) {
  // Test that if multiple attempts of the same task are received,
  // the next attempt only runs after the previous attempt finishes.
  instrumented_io_context io_service;
  MockWaiter waiter;
  OutOfOrderActorSchedulingQueue queue(
      io_service,
      waiter,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      std::make_shared<ConcurrencyGroupManager<FiberState>>(),
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{});
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  auto fn_ok_1 = [&attempt_1_start_promise,
                  &attempt_1_finish_promise](rpc::SendReplyCallback callback) {
    attempt_1_start_promise.set_value();
    attempt_1_finish_promise.get_future().wait();
  };
  std::promise<void> attempt_2_start_promise;
  auto fn_ok_2 = [&attempt_2_start_promise](rpc::SendReplyCallback callback) {
    attempt_2_start_promise.set_value();
  };
  int n_rej = 0;
  auto fn_rej = [&n_rej](const Status &status, rpc::SendReplyCallback callback) {
    n_rej++;
  };
  queue.Add(-1,
            -1,
            fn_ok_1,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/1,
            {});
  attempt_1_start_promise.get_future().wait();
  queue.Add(-1,
            -1,
            fn_ok_2,
            fn_rej,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/2,
            {});
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
  OutOfOrderActorSchedulingQueue queue(
      io_service,
      waiter,
      std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(
          std::vector<ConcurrencyGroup>(),
          /*max_concurrency_for_default_concurrency_group=*/100),
      std::make_shared<ConcurrencyGroupManager<FiberState>>(),
      /*is_asyncio=*/false,
      /*fiber_max_concurrency=*/1,
      /*concurrency_groups=*/{});
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::FromRandom(job_id);

  std::promise<void> attempt_1_start_promise;
  std::promise<void> attempt_1_finish_promise;
  auto fn_ok_1 = [&attempt_1_start_promise,
                  &attempt_1_finish_promise](rpc::SendReplyCallback callback) {
    attempt_1_start_promise.set_value();
    attempt_1_finish_promise.get_future().wait();
  };
  auto fn_rej_1 = [](const Status &status, rpc::SendReplyCallback callback) {
    ASSERT_FALSE(true);
  };
  queue.Add(-1,
            -1,
            fn_ok_1,
            fn_rej_1,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/1,
            {});
  attempt_1_start_promise.get_future().wait();

  auto fn_ok_2 = [](rpc::SendReplyCallback callback) { ASSERT_FALSE(true); };
  std::atomic<bool> attempt_2_cancelled = false;
  auto fn_rej_2 = [&attempt_2_cancelled](const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_2_cancelled.store(true);
  };
  queue.Add(-1,
            -1,
            fn_ok_2,
            fn_rej_2,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/2,
            {});

  auto fn_ok_4 = [](rpc::SendReplyCallback callback) { ASSERT_FALSE(true); };
  std::atomic<bool> attempt_4_cancelled = false;
  auto fn_rej_4 = [&attempt_4_cancelled](const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_4_cancelled.store(true);
  };
  // Adding attempt 4 should cancel the old attempt 2
  queue.Add(-1,
            -1,
            fn_ok_4,
            fn_rej_4,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/4,
            {});
  ASSERT_TRUE(attempt_2_cancelled.load());

  auto fn_ok_3 = [](rpc::SendReplyCallback callback) { ASSERT_FALSE(true); };
  std::atomic<bool> attempt_3_cancelled = false;
  auto fn_rej_3 = [&attempt_3_cancelled](const Status &status,
                                         rpc::SendReplyCallback callback) {
    ASSERT_TRUE(status.IsSchedulingCancelled());
    attempt_3_cancelled.store(true);
  };
  // Attempt 3 should be cancelled immediately since there is attempt 4
  // in the queue.
  queue.Add(-1,
            -1,
            fn_ok_3,
            fn_rej_3,
            nullptr,
            "",
            FunctionDescriptorBuilder::Empty(),
            task_id,
            /*attempt_number=*/3,
            {});
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
