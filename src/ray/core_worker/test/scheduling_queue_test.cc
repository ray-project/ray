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
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {
namespace core {

class MockActorSchedulingQueue {
 public:
  MockActorSchedulingQueue(instrumented_io_context &main_io_service,
                           DependencyWaiter &waiter)
      : queue_(main_io_service, waiter) {}
  void Add(int64_t seq_no,
           int64_t client_processed_up_to,
           std::function<void(rpc::SendReplyCallback)> accept_request,
           std::function<void(rpc::SendReplyCallback)> reject_request,
           rpc::SendReplyCallback send_reply_callback = nullptr,
           TaskID task_id = TaskID::Nil(),
           const std::vector<rpc::ObjectReference> &dependencies = {}) {
    queue_.Add(seq_no,
               client_processed_up_to,
               std::move(accept_request),
               std::move(reject_request),
               send_reply_callback,
               "",
               FunctionDescriptorBuilder::Empty(),
               task_id,
               dependencies);
  }

 private:
  ActorSchedulingQueue queue_;
};

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
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej, nullptr);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr);
  queue.Add(2, -1, fn_ok, fn_rej, nullptr);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr);
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);
}

TEST(SchedulingQueueTest, TestWaitForObjects) {
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  ObjectID obj3 = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;

  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej, nullptr);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, TaskID::Nil(), ObjectIdsToRefs({obj1}));
  queue.Add(2, -1, fn_ok, fn_rej, nullptr, TaskID::Nil(), ObjectIdsToRefs({obj2}));
  queue.Add(3, -1, fn_ok, fn_rej, nullptr, TaskID::Nil(), ObjectIdsToRefs({obj3}));

  ASSERT_EQ(n_ok, 1);

  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(2);
  ASSERT_EQ(n_ok, 2);

  waiter.Complete(1);
  ASSERT_EQ(n_ok, 4);
}

TEST(SchedulingQueueTest, TestWaitForObjectsNotSubjectToSeqTimeout) {
  ObjectID obj1 = ObjectID::FromRandom();
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;

  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(0, -1, fn_ok, fn_rej, nullptr);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr, TaskID::Nil(), ObjectIdsToRefs({obj1}));

  ASSERT_EQ(n_ok, 1);
  io_service.run();
  ASSERT_EQ(n_rej, 0);
  waiter.Complete(0);
  ASSERT_EQ(n_ok, 2);
}

TEST(SchedulingQueueTest, TestOutOfOrder) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(2, -1, fn_ok, fn_rej, nullptr);
  queue.Add(0, -1, fn_ok, fn_rej, nullptr);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr);
  queue.Add(1, -1, fn_ok, fn_rej, nullptr);
  io_service.run();
  ASSERT_EQ(n_ok, 4);
  ASSERT_EQ(n_rej, 0);
}

TEST(SchedulingQueueTest, TestSeqWaitTimeout) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(2, -1, fn_ok, fn_rej, nullptr);
  queue.Add(0, -1, fn_ok, fn_rej, nullptr);
  queue.Add(3, -1, fn_ok, fn_rej, nullptr);
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 0);
  io_service.run();  // immediately triggers timeout
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);
  queue.Add(4, -1, fn_ok, fn_rej, nullptr);
  queue.Add(5, -1, fn_ok, fn_rej, nullptr);
  ASSERT_EQ(n_ok, 3);
  ASSERT_EQ(n_rej, 2);
}

TEST(SchedulingQueueTest, TestSkipAlreadyProcessedByClient) {
  instrumented_io_context io_service;
  MockWaiter waiter;
  MockActorSchedulingQueue queue(io_service, waiter);
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
  queue.Add(2, 2, fn_ok, fn_rej, nullptr);
  queue.Add(3, 2, fn_ok, fn_rej, nullptr);
  queue.Add(1, 2, fn_ok, fn_rej, nullptr);
  io_service.run();
  ASSERT_EQ(n_ok, 1);
  ASSERT_EQ(n_rej, 2);
}

TEST(SchedulingQueueTest, TestCancelQueuedTask) {
  std::unique_ptr<SchedulingQueue> queue = std::make_unique<NormalSchedulingQueue>();
  ASSERT_TRUE(queue->TaskQueueEmpty());
  int n_ok = 0;
  int n_rej = 0;
  auto fn_ok = [&n_ok](rpc::SendReplyCallback callback) { n_ok++; };
  auto fn_rej = [&n_rej](rpc::SendReplyCallback callback) { n_rej++; };
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
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
