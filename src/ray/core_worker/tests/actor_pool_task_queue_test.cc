// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/actor_pool_task_queue.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace ray {
namespace core {
namespace {

/// Helper to create a simple PoolTask for testing.
PoolTask CreateTestPoolTask(int32_t attempt_number = 0) {
  PoolTask item;
  item.pool_task_id = TaskID::FromRandom(JobID());
  item.function = RayFunction();
  item.attempt_number = attempt_number;
  item.enqueued_at_ms = 12345;
  return item;
}

}  // namespace

TEST(FifoPoolTaskQueueTest, BasicPushPop) {
  FifoPoolTaskQueue queue;

  // Initially empty
  EXPECT_FALSE(queue.HasWork());
  EXPECT_EQ(queue.Size(), 0);
  EXPECT_FALSE(queue.Pop().has_value());

  // Push one item
  auto item1 = CreateTestPoolTask();
  auto item1_id = item1.pool_task_id;
  queue.Push(std::move(item1));

  EXPECT_TRUE(queue.HasWork());
  EXPECT_EQ(queue.Size(), 1);

  // Pop the item
  auto popped = queue.Pop();
  ASSERT_TRUE(popped.has_value());
  EXPECT_EQ(popped->pool_task_id, item1_id);

  // Empty again
  EXPECT_FALSE(queue.HasWork());
  EXPECT_EQ(queue.Size(), 0);
  EXPECT_FALSE(queue.Pop().has_value());
}

TEST(FifoPoolTaskQueueTest, FIFOOrdering) {
  FifoPoolTaskQueue queue;

  // Push multiple items
  std::vector<TaskID> task_ids;
  for (int i = 0; i < 5; i++) {
    auto item = CreateTestPoolTask();
    task_ids.push_back(item.pool_task_id);
    queue.Push(std::move(item));
  }

  EXPECT_EQ(queue.Size(), 5);

  // Pop in FIFO order
  for (int i = 0; i < 5; i++) {
    auto popped = queue.Pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(popped->pool_task_id, task_ids[i]);
  }

  EXPECT_EQ(queue.Size(), 0);
  EXPECT_FALSE(queue.Pop().has_value());
}

TEST(FifoPoolTaskQueueTest, RetryWithIncrementedAttempt) {
  FifoPoolTaskQueue queue;

  // Push item with attempt_number = 0
  auto item = CreateTestPoolTask(0);
  auto item_id = item.pool_task_id;
  queue.Push(std::move(item));

  // Pop and increment attempt number (simulating retry)
  auto popped = queue.Pop();
  ASSERT_TRUE(popped.has_value());
  EXPECT_EQ(popped->attempt_number, 0);

  // Re-enqueue with incremented attempt
  popped->attempt_number++;
  queue.Push(std::move(*popped));

  // Pop again and verify attempt number increased
  auto retry_popped = queue.Pop();
  ASSERT_TRUE(retry_popped.has_value());
  EXPECT_EQ(retry_popped->pool_task_id, item_id);
  EXPECT_EQ(retry_popped->attempt_number, 1);
}

TEST(FifoPoolTaskQueueTest, Clear) {
  FifoPoolTaskQueue queue;

  // Push multiple items
  for (int i = 0; i < 10; i++) {
    queue.Push(CreateTestPoolTask());
  }

  EXPECT_EQ(queue.Size(), 10);
  EXPECT_TRUE(queue.HasWork());

  // Clear all
  queue.Clear();

  EXPECT_EQ(queue.Size(), 0);
  EXPECT_FALSE(queue.HasWork());
  EXPECT_FALSE(queue.Pop().has_value());
}

TEST(FifoPoolTaskQueueTest, ManyItems) {
  FifoPoolTaskQueue queue;

  const int num_items = 1000;
  std::vector<TaskID> task_ids;

  // Push many items
  for (int i = 0; i < num_items; i++) {
    auto item = CreateTestPoolTask();
    task_ids.push_back(item.pool_task_id);
    queue.Push(std::move(item));
  }

  EXPECT_EQ(queue.Size(), num_items);

  // Pop all and verify order
  for (int i = 0; i < num_items; i++) {
    ASSERT_TRUE(queue.HasWork());
    auto popped = queue.Pop();
    ASSERT_TRUE(popped.has_value());
    EXPECT_EQ(popped->pool_task_id, task_ids[i]);
  }

  EXPECT_EQ(queue.Size(), 0);
  EXPECT_FALSE(queue.HasWork());
}

TEST(FifoPoolTaskQueueTest, InterleavedPushPop) {
  FifoPoolTaskQueue queue;

  // Interleave pushes and pops
  auto item1 = CreateTestPoolTask();
  auto id1 = item1.pool_task_id;
  queue.Push(std::move(item1));

  auto item2 = CreateTestPoolTask();
  auto id2 = item2.pool_task_id;
  queue.Push(std::move(item2));

  EXPECT_EQ(queue.Size(), 2);

  // Pop first
  auto popped1 = queue.Pop();
  ASSERT_TRUE(popped1.has_value());
  EXPECT_EQ(popped1->pool_task_id, id1);
  EXPECT_EQ(queue.Size(), 1);

  // Push another
  auto item3 = CreateTestPoolTask();
  auto id3 = item3.pool_task_id;
  queue.Push(std::move(item3));
  EXPECT_EQ(queue.Size(), 2);

  // Pop remaining two
  auto popped2 = queue.Pop();
  ASSERT_TRUE(popped2.has_value());
  EXPECT_EQ(popped2->pool_task_id, id2);

  auto popped3 = queue.Pop();
  ASSERT_TRUE(popped3.has_value());
  EXPECT_EQ(popped3->pool_task_id, id3);

  EXPECT_EQ(queue.Size(), 0);
}

}  // namespace core
}  // namespace ray
