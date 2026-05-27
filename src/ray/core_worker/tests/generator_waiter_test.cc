// Copyright 2023 The Ray Authors.
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

#include "ray/core_worker/generator_waiter.h"

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

TEST(GeneratorWaiterTest, TestBasic) {
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter =
      std::make_shared<TaskGeneratorBackpressureWaiter>(
          1,
          /*check_signals*/ []() { return Status::OK(); });

  auto wait = [waiter]() {
    auto status = waiter->WaitUntilObjectConsumed();
    ASSERT_TRUE(status.ok());
  };

  // Create and start a new thread
  std::thread t1(wait);
  // No objects are generated yet, so it should finish.
  t1.join();

  // 1 generated -> wait -> 1 consumed
  waiter->IncrementObjectGenerated();
  std::thread t2(wait);
  waiter->OnObjectReportAccepted();
  waiter->OnObjectConsumed(1);
  t2.join();

  // 2 generated -> wait -> 0 consumed (ignored) -> 3 consumed (resume).
  waiter->IncrementObjectGenerated();
  std::thread t3(wait);
  waiter->OnObjectReportAccepted();
  // If a lower value is given, it should ignore.
  waiter->OnObjectConsumed(0);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 1);
  // Larger value should resume.
  waiter->OnObjectConsumed(3);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 3);
  t3.join();
}

TEST(GeneratorWaiterTest, TestAllObjectsReported) {
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter =
      std::make_shared<TaskGeneratorBackpressureWaiter>(
          -1,
          /*check_signals*/ []() { return Status::OK(); });

  absl::Mutex mutex;
  absl::CondVar cond_var;
  bool done = false;

  auto wait = [&]() {
    auto status = waiter->WaitAllObjectsReported();
    ASSERT_TRUE(status.ok());

    absl::MutexLock lock(&mutex);
    done = true;
    cond_var.SignalAll();
  };

  // First object report in flight.
  waiter->IncrementObjectGenerated();
  std::thread t(wait);

  {
    absl::MutexLock lock(&mutex);
    cond_var.WaitWithTimeout(&mutex, absl::Milliseconds(10));
    ASSERT_FALSE(done);
    // Second object report in flight.
    waiter->IncrementObjectGenerated();
    // First object report acked, still blocked.
    waiter->OnObjectReportAccepted();
    cond_var.WaitWithTimeout(&mutex, absl::Milliseconds(10));
    ASSERT_FALSE(done);
    // Second object report acked, unblocked.
    waiter->OnObjectReportAccepted();
    cond_var.WaitWithTimeout(&mutex, absl::Milliseconds(10));
    ASSERT_TRUE(done);
  }

  t.join();
}

TEST(GeneratorWaiterTest, TestLargerThreshold) {
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter =
      std::make_shared<TaskGeneratorBackpressureWaiter>(
          3,
          /*check_signals*/ []() { return Status::OK(); });

  auto wait = [waiter]() {
    auto status = waiter->WaitUntilObjectConsumed();
    ASSERT_TRUE(status.ok());
  };

  // 1 generated -> wait -> 1 consumed
  waiter->IncrementObjectGenerated();
  waiter->IncrementObjectGenerated();
  std::thread t2(wait);
  t2.join();

  waiter->IncrementObjectGenerated();
  std::thread t3(wait);
  waiter->OnObjectReportAccepted();
  waiter->OnObjectConsumed(1);
  t3.join();
}

TEST(GeneratorWaiterTest, TestSignalFailure) {
  std::shared_ptr<std::atomic<bool>> signal_failed =
      std::make_shared<std::atomic<bool>>(false);
  std::shared_ptr<TaskGeneratorBackpressureWaiter> waiter =
      std::make_shared<TaskGeneratorBackpressureWaiter>(
          1,
          /*check_signals*/ [signal_failed]() {
            if (*signal_failed) {
              return Status::NotFound("");
            } else {
              return Status::OK();
            }
          });
  waiter->IncrementObjectGenerated();

  auto wait = [signal_failed, waiter]() {
    auto status = waiter->WaitUntilObjectConsumed();
    if (*signal_failed) {
      ASSERT_TRUE(status.IsNotFound());
    } else {
      ASSERT_TRUE(status.ok());
    }
  };

  // Create and start a new thread
  std::thread t(wait);
  *signal_failed = true;
  t.join();
}

TEST(GeneratorWaiterTest, ReserveActorWideSlotBlocksAndAtomicallyIncrements) {
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      2, []() { return Status::OK(); });
  ActorTaskBackpressureMetadata md(waiter);

  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_EQ(md.per_task_generated, 2);
  ASSERT_EQ(waiter->TotalObjectGenerated(), 2);

  std::atomic<bool> third_done(false);
  std::thread t([&] {
    ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
    third_done = true;
  });

  absl::SleepFor(absl::Milliseconds(50));
  ASSERT_FALSE(third_done.load());

  waiter->OnConsumedForTask(md, 1);  // delta=1, unconsumed=1 < 2.
  t.join();
  ASSERT_TRUE(third_done.load());
  ASSERT_EQ(md.per_task_generated, 3);
  ASSERT_EQ(md.per_task_consumed, 1);
}

TEST(GeneratorWaiterTest, OnConsumedForTaskAdvancesByDeltaIgnoresStale) {
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      10, []() { return Status::OK(); });
  ActorTaskBackpressureMetadata md(waiter);

  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_EQ(waiter->TotalObjectGenerated(), 3);

  waiter->OnConsumedForTask(md, 2);
  ASSERT_EQ(md.per_task_consumed, 2);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 2);

  // Stale / out-of-order reply: no-op.
  waiter->OnConsumedForTask(md, 1);
  ASSERT_EQ(md.per_task_consumed, 2);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 2);

  // Forward update: only the delta lands.
  waiter->OnConsumedForTask(md, 3);
  ASSERT_EQ(md.per_task_consumed, 3);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 3);
}

TEST(GeneratorWaiterTest, OnConsumedForTaskClampsTotalToPerTaskGenerated) {
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      10, []() { return Status::OK(); });
  ActorTaskBackpressureMetadata md(waiter);

  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_EQ(md.per_task_generated, 2);

  waiter->OnConsumedForTask(md, 3);
  ASSERT_EQ(md.per_task_consumed, 2);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 2);
}

TEST(GeneratorWaiterTest, TeardownReclaimsOutstandingAndIgnoresLateReports) {
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      2, []() { return Status::OK(); });
  ActorTaskBackpressureMetadata md(waiter);

  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());

  // Another task should be blocked at the cap right now.
  ActorTaskBackpressureMetadata md2(waiter);
  std::atomic<bool> reserved(false);
  std::thread t([&] {
    ASSERT_TRUE(waiter->ReserveActorWideSlot(md2).ok());
    reserved = true;
  });
  absl::SleepFor(absl::Milliseconds(50));
  ASSERT_FALSE(reserved.load());

  // Teardown the first task without it ack'ing any reports.
  waiter->TeardownTask(md);
  ASSERT_FALSE(md.task_alive);
  ASSERT_EQ(waiter->TotalObjectGenerated(), 0);

  // The blocked reservation should now proceed.
  t.join();
  ASSERT_TRUE(reserved.load());
  ASSERT_EQ(md2.per_task_generated, 1);
  ASSERT_EQ(waiter->TotalObjectGenerated(), 1);

  // A late consumption update against the torn-down metadata is a no-op.
  int64_t consumed_before = waiter->TotalObjectConsumed();
  waiter->OnConsumedForTask(md, 5);
  ASSERT_EQ(waiter->TotalObjectConsumed(), consumed_before);
  ASSERT_EQ(md.per_task_consumed, 0);
}

TEST(GeneratorWaiterTest, TeardownTaskIdempotent) {
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      2, []() { return Status::OK(); });
  ActorTaskBackpressureMetadata md(waiter);
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_TRUE(waiter->ReserveActorWideSlot(md).ok());
  ASSERT_EQ(waiter->TotalObjectGenerated(), 2);

  waiter->TeardownTask(md);
  ASSERT_EQ(waiter->TotalObjectGenerated(), 0);

  waiter->TeardownTask(md);
  ASSERT_EQ(waiter->TotalObjectGenerated(), 0);
}

TEST(GeneratorWaiterTest, ReserveActorWideSlotMultiThreadedCapNeverOverShoots) {
  constexpr int kThreshold = 5;
  constexpr int kThreads = 4;
  constexpr int kPerThread = 50;
  auto waiter = std::make_shared<ActorWideGeneratorBackpressureWaiter>(
      kThreshold, []() { return Status::OK(); });

  std::vector<std::shared_ptr<ActorTaskBackpressureMetadata>> mds;
  for (int i = 0; i < kThreads; ++i) {
    mds.push_back(std::make_shared<ActorTaskBackpressureMetadata>(waiter));
  }

  std::atomic<bool> consumer_stop(false);
  std::thread consumer([&] {
    int64_t per_task_total[kThreads] = {0};
    int idx = 0;
    while (!consumer_stop.load() ||
           waiter->TotalObjectConsumed() < kThreads * kPerThread) {
      if (waiter->TotalObjectGenerated() - waiter->TotalObjectConsumed() > 0) {
        per_task_total[idx] += 1;
        waiter->OnConsumedForTask(*mds[idx], per_task_total[idx]);
        idx = (idx + 1) % kThreads;
      } else {
        absl::SleepFor(absl::Microseconds(100));
      }
    }
  });

  std::vector<std::thread> producers;
  for (int i = 0; i < kThreads; ++i) {
    producers.emplace_back([&, i] {
      for (int j = 0; j < kPerThread; ++j) {
        ASSERT_TRUE(waiter->ReserveActorWideSlot(*mds[i]).ok());
        ASSERT_LE(waiter->TotalObjectGenerated() - waiter->TotalObjectConsumed(),
                  kThreshold);
      }
    });
  }
  for (auto &t : producers) t.join();
  consumer_stop = true;
  consumer.join();

  ASSERT_EQ(waiter->TotalObjectGenerated(), kThreads * kPerThread);
  ASSERT_EQ(waiter->TotalObjectConsumed(), kThreads * kPerThread);
}

}  // namespace core
}  // namespace ray
