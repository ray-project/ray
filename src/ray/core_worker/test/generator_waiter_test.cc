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

#include <thread>

#include "gtest/gtest.h"
#include "ray/core_worker/common.h"

namespace ray {
namespace core {

TEST(GeneratorWaiterTest, TestBasic) {
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(
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
  waiter->HandleObjectReported(1);
  t2.join();

  // 2 generated -> wait -> 0 consumed (ignored) -> 3 consumed (resume).
  waiter->IncrementObjectGenerated();
  std::thread t3(wait);
  // If a lower value is given, it should ignore.
  waiter->HandleObjectReported(0);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 1);
  // Larger value should resume.
  waiter->HandleObjectReported(3);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 3);
  t3.join();
}

TEST(GeneratorWaiterTest, TestAllObjectsReported) {
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(
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
    waiter->HandleObjectReported(0);
    cond_var.WaitWithTimeout(&mutex, absl::Milliseconds(10));
    ASSERT_FALSE(done);
    // Second object report acked, unblocked.
    waiter->HandleObjectReported(0);
    cond_var.WaitWithTimeout(&mutex, absl::Milliseconds(10));
    ASSERT_TRUE(done);
  }

  t.join();
}

TEST(GeneratorWaiterTest, TestLargerThreshold) {
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(
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
  waiter->HandleObjectReported(1);
  t3.join();
}

TEST(GeneratorWaiterTest, TestSignalFailure) {
  std::shared_ptr<std::atomic<bool>> signal_failed =
      std::make_shared<std::atomic<bool>>(false);
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(1,
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

}  // namespace core
}  // namespace ray
