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
      std::make_shared<GeneratorBackpressureWaiter>(1);

  auto wait = [waiter]() {
    auto status =
        waiter->WaitUntilObjectConsumed(/*check_signals*/ []() { return Status::OK(); });
    ASSERT_TRUE(status.ok());
  };

  // Create and start a new thread
  std::thread t1(wait);
  // No objects are generated yet, so it should finish.
  t1.join();

  // 1 generated -> wait -> 1 consumed
  waiter->IncrementObjectGenerated();
  std::thread t2(wait);
  waiter->UpdateTotalObjectConsumed(1);
  t2.join();

  // 2 generated -> wait -> 0 consumed (ignored) -> 3 consumed (resume).
  waiter->IncrementObjectGenerated();
  std::thread t3(wait);
  // If a lower value is given, it should ignore.
  waiter->UpdateTotalObjectConsumed(0);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 1);
  // Larger value should resume.
  waiter->UpdateTotalObjectConsumed(3);
  ASSERT_EQ(waiter->TotalObjectConsumed(), 3);
  t3.join();
}

TEST(GeneratorWaiterTest, TestLargerThreshold) {
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(3);

  auto wait = [waiter]() {
    auto status =
        waiter->WaitUntilObjectConsumed(/*check_signals*/ []() { return Status::OK(); });
    ASSERT_TRUE(status.ok());
  };

  // 1 generated -> wait -> 1 consumed
  waiter->IncrementObjectGenerated();
  waiter->IncrementObjectGenerated();
  std::thread t2(wait);
  t2.join();

  waiter->IncrementObjectGenerated();
  std::thread t3(wait);
  waiter->UpdateTotalObjectConsumed(1);
  t3.join();
}

TEST(GeneratorWaiterTest, TestSignalFailure) {
  std::shared_ptr<GeneratorBackpressureWaiter> waiter =
      std::make_shared<GeneratorBackpressureWaiter>(1);
  std::shared_ptr<std::atomic<bool>> signal_failed =
      std::make_shared<std::atomic<bool>>(false);
  waiter->IncrementObjectGenerated();

  auto wait = [signal_failed, waiter]() {
    auto status = waiter->WaitUntilObjectConsumed(/*check_signals*/ [signal_failed]() {
      if (*signal_failed) {
        return Status::NotFound("");
      } else {
        return Status::OK();
      }
    });

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
