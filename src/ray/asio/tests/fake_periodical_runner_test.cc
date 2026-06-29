// Copyright 2026 The Ray Authors.
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

#include "ray/asio/fake_periodical_runner.h"

#include <memory>

#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/util/clock.h"

namespace ray {
namespace {

TEST(FakePeriodicalRunnerTest, DoesNotRunBeforePeriodElapses) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  EXPECT_EQ(count, 0);
  clock.AdvanceTime(absl::Milliseconds(99));
  EXPECT_EQ(count, 0);
}

TEST(FakePeriodicalRunnerTest, RunsOnceWhenPeriodElapses) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(count, 1);
}

TEST(FakePeriodicalRunnerTest, RunsRepeatedlyAcrossMultipleAdvances) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  clock.AdvanceTime(absl::Milliseconds(100));
  clock.AdvanceTime(absl::Milliseconds(100));
  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(count, 3);
}

TEST(FakePeriodicalRunnerTest, CatchesUpOnLargeTimeJump) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  // Jumping forward by 5 periods at once should invoke the function 5 times.
  clock.AdvanceTime(absl::Milliseconds(500));
  EXPECT_EQ(count, 5);
}

TEST(FakePeriodicalRunnerTest, PartialPeriodDoesNotAccumulateExtraRun) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  // 150ms total -> one full period elapsed, the remaining 50ms carries over.
  clock.AdvanceTime(absl::Milliseconds(150));
  EXPECT_EQ(count, 1);

  // Another 50ms reaches the second period boundary.
  clock.AdvanceTime(absl::Milliseconds(50));
  EXPECT_EQ(count, 2);
}

TEST(FakePeriodicalRunnerTest, MultipleFunctionsWithDifferentPeriods) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int fast = 0;
  int slow = 0;
  runner.RunFnPeriodically([&fast] { fast++; }, /*period_ms=*/100, "fast");
  runner.RunFnPeriodically([&slow] { slow++; }, /*period_ms=*/300, "slow");

  clock.AdvanceTime(absl::Milliseconds(300));
  EXPECT_EQ(fast, 3);
  EXPECT_EQ(slow, 1);
}

TEST(FakePeriodicalRunnerTest, SchedulesRelativeToClockStartTime) {
  FakeClock clock(absl::FromUnixSeconds(500));
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/1000, "fn");

  clock.AdvanceTime(absl::Milliseconds(999));
  EXPECT_EQ(count, 0);
  clock.AdvanceTime(absl::Milliseconds(1));
  EXPECT_EQ(count, 1);
}

TEST(FakePeriodicalRunnerTest, SchedulesRelativeToRegistrationTime) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);

  // Advance before registering; the new task should be scheduled relative to
  // the current time, not the clock's original start.
  clock.AdvanceTime(absl::Milliseconds(50));
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  clock.AdvanceTime(absl::Milliseconds(99));
  EXPECT_EQ(count, 0);
  clock.AdvanceTime(absl::Milliseconds(1));
  EXPECT_EQ(count, 1);
}

TEST(FakePeriodicalRunnerTest, SetTimeTriggersDueFunctions) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");

  clock.SetTime(clock.Now() + absl::Milliseconds(250));
  EXPECT_EQ(count, 2);
}

TEST(FakePeriodicalRunnerTest, ZeroPeriodIsIgnored) {
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int count = 0;
  runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/0, "fn");

  clock.AdvanceTime(absl::Seconds(10));
  EXPECT_EQ(count, 0);
}

TEST(FakePeriodicalRunnerTest, UnregistersFromClockOnDestruction) {
  FakeClock clock;
  int count = 0;
  {
    FakePeriodicalRunner runner(clock);
    runner.RunFnPeriodically([&count] { count++; }, /*period_ms=*/100, "fn");
    clock.AdvanceTime(absl::Milliseconds(100));
    EXPECT_EQ(count, 1);
  }
  // After the runner is destroyed, advancing the clock must not call into it.
  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(count, 1);
}

TEST(FakePeriodicalRunnerTest, CallbackRegisteredDuringCallbackRunsNextAdvance) {
  // Registering a new function from within a running callback must not deadlock
  // and the new function should fire on a subsequent advance.
  FakeClock clock;
  FakePeriodicalRunner runner(clock);
  int outer = 0;
  int inner = 0;
  runner.RunFnPeriodically(
      [&] {
        outer++;
        if (outer == 1) {
          runner.RunFnPeriodically([&inner] { inner++; }, /*period_ms=*/100, "inner");
        }
      },
      /*period_ms=*/100,
      "outer");

  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(outer, 1);
  EXPECT_EQ(inner, 0);

  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(outer, 2);
  EXPECT_EQ(inner, 1);
}

TEST(FakePeriodicalRunnerTest, CallbackDestroyingAnotherRunnerDoesNotUseAfterFree) {
  // A callback on one runner destroys a second runner during the same time
  // advance. The destroyed runner unregisters its callback from the clock; the
  // clock must not invoke that (now-dangling) callback. Without the re-check in
  // NotifyTimeChanged this is a use-after-free (caught under ASAN).
  FakeClock clock;
  FakePeriodicalRunner first(clock);

  auto second = std::make_unique<FakePeriodicalRunner>(clock);
  int second_count = 0;
  second->RunFnPeriodically([&second_count] { second_count++; }, /*period_ms=*/100, "b");

  int first_count = 0;
  first.RunFnPeriodically(
      [&] {
        first_count++;
        // Destroy the other observer from within this callback.
        second.reset();
      },
      /*period_ms=*/100,
      "a");

  clock.AdvanceTime(absl::Milliseconds(100));
  EXPECT_EQ(first_count, 1);
  // `second` was destroyed before its callback would have run; it must not fire.
  EXPECT_EQ(second_count, 0);
}

}  // namespace
}  // namespace ray
