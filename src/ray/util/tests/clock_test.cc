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

#include "ray/util/clock.h"

#include "gtest/gtest.h"

namespace ray {
namespace {

TEST(FakeClockTest, CustomStartTime) {
  FakeClock clock(absl::FromUnixSeconds(500));
  EXPECT_EQ(clock.Now(), absl::FromUnixSeconds(500));
}

TEST(FakeClockTest, AdvanceTime) {
  FakeClock clock;
  absl::Time start = clock.Now();
  clock.AdvanceTime(absl::Seconds(5));
  EXPECT_EQ(clock.Now(), start + absl::Seconds(5));
}

TEST(FakeClockTest, SetTime) {
  FakeClock clock;
  absl::Time target = absl::FromUnixSeconds(9999);
  clock.SetTime(target);
  EXPECT_EQ(clock.Now(), target);
}

TEST(FakeClockTest, DoesNotAdvanceOnItsOwn) {
  FakeClock clock;
  absl::Time t1 = clock.Now();
  absl::Time t2 = clock.Now();
  EXPECT_EQ(t1, t2);
}

TEST(ClockTest, ReturnsRealTime) {
  Clock clock;
  absl::Time before = absl::Now();
  absl::Time t = clock.Now();
  absl::Time after = absl::Now();
  EXPECT_GE(t, before);
  EXPECT_LE(t, after);
}

}  // namespace
}  // namespace ray
