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

#include "ray/observability/metric_utils.h"

#include "absl/time/time.h"
#include "gtest/gtest.h"

namespace ray {
namespace observability {
namespace {

// A fixed base time to anchor sample timestamps. Values are arbitrary.
const absl::Time kT0 = absl::FromUnixSeconds(1000);

TEST(LatencySlidingWindowTest, EmptyWindowHasNoMax) {
  LatencySlidingWindow window(absl::Seconds(30));
  EXPECT_FALSE(window.WindowedMax().has_value());
}

TEST(LatencySlidingWindowTest, FirstSampleAlwaysReported) {
  LatencySlidingWindow window(absl::Seconds(30));
  auto reported = window.Add(kT0, 5.0);
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 5.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 5.0);
}

TEST(LatencySlidingWindowTest, ReportsOnlyWhenMaxChanges) {
  LatencySlidingWindow window(absl::Seconds(30));

  // First sample: reported.
  EXPECT_TRUE(window.Add(kT0, 10.0).has_value());

  // Lower value within the window: max unchanged, not reported.
  EXPECT_FALSE(window.Add(kT0 + absl::Seconds(1), 4.0).has_value());

  // Equal to the current max: still unchanged, not reported.
  EXPECT_FALSE(window.Add(kT0 + absl::Seconds(2), 10.0).has_value());

  // Higher value: max changes, reported.
  auto reported = window.Add(kT0 + absl::Seconds(3), 12.0);
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 12.0);
}

TEST(LatencySlidingWindowTest, EvictsSamplesOutsideWindow) {
  LatencySlidingWindow window(absl::Seconds(30));

  // A high sample at t=0.
  EXPECT_TRUE(window.Add(kT0, 100.0).has_value());
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 100.0);

  // A lower sample 10s later stays within the window; max stays 100.
  EXPECT_FALSE(window.Add(kT0 + absl::Seconds(10), 20.0).has_value());
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 100.0);

  // 31s after the first sample, the high sample falls out of the 30s window. The
  // max now drops to the surviving samples, so it is reported.
  auto reported = window.Add(kT0 + absl::Seconds(31), 5.0);
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 20.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 20.0);
}

TEST(LatencySlidingWindowTest, SampleAtWindowEdgeIsRetained) {
  LatencySlidingWindow window(absl::Seconds(30));

  EXPECT_TRUE(window.Add(kT0, 50.0).has_value());

  // Exactly 30s later: the first sample is at age == window and is retained
  // (eviction is strictly older-than), so the max is still 50.
  EXPECT_FALSE(window.Add(kT0 + absl::Seconds(30), 10.0).has_value());
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 50.0);

  // Just past the edge: the first sample is evicted, max drops to 10.
  auto reported = window.Add(kT0 + absl::Seconds(30) + absl::Nanoseconds(1), 10.0);
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 10.0);
}

TEST(LatencySlidingWindowTest, AllSamplesEvictedKeepsLatest) {
  LatencySlidingWindow window(absl::Seconds(30));

  EXPECT_TRUE(window.Add(kT0, 80.0).has_value());

  // Far in the future: every prior sample is evicted, leaving only the new one.
  auto reported = window.Add(kT0 + absl::Hours(1), 3.0);
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 3.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 3.0);
}

}  // namespace
}  // namespace observability
}  // namespace ray
