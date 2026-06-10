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

#include "ray/observability/windowed_metric.h"

#include "absl/time/time.h"
#include "gtest/gtest.h"

namespace ray {
namespace observability {
namespace {

// A fixed base time to anchor sample timestamps. Values are arbitrary.
const absl::Time kT0 = absl::FromUnixSeconds(1000);

TEST(WindowedMetricTest, EmptyWindowHasNoMax) {
  WindowedMetric window(absl::Seconds(30));
  EXPECT_FALSE(window.WindowedMax().has_value());
}

TEST(WindowedMetricTest, FirstSampleReported) {
  WindowedMetric window(absl::Seconds(30));
  window.Add(kT0, 5.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 5.0);
}

TEST(WindowedMetricTest, ReportsOnlyWhenMaxChanges) {
  WindowedMetric window(absl::Seconds(30));

  // First sample: max changes, reported.
  window.Add(kT0, 10.0);
  EXPECT_TRUE(window.WindowedMax().has_value());

  // A second read with no intervening change returns nothing.
  EXPECT_FALSE(window.WindowedMax().has_value());

  // Lower value within the window: max unchanged, not reported.
  window.Add(kT0 + absl::Seconds(1), 4.0);
  EXPECT_FALSE(window.WindowedMax().has_value());

  // Equal to the current max: still unchanged, not reported.
  window.Add(kT0 + absl::Seconds(2), 10.0);
  EXPECT_FALSE(window.WindowedMax().has_value());

  // Higher value: max changes, reported.
  window.Add(kT0 + absl::Seconds(3), 12.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 12.0);
}

TEST(WindowedMetricTest, ChangesCollapseBetweenReads) {
  WindowedMetric window(absl::Seconds(30));

  // Multiple changes between reads collapse into a single report of the latest max.
  window.Add(kT0, 10.0);
  window.Add(kT0 + absl::Seconds(1), 20.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 20.0);
  EXPECT_FALSE(window.WindowedMax().has_value());
}

TEST(WindowedMetricTest, EvictsSamplesOutsideWindow) {
  WindowedMetric window(absl::Seconds(30));

  // A high sample at t=0.
  window.Add(kT0, 100.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 100.0);

  // A lower sample 10s later stays within the window; max stays 100 (unchanged).
  window.Add(kT0 + absl::Seconds(10), 20.0);
  EXPECT_FALSE(window.WindowedMax().has_value());

  // 31s after the first sample, the high sample falls out of the 30s window. The
  // max now drops to the surviving samples, so it is reported.
  window.Add(kT0 + absl::Seconds(31), 5.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 20.0);
}

TEST(WindowedMetricTest, SampleAtWindowEdgeIsRetained) {
  WindowedMetric window(absl::Seconds(30));

  window.Add(kT0, 50.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 50.0);

  // Exactly 30s later: the first sample is at age == window and is retained
  // (eviction is strictly older-than), so the max is still 50 (unchanged).
  window.Add(kT0 + absl::Seconds(30), 10.0);
  EXPECT_FALSE(window.WindowedMax().has_value());

  // Just past the edge: the first sample is evicted, max drops to 10.
  window.Add(kT0 + absl::Seconds(30) + absl::Nanoseconds(1), 10.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 10.0);
}

TEST(WindowedMetricTest, AllSamplesEvictedKeepsLatest) {
  WindowedMetric window(absl::Seconds(30));

  window.Add(kT0, 80.0);
  EXPECT_DOUBLE_EQ(*window.WindowedMax(), 80.0);

  // Far in the future: every prior sample is evicted, leaving only the new one.
  window.Add(kT0 + absl::Hours(1), 3.0);
  auto reported = window.WindowedMax();
  ASSERT_TRUE(reported.has_value());
  EXPECT_DOUBLE_EQ(*reported, 3.0);
}

TEST(WindowedMetricTest, NonPositiveWindowKeepsLatestSample) {
  // A zero (or negative) window must not evict the just-added sample, which would
  // leave the container empty and crash the max computation.
  WindowedMetric zero_window(absl::ZeroDuration());
  zero_window.Add(kT0, 7.0);
  EXPECT_DOUBLE_EQ(*zero_window.WindowedMax(), 7.0);
  // The next sample evicts the prior one but is itself retained, so the max tracks
  // the latest value rather than crashing.
  zero_window.Add(kT0 + absl::Seconds(1), 2.0);
  EXPECT_DOUBLE_EQ(*zero_window.WindowedMax(), 2.0);

  WindowedMetric negative_window(-absl::Seconds(5));
  negative_window.Add(kT0, 1.0);
  EXPECT_DOUBLE_EQ(*negative_window.WindowedMax(), 1.0);
}

}  // namespace
}  // namespace observability
}  // namespace ray
