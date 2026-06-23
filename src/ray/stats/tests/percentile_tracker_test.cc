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
//
// This implementation is based on tehuti's Percentiles class:
// https://github.com/tehuti-io/tehuti
// Original implementation: Copyright LinkedIn Corp. under Apache License 2.0

#include "ray/stats/percentile_tracker.h"

#include <gtest/gtest.h>

#include <random>

namespace ray {
namespace stats {

TEST(PercentileTrackerTest, BasicPercentiles) {
  // Create a tracker with 1024 buckets, max value 10000ms
  auto tracker = PercentileTracker::Create(1024, 10000.0);

  // Record some sample latencies
  std::vector<double> values = {1, 2, 3, 5, 10, 15, 20, 50, 100, 200, 500, 1000};
  for (double v : values) {
    tracker.Record(v);
  }

  EXPECT_EQ(tracker.GetCount(), 12);
  EXPECT_GT(tracker.GetP50(), 10.0);   // Median should be around 17.5
  EXPECT_LT(tracker.GetP50(), 25.0);   // Should be less than 25
  EXPECT_GT(tracker.GetP95(), 200.0);  // P95 should be > 200
  EXPECT_GT(tracker.GetP99(), 500.0);  // P99 should be > 500
  EXPECT_FALSE(std::isnan(tracker.GetMax()));
  EXPECT_FALSE(std::isnan(tracker.GetMean()));
}

TEST(PercentileTrackerTest, EmptyTracker) {
  auto tracker = PercentileTracker::Create(1024, 10000.0);

  EXPECT_EQ(tracker.GetCount(), 0);
  EXPECT_TRUE(std::isnan(tracker.GetP50()));
  EXPECT_TRUE(std::isnan(tracker.GetP95()));
  EXPECT_TRUE(std::isnan(tracker.GetP99()));
  EXPECT_TRUE(std::isnan(tracker.GetMax()));
  EXPECT_TRUE(std::isnan(tracker.GetMean()));
}

TEST(PercentileTrackerTest, ClearTracker) {
  auto tracker = PercentileTracker::Create(1024, 10000.0);

  tracker.Record(100.0);
  tracker.Record(200.0);
  EXPECT_EQ(tracker.GetCount(), 2);

  tracker.Clear();
  EXPECT_EQ(tracker.GetCount(), 0);
  EXPECT_TRUE(std::isnan(tracker.GetP50()));
}

TEST(PercentileTrackerTest, LargeDataset) {
  auto tracker = PercentileTracker::Create(2048, 10000.0);

  std::mt19937 gen(42);                               // Fixed seed for reproducibility
  std::exponential_distribution<> dist(1.0 / 100.0);  // Mean ~100ms

  // Simulate 10000 latency measurements
  for (int i = 0; i < 10000; i++) {
    double latency = dist(gen);
    tracker.Record(latency);
  }

  EXPECT_EQ(tracker.GetCount(), 10000);

  // For exponential distribution with mean 100:
  // P50 should be around 69 (ln(2) * 100)
  // P95 should be around 300
  // P99 should be around 460
  double p50 = tracker.GetP50();
  double p95 = tracker.GetP95();
  double p99 = tracker.GetP99();
  double mean = tracker.GetMean();

  EXPECT_GT(p50, 40.0);
  EXPECT_LT(p50, 100.0);
  EXPECT_GT(p95, 200.0);
  EXPECT_LT(p95, 400.0);
  EXPECT_GT(p99, 300.0);
  EXPECT_GT(mean, 80.0);
  EXPECT_LT(mean, 120.0);

  // Percentiles should be ordered
  EXPECT_LT(p50, p95);
  EXPECT_LT(p95, p99);
}

TEST(PercentileTrackerTest, MaxValue) {
  auto tracker = PercentileTracker::Create(1024, 10000.0);

  tracker.Record(5.0);
  tracker.Record(100.0);
  tracker.Record(50.0);
  tracker.Record(200.0);

  double max_val = tracker.GetMax();
  EXPECT_FALSE(std::isnan(max_val));
  // Max will be approximate due to histogram bucketing
  // Should be close to 200 (within 10% error)
  EXPECT_GT(max_val, 180.0);
  EXPECT_LT(max_val, 220.0);
}

TEST(PercentileTrackerTest, OutOfRangeValues) {
  auto tracker = PercentileTracker::Create(1024, 1000.0);

  // Record values within and outside the expected range
  tracker.Record(50.0);
  tracker.Record(500.0);
  tracker.Record(5000.0);   // Above max
  tracker.Record(10000.0);  // Way above max

  EXPECT_EQ(tracker.GetCount(), 4);
  EXPECT_FALSE(std::isnan(tracker.GetP50()));

  // All values should be captured, even out-of-range ones
  // (they go to the overflow bucket)
}

TEST(QuadraticBucketSchemeTest, BucketMapping) {
  QuadraticBucketScheme scheme(100, 1000.0);

  // Test basic bucket mapping
  EXPECT_GE(scheme.ToBucket(0.0), 0);
  EXPECT_LT(scheme.ToBucket(0.0), 100);

  EXPECT_GE(scheme.ToBucket(500.0), 0);
  EXPECT_LT(scheme.ToBucket(500.0), 100);

  // Values at or beyond max go to last bucket
  EXPECT_EQ(scheme.ToBucket(1000.0), 99);
  EXPECT_EQ(scheme.ToBucket(5000.0), 99);

  // Bucket boundaries should increase monotonically up to and including the last bucket
  for (int i = 0; i < 99; i++) {
    EXPECT_LT(scheme.FromBucket(i), scheme.FromBucket(i + 1));
  }

  // Last bucket returns max_value, not infinity
  EXPECT_EQ(scheme.FromBucket(99), 1000.0);
  EXPECT_FALSE(std::isinf(scheme.FromBucket(99)));
}

TEST(PercentileTrackerTest, OverflowValuesDoNotProduceInfinity) {
  auto tracker = PercentileTracker::Create(1024, 100.0);

  // Record values well above max_value
  for (int i = 0; i < 95; i++) {
    tracker.Record(1.0);
  }
  for (int i = 0; i < 5; i++) {
    tracker.Record(9999.0);  // way above max_value of 100.0
  }

  // Overflow values should clamp to max_value, not produce infinity
  EXPECT_FALSE(std::isinf(tracker.GetP99()));
  EXPECT_FALSE(std::isinf(tracker.GetMax()));
  EXPECT_FALSE(std::isinf(tracker.GetMean()));
  EXPECT_EQ(tracker.GetCount(), 100);
}

}  // namespace stats
}  // namespace ray
