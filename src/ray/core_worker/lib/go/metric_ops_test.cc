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

#include "ray/core_worker/lib/go/metric_ops.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <thread>
#include <vector>

namespace ray {
namespace go {

using ::testing::ElementsAre;

TEST(MetricOpsTest, BuildTagsMatchesKeysAndValues) {
  const char *keys[] = {"k1", "k2"};
  const char *values[] = {"v1", "v2"};
  auto tags = MetricOps::BuildTags(keys, values, 2);
  ASSERT_EQ(tags.size(), 2u);
  EXPECT_EQ(tags[0].first, stats::TagKeyType::Register("k1"));
  EXPECT_EQ(tags[0].second, "v1");
  EXPECT_EQ(tags[1].first, stats::TagKeyType::Register("k2"));
  EXPECT_EQ(tags[1].second, "v2");
}

TEST(MetricOpsTest, BuildTagsEmptyWhenCountZero) {
  auto tags = MetricOps::BuildTags(nullptr, nullptr, 0);
  EXPECT_TRUE(tags.empty());
}

TEST(MetricOpsTest, BuildTagsSkipsNullEntries) {
  const char *keys[] = {"k1", nullptr};
  const char *values[] = {"v1", "v2"};
  auto tags = MetricOps::BuildTags(keys, values, 2);
  ASSERT_EQ(tags.size(), 1u);
  EXPECT_EQ(tags[0].first, stats::TagKeyType::Register("k1"));
  EXPECT_EQ(tags[0].second, "v1");
}

TEST(MetricOpsTest, ToStringVectorAndToDoubleVector) {
  const char *strs[] = {"a", "b"};
  auto sv = MetricOps::ToStringVector(strs, 2);
  EXPECT_THAT(sv, ElementsAre("a", "b"));

  const double ds[] = {0.5, 1.5, 2.5};
  auto dv = MetricOps::ToDoubleVector(ds, 3);
  EXPECT_THAT(dv, ElementsAre(0.5, 1.5, 2.5));
}

TEST(MetricOpsTest, IsValidMetricNameAcceptsPattern) {
  EXPECT_TRUE(MetricOps::IsValidMetricName("ray_my_metric"));
  EXPECT_TRUE(MetricOps::IsValidMetricName("ray:metric1"));
  EXPECT_TRUE(MetricOps::IsValidMetricName("_leading_underscore"));
  EXPECT_TRUE(MetricOps::IsValidMetricName(":leading_colon"));
  EXPECT_TRUE(MetricOps::IsValidMetricName("a"));
}

TEST(MetricOpsTest, IsValidMetricNameRejectsInvalid) {
  EXPECT_FALSE(MetricOps::IsValidMetricName(nullptr));
  EXPECT_FALSE(MetricOps::IsValidMetricName(""));
  EXPECT_FALSE(MetricOps::IsValidMetricName("1starts_with_digit"));
  EXPECT_FALSE(MetricOps::IsValidMetricName("has space"));
  EXPECT_FALSE(MetricOps::IsValidMetricName("has-dash"));
  EXPECT_FALSE(MetricOps::IsValidMetricName("has.dot"));
}

TEST(MetricOpsTest, RegisterRecordUnregisterSmoke) {
  const char *tag_keys[] = {"k1"};
  const char *tag_values[] = {"v1"};
  char *error = nullptr;
  int64_t handle =
      ray_metric_register_count("ray_test_count", "desc", "unit", tag_keys, 1, &error);
  EXPECT_NE(handle, -1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_record(handle, 42.0, tag_keys, tag_values, 1, &error), 1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle, &error), 1);
  EXPECT_EQ(error, nullptr);
}

TEST(MetricOpsTest, RegisterTagKeySmoke) {
  char *error = nullptr;
  EXPECT_EQ(ray_metric_register_tag_key("ray_go_test_key", &error), 1);
  EXPECT_EQ(error, nullptr);
}

TEST(MetricOpsTest, RegisterRejectsInvalidName) {
  char *error = nullptr;
  int64_t handle = ray_metric_register_count("1bad", "d", "u", nullptr, 0, &error);
  EXPECT_EQ(handle, -1);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, RegisterHistogramRejectsEmptyBoundaries) {
  char *error = nullptr;
  int64_t handle = ray_metric_register_histogram(
      "ray_test_hist", "d", "u", nullptr, 0, nullptr, 0, &error);
  EXPECT_EQ(handle, -1);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, RegisterHistogramRejectsNonMonotonicBoundaries) {
  const double bad_boundaries[] = {1.0, 0.5};
  char *error = nullptr;
  int64_t handle = ray_metric_register_histogram(
      "ray_test_hist", "d", "u", bad_boundaries, 2, nullptr, 0, &error);
  EXPECT_EQ(handle, -1);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, RecordUnknownHandleRejected) {
  char *error = nullptr;
  EXPECT_EQ(ray_metric_record(12345, 1.0, nullptr, nullptr, 0, &error), 0);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, UnregisterUnknownHandleRejected) {
  char *error = nullptr;
  EXPECT_EQ(ray_metric_unregister(12345, &error), 0);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, UnregisterTwiceRejected) {
  char *error = nullptr;
  int64_t handle =
      ray_metric_register_count("ray_test_count_twice", "d", "u", nullptr, 0, &error);
  EXPECT_NE(handle, -1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle, &error), 1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle, &error), 0);
  EXPECT_NE(error, nullptr);
  free(error);
}

TEST(MetricOpsTest, RecordAfterUnregisterRejected) {
  char *error = nullptr;
  int64_t handle =
      ray_metric_register_gauge("ray_test_gauge", "d", "u", nullptr, 0, &error);
  EXPECT_NE(handle, -1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle, &error), 1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_record(handle, 1.0, nullptr, nullptr, 0, &error), 0);
  EXPECT_NE(error, nullptr);
  free(error);
}

// Handles are monotonic (never reused), not raw object addresses: two
// consecutive registrations must return consecutive values, and after an
// unregister the retired handle stays rejected while the newly issued handle is
// accepted.
TEST(MetricOpsTest, HandleIsMonotonicAndNeverReused) {
  char *error = nullptr;
  int64_t handle1 =
      ray_metric_register_count("ray_test_handle_seq_a", "d", "u", nullptr, 0, &error);
  ASSERT_NE(handle1, -1);
  int64_t handle2 =
      ray_metric_register_sum("ray_test_handle_seq_b", "d", "u", nullptr, 0, &error);
  ASSERT_NE(handle2, -1);
  EXPECT_EQ(handle2, handle1 + 1);

  EXPECT_EQ(ray_metric_unregister(handle1, &error), 1);
  EXPECT_EQ(error, nullptr);

  // The retired handle is rejected with the unknown-handle error.
  EXPECT_EQ(ray_metric_record(handle1, 1.0, nullptr, nullptr, 0, &error), 0);
  ASSERT_NE(error, nullptr);
  free(error);
  error = nullptr;
  EXPECT_EQ(ray_metric_unregister(handle1, &error), 0);
  ASSERT_NE(error, nullptr);
  free(error);
  error = nullptr;

  // Concurrently issued handles keep marching forward and still work: after
  // this register the previous (retired) handle1 is no longer the latest, and a
  // new register must not hand back handle1.
  int64_t handle3 =
      ray_metric_register_gauge("ray_test_handle_seq_c", "d", "u", nullptr, 0, &error);
  ASSERT_NE(handle3, -1);
  EXPECT_GT(handle3, handle2);
  EXPECT_NE(handle3, handle1);

  EXPECT_EQ(ray_metric_record(handle2, 2.0, nullptr, nullptr, 0, &error), 1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle2, &error), 1);
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(ray_metric_unregister(handle3, &error), 1);
  EXPECT_EQ(error, nullptr);
}

// F8: opencensus views registered by Record are process-lifetime (no
// deregistration API), so re-registering a metric with the SAME name must reuse
// the existing view and keep working across register/record/unregister cycles.
TEST(MetricOpsTest, SameNameReregisterReusesView) {
  char *error = nullptr;
  for (int i = 0; i < 3; ++i) {
    int64_t handle =
        ray_metric_register_count("ray_test_reregister", "d", "u", nullptr, 0, &error);
    ASSERT_NE(handle, -1);
    ASSERT_EQ(error, nullptr);
    EXPECT_EQ(ray_metric_record(handle, 1.0, nullptr, nullptr, 0, &error), 1);
    EXPECT_EQ(error, nullptr);
    EXPECT_EQ(ray_metric_unregister(handle, &error), 1);
    EXPECT_EQ(error, nullptr);
  }
}

// F6: a record that races an unregister must never dereference a freed metric
// nor deadlock. Records copy the shared_ptr out under the registry lock and
// call Record outside of it, while unregister erases under the lock and
// releases outside of it.
TEST(MetricOpsTest, ConcurrentRecordAndUnregisterHammer) {
  constexpr int kIterations = 200;
  const char *tag_keys[] = {"k1"};
  const char *tag_values[] = {"v1"};

  for (int iter = 0; iter < kIterations; ++iter) {
    char *error = nullptr;
    int64_t handle =
        ray_metric_register_gauge("ray_test_hammer", "d", "u", nullptr, 0, &error);
    ASSERT_NE(handle, -1);
    ASSERT_EQ(error, nullptr);

    std::atomic<bool> start{false};
    std::atomic<int> records_ok{0};
    std::atomic<int> unregisters_ok{0};

    std::thread recorder([&]() {
      while (!start.load()) {
      }
      char *rec_error = nullptr;
      if (ray_metric_record(handle, 1.0, tag_keys, tag_values, 1, &rec_error) == 1) {
        records_ok.fetch_add(1);
      }
      if (rec_error) {
        free(rec_error);
      }
    });
    std::thread unregisterer([&]() {
      while (!start.load()) {
      }
      char *unreg_error = nullptr;
      if (ray_metric_unregister(handle, &unreg_error) == 1) {
        unregisters_ok.fetch_add(1);
      }
      if (unreg_error) {
        free(unreg_error);
      }
    });

    start.store(true);
    recorder.join();
    unregisterer.join();

    EXPECT_EQ(unregisters_ok.load(), 1);
    // Exactly one of the two operations wins the race: either the record lands
    // before the unregister, or it is rejected with the unknown-handle error.
    EXPECT_LE(records_ok.load(), 1);

    // The handle is always gone afterwards, and a second unregister is rejected.
    char *error2 = nullptr;
    EXPECT_EQ(ray_metric_unregister(handle, &error2), 0);
    ASSERT_NE(error2, nullptr);
    free(error2);
  }
}

}  // namespace go
}  // namespace ray
