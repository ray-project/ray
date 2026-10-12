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

#include <cstdlib>

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

}  // namespace go
}  // namespace ray
