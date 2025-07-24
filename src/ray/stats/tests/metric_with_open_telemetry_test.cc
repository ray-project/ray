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

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/telemetry/open_telemetry_metric_recorder.h"

namespace ray {
namespace telemetry {

using namespace std::literals;
using OpenTelemetryMetricRecorder = ray::telemetry::OpenTelemetryMetricRecorder;
using StatsConfig = ray::stats::StatsConfig;
using TagsMap = absl::flat_hash_map<std::string,std::string>;

DECLARE_stats(metric_gauge_test);
DEFINE_stats(metric_gauge_test,
             "A test gauge metric",
             ("Tag1", "Tag2", "Tag3"),
             (),
             ray::stats::GAUGE);

static ray::stats::Gauge LegacyMetricGaugeTest("legacy_metric_gauge_test",
                                               "A legacy test gauge metric",
                                               "",
                                               {"Tag1", "Tag2", "Tag3"});

DECLARE_stats(metric_counter_test);
DEFINE_stats(metric_counter_test,
             "A test counter metric",
             ("Tag1", "Tag2"),
             (),
             ray::stats::COUNT);

static ray::stats::Count LegacyMetricCounterTest("legacy_metric_counter_test",
                                                 "A legacy test counter metric",
                                                 "",
                                                 {"Tag1", "Tag2"});

DECLARE_stats(metric_sum_test);
DEFINE_stats(metric_sum_test, "A test sum metric", ("Tag1", "Tag2"), (), ray::stats::SUM);

static ray::stats::Sum LegacyMetricSumTest("legacy_metric_sum_test",
                                           "A legacy test sum metric",
                                           "",
                                           {"Tag1", "Tag2"});

class MetricTest : public ::testing::Test {
 public:
  MetricTest() = default;
  static void SetUpTestSuite() {
    StatsConfig::instance().SetIsDisableStats(false);
    for (auto &f : StatsConfig::instance().PopInitializers()) {
      f();
    }
    StatsConfig::instance().SetIsInitialized(true);
  }
};

TEST_F(MetricTest, TestCounterMetric) {
  StatsConfig::instance().SetGlobalTags({});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "metric_counter_test"));
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  STATS_metric_counter_test.Record(100.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
  LegacyMetricCounterTest.Record(100.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "legacy_metric_counter_test"));
}

TEST_F(MetricTest, TestSumMetric) {
  StatsConfig::instance().SetGlobalTags({});
  ASSERT_TRUE(
      OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered("metric_sum_test"));
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  STATS_metric_sum_test.Record(200.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
  LegacyMetricSumTest.Record(200.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "legacy_metric_sum_test"));
}

// Parameterized test for different possible cases when using gauge metrics
struct GaugeMetricCase {
  std::string metric_name;
  double record_value;
  TagsMap record_tags;
  TagsMap global_tags;
  TagsMap expected_tags;
  double expected_value;
};

struct MetricObservations {
  double value; // Recorded metric value
  TagsMap tags; // Recorded tags
};

class GaugeMetricTest : public MetricTest,
                        public ::testing::WithParamInterface<GaugeMetricCase> {
  public:
  // void TearDown() override {
  //   // Manually clear recorder state via friend access
  //   auto &rec = OpenTelemetryMetricRecorder::GetInstance();
  //   std::lock_guard<std::mutex> lock(rec.mutex_);
  //   rec.observations_by_name_.clear();
  //   rec.registered_instruments_.clear();
  //   rec.gauge_metric_names_.clear();
  //   // Restore global tags
  //   StatsConfig::instance().SetGlobalTags(saved_tags_);
  // }

  // Fetch both the recorded value and the actual tag map used for a given metric.
  std::optional<MetricObservations> getMetricObservations(const std::string &metric_name) {
    auto &recorder = OpenTelemetryMetricRecorder::GetInstance();
    std::lock_guard<std::mutex> lock(recorder.mutex_);

    auto metric_it = recorder.observations_by_name_.find(metric_name);
    if (metric_it == recorder.observations_by_name_.end()) {
      return std::nullopt;  // Not registered
    }
    auto &obs_it = metric_it->second;

    // Return the value and the tag-key used in the recorder
    return MetricObservations{obs_it->second, obs_it->first};
  }

  // Number of distinct metrics with at least one observation
  size_t getNumRecordedMetrics() {
    auto &rec = OpenTelemetryMetricRecorder::GetInstance();
    std::lock_guard<std::mutex> lock(rec.mutex_);
    return rec.observations_by_name_.size();
  }
};

TEST_P(GaugeMetricTest, RecordsValueAndTagsForAllMetricTypes) {
  const auto &tc = GetParam();
  // Apply per-case global tags
  StatsConfig::instance().SetGlobalTags(tc.global_tags);

  // Record the metric
  STATS_metric_gauge_test.Record(tc.record_value, tc.record_tags);
  LegacyMetricGaugeTest.Record(tc.record_value, tc.record_tags);

  // Verify that just two metrics have been recorded
  ASSERT_TRUE(getNumRecordedMetrics()==2);

  // Verify observations
  auto opt = getMetricObservations(tc.metric_name);
  ASSERT_TRUE(opt.has_value());
  EXPECT_EQ(opt->value, tc.expected_value);
  EXPECT_THAT(opt->tags, ::testing::UnorderedElementsAreArray(tc.expected_tags));

  // verify legacy metric observations
  auto legacy_opt = getMetricObservations(legacy_name, tc.record_tags);
  ASSERT_TRUE(legacy_opt.has_value());
  EXPECT_EQ(legacy_opt->value, tc.expected_value);
  EXPECT_THAT(legacy_opt->tags, ::testing::UnorderedElementsAreArray(tc.expected_tags));
}

INSTANTIATE_TEST_SUITE_P(
    GaugeMetric,
    GaugeMetricTest,
    ::testing::Values(
        // Gauge metric without global tags
        GaugeMetricCase{
            "metric_gauge_test",
            42.0,
            {{"Tag1","Value1"}, {"Tag2","Value2"}},
            {},  // no global tags
            {{"Tag1","Value1"}, {"Tag2","Value2"}},
            42.0
        },
        // Gauge metric with global tags
        GaugeMetricCase{
            "metric_gauge_test",
            42.0,
            {{"Tag1","Value1"}, {"Tag2","Value2"}},
            {{"Tag3","Global"}},
            {{"Tag1","Value1"}, {"Tag2","Value2"},{"Tag3","Global"}},
            42.0
        },
        // Gauge metric recorded with unsupported tag
        GaugeMetricCase{
            "metric_gauge_test",
            42.0,
            {{"Tag1","Value1"}, {"Tag2","Value2"}, {"UnSupportedTag","Value"}},
            {},  // no global tags
            {{"Tag1","Value1"}, {"Tag2","Value2"}}, // Unsupported tag will not be recorded
            42.0
        }
    ));
}  // namespace telemetry
}  // namespace ray