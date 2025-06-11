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

DECLARE_stats(metric_gauge_test);
DEFINE_stats(
    metric_gauge_test, "A test gauge metric", ("Tag1", "Tag2"), (), ray::stats::GAUGE);

static ray::stats::Gauge LegacyMetricGaugeTest("legacy_metric_gauge_test",
                                               "A legacy test gauge metric",
                                               "",
                                               {"Tag1", "Tag2"});

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
    StatsConfig::instance().SetGlobalTags({});
    StatsConfig::instance().SetIsDisableStats(false);
    for (auto &f : StatsConfig::instance().PopInitializers()) {
      f();
    }
    StatsConfig::instance().SetIsInitialized(true);
  }

  std::optional<double> GetObservableMetricValue(
      const std::string &name,
      const absl::flat_hash_map<std::string, std::string> &tags) {
    auto &recorder = OpenTelemetryMetricRecorder::GetInstance();
    std::lock_guard<std::mutex> lock(recorder.mutex_);
    auto it = recorder.observations_by_name_.find(name);
    if (it == recorder.observations_by_name_.end()) {
      return std::nullopt;  // Not registered
    }
    auto tag_it = it->second.find(tags);
    if (tag_it != it->second.end()) {
      return tag_it->second;  // Get the value
    }
    return std::nullopt;
  }
};

TEST_F(MetricTest, TestGaugeMetric) {
  ASSERT_TRUE(
      OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered("metric_gauge_test"));
  STATS_metric_gauge_test.Record(42.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
  LegacyMetricGaugeTest.Record(24.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  // Test valid tags for a registered metric.
  ASSERT_EQ(GetObservableMetricValue("metric_gauge_test",
                                     {{"Tag1", "Value1"}, {"Tag2", "Value2"}}),
            42.0);
  // Test invalid tags for a registered metric.
  ASSERT_EQ(GetObservableMetricValue("metric_gauge_test",
                                     {{"Tag1", "Value1"}, {"Tag2", "Value3"}}),
            std::nullopt);
  // Test unregistered metric.
  ASSERT_EQ(GetObservableMetricValue("metric_gauge_test_unregistered",
                                     {{"Tag1", "Value1"}, {"Tag2", "Value2"}}),
            std::nullopt);
  // Test valid tags for a legacy metric.
  ASSERT_EQ(GetObservableMetricValue("legacy_metric_gauge_test",
                                     {{"Tag1", "Value1"}, {"Tag2", "Value2"}}),
            24.0);
  // Test invalid tags for a legacy metric.
  ASSERT_EQ(GetObservableMetricValue("legacy_metric_gauge_test",
                                     {{"Tag1", "Value1"}, {"Tag2", "Value3"}}),
            std::nullopt);
}

TEST_F(MetricTest, TestCounterMetric) {
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

}  // namespace telemetry
}  // namespace ray
