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
#include "ray/observability/open_telemetry_metric_recorder.h"
#include "ray/stats/metric.h"
#include "ray/stats/tag_defs.h"

namespace ray {
namespace observability {

using namespace std::literals;
using OpenTelemetryMetricRecorder = ray::observability::OpenTelemetryMetricRecorder;
using StatsConfig = ray::stats::StatsConfig;
using TagsMap = absl::flat_hash_map<std::string, std::string>;

static ray::stats::Gauge MetricGaugeTest("metric_gauge_test",
                                         "A test gauge metric",
                                         "",
                                         {"Tag1", "Tag2", "Tag3"});

static ray::stats::Count MetricCounterTest("metric_counter_test",
                                           "A test counter metric",
                                           "",
                                           {"Tag1", "Tag2"});

static ray::stats::Sum MetricSumTest("metric_sum_test",
                                     "A test sum metric",
                                     "",
                                     {"Tag1", "Tag2"});

static ray::stats::Histogram MetricHistogramTest("metric_histogram_test",
                                                 "A test histogram metric",
                                                 "",
                                                 {1, 10, 100, 1000, 10000},
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

TEST_F(MetricTest, TestCounterMetric) {
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  MetricCounterTest.Record(100.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "metric_counter_test"));
}

TEST_F(MetricTest, TestSumMetric) {
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  MetricSumTest.Record(200.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  ASSERT_TRUE(
      OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered("metric_sum_test"));
}

TEST_F(MetricTest, TestHistogramMetric) {
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  MetricHistogramTest.Record(300.0, {{"Tag1"sv, "Value1"}, {"Tag2"sv, "Value2"}});
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "metric_histogram_test"));
}

// Parameterized test for different possible cases when using gauge metrics
struct GaugeMetricCase {
  std::string metric_name;
  double record_value;
  stats::TagsType record_tags;
  stats::TagsType global_tags;
  TagsMap expected_tags;
  double expected_value;
};

class GaugeMetricTest : public MetricTest,
                        public ::testing::WithParamInterface<GaugeMetricCase> {
  void TearDown() override { StatsConfig::instance().SetGlobalTags({}); }
};

TEST_P(GaugeMetricTest, TestGaugeMetricValidCases) {
  const auto &tc = GetParam();
  // Apply per-case global tags
  StatsConfig::instance().SetGlobalTags(tc.global_tags);

  // Record the metric
  MetricGaugeTest.Record(tc.record_value, tc.record_tags);

  // Verify observations
  auto actual = GetObservableMetricValue(tc.metric_name, tc.expected_tags);
  ASSERT_TRUE(actual.has_value());
  EXPECT_EQ(actual, tc.expected_value);
}

INSTANTIATE_TEST_SUITE_P(
    GaugeMetric,
    GaugeMetricTest,
    ::testing::Values(
        // Gauge metric without global tags
        GaugeMetricCase{
            /*metric_name=*/"metric_gauge_test",
            /*record_value=*/42.0,
            /*record_tags=*/
            {{stats::TagKeyType::Register("Tag1"), "Value1"},
             {stats::TagKeyType::Register("Tag2"), "Value1"}},
            /*global_tags=*/{},  // no global tags
            /*expected_tags=*/{{"Tag1", "Value1"}, {"Tag2", "Value1"}, {"Tag3", ""}},
            /*expected_value=*/42.0},
        // Gauge metric with a single global tag that is metric-specific
        GaugeMetricCase{/*metric_name=*/"metric_gauge_test",
                        /*record_value=*/52.0,
                        /*record_tags=*/
                        {{stats::TagKeyType::Register("Tag1"), "Value2"},
                         {stats::TagKeyType::Register("Tag2"), "Value2"}},
                        /*global_tags=*/{{stats::TagKeyType::Register("Tag3"), "Global"}},
                        /*expected_tags=*/
                        {{"Tag1", "Value2"}, {"Tag2", "Value2"}, {"Tag3", "Global"}},
                        /*expected_value=*/52.0},
        // Gauge metric with a non-metric-specific global tag
        GaugeMetricCase{
            /*metric_name=*/"metric_gauge_test",
            /*record_value=*/62.0,
            /*record_tags=*/
            {{stats::TagKeyType::Register("Tag1"), "Value3"},
             {stats::TagKeyType::Register("Tag2"), "Value3"}},
            /*global_tags=*/
            {
                {stats::TagKeyType::Register("Tag4"),
                 "Global"}  // Tag4 not registered in metric definition
            },
            /*expected_tags=*/
            {{"Tag1", "Value3"}, {"Tag2", "Value3"}, {"Tag3", ""}, {"Tag4", "Global"}},
            /*expected_value=*/62.0},
        // Gauge metric where global tags overwrite record tags
        GaugeMetricCase{/*metric_name=*/"metric_gauge_test",
                        /*record_value=*/72.0,
                        /*record_tags=*/
                        {{stats::TagKeyType::Register("Tag1"), "Value4"},
                         {stats::TagKeyType::Register("Tag2"), "Value4"},
                         {stats::TagKeyType::Register("Tag3"), "local"}},
                        /*global_tags=*/
                        {{stats::TagKeyType::Register("Tag3"), "Global"}},
                        /*expected_tags=*/
                        {{"Tag1", "Value4"}, {"Tag2", "Value4"}, {"Tag3", "Global"}},
                        /*expected_value=*/72.0},
        // Gauge metric recorded with an unsupported tag
        GaugeMetricCase{/*metric_name=*/"metric_gauge_test",
                        /*record_value=*/82.0,
                        /*record_tags=*/
                        {{stats::TagKeyType::Register("Tag1"), "Value5"},
                         {stats::TagKeyType::Register("Tag2"), "Value5"},
                         {stats::TagKeyType::Register("UnSupportedTag"), "Value"}},
                        /*global_tags=*/{},  // no global tags
                        /*expected_tags=*/
                        {{"Tag1", "Value5"},  // unsupported tag dropped
                         {"Tag2", "Value5"},
                         {"Tag3", ""}},
                        /*expected_value=*/82.0}));

}  // namespace observability
}  // namespace ray
