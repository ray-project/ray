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

using OpenTelemetryMetricRecorder = ray::telemetry::OpenTelemetryMetricRecorder;
using StatsConfig = ray::stats::StatsConfig;

DECLARE_stats(metric_gauge_test);
DEFINE_stats(
    metric_gauge_test, "A test gauge metric", ("Tag1", "Tag2"), (), ray::stats::GAUGE);

DECLARE_stats(metric_counter_test);
DEFINE_stats(metric_counter_test,
             "A test counter metric",
             ("Tag1", "Tag2"),
             (),
             ray::stats::COUNT);

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
};

TEST_F(MetricTest, TestGaugeMetric) {
  ASSERT_TRUE(
      OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered("metric_gauge_test"));
  STATS_metric_gauge_test.Record(42.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
  ASSERT_EQ(OpenTelemetryMetricRecorder::GetInstance().GetObservableMetricValue(
                "metric_gauge_test", {{"Tag1", "Value1"}, {"Tag2", "Value2"}}),
            42.0);
}

TEST_F(MetricTest, TestCounterMetric) {
  ASSERT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "metric_counter_test"));
  // We only test that recording is not crashing. The actual value is not checked
  // because open telemetry does not provide a way to retrieve the value of a counter.
  // Checking value is performed via e2e tests instead (e.g., in test_metrics_agent.py).
  STATS_metric_counter_test.Record(100.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
}

}  // namespace telemetry
}  // namespace ray
