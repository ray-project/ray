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

DECLARE_stats(metric_test);
DEFINE_stats(metric_test, "A test gauge metric", ("Tag1", "Tag2"), (), ray::stats::GAUGE);

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
      OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered("metric_test"));
  STATS_metric_test.Record(42.0, {{"Tag1", "Value1"}, {"Tag2", "Value2"}});
  ASSERT_EQ(OpenTelemetryMetricRecorder::GetInstance().GetMetricValue(
                "metric_test", {{"Tag1", "Value1"}, {"Tag2", "Value2"}}),
            42.0);
}

}  // namespace telemetry
}  // namespace ray
