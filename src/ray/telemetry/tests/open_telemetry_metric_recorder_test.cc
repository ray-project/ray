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

#include "ray/telemetry/open_telemetry_metric_recorder.h"

#include "gtest/gtest.h"

namespace ray {
namespace telemetry {

class OpenTelemetryMetricRecorderTest : public ::testing::Test {
 public:
  OpenTelemetryMetricRecorderTest()
      : recorder_(OpenTelemetryMetricRecorder::GetInstance()) {}

  static void SetUpTestSuite() {
    // Initialize the OpenTelemetryMetricRecorder with a mock endpoint and intervals
    OpenTelemetryMetricRecorder::GetInstance().RegisterGrpcExporter(
        "localhost:1234",
        std::chrono::milliseconds(10000),
        std::chrono::milliseconds(5000));
  }

 protected:
  OpenTelemetryMetricRecorder &recorder_;
};

TEST_F(OpenTelemetryMetricRecorderTest, TestGaugeMetric) {
  recorder_.RegisterGaugeMetric("test_metric", "Test metric description");
  recorder_.SetMetricValue("test_metric", {{"tag1", "value1"}}, 42.0);
  ASSERT_EQ(recorder_.GetMetricValue("test_metric", {{"tag1", "value1"}}), 42.0);
  ASSERT_EQ(recorder_.GetMetricValue("test_metric", {{"tag1", "value2"}}), std::nullopt);
}

}  // namespace telemetry
}  // namespace ray
